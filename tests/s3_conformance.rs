//! Run the put-if-absent conformance suite against a real S3 server.
//!
//! `storage::conformance`'s own tests check it against stores we control: two
//! that are correct, three deliberately broken. That leaves the part those
//! cannot reach — whether any of it survives being spoken over HTTP, with
//! `object_store`'s AWS client in between turning `PutMode::Create` into
//! `If-None-Match: *` and a `412 PreconditionFailed` back into
//! `Error::AlreadyExists`. Every link in that chain is a place the check could
//! silently stop meaning anything.
//!
//! `s3s-fs` is a filesystem-backed S3 server. It is a development tool, not a
//! production store, and what follows is not a criticism of it.
//!
//! # What this found
//!
//! s3s-fs **passes the sequential checks and fails the concurrent one** — six of
//! eight racers won a key that only one should have. Its `PutObject` resolves
//! `If-None-Match: *` by testing `path.exists()` and then writing, with nothing
//! between the two, so concurrent callers all observe absence and all proceed.
//!
//! That is the most useful thing in this file. Every store deliberately broken in
//! `conformance`'s unit tests was broken by me, which makes them proof that the
//! check detects *what I thought of*. Here an independent implementation, written
//! with no knowledge of this project, fails in exactly the way the concurrency
//! probe exists to catch — and would have passed a suite that only put twice in a
//! row.
//!
//! # What is asserted, and what is only reported
//!
//! The sequential behaviour is **asserted**, because it is the end-to-end proof
//! that the header mapping works, and it is stable.
//!
//! The concurrency verdict is **reported, not asserted**. Pinning "s3s-fs fails"
//! would turn a fix on their side into a red build here, and the point was never
//! to police s3s-fs.

use blaze::storage::{ConformanceError, verify_conditional_put};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use s3s::service::S3ServiceBuilder;
use std::sync::Arc;

/// Serve `s3s-fs` on an ephemeral port and return its endpoint.
async fn spawn_s3(dir: &std::path::Path) -> String {
    std::fs::create_dir_all(dir.join("blaze")).unwrap();
    let fs = s3s_fs::FileSystem::new(dir).unwrap();
    let mut builder = S3ServiceBuilder::new(fs);
    builder.set_auth(s3s::auth::SimpleAuth::from_single("blaze", "blazesecret"));
    let service = builder.build();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let service = service.clone();
            tokio::spawn(async move {
                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                .await;
            });
        }
    });
    format!("http://{addr}")
}

fn client(endpoint: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_bucket_name("blaze")
            .with_region("us-east-1")
            .with_access_key_id("blaze")
            .with_secret_access_key("blazesecret")
            // The setting `main` pins explicitly, so this exercises the
            // production configuration rather than a variant of it.
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    )
}

/// `PutMode::Create` must survive the whole round trip: client, HTTP,
/// `If-None-Match: *`, a server-side precondition failure, and back into an
/// `AlreadyExists` this code can match on.
///
/// This is the assertion the unit tests structurally cannot make. They call
/// `put_opts` on an in-process store, so they would pass unchanged even if the
/// AWS client dropped the header on the floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_put_survives_the_round_trip_to_a_real_server() {
    let dir = tempfile::tempdir().unwrap();
    let store = client(&spawn_s3(dir.path()).await);
    let create = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let key = StorePath::from("graph/edges/roundtrip");

    store
        .put_opts(&key, PutPayload::from_static(b"first"), create.clone())
        .await
        .expect("a fresh key must accept a conditional put over HTTP");

    let again = store
        .put_opts(&key, PutPayload::from_static(b"second"), create)
        .await;
    assert!(
        matches!(again, Err(object_store::Error::AlreadyExists { .. })),
        "a 412 must arrive as AlreadyExists, not as a generic error the commit \
         path would treat as a retryable failure: {again:?}"
    );

    let stored = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(
        stored.as_ref(),
        b"first",
        "the conflict was reported but the body was overwritten anyway"
    );
}

/// The full suite against the same server. Whatever the verdict, it must be a
/// *verdict* — a store is either usable for commits or it is not, and failing to
/// decide is the only unacceptable outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_suite_reaches_a_verdict_against_a_real_server() {
    let dir = tempfile::tempdir().unwrap();
    let store = client(&spawn_s3(dir.path()).await);

    match verify_conditional_put(&store, &StorePath::from("graph/edges")).await {
        Ok(()) => eprintln!("s3s-fs conforms; it can arbitrate commits"),
        // A transport or auth failure means the harness is broken, not that a
        // verdict was reached — the one outcome worth failing on.
        Err(e @ ConformanceError::Unusable { .. }) => {
            panic!("the suite could not talk to the server at all: {e}")
        }
        Err(e) => eprintln!("s3s-fs does not conform: {e}"),
    }
}
