//! The startup preflight, exercised through the real binary.
//!
//! `storage::conformance` tests the *check*. `s3_conformance` tests that the
//! check survives being spoken over HTTP. Neither touches what the worker does
//! with the verdict, and that gap had a shape worth caring about: the refusing
//! branch is what everyone sees, while `--allow-unsafe-commits` is the branch an
//! operator reaches **when they are already in trouble** — a store the check is
//! wrong about, at the moment they need to start anyway. It had never been
//! executed. A slip in that path (a bad error return, a panic in the warning
//! loop, a flag that quietly does nothing) would only ever surface then.
//!
//! So this spawns the actual binary against `s3s-fs`, which genuinely fails the
//! concurrency probe, and asserts both branches.
//!
//! # Why the verdict is checked before anything is asserted
//!
//! s3s-fs fails the probe *by racing*: its `PutObject` tests `path.exists()` and
//! then writes, so concurrent callers can all observe absence. That is a race,
//! not a constant, and a run where every racer happened to serialise would make
//! the store look conforming. The suite is therefore run in-process first, and if
//! it returns a pass the test reports and stops rather than asserting something
//! that only holds when a race goes a particular way. Same reasoning as
//! `s3_conformance`'s reported-not-asserted verdict: the point is the worker's
//! behaviour, not policing s3s-fs.

use blaze::storage::verify_conditional_put;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use s3s::service::S3ServiceBuilder;
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    )
}

/// A free localhost port. Racy in principle; the window is microseconds and the
/// alternative is a fixed port that collides with whatever else is running.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The worker under test, configured to reach `endpoint` as its warehouse.
///
/// `AmazonS3Builder::from_env()` is what `main` uses, so the credentials and
/// endpoint go in as environment rather than flags — which is also how a real
/// deployment supplies them.
fn worker(endpoint: &str, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_blaze"));
    cmd.env("AWS_ENDPOINT", endpoint)
        .env("AWS_ALLOW_HTTP", "true")
        .env("AWS_ACCESS_KEY_ID", "blaze")
        .env("AWS_SECRET_ACCESS_KEY", "blazesecret")
        .env("AWS_REGION", "us-east-1")
        .env("RUST_LOG", "info")
        .arg("--warehouse")
        .arg("s3://blaze/graph")
        .arg("--retention-interval-secs")
        .arg("0")
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// GET `/v1/health`, or `None` if the port is not serving yet.
fn health(port: u16) -> Option<String> {
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    sock.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    sock.write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut body = String::new();
    sock.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Wait for the worker to serve, or give up. Returns the health body.
fn await_serving(child: &mut Child, port: u16) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        // A worker that died is never going to serve; stop waiting for it.
        if let Ok(Some(_)) = child.try_wait() {
            return None;
        }
        if let Some(body) = health(port)
            && body.contains("\"status\":\"ok\"")
        {
            return Some(body);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    None
}

fn output_of(child: Child) -> String {
    let out = child.wait_with_output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Both branches of the preflight gate, against a store that really does fail
/// it.
///
/// One test rather than two because they share an expensive fixture and, more
/// to the point, because the pair is the claim: refusing by default is only
/// worth anything if the override actually works, and an override that starts
/// anything is only worth anything if the default refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_preflight_refuses_by_default_and_the_escape_hatch_starts_anyway() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = spawn_s3(dir.path()).await;

    // Establish the verdict first — see the module docs.
    if verify_conditional_put(&client(&endpoint), &StorePath::from("graph/edges"))
        .await
        .is_ok()
    {
        eprintln!(
            "s3s-fs arbitrated correctly this run, so there is no non-conforming \
             store to point the worker at; skipping"
        );
        return;
    }

    // 1. Default: refuse, and say why.
    let refused = output_of(
        worker(
            &endpoint,
            &["--listen", "127.0.0.1:1", "--grpc-listen", "127.0.0.1:1"],
        )
        .spawn()
        .unwrap(),
    );
    assert!(
        refused.contains("refusing to start"),
        "the worker started, or failed for some other reason: {refused}"
    );
    assert!(
        refused.contains("--allow-unsafe-commits"),
        "the refusal must name the way out; an operator who needs it should not \
         have to find it in the source: {refused}"
    );
    // The message has to identify *which* property broke, or it tells an
    // operator only that something is wrong with something.
    assert!(
        refused.contains("concurrent") || refused.contains("winner"),
        "the refusal does not name the property that failed: {refused}"
    );

    // 2. With the hatch: start anyway, serve, and complain about it.
    let (api, grpc) = (free_port(), free_port());
    let mut child = worker(
        &endpoint,
        &[
            "--allow-unsafe-commits",
            "--listen",
            &format!("127.0.0.1:{api}"),
            "--grpc-listen",
            &format!("127.0.0.1:{grpc}"),
        ],
    )
    .spawn()
    .unwrap();

    let serving = await_serving(&mut child, api);
    let _ = child.kill();
    let logs = output_of(child);

    assert!(
        serving.is_some(),
        "--allow-unsafe-commits did not start the worker, which is the one \
         situation the flag exists for: {logs}"
    );
    assert!(
        logs.contains("--allow-unsafe-commits is set")
            || logs.contains("two leaders can silently publish divergent histories"),
        "the worker started unsafely without saying so; the flag must be loud, \
         not silent: {logs}"
    );
}
