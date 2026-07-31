//! Does this object store actually provide put-if-absent?
//!
//! The whole HA commit protocol is one line: `put_opts(path, payload,
//! PutMode::Create)` must let **exactly one** of N concurrent callers win. Two
//! workers that both believe they committed a snapshot at the same sequence
//! produce divergent topology that nothing downstream can detect, because each
//! one's local stack agrees with what it wrote.
//!
//! That property belongs to the deployed store, not to us, and it cannot be
//! proven by testing a different store. What it *can* be is a **checked
//! precondition**: probe the real endpoint at startup and refuse to run as a
//! leader if it does not hold.
//!
//! This matters more than it looks, because the guarantee is implicit in two
//! places at once. `object_store` defaults S3 to
//! [`S3ConditionalPut::ETagMatch`], and an S3-compatible store may accept
//! `If-None-Match` in its API surface while ignoring it. Neither is visible
//! until a leadership handoff.
//!
//! The same suite runs in three places, which is the point of it being one
//! function: unit tests here against `LocalFileSystem` and `InMemory`, CI
//! against a real S3 implementation, and production at boot against whatever is
//! configured.
//!
//! [`S3ConditionalPut::ETagMatch`]: object_store::aws::S3ConditionalPut

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::sync::Arc;
use tracing::{info, warn};

/// How many callers race for one key, and how many keys are raced for.
///
/// Arbitration bugs are not always deterministic — a store that takes a lock per
/// shard, or that resolves conflicts by timestamp, can be right most of the time.
/// One round would not see it.
const RACERS: usize = 8;
const ROUNDS: usize = 4;

/// Why a store cannot be used for leader commits.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    #[error(
        "store rejected a plain conditional put of a fresh key: {source}. \
         PutMode::Create is unsupported here, so snapshot commits have no way to \
         arbitrate between leaders"
    )]
    CreateUnsupported {
        #[source]
        source: object_store::Error,
    },

    #[error(
        "store accepted a second conditional put of an existing key. It is \
         ignoring the precondition, so two leaders would both believe they \
         committed the same snapshot sequence and the topology they publish \
         would silently diverge"
    )]
    OverwritesOnCreate,

    #[error(
        "store reported a conflict for an existing key but the stored bytes \
         changed anyway. The precondition is advisory here, which is worse than \
         not supporting it: the loser of a commit race is told it lost while \
         having overwritten the winner"
    )]
    ConflictButOverwrote,

    #[error(
        "{winners} of {racers} concurrent conditional puts of the same fresh key \
         succeeded (expected exactly 1). Concurrent commits are not arbitrated, \
         so split-brain writes would both be accepted"
    )]
    ConcurrentWinners { winners: usize, racers: usize },

    #[error(
        "the winner of a concurrent conditional put is not the value the store \
         kept. Whichever caller is told it committed, a different caller's \
         snapshot is what a reader would see"
    )]
    WinnerNotStored,

    #[error("probing the store failed: {source}")]
    Unusable {
        #[source]
        source: anyhow::Error,
    },
}

/// Verify that `store` provides put-if-absent, under `prefix`.
///
/// Probe objects are written under `{prefix}/_preflight/` and deleted
/// afterwards. That sits outside `metadata/`, which is the only prefix the
/// catalog lists, so a probe left behind by a crash cannot be mistaken for a
/// snapshot — and it is under the table prefix deliberately, so the probe runs
/// against the same bucket, path and credentials the real commits will use
/// rather than somewhere the permissions might differ.
pub async fn verify_conditional_put(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<(), ConformanceError> {
    let base = prefix.clone().join("_preflight");
    let run = uuid::Uuid::new_v4();
    let mut probes = Vec::new();
    let result = run_checks(store, &base, run, &mut probes).await;

    // Clean up whatever we wrote, even on failure. A store that cannot delete is
    // not a conformance failure — commits never delete — so this only warns.
    for path in &probes {
        if let Err(e) = store.delete(path).await {
            warn!(path = %path, error = %e, "could not remove a preflight probe object");
        }
    }
    result
}

async fn run_checks(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    run: uuid::Uuid,
    probes: &mut Vec<Path>,
) -> Result<(), ConformanceError> {
    let create = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let unusable = |e: object_store::Error| ConformanceError::Unusable { source: e.into() };

    // 1. A fresh key must accept a conditional put. This also rules out a store
    //    that reports a conflict unconditionally, which would pass every check
    //    below while never committing anything.
    let key = base.clone().join(format!("{run}-sequential"));
    probes.push(key.clone());
    store
        .put_opts(&key, PutPayload::from_static(b"first"), create.clone())
        .await
        .map_err(|source| ConformanceError::CreateUnsupported { source })?;

    // 2. The same key must now refuse one.
    match store
        .put_opts(&key, PutPayload::from_static(b"second"), create.clone())
        .await
    {
        Err(object_store::Error::AlreadyExists { .. }) => {}
        Ok(_) => return Err(ConformanceError::OverwritesOnCreate),
        Err(e) => return Err(unusable(e)),
    }

    // 3. And the first write must be what is actually stored. A store can report
    //    the conflict correctly and still have written the body — the failure
    //    mode that looks healthiest and corrupts fastest.
    let got = store
        .get(&key)
        .await
        .map_err(unusable)?
        .bytes()
        .await
        .map_err(unusable)?;
    if got.as_ref() != b"first" {
        return Err(ConformanceError::ConflictButOverwrote);
    }

    // 4. Now the case the protocol actually depends on: simultaneous callers, not
    //    sequential ones. A store that serializes puts per key passes 1-3 for free
    //    and can still resolve a genuine race by last-write-wins.
    for round in 0..ROUNDS {
        let key = base.clone().join(format!("{run}-race-{round}"));
        probes.push(key.clone());
        let mut tasks = Vec::with_capacity(RACERS);
        for racer in 0..RACERS {
            let (store, key, opts) = (store.clone(), key.clone(), create.clone());
            let body = format!("racer-{racer}");
            tasks.push(tokio::spawn(async move {
                let outcome = store
                    .put_opts(&key, PutPayload::from(body.clone()), opts)
                    .await;
                (body, outcome)
            }));
        }

        let mut winner = None;
        let mut winners = 0usize;
        for task in tasks {
            let (body, outcome) = task
                .await
                .map_err(|e| ConformanceError::Unusable { source: e.into() })?;
            match outcome {
                Ok(_) => {
                    winners += 1;
                    winner = Some(body);
                }
                Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(e) => return Err(unusable(e)),
            }
        }
        if winners != 1 {
            return Err(ConformanceError::ConcurrentWinners {
                winners,
                racers: RACERS,
            });
        }

        let stored = store
            .get(&key)
            .await
            .map_err(unusable)?
            .bytes()
            .await
            .map_err(unusable)?;
        if stored.as_ref() != winner.expect("exactly one winner").as_bytes() {
            return Err(ConformanceError::WinnerNotStored);
        }
    }

    info!(
        racers = RACERS,
        rounds = ROUNDS,
        "object store provides put-if-absent; snapshot commits can arbitrate"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutResult, Result as OsResult,
    };

    fn prefix() -> Path {
        Path::from("graph/edges")
    }

    /// The two stores the test suite and the local dev path actually use. Both
    /// are correct — `LocalFileSystem` via `create_new`, `InMemory` via a lock —
    /// so this is the suite's own negative control: if it cannot pass a store
    /// that is right, it cannot be trusted to fail one that is wrong.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_stores_we_ship_with_conform() {
        let dir = tempfile::tempdir().unwrap();
        let local: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        verify_conditional_put(&local, &prefix()).await.unwrap();

        let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        verify_conditional_put(&memory, &prefix()).await.unwrap();
    }

    /// Probes must not survive a successful run — an operator watching object
    /// counts should not see the preflight, and nothing should accumulate one
    /// object per restart.
    #[tokio::test]
    async fn probes_are_cleaned_up() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        verify_conditional_put(&store, &prefix()).await.unwrap();
        let left: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
            .await
            .unwrap();
        assert!(left.is_empty(), "preflight left {left:?} behind");
    }

    /// The ways a store can break the precondition. One wrapper rather than
    /// three, because the delegation boilerplate is the same and the behaviours
    /// differ by a few lines each.
    #[derive(Debug, Clone, Copy)]
    enum Broken {
        /// Every put succeeds: the precondition is parsed and dropped on the
        /// floor, which is what an S3-compatible store that does not implement
        /// `If-None-Match` looks like.
        IgnoresPrecondition,
        /// Reports the conflict correctly and writes anyway — the same protocol
        /// break, wearing a correct answer.
        ConflictsButWrites,
        /// Sequential puts arbitrate, concurrent ones do not. Nothing passes the
        /// first three checks and fails the race by accident, so this has to be
        /// built deliberately to show the race check carries its own weight.
        RacesUnarbitrated,
    }

    #[derive(Debug)]
    struct BrokenStore {
        inner: Arc<dyn ObjectStore>,
        how: Broken,
        seen: parking_lot::Mutex<std::collections::HashSet<String>>,
    }

    impl BrokenStore {
        fn boxed(how: Broken) -> Arc<dyn ObjectStore> {
            Arc::new(Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                how,
                seen: Default::default(),
            })
        }

        fn conflict(location: &Path) -> object_store::Error {
            object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "conflict".into(),
            }
        }
    }

    impl std::fmt::Display for BrokenStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "BrokenStore({:?})", self.how)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for BrokenStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            let conditional = matches!(opts.mode, PutMode::Create);
            let unconditional = PutOptions::default();
            match self.how {
                Broken::IgnoresPrecondition => {
                    self.inner.put_opts(location, payload, unconditional).await
                }
                Broken::ConflictsButWrites => {
                    let existed = self.inner.head(location).await.is_ok();
                    let res = self.inner.put_opts(location, payload, unconditional).await;
                    if existed && conditional {
                        res?;
                        return Err(Self::conflict(location));
                    }
                    res
                }
                Broken::RacesUnarbitrated => {
                    // Check-then-act with a real window between the two: callers
                    // that both check before either records will both win. The
                    // yield is what makes this a fake of a *remote* store — the
                    // gap in a real one is a network round trip, and without it
                    // `InMemory` completes each put before the next task is
                    // polled and the race never happens.
                    let seen = self.seen.lock().contains(location.as_ref());
                    if conditional && seen {
                        return Err(Self::conflict(location));
                    }
                    tokio::task::yield_now().await;
                    let res = self.inner.put_opts(location, payload, unconditional).await;
                    self.seen.lock().insert(location.to_string());
                    res
                }
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_store_that_ignores_the_precondition_is_rejected() {
        let store = BrokenStore::boxed(Broken::IgnoresPrecondition);
        assert!(matches!(
            verify_conditional_put(&store, &prefix()).await,
            Err(ConformanceError::OverwritesOnCreate)
        ));
    }

    #[tokio::test]
    async fn a_store_that_conflicts_but_writes_anyway_is_rejected() {
        let store = BrokenStore::boxed(Broken::ConflictsButWrites);
        assert!(matches!(
            verify_conditional_put(&store, &prefix()).await,
            Err(ConformanceError::ConflictButOverwrote)
        ));
    }

    /// The check that justifies racing at all: this store passes the sequential
    /// checks and fails only under concurrency.
    ///
    /// Multi-threaded deliberately. On a current-thread runtime the racers only
    /// interleave at await points, so a store that does not await inside its put
    /// is serialized by the runtime and the race cannot be observed at all —
    /// which would make this test pass for the wrong reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_store_that_does_not_arbitrate_races_is_rejected() {
        let store = BrokenStore::boxed(Broken::RacesUnarbitrated);
        match verify_conditional_put(&store, &prefix()).await {
            Err(ConformanceError::ConcurrentWinners { winners, .. }) => {
                assert!(winners > 1, "expected several winners, got {winners}");
            }
            other => panic!("expected a concurrency failure, got {other:?}"),
        }
    }
}
