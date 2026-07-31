//! What a crash in the middle of a flush tick leaves behind.
//!
//! A tick does three things that can each fail independently: it uploads a
//! Parquet data file, it folds the memtable into a run and uploads that, then it
//! commits a metadata JSON naming both. Only the third is atomic. So there are
//! two windows where a worker can die having done real work that nothing
//! references, and the question these tests answer is what the *next* successful
//! commit says about the state.
//!
//! The dangerous version is a **span gap**. A committed run set fixes resolution
//! order by sequence span, and `SnapshotMeta::run_set` requires those spans to be
//! contiguous and to end at the snapshot's own sequence. A fold that happened but
//! was never committed has consumed a sequence and drained the memtable into a
//! run only this process knows about — so a naive next commit could describe a
//! set with a hole in it, and a cold start would then resolve through the wrong
//! order or reject the snapshot outright.
//!
//! This was reasoned about when the run-set format landed but never executed.
//! Nothing here needs an object store with real semantics: the failures are about
//! *sequencing*, so a wrapper that fails a chosen put reproduces them exactly and
//! deterministically. (The one open question that does need real S3 is the
//! put-if-absent commit race, which `LocalFileSystem` answers with `create_new`
//! and therefore does not test at all.)

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, RunSet, SnapshotCatalog};
use futures::stream::BoxStream;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An object store that fails puts whose path contains a chosen substring.
///
/// Cutting on a substring rather than a call count is deliberate: the tick's put
/// order is an implementation detail that tiering already changed once, and a
/// test pinned to "fail the second put" would silently start cutting somewhere
/// else. Paths are stable — `data/part-*`, `puffin/dsu-*`, `metadata/snap-*`.
#[derive(Debug)]
struct CutStore {
    inner: Arc<dyn ObjectStore>,
    cut: parking_lot::Mutex<Option<String>>,
    puts: parking_lot::Mutex<Vec<String>>,
    failures: AtomicUsize,
}

impl CutStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            cut: parking_lot::Mutex::new(None),
            puts: parking_lot::Mutex::new(Vec::new()),
            failures: AtomicUsize::new(0),
        }
    }

    /// Fail every subsequent put whose path contains `needle`.
    fn cut_on(&self, needle: &str) {
        *self.cut.lock() = Some(needle.to_string());
    }

    fn heal(&self) {
        *self.cut.lock() = None;
    }

    fn failures(&self) -> usize {
        self.failures.load(Ordering::Relaxed)
    }

    /// Paths put successfully, in order.
    fn puts(&self) -> Vec<String> {
        self.puts.lock().clone()
    }

    fn should_fail(&self, path: &StorePath) -> bool {
        self.cut
            .lock()
            .as_ref()
            .is_some_and(|needle| path.as_ref().contains(needle.as_str()))
    }
}

impl std::fmt::Display for CutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CutStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CutStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.should_fail(location) {
            self.failures.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "CutStore",
                source: format!("cut: {location}").into(),
            });
        }
        let res = self.inner.put_opts(location, payload, opts).await;
        if res.is_ok() {
            self.puts.lock().push(location.to_string());
        }
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<StorePath>>,
    ) -> BoxStream<'static, OsResult<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Harness {
    store: Arc<CutStore>,
    object_store: Arc<dyn ObjectStore>,
    catalog: Arc<SnapshotCatalog>,
    forest: Arc<ScopedForest>,
    buffer: Arc<EdgeBuffer>,
    flusher: Flusher,
    prefix: StorePath,
    offset: u64,
    _dir: tempfile::TempDir,
    _cache: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let store = Arc::new(CutStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let prefix = StorePath::from("graph/edges");
        let catalog = Arc::new(SnapshotCatalog::new(object_store.clone(), prefix.clone()));
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        let flusher = Flusher {
            forest: forest.clone(),
            buffer: buffer.clone(),
            store: object_store.clone(),
            catalog: catalog.clone(),
            elector: Arc::new(StaticElector(true)),
            table_prefix: prefix.clone(),
            worker_id: "cut-test".into(),
            base_dir: Some(cache.path().to_path_buf()),
            fold_after_links: u64::MAX,
            max_delta_layers: 24,
            tier_fanout: 4,
            write: Default::default(),
            inline_merges: true,
            layers: parking_lot::Mutex::new(None),
            pending_merge: parking_lot::Mutex::new(None),
        };
        Self {
            store,
            object_store,
            catalog,
            forest,
            buffer,
            flusher,
            prefix,
            offset: 0,
            _dir: dir,
            _cache: cache,
        }
    }

    /// Apply a deterministic batch of edges and buffer them, the way ingest does.
    fn ingest(&mut self, batch: u64) {
        for i in 0..40u64 {
            let n = self.offset + i;
            let e = EdgeEvent {
                src: n * 2,
                dst: n * 2 + 1 + (batch % 3),
                visibility: if i % 3 == 0 {
                    Visibility::Global
                } else {
                    Visibility::Scoped([1 + (i % 5) as u32].into_iter().collect())
                },
                event_time_ms: 1_700_000_000_000,
                props: None,
            };
            self.forest.apply(&e);
            self.buffer.append(self.offset + i, &e);
        }
        self.offset += 40;
    }

    async fn latest(&self) -> Option<blaze::storage::SnapshotMeta> {
        self.catalog.latest().await.unwrap()
    }
}

/// Every committed run set must validate: spans contiguous, ending exactly at the
/// snapshot's own sequence. That is the property a mid-tick failure threatens,
/// because a fold consumes a sequence whether or not the commit that would have
/// described it ever lands.
fn assert_contiguous(meta: &blaze::storage::SnapshotMeta) {
    match meta.run_set().unwrap_or_else(|e| {
        panic!("snapshot {} has an invalid run set: {e}", meta.sequence);
    }) {
        RunSet::Runs(runs) => {
            for pair in runs.windows(2) {
                assert!(
                    pair[0].adjacent_to(&pair[1]),
                    "gap between spans {:?} and {:?} in snapshot {}",
                    (pair[0].min_sequence, pair[0].max_sequence),
                    (pair[1].min_sequence, pair[1].max_sequence),
                    meta.sequence
                );
            }
            assert_eq!(
                runs.last().unwrap().max_sequence,
                meta.sequence,
                "newest run must end at the snapshot's sequence"
            );
        }
        RunSet::SequencesOnly(_) => panic!("expected a run set, got the legacy chain format"),
    }
}

/// A run object that never uploaded must not be committed. Obvious, and the
/// cheapest thing to get wrong, since the fold that produced it already succeeded
/// and already drained the memtable.
#[tokio::test]
async fn a_failed_run_upload_commits_nothing() {
    let mut h = Harness::new();
    h.ingest(0);
    h.flusher.tick().await.unwrap();
    let before = h.latest().await.expect("first tick commits");
    assert_contiguous(&before);

    h.ingest(1);
    h.store.cut_on("puffin/dsu-");
    let err = h.flusher.tick().await;
    assert!(err.is_err(), "a failed run upload must fail the tick");
    assert_eq!(h.store.failures(), 1);

    let after = h.latest().await.unwrap();
    assert_eq!(
        after.sequence, before.sequence,
        "catalog advanced despite the run never uploading"
    );
    assert_eq!(after.watermark, before.watermark);
    assert!(
        !h.store
            .puts()
            .iter()
            .any(|p| p.contains(&format!("snap-{:012}", before.sequence + 1))),
        "a snapshot was written for a sequence whose run does not exist"
    );
}

/// The window that actually matters: the run uploaded, the commit did not. The
/// tick has consumed a sequence, drained the memtable into a run, and left an
/// orphan object behind — and the next commit has to describe a run set that
/// still validates.
#[tokio::test]
async fn the_run_set_stays_contiguous_across_a_failed_commit() {
    let mut h = Harness::new();
    h.ingest(0);
    h.flusher.tick().await.unwrap();
    let before = h.latest().await.unwrap();

    h.ingest(1);
    h.store.cut_on("metadata/");
    assert!(h.flusher.tick().await.is_err(), "the commit must fail");
    assert_eq!(
        h.latest().await.unwrap().sequence,
        before.sequence,
        "catalog must not advance on a failed commit"
    );
    // The run *did* upload, so an orphan object is expected. Harmless, like an
    // uncommitted Iceberg file — but it proves the window was real.
    assert!(
        h.store
            .puts()
            .iter()
            .any(|p| p.contains(&format!("dsu-{:012}", before.sequence + 1))),
        "expected an orphan run object from the cut tick"
    );

    h.store.heal();
    h.ingest(2);
    h.flusher.tick().await.unwrap();
    let after = h.latest().await.unwrap();
    assert!(after.sequence > before.sequence);
    assert_contiguous(&after);
}

/// Two failures in a row, at both cut points, then recovery. A single failure
/// could be absorbed by luck; the invariant has to hold when the stack is carrying
/// more than one uncommitted fold.
#[tokio::test]
async fn the_run_set_survives_repeated_mid_tick_failures() {
    let mut h = Harness::new();
    h.ingest(0);
    h.flusher.tick().await.unwrap();

    for (round, cut) in ["metadata/", "puffin/dsu-", "metadata/"].iter().enumerate() {
        h.ingest(round as u64 + 1);
        h.store.cut_on(cut);
        assert!(
            h.flusher.tick().await.is_err(),
            "round {round} cut on {cut} should fail the tick"
        );
        h.store.heal();
    }

    h.ingest(9);
    h.flusher.tick().await.unwrap();
    let meta = h.latest().await.unwrap();
    assert_contiguous(&meta);
}

/// The reason any of this matters: a worker that cold-starts from the catalog
/// after a mid-tick failure must reconstruct the same routing answers as the
/// worker that survived it.
///
/// Asserted over every node in every scope rather than a sample, because a span
/// gap does not corrupt state uniformly — it reorders resolution for exactly the
/// nodes whose parents were written in the runs that swapped places.
#[tokio::test]
async fn a_cold_start_after_a_mid_tick_failure_matches_the_survivor() {
    let mut h = Harness::new();
    for round in 0..3u64 {
        h.ingest(round);
        h.flusher.tick().await.unwrap();
    }

    h.ingest(3);
    h.store.cut_on("metadata/");
    assert!(h.flusher.tick().await.is_err());
    h.store.heal();

    // Two clean ticks: the first has to merge rather than layer a delta, because
    // the stack is carrying a run the catalog never saw.
    for round in 4..6u64 {
        h.ingest(round);
        h.flusher.tick().await.unwrap();
    }
    let meta = h.latest().await.unwrap();
    assert_contiguous(&meta);

    // Cold start: a fresh forest hydrated from nothing but the committed run set.
    let cold = ScopedForest::new();
    let catalog = SnapshotCatalog::new(h.object_store.clone(), h.prefix.clone());
    blaze::storage::hydrate_from_catalog(&cold, &h.object_store, &catalog)
        .await
        .unwrap();

    let nodes = h.offset * 2 + 4;
    let mut checked = 0usize;
    for node in 0..nodes {
        for scope in [0u32, 1, 2, 3, 4, 5, 77] {
            assert_eq!(
                cold.scope_root(scope, node),
                h.forest.scope_root(scope, node),
                "scope {scope} root({node}) diverged after a mid-tick failure"
            );
            checked += 1;
        }
    }
    assert!(checked > 1_000, "only checked {checked} answers");
}

/// The watermark is the recovery-point promise: everything at or below it is in
/// committed artifacts. A failed tick must not move it, and no tick may ever move
/// it backwards.
#[tokio::test]
async fn the_watermark_never_advances_on_a_failed_tick() {
    let mut h = Harness::new();
    let mut high = 0u64;
    for round in 0..6u64 {
        h.ingest(round);
        // Cut every other tick, alternating which half of the commit is lost.
        let cut = match round % 3 {
            1 => Some("puffin/dsu-"),
            2 => Some("metadata/"),
            _ => None,
        };
        if let Some(needle) = cut {
            h.store.cut_on(needle);
        }
        let before = h.latest().await.map(|m| m.watermark).unwrap_or(0);
        let outcome = h.flusher.tick().await;
        h.store.heal();
        let after = h.latest().await.map(|m| m.watermark).unwrap_or(0);

        assert!(after >= high, "watermark went backwards: {high} -> {after}");
        high = after;
        if outcome.is_err() {
            assert_eq!(
                after, before,
                "watermark advanced on a tick that failed at round {round}"
            );
        }
    }

    // Everything the watermark claims must be reachable from the committed run
    // set alone — which is exactly what a restart would have.
    let meta = h.latest().await.unwrap();
    assert_contiguous(&meta);
    let cold = ScopedForest::new();
    let catalog = SnapshotCatalog::new(h.object_store.clone(), h.prefix.clone());
    let watermark = blaze::storage::hydrate_from_catalog(&cold, &h.object_store, &catalog)
        .await
        .unwrap();
    assert_eq!(watermark, meta.watermark);
    // And the recovered state is not empty, which would satisfy every assertion
    // above vacuously.
    assert!(cold.scope_root(1, 0) > 0 || cold.scope_root(0, 2) > 0);
}

/// *Why* there is no gap, pinned so a refactor cannot quietly remove it.
///
/// A failed commit leaves the stack holding a run the catalog never saw, so
/// `LocalLayers::fully_committed` is false and the next fold is forced to merge
/// into a self-contained run rather than layer another delta on a chain nobody
/// else can reconstruct. The orphaned sequence is therefore *absorbed* — it
/// appears inside an older run's span, not as a run of its own.
///
/// Without that, `run_metas` would refuse to describe the stack (an older run
/// with no remote path), the commit would fall back to the legacy chain format,
/// and a cold start would lose the tiering order. The tests above would catch it,
/// but only by accident; this says it out loud.
#[tokio::test]
async fn the_orphaned_sequence_is_absorbed_rather_than_skipped() {
    let mut h = Harness::new();
    for round in 0..3u64 {
        h.ingest(round);
        h.flusher.tick().await.unwrap();
    }
    let before = h.latest().await.unwrap().sequence;

    h.ingest(3);
    h.store.cut_on("metadata/");
    assert!(h.flusher.tick().await.is_err());
    h.store.heal();
    let orphaned = before + 1;

    h.ingest(4);
    h.flusher.tick().await.unwrap();
    let meta = h.latest().await.unwrap();
    assert_contiguous(&meta);

    let RunSet::Runs(runs) = meta.run_set().unwrap() else {
        unreachable!("assert_contiguous already rejected the chain format")
    };
    let covering = runs
        .iter()
        .find(|r| r.min_sequence <= orphaned && orphaned <= r.max_sequence)
        .unwrap_or_else(|| {
            panic!("sequence {orphaned} is in no run's span: {runs:?}");
        });
    assert!(
        covering.min_sequence < orphaned,
        "the orphaned fold should have been merged into an older run, not kept \
         as a run of its own: {covering:?}"
    );
    assert!(
        runs.iter()
            .all(|r| r.max_sequence != orphaned || r.min_sequence < orphaned),
        "a run standing alone at the orphaned sequence means the merge did not happen"
    );
}
