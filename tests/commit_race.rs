//! Two leaders, one catalog: what happens when both try to commit the same
//! sequence.
//!
//! This is the half of the split-brain question that is *ours*. It assumes the
//! store provides put-if-absent and asks whether the flush loop does the right
//! thing with the answer — `storage::conformance` asks the other half, whether
//! the store provides it at all.
//!
//! `LocalFileSystem` is a perfectly good arbiter for this: `PutMode::Create` on
//! it is `create_new`, which really is atomic. The reason it does not settle the
//! S3 question is that it says nothing about a *remote* store's semantics, not
//! that it is a weak primitive locally.
//!
//! What must hold when a worker loses:
//!
//! - the winner's snapshot is the one that is stored, and no sequence is
//!   committed twice;
//! - the loser must **not** mark its folded run committed, because a run the
//!   catalog never saw cannot be the base of a delta — the next tick has to
//!   merge into a self-contained run instead;
//! - and the loser must not lose ground it has consumed beyond the winner's
//!   watermark.
//!
//! Every one of those is about the loser. The winner's path is the one that gets
//! exercised constantly; the loser's runs on the rarest day.
//!
//! **The workers here consume the same offsets**, because a watermark is a
//! high-water mark over one shared ordered log and only means anything under
//! that assumption. Dropping segments at *another* worker's watermark — which
//! `tick` does, and which is how followers reclaim memory — is sound only
//! because everything at or below it is in that worker's committed data file. An
//! earlier version of this test gave the two workers disjoint offset ranges,
//! which no shared log can produce, and made that drop look like data loss.

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{CommitOutcome, DataFileMeta, Flusher, RunSet, SnapshotCatalog, SnapshotMeta};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

fn edge(n: u64, scope: Option<u32>) -> EdgeEvent {
    EdgeEvent {
        src: n * 2,
        dst: n * 2 + 1,
        visibility: match scope {
            None => Visibility::Global,
            Some(s) => Visibility::Scoped([s].into_iter().collect()),
        },
        event_time_ms: 1_700_000_000_000,
        props: None,
    }
}

fn meta(sequence: u64, committer: &str, watermark: u64) -> SnapshotMeta {
    SnapshotMeta {
        sequence,
        committed_at_ms: 0,
        watermark,
        position: blaze::core::StreamPosition::single(watermark),
        stream: None,
        data_files: vec![DataFileMeta {
            path: format!("data/part-{sequence}-{committer}.parquet"),
            rows: 1,
            bytes: 1,
            min_offset: 0,
            max_offset: watermark,
            first_position: Default::default(),
            last_position: Default::default(),
        }],
        puffin_path: format!("puffin/dsu-{sequence}-{committer}.puffin"),
        committer: committer.to_string(),
        base_sequence: sequence,
        delta_chain_len: 0,
        format_version: 0,
        runs: Vec::new(),
    }
}

/// A worker: its own forest, buffer and local run cache, sharing the store and
/// catalog with every other. That is exactly the split-brain shape — the thing
/// they contend over is the catalog, and nothing else is shared.
struct Worker {
    flusher: Arc<Flusher>,
    forest: Arc<ScopedForest>,
    buffer: Arc<EdgeBuffer>,
    _cache: tempfile::TempDir,
}

impl Worker {
    fn new(
        id: &str,
        store: Arc<dyn ObjectStore>,
        catalog: Arc<SnapshotCatalog>,
        prefix: StorePath,
    ) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        Self {
            flusher: Arc::new(Flusher {
                forest: forest.clone(),
                buffer: buffer.clone(),
                store,
                catalog,
                // Both believe they are the leader. That is the premise.
                elector: Arc::new(StaticElector(true)),
                table_prefix: prefix,
                worker_id: id.into(),
                stream: None,
                base_dir: Some(cache.path().to_path_buf()),
                follower_fold_after_links: u64::MAX,
                max_delta_layers: 24,
                tier_fanout: 4,
                write: Default::default(),
                inline_merges: true,
                layers: parking_lot::Mutex::new(None),
                pending_merge: parking_lot::Mutex::new(None),
            }),
            forest,
            buffer,
            _cache: cache,
        }
    }

    fn ingest(&self, range: std::ops::Range<u64>, scope: Option<u32>) {
        for n in range {
            let e = edge(n, scope);
            self.forest.apply(&e);
            self.buffer.append(0, n, &e);
        }
    }
}

fn harness(
    workers: &[&str],
) -> (
    Arc<dyn ObjectStore>,
    Arc<SnapshotCatalog>,
    Vec<Worker>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let ws = workers
        .iter()
        .map(|id| Worker::new(id, store.clone(), catalog.clone(), prefix.clone()))
        .collect();
    (store, catalog, ws, dir)
}

/// The primitive, used the way the flush loop uses it: N callers, one sequence.
///
/// Multi-threaded on purpose. On a current-thread runtime these would interleave
/// only at await points, and a store that resolves the put without yielding would
/// be serialized by the runtime — the test would pass without ever racing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_one_committer_wins_a_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(SnapshotCatalog::new(store, StorePath::from("graph/edges")));

    for sequence in 1..=6u64 {
        let mut tasks = Vec::new();
        for racer in 0..8 {
            let catalog = catalog.clone();
            let id = format!("worker-{racer}");
            tasks.push(tokio::spawn(async move {
                let outcome = catalog.commit(&meta(sequence, &id, sequence * 100)).await;
                (id, outcome.unwrap())
            }));
        }
        let mut winners = Vec::new();
        for t in tasks {
            let (id, outcome) = t.await.unwrap();
            match outcome {
                CommitOutcome::Committed => winners.push(id),
                CommitOutcome::Conflict => {}
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "sequence {sequence} had {} committers: {winners:?}",
            winners.len()
        );
        // The winner must also be what the catalog actually holds — a store that
        // reported the conflict correctly but wrote anyway would pass the count
        // above and fail here.
        let stored = catalog.get(sequence).await.unwrap();
        assert_eq!(stored.committer, winners[0]);
        assert_eq!(stored.sequence, sequence);
    }
}

/// The loser's obligations, checked one at a time rather than inferred from the
/// catalog staying tidy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_loser_of_a_race_keeps_its_run_local_and_its_progress() {
    let (_store, catalog, workers, _dir) = harness(&["alpha", "beta"]);
    let (a, b) = (&workers[0], &workers[1]);

    // Same log, same offsets: both have consumed up to 59.
    a.ingest(0..60, Some(1));
    b.ingest(0..60, Some(1));

    let (ra, rb) = tokio::join!(a.flusher.tick(), b.flusher.tick());
    ra.unwrap();
    rb.unwrap();

    let latest = catalog.latest().await.unwrap().expect("someone committed");
    assert_eq!(latest.sequence, 1, "both workers targeted sequence 1");
    let (winner, loser) = if latest.committer == "alpha" {
        (a, b)
    } else {
        (b, a)
    };

    // The winner's stack is fully committed; the loser's is not, and that is what
    // forces its next fold to merge into a self-contained run rather than layer a
    // delta on a chain the catalog never saw.
    assert!(
        winner
            .flusher
            .layers
            .lock()
            .as_ref()
            .expect("winner folded")
            .fully_committed(),
        "the winner's run should be marked committed"
    );
    assert!(
        !loser
            .flusher
            .layers
            .lock()
            .as_ref()
            .expect("loser folded too")
            .fully_committed(),
        "the loser marked a run committed that the catalog never accepted"
    );

    // The loser's buffered segments are covered by the winner's watermark, so
    // dropping them is correct — those edges are in the winner's data file. What
    // it must not do is lose ground it has consumed *beyond* that.
    loser.ingest(60..100, Some(1));

    // Now let the loser commit alone. Its run set must describe the whole of its
    // own state, not a delta over the winner's: the winner's run is not in its
    // stack and never will be.
    loser.flusher.tick().await.unwrap();
    let next = catalog.latest().await.unwrap().unwrap();
    assert!(next.sequence > latest.sequence);
    assert_eq!(
        next.watermark, 99,
        "the loser must publish everything it has consumed, not restart from the \
         winner's watermark"
    );
    let RunSet::Runs(runs) = next.run_set().unwrap() else {
        panic!("expected a run set");
    };
    assert_eq!(
        runs.first().unwrap().min_sequence,
        1,
        "the loser's recovery commit must cover its state from the start: {runs:?}"
    );
    for pair in runs.windows(2) {
        assert!(pair[0].adjacent_to(&pair[1]), "gap in {runs:?}");
    }
}

/// Sustained contention. Any committed snapshot has to be loadable and internally
/// valid — at *every* sequence, not just the newest — because a cold start after
/// a split brain reads whichever one it finds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_leaders_never_publish_an_invalid_snapshot() {
    let (store, catalog, workers, _dir) = harness(&["alpha", "beta", "gamma"]);

    for round in 0..6u64 {
        for (i, w) in workers.iter().enumerate() {
            let base = round * 1_000 + i as u64 * 100;
            w.ingest(base..base + 30, Some(1 + i as u32));
        }
        let mut ticks = Vec::new();
        for w in &workers {
            let f = w.flusher.clone();
            ticks.push(tokio::spawn(async move { f.tick().await }));
        }
        for t in ticks {
            // A tick may fail outright under contention; what it must not do is
            // publish something invalid.
            let _ = t.await.unwrap();
        }
    }

    let newest = catalog
        .latest()
        .await
        .unwrap()
        .expect("something committed");
    assert!(newest.sequence >= 3, "only got to {}", newest.sequence);

    let mut watermark = 0u64;
    for sequence in 1..=newest.sequence {
        let meta = catalog
            .get(sequence)
            .await
            .unwrap_or_else(|e| panic!("sequence {sequence} missing from a dense catalog: {e}"));
        assert_eq!(meta.sequence, sequence);

        // Every snapshot's run set validates, and every run it names exists.
        match meta.run_set().unwrap_or_else(|e| {
            panic!(
                "snapshot {sequence} (by {}) has an invalid run set: {e}",
                meta.committer
            )
        }) {
            RunSet::Runs(runs) => {
                for pair in runs.windows(2) {
                    assert!(
                        pair[0].adjacent_to(&pair[1]),
                        "gap in snapshot {sequence}: {runs:?}"
                    );
                }
                assert_eq!(runs.last().unwrap().max_sequence, sequence);
                for run in &runs {
                    store
                        .head(&StorePath::from(run.path.clone()))
                        .await
                        .unwrap_or_else(|e| {
                            panic!("snapshot {sequence} names a missing run {}: {e}", run.path)
                        });
                }
            }
            RunSet::SequencesOnly(_) => panic!("snapshot {sequence} lost the run-set format"),
        }

        assert!(
            meta.watermark >= watermark,
            "watermark went backwards at sequence {sequence}: {watermark} -> {}",
            meta.watermark
        );
        watermark = meta.watermark;
    }

    // And the newest is actually loadable into a forest that knows nothing else.
    let cold = ScopedForest::new();
    let recovered = blaze::storage::hydrate_from_catalog(&cold, &store, &catalog)
        .await
        .unwrap();
    assert_eq!(recovered, newest.watermark);
}
