//! Log-backed ingest, and the property it exists to provide.
//!
//! The pipeline used to mint its own offsets. That is fine until a second worker
//! reads the same events, because the committed watermark is *measured in
//! offsets*: a snapshot saying "committed through 3000" means one thing to the
//! worker whose counter produced it and something else to any other worker. The
//! recovery story — hydrate to the watermark, resume after it — silently depends
//! on a numbering only one process can reproduce.
//!
//! With the offset coming from the log, the numbering is the log's, and these
//! tests assert what that buys:
//!
//! 1. A worker that dies with events applied but not committed, and is replaced
//!    by a **fresh process that has only the catalog**, reaches exactly the state
//!    an uninterrupted run reaches.
//! 2. The replay that recovery performs does not double-write. At-least-once
//!    delivery means the tail arrives twice; the rows must land once.

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::{ConsumerConfig, EdgeBuffer, FileLog, LogSource, Pipeline, consume};
use blaze::storage::{Flusher, SnapshotCatalog};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NODES: u64 = 3_000;
const SCOPES: u32 = 8;
const RECORDS: u64 = 6_000;
/// Where the "crash" happens: applied through here, committed at `COMMITTED`.
const COMMITTED: u64 = 2_500;

/// Deterministic, and deliberately mixed: globals exercise the shared tier and
/// the registry notification, scoped edges the overlays.
fn event(n: u64) -> EdgeEvent {
    EdgeEvent {
        src: (n * 7919) % NODES,
        dst: (n * 104_729) % NODES,
        visibility: if n.is_multiple_of(3) {
            Visibility::Global
        } else if n.is_multiple_of(7) {
            Visibility::Scoped(
                [
                    1 + (n % SCOPES as u64) as u32,
                    1 + ((n / 3) % SCOPES as u64) as u32,
                ]
                .into_iter()
                .collect(),
            )
        } else {
            Visibility::Scoped([1 + (n % SCOPES as u64) as u32].into_iter().collect())
        },
        event_time_ms: 1_700_000_000_000 + n as i64,
        props: None,
    }
}

/// Rows a record contributes to Parquet: one per visible scope, globals as one.
fn rows(n: u64) -> u64 {
    match event(n).visibility.normalize() {
        Visibility::Global => 1,
        Visibility::Scoped(s) => s.len() as u64,
    }
}

fn write_log(path: &std::path::Path, records: std::ops::RangeInclusive<u64>) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for n in records {
        // Offset is the line number, so record n is written on line n.
        writeln!(f, "{}", serde_json::to_string(&event(n)).unwrap()).unwrap();
    }
    f.flush().unwrap();
}

struct Worker {
    forest: Arc<ScopedForest>,
    catalog: Arc<SnapshotCatalog>,
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    flusher: Flusher,
    pipeline: Pipeline,
}

/// Stand a worker up against an existing warehouse, hydrating whatever is
/// committed there — which is what a replacement process does on failover.
async fn boot(dir: &std::path::Path, cache: &std::path::Path) -> Worker {
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap());
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let forest = Arc::new(ScopedForest::new());
    let watermark = blaze::storage::hydrate_from_catalog(&forest, &store, &catalog)
        .await
        .unwrap();
    let buffer = Arc::new(EdgeBuffer::new());

    Worker {
        pipeline: Pipeline::new(forest.clone(), buffer.clone(), watermark),
        flusher: Flusher {
            forest: forest.clone(),
            buffer: buffer.clone(),
            store: store.clone(),
            catalog: catalog.clone(),
            elector: Arc::new(StaticElector(true)),
            table_prefix: prefix.clone(),
            worker_id: "log-ingest".into(),
            stream: None,
            base_dir: Some(cache.to_path_buf()),
            fold_after_links: u64::MAX,
            max_delta_layers: 8,
            tier_fanout: 4,
            write: Default::default(),
            inline_merges: true,
            layers: parking_lot::Mutex::new(None),
            pending_merge: parking_lot::Mutex::new(None),
        },
        forest,
        catalog,
        store,
        prefix,
    }
}

impl Worker {
    fn watermark(&self) -> u64 {
        self.pipeline.stats.last_offset()
    }

    /// Consume the log from the worker's own watermark until it drains.
    fn consume_from_watermark(&self, log_path: &std::path::Path) -> u64 {
        self.consume_from(log_path, self.watermark())
    }

    /// Consume from an arbitrary position, which is what a real consumer group
    /// does: the broker's committed offset lags blaze's snapshot watermark, so
    /// resuming from it redelivers records blaze has already durably applied.
    fn consume_from(&self, log_path: &std::path::Path, from: u64) -> u64 {
        let mut log = FileLog::open(log_path, false).unwrap();
        log.seek_after(from).unwrap();
        consume(
            log,
            &self.pipeline,
            ConsumerConfig {
                batch: 512,
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap()
    }

    /// Consume only up to `through`, leaving the rest of the log unread — a
    /// worker that has not caught up yet.
    fn consume_through(&self, log_path: &std::path::Path, through: u64) -> u64 {
        let mut log = FileLog::open(log_path, false).unwrap();
        log.seek_after(self.watermark()).unwrap();
        let mut last = self.watermark();
        while last < through {
            // Cap the last poll so the worker stops on `through` exactly rather
            // than on the next batch boundary past it.
            let batch = log.poll(256.min((through - last) as usize)).unwrap();
            if batch.is_empty() {
                break;
            }
            last = self.pipeline.apply_batch(&batch).unwrap();
        }
        last
    }

    /// Every answer this worker gives, over all scopes and nodes.
    fn answers(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity((SCOPES as usize + 1) * NODES as usize);
        for scope in [0u32].into_iter().chain(1..=SCOPES) {
            for node in 0..NODES {
                out.push(self.forest.scope_root(scope, node));
            }
        }
        out
    }

    /// Rows across every data file the catalog names, over the whole history.
    async fn committed_rows(&self) -> u64 {
        let Some(latest) = self.catalog.latest().await.unwrap() else {
            return 0;
        };
        let mut total = 0;
        for seq in 1..=latest.sequence {
            let snap = self.catalog.get(seq).await.unwrap();
            total += snap.data_files.iter().map(|f| f.rows).sum::<u64>();
        }
        total
    }
}

/// A fresh worker that only ever sees the committed state, then the log.
///
/// This is the assertion the old minted-offset numbering could not support: the
/// replacement process has no memory of the dead one, only its snapshots, and it
/// has to work out from the watermark alone which records it still owes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_worker_resumes_exactly_where_the_watermark_left_off() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let log = dir.path().join("edges.ndjson");
    write_log(&log, 1..=RECORDS);

    // Worker A: commits through COMMITTED, then applies the rest and dies with
    // those events in memory only.
    let a = boot(dir.path(), cache.path()).await;
    a.consume_through(&log, COMMITTED);
    a.flusher.tick().await.unwrap();
    assert_eq!(a.watermark(), COMMITTED);
    let applied = a.consume_from_watermark(&log);
    assert_eq!(
        applied, RECORDS,
        "worker A did not reach the end of the log"
    );
    let uninterrupted = a.answers();
    drop(a); // crash: nothing after COMMITTED was ever committed

    // Worker B: a fresh process with nothing but the warehouse.
    let cache_b = tempfile::tempdir().unwrap();
    let b = boot(dir.path(), cache_b.path()).await;
    assert_eq!(
        b.watermark(),
        COMMITTED,
        "hydration recovered a different position than was committed"
    );
    b.consume_from_watermark(&log);
    assert_eq!(b.watermark(), RECORDS);

    assert_eq!(
        b.answers(),
        uninterrupted,
        "the replacement worker reached a different state than the uninterrupted run"
    );

    // Answers alone are a weaker check than they look: past the percolation
    // threshold the graph is dense enough that dropping a single edge usually
    // changes no root at all, so a resume that skipped one record would still
    // compare equal. The row count is exact — every record contributes a known
    // number of rows, so this catches a record missed or applied twice even when
    // connectivity cannot tell.
    b.flusher.tick().await.unwrap();
    assert_eq!(
        b.committed_rows().await,
        (1..=RECORDS).map(rows).sum::<u64>(),
        "the resumed worker committed a different number of rows than the log holds"
    );
}

/// At-least-once delivery, absorbed exactly once.
///
/// Seeking to blaze's own watermark would make this trivially true, so the
/// worker here resumes from a position *behind* it — which is the realistic
/// case, not a contrived one. A consumer group commits offsets to the broker on
/// its own cadence, and that cadence has nothing to do with when blaze commits a
/// snapshot; on restart the broker hands back everything after *its* offset,
/// including records blaze already has durably. The rows still have to land
/// once. Otherwise every restart inflates the table by the size of the lag, and
/// the duplicates are indistinguishable from real edges afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn records_redelivered_below_the_watermark_are_not_written_twice() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let log = dir.path().join("edges.ndjson");
    write_log(&log, 1..=RECORDS);
    let expected: u64 = (1..=RECORDS).map(rows).sum();

    let a = boot(dir.path(), cache.path()).await;
    a.consume_through(&log, COMMITTED);
    a.flusher.tick().await.unwrap();
    a.consume_from_watermark(&log); // applied, never committed
    drop(a);

    // The replacement's consumer group is 1000 records behind what blaze
    // committed, so the first 1000 records it is handed are already durable.
    let cache_b = tempfile::tempdir().unwrap();
    let b = boot(dir.path(), cache_b.path()).await;
    assert_eq!(b.watermark(), COMMITTED);
    b.consume_from(&log, COMMITTED - 1_000);
    b.flusher.tick().await.unwrap();
    assert_eq!(b.watermark(), RECORDS);

    assert_eq!(
        b.committed_rows().await,
        expected,
        "committed row count does not match the log exactly"
    );

    // The extreme of the same case: replay the entire log from the start into a
    // fully caught-up worker. Every record is a duplicate; nothing is written.
    let before = b.committed_rows().await;
    b.consume_from(&log, 0);
    b.flusher.tick().await.unwrap();
    assert_eq!(
        b.committed_rows().await,
        before,
        "a full replay re-wrote rows"
    );
    assert_eq!(b.watermark(), RECORDS);
}

/// Follow mode is the streaming shape: the worker catches up, then keeps pace
/// with a producer appending to the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_following_worker_picks_up_records_appended_after_it_caught_up() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let log = dir.path().join("edges.ndjson");
    write_log(&log, 1..=1_000);

    let w = boot(dir.path(), cache.path()).await;
    let mut source = FileLog::open(&log, true).unwrap();
    source.seek_after(0).unwrap();

    let batch = source.poll(10_000).unwrap();
    assert_eq!(w.pipeline.apply_batch(&batch).unwrap(), 1_000);
    assert!(
        source.poll(10_000).unwrap().is_empty(),
        "a caught-up follower should see an empty poll, not the end of the log"
    );
    assert!(source.is_live());

    // The producer writes more; the same open source picks it up in place.
    write_log(&log, 1_001..=1_500);
    let more = source.poll(10_000).unwrap();
    assert_eq!(more.len(), 500);
    assert_eq!(more[0].offset, 1_001);
    assert_eq!(w.pipeline.apply_batch(&more).unwrap(), 1_500);

    w.flusher.tick().await.unwrap();
    let cold = ScopedForest::new();
    let recovered = blaze::storage::hydrate_from_catalog(&cold, &w.store, &w.catalog)
        .await
        .unwrap();
    assert_eq!(recovered, 1_500);
    assert_eq!(
        (0..NODES)
            .map(|n| cold.scope_root(1, n))
            .collect::<Vec<_>>(),
        (0..NODES)
            .map(|n| w.forest.scope_root(1, n))
            .collect::<Vec<_>>()
    );
    let _ = &w.prefix;
}
