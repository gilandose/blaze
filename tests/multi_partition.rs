//! A multi-partition stream, checkpointed and resumed.
//!
//! Stage 2 of [design 010](../docs/design/010-stream-position.md). The scalar
//! watermark could describe one ordered log; a `StreamPosition` describes
//! several, and the order on positions is **partial** — two can be mutually
//! incomparable. These tests exercise the places that matters:
//!
//! 1. A restart mid-stream, resuming every partition from where the last
//!    snapshot left it, reaches the state an uninterrupted run reaches — and
//!    commits the same number of rows, which is the assertion with teeth once
//!    the graph is dense enough to absorb a dropped edge.
//! 2. Redelivery below the committed position on *one* partition does not
//!    suppress live records on another.
//! 3. Partitions grow. A topic that gains a partition needs no metadata saying
//!    so: the new one is simply absent from the committed position, reads as 0,
//!    and consumes from its beginning.
//!
//! Consumption stays single-writer throughout. This is about checkpointing a
//! partitioned stream correctly, not about consuming it in parallel.

use blaze::core::{EdgeEvent, ScopedForest, StreamPosition, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::{ConsumerConfig, EdgeBuffer, LogSource, PartitionedLog, Pipeline, consume};
use blaze::storage::{Flusher, SnapshotCatalog};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NODES: u64 = 2_500;
const SCOPES: u32 = 6;
const PARTITIONS: u32 = 4;
/// Records per partition. Deliberately uneven so the partitions cannot all be
/// at the same offset, which is what makes an incomparable position possible.
const PER_PARTITION: [u64; 4] = [1_200, 800, 1_500, 600];

/// Edges are drawn from one global sequence and then dealt out to partitions, so
/// the union of all partitions is the same edge set however it is split.
fn event(n: u64) -> EdgeEvent {
    EdgeEvent {
        src: (n * 7919) % NODES,
        dst: (n * 104_729) % NODES,
        visibility: if n.is_multiple_of(3) {
            Visibility::Global
        } else {
            Visibility::Scoped([1 + (n % SCOPES as u64) as u32].into_iter().collect())
        },
        event_time_ms: 1_700_000_000_000 + n as i64,
        props: None,
    }
}

fn rows_for(n: u64) -> u64 {
    match event(n).visibility.normalize() {
        Visibility::Global => 1,
        Visibility::Scoped(s) => s.len() as u64,
    }
}

/// The global sequence number of record `offset` (one-based) on `partition`.
///
/// Interleaved rather than blocked, so no partition holds a contiguous run of
/// the underlying edge set — a resume that mixed partitions up would produce a
/// visibly different graph rather than an accidentally equivalent one.
fn seq_of(partition: u32, offset: u64) -> u64 {
    (offset - 1) * PARTITIONS as u64 + partition as u64
}

/// Write `partition-<n>.ndjson` files, each its own ordered log.
fn write_partitions(dir: &std::path::Path, counts: &[u64]) {
    for (partition, &count) in counts.iter().enumerate() {
        let path = dir.join(format!("partition-{partition}.ndjson"));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        for offset in 1..=count {
            let e = event(seq_of(partition as u32, offset));
            writeln!(f, "{}", serde_json::to_string(&e).unwrap()).unwrap();
        }
        f.flush().unwrap();
    }
}

/// Every record in the stream, as (partition, offset) pairs.
fn all_records(counts: &[u64]) -> Vec<(u32, u64)> {
    let mut out = Vec::new();
    for (p, &count) in counts.iter().enumerate() {
        for offset in 1..=count {
            out.push((p as u32, offset));
        }
    }
    out
}

fn expected_rows(counts: &[u64]) -> u64 {
    all_records(counts)
        .into_iter()
        .map(|(p, o)| rows_for(seq_of(p, o)))
        .sum()
}

struct Worker {
    forest: Arc<ScopedForest>,
    catalog: Arc<SnapshotCatalog>,
    flusher: Flusher,
    pipeline: Pipeline,
}

async fn boot(dir: &std::path::Path, cache: &std::path::Path) -> Worker {
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap());
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let forest = Arc::new(ScopedForest::new());
    blaze::storage::hydrate_from_catalog(&forest, &store, &catalog)
        .await
        .unwrap();
    // Resume every partition, not just partition 0 — the whole point.
    let start = catalog
        .latest()
        .await
        .unwrap()
        .map(|s| s.stream_position())
        .unwrap_or_default();
    let buffer = Arc::new(EdgeBuffer::new());

    Worker {
        pipeline: Pipeline::resuming(forest.clone(), buffer.clone(), start),
        flusher: Flusher {
            forest: forest.clone(),
            buffer: buffer.clone(),
            store: store.clone(),
            catalog: catalog.clone(),
            elector: Arc::new(StaticElector(true)),
            table_prefix: prefix.clone(),
            worker_id: "multi-partition".into(),
            stream: None,
            base_dir: Some(cache.to_path_buf()),
            follower_fold_after_links: u64::MAX,
            max_delta_layers: 8,
            tier_fanout: 4,
            write: Default::default(),
            inline_merges: true,
            layers: parking_lot::Mutex::new(None),
            pending_merge: parking_lot::Mutex::new(None),
        },
        forest,
        catalog,
    }
}

impl Worker {
    fn position(&self) -> StreamPosition {
        self.pipeline.stats.position()
    }

    /// Consume every partition to the end, resuming from `from`.
    fn drain(&self, logs: &std::path::Path, from: &StreamPosition) {
        let mut source = PartitionedLog::open_dir(logs, false).unwrap();
        source.seek(from).unwrap();
        consume(
            source,
            &self.pipeline,
            ConsumerConfig {
                batch: 64,
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    }

    fn answers(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity((SCOPES as usize + 1) * NODES as usize);
        for scope in [0u32].into_iter().chain(1..=SCOPES) {
            for node in 0..NODES {
                out.push(self.forest.scope_root(scope, node));
            }
        }
        out
    }

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

/// A restart mid-stream, with every partition at a different offset.
///
/// The committed position here is genuinely multi-dimensional: a round-robin
/// poll leaves the partitions at different places, and the snapshot has to
/// record all of them. Resuming from a scalar could not express where three of
/// the four partitions stood.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_resumes_every_partition_where_it_left_off() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let logs = dir.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    write_partitions(&logs, &PER_PARTITION);

    // Worker A: consume part of the stream, commit, then apply the rest and die
    // with it in memory only.
    let a = boot(dir.path(), cache.path()).await;
    {
        let mut source = PartitionedLog::open_dir(&logs, false).unwrap();
        assert_eq!(source.partitions(), PARTITIONS as usize);
        // A handful of round-robin polls, leaving each partition somewhere
        // different — precisely the shape a scalar cannot describe.
        for _ in 0..6 {
            let batch = source.poll(160).unwrap();
            a.pipeline.apply_batch(&batch).unwrap();
        }
    }
    let committed = a.position();
    assert!(
        committed.len() > 1 && committed.iter().map(|(_, o)| o).collect::<Vec<_>>().len() > 1,
        "expected several partitions in flight, got {committed}"
    );
    a.flusher.tick().await.unwrap();

    let snapshot = a.catalog.latest().await.unwrap().unwrap();
    assert_eq!(
        snapshot.stream_position(),
        committed,
        "the snapshot did not record where every partition stood"
    );
    assert_eq!(
        snapshot.watermark, 0,
        "a multi-partition position must not claim a scalar watermark"
    );

    a.drain(&logs, &a.position());
    let uninterrupted = a.answers();
    drop(a);

    // Worker B: a fresh process with nothing but the catalog.
    let cache_b = tempfile::tempdir().unwrap();
    let b = boot(dir.path(), cache_b.path()).await;
    assert_eq!(
        b.position(),
        committed,
        "hydration recovered a different position than was committed"
    );
    b.drain(&logs, &b.position());

    assert_eq!(
        b.answers(),
        uninterrupted,
        "the replacement worker reached a different state than the uninterrupted run"
    );

    // Exact, where connectivity is not: past percolation the graph absorbs a
    // dropped edge without changing any root, so a resume that skipped one
    // record on one partition would still compare equal above.
    b.flusher.tick().await.unwrap();
    assert_eq!(
        b.committed_rows().await,
        expected_rows(&PER_PARTITION),
        "committed rows do not match the stream exactly"
    );
    let final_position = b.catalog.latest().await.unwrap().unwrap().stream_position();
    for (p, &count) in PER_PARTITION.iter().enumerate() {
        assert_eq!(final_position.get(p as u32), count, "partition {p}");
    }
}

/// Redelivery is per-partition. A consumer group whose committed offsets lag
/// blaze's position hands back records already applied — but only on the
/// partitions it lagged on, and the live records on the others must still land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redelivery_on_one_partition_does_not_suppress_another() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let logs = dir.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    write_partitions(&logs, &PER_PARTITION);

    let w = boot(dir.path(), cache.path()).await;
    w.drain(&logs, &StreamPosition::new());
    w.flusher.tick().await.unwrap();
    let after_first_pass = w.committed_rows().await;
    assert_eq!(after_first_pass, expected_rows(&PER_PARTITION));

    // Rewind partitions 0 and 2 well behind where blaze committed, and leave 1
    // and 3 at the head. Everything redelivered is already durable.
    let lagging: StreamPosition = [
        (0, PER_PARTITION[0] / 2),
        (1, PER_PARTITION[1]),
        (2, PER_PARTITION[2] / 3),
        (3, PER_PARTITION[3]),
    ]
    .into_iter()
    .collect();
    w.drain(&logs, &lagging);
    w.flusher.tick().await.unwrap();

    assert_eq!(
        w.committed_rows().await,
        after_first_pass,
        "redelivered records were written a second time"
    );
    // And the position did not move backwards on the rewound partitions.
    let position = w.position();
    for (p, &count) in PER_PARTITION.iter().enumerate() {
        assert_eq!(position.get(p as u32), count, "partition {p} rewound");
    }
}

/// Kafka partitions can be added and never removed. A topic that grows needs no
/// metadata describing the topology: the new partition is simply absent from the
/// committed position, which reads as 0, which is where it genuinely starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_topic_that_grows_a_partition_needs_no_migration() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let logs = dir.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();

    // Two partitions to begin with.
    let small = [400u64, 300];
    write_partitions(&logs, &small);
    let w = boot(dir.path(), cache.path()).await;
    w.drain(&logs, &StreamPosition::new());
    w.flusher.tick().await.unwrap();
    let committed = w.catalog.latest().await.unwrap().unwrap().stream_position();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed.get(2), 0, "partition 2 does not exist yet");
    let rows_before = w.committed_rows().await;

    // The topic gains partitions 2 and 3. Nothing rewrites the old snapshot.
    for partition in 2u32..4 {
        let path = logs.join(format!("partition-{partition}.ndjson"));
        let mut f = std::fs::File::create(&path).unwrap();
        for offset in 1..=250u64 {
            let e = event(seq_of(partition, offset));
            writeln!(f, "{}", serde_json::to_string(&e).unwrap()).unwrap();
        }
        f.flush().unwrap();
    }

    // Resuming from the two-partition position consumes the new ones from their
    // beginning, because absent reads as zero.
    let source = PartitionedLog::open_dir(&logs, false).unwrap();
    assert_eq!(source.partitions(), 4);
    w.drain(&logs, &committed);
    w.flusher.tick().await.unwrap();

    let grown = w.catalog.latest().await.unwrap().unwrap().stream_position();
    assert_eq!(grown.len(), 4);
    assert_eq!(grown.get(0), small[0], "an existing partition moved");
    assert_eq!(grown.get(1), small[1]);
    assert_eq!(
        grown.get(2),
        250,
        "the new partition was not consumed whole"
    );
    assert_eq!(grown.get(3), 250);

    let added: u64 = (2u32..4)
        .flat_map(|p| (1..=250u64).map(move |o| rows_for(seq_of(p, o))))
        .sum();
    assert_eq!(
        w.committed_rows().await,
        rows_before + added,
        "the grown topic did not contribute exactly its new records"
    );
}
