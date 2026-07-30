//! The micro-batch flush loop.
//!
//! Every worker seals its active Arrow buffer on each tick and prunes
//! segments the committed watermark has passed. Only the elected leader
//! writes: Parquet data file + Puffin DSU sidecar first, then the atomic
//! catalog commit that makes both visible. A failed commit (lost race)
//! leaves sealed segments in place for the next tick — at-least-once flush,
//! exactly-once visibility.

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::core::{RoutingBase, ScopedForest};
use crate::ha::LeaderElector;
use crate::ingest::EdgeBuffer;
use crate::storage::base::PuffinBase;
use crate::storage::catalog::{CommitOutcome, DataFileMeta, SnapshotCatalog, SnapshotMeta};
use crate::storage::{codec, parquet_io, puffin};

pub struct Flusher {
    pub forest: Arc<ScopedForest>,
    pub buffer: Arc<EdgeBuffer>,
    pub store: Arc<dyn ObjectStore>,
    pub catalog: Arc<SnapshotCatalog>,
    pub elector: Arc<dyn LeaderElector>,
    pub table_prefix: Path,
    pub worker_id: String,
    /// Where to write folded routing bases. `None` = RAM mode: the memtable
    /// *is* the state, so there is nothing to fold it into.
    pub base_dir: Option<std::path::PathBuf>,
    /// Fold once the memtable holds at least this many links. Size-triggered
    /// rather than every tick because a fold rewrites the whole base: the
    /// trigger is what trades write amplification against resident heap.
    pub fold_after_links: u64,
}

/// Links a memtable may hold before a fold is due (~50 MB of DashMap).
pub const DEFAULT_FOLD_AFTER_LINKS: u64 = 1_000_000;

/// Warn when a fold stalls ingest for longer than this.
const FOLD_STALL_WARN_MS: u64 = 5_000;

/// Write `bytes` to `path` via a temp file and rename, so a torn write can
/// never be observed — or mapped — as a routing base.
fn write_atomically(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension("partial");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl Flusher {
    /// Run the flush loop until the task is aborted.
    pub async fn run(self: Arc<Self>, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately; skip it so a fresh worker doesn't
        // commit an empty snapshot at startup.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(e) = self.tick().await {
                warn!(error = %e, "flush tick failed; will retry next interval");
            }
        }
    }

    /// One flush cycle. Public for tests.
    pub async fn tick(&self) -> anyhow::Result<()> {
        self.buffer.seal_active();

        // Observe the committed watermark and drop covered segments. This is
        // how followers garbage-collect the data the leader persisted for
        // everyone.
        let latest = self.catalog.latest().await?;
        if let Some(latest) = &latest {
            let dropped = self.buffer.drop_committed(latest.watermark);
            if dropped > 0 {
                info!(
                    watermark = latest.watermark,
                    dropped, "pruned segments covered by committed snapshot"
                );
            }
        }

        let sequence = latest.as_ref().map(|s| s.sequence + 1).unwrap_or(1);

        if !self.elector.is_leader() {
            // Followers serve queries from the same structures the leader
            // does, so their memtable grows at exactly the same rate. Folding
            // only on the leader would relocate the leak, not fix it.
            self.fold_if_due(sequence, latest.as_ref().map(|s| s.watermark).unwrap_or(0))?;
            return Ok(());
        }

        let segments = self.buffer.sealed_segments();
        if segments.is_empty() {
            return Ok(());
        }
        let watermark = segments.iter().map(|s| s.max_offset).max().unwrap_or(0);
        let min_offset = segments.iter().map(|s| s.min_offset).min().unwrap_or(0);

        // 1. Data file.
        let (parquet_bytes, rows) = parquet_io::segments_to_parquet(&segments)?;
        let data_path = self.table_prefix.clone().join("data").join(format!(
            "part-{sequence:012}-{}.parquet",
            uuid::Uuid::new_v4()
        ));
        let parquet_len = parquet_bytes.len() as u64;
        self.store
            .put(&data_path, PutPayload::from(parquet_bytes))
            .await?;

        // 2. Puffin DSU sidecar. Streamed out of the forest rather than
        // snapshotted into the heap first: compaction holds the union lock, so
        // an O(state) allocation here would stall ingest for its duration.
        // When a fold is due these are the same bytes the worker now serves
        // from, so the leader never re-downloads what it just produced.
        let puffin_bytes = match self.fold_if_due(sequence, watermark)? {
            Some(bytes) => bytes,
            None => puffin::write(
                &codec::compact_to_blobs(&self.forest, sequence),
                puffin_metadata(watermark),
            ),
        };
        let puffin_path = self
            .table_prefix
            .clone()
            .join("puffin")
            .join(format!("dsu-{sequence:012}.puffin"));
        self.store
            .put(&puffin_path, PutPayload::from(puffin_bytes))
            .await?;

        // 3. Atomic commit.
        let meta = SnapshotMeta {
            sequence,
            committed_at_ms: now_ms(),
            watermark,
            data_files: vec![DataFileMeta {
                path: data_path.to_string(),
                rows,
                bytes: parquet_len,
                min_offset,
                max_offset: watermark,
            }],
            puffin_path: puffin_path.to_string(),
            committer: self.worker_id.clone(),
        };
        match self.catalog.commit(&meta).await? {
            CommitOutcome::Committed => {
                self.buffer.drop_committed(watermark);
                info!(sequence, rows, watermark, "committed micro-batch snapshot");
            }
            CommitOutcome::Conflict => {
                // Someone else committed this sequence (leadership handoff
                // race). Our data/puffin files are orphans — harmless, like
                // uncommitted Iceberg files — and sealed segments stay for
                // re-flush after re-observing the catalog.
                warn!(sequence, "commit conflict; deferring to next tick");
            }
        }
        Ok(())
    }

    /// Fold the memtable into a fresh local base, if one is due.
    ///
    /// This is the step that makes the disk tier's RAM bound hold for longer
    /// than one flush interval. Compaction alone only *reads* the forest, so
    /// without a fold the memtable accumulates for the life of the worker and
    /// the process ends up heap-resident again — a cold start's 51 MB is not a
    /// steady state, it is an initial condition.
    ///
    /// Returns the Puffin bytes when it folded, so the leader can commit the
    /// very file it is now serving from. `Ok(None)` means no fold was due (or
    /// the worker is in RAM mode).
    ///
    /// The fold precedes the catalog commit, so a lost commit race leaves a
    /// local base whose bytes were never committed. That is harmless: it
    /// encodes this worker's own composed state, which is correct regardless of
    /// who won, and restart recovery goes through the catalog either way.
    fn fold_if_due(&self, sequence: u64, watermark: u64) -> anyhow::Result<Option<bytes::Bytes>> {
        let Some(dir) = &self.base_dir else {
            return Ok(None);
        };
        let links = self.forest.memtable_links();
        if links < self.fold_after_links {
            return Ok(None);
        }
        std::fs::create_dir_all(dir)?;
        // One stable path per worker. Renaming over a mapped file is safe: the
        // old mapping keeps the old inode alive until the last query using it
        // drops, and the space is reclaimed then — so nothing has to track or
        // garbage-collect previous folds.
        let path = dir.join(format!("routing-fold-{}.puffin", self.worker_id));

        let started = std::time::Instant::now();
        let mut writer = codec::BlobWriter::new(sequence);
        let bytes = self.forest.compact_and_fold(&mut writer, |w| {
            let bytes = puffin::write(&w.finish(), puffin_metadata(watermark));
            write_atomically(&path, &bytes)?;
            let base: Arc<dyn RoutingBase> = Arc::new(PuffinBase::open(&path)?);
            anyhow::Ok((base, bytes))
        })?;

        let stalled_ms = started.elapsed().as_millis() as u64;
        let stats = self.forest.stats();
        info!(
            sequence,
            folded_links = links,
            memtable_links_now = stats.global_links,
            base_shared_pairs = stats.base_shared_pairs,
            base_overlay_pairs = stats.base_overlay_pairs,
            base_mb = stats.base_mapped_bytes / (1024 * 1024),
            stalled_ms,
            "folded memtable into a fresh routing base"
        );
        // A fold rewrites the whole base, so the stall grows with total state
        // rather than with what was folded. Say so out loud: silently pausing
        // ingest for tens of seconds is the kind of thing an operator should
        // hear about before it becomes a backlog.
        if stalled_ms > FOLD_STALL_WARN_MS {
            warn!(
                stalled_ms,
                folded_links = links,
                base_mb = stats.base_mapped_bytes / (1024 * 1024),
                "fold stalled ingest for a long time; raise --fold-after-links, \
                 or land delta snapshots (docs/design/001) so folds stop being O(state)"
            );
        }
        Ok(Some(bytes))
    }
}

fn puffin_metadata(watermark: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("created-by".to_string(), "blaze".to_string()),
        ("watermark".to_string(), watermark.to_string()),
    ])
}

/// Open the latest committed routing snapshot as an mmap'd base on local
/// disk, returning the base and the watermark to resume offsets from.
///
/// The Puffin object is cached under `data_dir` (a read-through cache of
/// object storage — invariant I4 unchanged: losing the disk costs a
/// re-download, never data). Startup cost is O(number of blobs), not
/// O(pairs), so a multi-gigabyte base is serving queries in milliseconds
/// instead of after a full hydration.
pub async fn open_base_from_catalog(
    store: &Arc<dyn ObjectStore>,
    catalog: &SnapshotCatalog,
    data_dir: &std::path::Path,
) -> anyhow::Result<Option<(Arc<PuffinBase>, u64)>> {
    let Some(latest) = catalog.latest().await? else {
        return Ok(None);
    };
    std::fs::create_dir_all(data_dir)?;
    // Snapshot artifacts are immutable, so a cached file for this sequence is
    // always the right bytes; name it by sequence to keep that obvious.
    let local = data_dir.join(format!("routing-base-{:012}.puffin", latest.sequence));
    if !local.exists() {
        let bytes = store
            .get(&Path::from(latest.puffin_path.clone()))
            .await?
            .bytes()
            .await?;
        // Write to a temp path and rename so a torn download can never be
        // mapped as a base.
        let tmp = local.with_extension("puffin.partial");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &local)?;
        info!(
            sequence = latest.sequence,
            bytes = bytes.len(),
            path = %local.display(),
            "cached routing base from object storage"
        );
    }
    let base = Arc::new(PuffinBase::open(&local)?);
    let stats = base.stats();
    info!(
        sequence = latest.sequence,
        watermark = latest.watermark,
        shared_pairs = stats.shared_pairs,
        overlay_pairs = stats.overlay_pairs,
        mapped_mb = stats.mapped_bytes / (1024 * 1024),
        "opened mmap routing base"
    );
    Ok(Some((base, latest.watermark)))
}

/// Load the latest committed snapshot and hydrate in-memory state from its
/// Puffin sidecar. Returns the watermark to resume offsets from.
pub async fn hydrate_from_catalog(
    forest: &ScopedForest,
    store: &Arc<dyn ObjectStore>,
    catalog: &SnapshotCatalog,
) -> anyhow::Result<u64> {
    let Some(latest) = catalog.latest().await? else {
        return Ok(0);
    };
    let bytes = store
        .get(&Path::from(latest.puffin_path.clone()))
        .await?
        .bytes()
        .await?;
    let blobs = puffin::read(&bytes)?;
    let snapshot = codec::blobs_to_snapshot(&blobs)?;
    forest.hydrate(&snapshot);
    info!(
        sequence = latest.sequence,
        watermark = latest.watermark,
        "hydrated DSU state from puffin snapshot"
    );
    Ok(latest.watermark)
}
