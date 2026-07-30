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

        if !self.elector.is_leader() {
            return Ok(());
        }

        let segments = self.buffer.sealed_segments();
        if segments.is_empty() {
            return Ok(());
        }
        let sequence = latest.as_ref().map(|s| s.sequence + 1).unwrap_or(1);
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

        // 2. Puffin DSU sidecar.
        let snapshot = self.forest.snapshot();
        let blobs = codec::snapshot_to_blobs(&snapshot, sequence);
        let puffin_bytes = puffin::write(
            &blobs,
            BTreeMap::from([
                ("created-by".to_string(), "blaze".to_string()),
                ("watermark".to_string(), watermark.to_string()),
            ]),
        );
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
