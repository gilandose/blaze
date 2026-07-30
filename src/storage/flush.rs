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
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::core::{RoutingBase, ScopedForest};
use crate::ha::LeaderElector;
use crate::ingest::EdgeBuffer;
use crate::storage::base::PuffinBase;
use crate::storage::catalog::{CommitOutcome, DataFileMeta, SnapshotCatalog, SnapshotMeta};
use crate::storage::layered::LayeredBase;
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
    /// Fold once the memtable holds at least this many links. Only governs
    /// workers that are *not* committing — a leader folds every tick, since it
    /// has to produce a layer to commit anyway.
    pub fold_after_links: u64,
    /// Compact once the base carries this many delta layers. Bounds both the
    /// per-lookup layer scan and cold-start chain length.
    pub max_delta_layers: usize,
    /// Local layer stack this worker serves from. Only the flush loop writes
    /// it, so a plain mutex around the bookkeeping is enough; the mapped state
    /// itself is shared immutably with the forest.
    pub layers: Mutex<Option<LocalLayers>>,
}

/// The mapped layer files this worker is serving from, plus the catalog
/// bookkeeping needed to know whether a delta may be committed on top of them.
#[derive(Debug)]
pub struct LocalLayers {
    pub base: Arc<LayeredBase>,
    /// Files backing `base`, oldest first — kept so compaction can unlink what
    /// it subsumes.
    pub paths: Vec<PathBuf>,
    /// Catalog sequence of the base layer.
    pub base_sequence: u64,
    /// Catalog sequence the newest layer was committed as, or `None` when the
    /// newest layer exists only on this worker's disk.
    ///
    /// This is what makes it safe for a worker to commit a delta at all. A
    /// follower folds locally without committing, so its stack can run ahead of
    /// the catalog; if it then became leader and committed a delta, the
    /// committed chain would be missing those layers and a cold start would
    /// reconstruct incomplete topology. A worker whose newest layer is
    /// local-only must therefore compact and commit a full base instead.
    pub committed_through: Option<u64>,
}

/// Links a memtable may hold before a non-committing worker folds (~50 MB of
/// DashMap).
pub const DEFAULT_FOLD_AFTER_LINKS: u64 = 1_000_000;

/// Delta layers to carry before compacting.
///
/// This is a read/write amplification dial, and both sides are measured. A
/// delta fold costs ~400 ms and 12.5 MB where rewriting the base costs ~3.1 s
/// and 125 MB, so more layers means cheaper flushes. But a lookup that misses
/// has to probe every layer: ~0.26 µs each on top of a ~0.8 µs base probe. At
/// 24 layers that is ~7 µs — comfortably inside the sub-millisecond SLO — and a
/// compaction roughly every 24 minutes at a 60s flush interval.
///
/// Raising this much above ~100 starts to trade the SLO away; the fix that
/// removes the trade is a per-layer membership filter, so a miss costs a RAM
/// probe instead of a binary search (see docs/design/001).
pub const DEFAULT_MAX_DELTA_LAYERS: usize = 24;

/// Warn when a fold stalls ingest for longer than this.
const FOLD_STALL_WARN_MS: u64 = 5_000;

/// What a fold produced, which decides how the commit describes it.
enum Layer {
    /// A full base: self-contained, resets the chain.
    Base,
    /// A delta over the base at `base_sequence`.
    Delta { base_sequence: u64 },
}

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
            // only on the leader would relocate the leak, not fix it. These
            // folds are local-only, which is why `committed_through` exists.
            self.fold(
                sequence,
                latest.as_ref().map(|s| s.watermark).unwrap_or(0),
                false,
            )?;
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

        // 2. Puffin DSU sidecar. The leader always folds, because the layer it
        // folds out is the very thing it commits — and serves from afterwards,
        // so it never re-downloads its own snapshot. Normally that layer is a
        // small delta; it becomes a full base when the chain has grown long
        // enough to compact. In RAM mode there is no base to layer over, so
        // fall back to writing the whole map.
        let (puffin_bytes, layer) = match self.fold(sequence, watermark, true)? {
            Some(folded) => folded,
            None => (
                puffin::write(
                    &codec::compact_to_blobs(&self.forest, sequence),
                    puffin_metadata(watermark),
                ),
                Layer::Base,
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
        let base_sequence = match layer {
            Layer::Base => sequence,
            Layer::Delta { base_sequence } => base_sequence,
        };
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
            base_sequence,
            delta_chain_len: sequence - base_sequence,
        };
        match self.catalog.commit(&meta).await? {
            CommitOutcome::Committed => {
                self.buffer.drop_committed(watermark);
                // The layer we just folded is now part of the committed chain,
                // so a delta may be layered on it next tick.
                if let Some(local) = self.layers.lock().as_mut() {
                    local.committed_through = Some(sequence);
                }
                info!(
                    sequence,
                    rows,
                    watermark,
                    base_sequence,
                    delta_chain_len = sequence - base_sequence,
                    "committed micro-batch snapshot"
                );
            }
            CommitOutcome::Conflict => {
                // Someone else committed this sequence (leadership handoff
                // race). Our data/puffin files are orphans — harmless, like
                // uncommitted Iceberg files — and sealed segments stay for
                // re-flush after re-observing the catalog. `committed_through`
                // stays as it was, so the layer we just folded is local-only
                // and the next tick will compact rather than commit a delta
                // whose chain the catalog never saw.
                warn!(sequence, "commit conflict; deferring to next tick");
            }
        }
        Ok(())
    }

    /// Drain the memtable into a new local layer.
    ///
    /// This is the step that makes the disk tier's RAM bound hold for longer
    /// than one flush interval: compaction alone only *reads* the forest, so
    /// without a fold the memtable accumulates for the life of the worker.
    ///
    /// Normally the layer is a **delta** — only what the memtable itself
    /// contributes — so the fold costs O(memtable) in time and bytes and the
    /// base is appended to rather than rewritten. It becomes a full base when
    /// the chain is long enough to compact, when nothing has been mapped yet, or
    /// when this worker's newest layer is local-only (see
    /// [`LocalLayers::committed_through`]).
    ///
    /// `force` = the caller is a leader that needs a layer to commit; otherwise
    /// the memtable-size trigger decides. Returns the Puffin bytes and how to
    /// describe them, so the leader commits exactly the file it now serves
    /// from. `Ok(None)` means nothing was folded — no fold due, or RAM mode,
    /// where the memtable *is* the state and there is nothing to layer over.
    ///
    /// A fold precedes the catalog commit, so a lost race leaves a local layer
    /// that was never committed. That is harmless — it encodes this worker's
    /// own composed state, which is correct regardless of who won — and the
    /// `committed_through` check keeps it from being built on.
    fn fold(
        &self,
        sequence: u64,
        watermark: u64,
        force: bool,
    ) -> anyhow::Result<Option<(bytes::Bytes, Layer)>> {
        let Some(dir) = &self.base_dir else {
            return Ok(None);
        };
        let links = self.forest.memtable_links();
        if !force && links < self.fold_after_links {
            return Ok(None);
        }
        std::fs::create_dir_all(dir)?;

        // Decide before touching the forest, so the lock hold is exactly the
        // work and nothing else.
        let current = {
            let held = self.layers.lock();
            held.as_ref().map(|l| {
                (
                    l.base.clone(),
                    l.paths.clone(),
                    l.base_sequence,
                    l.committed_through,
                )
            })
        };
        // Captured now: compaction unlinks exactly the layers it read.
        let superseded: Vec<PathBuf> = current
            .as_ref()
            .map(|(_, paths, _, _)| paths.clone())
            .unwrap_or_default();
        let compact = match &current {
            None => true,
            Some((base, _, _, committed_through)) => {
                base.delta_count() + 1 >= self.max_delta_layers || committed_through.is_none()
            }
        };

        // Layer files accumulate, so each needs its own name — unlike a single
        // rewritten base, the older ones are still mapped and still consulted.
        let path = dir.join(format!(
            "routing-{}-{sequence:012}-{}.puffin",
            self.worker_id,
            if compact { "base" } else { "delta" }
        ));
        let started = std::time::Instant::now();
        let mut writer = codec::BlobWriter::new(sequence);

        let (bytes, next) = if compact {
            // Full base: stream the whole composed state (base ⊕ memtable).
            let out = self.forest.compact_and_fold(&mut writer, |w| {
                let bytes = puffin::write(&w.finish(), puffin_metadata(watermark));
                write_atomically(&path, &bytes)?;
                let mapped = Arc::new(PuffinBase::open(&path)?);
                let layered = Arc::new(LayeredBase::new(mapped));
                anyhow::Ok((layered.clone() as Arc<dyn RoutingBase>, (bytes, layered)))
            })?;
            (
                out.0,
                LocalLayers {
                    base: out.1,
                    paths: vec![path.clone()],
                    base_sequence: sequence,
                    committed_through: None,
                },
            )
        } else {
            let (base, paths, base_sequence, _) = current.expect("checked above");
            // Delta: stream only the memtable's own pairs and append them as a
            // new layer over the mappings already open.
            let out = self.forest.fold_delta(&mut writer, |w, _| {
                let bytes = puffin::write(&w.finish(), puffin_metadata(watermark));
                write_atomically(&path, &bytes)?;
                let mapped = Arc::new(PuffinBase::open(&path)?);
                let layered = Arc::new(base.pushed(mapped));
                anyhow::Ok((layered.clone() as Arc<dyn RoutingBase>, (bytes, layered)))
            })?;
            let mut paths = paths;
            paths.push(path.clone());
            (
                out.0,
                LocalLayers {
                    base: out.1,
                    paths,
                    base_sequence,
                    committed_through: None,
                },
            )
        };

        // Compaction subsumed the layers it read, so unlink them. Unlinking a
        // mapped file is safe: the mapping keeps the inode alive until the last
        // query using it drops, and the space is reclaimed then. A delta added
        // to the stack supersedes nothing.
        let superseded = if compact { superseded } else { Vec::new() };
        let layer = if compact {
            Layer::Base
        } else {
            Layer::Delta {
                base_sequence: next.base_sequence,
            }
        };
        let stalled_ms = started.elapsed().as_millis() as u64;
        let delta_layers = next.base.delta_count();
        *self.layers.lock() = Some(next);
        for old in superseded {
            if let Err(e) = std::fs::remove_file(&old) {
                warn!(path = %old.display(), error = %e, "could not unlink superseded layer");
            }
        }

        let stats = self.forest.stats();
        info!(
            sequence,
            folded_links = links,
            kind = if compact { "base" } else { "delta" },
            delta_layers,
            bytes = bytes.len(),
            memtable_links_now = stats.global_links,
            base_mb = stats.base_mapped_bytes / (1024 * 1024),
            stalled_ms,
            "folded memtable into a new routing layer"
        );
        // A delta fold is O(memtable); a compaction rewrites everything. Only
        // the latter should ever be slow, and silently pausing ingest for tens
        // of seconds is the kind of thing an operator should hear about before
        // it becomes a backlog.
        if stalled_ms > FOLD_STALL_WARN_MS {
            warn!(
                stalled_ms,
                folded_links = links,
                kind = if compact { "base" } else { "delta" },
                base_mb = stats.base_mapped_bytes / (1024 * 1024),
                "fold stalled ingest for a long time; raise --max-delta-layers to \
                 compact less often, or move compaction off the ingest path"
            );
        }
        Ok(Some((bytes, layer)))
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
) -> anyhow::Result<Option<(Arc<LayeredBase>, u64, LocalLayers)>> {
    let Some(latest) = catalog.latest().await? else {
        return Ok(None);
    };
    std::fs::create_dir_all(data_dir)?;

    // Committed routing state is the base plus every delta above it, so a cold
    // start maps the whole chain. `SnapshotMeta::chain` is inclusive and dense,
    // and `catalog.get` fails loudly on a hole rather than skipping it (I5) —
    // a skipped delta would serve stale topology as if it were current.
    let chain = latest.chain();
    let mut paths = Vec::new();
    let mut layers = Vec::new();
    for sequence in chain.clone() {
        let meta = if sequence == latest.sequence {
            latest.clone()
        } else {
            catalog.get(sequence).await?
        };
        let local = data_dir.join(format!("routing-layer-{sequence:012}.puffin"));
        if !local.exists() {
            let bytes = store
                .get(&Path::from(meta.puffin_path.clone()))
                .await?
                .bytes()
                .await?;
            // Write to a temp path and rename so a torn download can never be
            // mapped as a layer.
            let tmp = local.with_extension("puffin.partial");
            std::fs::write(&tmp, &bytes)?;
            std::fs::rename(&tmp, &local)?;
            info!(
                sequence,
                bytes = bytes.len(),
                path = %local.display(),
                "cached routing layer from object storage"
            );
        }
        layers.push(Arc::new(PuffinBase::open(&local)?));
        paths.push(local);
    }

    let base = Arc::new(LayeredBase::from_layers(layers)?);
    let stats = base.stats();
    info!(
        sequence = latest.sequence,
        base_sequence = *chain.start(),
        delta_layers = base.delta_count(),
        watermark = latest.watermark,
        shared_pairs = stats.shared_pairs,
        overlay_pairs = stats.overlay_pairs,
        mapped_mb = stats.mapped_bytes / (1024 * 1024),
        "opened mmap routing base"
    );
    let local = LocalLayers {
        base: base.clone(),
        paths,
        base_sequence: *chain.start(),
        // Every layer we just mapped came from the catalog, so a delta may be
        // layered on top immediately.
        committed_through: Some(latest.sequence),
    };
    Ok(Some((base, latest.watermark, local)))
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
    // Base first, then each delta in sequence order. Pairs are fully resolved
    // at their commit time and roots only ever decrease, so applying a later
    // pair for the same node simply overwrites with the newer, lower root —
    // last-writer-wins by sequence is exactly right here.
    let chain = latest.chain();
    for sequence in chain.clone() {
        let meta = if sequence == latest.sequence {
            latest.clone()
        } else {
            catalog.get(sequence).await?
        };
        let bytes = store
            .get(&Path::from(meta.puffin_path.clone()))
            .await?
            .bytes()
            .await?;
        let snapshot = codec::blobs_to_snapshot(&puffin::read(&bytes)?)?;
        forest.hydrate(&snapshot);
    }
    info!(
        sequence = latest.sequence,
        base_sequence = *chain.start(),
        layers = chain.clone().count(),
        watermark = latest.watermark,
        "hydrated DSU state from puffin base + delta chain"
    );
    Ok(latest.watermark)
}
