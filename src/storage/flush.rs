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
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::core::{RoutingBase, ScopedForest};
use crate::ha::LeaderElector;
use crate::ingest::EdgeBuffer;
use crate::storage::base::PuffinBase;
use crate::storage::catalog::{
    CommitOutcome, DataFileMeta, RunMeta, RunSet, SnapshotCatalog, SnapshotMeta,
};
use crate::storage::compact::compact_layers;
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
    /// One entry per layer in `base`, in the same order — oldest span first.
    pub runs: Vec<LocalRun>,
}

/// One run in this worker's local stack: the file it is mapped from, plus the
/// catalog identity it has, or will have once committed.
#[derive(Debug, Clone)]
pub struct LocalRun {
    /// Local file backing this run. Mapped now; unlinked once a merge subsumes
    /// it.
    pub path: PathBuf,
    /// Where the same bytes live in object storage. `None` while the run exists
    /// only on this worker's disk — see [`LocalLayers::fully_committed`].
    pub remote: Option<String>,
    /// Size tier; see [`RunMeta::level`].
    pub level: u8,
    /// Catalog sequences this run covers, inclusive.
    ///
    /// Provisional while `remote` is `None`. A follower folds whenever its
    /// memtable fills, whether or not the catalog advanced, so two of its
    /// local-only runs can claim the same span; `fully_committed` is what stops
    /// a provisional span from reaching the catalog.
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub pairs: u64,
    pub bytes: u64,
}

impl LocalRun {
    /// Describe a run from the mapped file itself, so the counts are what is
    /// actually on disk rather than what the writer believed it wrote.
    fn written(path: PathBuf, mapped: &PuffinBase, level: u8, span: RangeInclusive<u64>) -> Self {
        let stats = mapped.stats();
        Self {
            path,
            remote: None,
            level,
            min_sequence: *span.start(),
            max_sequence: *span.end(),
            pairs: stats.shared_pairs + stats.overlay_pairs,
            bytes: stats.mapped_bytes,
        }
    }

    /// A run this worker holds locally that the catalog already knows about.
    ///
    /// Sizes come from the mapped file rather than from `meta`, so they are right
    /// even for a pre-`runs` snapshot, which recorded none.
    fn committed(meta: &RunMeta, path: PathBuf, mapped: &PuffinBase) -> Self {
        let stats = mapped.stats();
        Self {
            path,
            remote: Some(meta.path.clone()),
            level: meta.level,
            min_sequence: meta.min_sequence,
            max_sequence: meta.max_sequence,
            pairs: stats.shared_pairs + stats.overlay_pairs,
            bytes: stats.mapped_bytes,
        }
    }

    fn meta(&self, path: String) -> RunMeta {
        RunMeta {
            path,
            level: self.level,
            min_sequence: self.min_sequence,
            max_sequence: self.max_sequence,
            pairs: self.pairs,
            bytes: self.bytes,
        }
    }
}

impl LocalLayers {
    /// What the compatibility `SnapshotMeta::base_sequence` field reports.
    ///
    /// Note this is the oldest run's `max_sequence`, not its `min_sequence`: the
    /// old field names the sequence the base was *committed at*, while a span
    /// says which sequences a run *covers*. The two agree because a run's span
    /// always ends at the sequence it was committed at — a fold covers only its
    /// own sequence, and a merge extends its span forward to the commit that
    /// publishes it. Reading `min_sequence` here would report a merged base as
    /// living at the first sequence it subsumed, which the old format takes to
    /// mean the chain above it was never merged away.
    pub fn base_sequence(&self) -> u64 {
        self.runs.first().map(|r| r.max_sequence).unwrap_or(0)
    }

    /// Whether every run in this stack is in the catalog.
    ///
    /// This is what makes it safe for a worker to commit a delta at all. A
    /// follower folds locally without committing, so its stack can run ahead of
    /// the catalog; if it then became leader and committed a delta, the
    /// committed run set would be missing those runs and a cold start would
    /// reconstruct incomplete topology. A worker holding a local-only run must
    /// therefore merge and commit a self-contained run instead.
    pub fn fully_committed(&self) -> bool {
        self.runs.iter().all(|r| r.remote.is_some())
    }

    /// Files backing the stack, oldest first — kept so a merge can unlink what
    /// it subsumes.
    pub fn paths(&self) -> Vec<PathBuf> {
        self.runs.iter().map(|r| r.path.clone()).collect()
    }

    /// How the catalog should describe this stack, with `newest_remote` supplying
    /// the object-store path of a run that has been uploaded but not yet
    /// committed.
    ///
    /// `None` when an *older* run is still local-only, which is a stack that
    /// cannot be described as a committed run set at all. `fold` forces a
    /// self-contained merge in exactly that case, so this is unreachable from the
    /// delta path; returning `None` rather than asserting means a bug there
    /// degrades to the base+chain format instead of taking down the flush loop.
    pub fn run_metas(&self, newest_remote: &str) -> Option<Vec<RunMeta>> {
        let (newest, older) = self.runs.split_last()?;
        let mut out = Vec::with_capacity(self.runs.len());
        for run in older {
            out.push(run.meta(run.remote.clone()?));
        }
        let remote = newest
            .remote
            .clone()
            .unwrap_or_else(|| newest_remote.to_string());
        out.push(newest.meta(remote));
        Some(out)
    }

    /// Record that the newest run is now in the catalog at `remote`.
    pub fn mark_committed(&mut self, remote: String) {
        if let Some(newest) = self.runs.last_mut() {
            newest.remote = Some(remote);
        }
    }
}

/// Level for a run merged from `inputs`.
///
/// Flat compaction merges *everything*, so there is no cohort being promoted and
/// this only counts merge generations. A tiered policy assigns the level from the
/// cohort it chose to merge instead (design 006), at which point this goes away;
/// until then nothing branches on `level`, which is what makes saturating at 255
/// harmless rather than a silently wrong answer.
fn merged_level(inputs: &[LocalRun]) -> u8 {
    match inputs.iter().map(|r| r.level).max() {
        // Nothing subsumed: the first fold on a fresh deployment is a level-0 run
        // like any other, it just happens to be self-contained. Calling it a
        // promoted merge output would leave a tiered policy with a lone L1 that
        // never participates in the first L0 cohort.
        None => 0,
        Some(highest) => highest.saturating_add(1),
    }
}

/// Links a memtable may hold before a non-committing worker folds (~50 MB of
/// DashMap).
pub const DEFAULT_FOLD_AFTER_LINKS: u64 = 1_000_000;

/// Delta layers to carry before compacting.
///
/// This is a read *and write* amplification dial, because every path resolves
/// through the stack: queries, `apply`, folds (O(memtable × layers)) and
/// compaction (O(pairs × layers)).
///
/// Measured on identical state (`examples/layer_depth.rs`), with the per-layer
/// blocked filters in place:
///
/// | layers | ingest/s | vs 1 layer | lookup µs | vs 1 layer |
/// |---|---|---|---|---|
/// | 1 | 216,963 | 1.00x | 1.01 | 1.00x |
/// | 8 | 109,487 | 1.98x | 3.10 | 3.06x |
/// | 16 | 65,850 | 3.29x | 4.90 | 4.84x |
///
/// The filters roughly halved the depth penalty — before them, 8 layers cost
/// 3.97x ingest and 6.10x lookups. Usefully, **16 layers now cost less than 8
/// used to**, and since total compaction work over a backfill goes as
/// `N²/(2·F·L)`, doubling viable depth halves the merge work.
///
/// The default of 24 suits *serving*: at ~50 links/s the ingest cost is
/// irrelevant and single-digit-µs lookups are far inside the SLO. It is still
/// the wrong choice while **backfilling**, where ingest rate is the objective —
/// though the trade is now much flatter than it was.
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

        // Before anything else: if the layer chain has grown long, merge it
        // from storage and publish the result as its own snapshot. That takes no
        // union lock, so ingest runs throughout; this tick then ends and the
        // next one commits its delta on top of the fresh base.
        if self
            .maybe_compact_chain(sequence, latest.as_ref().map(|s| s.watermark).unwrap_or(0))
            .await?
        {
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
        // The run set names the file we just uploaded, but does *not* mark it
        // committed on the local stack: that only happens if the commit below
        // wins, or a lost race would leave a local-only run looking committed and
        // the next tick would layer a delta on a chain the catalog never saw.
        let runs = match self.layers.lock().as_ref() {
            // RAM mode: the memtable is the state, so there is no run structure
            // to describe.
            None => Vec::new(),
            Some(local) => local.run_metas(puffin_path.as_ref()).unwrap_or_else(|| {
                warn!(
                    sequence,
                    "stack holds a local-only run below the newest; describing \
                         this commit in the base+chain format only"
                );
                Vec::new()
            }),
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
            runs,
        };
        match self.catalog.commit(&meta).await? {
            CommitOutcome::Committed => {
                self.buffer.drop_committed(watermark);
                // The layer we just folded is now part of the committed run set,
                // so a delta may be layered on it next tick.
                if let Some(local) = self.layers.lock().as_mut() {
                    local.mark_committed(puffin_path.to_string());
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
                // re-flush after re-observing the catalog. The run we just
                // folded keeps `remote: None`, so it stays local-only and the
                // next tick will merge rather than commit a delta whose chain
                // the catalog never saw.
                warn!(sequence, "commit conflict; deferring to next tick");
            }
        }
        Ok(())
    }

    /// Merge the layer chain into a single base **from storage**, and publish
    /// it. Returns whether it did.
    ///
    /// This is the compaction that matters, and what makes it different from
    /// `ScopedForest::compact_and_fold` is what it does *not* touch: the inputs
    /// are immutable committed layer files that are already mapped, so no union
    /// lock is taken, the forest is never read, and ingest runs at full rate for
    /// however many minutes the merge takes. At 2B links that is the difference
    /// between a 32-minute ingest stall and none.
    ///
    /// It is published as its own snapshot carrying the **previous** watermark
    /// and no data files, because that is exactly what it covers: the merged
    /// layers, not the memtable, which `swap_base` deliberately preserves. A
    /// commit claiming this tick's watermark would tell a cold start that merges
    /// still sitting in the memtable were durable, and replay would skip them.
    async fn maybe_compact_chain(&self, sequence: u64, watermark: u64) -> anyhow::Result<bool> {
        let Some(dir) = &self.base_dir else {
            return Ok(false);
        };
        let Some((stack, inputs)) = ({
            let held = self.layers.lock();
            held.as_ref().map(|l| (l.base.clone(), l.runs.clone()))
        }) else {
            return Ok(false);
        };
        if stack.delta_count() + 1 < self.max_delta_layers {
            return Ok(false);
        }

        let started = std::time::Instant::now();
        let path = dir.join(format!(
            "routing-{}-{sequence:012}-base.puffin",
            self.worker_id
        ));
        // The merge is minutes of synchronous CPU and disk work. Left inline in
        // an async fn it would occupy a tokio worker thread for the duration,
        // costing the runtime a whole worker (the scheduler steals around it, so
        // the API degrades rather than stalls, but it should not be there).
        //
        // Note this does *not* yet let folds proceed during a compaction — the
        // tick is still sequential, so the memtable grows for the merge's
        // duration. Fixing that needs the compaction to run detached, which needs
        // the catalog to describe a *set of runs* rather than one base plus a
        // contiguous chain: a base merged from layers 0..k cannot be expressed as
        // a base at a later sequence without discarding the deltas committed
        // meanwhile. That format change is design 006's anyway, so the two land
        // together.
        let merge_stack = stack.clone();
        let merge_path = path.clone();
        let meta = puffin_metadata(watermark);
        let (bytes, cstats) = tokio::task::spawn_blocking(move || {
            let (blobs, cstats) = compact_layers(&merge_stack, sequence);
            let bytes = puffin::write(&blobs, meta);
            write_atomically(&merge_path, &bytes)?;
            anyhow::Ok((bytes, cstats))
        })
        .await??;
        let merged_ms = started.elapsed().as_millis() as u64;
        let merged_bytes = bytes.len() as u64;

        // Publish before adopting. An uncommitted base is a harmless orphan; a
        // worker serving from a base the catalog does not know about would hand
        // out topology that no cold start could reproduce.
        let puffin_path = self
            .table_prefix
            .clone()
            .join("puffin")
            .join(format!("dsu-{sequence:012}.puffin"));
        self.store
            .put(&puffin_path, PutPayload::from(bytes))
            .await?;
        let merged = RunMeta {
            path: puffin_path.to_string(),
            level: merged_level(&inputs),
            min_sequence: inputs.first().map(|r| r.min_sequence).unwrap_or(sequence),
            // The merge subsumes everything through the previous commit, and this
            // snapshot contributes no routing state of its own — no data files,
            // the previous watermark — so extending the span over `sequence`
            // keeps run spans dense without claiming anything untrue.
            max_sequence: sequence,
            pairs: cstats.shared_pairs + cstats.overlay_pairs,
            bytes: merged_bytes,
        };
        let meta = SnapshotMeta {
            sequence,
            committed_at_ms: now_ms(),
            watermark,
            data_files: vec![],
            puffin_path: puffin_path.to_string(),
            committer: self.worker_id.clone(),
            base_sequence: sequence,
            delta_chain_len: 0,
            runs: vec![merged.clone()],
        };
        if self.catalog.commit(&meta).await? == CommitOutcome::Conflict {
            warn!(
                sequence,
                "compaction lost the commit race; discarding the merge"
            );
            let _ = std::fs::remove_file(&path);
            return Ok(false);
        }

        let merged_layers = stack.layers();
        let flat = Arc::new(LayeredBase::new(Arc::new(PuffinBase::open(&path)?)));
        self.forest.swap_base(flat.clone());
        let run = LocalRun::committed(&merged, path.clone(), flat.layer(0));
        *self.layers.lock() = Some(LocalLayers {
            base: flat,
            runs: vec![run],
        });
        // Unlinking a mapped file is safe: the mapping keeps the inode alive
        // until the last query using it drops, and the space is reclaimed then.
        for old in inputs.iter().map(|r| &r.path) {
            if let Err(e) = std::fs::remove_file(old) {
                warn!(path = %old.display(), error = %e, "could not unlink merged layer");
            }
        }
        info!(
            sequence,
            merged_layers,
            shared_pairs = cstats.shared_pairs,
            overlay_pairs = cstats.overlay_pairs,
            registry_entries = cstats.registry_entries,
            registry_corrections = cstats.registry_corrections,
            moved_roots = cstats.moved_roots,
            merged_ms,
            "compacted the layer chain from storage; ingest was never stalled"
        );
        Ok(true)
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
            held.as_ref()
                .map(|l| (l.base.clone(), l.runs.clone(), l.fully_committed()))
        };
        // Captured now: a merge unlinks exactly the runs it read.
        let inputs: Vec<LocalRun> = current
            .as_ref()
            .map(|(_, runs, _)| runs.clone())
            .unwrap_or_default();
        let compact = match &current {
            None => true,
            Some((base, _, fully_committed)) => {
                base.delta_count() + 1 >= self.max_delta_layers || !fully_committed
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
            // No committed chain to merge (the first commit in disk mode):
            // stream the live forest under the lock. Every other compaction goes
            // through `maybe_compact_chain`, which takes no lock at all.
            let (bytes, layered) = self.forest.compact_and_fold(&mut writer, |w| {
                let bytes = puffin::write(&w.finish(), puffin_metadata(watermark));
                write_atomically(&path, &bytes)?;
                let mapped = Arc::new(PuffinBase::open(&path)?);
                let layered = Arc::new(LayeredBase::new(mapped));
                anyhow::Ok((layered.clone() as Arc<dyn RoutingBase>, (bytes, layered)))
            })?;
            // Subsumes everything below it, so it inherits the whole span.
            let span = inputs.first().map(|r| r.min_sequence).unwrap_or(sequence)..=sequence;
            let run =
                LocalRun::written(path.clone(), layered.layer(0), merged_level(&inputs), span);
            (
                bytes,
                LocalLayers {
                    base: layered,
                    runs: vec![run],
                },
            )
        } else {
            let (base, mut runs, _) = current.expect("checked above");
            // Delta: stream only the memtable's own pairs and append them as a
            // new layer over the mappings already open.
            let (bytes, layered) = self.forest.fold_delta(&mut writer, |w, _| {
                let bytes = puffin::write(&w.finish(), puffin_metadata(watermark));
                write_atomically(&path, &bytes)?;
                let mapped = Arc::new(PuffinBase::open(&path)?);
                let layered = Arc::new(base.pushed(mapped));
                anyhow::Ok((layered.clone() as Arc<dyn RoutingBase>, (bytes, layered)))
            })?;
            let newest = layered.layers() - 1;
            runs.push(LocalRun::written(
                path.clone(),
                layered.layer(newest),
                0,
                sequence..=sequence,
            ));
            (
                bytes,
                LocalLayers {
                    base: layered,
                    runs,
                },
            )
        };

        // A merge subsumed the runs it read, so unlink them. Unlinking a mapped
        // file is safe: the mapping keeps the inode alive until the last query
        // using it drops, and the space is reclaimed then. A delta added to the
        // stack supersedes nothing.
        let superseded: Vec<PathBuf> = if compact {
            inputs.iter().map(|r| r.path.clone()).collect()
        } else {
            Vec::new()
        };
        let layer = if compact {
            Layer::Base
        } else {
            Layer::Delta {
                base_sequence: next.base_sequence(),
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

    // Committed routing state is every run in the snapshot's run set, so a cold
    // start maps all of them, oldest span first.
    let runs = committed_runs(catalog, &latest).await?;
    let mut local_runs = Vec::with_capacity(runs.len());
    let mut layers = Vec::with_capacity(runs.len());
    for run in &runs {
        let local = data_dir.join(run_cache_name(run));
        if !local.exists() {
            let bytes = store
                .get(&Path::from(run.path.clone()))
                .await?
                .bytes()
                .await?;
            // Write to a temp path and rename so a torn download can never be
            // mapped as a layer.
            let tmp = local.with_extension("puffin.partial");
            std::fs::write(&tmp, &bytes)?;
            std::fs::rename(&tmp, &local)?;
            info!(
                min_sequence = run.min_sequence,
                max_sequence = run.max_sequence,
                bytes = bytes.len(),
                path = %local.display(),
                "cached routing run from object storage"
            );
        }
        let mapped = Arc::new(PuffinBase::open(&local)?);
        // Every run here came from the catalog, so a delta may be layered on top
        // immediately.
        local_runs.push(LocalRun::committed(run, local, &mapped));
        layers.push(mapped);
    }

    let base = Arc::new(LayeredBase::from_layers(layers)?);
    let stats = base.stats();
    info!(
        sequence = latest.sequence,
        base_sequence = local_runs.first().map(|r| r.max_sequence).unwrap_or(0),
        runs = local_runs.len(),
        delta_layers = base.delta_count(),
        watermark = latest.watermark,
        shared_pairs = stats.shared_pairs,
        overlay_pairs = stats.overlay_pairs,
        mapped_mb = stats.mapped_bytes / (1024 * 1024),
        "opened mmap routing base"
    );
    let local = LocalLayers {
        base: base.clone(),
        runs: local_runs,
    };
    Ok(Some((base, latest.watermark, local)))
}

/// The runs making up committed routing state as of `latest`, oldest span first.
///
/// A snapshot carrying an explicit run set names every run's object path, so this
/// is a single catalog read. The pre-`runs` format recorded only the *newest*
/// commit's Puffin path, so each link there needs its own snapshot fetched, and
/// `catalog.get` fails loudly on a hole rather than skipping it (I5) — a skipped
/// delta would serve stale topology as if it were current.
///
/// Worth noting what changed for the better: on the run-set path an intermediate
/// snapshot JSON is no longer load-bearing at all, so losing one costs nothing.
/// A missing *layer object* is still fatal, which is where the guarantee actually
/// belongs.
async fn committed_runs(
    catalog: &SnapshotCatalog,
    latest: &SnapshotMeta,
) -> anyhow::Result<Vec<RunMeta>> {
    match latest.run_set() {
        RunSet::Runs(runs) => Ok(runs),
        RunSet::SequencesOnly(chain) => {
            let mut out = Vec::new();
            for sequence in chain {
                let meta = if sequence == latest.sequence {
                    latest.clone()
                } else {
                    catalog.get(sequence).await?
                };
                out.push(RunMeta {
                    path: meta.puffin_path,
                    // That format recorded neither levels nor spans. One commit was
                    // one layer, so each run's span is its own sequence; the levels
                    // are all 0 and the first merge relabels them.
                    level: 0,
                    min_sequence: sequence,
                    max_sequence: sequence,
                    pairs: 0,
                    bytes: 0,
                });
            }
            Ok(out)
        }
    }
}

/// Local cache filename for a run.
///
/// Single-sequence runs keep the historic name, so upgrading a worker does not
/// invalidate its warm cache. Runs spanning several sequences only exist in the
/// new format, so they are free to be named by their span.
fn run_cache_name(run: &RunMeta) -> String {
    if run.min_sequence == run.max_sequence {
        format!("routing-layer-{:012}.puffin", run.min_sequence)
    } else {
        format!(
            "routing-layer-{:012}-{:012}.puffin",
            run.min_sequence, run.max_sequence
        )
    }
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
    // Oldest span first. Pairs are fully resolved at their commit time and roots
    // only ever decrease, so applying a later pair for the same node simply
    // overwrites with the newer, lower root — last-writer-wins by span order is
    // exactly right here.
    let runs = committed_runs(catalog, &latest).await?;
    for run in &runs {
        let bytes = store
            .get(&Path::from(run.path.clone()))
            .await?
            .bytes()
            .await?;
        let snapshot = codec::blobs_to_snapshot(&puffin::read(&bytes)?)?;
        forest.hydrate(&snapshot);
    }
    info!(
        sequence = latest.sequence,
        base_sequence = runs.first().map(|r| r.max_sequence).unwrap_or(0),
        runs = runs.len(),
        watermark = latest.watermark,
        "hydrated DSU state from the committed run set"
    );
    Ok(latest.watermark)
}
