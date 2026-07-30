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
use std::ops::{Range, RangeInclusive};
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
use crate::storage::{codec, parquet_io, puffin, tier};

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
    /// Depth ceiling. Past this many runs, merge the lowest stretch available
    /// even when no level is due — lookups and ingest both pay per run, and
    /// cold-start fetches one file each.
    pub max_delta_layers: usize,
    /// Runs per level before they merge into one run at the level above. See
    /// [`tier::pick_merge`].
    pub tier_fanout: usize,
    /// Await each merge inside the tick that starts it, instead of letting it run
    /// in the background.
    ///
    /// Off in production: the whole point of a detached merge is that folds keep
    /// draining the memtable while it works. Worth having because a detached merge
    /// makes a tick no longer a complete unit of work, which matters when you want
    /// ingest to be unable to outrun compaction — during a backfill, say — or when
    /// you want each tick to be deterministic.
    pub inline_merges: bool,
    /// A merge running in the background, waiting to be spliced in.
    pub pending_merge: Mutex<Option<PendingMerge>>,
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
    /// Best-effort value for the compatibility `SnapshotMeta::base_sequence`
    /// field. **It is not always truthful, and cannot be made so.**
    ///
    /// The old field names the sequence a base was *committed at*, while a span
    /// says which sequences a run *covers*. Those agree only while the stack is
    /// chain-shaped: a fold covers its own sequence, and a merge that reaches the
    /// top of the stack extends its span to the commit that publishes it. A
    /// **tiered merge below the top** keeps the span it subsumed and is published
    /// at a later sequence than its span ends, so the oldest run's `max_sequence`
    /// is then not where that run lives, and an old reader following `chain()`
    /// fetches a different, already-subsumed object.
    ///
    /// There is no value that fixes this, because `base_sequence` +
    /// `delta_chain_len` cannot describe runs at several levels at all — see
    /// [`SnapshotMeta::format_version`]. Nothing in this build reads these
    /// fields; they are written so a chain-shaped catalog stays readable, and
    /// `format_version` marks the point past which they should not be trusted.
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

/// `format_version` for a commit carrying `runs`.
///
/// Zero when the run set is empty — RAM mode, where the memtable *is* the state
/// and every commit is a self-contained snapshot — so that a non-zero version is
/// a reliable promise that `runs` is populated, and `SnapshotMeta::run_set` can
/// treat a violation as corruption rather than as an old snapshot.
fn run_set_format(runs: &[RunMeta]) -> u32 {
    if runs.is_empty() {
        0
    } else {
        crate::storage::catalog::FORMAT_RUN_SETS
    }
}

/// A merge running on a blocking thread, and what is needed to splice it in when
/// it lands.
pub struct PendingMerge {
    handle: Option<tokio::task::JoinHandle<anyhow::Result<MergeResult>>>,
    /// Set once the task has been awaited, so the result survives being polled.
    finished: Option<anyhow::Result<MergeResult>>,
    /// Local paths of the runs being merged, in order.
    inputs: Vec<PathBuf>,
    level: u8,
    span: (u64, u64),
    /// Where the merged run was written locally.
    path: PathBuf,
}

impl std::fmt::Debug for PendingMerge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingMerge")
            .field("level", &self.level)
            .field("span", &self.span)
            .field("runs", &self.inputs.len())
            .finish()
    }
}

struct MergeResult {
    bytes: bytes::Bytes,
    cstats: crate::storage::compact::CompactionStats,
    merged_ms: u64,
}

/// Where the runs at `inputs` sit in `runs` now, as a contiguous window.
///
/// By identity rather than by remembered indices, because a background merge
/// outlives the stack it was planned against: folds append below it, and a
/// full-stack fold can replace it outright. `None` means the window is gone and
/// the merge no longer describes this stack.
fn locate_run_window(runs: &[LocalRun], inputs: &[PathBuf]) -> Option<Range<usize>> {
    if inputs.is_empty() || inputs.len() > runs.len() {
        return None;
    }
    runs.windows(inputs.len())
        .position(|w| w.iter().map(|r| &r.path).eq(inputs.iter()))
        .map(|i| i..i + inputs.len())
}

/// Where a run's bytes live in object storage.
///
/// The UUID is not decoration. Two workers can each believe they hold the lease
/// for the same sequence — that is exactly the race `CommitOutcome::Conflict`
/// exists to settle — and both upload before either learns who won. Without a
/// unique suffix they write the same key, so the loser can overwrite the winner's
/// object *after* the winning commit, leaving a snapshot that names a run composed
/// over a different stack. Parquet data files have always been named this way; this
/// closes the same hole for Puffin.
fn run_object_path(table_prefix: &Path, sequence: u64) -> Path {
    table_prefix.clone().join("puffin").join(format!(
        "dsu-{sequence:012}-{}.puffin",
        uuid::Uuid::new_v4()
    ))
}

/// Local cache filename for a run.
///
/// Keyed on the object path, not only the span, so two runs that cover the same
/// sequences but hold different bytes — divergent local stacks after a lost commit
/// race — cannot alias onto one cached file.
fn run_cache_name(run: &RunMeta) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for b in run.path.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!(
        "routing-run-{:012}-{:012}-{hash:016x}.puffin",
        run.min_sequence, run.max_sequence
    )
}

/// The committed run set with `range` replaced by `merged`.
///
/// `None` if any run left in place is local-only, since the set would then name an
/// object that does not exist.
fn splice_run_set(
    inputs: &[LocalRun],
    range: Range<usize>,
    merged: RunMeta,
    sequence: u64,
) -> Option<Vec<RunMeta>> {
    let mut out = Vec::with_capacity(inputs.len() - range.len() + 1);
    for run in &inputs[..range.start] {
        out.push(run.meta(run.remote.clone()?));
    }
    out.push(merged);
    for run in &inputs[range.end..] {
        out.push(run.meta(run.remote.clone()?));
    }
    // This snapshot contributes no routing state of its own — no data files, the
    // previous watermark — so whichever run ends up newest simply also describes
    // `sequence`. Without that the set would leave a hole at its own sequence,
    // and runs resolve by span, so a hole is a sequence belonging to no run.
    out.last_mut()?.max_sequence = sequence;
    Some(out)
}

/// Level for a run merged from `inputs`.
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

        let mut sequence = latest.as_ref().map(|s| s.sequence + 1).unwrap_or(1);

        if !self.elector.is_leader() {
            // Followers serve queries from the same structures the leader
            // does, so their memtable grows at exactly the same rate. Folding
            // only on the leader would relocate the leak, not fix it. These
            // folds are never committed, which is what `LocalLayers::
            // fully_committed` exists to detect.
            self.fold(
                sequence,
                latest.as_ref().map(|s| s.watermark).unwrap_or(0),
                false,
            )?;
            return Ok(());
        }

        let prev_watermark = latest.as_ref().map(|s| s.watermark).unwrap_or(0);

        // Adopt a merge started on an earlier tick, if one is ready, then start
        // whatever the policy has due now. Adopting first means a merge is never
        // published by the tick that launched it, so "detached" really is
        // detached — the alternative ordering would quietly behave like the inline
        // path whenever a merge happened to finish quickly.
        //
        // Either way the tick *continues* and commits this batch at the next
        // sequence. It used to return here, which was fine when a merge fired
        // roughly one tick in twenty-four; under tiering it fires on ~10% of ticks
        // and up to three in a row, and each early return held the watermark back
        // for a whole interval. Retained segments and a stalled watermark are the
        // recovery-point objective, and that is not worth paying for a merge that
        // took no lock and touched no buffered data.
        if self.adopt_merge(sequence, prev_watermark).await? {
            // The merge took this sequence; the data batch follows at the next.
            sequence += 1;
        }
        // Returns immediately: the merge runs on a blocking thread, so the fold
        // below keeps draining the memtable while it works. That is the whole
        // point — a high-level merge is minutes of CPU, and awaiting it inside the
        // tick let the memtable grow for the duration, unbounded by the fold
        // trigger.
        self.start_merge(sequence, prev_watermark)?;
        if self.inline_merges {
            self.wait_for_merge().await;
            if self.adopt_merge(sequence, prev_watermark).await? {
                sequence += 1;
            }
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
        let puffin_path = run_object_path(&self.table_prefix, sequence);
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
            format_version: run_set_format(&runs),
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

    /// Start a merge of whichever runs the tiering policy picks, if one is due
    /// and none is already in flight.
    ///
    /// Returns immediately. The merge runs on a blocking thread and is picked up
    /// by [`Flusher::adopt_merge`] on a later tick, so **folds and commits
    /// continue while it works** — which is the point. Merges are minutes of CPU
    /// and disk for a high-level run, and awaiting one inside the tick meant the
    /// memtable grew for its whole duration, unbounded by the fold trigger.
    ///
    /// What makes this possible at all is that the merge is pure storage-side: the
    /// inputs are immutable committed layer files that are already mapped, so it
    /// takes no union lock, never reads the forest, and cannot conflict with
    /// ingest. It merges a **subset** — see [`tier::pick_merge`] for the policy and
    /// [`LayeredBase::slice`] for why a subset merge is sound.
    fn start_merge(&self, sequence: u64, watermark: u64) -> anyhow::Result<()> {
        let Some(dir) = &self.base_dir else {
            return Ok(());
        };
        // One at a time. Two concurrent merges could pick overlapping ranges, and
        // the second to finish would splice over a stack the first had already
        // rewritten.
        if self.pending_merge.lock().is_some() {
            return Ok(());
        }
        let Some((stack, inputs)) = ({
            let held = self.layers.lock();
            held.as_ref().map(|l| (l.base.clone(), l.runs.clone()))
        }) else {
            return Ok(());
        };
        // A local-only run in the stack cannot be named in a committed run set,
        // and a partial merge would leave one there. Decline; `fold` handles that
        // case by merging the whole stack into one self-contained run.
        if inputs.iter().any(|r| r.remote.is_none()) {
            return Ok(());
        }
        let levels: Vec<u8> = inputs.iter().map(|r| r.level).collect();
        let Some(range) = tier::pick_merge(&levels, self.tier_fanout, self.max_delta_layers) else {
            return Ok(());
        };

        let level = merged_level(&inputs[range.clone()]);
        let path = dir.join(format!(
            "routing-{}-{sequence:012}-L{level}.puffin",
            self.worker_id
        ));
        let merge_stack = stack.slice(range.clone())?;
        let merge_path = path.clone();
        let meta = puffin_metadata(watermark);
        let started = std::time::Instant::now();
        // `sequence` here is only what the run's blob metadata records; the
        // sequence it is actually *committed* at is decided at adoption, which may
        // be several ticks later. Nothing resolves by that number — spans do that.
        let handle = tokio::task::spawn_blocking(move || {
            let (blobs, cstats) = compact_layers(&merge_stack, sequence);
            let bytes = puffin::write(&blobs, meta);
            write_atomically(&merge_path, &bytes)?;
            anyhow::Ok(MergeResult {
                bytes,
                cstats,
                merged_ms: started.elapsed().as_millis() as u64,
            })
        });
        info!(
            level,
            runs = range.len(),
            spans = format!(
                "{}..={}",
                inputs[range.start].min_sequence,
                inputs[range.end - 1].max_sequence
            ),
            "started a background merge; ingest and folds continue"
        );
        *self.pending_merge.lock() = Some(PendingMerge {
            handle: Some(handle),
            finished: None,
            // Identity, not indices: the stack may have grown by the time this
            // lands, and a run that moved means the merge no longer describes a
            // contiguous window of it.
            inputs: inputs[range.clone()]
                .iter()
                .map(|r| r.path.clone())
                .collect(),
            level,
            span: (
                inputs[range.start].min_sequence,
                inputs[range.end - 1].max_sequence,
            ),
            path,
        });
        Ok(())
    }

    /// Wait for an in-flight merge to finish, leaving its result ready for the
    /// next [`Flusher::adopt_merge`].
    ///
    /// For a graceful shutdown, and for `--inline-merges`, where a tick is
    /// supposed to be a complete unit of work.
    pub async fn wait_for_merge(&self) {
        let handle = {
            let mut held = self.pending_merge.lock();
            held.as_mut().and_then(|p| p.handle.take())
        };
        let Some(handle) = handle else { return };
        let result = match handle.await {
            Ok(r) => r,
            Err(e) => Err(anyhow::anyhow!("merge task failed: {e}")),
        };
        if let Some(pending) = self.pending_merge.lock().as_mut() {
            pending.finished = Some(result);
        }
    }

    /// Publish a finished merge and splice it into the stack. Returns whether it
    /// did, i.e. whether it consumed `sequence`.
    ///
    /// The merge covers an **earlier span** than commits made while it ran, which
    /// is precisely what the run-set format exists to express: the result is
    /// spliced back at the position its inputs held, and the deltas committed
    /// meanwhile stay above it.
    ///
    /// Published as its own snapshot carrying the **previous** watermark and no
    /// data files, because that is what it covers: the merged runs, not the
    /// memtable, which `swap_base` deliberately preserves. A commit claiming this
    /// tick's watermark would tell a cold start that merges still sitting in the
    /// memtable were durable, and replay would skip them.
    async fn adopt_merge(&self, sequence: u64, watermark: u64) -> anyhow::Result<bool> {
        // Take it only if it is ready, so a tick never blocks on the merge.
        let pending = {
            let mut held = self.pending_merge.lock();
            let ready = held.as_ref().is_some_and(|p| {
                p.finished.is_some() || p.handle.as_ref().is_some_and(|h| h.is_finished())
            });
            if ready { held.take() } else { None }
        };
        let Some(mut pending) = pending else {
            return Ok(false);
        };
        let result = match pending.finished.take() {
            Some(r) => r,
            None => match pending.handle.take() {
                // Already finished, so this resolves without waiting.
                Some(h) => h.await.unwrap_or_else(|e| Err(anyhow::anyhow!("{e}"))),
                None => return Ok(false),
            },
        };
        let result = match result {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "background merge failed; will retry next cycle");
                let _ = std::fs::remove_file(&pending.path);
                return Ok(false);
            }
        };

        let Some((stack, current)) = ({
            let held = self.layers.lock();
            held.as_ref().map(|l| (l.base.clone(), l.runs.clone()))
        }) else {
            return Ok(false);
        };
        // Where the merged runs sit *now*. Folds only append, so the window is
        // normally still where it was — but `fold` can also rebuild the whole
        // stack into one self-contained run, and then this merge describes state
        // that no longer exists in that shape. Discarding is the right answer, and
        // finding the window by identity is what detects it.
        let Some(range) = locate_run_window(&current, &pending.inputs) else {
            warn!("the stack changed under a background merge; discarding it");
            let _ = std::fs::remove_file(&pending.path);
            return Ok(false);
        };
        if current.iter().any(|r| r.remote.is_none()) {
            // A local-only run appeared while we merged, so the set cannot be
            // described. Keep the merged file: the next tick may be able to use it.
            return Ok(false);
        }

        let merged_bytes = result.bytes.len() as u64;
        let cstats = result.cstats;
        // Publish before adopting. An uncommitted run is a harmless orphan; a
        // worker serving from state the catalog does not know about would hand out
        // topology that no cold start could reproduce.
        let puffin_path = run_object_path(&self.table_prefix, sequence);
        self.store
            .put(&puffin_path, PutPayload::from(result.bytes))
            .await?;
        let merged = RunMeta {
            path: puffin_path.to_string(),
            level: pending.level,
            // Inherits the span of everything it subsumed, which is what fixes its
            // place in the resolution order — and why it can be published at a
            // sequence well past the span it covers.
            min_sequence: pending.span.0,
            max_sequence: pending.span.1,
            pairs: cstats.shared_pairs + cstats.overlay_pairs,
            bytes: merged_bytes,
        };
        let Some(runs) = splice_run_set(&current, range.clone(), merged, sequence) else {
            warn!(
                sequence,
                "run set became undescribable mid-merge; discarding"
            );
            let _ = std::fs::remove_file(&pending.path);
            return Ok(false);
        };
        let base_sequence = runs[0].max_sequence;
        let meta = SnapshotMeta {
            sequence,
            committed_at_ms: now_ms(),
            watermark,
            data_files: vec![],
            puffin_path: puffin_path.to_string(),
            committer: self.worker_id.clone(),
            base_sequence,
            delta_chain_len: sequence - base_sequence,
            format_version: run_set_format(&runs),
            runs: runs.clone(),
        };
        if self.catalog.commit(&meta).await? == CommitOutcome::Conflict {
            warn!(
                sequence,
                "compaction lost the commit race; discarding the merge"
            );
            let _ = std::fs::remove_file(&pending.path);
            return Ok(false);
        }

        let merged_runs = range.len();
        let spliced =
            Arc::new(stack.spliced(range.clone(), Arc::new(PuffinBase::open(&pending.path)?))?);
        self.forest.swap_base(spliced.clone());
        // Local files in the spliced order: kept below, the merge, kept above.
        let local_paths = current[..range.start]
            .iter()
            .map(|r| r.path.clone())
            .chain(std::iter::once(pending.path.clone()))
            .chain(current[range.end..].iter().map(|r| r.path.clone()));
        let next: Vec<LocalRun> = runs
            .iter()
            .zip(local_paths)
            .enumerate()
            .map(|(i, (meta, local))| LocalRun::committed(meta, local, spliced.layer(i)))
            .collect();
        let runs_now = next.len();
        *self.layers.lock() = Some(LocalLayers {
            base: spliced,
            runs: next,
        });
        // Only the runs the merge subsumed are gone. Unlinking a mapped file is
        // safe: the mapping keeps the inode alive until the last query using it
        // drops, and the space is reclaimed then.
        for old in current[range].iter().map(|r| &r.path) {
            if let Err(e) = std::fs::remove_file(old) {
                warn!(path = %old.display(), error = %e, "could not unlink merged run");
            }
        }
        info!(
            sequence,
            merged_runs,
            merged_level = pending.level,
            runs_now,
            shared_pairs = cstats.shared_pairs,
            overlay_pairs = cstats.overlay_pairs,
            registry_entries = cstats.registry_entries,
            registry_corrections = cstats.registry_corrections,
            moved_roots = cstats.moved_roots,
            merged_ms = result.merged_ms,
            "merged runs from storage; ingest was never stalled"
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
    /// base is appended to rather than rewritten. It becomes a self-contained run
    /// only when nothing has been mapped yet, or when this worker holds a
    /// local-only run (see [`LocalLayers::fully_committed`]), which is a stack
    /// that cannot be described as a committed run set.
    ///
    /// It deliberately does **not** compact on depth. That was the only depth
    /// control before tiering, and leaving it in place would preempt the policy:
    /// with a merge in flight the stack still looks deep, so the fold would
    /// rewrite the whole thing inline under the union lock — doing the merge's
    /// work synchronously, which is exactly what detaching it was for. Depth is
    /// [`tier::pick_merge`]'s job now, via its ceiling branch. A stack that stays
    /// over the ceiling means merges are not completing, which degrades smoothly
    /// and should be alerted on rather than papered over here.
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
    /// `fully_committed` check keeps it from being built on.
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
            Some((_, _, fully_committed)) => !fully_committed,
        };
        if let Some((base, _, _)) = &current
            && base.layers() > self.max_delta_layers
        {
            warn!(
                runs = base.layers(),
                max_delta_layers = self.max_delta_layers,
                "run count is over the ceiling; merges are not keeping up, so lookups \
                 and ingest are both paying for the depth"
            );
        }

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
    match latest.run_set()? {
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
