//! Snapshot catalog over object storage.
//!
//! Commits follow the Iceberg model: data + Puffin files are written first,
//! then a single immutable metadata JSON makes them visible. The metadata
//! object is created with a put-if-absent (`PutMode::Create`), so two
//! would-be leaders racing on the same sequence number cannot clobber each
//! other — exactly one wins, the loser observes the conflict and re-reads.
//!
//! Swap point: a production deployment replaces this with an Iceberg REST
//! catalog commit (same optimistic-concurrency shape); the on-disk layout —
//! `data/*.parquet`, `puffin/*.puffin`, `metadata/snap-*.json` — is already
//! Iceberg-flavored to make that migration mechanical.

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::{StreamId, StreamPosition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileMeta {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    /// Legacy scalar span, exact for a single-partition stream and `0` otherwise.
    /// Prefer `first_position`/`last_position`.
    pub min_offset: u64,
    pub max_offset: u64,
    /// Lowest offset this file holds per partition.
    #[serde(default, skip_serializing_if = "StreamPosition::is_empty")]
    pub first_position: StreamPosition,
    /// Highest offset this file holds per partition.
    #[serde(default, skip_serializing_if = "StreamPosition::is_empty")]
    pub last_position: StreamPosition,
}

/// One committed micro-batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub sequence: u64,
    pub committed_at_ms: i64,
    /// Highest event offset covered by this snapshot (inclusive), as a scalar.
    ///
    /// **Legacy, and only meaningful for a single-partition stream.** Superseded
    /// by `position`, which can describe more than one partition. Still written
    /// when the position *has* an exact scalar form — a position covering
    /// partition 0 alone is not approximated by this number, it is this number —
    /// so a binary predating `position` reads a newer catalog correctly for the
    /// only topology it could ever have handled.
    ///
    /// Written as `0` for a multi-partition position, because no `u64` describes
    /// one. Read [`SnapshotMeta::stream_position`], never this field.
    pub watermark: u64,
    /// Where the input stream stands as of this snapshot, per partition.
    ///
    /// Absent on snapshots written before it existed; use
    /// [`SnapshotMeta::stream_position`], which synthesises `{0: watermark}` for
    /// those, the same way [`SnapshotMeta::run_set`] synthesises a run set for
    /// pre-`runs` snapshots.
    #[serde(default, skip_serializing_if = "StreamPosition::is_empty")]
    pub position: StreamPosition,
    /// Which stream `position` is measured in.
    ///
    /// Offsets without this are numbers without units. A worker pointed at a
    /// renamed or rebuilt topic resumes at an offset that exists and means
    /// something else — it hydrates cleanly, replays a plausible suffix, and
    /// produces a table nothing downstream can tell is wrong. Absent on
    /// snapshots written before it existed, and on those the check cannot be
    /// made rather than being made wrongly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<StreamId>,
    pub data_files: Vec<DataFileMeta>,
    /// Puffin sidecar for *this* commit. A base layer when
    /// `base_sequence == sequence`, otherwise a delta over the chain below it.
    pub puffin_path: String,
    pub committer: String,
    /// Sequence of the newest full base. Routing state as of this snapshot is
    /// that base plus every delta in `base_sequence+1..=sequence`, applied in
    /// order.
    #[serde(default = "default_base_sequence")]
    pub base_sequence: u64,
    /// Deltas layered on the base, i.e. `sequence - base_sequence`. Stored
    /// rather than derived so a reader can see at a glance how much chain a
    /// cold start has to fetch.
    #[serde(default)]
    pub delta_chain_len: u64,
    /// The runs that make up routing state as of this snapshot, oldest first.
    ///
    /// This supersedes `base_sequence` + `delta_chain_len`, which can only
    /// describe *one* base followed by a contiguous chain — a shape that breaks
    /// as soon as merges stop being all-or-nothing. Two things need that:
    ///
    /// - **Tiered compaction** merges a *subset* of runs, so there is no single
    ///   privileged base, just runs at different size levels.
    /// - **Detached compaction** finishes after later deltas have been
    ///   committed, and its output covers an *earlier* span than the newest
    ///   commit. There is no sequence number at which that can be expressed as
    ///   "a base plus everything after it".
    ///
    /// Empty on snapshots written before this field existed; use
    /// [`SnapshotMeta::run_set`] rather than reading it directly.
    #[serde(default)]
    pub runs: Vec<RunMeta>,
    /// Catalog format generation, `0` for anything written before run sets.
    ///
    /// **Reading a run-set catalog with a pre-run-set binary is not safe**, and
    /// this field exists to name that rather than fix it. It cannot fix it: a
    /// tiered stack is runs at several levels, which `base_sequence` +
    /// `delta_chain_len` simply cannot describe, so there is no value of those
    /// fields that an old reader resolves correctly. They are written on a
    /// best-effort basis for a chain-shaped stack and are wrong for any other.
    ///
    /// An old binary ignores this field — that is the whole problem, and why the
    /// upgrade is one-way. Once a tiered merge has been committed, roll forward,
    /// not back. A new reader treats `format_version >= FORMAT_RUN_SETS` as a
    /// promise that `runs` is populated and well-formed, and fails loudly if not.
    #[serde(default)]
    pub format_version: u32,
}

/// `format_version` for snapshots that describe themselves as a set of runs.
pub const FORMAT_RUN_SETS: u32 = 1;

/// `format_version` for snapshots carrying a per-partition [`StreamPosition`].
///
/// A **single**-partition snapshot at this version is still readable by an older
/// binary, because `watermark` carries the same number. A multi-partition one is
/// not, and cannot be: there is no scalar that describes it. That makes the
/// upgrade one-way exactly when the topology stops being one a pre-010 binary
/// could serve — the same bargain [`FORMAT_RUN_SETS`] struck, for the same
/// reason.
pub const FORMAT_STREAM_POSITION: u32 = 2;

/// One immutable run of routing pairs — a folded memtable, or the output of
/// merging several runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunMeta {
    pub path: String,
    /// Size tier. 0 is a freshly folded memtable; merging `fanout` runs at level
    /// `n` produces one run at level `n + 1`.
    #[serde(default)]
    pub level: u8,
    /// Catalog sequences this run covers, inclusive.
    ///
    /// This is what fixes a run's place in the resolution order, and it is why a
    /// merged run inherits the *span* of everything it subsumed rather than
    /// taking the sequence it was committed at. Runs resolve oldest-span-first,
    /// and the disjoint-keys invariant holds because a run's keys are composed
    /// roots of every run with an earlier span.
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub pairs: u64,
    pub bytes: u64,
}

impl RunMeta {
    /// Whether `self` is immediately followed by `next` with no gap — merges may
    /// only span a contiguous stretch, or a run left in the middle would end up
    /// resolved out of order.
    pub fn adjacent_to(&self, next: &RunMeta) -> bool {
        self.max_sequence + 1 == next.min_sequence
    }
}

/// Snapshots written before delta support had no `base_sequence`; every one of
/// them was a full base, so treating a missing field as "this commit is its own
/// base" reads old catalogs correctly.
fn default_base_sequence() -> u64 {
    0
}

impl SnapshotMeta {
    /// Where the stream stands as of this snapshot — the single way readers
    /// should ask.
    ///
    /// Falls back to `{0: watermark}` for snapshots written before `position`
    /// existed. That is not a guess: every such snapshot was written by a worker
    /// that could only consume one stream, and partition 0 is what a
    /// single-partition source produces, so the synthesised position is exactly
    /// what the scalar meant.
    pub fn stream_position(&self) -> StreamPosition {
        if self.position.is_empty() {
            StreamPosition::single(self.watermark)
        } else {
            self.position.clone()
        }
    }

    /// Set the position, keeping `watermark` in step where a scalar is exact.
    ///
    /// The two fields are written together and never independently, so the
    /// invariant "`watermark` is either the same number or `0`" holds by
    /// construction rather than by everyone remembering.
    pub fn set_stream_position(&mut self, position: StreamPosition) {
        self.watermark = position.as_scalar().unwrap_or(0);
        if position.len() > 1 {
            self.format_version = self.format_version.max(FORMAT_STREAM_POSITION);
        }
        self.position = position;
    }

    /// Whether this snapshot may follow `previous` in the same table.
    ///
    /// Every partition must be at or ahead of where it was. Under a scalar
    /// watermark this was `>=` and needed no argument; under a partial order two
    /// successive positions can be **incomparable**, and that is not a
    /// tolerable state — it means some partition went backwards, which is a
    /// second consumer applying records this one has not seen. Left alone it
    /// double-writes rows and, on the buffer side, drops segments that are not
    /// durable. Caught here it is one error at commit time.
    pub fn advances_from(&self, previous: &SnapshotMeta) -> anyhow::Result<()> {
        let (before, after) = (previous.stream_position(), self.stream_position());
        anyhow::ensure!(
            after.dominates(&before),
            "snapshot {} moves the stream position backwards: {} does not cover {}. \
             A partition can only advance, so this is another consumer committing \
             to the same table.",
            self.sequence,
            after,
            before,
        );
        if let (Some(a), Some(b)) = (&self.stream, &previous.stream) {
            anyhow::ensure!(
                a.same_stream(b),
                "snapshot {} claims stream {a} but snapshot {} committed {b}; \
                 offsets from one are meaningless in the other",
                self.sequence,
                previous.sequence,
            );
        }
        Ok(())
    }

    /// Sequences whose Puffin files must be read, oldest first, to reconstruct
    /// routing state as of this snapshot.
    pub fn chain(&self) -> std::ops::RangeInclusive<u64> {
        // `base_sequence == 0` means a pre-delta snapshot: it is its own base.
        let base = if self.base_sequence == 0 {
            self.sequence
        } else {
            self.base_sequence
        };
        base..=self.sequence
    }

    /// The runs to assemble, oldest first — the single way readers should ask.
    ///
    /// Falls back to synthesising a run set from `base_sequence` +
    /// `delta_chain_len` for snapshots written before `runs` existed, so an old
    /// catalog reads correctly without a migration. The synthesised runs carry no
    /// paths, because the old format only recorded the *newest* commit's Puffin
    /// path and a reader has to fetch each sequence's own snapshot to learn the
    /// rest; `sequences_only` says so explicitly rather than inventing paths.
    ///
    /// Fallible because the span structure is checked here, and it is checked here
    /// because this is the one place every reader passes through. Runs resolve
    /// oldest-span-first, so a gap means some sequence's state belongs to no run
    /// and an overlap means two runs claim it — either way resolution order stops
    /// being total, and the disjoint-keys argument stops holding. Nothing else
    /// would notice: a malformed set assembles and answers just as cleanly as a
    /// correct one, only wrongly. The cost is a walk of a handful of runs, once
    /// per commit or cold start.
    pub fn run_set(&self) -> anyhow::Result<RunSet> {
        if self.format_version >= FORMAT_RUN_SETS {
            anyhow::ensure!(
                !self.runs.is_empty(),
                "snapshot {} claims the run-set format but lists no runs",
                self.sequence
            );
        }
        if self.runs.is_empty() {
            return Ok(RunSet::SequencesOnly(self.chain()));
        }

        let mut runs = self.runs.clone();
        runs.sort_by_key(|r| r.min_sequence);
        for run in &runs {
            anyhow::ensure!(
                run.min_sequence <= run.max_sequence,
                "snapshot {}: run {} spans backwards ({}..={})",
                self.sequence,
                run.path,
                run.min_sequence,
                run.max_sequence
            );
        }
        for pair in runs.windows(2) {
            anyhow::ensure!(
                pair[0].adjacent_to(&pair[1]),
                "snapshot {}: runs {}..={} and {}..={} are not adjacent, so sequence order is \
                 ambiguous",
                self.sequence,
                pair[0].min_sequence,
                pair[0].max_sequence,
                pair[1].min_sequence,
                pair[1].max_sequence
            );
        }
        // Note the *oldest* run is deliberately not required to start at 1: a
        // worker that cold-started from a pre-run-set catalog inherits that
        // catalog's base sequence, and everything below it is already inside that
        // base.
        let newest = runs.last().expect("non-empty");
        anyhow::ensure!(
            newest.max_sequence == self.sequence,
            "snapshot {}: newest run ends at {}, so this commit's own sequence is in no run",
            self.sequence,
            newest.max_sequence
        );
        Ok(RunSet::Runs(runs))
    }
}

/// How a snapshot describes its routing state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSet {
    /// Explicit runs with levels, spans and paths.
    Runs(Vec<RunMeta>),
    /// A pre-`runs` snapshot: a base sequence followed by a contiguous chain,
    /// with each link's path found by reading that sequence's own snapshot.
    SequencesOnly(std::ops::RangeInclusive<u64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    /// Another worker committed this sequence first.
    Conflict,
}

pub struct SnapshotCatalog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    /// Highest sequence this process has observed as committed (0 = none
    /// yet). Sequences are dense, so `latest()` can probe forward from here
    /// with cheap HEADs instead of listing the whole metadata/ prefix —
    /// which would grow linearly with snapshot history on S3.
    cached_seq: AtomicU64,
}

impl SnapshotCatalog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self {
            store,
            prefix,
            cached_seq: AtomicU64::new(0),
        }
    }

    fn snap_path(&self, sequence: u64) -> Path {
        // Zero-padded so lexicographic listing order == sequence order.
        self.prefix
            .clone()
            .join("metadata")
            .join(format!("snap-{sequence:012}.json"))
    }

    /// Latest committed snapshot, if any. First call (or an empty catalog)
    /// scans the metadata/ prefix; afterwards it probes forward from the
    /// cached sequence — normally zero or one HEAD per tick.
    pub async fn latest(&self) -> anyhow::Result<Option<SnapshotMeta>> {
        let mut seq = self.cached_seq.load(Ordering::Acquire);
        if seq == 0 {
            seq = self.scan_latest_seq().await?;
            if seq == 0 {
                return Ok(None);
            }
        }
        while self.exists(seq + 1).await? {
            seq += 1;
        }
        self.cached_seq.fetch_max(seq, Ordering::AcqRel);
        let bytes = self.store.get(&self.snap_path(seq)).await?.bytes().await?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// One specific snapshot by sequence. Hydration walks a delta chain and
    /// needs each link's Puffin path; a missing link is a hard error (I5) —
    /// skipping it would silently serve stale topology.
    pub async fn get(&self, sequence: u64) -> anyhow::Result<SnapshotMeta> {
        let path = self.snap_path(sequence);
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(|e| anyhow::anyhow!("delta chain is missing snapshot {sequence}: {e}"))?
            .bytes()
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn exists(&self, sequence: u64) -> anyhow::Result<bool> {
        match self.store.head(&self.snap_path(sequence)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Full listing fallback for a cold catalog view.
    async fn scan_latest_seq(&self) -> anyhow::Result<u64> {
        let prefix = self.prefix.clone().join("metadata");
        let mut newest = 0u64;
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            let name = meta.location.filename().unwrap_or_default();
            if let Some(seq) = name
                .strip_prefix("snap-")
                .and_then(|r| r.strip_suffix(".json"))
                .and_then(|r| r.parse::<u64>().ok())
            {
                newest = newest.max(seq);
            }
        }
        Ok(newest)
    }

    /// Atomically publish a snapshot. Fails with `Conflict` if some other
    /// worker already committed this sequence number.
    pub async fn commit(&self, snapshot: &SnapshotMeta) -> anyhow::Result<CommitOutcome> {
        let path = self.snap_path(snapshot.sequence);
        let payload = PutPayload::from(serde_json::to_vec_pretty(snapshot)?);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&path, payload, opts).await {
            Ok(_) => {
                self.cached_seq
                    .fetch_max(snapshot.sequence, Ordering::AcqRel);
                Ok(CommitOutcome::Committed)
            }
            Err(object_store::Error::AlreadyExists { .. }) => Ok(CommitOutcome::Conflict),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// Deliberately legacy-shaped: no `position`, no `stream`. The existing
    /// tests keep exercising the pre-010 form, and the fallback that reads it.
    fn snap(seq: u64, watermark: u64) -> SnapshotMeta {
        SnapshotMeta {
            sequence: seq,
            committed_at_ms: 0,
            watermark,
            position: StreamPosition::new(),
            stream: None,
            data_files: vec![],
            puffin_path: format!("puffin/dsu-{seq}.puffin"),
            committer: "test".into(),
            runs: Vec::new(),
            format_version: 0,
            base_sequence: seq,
            delta_chain_len: 0,
        }
    }

    fn run(level: u8, min_sequence: u64, max_sequence: u64) -> RunMeta {
        RunMeta {
            path: format!("puffin/run-L{level}-{min_sequence}-{max_sequence}.puffin"),
            level,
            min_sequence,
            max_sequence,
            pairs: 1,
            bytes: 1,
        }
    }

    /// A catalog written before `runs` existed has to keep reading correctly —
    /// there is no migration, so the absent field must land on the chain path
    /// rather than on an empty run set (which would read as "no state at all").
    #[test]
    fn a_snapshot_without_runs_falls_back_to_its_chain() {
        let json = r#"{
            "sequence": 7,
            "committed_at_ms": 0,
            "watermark": 100,
            "data_files": [],
            "puffin_path": "puffin/dsu-7.puffin",
            "committer": "old-worker",
            "base_sequence": 4,
            "delta_chain_len": 3
        }"#;
        let meta: SnapshotMeta = serde_json::from_str(json).unwrap();
        assert!(meta.runs.is_empty());
        assert_eq!(meta.run_set().unwrap(), RunSet::SequencesOnly(4..=7));
    }

    /// Pre-delta snapshots recorded no `base_sequence` at all; each was its own
    /// base, and `chain()` already encodes that. Checked here too because
    /// `run_set` is now the only entry point readers use.
    #[test]
    fn a_pre_delta_snapshot_is_its_own_base() {
        let json = r#"{
            "sequence": 3,
            "committed_at_ms": 0,
            "watermark": 10,
            "data_files": [],
            "puffin_path": "puffin/dsu-3.puffin",
            "committer": "ancient"
        }"#;
        let meta: SnapshotMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.run_set().unwrap(), RunSet::SequencesOnly(3..=3));
    }

    /// Resolution order is by span, not by the order the runs happen to be
    /// listed in — a detached merge appends a run covering an *earlier* span
    /// than runs already in the list, so the sort is load-bearing, not cosmetic.
    #[test]
    fn runs_resolve_oldest_span_first_however_they_were_listed() {
        let mut meta = snap(9, 900);
        meta.runs = vec![run(0, 9, 9), run(2, 1, 6), run(1, 7, 8)];

        let RunSet::Runs(runs) = meta.run_set().unwrap() else {
            panic!("explicit runs must not fall back to the chain");
        };
        assert_eq!(
            runs.iter().map(|r| r.min_sequence).collect::<Vec<_>>(),
            vec![1, 7, 9]
        );
        // Levels are deliberately out of order relative to spans: a big old run
        // sits *below* newer small ones. Level says how much was merged into a
        // run, span says where it resolves; conflating them inverts the stack.
        assert_eq!(
            runs.iter().map(|r| r.level).collect::<Vec<_>>(),
            vec![2, 1, 0]
        );
        assert!(runs[0].adjacent_to(&runs[1]));
        assert!(runs[1].adjacent_to(&runs[2]));
    }

    #[test]
    fn adjacency_rejects_gaps_and_overlaps() {
        assert!(run(0, 1, 4).adjacent_to(&run(0, 5, 5)));
        // A gap would leave a run to be resolved between these two.
        assert!(!run(0, 1, 4).adjacent_to(&run(0, 6, 6)));
        // An overlap means both claim the same sequence.
        assert!(!run(0, 1, 4).adjacent_to(&run(0, 4, 6)));
    }

    /// A malformed run set must fail loudly, because nothing downstream would
    /// notice. Runs resolve by span, so a gap leaves a sequence owned by no run
    /// and an overlap has two runs claiming one — either way the order stops being
    /// total, and the stack still assembles and still answers, just wrongly.
    #[test]
    fn a_malformed_run_set_is_rejected() {
        let mut meta = snap(9, 900);
        meta.format_version = FORMAT_RUN_SETS;

        meta.runs = vec![run(0, 1, 4), run(0, 6, 9)];
        let err = meta.run_set().unwrap_err().to_string();
        assert!(err.contains("not adjacent"), "gap not caught: {err}");

        meta.runs = vec![run(0, 1, 5), run(0, 4, 9)];
        let err = meta.run_set().unwrap_err().to_string();
        assert!(err.contains("not adjacent"), "overlap not caught: {err}");

        meta.runs = vec![run(0, 1, 4), run(0, 5, 7)];
        let err = meta.run_set().unwrap_err().to_string();
        assert!(
            err.contains("own sequence is in no run"),
            "short set not caught: {err}"
        );

        meta.runs = vec![run(0, 6, 2)];
        let err = meta.run_set().unwrap_err().to_string();
        assert!(err.contains("spans backwards"), "got: {err}");

        // The well-formed set it was nearly.
        meta.runs = vec![run(1, 1, 4), run(0, 5, 9)];
        assert!(meta.run_set().is_ok());
    }

    /// The format marker is the only thing distinguishing "written before run
    /// sets" from "written by a run-set writer that emitted none", and those need
    /// opposite handling: read the chain, or refuse.
    #[test]
    fn claiming_the_run_set_format_without_runs_is_corruption() {
        let mut meta = snap(3, 300);
        meta.format_version = FORMAT_RUN_SETS;
        let err = meta.run_set().unwrap_err().to_string();
        assert!(err.contains("lists no runs"), "got: {err}");

        // Without the marker the same snapshot is simply an old one.
        meta.format_version = 0;
        assert_eq!(meta.run_set().unwrap(), RunSet::SequencesOnly(3..=3));
    }

    #[tokio::test]
    async fn runs_survive_a_commit_round_trip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SnapshotCatalog::new(store, Path::from("warehouse/edges"));

        let mut meta = snap(2, 200);
        meta.runs = vec![run(1, 1, 1), run(0, 2, 2)];
        assert_eq!(
            catalog.commit(&meta).await.unwrap(),
            CommitOutcome::Committed
        );

        let read = catalog.get(2).await.unwrap();
        assert_eq!(read.runs, meta.runs);
        assert_eq!(read.run_set().unwrap(), RunSet::Runs(meta.runs));
    }

    #[tokio::test]
    async fn commit_latest_and_cas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SnapshotCatalog::new(store, Path::from("warehouse/edges"));

        assert!(catalog.latest().await.unwrap().is_none());
        assert_eq!(
            catalog.commit(&snap(1, 100)).await.unwrap(),
            CommitOutcome::Committed
        );
        // Same sequence from a rival worker loses.
        assert_eq!(
            catalog.commit(&snap(1, 100)).await.unwrap(),
            CommitOutcome::Conflict
        );
        assert_eq!(
            catalog.commit(&snap(2, 250)).await.unwrap(),
            CommitOutcome::Committed
        );

        let latest = catalog.latest().await.unwrap().unwrap();
        assert_eq!(latest.sequence, 2);
        assert_eq!(latest.watermark, 250);
    }

    /// The migration: a snapshot written before `position` existed reads as
    /// `{0: watermark}`. Not a guess — every such snapshot came from a worker
    /// that could only consume one stream, so partition 0 is where its offsets
    /// were, and the synthesised position is exactly what the scalar meant.
    #[test]
    fn a_legacy_scalar_reads_as_partition_zero() {
        let json = r#"{
            "sequence": 7, "committed_at_ms": 0, "watermark": 4242,
            "data_files": [], "puffin_path": "puffin/dsu-7.puffin",
            "committer": "old-binary"
        }"#;
        let snap: SnapshotMeta = serde_json::from_str(json).unwrap();
        assert!(snap.position.is_empty(), "nothing to deserialize into it");
        assert_eq!(snap.stream, None);

        let position = snap.stream_position();
        assert_eq!(position.get(0), 4242);
        assert_eq!(position.len(), 1);
        assert!(position.dominates(&StreamPosition::single(4242)));
    }

    /// A single-partition snapshot stays readable by a binary that predates
    /// `position`, because `watermark` carries the same number rather than an
    /// approximation of it. That is what keeps rollback available for the only
    /// topology an older binary could ever have served.
    #[test]
    fn a_single_partition_snapshot_still_writes_the_scalar() {
        let mut snap = snap(3, 0);
        snap.set_stream_position(StreamPosition::single(900));
        assert_eq!(snap.watermark, 900);
        assert_eq!(snap.format_version, 0, "no format bump for one partition");

        let round: SnapshotMeta =
            serde_json::from_slice(&serde_json::to_vec(&snap).unwrap()).unwrap();
        assert_eq!(round.stream_position(), StreamPosition::single(900));
    }

    /// And a multi-partition one does not pretend: there is no `u64` that
    /// describes it, so `watermark` goes to 0 and the format version names why.
    #[test]
    fn a_multi_partition_snapshot_refuses_to_invent_a_scalar() {
        let mut snap = snap(4, 0);
        snap.set_stream_position([(0, 10), (1, 20)].into_iter().collect());
        assert_eq!(snap.watermark, 0);
        assert_eq!(snap.format_version, FORMAT_STREAM_POSITION);

        let round: SnapshotMeta =
            serde_json::from_slice(&serde_json::to_vec(&snap).unwrap()).unwrap();
        assert_eq!(round.stream_position().get(1), 20);
        // The fallback must not fire when a real position is present, or the
        // multi-partition snapshot would silently read as `{0: 0}`.
        assert_eq!(round.stream_position().len(), 2);
    }

    /// Under a scalar this was `>=` and needed no argument. Under a partial
    /// order two successive positions can be incomparable, which means some
    /// partition went backwards — a second consumer committing to the same
    /// table. One error here beats double-written rows and undurable segments
    /// dropped from a follower's buffer.
    #[test]
    fn a_position_that_goes_backwards_is_rejected() {
        let mut first = snap(1, 0);
        first.set_stream_position([(0, 50), (1, 50)].into_iter().collect());

        let mut forward = snap(2, 0);
        forward.set_stream_position([(0, 60), (1, 50)].into_iter().collect());
        forward.advances_from(&first).unwrap();

        // Same total, redistributed: partition 1 moved backwards.
        let mut sideways = snap(2, 0);
        sideways.set_stream_position([(0, 60), (1, 40)].into_iter().collect());
        let err = sideways.advances_from(&first).unwrap_err().to_string();
        assert!(err.contains("backwards"), "{err}");

        // A partition appearing for the first time is not going backwards: it
        // was at 0 and now it is not.
        let mut grown = snap(2, 0);
        grown.set_stream_position([(0, 50), (1, 50), (2, 5)].into_iter().collect());
        grown.advances_from(&first).unwrap();
    }

    #[test]
    fn a_legacy_predecessor_still_constrains_its_successor() {
        let old = snap(1, 500); // scalar only
        let mut ahead = snap(2, 0);
        ahead.set_stream_position(StreamPosition::single(600));
        ahead.advances_from(&old).unwrap();

        let mut behind = snap(2, 0);
        behind.set_stream_position(StreamPosition::single(400));
        assert!(behind.advances_from(&old).is_err());
    }

    #[test]
    fn a_stream_identity_change_is_rejected() {
        let mut a = snap(1, 0);
        a.set_stream_position(StreamPosition::single(10));
        a.stream = Some(StreamId::new("kafka", "edges"));

        let mut b = snap(2, 0);
        b.set_stream_position(StreamPosition::single(20));
        b.stream = Some(StreamId::new("kafka", "edges-v2"));
        let err = b.advances_from(&a).unwrap_err().to_string();
        assert!(err.contains("meaningless in the other"), "{err}");

        // A consumer group change is not a stream change.
        let mut c = snap(2, 0);
        c.set_stream_position(StreamPosition::single(20));
        c.stream = Some(StreamId::new("kafka", "edges").with_group("blaze-b"));
        c.advances_from(&a).unwrap();

        // And a legacy predecessor with no identity cannot be checked against,
        // so it is not checked rather than being failed.
        let legacy = snap(1, 10);
        c.advances_from(&legacy).unwrap();
    }
}
