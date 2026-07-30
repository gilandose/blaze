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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileMeta {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub min_offset: u64,
    pub max_offset: u64,
}

/// One committed micro-batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub sequence: u64,
    pub committed_at_ms: i64,
    /// Highest event offset covered by this snapshot (inclusive).
    pub watermark: u64,
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
}

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
    pub fn run_set(&self) -> RunSet {
        if !self.runs.is_empty() {
            let mut runs = self.runs.clone();
            runs.sort_by_key(|r| r.min_sequence);
            return RunSet::Runs(runs);
        }
        RunSet::SequencesOnly(self.chain())
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

    fn snap(seq: u64, watermark: u64) -> SnapshotMeta {
        SnapshotMeta {
            sequence: seq,
            committed_at_ms: 0,
            watermark,
            data_files: vec![],
            puffin_path: format!("puffin/dsu-{seq}.puffin"),
            committer: "test".into(),
            runs: Vec::new(),
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
        assert_eq!(meta.run_set(), RunSet::SequencesOnly(4..=7));
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
        assert_eq!(meta.run_set(), RunSet::SequencesOnly(3..=3));
    }

    /// Resolution order is by span, not by the order the runs happen to be
    /// listed in — a detached merge appends a run covering an *earlier* span
    /// than runs already in the list, so the sort is load-bearing, not cosmetic.
    #[test]
    fn runs_resolve_oldest_span_first_however_they_were_listed() {
        let mut meta = snap(9, 900);
        meta.runs = vec![run(0, 9, 9), run(2, 1, 6), run(1, 7, 8)];

        let RunSet::Runs(runs) = meta.run_set() else {
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
        assert_eq!(read.run_set(), RunSet::Runs(meta.runs));
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
}
