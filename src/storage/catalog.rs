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
            base_sequence: seq,
            delta_chain_len: 0,
        }
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
