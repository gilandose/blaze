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
    /// Puffin sidecar carrying the DSU routing maps as of `watermark`.
    pub puffin_path: String,
    pub committer: String,
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
}

impl SnapshotCatalog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }

    fn snap_path(&self, sequence: u64) -> Path {
        // Zero-padded so lexicographic listing order == sequence order.
        self.prefix
            .clone()
            .join("metadata")
            .join(format!("snap-{sequence:012}.json"))
    }

    /// Latest committed snapshot, if any.
    pub async fn latest(&self) -> anyhow::Result<Option<SnapshotMeta>> {
        let prefix = self.prefix.clone().join("metadata");
        let mut newest: Option<Path> = None;
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            let name = meta.location.filename().unwrap_or_default();
            if name.starts_with("snap-")
                && name.ends_with(".json")
                && newest
                    .as_ref()
                    .map(|p| meta.location.as_ref() > p.as_ref())
                    .unwrap_or(true)
            {
                newest = Some(meta.location);
            }
        }
        match newest {
            None => Ok(None),
            Some(path) => {
                let bytes = self.store.get(&path).await?.bytes().await?;
                Ok(Some(serde_json::from_slice(&bytes)?))
            }
        }
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
            Ok(_) => Ok(CommitOutcome::Committed),
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
