//! Memory-mapped routing base: serve committed DSU state from a Puffin file
//! on local disk instead of the heap.
//!
//! The Puffin payloads the flusher already writes are sorted, fixed-stride
//! `(node, root)` tables, which makes them directly usable as an on-disk
//! index: `mmap` the file once, then answer a lookup with a binary search
//! over a byte range. Nothing is deserialized at load time, so opening a
//! multi-gigabyte base is O(number of blobs), not O(pairs) — that is what
//! turns a ~10 minute hydration into a few milliseconds.
//!
//! Durability is unchanged (invariant I4): the local file is only ever a
//! read-through cache of an object-storage snapshot, and the mapping is
//! read-only.

use memmap2::Mmap;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::Path;
use tracing::{debug, warn};

use crate::core::base::{BaseStats, RoutingBase, ScopeList};
use crate::core::{NodeId, ScopeId};
use crate::storage::codec;
use crate::storage::puffin;

/// Byte range of a sorted `(u64 node, u64 root)` table inside the mapping.
#[derive(Debug, Clone, Copy)]
struct PairTable {
    /// Offset of the first entry (i.e. past the 8-byte count header).
    start: usize,
    count: usize,
}

const PAIR_STRIDE: usize = 16;
const REGISTRY_STRIDE: usize = 12;

impl PairTable {
    fn parse(data: &[u8], range: std::ops::Range<usize>) -> anyhow::Result<Self> {
        let payload = &data[range.clone()];
        anyhow::ensure!(payload.len() >= 8, "pair table truncated (no header)");
        let count = u64::from_le_bytes(payload[..8].try_into().unwrap()) as usize;
        anyhow::ensure!(
            payload.len() == 8 + count * PAIR_STRIDE,
            "pair table length mismatch: header says {count} pairs, payload has {} bytes",
            payload.len() - 8
        );
        Ok(Self {
            start: range.start + 8,
            count,
        })
    }

    fn key_at(&self, data: &[u8], i: usize) -> NodeId {
        let at = self.start + i * PAIR_STRIDE;
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    fn value_at(&self, data: &[u8], i: usize) -> NodeId {
        let at = self.start + i * PAIR_STRIDE + 8;
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    /// Binary search the sorted keys. Touches ~log2(count) pages; sequential
    /// reads of a page-cached base cost ~1µs, a cold NVMe page ~10-50µs.
    fn lookup(&self, data: &[u8], node: NodeId) -> Option<NodeId> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(data, mid).cmp(&node) {
                std::cmp::Ordering::Equal => return Some(self.value_at(data, mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    fn keys(&self, data: &[u8]) -> Vec<NodeId> {
        (0..self.count).map(|i| self.key_at(data, i)).collect()
    }
}

/// Sorted `(u64 root, u32 scope)` index inside the mapping.
#[derive(Debug, Clone, Copy)]
struct RegistryTable {
    start: usize,
    count: usize,
}

impl RegistryTable {
    fn parse(data: &[u8], range: std::ops::Range<usize>) -> anyhow::Result<Self> {
        let payload = &data[range.clone()];
        anyhow::ensure!(payload.len() >= 8, "registry table truncated (no header)");
        let count = u64::from_le_bytes(payload[..8].try_into().unwrap()) as usize;
        anyhow::ensure!(
            payload.len() == 8 + count * REGISTRY_STRIDE,
            "registry table length mismatch: header says {count} entries, payload has {} bytes",
            payload.len() - 8
        );
        Ok(Self {
            start: range.start + 8,
            count,
        })
    }

    fn root_at(&self, data: &[u8], i: usize) -> NodeId {
        let at = self.start + i * REGISTRY_STRIDE;
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    fn scope_at(&self, data: &[u8], i: usize) -> ScopeId {
        let at = self.start + i * REGISTRY_STRIDE + 8;
        u32::from_le_bytes(data[at..at + 4].try_into().unwrap())
    }

    /// All scopes recorded for `root`: lower-bound search, then scan the run.
    fn scopes_for(&self, data: &[u8], root: NodeId) -> ScopeList {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.root_at(data, mid) < root {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut out = ScopeList::new();
        let mut i = lo;
        while i < self.count && self.root_at(data, i) == root {
            out.push(self.scope_at(data, i));
            i += 1;
        }
        out
    }
}

/// A [`RoutingBase`] served from an mmap'd Puffin file.
#[derive(Debug)]
pub struct PuffinBase {
    mmap: Mmap,
    shared: Option<PairTable>,
    overlays: BTreeMap<ScopeId, PairTable>,
    registry: Option<RegistryTable>,
    /// Populated only when the file predates the registry blob: rebuilt at
    /// load time so runtime lookups stay O(1)-ish either way.
    fallback_registry: Option<HashMap<NodeId, ScopeList>>,
    sequence: u64,
}

impl PuffinBase {
    /// Map a Puffin routing snapshot from local disk.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)
            .map_err(|e| anyhow::anyhow!("open routing base {}: {e}", path.display()))?;
        // SAFETY: the mapping is read-only and the file is an immutable
        // snapshot artifact — the flusher only ever creates new paths, and
        // the local cache replaces files by atomic rename.
        let mmap = unsafe { Mmap::map(&file)? };
        Self::from_mmap(mmap)
    }

    fn from_mmap(mmap: Mmap) -> anyhow::Result<Self> {
        let index = puffin::read_index(&mmap)?;
        let mut shared = None;
        let mut overlays = BTreeMap::new();
        let mut registry = None;
        let mut sequence = 0u64;
        for blob in &index {
            sequence = sequence.max(blob.sequence_number.max(0) as u64);
            match blob.blob_type.as_str() {
                codec::GLOBAL_BLOB_TYPE => {
                    shared = Some(PairTable::parse(&mmap, blob.range())?);
                }
                codec::SCOPE_BLOB_TYPE => {
                    let scope: ScopeId = blob
                        .properties
                        .get(codec::SCOPE_ID_PROP)
                        .ok_or_else(|| {
                            anyhow::anyhow!("scope blob missing {}", codec::SCOPE_ID_PROP)
                        })?
                        .parse()?;
                    overlays.insert(scope, PairTable::parse(&mmap, blob.range())?);
                }
                codec::REGISTRY_BLOB_TYPE => {
                    registry = Some(RegistryTable::parse(&mmap, blob.range())?);
                }
                // Unknown blob types are ignored (forward compatibility).
                _ => {}
            }
        }

        let mut base = Self {
            mmap,
            shared,
            overlays,
            registry,
            fallback_registry: None,
            sequence,
        };
        if base.registry.is_none() && !base.overlays.is_empty() {
            warn!(
                "routing base has no {} blob; rebuilding the registry in memory",
                codec::REGISTRY_BLOB_TYPE
            );
            base.fallback_registry = Some(base.build_fallback_registry());
        }
        let stats = base.stats();
        debug!(
            sequence = stats.sequence,
            shared_pairs = stats.shared_pairs,
            overlay_pairs = stats.overlay_pairs,
            scopes = stats.scopes,
            mapped_mb = stats.mapped_bytes / (1024 * 1024),
            registry_indexed = stats.registry_indexed,
            "mapped routing base"
        );
        Ok(base)
    }

    /// Rebuild `root -> scopes` by scanning overlay members and resolving each
    /// through the shared table (mirrors `codec::registry_from_snapshot`).
    fn build_fallback_registry(&self) -> HashMap<NodeId, ScopeList> {
        let mut out: HashMap<NodeId, ScopeList> = HashMap::new();
        for (&scope, table) in &self.overlays {
            for i in 0..table.count {
                let node = table.key_at(&self.mmap, i);
                let root = table.value_at(&self.mmap, i);
                for member in [node, root] {
                    let live = self.shared_parent(member).unwrap_or(member);
                    let entry = out.entry(live).or_default();
                    if !entry.contains(&scope) {
                        entry.push(scope);
                    }
                }
            }
        }
        out
    }
}

impl RoutingBase for PuffinBase {
    fn shared_parent(&self, node: NodeId) -> Option<NodeId> {
        self.shared.as_ref()?.lookup(&self.mmap, node)
    }

    fn overlay_parent(&self, scope: ScopeId, node: NodeId) -> Option<NodeId> {
        self.overlays.get(&scope)?.lookup(&self.mmap, node)
    }

    fn scopes_for_root(&self, root: NodeId) -> ScopeList {
        if let Some(reg) = &self.registry {
            return reg.scopes_for(&self.mmap, root);
        }
        self.fallback_registry
            .as_ref()
            .and_then(|m| m.get(&root).cloned())
            .unwrap_or_default()
    }

    fn scopes(&self) -> Vec<ScopeId> {
        self.overlays.keys().copied().collect()
    }

    fn shared_nodes(&self) -> Vec<NodeId> {
        self.shared
            .as_ref()
            .map(|t| t.keys(&self.mmap))
            .unwrap_or_default()
    }

    fn overlay_nodes(&self, scope: ScopeId) -> Vec<NodeId> {
        self.overlays
            .get(&scope)
            .map(|t| t.keys(&self.mmap))
            .unwrap_or_default()
    }

    fn stats(&self) -> BaseStats {
        BaseStats {
            sequence: self.sequence,
            shared_pairs: self.shared.map(|t| t.count as u64).unwrap_or(0),
            overlay_pairs: self.overlays.values().map(|t| t.count as u64).sum(),
            scopes: self.overlays.len() as u64,
            mapped_bytes: self.mmap.len() as u64,
            registry_indexed: self.registry.is_some(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ForestSnapshot;
    use std::collections::BTreeMap as Map;
    use std::io::Write;

    fn write_base(snap: &ForestSnapshot, with_registry: bool) -> (tempfile::TempDir, PuffinBase) {
        let mut blobs = codec::snapshot_to_blobs(snap, 7);
        if !with_registry {
            blobs.retain(|b| b.blob_type != codec::REGISTRY_BLOB_TYPE);
        }
        let bytes = puffin::write(&blobs, Map::new());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("base.puffin");
        let mut f = File::create(&path).unwrap();
        f.write_all(&bytes).unwrap();
        f.sync_all().unwrap();
        let base = PuffinBase::open(&path).unwrap();
        (dir, base)
    }

    fn sample() -> ForestSnapshot {
        ForestSnapshot {
            global: vec![(500, 105), (900, 105), (7, 3)],
            scopes: vec![(7, vec![(105, 2), (40, 2)]), (2999, vec![(9, 1)])],
        }
    }

    #[test]
    fn lookups_and_iteration() {
        let (_dir, base) = write_base(&sample(), true);
        let stats = base.stats();
        assert_eq!(stats.sequence, 7);
        assert_eq!(stats.shared_pairs, 3);
        assert_eq!(stats.overlay_pairs, 3);
        assert_eq!(stats.scopes, 2);
        assert!(stats.registry_indexed);

        // Shared tier: stored nodes resolve, roots and unknowns do not.
        assert_eq!(base.shared_parent(500), Some(105));
        assert_eq!(base.shared_parent(900), Some(105));
        assert_eq!(base.shared_parent(105), None);
        assert_eq!(base.shared_parent(123_456), None);

        // Overlays are per scope and never bleed across tenants.
        assert_eq!(base.overlay_parent(7, 105), Some(2));
        assert_eq!(base.overlay_parent(7, 9), None);
        assert_eq!(base.overlay_parent(2999, 9), Some(1));
        assert_eq!(base.overlay_parent(4242, 105), None);

        assert_eq!(base.scopes(), vec![7, 2999]);
        let mut shared = base.shared_nodes();
        shared.sort_unstable();
        assert_eq!(shared, vec![7, 500, 900]);
        let mut ov = base.overlay_nodes(7);
        ov.sort_unstable();
        assert_eq!(ov, vec![40, 105]);
    }

    #[test]
    fn registry_index_resolves_live_roots() {
        let (_dir, base) = write_base(&sample(), true);
        // Overlay member 105 is a live shared root referenced by scope 7.
        assert_eq!(base.scopes_for_root(105).as_slice(), &[7]);
        // Member 9 of scope 2999 resolves to itself (not in the shared map).
        assert_eq!(base.scopes_for_root(9).as_slice(), &[2999]);
        // Member 1 likewise.
        assert_eq!(base.scopes_for_root(1).as_slice(), &[2999]);
        assert!(base.scopes_for_root(999_999).is_empty());
    }

    #[test]
    fn registry_falls_back_when_blob_absent() {
        let (_dir, base) = write_base(&sample(), false);
        assert!(!base.stats().registry_indexed);
        // Same answers as the indexed path.
        assert_eq!(base.scopes_for_root(105).as_slice(), &[7]);
        assert_eq!(base.scopes_for_root(2).as_slice(), &[7]);
        assert_eq!(base.scopes_for_root(9).as_slice(), &[2999]);
    }

    #[test]
    fn rejects_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.puffin");
        std::fs::write(&path, b"not a puffin file at all").unwrap();
        assert!(PuffinBase::open(&path).is_err());
    }
}
