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
use crate::storage::filter::{BlockedFilter, overlay_key};
use crate::storage::puffin;

const PAIR_STRIDE: usize = 16;
const REGISTRY_STRIDE: usize = 12;
/// Page size the sparse index blocks against. Not required to match the
/// kernel's — it only decides how much of the mapping one narrowed search can
/// touch — but 4 KiB is the size that matters on the platforms we run on.
const PAGE_BYTES: usize = 4096;

/// In-RAM index over a sorted on-disk table: the first key of every
/// `PAGE_BYTES`-sized block of entries.
///
/// Without it, a lookup binary-searches the mapping itself. At 2B pairs that
/// is ~31 probes at up to ~16 GB strides, and the first ~26 land on distinct
/// cold pages — ~150-250 µs of page faults for one answer, nearly all of it
/// spent walking levels small enough to hold in RAM for free.
///
/// With it, the binary search runs over `first_keys` (a heap `Vec`, so no
/// faults) and narrows to one block before touching the mapping at all. A
/// block is `PAGE_BYTES` of *contiguous* bytes, so it spans 1-2 mapped pages
/// however the payload happens to be aligned inside the Puffin file.
///
/// Cost is one `u64` per block: `8/PAGE_BYTES × stride` of the table's own
/// size — 8 bytes per 4 KiB block, i.e. **0.2%** for every table regardless of
/// stride, so ~32 MB indexes a 16 GB shared tier. (Measured: 0.2 MB for a
/// 113 MB base.)
#[derive(Debug)]
struct SparseIndex {
    /// Entries per block.
    block: usize,
    first_keys: Vec<NodeId>,
}

impl SparseIndex {
    /// `None` when the whole table already fits inside one block, where
    /// narrowing would only add work.
    fn build(count: usize, stride: usize, key_at: impl Fn(usize) -> NodeId) -> Option<Self> {
        let block = PAGE_BYTES / stride;
        if count <= block {
            return None;
        }
        Some(Self {
            block,
            first_keys: (0..count).step_by(block).map(key_at).collect(),
        })
    }

    /// Entry range that can contain `key`, for a table with **unique** keys:
    /// the single block whose first key is the greatest one `<= key`. Empty
    /// when `key` precedes the table entirely.
    fn narrow_exact(&self, count: usize, key: NodeId) -> std::ops::Range<usize> {
        let after = self.first_keys.partition_point(|&k| k <= key);
        if after == 0 {
            return 0..0;
        }
        let b = after - 1;
        b * self.block..((b + 1) * self.block).min(count)
    }

    /// Entry range guaranteed to contain the lower bound of `key`, for a table
    /// whose keys may **repeat**. Spans at most two blocks: the run holding the
    /// first occurrence of `key` can start in the block before the first one
    /// whose leading key is `>= key`.
    fn narrow_lower_bound(&self, count: usize, key: NodeId) -> std::ops::Range<usize> {
        let b = self.first_keys.partition_point(|&k| k < key);
        let lo = b.saturating_sub(1);
        lo * self.block..((b + 1) * self.block).min(count)
    }
}

/// Byte range of a sorted `(u64 node, u64 root)` table inside the mapping.
#[derive(Debug)]
struct PairTable {
    /// Offset of the first entry (i.e. past the 8-byte count header).
    start: usize,
    count: usize,
    index: Option<SparseIndex>,
}

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
        let mut table = Self {
            start: range.start + 8,
            count,
            index: None,
        };
        // One key read per block, so open time stays O(count/256) — a strided
        // scan, not a deserialization.
        table.index = SparseIndex::build(count, PAIR_STRIDE, |i| table.key_at(data, i));
        Ok(table)
    }

    fn key_at(&self, data: &[u8], i: usize) -> NodeId {
        let at = self.start + i * PAIR_STRIDE;
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    fn value_at(&self, data: &[u8], i: usize) -> NodeId {
        let at = self.start + i * PAIR_STRIDE + 8;
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    /// Binary search the sorted keys, narrowed to one block by the sparse
    /// index so the mapping is touched over `PAGE_BYTES` of contiguous bytes:
    /// ~1 µs page-cached, one or two ~10-50 µs faults cold.
    fn lookup(&self, data: &[u8], node: NodeId) -> Option<NodeId> {
        let range = match &self.index {
            Some(ix) => ix.narrow_exact(self.count, node),
            None => 0..self.count,
        };
        let (mut lo, mut hi) = (range.start, range.end);
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

    /// Stream every pair in stored (ascending key) order: a sequential scan of
    /// the mapping with nothing collected, which is what keeps compaction at
    /// O(memtable) heap instead of O(state).
    fn for_each(&self, data: &[u8], f: &mut dyn FnMut(NodeId, NodeId)) {
        for i in 0..self.count {
            f(self.key_at(data, i), self.value_at(data, i));
        }
    }
}

/// Sorted `(u64 root, u32 scope)` index inside the mapping.
#[derive(Debug)]
struct RegistryTable {
    start: usize,
    count: usize,
    index: Option<SparseIndex>,
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
        let mut table = Self {
            start: range.start + 8,
            count,
            index: None,
        };
        table.index = SparseIndex::build(count, REGISTRY_STRIDE, |i| table.root_at(data, i));
        Ok(table)
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
    /// A root's scopes are contiguous but may spill past the narrowed range,
    /// so the scan is bounded by `count` rather than by the block.
    fn scopes_for(&self, data: &[u8], root: NodeId) -> ScopeList {
        let range = match &self.index {
            Some(ix) => ix.narrow_lower_bound(self.count, root),
            None => 0..self.count,
        };
        let (mut lo, mut hi) = (range.start, range.end);
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
    /// Membership filters over this layer's keys, read into the heap at open
    /// time. They must be resident to be worth anything — the point is to answer
    /// a miss from one cache line instead of a binary search over the mapping, so
    /// leaving them mmap'd would just trade a page fault for a page fault.
    ///
    /// `None` for layers written before filters existed; a lookup then falls back
    /// to searching, which is correct and merely slower.
    shared_filter: Option<BlockedFilter>,
    overlay_filter: Option<BlockedFilter>,
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
        // Lookups are point probes into a sorted table, so the kernel's
        // default readahead fetches pages a binary search will never visit —
        // pure I/O and page-cache waste at base sizes measured in tens of GB.
        if let Err(e) = mmap.advise(memmap2::Advice::Random) {
            debug!(error = %e, "MADV_RANDOM unavailable; readahead left at default");
        }
        Self::from_mmap(mmap)
    }

    fn from_mmap(mmap: Mmap) -> anyhow::Result<Self> {
        let index = puffin::read_index(&mmap)?;
        let mut shared = None;
        let mut overlays = BTreeMap::new();
        let mut registry = None;
        let mut shared_filter = None;
        let mut overlay_filter = None;
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
                codec::SHARED_FILTER_BLOB_TYPE => {
                    shared_filter = Some(BlockedFilter::decode(&mmap[blob.range()])?);
                }
                codec::OVERLAY_FILTER_BLOB_TYPE => {
                    overlay_filter = Some(BlockedFilter::decode(&mmap[blob.range()])?);
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
            shared_filter,
            overlay_filter,
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

    /// Sweep one table front-to-back, telling the kernel so for the duration.
    /// The mapping is advised `MADV_RANDOM` for point lookups, which is
    /// exactly wrong for compaction's full scan — and leaving it that way
    /// would cost one fault per page instead of one per readahead window.
    fn scan(&self, table: &PairTable, f: &mut dyn FnMut(NodeId, NodeId)) {
        let len = table.count * PAIR_STRIDE;
        if len > 0 {
            let _ = self
                .mmap
                .advise_range(memmap2::Advice::Sequential, table.start, len);
        }
        table.for_each(&self.mmap, f);
        if len > 0 {
            let _ = self
                .mmap
                .advise_range(memmap2::Advice::Random, table.start, len);
        }
    }

    /// Number of pairs in the shared table.
    pub fn shared_len(&self) -> usize {
        self.shared.as_ref().map(|t| t.count).unwrap_or(0)
    }

    /// `i`th shared pair in ascending key order. Positional access is what lets
    /// [`super::layered::LayeredBase`] k-way merge several layers without
    /// buffering any of them.
    pub fn shared_pair_at(&self, i: usize) -> (NodeId, NodeId) {
        let t = self.shared.as_ref().expect("shared table");
        (t.key_at(&self.mmap, i), t.value_at(&self.mmap, i))
    }

    pub fn overlay_len(&self, scope: ScopeId) -> usize {
        self.overlays.get(&scope).map(|t| t.count).unwrap_or(0)
    }

    pub fn overlay_pair_at(&self, scope: ScopeId, i: usize) -> (NodeId, NodeId) {
        let t = self.overlays.get(&scope).expect("overlay table");
        (t.key_at(&self.mmap, i), t.value_at(&self.mmap, i))
    }

    /// Entries in this layer's `root -> scope` registry blob.
    pub fn registry_len(&self) -> usize {
        self.registry.as_ref().map(|t| t.count).unwrap_or(0)
    }

    /// `i`th registry entry, in the blob's stored `(root, scope)` order.
    ///
    /// Ordered access matters: compaction merges the layers' registries as
    /// sorted runs rather than sorting a copy of them, which is what keeps it
    /// off an O(state) buffer.
    pub fn registry_at(&self, i: usize) -> (NodeId, ScopeId) {
        let t = self.registry.as_ref().expect("registry table");
        (t.root_at(&self.mmap, i), t.scope_at(&self.mmap, i))
    }

    /// Heap held by the sparse indexes and the membership filters — the parts of
    /// a layer that are *not* free to map, so they belong in stats rather than in
    /// the shadows.
    fn index_bytes(&self) -> u64 {
        self.table_index_bytes() + self.filter_bytes()
    }

    fn filter_bytes(&self) -> u64 {
        self.shared_filter
            .iter()
            .chain(self.overlay_filter.iter())
            .map(|f| f.heap_bytes() as u64)
            .sum()
    }

    fn table_index_bytes(&self) -> u64 {
        let pair = self
            .shared
            .iter()
            .chain(self.overlays.values())
            .filter_map(|t| t.index.as_ref());
        let registry = self.registry.as_ref().and_then(|r| r.index.as_ref());
        pair.chain(registry)
            .map(|ix| (ix.first_keys.len() * std::mem::size_of::<NodeId>()) as u64)
            .sum()
    }
}

impl RoutingBase for PuffinBase {
    fn shared_parent(&self, node: NodeId) -> Option<NodeId> {
        // A negative filter answer is definitive, and costs one cache line
        // instead of a narrowed binary search over the mapping.
        if let Some(f) = &self.shared_filter
            && !f.may_contain(node)
        {
            return None;
        }
        self.shared.as_ref()?.lookup(&self.mmap, node)
    }

    fn overlay_parent(&self, scope: ScopeId, node: NodeId) -> Option<NodeId> {
        if let Some(f) = &self.overlay_filter
            && !f.may_contain(overlay_key(scope, node))
        {
            return None;
        }
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

    fn for_each_shared_pair(&self, f: &mut dyn FnMut(NodeId, NodeId)) {
        if let Some(t) = &self.shared {
            self.scan(t, f);
        }
    }

    fn for_each_overlay_pair(&self, scope: ScopeId, f: &mut dyn FnMut(NodeId, NodeId)) {
        if let Some(t) = self.overlays.get(&scope) {
            self.scan(t, f);
        }
    }

    fn stats(&self) -> BaseStats {
        BaseStats {
            sequence: self.sequence,
            shared_pairs: self.shared.as_ref().map(|t| t.count as u64).unwrap_or(0),
            overlay_pairs: self.overlays.values().map(|t| t.count as u64).sum(),
            scopes: self.overlays.len() as u64,
            mapped_bytes: self.mmap.len() as u64,
            index_bytes: self.index_bytes(),
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
        // Streaming iteration yields every pair in ascending key order — the
        // ordering `ScopedForest::compact_into` merge-joins against.
        assert_eq!(collect_shared(&base), vec![(7, 3), (500, 105), (900, 105)]);
        assert_eq!(collect_overlay(&base, 7), vec![(40, 2), (105, 2)]);
        assert_eq!(collect_overlay(&base, 2999), vec![(9, 1)]);
        assert!(collect_overlay(&base, 4242).is_empty());
    }

    fn collect_shared(base: &PuffinBase) -> Vec<(NodeId, NodeId)> {
        let mut out = Vec::new();
        base.for_each_shared_pair(&mut |n, r| out.push((n, r)));
        out
    }

    fn collect_overlay(base: &PuffinBase, scope: ScopeId) -> Vec<(NodeId, NodeId)> {
        let mut out = Vec::new();
        base.for_each_overlay_pair(scope, &mut |n, r| out.push((n, r)));
        out
    }

    /// The sparse index must be an optimization only: over a table big enough
    /// to be blocked, every present key still resolves and every absent one
    /// still misses — including the keys that sit on block boundaries, which
    /// is where an off-by-one in the narrowing would hide.
    #[test]
    fn sparse_index_agrees_with_a_full_scan() {
        // 4000 pairs at a 16-byte stride is ~16 blocks of 256.
        const N: u64 = 4_000;
        let snap = ForestSnapshot {
            // Keys stride by 3 so two-thirds of the id space is absent.
            global: (1..=N).map(|i| (i * 3, 1)).collect(),
            scopes: vec![(
                7,
                (1..=N)
                    .map(|i| (i * 3, 2))
                    .collect::<Vec<(NodeId, NodeId)>>(),
            )],
        };
        let (_dir, base) = write_base(&snap, true);
        let table = base.shared.as_ref().unwrap();
        let index = table.index.as_ref().expect("table is big enough to block");
        assert_eq!(index.block, 256);
        assert_eq!(index.first_keys.len(), N as usize / 256 + 1);
        assert!(base.stats().index_bytes >= (N / 256) * 8);

        for i in 0..=(N * 3 + 4) {
            let expect = (i % 3 == 0 && i > 0 && i <= N * 3).then_some(1);
            assert_eq!(base.shared_parent(i), expect, "shared lookup of {i}");
        }
        // Block-boundary keys specifically (first and last pair of each block).
        for b in 0..index.first_keys.len() {
            for i in [b * 256, (b * 256 + 255).min(N as usize - 1)] {
                let key = table.key_at(&base.mmap, i);
                assert_eq!(base.shared_parent(key), Some(1), "boundary key {key}");
                assert_eq!(base.overlay_parent(7, key), Some(2));
            }
        }
    }

    /// The mechanical guarantee behind the latency claim: whatever the key, the
    /// narrowed search reads at most one `PAGE_BYTES` block of the mapping (two
    /// for the duplicate-key registry). Sub-millisecond cold lookups depend on
    /// this bound, and it cannot be observed on a probe whose base fits in page
    /// cache — so pin it here instead.
    #[test]
    fn narrowed_search_reads_at_most_one_block() {
        const N: u64 = 50_000;
        let snap = ForestSnapshot {
            global: (1..=N).map(|i| (i * 7, 1)).collect(),
            scopes: vec![],
        };
        let (_dir, base) = write_base(&snap, false);
        let table = base.shared.as_ref().unwrap();
        let index = table.index.as_ref().unwrap();

        let mut widest = 0usize;
        for key in (0..N * 7 + 20).step_by(13) {
            let r = index.narrow_exact(table.count, key);
            widest = widest.max(r.len());
        }
        assert!(
            widest * PAIR_STRIDE <= PAGE_BYTES,
            "narrowed range spans {} bytes, more than one {PAGE_BYTES}-byte block",
            widest * PAIR_STRIDE
        );
        // Unnarrowed, the same search would range over the whole 800 KB table
        // — ~17 probes on as many distinct pages.
        assert!(table.count * PAIR_STRIDE / PAGE_BYTES > 100);

        let reg = RegistryTable {
            start: table.start,
            count: table.count,
            index: SparseIndex::build(table.count, REGISTRY_STRIDE, |i| {
                table.key_at(&base.mmap, i)
            }),
        };
        let rix = reg.index.as_ref().unwrap();
        let mut widest = 0usize;
        for key in (0..N * 7 + 20).step_by(13) {
            widest = widest.max(rix.narrow_lower_bound(reg.count, key).len());
        }
        assert!(widest * REGISTRY_STRIDE <= 2 * PAGE_BYTES);
    }

    /// Registry roots repeat across scopes, so its runs can straddle blocks;
    /// the narrowed lower bound has to find the whole run either way.
    #[test]
    fn sparse_index_finds_registry_runs_across_blocks() {
        // 40 roots × 30 scopes = 1200 entries at a 12-byte stride: ~4 blocks
        // of 341, with every run of 30 far more likely to straddle than not.
        let scopes: Vec<ScopeId> = (1..=30).collect();
        let snap = ForestSnapshot {
            global: vec![],
            scopes: scopes
                .iter()
                .map(|&s| (s, (1..=40u64).map(|r| (r + 1000, r)).collect()))
                .collect(),
        };
        let (_dir, base) = write_base(&snap, true);
        let reg = base.registry.as_ref().unwrap();
        assert!(reg.index.is_some(), "registry is big enough to block");
        for root in 1..=40u64 {
            let mut got = base.scopes_for_root(root).to_vec();
            got.sort_unstable();
            assert_eq!(got, scopes, "root {root} lost scopes");
            let mut got = base.scopes_for_root(root + 1000).to_vec();
            got.sort_unstable();
            assert_eq!(got, scopes, "overlay member {} lost scopes", root + 1000);
        }
        assert!(base.scopes_for_root(999).is_empty());
        assert!(base.scopes_for_root(1041).is_empty());
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
