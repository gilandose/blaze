//! The read-only **routing base**: committed DSU state that lives outside the
//! process heap (typically an mmap'd Puffin file on local NVMe).
//!
//! The base is an immutable snapshot; everything applied after it lives in the
//! in-memory **memtable** (the `Dsu` maps inside `ScopedForest`). Composition
//! is the same trick the engine already uses twice — scope overlay over shared
//! tier, delta over base — applied across the RAM/disk boundary:
//!
//! ```text
//! composed_root(x) = memtable.find(base_root(x) or x)
//! ```
//!
//! # The invariant that makes it exact
//!
//! **Every mutation resolves its operands through the composed path before
//! touching the memtable.** Consequently a memtable key is always a *composed
//! root* — a node the base does not store a parent for. That is what lets a
//! lookup stop after one base probe plus one memtable walk (no re-probing),
//! and it is why a node with a memtable parent can skip the base probe
//! entirely. `ScopedForest` upholds this in `apply_*`, in merge fix-ups, and
//! in `snapshot`.
//!
//! Since roots only ever decrease (canonical lowest-id-wins), the base answer
//! is a valid *earlier* representative and the memtable walk refines it
//! downward — never the reverse.

use serde::Serialize;
use smallvec::SmallVec;

use super::types::{NodeId, ScopeId};

/// Scopes referencing a given shared root; small in practice (most roots are
/// referenced by one or two tenants).
pub type ScopeList = SmallVec<[ScopeId; 4]>;

/// A committed, immutable routing snapshot that can be consulted without
/// holding it in the heap.
///
/// All methods must be safe to call concurrently from query threads and take
/// no lock shared with the ingest writer (invariant I3).
pub trait RoutingBase: Send + Sync + std::fmt::Debug {
    /// Parent recorded for `node` in the shared tier, or `None` when the base
    /// stores none (i.e. `node` was a root, or unknown, as of the base).
    ///
    /// Snapshots are fully resolved, so the value is `node`'s shared root as
    /// of the base — one probe, no chain to walk.
    fn shared_parent(&self, node: NodeId) -> Option<NodeId>;

    /// Same, within `scope`'s overlay.
    fn overlay_parent(&self, scope: ScopeId, node: NodeId) -> Option<NodeId>;

    /// Scopes that held overlay state keyed on shared root `root` as of the
    /// base. Drives merge notifications for roots the memtable has never
    /// seen.
    fn scopes_for_root(&self, root: NodeId) -> ScopeList;

    /// Scopes with any overlay state in the base.
    fn scopes(&self) -> Vec<ScopeId>;

    /// Visit every `(node, parent)` pair in the shared tier, in **ascending
    /// node order**. Used by compaction to re-emit a full snapshot; not on any
    /// query path.
    ///
    /// Streaming, not collecting, is load-bearing: compaction runs under the
    /// union lock, and at 2B links a `Vec<NodeId>` of keys would be a ~16 GB
    /// allocation that stalls ingest. Ascending order lets the caller
    /// merge-join against its (much smaller) memtable key set in one pass and
    /// emit already-sorted output.
    fn for_each_shared_pair(&self, f: &mut dyn FnMut(NodeId, NodeId));

    /// Same, for `scope`'s overlay: ascending node order, no allocation.
    fn for_each_overlay_pair(&self, scope: ScopeId, f: &mut dyn FnMut(NodeId, NodeId));

    /// Whether this base can answer downward queries at all.
    ///
    /// Default `false`: a base written without the member index says so rather
    /// than reporting every component as childless, which would look like a
    /// correct answer for a singleton.
    fn has_member_index(&self) -> bool {
        false
    }

    /// At most `limit` nodes whose recorded shared parent is `parent`, appended
    /// to `out`.
    ///
    /// **The limit is load-bearing, not an optimisation.** In a flattened run a
    /// component's root has every member as a direct child, so an unbounded
    /// fetch makes a capped query cost O(component) instead of O(cap) — measured
    /// at 1.5 ms for `cap = 1000` past percolation before this existed.
    /// Implementations may return fewer, never more.
    fn shared_children(&self, _parent: NodeId, _limit: usize, _out: &mut Vec<NodeId>) {}

    /// Same within `scope`'s overlay.
    fn overlay_children(
        &self,
        _scope: ScopeId,
        _parent: NodeId,
        _limit: usize,
        _out: &mut Vec<NodeId>,
    ) {
    }

    fn stats(&self) -> BaseStats;
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BaseStats {
    /// Catalog sequence this base was compacted from.
    pub sequence: u64,
    pub shared_pairs: u64,
    pub overlay_pairs: u64,
    pub scopes: u64,
    /// Bytes mapped (file size), not resident bytes.
    pub mapped_bytes: u64,
    /// Heap held by in-RAM lookup indexes over the mapping. Small and
    /// proportional to `mapped_bytes` (~0.2%), but not zero — the one part of
    /// a disk-backed base that still scales with state.
    pub index_bytes: u64,
    /// True when the base carried a precomputed registry blob; false means
    /// the registry was rebuilt in memory at load time.
    pub registry_indexed: bool,
}
