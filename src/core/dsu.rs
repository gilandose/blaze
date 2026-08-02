//! A concurrent disjoint-set-union (union-find) over sparse `u64` node ids.
//!
//! **Canonical roots**: unions always keep the *lowest node id* as the root,
//! so a component's representative is deterministic — the smallest graph id
//! it contains ("lowest graph id wins"). Merges can therefore only ever move
//! a component's id downward, which trends toward stable ids as graphs grow.
//! Arbitrary-direction linking plus path compression on the write path keeps
//! amortized costs at O(log n) worst case, O(α) in practice.
//!
//! **Two find flavors**:
//! - [`Dsu::find`] (write path): compresses via path-halving as it walks.
//!   Halving is safe to race because a node's parent is only ever replaced by
//!   another ancestor of that node.
//! - [`Dsu::find_ro`] (query path): read-only walk taking only DashMap read
//!   locks, so heavy query traffic does not contend with the single ingest
//!   writer on shard write locks. If it encounters a pathologically long
//!   chain it performs one repair write to cap future walks.
//!
//! Writes (`union`) are expected to be externally serialized by the caller
//! (see `ScopedForest`, which owns a single union lock across the global DSU
//! and all scope overlays).

use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};

use super::types::NodeId;

/// Read-only walks longer than this trigger a single compressing write; keeps
/// the query path effectively read-only while capping pathological chains.
const RO_COMPRESS_DEPTH: usize = 8;

/// Result of a union that actually merged two components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Merge {
    /// The root that was absorbed (no longer a root afterwards). Always the
    /// larger id of the two.
    pub child: NodeId,
    /// The surviving root: the smallest id in the merged component.
    pub parent: NodeId,
}

#[derive(Debug, Default)]
pub struct Dsu {
    /// `node -> parent`. A node with no entry is a root (or has never been
    /// seen, which is the same thing: a singleton component). Parents are
    /// always smaller... not necessarily — but every chain terminates at the
    /// component's minimum id.
    parents: DashMap<NodeId, NodeId>,
    /// The **merge edges** inverted: `parent -> roots absorbed into it`. `None`
    /// unless the member index is on, because it is only ever read by
    /// `members` and every deployment would otherwise pay for it.
    ///
    /// Deliberately *not* maintained as the inverse of `parents`. A true
    /// inverse would also be correct — path-halving and the read-repair only
    /// ever re-point a node at an ancestor, so nothing becomes unreachable from
    /// the root either way — but keeping it would mean a removal from the old
    /// parent's child list on every compressing write, including the one
    /// `find_ro` makes on the **query path**. That is a write where there is
    /// currently only a read, in the one place invariant I3 is about.
    ///
    /// Recording only [`Dsu::union`]'s link avoids it and is still complete: a
    /// non-root got there by being absorbed exactly once, so its merge edge is
    /// recorded, and the chain of merge edges above it terminates at the root.
    /// Compression moves the `parents` pointer; it does not un-merge anything.
    /// Append-only also means no duplicates and no removals at all.
    children: Option<DashMap<NodeId, SmallVec<[NodeId; 2]>>>,
    /// Number of successful merges (components joined).
    merges: AtomicU64,
}

impl Dsu {
    pub fn new() -> Self {
        Self::default()
    }

    /// A DSU that also records merge edges downward, so `members` can list a
    /// component. See [`Dsu::children`].
    pub fn tracking_members(track: bool) -> Self {
        Self {
            children: track.then(DashMap::new),
            ..Self::default()
        }
    }

    /// Whether downward walks are answerable here.
    pub fn tracks_members(&self) -> bool {
        self.children.is_some()
    }

    /// Roots absorbed directly into `parent`, appended to `out`.
    ///
    /// Never deduplicated against `out`: a root is absorbed exactly once, so
    /// this map cannot contain a duplicate, and the caller's visited set covers
    /// the case where two sources offer the same node.
    pub fn children_of(&self, parent: NodeId, out: &mut Vec<NodeId>) {
        if let Some(children) = &self.children
            && let Some(kids) = children.get(&parent)
        {
            out.extend_from_slice(&kids);
        }
    }

    /// Record `child -> parent` in both directions.
    fn link(&self, child: NodeId, parent: NodeId) {
        self.parents.insert(child, parent);
        if let Some(children) = &self.children {
            children.entry(parent).or_default().push(child);
        }
    }

    /// Current representative of `x`'s component — the lowest node id in it.
    /// Lock-free; applies path-halving so chains flatten on the write path.
    pub fn find(&self, mut x: NodeId) -> NodeId {
        loop {
            let p = match self.parents.get(&x) {
                Some(p) => *p,
                None => return x,
            };
            let gp = match self.parents.get(&p) {
                Some(gp) => *gp,
                None => return p,
            };
            // Path halving: point x at its grandparent. Racing writers only
            // ever store ancestors of x, so any interleaving stays correct.
            self.parents.insert(x, gp);
            x = gp;
        }
    }

    /// Read-only representative lookup for the query path: takes only shard
    /// read locks, so it cannot contend with the ingest writer. Long chains
    /// (possible under min-id linking before the write path re-touches them)
    /// get one repair write pointing the start node at its root.
    pub fn find_ro(&self, x: NodeId) -> NodeId {
        let mut cur = x;
        let mut depth = 0usize;
        while let Some(p) = self.parents.get(&cur).map(|r| *r) {
            cur = p;
            depth += 1;
        }
        if depth > RO_COMPRESS_DEPTH {
            // cur is an ancestor (the root) of x, so this is the same class
            // of racy-but-safe write as path halving.
            self.parents.insert(x, cur);
        }
        cur
    }

    /// Merge the components of `u` and `v`; the smaller root id survives.
    /// Returns the merge that happened, or `None` if already connected.
    /// Callers must serialize unions externally.
    pub fn union(&self, u: NodeId, v: NodeId) -> Option<Merge> {
        let ru = self.find(u);
        let rv = self.find(v);
        if ru == rv {
            return None;
        }
        // Lowest graph id wins: deterministic canonical representatives.
        let (child, parent) = if ru < rv { (rv, ru) } else { (ru, rv) };
        self.link(child, parent);
        self.merges.fetch_add(1, Ordering::Relaxed);
        Some(Merge { child, parent })
    }

    /// Whether `u` and `v` share a component (read-only walk).
    pub fn connected(&self, u: NodeId, v: NodeId) -> bool {
        self.find_ro(u) == self.find_ro(v)
    }

    /// Number of parent links, i.e. nodes that are not roots. Equals the
    /// total number of merges ever performed.
    pub fn len_links(&self) -> usize {
        self.parents.len()
    }

    pub fn merges(&self) -> u64 {
        self.merges.load(Ordering::Relaxed)
    }

    /// Every node with a parent link (i.e. every non-root this map knows).
    ///
    /// Collected eagerly: `find` may write (path-halving) into the same
    /// DashMap shard an iterator would hold a read lock on, so callers must
    /// not resolve while iterating.
    pub fn keys(&self) -> Vec<NodeId> {
        self.parents.iter().map(|e| *e.key()).collect()
    }

    /// Fully-resolved `(node, root)` pairs for every non-root node.
    pub fn snapshot(&self) -> Vec<(NodeId, NodeId)> {
        self.keys().into_iter().map(|k| (k, self.find(k))).collect()
    }

    /// Rebuild state from `(node, root)` pairs produced by [`Dsu::snapshot`].
    /// Pairs are fully resolved, so hydrated trees are depth 1.
    pub fn hydrate(&self, pairs: &[(NodeId, NodeId)]) {
        for &(node, root) in pairs {
            if node != root {
                // Hydrated pairs *are* the merge edges as far as this DSU is
                // concerned: the chains they came from were flattened on the
                // way out, so `node`'s recorded parent is its root and the
                // downward walk from that root reaches it in one hop.
                self.link(node, root);
            }
        }
        self.merges
            .store(self.parents.len() as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_find_basics() {
        let d = Dsu::new();
        assert_eq!(d.find(7), 7);
        assert!(!d.connected(1, 2));
        assert!(d.union(1, 2).is_some());
        assert!(d.connected(1, 2));
        assert!(d.union(1, 2).is_none());
        d.union(3, 4);
        assert!(!d.connected(2, 3));
        d.union(2, 3);
        assert!(d.connected(1, 4));
        assert_eq!(d.merges(), 3);
    }

    #[test]
    fn lowest_id_wins() {
        let d = Dsu::new();
        // Graph 500 routes to Graph 105: the smaller id is the survivor.
        let m = d.union(500, 105).unwrap();
        assert_eq!(m.parent, 105);
        assert_eq!(m.child, 500);
        assert_eq!(d.find(500), 105);
        assert_eq!(d.find_ro(500), 105);

        // Chained merges in descending order still resolve to the minimum.
        for k in (10..20u64).rev() {
            d.union(k, k + 1);
        }
        for k in 10..=20u64 {
            assert_eq!(d.find_ro(k), 10);
            assert_eq!(d.find(k), 10);
        }
        // Merging the two components: 10 absorbs the 105-rooted class? No —
        // 10 < 105, so 10 wins.
        let m = d.union(500, 15).unwrap();
        assert_eq!(m.parent, 10);
        assert_eq!(d.find_ro(105), 10);
    }

    #[test]
    fn find_ro_repairs_long_chains() {
        let d = Dsu::new();
        // Build a long chain by merging in descending id order; the write
        // path only compresses the endpoints it touches.
        for k in (0..200u64).rev() {
            d.union(k, k + 1);
        }
        assert_eq!(d.find_ro(200), 0);
        // The repair write must have shortcut node 200 directly to the root.
        assert_eq!(*d.parents.get(&200).unwrap(), 0);
    }

    fn children(d: &Dsu, parent: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        d.children_of(parent, &mut out);
        out.sort_unstable();
        out
    }

    /// Compression rearranges `parents` underneath the index, and the index has
    /// to keep answering.
    ///
    /// Merging in descending order builds a chain 200 -> 199 -> ... -> 0, and
    /// `find_ro` then repairs 200 to point straight at 0. The structural
    /// assertions pin the representation this DSU actually keeps — merge edges,
    /// so 200 stays 199's child — and the walk at the end is the property that
    /// has to hold under *any* representation: nobody is lost.
    #[test]
    fn compression_does_not_hide_a_member() {
        let d = Dsu::tracking_members(true);
        for k in (0..200u64).rev() {
            d.union(k, k + 1);
        }
        assert_eq!(d.find_ro(200), 0);
        assert_eq!(
            *d.parents.get(&200).unwrap(),
            0,
            "the repair write happened"
        );
        assert_eq!(
            children(&d, 0),
            vec![1],
            "the merge edge is what is indexed"
        );
        assert_eq!(children(&d, 199), vec![200], "and 200 is still 199's child");

        // Walk the whole thing down and check nobody was lost.
        let mut seen = vec![0u64];
        let mut frontier = vec![0u64];
        while let Some(k) = frontier.pop() {
            let kids = children(&d, k);
            seen.extend(&kids);
            frontier.extend(&kids);
        }
        seen.sort_unstable();
        assert_eq!(seen, (0..=200u64).collect::<Vec<_>>());
    }

    #[test]
    fn tracking_is_opt_in() {
        let d = Dsu::new();
        d.union(500, 105);
        assert!(!d.tracks_members());
        assert!(children(&d, 105).is_empty(), "no index, no children");
        assert_eq!(d.find(500), 105, "and resolution is unaffected");
    }

    /// Hydration is the other way state arrives, and a forest restored from a
    /// snapshot has to answer members exactly as the one that wrote it did.
    #[test]
    fn hydrate_records_the_index_too() {
        let src = Dsu::new();
        for i in 0..50u64 {
            src.union(i, i + 1);
        }
        let fresh = Dsu::tracking_members(true);
        fresh.hydrate(&src.snapshot());
        assert_eq!(children(&fresh, 0), (1..=50u64).collect::<Vec<_>>());
    }

    #[test]
    fn snapshot_roundtrip() {
        let d = Dsu::new();
        for i in 0..100u64 {
            d.union(i, i + 1);
        }
        d.union(500, 105);
        let snap = d.snapshot();

        // Snapshots are canonical: every stored root is the component min.
        for &(_, r) in &snap {
            assert!(r == 0 || r == 105);
        }

        let fresh = Dsu::new();
        fresh.hydrate(&snap);
        assert!(fresh.connected(0, 100));
        assert!(fresh.connected(500, 105));
        assert!(!fresh.connected(0, 500));
        for &(n, r) in &snap {
            assert_eq!(fresh.find(n), fresh.find(r));
        }
    }
}
