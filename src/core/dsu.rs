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
    /// Number of successful merges (components joined).
    merges: AtomicU64,
}

impl Dsu {
    pub fn new() -> Self {
        Self::default()
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
        self.parents.insert(child, parent);
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

    /// Fully-resolved `(node, root)` pairs for every non-root node.
    ///
    /// Keys are collected before resolving because `find` may write
    /// (path-halving) into the same DashMap shard an iterator would hold a
    /// read lock on.
    pub fn snapshot(&self) -> Vec<(NodeId, NodeId)> {
        let keys: Vec<NodeId> = self.parents.iter().map(|e| *e.key()).collect();
        keys.into_iter().map(|k| (k, self.find(k))).collect()
    }

    /// Rebuild state from `(node, root)` pairs produced by [`Dsu::snapshot`].
    /// Pairs are fully resolved, so hydrated trees are depth 1.
    pub fn hydrate(&self, pairs: &[(NodeId, NodeId)]) {
        for &(node, root) in pairs {
            if node != root {
                self.parents.insert(node, root);
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
