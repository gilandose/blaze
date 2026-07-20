//! A concurrent disjoint-set-union (union-find) over sparse `u64` node ids.
//!
//! Reads (`find`) are lock-free: they walk parent pointers stored in a
//! [`DashMap`] and apply path-halving as they go. Halving writes are safe to
//! race because a node's parent is only ever replaced by another ancestor of
//! that node, which preserves set membership.
//!
//! Writes (`union`) are expected to be externally serialized by the caller
//! (see `ScopedForest`, which owns a single union lock across the global DSU
//! and all scope overlays). This keeps the hot query path wait-free while the
//! mutation path — bounded by ingest throughput, not query traffic — pays one
//! uncontended mutex.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::types::NodeId;

/// Result of a union that actually merged two components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Merge {
    /// The root that was absorbed (no longer a root afterwards).
    pub child: NodeId,
    /// The surviving root.
    pub parent: NodeId,
}

#[derive(Debug, Default)]
pub struct Dsu {
    /// `node -> parent`. A node with no entry is a root (or has never been
    /// seen, which is the same thing: a singleton component).
    parents: DashMap<NodeId, NodeId>,
    /// Approximate union-by-rank bookkeeping; only correctness-neutral
    /// balance depends on it.
    ranks: DashMap<NodeId, u8>,
    /// Number of successful merges (components joined).
    merges: AtomicU64,
}

impl Dsu {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current representative of `x`'s component. Lock-free; applies
    /// path-halving so hot chains flatten under read load.
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

    /// Merge the components of `u` and `v`. Returns the merge that happened,
    /// or `None` if they were already connected. Callers must serialize
    /// unions externally.
    pub fn union(&self, u: NodeId, v: NodeId) -> Option<Merge> {
        let ru = self.find(u);
        let rv = self.find(v);
        if ru == rv {
            return None;
        }
        let rank_u = self.ranks.get(&ru).map(|r| *r).unwrap_or(0);
        let rank_v = self.ranks.get(&rv).map(|r| *r).unwrap_or(0);
        let (child, parent) = if rank_u < rank_v { (ru, rv) } else { (rv, ru) };
        self.parents.insert(child, parent);
        if rank_u == rank_v {
            *self.ranks.entry(parent).or_insert(0) += 1;
        }
        self.merges.fetch_add(1, Ordering::Relaxed);
        Some(Merge { child, parent })
    }

    /// Whether `u` and `v` share a component.
    pub fn connected(&self, u: NodeId, v: NodeId) -> bool {
        self.find(u) == self.find(v)
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
    pub fn hydrate(&self, pairs: &[(NodeId, NodeId)]) {
        for &(node, root) in pairs {
            if node != root {
                self.parents.insert(node, root);
                // Give hydrated roots a nonzero rank so future unions prefer
                // them as parents of fresh singletons.
                self.ranks.entry(root).or_insert(1);
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
    fn snapshot_roundtrip() {
        let d = Dsu::new();
        for i in 0..100u64 {
            d.union(i, i + 1);
        }
        d.union(500, 105);
        let snap = d.snapshot();

        let fresh = Dsu::new();
        fresh.hydrate(&snap);
        assert!(fresh.connected(0, 100));
        assert!(fresh.connected(500, 105));
        assert!(!fresh.connected(0, 500));
        // Hydrated pairs are fully resolved: depth 1 everywhere.
        for &(n, r) in &snap {
            assert_eq!(fresh.find(n), fresh.find(r));
        }
    }
}
