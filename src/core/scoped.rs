//! Multi-tenant connectivity: one global DSU plus per-scope overlay DSUs.
//!
//! # The scoping problem
//!
//! A scope `s` sees the union of the global edge set and its own edge set, so
//! its connectivity is defined over `G_global ∪ G_s`. With thousands of
//! scopes we can afford neither a full DSU copy per scope (a global edge
//! would fan out to every copy) nor recomputing closures at query time.
//!
//! # Layered DSU with merge notifications
//!
//! - The **global DSU** holds only globally-visible edges.
//! - Each scope holds a sparse **overlay DSU** whose elements are node ids
//!   that were *global roots* at the time a scope edge was applied.
//! - A **registry** maps `global root -> set of scopes holding overlay state
//!   keyed by that root`. When a global union absorbs root `B` into root `A`,
//!   only the scopes registered on `B` receive a fix-up
//!   (`overlay.union(B, A)`), which records that the two overlay elements now
//!   denote the same global component. No 3000-way broadcast: the cost is
//!   proportional to the number of scopes that actually reference the
//!   absorbed root.
//!
//! Queries then compose the two layers:
//! `scope_root(s, x) = overlay(s).find(global.find(x))`, falling back to the
//! global root when the scope has no overlay state for it.
//!
//! Unions (from the single ingest pipeline) are serialized behind one mutex;
//! finds — the sub-millisecond API path — never take it.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use super::dsu::Dsu;
use super::types::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, Visibility};

#[derive(Debug, Default)]
pub struct ScopedForest {
    global: Dsu,
    overlays: DashMap<ScopeId, Dsu>,
    /// global root -> scopes with overlay state keyed by that root.
    registry: DashMap<NodeId, HashSet<ScopeId>>,
    /// Serializes all mutations (global unions, overlay unions, fix-ups).
    union_lock: Mutex<()>,
    events_applied: AtomicU64,
    scope_links: AtomicU64,
    fixups: AtomicU64,
}

/// A consistent point-in-time capture of the whole forest, suitable for
/// Puffin serialization and startup hydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForestSnapshot {
    pub global: Vec<(NodeId, NodeId)>,
    pub scopes: Vec<(ScopeId, Vec<(NodeId, NodeId)>)>,
}

impl ScopedForest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one edge event to in-memory topology.
    pub fn apply(&self, event: &EdgeEvent) {
        match event.visibility.clone().normalize() {
            Visibility::Global => self.apply_global(event.src, event.dst),
            Visibility::Scoped(scopes) => self.apply_scoped(event.src, event.dst, &scopes),
        }
        self.events_applied.fetch_add(1, Ordering::Relaxed);
    }

    fn apply_global(&self, u: NodeId, v: NodeId) {
        let _g = self.union_lock.lock();
        if let Some(merge) = self.global.union(u, v) {
            // Root `merge.child` no longer exists globally; tell every scope
            // that keyed overlay state on it.
            if let Some((_, scopes)) = self.registry.remove(&merge.child) {
                for scope in &scopes {
                    if let Some(overlay) = self.overlays.get(scope) {
                        overlay.union(merge.child, merge.parent);
                        self.fixups.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.registry
                    .entry(merge.parent)
                    .or_default()
                    .extend(scopes);
            }
        }
    }

    fn apply_scoped(&self, u: NodeId, v: NodeId, scopes: &[ScopeId]) {
        let _g = self.union_lock.lock();
        let ru = self.global.find(u);
        let rv = self.global.find(v);
        for &scope in scopes {
            let overlay = self.overlays.entry(scope).or_default();
            overlay.union(ru, rv);
            drop(overlay);
            self.registry.entry(ru).or_default().insert(scope);
            self.registry.entry(rv).or_default().insert(scope);
            self.scope_links.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Component representative of `node` as seen by `scope`. Lock-free.
    pub fn scope_root(&self, scope: ScopeId, node: NodeId) -> NodeId {
        let g = self.global.find(node);
        if scope == GLOBAL_SCOPE {
            return g;
        }
        match self.overlays.get(&scope) {
            Some(overlay) => overlay.find(g),
            None => g,
        }
    }

    /// Whether `u` and `v` are connected in `scope`'s view. Lock-free.
    pub fn connected(&self, scope: ScopeId, u: NodeId, v: NodeId) -> bool {
        self.scope_root(scope, u) == self.scope_root(scope, v)
    }

    pub fn stats(&self) -> ForestStats {
        ForestStats {
            events_applied: self.events_applied.load(Ordering::Relaxed),
            global_merges: self.global.merges(),
            global_links: self.global.len_links() as u64,
            scope_links: self.scope_links.load(Ordering::Relaxed),
            active_scopes: self.overlays.len() as u64,
            merge_fixups: self.fixups.load(Ordering::Relaxed),
        }
    }

    /// Capture a consistent snapshot. Takes the union lock so no merge or
    /// fix-up is half-applied in the captured state; queries keep running.
    pub fn snapshot(&self) -> ForestSnapshot {
        let _g = self.union_lock.lock();
        let global = self.global.snapshot();
        let scope_ids: Vec<ScopeId> = self.overlays.iter().map(|e| *e.key()).collect();
        let mut scopes = Vec::with_capacity(scope_ids.len());
        for scope in scope_ids {
            if let Some(overlay) = self.overlays.get(&scope) {
                let pairs = overlay.snapshot();
                if !pairs.is_empty() {
                    scopes.push((scope, pairs));
                }
            }
        }
        scopes.sort_by_key(|(s, _)| *s);
        ForestSnapshot { global, scopes }
    }

    /// Rebuild forest state from a snapshot (startup recovery path).
    pub fn hydrate(&self, snap: &ForestSnapshot) {
        let _g = self.union_lock.lock();
        self.global.hydrate(&snap.global);
        for (scope, pairs) in &snap.scopes {
            let overlay = self.overlays.entry(*scope).or_default();
            overlay.hydrate(pairs);
            drop(overlay);
            // Re-register overlay members under their *current* global roots
            // so future global merges keep notifying this scope. Members may
            // be stale global roots; resolving through the hydrated global
            // DSU lands on the live root.
            for &(a, b) in pairs {
                for m in [a, b] {
                    let live = self.global.find(m);
                    self.registry.entry(live).or_default().insert(*scope);
                }
            }
            self.scope_links
                .fetch_add(pairs.len() as u64, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ForestStats {
    pub events_applied: u64,
    pub global_merges: u64,
    pub global_links: u64,
    pub scope_links: u64,
    pub active_scopes: u64,
    pub merge_fixups: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use smallvec::smallvec;

    fn global_edge(u: NodeId, v: NodeId) -> EdgeEvent {
        EdgeEvent {
            src: u,
            dst: v,
            visibility: Visibility::Global,
            event_time_ms: 0,
            props: None,
        }
    }

    fn scoped_edge(u: NodeId, v: NodeId, scopes: &[ScopeId]) -> EdgeEvent {
        EdgeEvent {
            src: u,
            dst: v,
            visibility: Visibility::Scoped(scopes.iter().copied().collect()),
            event_time_ms: 0,
            props: None,
        }
    }

    #[test]
    fn global_edges_visible_to_all_scopes() {
        let f = ScopedForest::new();
        f.apply(&global_edge(500, 105));
        assert!(f.connected(GLOBAL_SCOPE, 500, 105));
        assert!(f.connected(42, 500, 105));
        assert!(f.connected(2999, 500, 105));
    }

    #[test]
    fn scoped_edges_invisible_elsewhere() {
        let f = ScopedForest::new();
        f.apply(&scoped_edge(1, 2, &[7]));
        assert!(f.connected(7, 1, 2));
        assert!(!f.connected(GLOBAL_SCOPE, 1, 2));
        assert!(!f.connected(8, 1, 2));
    }

    #[test]
    fn multi_scope_edge() {
        let f = ScopedForest::new();
        f.apply(&scoped_edge(1, 2, &[3, 9]));
        assert!(f.connected(3, 1, 2));
        assert!(f.connected(9, 1, 2));
        assert!(!f.connected(4, 1, 2));
    }

    /// Regression for the stale-root hazard: overlay state keyed by a global
    /// root that later gets absorbed must remain reachable from the new root.
    #[test]
    fn global_merge_after_scope_edge_keeps_overlay_reachable() {
        let f = ScopedForest::new();
        // Scope edge keyed at global roots a=1, b=2.
        f.apply(&scoped_edge(1, 2, &[7]));
        // Global merge absorbs one of those roots into a brand-new root 3.
        f.apply(&global_edge(1, 3));
        // Scope 7 must see 3 connected to 2 (3 ~global~ 1 ~scope7~ 2).
        assert!(f.connected(7, 3, 2));
        assert!(f.connected(7, 3, 1));
        // Global view: 3~1 only.
        assert!(f.connected(GLOBAL_SCOPE, 1, 3));
        assert!(!f.connected(GLOBAL_SCOPE, 2, 3));
        // Another scope sees only the global part.
        assert!(f.connected(11, 1, 3));
        assert!(!f.connected(11, 2, 3));
    }

    /// Chains of global merges across previously-registered roots.
    #[test]
    fn repeated_global_collapses() {
        let f = ScopedForest::new();
        f.apply(&scoped_edge(10, 20, &[5]));
        f.apply(&scoped_edge(30, 40, &[5]));
        f.apply(&global_edge(10, 30)); // bridges the two scope islands globally
        assert!(f.connected(5, 20, 40));
        assert!(!f.connected(6, 20, 40));
        f.apply(&global_edge(10, 99));
        f.apply(&global_edge(99, 98));
        assert!(f.connected(5, 98, 20));
        assert!(f.connected(5, 98, 40));
        assert!(!f.connected(GLOBAL_SCOPE, 98, 20));
    }

    /// Naive reference model: per-scope BFS over (global ∪ scope) edges.
    struct Model {
        global_edges: Vec<(NodeId, NodeId)>,
        scope_edges: Vec<(ScopeId, NodeId, NodeId)>,
    }

    impl Model {
        fn connected(&self, scope: ScopeId, u: NodeId, v: NodeId) -> bool {
            if u == v {
                return true;
            }
            let mut adj: std::collections::HashMap<NodeId, Vec<NodeId>> =
                std::collections::HashMap::new();
            let add = |a: NodeId, b: NodeId, adj: &mut std::collections::HashMap<_, Vec<_>>| {
                adj.entry(a).or_default().push(b);
                adj.entry(b).or_default().push(a);
            };
            for &(a, b) in &self.global_edges {
                add(a, b, &mut adj);
            }
            for &(s, a, b) in &self.scope_edges {
                if s == scope {
                    add(a, b, &mut adj);
                }
            }
            let mut seen = std::collections::HashSet::new();
            let mut stack = vec![u];
            seen.insert(u);
            while let Some(x) = stack.pop() {
                if x == v {
                    return true;
                }
                for &n in adj.get(&x).map(|v| v.as_slice()).unwrap_or(&[]) {
                    if seen.insert(n) {
                        stack.push(n);
                    }
                }
            }
            false
        }
    }

    #[test]
    fn randomized_against_reference_model() {
        let mut rng = StdRng::seed_from_u64(0xB1A2E);
        const NODES: u64 = 60;
        const SCOPES: [ScopeId; 4] = [1, 2, 3, 4];

        for round in 0..8 {
            let f = ScopedForest::new();
            let mut model = Model {
                global_edges: vec![],
                scope_edges: vec![],
            };
            for _ in 0..300 {
                let u = rng.random_range(0..NODES);
                let v = rng.random_range(0..NODES);
                if rng.random_range(0..100) < 25 {
                    f.apply(&global_edge(u, v));
                    model.global_edges.push((u, v));
                } else {
                    let scope = SCOPES[rng.random_range(0..SCOPES.len())];
                    // Occasionally multi-scope.
                    if rng.random_range(0..10) == 0 {
                        let scope2 = SCOPES[rng.random_range(0..SCOPES.len())];
                        f.apply(&scoped_edge(u, v, &[scope, scope2]));
                        model.scope_edges.push((scope, u, v));
                        model.scope_edges.push((scope2, u, v));
                    } else {
                        f.apply(&scoped_edge(u, v, &[scope]));
                        model.scope_edges.push((scope, u, v));
                    }
                }
                // Spot-check a few queries every step.
                for _ in 0..4 {
                    let a = rng.random_range(0..NODES);
                    let b = rng.random_range(0..NODES);
                    let s = if rng.random_range(0..4) == 0 {
                        GLOBAL_SCOPE
                    } else {
                        SCOPES[rng.random_range(0..SCOPES.len())]
                    };
                    assert_eq!(
                        f.connected(s, a, b),
                        model.connected(s, a, b),
                        "round {round}: scope {s} connectivity({a},{b}) diverged"
                    );
                }
            }

            // Snapshot/hydrate must preserve every answer.
            let snap = f.snapshot();
            let f2 = ScopedForest::new();
            f2.hydrate(&snap);
            for _ in 0..400 {
                let a = rng.random_range(0..NODES);
                let b = rng.random_range(0..NODES);
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                assert_eq!(f2.connected(s, a, b), model.connected(s, a, b));
                assert_eq!(
                    f2.connected(GLOBAL_SCOPE, a, b),
                    model.connected(GLOBAL_SCOPE, a, b)
                );
            }

            // And hydrated forests must keep absorbing new global merges
            // correctly (registry rebuilt from snapshot).
            let u = rng.random_range(0..NODES);
            let v = rng.random_range(0..NODES);
            f2.apply(&global_edge(u, v));
            model.global_edges.push((u, v));
            for _ in 0..200 {
                let a = rng.random_range(0..NODES);
                let b = rng.random_range(0..NODES);
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                assert_eq!(
                    f2.connected(s, a, b),
                    model.connected(s, a, b),
                    "round {round}: post-hydration merge diverged in scope {s} ({a},{b})"
                );
            }
        }
    }

    #[test]
    fn visibility_normalize_folds_global_scope_id() {
        let v = Visibility::Scoped(smallvec![GLOBAL_SCOPE, 3]);
        assert_eq!(v.normalize(), Visibility::Global);
    }
}
