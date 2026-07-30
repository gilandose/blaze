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
//! # Canonical representatives: lowest graph id wins
//!
//! Both layers union by *minimum id* (see [`Dsu`]), so `scope_root` is the
//! smallest graph id in the component as seen by that scope — deterministic
//! and monotonically non-increasing as merges happen. This holds through the
//! overlay: a global root is the min of its global component, overlay
//! elements are (possibly historical) global roots, and every fix-up inserts
//! the new, lower global root as an overlay element — so the overlay class
//! min is exactly the min graph id of the scope component.
//!
//! Unions (from the single ingest pipeline) are serialized behind one mutex;
//! finds — the sub-millisecond API path — never take it and use the
//! read-only `find_ro` walk so query load cannot contend with the writer.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::base::{BaseStats, RoutingBase};
use super::dsu::Dsu;
use super::types::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, Visibility};

#[derive(Debug, Default)]
pub struct ScopedForest {
    global: Dsu,
    overlays: DashMap<ScopeId, Dsu>,
    /// global root -> scopes with overlay state keyed by that root.
    registry: DashMap<NodeId, HashSet<ScopeId>>,
    /// Optional committed state living outside the heap (mmap'd base). When
    /// present, the DSU maps above are the *memtable*: everything applied
    /// since the base was compacted. See [`super::base`] for the composition
    /// rules and the invariant that keeps it exact.
    base: Option<Arc<dyn RoutingBase>>,
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

    /// Build a forest whose committed state is served from `base` (mmap'd
    /// routing snapshot); the in-heap maps become the memtable of everything
    /// applied after it.
    pub fn with_base(base: Arc<dyn RoutingBase>) -> Self {
        Self {
            base: Some(base),
            ..Self::default()
        }
    }

    /// Stats for the attached base, if any.
    pub fn base_stats(&self) -> Option<BaseStats> {
        self.base.as_ref().map(|b| b.stats())
    }

    /// Apply one edge event to in-memory topology.
    pub fn apply(&self, event: &EdgeEvent) {
        match event.visibility.clone().normalize() {
            Visibility::Global => self.apply_global(event.src, event.dst),
            Visibility::Scoped(scopes) => self.apply_scoped(event.src, event.dst, &scopes),
        }
        self.events_applied.fetch_add(1, Ordering::Relaxed);
    }

    // --- composed resolution (base -> memtable) -------------------------
    //
    // Memtable keys are always composed roots (see `super::base`), so a node
    // that already has a memtable parent cannot also be stored in the base:
    // one memtable walk settles it, and only a memtable-unknown node needs a
    // base probe.

    /// Shared-tier root of `node`, composing base and memtable.
    fn shared_root(&self, node: NodeId, compress: bool) -> NodeId {
        let m = if compress {
            self.global.find(node)
        } else {
            self.global.find_ro(node)
        };
        if m != node {
            return m;
        }
        match self.base.as_ref().and_then(|b| b.shared_parent(node)) {
            Some(b) if b != node => {
                if compress {
                    self.global.find(b)
                } else {
                    self.global.find_ro(b)
                }
            }
            _ => node,
        }
    }

    /// Overlay root of `shared_root` within `scope`, composing base and
    /// memtable. `shared_root` must already be shared-composed.
    fn overlay_root(&self, scope: ScopeId, shared_root: NodeId, compress: bool) -> NodeId {
        let mem = self.overlays.get(&scope);
        if let Some(overlay) = &mem {
            let m = if compress {
                overlay.find(shared_root)
            } else {
                overlay.find_ro(shared_root)
            };
            if m != shared_root {
                return m;
            }
        }
        let Some(b) = self
            .base
            .as_ref()
            .and_then(|b| b.overlay_parent(scope, shared_root))
        else {
            return shared_root;
        };
        if b == shared_root {
            return shared_root;
        }
        match &mem {
            Some(overlay) if compress => overlay.find(b),
            Some(overlay) => overlay.find_ro(b),
            None => b,
        }
    }

    fn apply_global(&self, u: NodeId, v: NodeId) {
        let _g = self.union_lock.lock();
        // Resolve through the base first so the memtable only ever links
        // composed roots (the invariant in `super::base`).
        let ru = self.shared_root(u, true);
        let rv = self.shared_root(v, true);
        let Some(merge) = self.global.union(ru, rv) else {
            return;
        };
        // Root `merge.child` no longer exists in the shared tier; tell every
        // scope that keyed overlay state on it — from the memtable registry
        // and from the base's registry index.
        let mut scopes: BTreeSet<ScopeId> = self
            .registry
            .remove(&merge.child)
            .map(|(_, s)| s.into_iter().collect())
            .unwrap_or_default();
        if let Some(base) = &self.base {
            scopes.extend(base.scopes_for_root(merge.child));
        }
        for &scope in &scopes {
            // Compose the overlay operands too: either endpoint's overlay
            // class may live in the base.
            let a = self.overlay_root(scope, merge.child, true);
            let b = self.overlay_root(scope, merge.parent, true);
            if a != b {
                self.overlays.entry(scope).or_default().union(a, b);
                self.fixups.fetch_add(1, Ordering::Relaxed);
            }
        }
        if !scopes.is_empty() {
            self.registry
                .entry(merge.parent)
                .or_default()
                .extend(scopes);
        }
    }

    fn apply_scoped(&self, u: NodeId, v: NodeId, scopes: &[ScopeId]) {
        let _g = self.union_lock.lock();
        let ru = self.shared_root(u, true);
        let rv = self.shared_root(v, true);
        for &scope in scopes {
            let a = self.overlay_root(scope, ru, true);
            let b = self.overlay_root(scope, rv, true);
            self.overlays.entry(scope).or_default().union(a, b);
            self.registry.entry(ru).or_default().insert(scope);
            self.registry.entry(rv).or_default().insert(scope);
            self.scope_links.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Component representative of `node` as seen by `scope`: the lowest
    /// graph id in that component. Read-only walk — never contends with the
    /// ingest writer.
    pub fn scope_root(&self, scope: ScopeId, node: NodeId) -> NodeId {
        let g = self.shared_root(node, false);
        if scope == GLOBAL_SCOPE {
            return g;
        }
        self.overlay_root(scope, g, false)
    }

    /// Whether `u` and `v` are connected in `scope`'s view. Lock-free.
    pub fn connected(&self, scope: ScopeId, u: NodeId, v: NodeId) -> bool {
        self.scope_root(scope, u) == self.scope_root(scope, v)
    }

    pub fn stats(&self) -> ForestStats {
        let base = self.base.as_ref().map(|b| b.stats()).unwrap_or_default();
        ForestStats {
            events_applied: self.events_applied.load(Ordering::Relaxed),
            global_merges: self.global.merges(),
            // Memtable links: with a base attached these count only what has
            // been applied since compaction.
            global_links: self.global.len_links() as u64,
            scope_links: self.scope_links.load(Ordering::Relaxed),
            active_scopes: self.overlays.len() as u64,
            merge_fixups: self.fixups.load(Ordering::Relaxed),
            base_shared_pairs: base.shared_pairs,
            base_overlay_pairs: base.overlay_pairs,
            base_mapped_bytes: base.mapped_bytes,
        }
    }

    /// Capture a consistent snapshot. Takes the union lock so no merge or
    /// fix-up is half-applied in the captured state; queries keep running.
    ///
    /// With a base attached this is the **compaction** read: base pairs are
    /// re-resolved through the memtable so the result is the complete
    /// composed state, and the next base subsumes both layers.
    pub fn snapshot(&self) -> ForestSnapshot {
        let _g = self.union_lock.lock();

        let mut shared_keys = self.global.keys();
        if let Some(base) = &self.base {
            shared_keys.extend(base.shared_nodes());
            shared_keys.sort_unstable();
            shared_keys.dedup();
        }
        let global: Vec<(NodeId, NodeId)> = shared_keys
            .into_iter()
            .filter_map(|k| {
                let r = self.shared_root(k, false);
                (r != k).then_some((k, r))
            })
            .collect();

        let mut scope_ids: Vec<ScopeId> = self.overlays.iter().map(|e| *e.key()).collect();
        if let Some(base) = &self.base {
            scope_ids.extend(base.scopes());
            scope_ids.sort_unstable();
            scope_ids.dedup();
        }
        let mut scopes = Vec::with_capacity(scope_ids.len());
        for scope in scope_ids {
            let mut keys = self
                .overlays
                .get(&scope)
                .map(|o| o.keys())
                .unwrap_or_default();
            if let Some(base) = &self.base {
                keys.extend(base.overlay_nodes(scope));
                keys.sort_unstable();
                keys.dedup();
            }
            let pairs: Vec<(NodeId, NodeId)> = keys
                .into_iter()
                .filter_map(|k| {
                    let r = self.overlay_root(scope, k, false);
                    (r != k).then_some((k, r))
                })
                .collect();
            if !pairs.is_empty() {
                scopes.push((scope, pairs));
            }
        }
        scopes.sort_by_key(|(s, _)| *s);
        ForestSnapshot { global, scopes }
    }

    /// Rebuild forest state from a snapshot (startup recovery path).
    ///
    /// RAM mode only: loading pairs into the memtable would break the
    /// "memtable keys are composed roots" invariant if a base were attached
    /// (use [`ScopedForest::with_base`] instead).
    pub fn hydrate(&self, snap: &ForestSnapshot) {
        debug_assert!(
            self.base.is_none(),
            "hydrate() is RAM mode only; a base-backed forest composes instead"
        );
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
    /// Pairs served from the mmap'd base (0 when running all-RAM).
    pub base_shared_pairs: u64,
    pub base_overlay_pairs: u64,
    pub base_mapped_bytes: u64,
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

        /// Lowest node id in `u`'s component as seen by `scope` — the value
        /// `scope_root` must return under lowest-graph-id-wins semantics.
        fn component_min(&self, scope: ScopeId, u: NodeId) -> NodeId {
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
            let mut min = u;
            while let Some(x) = stack.pop() {
                min = min.min(x);
                for &n in adj.get(&x).map(|v| v.as_slice()).unwrap_or(&[]) {
                    if seen.insert(n) {
                        stack.push(n);
                    }
                }
            }
            min
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
                    // Canonical representative: lowest graph id in the
                    // component wins, in every scope's view.
                    assert_eq!(
                        f.scope_root(s, a),
                        model.component_min(s, a),
                        "round {round}: scope {s} root({a}) is not the component min"
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
                assert_eq!(f2.scope_root(s, a), model.component_min(s, a));
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
