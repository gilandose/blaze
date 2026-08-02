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

use arc_swap::ArcSwap;
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
    /// The whole resolvable state, replaced wholesale by a fold and never
    /// mutated in place across it — see [`Tier`] and
    /// [`ScopedForest::compact_and_fold`].
    tier: ArcSwap<Tier>,
    /// Whether memtables track merge edges downward, so
    /// [`ScopedForest::members`] can answer. Set once at construction and
    /// carried across folds — a memtable created without it can never be
    /// retrofitted, because the edges it would have recorded are gone.
    member_index: bool,
    /// Serializes all mutations (global unions, overlay unions, fix-ups) and
    /// the fold.
    union_lock: Mutex<()>,
    events_applied: AtomicU64,
    scope_links: AtomicU64,
    fixups: AtomicU64,
    folds: AtomicU64,
}

/// One consistent generation of forest state: an optional committed base plus
/// the memtable layered over it.
///
/// These four things have to move together. A fold publishes a base that
/// subsumes the memtable and must drop the memtable in the same step — and
/// "clear the maps in place" is not that step: `DashMap::clear` is not atomic
/// across shards, so a concurrent query could walk a half-cleared chain and
/// stop at an intermediate node, returning a non-root as a component id.
///
/// So a fold builds a new `Tier` and swaps the pointer. Readers load it once
/// per query and resolve entirely within one generation; a reader holding the
/// old generation keeps getting correct answers from the old base and memtable
/// until it finishes, and that generation is freed when the last such reader
/// drops it. Classic RCU, and the reason no query ever has to be blocked.
#[derive(Debug, Default)]
struct Tier {
    /// Committed state living outside the heap (mmap'd base). When present the
    /// memtable holds everything applied since it was compacted. See
    /// [`super::base`] for the composition rules.
    base: Option<Arc<dyn RoutingBase>>,
    /// Held behind an `Arc` so a *base* swap can reuse it. Compaction runs
    /// concurrently with ingest and then installs an equivalent base, which
    /// must not discard the memtable that grew meanwhile — see
    /// [`ScopedForest::swap_base`].
    mem: Arc<Memtable>,
}

/// The in-heap half of a tier: everything applied since the base.
#[derive(Debug, Default)]
struct Memtable {
    global: Dsu,
    overlays: DashMap<ScopeId, Dsu>,
    /// global root -> scopes with overlay state keyed by that root.
    registry: DashMap<NodeId, HashSet<ScopeId>>,
    /// Propagated to overlays as they are created on demand.
    member_index: bool,
}

impl Memtable {
    fn new(member_index: bool) -> Self {
        Self {
            global: Dsu::tracking_members(member_index),
            member_index,
            ..Self::default()
        }
    }

    /// `scope`'s overlay, created with this memtable's index setting rather
    /// than by `or_default` — a scope first seen after the flag was read must
    /// not silently opt out of it.
    fn overlay(&self, scope: ScopeId) -> dashmap::mapref::one::RefMut<'_, ScopeId, Dsu> {
        self.overlays
            .entry(scope)
            .or_insert_with(|| Dsu::tracking_members(self.member_index))
    }
}

impl Tier {
    fn base(&self) -> Base<'_> {
        self.base.as_deref()
    }
}

/// A consistent point-in-time capture of the whole forest, suitable for
/// Puffin serialization and startup hydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForestSnapshot {
    pub global: Vec<(NodeId, NodeId)>,
    pub scopes: Vec<(ScopeId, Vec<(NodeId, NodeId)>)>,
}

/// Receives a compacted forest as an ordered *stream*, so compaction never
/// has to materialize the state it is writing.
///
/// This exists because [`ForestSnapshot`] is O(state): at 2B links it is a
/// ~32 GB `Vec` built under the union lock, which stalls ingest for as long
/// as it takes to allocate. A sink that writes bytes as they arrive (see
/// `storage::codec::compact_to_blobs`) keeps compaction at O(memtable) heap.
///
/// **Emission order is part of the contract**: every shared pair in ascending
/// node order, then each scope in ascending scope order with its overlay
/// pairs in ascending node order. Sinks can therefore write the fixed-stride
/// sorted tables the on-disk format wants with no buffering and no sort.
pub trait SnapshotSink {
    /// A composed shared-tier pair; only emitted when `root != node`.
    fn shared_pair(&mut self, node: NodeId, root: NodeId);

    /// A composed overlay pair for the most recently opened scope.
    fn overlay_pair(&mut self, node: NodeId, root: NodeId);

    /// Opens `scope`. A scope may turn out to contribute no pairs at all, so
    /// sinks that must not emit empty groups should buffer and discard in
    /// [`SnapshotSink::scope_end`].
    fn scope_start(&mut self, _scope: ScopeId) {}

    fn scope_end(&mut self, _scope: ScopeId) {}

    /// One `shared root -> scope` reverse-index entry per endpoint of each
    /// emitted overlay pair, already resolved to the *live* shared root.
    /// Grouped by scope, so a sink wanting root order must sort.
    fn registry_entry(&mut self, _root: NodeId, _scope: ScopeId) {}
}

/// The base a resolution is running against.
type Base<'a> = Option<&'a dyn RoutingBase>;

/// The answer to [`ScopedForest::members`].
///
/// Two variants rather than a `Vec` plus a flag nobody checks: past the
/// percolation threshold a component is a large fraction of the graph, and
/// "here are 1000 of them" must not be mistakable for "here are all of them".
/// A caller that gets `Truncated` has learned something real — this node is in
/// a hub — and should ask the analytics path, not retry with a bigger cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Members {
    /// Every member of the component. Includes the queried node and the root.
    Complete(Vec<NodeId>),
    /// Exactly `cap` members, all real, with at least one more not listed.
    Truncated(Vec<NodeId>),
}

impl Members {
    pub fn nodes(&self) -> &[NodeId] {
        match self {
            Members::Complete(v) | Members::Truncated(v) => v,
        }
    }

    pub fn into_nodes(self) -> Vec<NodeId> {
        match self {
            Members::Complete(v) | Members::Truncated(v) => v,
        }
    }

    pub fn is_truncated(&self) -> bool {
        matches!(self, Members::Truncated(_))
    }
}

/// A capped, deduplicating downward traversal of a parent forest.
///
/// The visited set is not an optimisation. The same node can be offered by
/// several layers and by both levels of a scoped walk (an overlay element is
/// also reachable through the shared tree below it), and a component whose root
/// changed since a run was written can be entered from two directions at once.
struct Walk {
    cap: usize,
    seen: HashSet<NodeId>,
    out: Vec<NodeId>,
    truncated: bool,
}

impl Walk {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            seen: HashSet::new(),
            out: Vec::new(),
            truncated: false,
        }
    }

    /// Admit `n`, returning whether it should be expanded in turn.
    fn admit(&mut self, n: NodeId) -> bool {
        if !self.seen.insert(n) {
            return false;
        }
        if self.out.len() >= self.cap {
            self.truncated = true;
            return false;
        }
        self.out.push(n);
        true
    }

    /// Walk downward from `seeds`, asking `children` for each node's children.
    ///
    /// Stops the moment the cap is exceeded: an unbounded frontier is exactly
    /// the failure mode the cap exists to prevent, so there is no point
    /// draining it to discover the truncation a second time.
    fn expand(&mut self, seeds: &[NodeId], mut children: impl FnMut(NodeId, &mut Vec<NodeId>)) {
        let mut frontier: Vec<NodeId> = Vec::new();
        for &s in seeds {
            if self.admit(s) {
                frontier.push(s);
            }
            if self.truncated {
                return;
            }
        }
        // One buffer for the whole walk: children of a leaf is the common case
        // and the common case should not allocate.
        let mut buf: Vec<NodeId> = Vec::new();
        while let Some(k) = frontier.pop() {
            buf.clear();
            children(k, &mut buf);
            for &c in &buf {
                if self.admit(c) {
                    frontier.push(c);
                }
                if self.truncated {
                    return;
                }
            }
        }
    }

    fn finish(self) -> Members {
        if self.truncated {
            Members::Truncated(self.out)
        } else {
            Members::Complete(self.out)
        }
    }
}

/// Collects a stream back into a [`ForestSnapshot`] (the RAM-mode path).
#[derive(Default)]
struct SnapshotCollector {
    global: Vec<(NodeId, NodeId)>,
    scopes: Vec<(ScopeId, Vec<(NodeId, NodeId)>)>,
    open: Option<(ScopeId, Vec<(NodeId, NodeId)>)>,
}

impl SnapshotSink for SnapshotCollector {
    fn shared_pair(&mut self, node: NodeId, root: NodeId) {
        self.global.push((node, root));
    }

    fn overlay_pair(&mut self, node: NodeId, root: NodeId) {
        if let Some((_, pairs)) = &mut self.open {
            pairs.push((node, root));
        }
    }

    fn scope_start(&mut self, scope: ScopeId) {
        self.open = Some((scope, Vec::new()));
    }

    fn scope_end(&mut self, _scope: ScopeId) {
        if let Some((scope, pairs)) = self.open.take()
            && !pairs.is_empty()
        {
            self.scopes.push((scope, pairs));
        }
    }
}

impl ScopedForest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a forest whose committed state is served from `base` (mmap'd
    /// routing snapshot); the in-heap maps become the memtable of everything
    /// applied after it.
    pub fn with_base(base: Arc<dyn RoutingBase>) -> Self {
        let forest = Self::default();
        forest.tier.store(Arc::new(Tier {
            base: Some(base),
            mem: Arc::new(Memtable::default()),
        }));
        forest
    }

    /// Track merge edges downward so [`ScopedForest::members`] can answer.
    ///
    /// Consuming rather than a setter, and applied before anything is ingested:
    /// the index is built as merges happen, so turning it on later would leave
    /// a hole exactly the size of everything already applied. Runs written by
    /// this worker still need `--member-index` for the *base* half of the
    /// answer; this covers only the memtable.
    pub fn tracking_members(mut self) -> Self {
        self.member_index = true;
        let previous = self.tier.load();
        debug_assert_eq!(
            previous.mem.global.len_links(),
            0,
            "tracking_members() must be called before any edge is applied"
        );
        self.tier.store(Arc::new(Tier {
            base: previous.base.clone(),
            mem: Arc::new(Memtable::new(true)),
        }));
        self
    }

    /// Whether this forest's memtable can answer downward walks.
    pub fn tracks_members(&self) -> bool {
        self.member_index
    }

    /// Whether [`ScopedForest::members`] can answer right now — both halves,
    /// memtable and base. Worth surfacing because the base half can go away
    /// underneath a running worker: a fold or a tiered merge that forgets the
    /// flag writes a run that cannot answer, and the stack goes quiet from the
    /// next swap rather than at startup.
    pub fn members_available(&self) -> bool {
        self.member_index && self.tier.load().base().is_none_or(|b| b.has_member_index())
    }

    /// Stats for the attached base, if any.
    pub fn base_stats(&self) -> Option<BaseStats> {
        self.tier.load().base.as_ref().map(|b| b.stats())
    }

    /// Links held in the heap right now — what a fold would reclaim.
    pub fn memtable_links(&self) -> u64 {
        let t = self.tier.load();
        let overlays: usize = t.mem.overlays.iter().map(|e| e.value().len_links()).sum();
        (t.mem.global.len_links() + overlays) as u64
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
    fn shared_root(t: &Tier, node: NodeId, compress: bool) -> NodeId {
        let m = if compress {
            t.mem.global.find(node)
        } else {
            t.mem.global.find_ro(node)
        };
        if m != node {
            return m;
        }
        match t.base().and_then(|b| b.shared_parent(node)) {
            Some(b) if b != node => {
                if compress {
                    t.mem.global.find(b)
                } else {
                    t.mem.global.find_ro(b)
                }
            }
            _ => node,
        }
    }

    /// Overlay root of `shared_root` within `scope`, composing base and
    /// memtable. `shared_root` must already be shared-composed.
    fn overlay_root(t: &Tier, scope: ScopeId, shared_root: NodeId, compress: bool) -> NodeId {
        let mem = t.mem.overlays.get(&scope);
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
        let Some(b) = t.base().and_then(|b| b.overlay_parent(scope, shared_root)) else {
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
        let t = self.tier.load();
        // Resolve through the base first so the memtable only ever links
        // composed roots (the invariant in `super::base`).
        let ru = Self::shared_root(&t, u, true);
        let rv = Self::shared_root(&t, v, true);
        let Some(merge) = t.mem.global.union(ru, rv) else {
            return;
        };
        // Root `merge.child` no longer exists in the shared tier; tell every
        // scope that keyed overlay state on it — from the memtable registry
        // and from the base's registry index.
        let mut scopes: BTreeSet<ScopeId> = t
            .mem
            .registry
            .remove(&merge.child)
            .map(|(_, s)| s.into_iter().collect())
            .unwrap_or_default();
        if let Some(base) = t.base() {
            scopes.extend(base.scopes_for_root(merge.child));
        }
        for &scope in &scopes {
            // Compose the overlay operands too: either endpoint's overlay
            // class may live in the base.
            let a = Self::overlay_root(&t, scope, merge.child, true);
            let b = Self::overlay_root(&t, scope, merge.parent, true);
            if a != b {
                t.mem.overlay(scope).union(a, b);
                self.fixups.fetch_add(1, Ordering::Relaxed);
            }
        }
        if !scopes.is_empty() {
            t.mem
                .registry
                .entry(merge.parent)
                .or_default()
                .extend(scopes);
        }
    }

    fn apply_scoped(&self, u: NodeId, v: NodeId, scopes: &[ScopeId]) {
        let _g = self.union_lock.lock();
        let t = self.tier.load();
        let ru = Self::shared_root(&t, u, true);
        let rv = Self::shared_root(&t, v, true);
        for &scope in scopes {
            let a = Self::overlay_root(&t, scope, ru, true);
            let b = Self::overlay_root(&t, scope, rv, true);
            t.mem.overlay(scope).union(a, b);
            t.mem.registry.entry(ru).or_default().insert(scope);
            t.mem.registry.entry(rv).or_default().insert(scope);
            self.scope_links.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Component representative of `node` as seen by `scope`: the lowest
    /// graph id in that component. Read-only walk — never contends with the
    /// ingest writer.
    pub fn scope_root(&self, scope: ScopeId, node: NodeId) -> NodeId {
        // One tier load per query: a fold is rare, so readers pay a single
        // lock-free load and then resolve entirely within one generation.
        let t = self.tier.load();
        let g = Self::shared_root(&t, node, false);
        if scope == GLOBAL_SCOPE {
            return g;
        }
        Self::overlay_root(&t, scope, g, false)
    }

    /// Whether `u` and `v` are connected in `scope`'s view. Lock-free.
    pub fn connected(&self, scope: ScopeId, u: NodeId, v: NodeId) -> bool {
        self.scope_root(scope, u) == self.scope_root(scope, v)
    }

    /// Everyone in `node`'s component as `scope` sees it, up to `cap`
    /// ([design 011](../../docs/design/011-member-index.md)).
    ///
    /// `None` when the state cannot answer — the memtable was built without
    /// tracking, or a run in the base was written without `--member-index`.
    /// That is deliberately not "an empty component": a caller cannot tell a
    /// singleton from an unindexed run, so the unanswerable case has its own
    /// value rather than a plausible-looking wrong one.
    ///
    /// The returned set **includes `node` and the component root**. Order is
    /// parent-tree order, which is arbitrary; callers wanting it sorted sort.
    ///
    /// Lock-free, like every other query: it reads one tier generation and the
    /// mmap'd index, and takes nothing the ingest writer holds.
    ///
    /// # Two levels, for a reason
    ///
    /// In a tenant scope the overlay's elements are *shared roots*, not nodes,
    /// so walking the overlay down from `scope_root` yields the set of shared
    /// components this scope has glued together — and each of those still has
    /// to be expanded through the shared tree to reach actual members. Walking
    /// only the overlay would return the component's shared roots and call it
    /// the membership.
    pub fn members(&self, scope: ScopeId, node: NodeId, cap: usize) -> Option<Members> {
        let t = self.tier.load();
        if !self.member_index {
            return None;
        }
        if t.base().is_some_and(|b| !b.has_member_index()) {
            return None;
        }

        let shared = Self::shared_root(&t, node, false);
        let mut walk = Walk::new(cap);
        if scope == GLOBAL_SCOPE {
            walk.expand(&[shared], |k, out| {
                t.mem.global.children_of(k, out);
                if let Some(b) = t.base() {
                    b.shared_children(k, out);
                }
            });
        } else {
            let root = Self::overlay_root(&t, scope, shared, false);
            // Bounded by `cap + 1` because every overlay element is itself a
            // member: more than `cap` of them already proves truncation, and
            // stopping there keeps a hub component from walking the whole
            // overlay before the cap is ever consulted.
            let mut seeds = Walk::new(cap.saturating_add(1));
            seeds.expand(&[root], |k, out| {
                if let Some(o) = t.mem.overlays.get(&scope) {
                    o.children_of(k, out);
                }
                if let Some(b) = t.base() {
                    b.overlay_children(scope, k, out);
                }
            });
            walk.truncated |= seeds.truncated;
            walk.expand(&seeds.out, |k, out| {
                t.mem.global.children_of(k, out);
                if let Some(b) = t.base() {
                    b.shared_children(k, out);
                }
            });
        }
        Some(walk.finish())
    }

    pub fn stats(&self) -> ForestStats {
        let t = self.tier.load();
        let base = t.base.as_ref().map(|b| b.stats()).unwrap_or_default();
        ForestStats {
            members_available: self.members_available(),
            events_applied: self.events_applied.load(Ordering::Relaxed),
            global_merges: t.mem.global.merges(),
            // Memtable links: with a base attached these count only what has
            // been applied since compaction.
            global_links: t.mem.global.len_links() as u64,
            scope_links: self.scope_links.load(Ordering::Relaxed),
            active_scopes: t.mem.overlays.len() as u64,
            merge_fixups: self.fixups.load(Ordering::Relaxed),
            base_shared_pairs: base.shared_pairs,
            base_overlay_pairs: base.overlay_pairs,
            base_mapped_bytes: base.mapped_bytes,
            base_index_bytes: base.index_bytes,
            base_sequence: base.sequence,
            folds: self.folds.load(Ordering::Relaxed),
        }
    }

    /// Stream the composed (base ⊕ memtable) state to `sink` in key order.
    ///
    /// This is **compaction**: base pairs are re-resolved through the memtable
    /// so the emitted state is complete and the next base subsumes both
    /// layers. Takes the union lock, so no merge or fix-up is half-applied in
    /// the captured state; queries keep running throughout.
    ///
    /// Heap cost is O(memtable), not O(state): the base is walked as an
    /// ascending stream and merge-joined against the memtable's (small,
    /// sorted) key set, so nothing per-node is ever collected. Memtable keys
    /// are composed roots and are therefore disjoint from base keys — the
    /// equal-key case is handled rather than relied upon.
    pub fn compact_into(&self, sink: &mut dyn SnapshotSink) {
        let _g = self.union_lock.lock();
        self.compact_locked(sink);
    }

    /// Compact, then **adopt the result and drop the memtable**, without ever
    /// releasing the union lock in between.
    ///
    /// This is what keeps heap use bounded in a running process. Without it
    /// the memtable only starts empty — it grows for the life of the worker,
    /// and the disk tier's RAM argument expires at the first flush.
    ///
    /// `build` turns the stream it was just handed into the base that encodes
    /// it (write the Puffin file, map it) and may return anything else the
    /// caller needs from the same bytes. It runs with the lock held, so
    /// nothing can be applied between the compaction and the fold and then be
    /// lost by it — which is precisely the bug a two-call API would have. The
    /// price is that ingest stalls for the duration; that is why folds are
    /// triggered by memtable size rather than run every flush, and why design
    /// 001's delta chains are what eventually make them cheap.
    ///
    /// A failing `build` leaves the forest exactly as it was.
    ///
    /// # Why the whole tier is replaced
    ///
    /// Publishing the base and then clearing the memtable in place looks
    /// equivalent and is not: `DashMap::clear` is not atomic across shards, so
    /// a concurrent `find_ro` can walk into a half-cleared chain and stop at an
    /// intermediate node, returning a non-root as a component id. Instead the
    /// fold builds a fresh [`Tier`] — new base, empty memtable — and swaps the
    /// pointer in one store. Readers are never blocked and never observe a
    /// mixture: a query that loaded the previous generation keeps resolving
    /// against the previous base *and* its intact memtable, which by
    /// construction gives the same answers, and that generation is freed once
    /// the last such reader drops it.
    pub fn compact_and_fold<S: SnapshotSink + ?Sized, T, E>(
        &self,
        sink: &mut S,
        build: impl FnOnce(&mut S) -> Result<(Arc<dyn RoutingBase>, T), E>,
    ) -> Result<T, E> {
        let _g = self.union_lock.lock();
        self.compact_locked(sink);
        let (base, out) = build(sink)?;
        self.tier.store(Arc::new(Tier {
            base: Some(base),
            mem: Arc::new(Memtable::new(self.member_index)),
        }));
        self.folds.fetch_add(1, Ordering::Relaxed);
        Ok(out)
    }

    /// Fold the memtable out as a **delta layer** rather than rewriting the
    /// base, then adopt the result and drop the memtable.
    ///
    /// This is the cheap fold, and the reason design 001 exists.
    /// [`ScopedForest::compact_and_fold`] streams the whole composed state, so
    /// it costs O(state) in time and bytes every time the memtable needs
    /// draining. This streams only what the memtable itself contributes, so
    /// both are O(memtable) — the base is appended to, never rewritten.
    ///
    /// **The memtable is the dirty set.** No side structure tracks what
    /// changed: because every fold empties the memtable, its key set *is*
    /// exactly the nodes re-rooted since the last one, and each key's composed
    /// root is its final value. That is stronger than tracking dirtiness
    /// separately, which can drift out of step with the data it describes.
    ///
    /// Path-compression writes add a few keys that are semantically unchanged
    /// (`(node, its current root)`), which is redundant but never wrong.
    ///
    /// `build` receives the streamed delta and the base it layers over, and
    /// must return a base that composes the two — see
    /// `storage::layered::LayeredBase::pushed`. Everything happens under the
    /// union lock, so nothing can be applied between the stream and the swap
    /// and then be lost by it.
    pub fn fold_delta<S: SnapshotSink + ?Sized, T, E>(
        &self,
        sink: &mut S,
        build: impl FnOnce(
            &mut S,
            Option<&Arc<dyn RoutingBase>>,
        ) -> Result<(Arc<dyn RoutingBase>, T), E>,
    ) -> Result<T, E> {
        let _g = self.union_lock.lock();
        let previous = self.tier.load();
        self.stream_memtable(&previous, sink);
        let (base, out) = build(sink, previous.base.as_ref())?;
        self.tier.store(Arc::new(Tier {
            base: Some(base),
            mem: Arc::new(Memtable::new(self.member_index)),
        }));
        self.folds.fetch_add(1, Ordering::Relaxed);
        Ok(out)
    }

    /// Install a base that resolves **identically** to the current one, keeping
    /// the memtable intact.
    ///
    /// This is how storage-side compaction lands. Compaction reads committed,
    /// immutable layer files with no lock held, so ingest keeps running for the
    /// minutes it takes; by the time the compacted base is ready the memtable
    /// has grown, and discarding it would lose everything applied meanwhile.
    ///
    /// Soundness rests on `base` answering exactly as the current base does —
    /// which a compaction of it does by construction. Memtable keys are composed
    /// roots of the old base, composed roots are identical under both, so they
    /// remain composed roots of the new one and the disjointness the layered
    /// reader depends on still holds.
    ///
    /// Passing an inequivalent base is a correctness bug, not a performance
    /// one: use [`ScopedForest::fold_delta`] or
    /// [`ScopedForest::compact_and_fold`] when the memtable should be drained.
    pub fn swap_base(&self, base: Arc<dyn RoutingBase>) {
        let _g = self.union_lock.lock();
        let previous = self.tier.load();
        self.tier.store(Arc::new(Tier {
            base: Some(base),
            mem: previous.mem.clone(),
        }));
    }

    /// Stream the memtable's own `(node, composed root)` pairs, in the order
    /// [`SnapshotSink`] requires. Unlike compaction this never touches the
    /// base's key space — only the keys the memtable itself holds.
    fn stream_memtable(&self, t: &Tier, sink: &mut (impl SnapshotSink + ?Sized)) {
        let mut keys = t.mem.global.keys();
        keys.sort_unstable();
        for k in keys {
            Self::emit_shared(t, sink, k);
        }

        let mut scope_ids: Vec<ScopeId> = t.mem.overlays.iter().map(|e| *e.key()).collect();
        scope_ids.sort_unstable();
        for scope in scope_ids {
            sink.scope_start(scope);
            let mut keys = t
                .mem
                .overlays
                .get(&scope)
                .map(|o| o.keys())
                .unwrap_or_default();
            keys.sort_unstable();
            for k in keys {
                Self::emit_overlay(t, sink, scope, k);
            }
            sink.scope_end(scope);
        }
    }

    fn compact_locked(&self, sink: &mut (impl SnapshotSink + ?Sized)) {
        let t = self.tier.load();
        let base = t.base();

        let mut mem = t.mem.global.keys();
        mem.sort_unstable();
        let mut i = 0usize;
        if let Some(base) = base {
            base.for_each_shared_pair(&mut |k, _| {
                while i < mem.len() && mem[i] < k {
                    Self::emit_shared(&t, sink, mem[i]);
                    i += 1;
                }
                if i < mem.len() && mem[i] == k {
                    i += 1;
                }
                Self::emit_shared(&t, sink, k);
            });
        }
        while i < mem.len() {
            Self::emit_shared(&t, sink, mem[i]);
            i += 1;
        }

        let mut scope_ids: Vec<ScopeId> = t.mem.overlays.iter().map(|e| *e.key()).collect();
        if let Some(base) = base {
            scope_ids.extend(base.scopes());
        }
        scope_ids.sort_unstable();
        scope_ids.dedup();

        for scope in scope_ids {
            sink.scope_start(scope);
            let mut mem = t
                .mem
                .overlays
                .get(&scope)
                .map(|o| o.keys())
                .unwrap_or_default();
            mem.sort_unstable();
            let mut i = 0usize;
            if let Some(base) = base {
                base.for_each_overlay_pair(scope, &mut |k, _| {
                    while i < mem.len() && mem[i] < k {
                        Self::emit_overlay(&t, sink, scope, mem[i]);
                        i += 1;
                    }
                    if i < mem.len() && mem[i] == k {
                        i += 1;
                    }
                    Self::emit_overlay(&t, sink, scope, k);
                });
            }
            while i < mem.len() {
                Self::emit_overlay(&t, sink, scope, mem[i]);
                i += 1;
            }
            sink.scope_end(scope);
        }
    }

    fn emit_shared(t: &Tier, sink: &mut (impl SnapshotSink + ?Sized), node: NodeId) {
        let root = Self::shared_root(t, node, false);
        if root != node {
            sink.shared_pair(node, root);
        }
    }

    fn emit_overlay(
        t: &Tier,
        sink: &mut (impl SnapshotSink + ?Sized),
        scope: ScopeId,
        node: NodeId,
    ) {
        let root = Self::overlay_root(t, scope, node, false);
        if root == node {
            return;
        }
        sink.overlay_pair(node, root);
        // Both endpoints are (possibly historical) shared roots. Record them
        // under their *live* shared roots so a future merge of one still
        // notifies this scope after the next cold start.
        let a = Self::shared_root(t, node, false);
        sink.registry_entry(a, scope);
        let b = Self::shared_root(t, root, false);
        if b != a {
            sink.registry_entry(b, scope);
        }
    }

    /// Capture a consistent snapshot in the heap. Convenience over
    /// [`ScopedForest::compact_into`] for RAM-mode callers and tests; the
    /// flush path streams instead of collecting.
    pub fn snapshot(&self) -> ForestSnapshot {
        let mut c = SnapshotCollector::default();
        self.compact_into(&mut c);
        ForestSnapshot {
            global: c.global,
            scopes: c.scopes,
        }
    }

    /// Rebuild forest state from a snapshot (startup recovery path).
    ///
    /// RAM mode only: loading pairs into the memtable would break the
    /// "memtable keys are composed roots" invariant if a base were attached
    /// (use [`ScopedForest::with_base`] instead).
    pub fn hydrate(&self, snap: &ForestSnapshot) {
        let _g = self.union_lock.lock();
        let t = self.tier.load();
        debug_assert!(
            t.base.is_none(),
            "hydrate() is RAM mode only; a base-backed forest composes instead"
        );
        t.mem.global.hydrate(&snap.global);
        for (scope, pairs) in &snap.scopes {
            let overlay = t.mem.overlay(*scope);
            overlay.hydrate(pairs);
            drop(overlay);
            // Re-register overlay members under their *current* global roots
            // so future global merges keep notifying this scope. Members may
            // be stale global roots; resolving through the hydrated global
            // DSU lands on the live root.
            for &(a, b) in pairs {
                for m in [a, b] {
                    let live = t.mem.global.find(m);
                    t.mem.registry.entry(live).or_default().insert(*scope);
                }
            }
            self.scope_links
                .fetch_add(pairs.len() as u64, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ForestStats {
    /// Whether `members` can answer — memtable tracking on *and* every run in
    /// the base carrying the index. False here with `--member-index` set means
    /// a run was written without it; see `docs/design/011-member-index.md`.
    pub members_available: bool,
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
    /// Heap held by the base's in-RAM lookup indexes.
    pub base_index_bytes: u64,
    pub base_sequence: u64,
    /// Times the memtable has been folded into a fresh base. If this stops
    /// advancing while `global_links` climbs, heap use is unbounded.
    pub folds: u64,
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

        /// Every node in `u`'s component as seen by `scope`, sorted — the set
        /// `members` must return when it fits under the cap.
        fn component(&self, scope: ScopeId, u: NodeId) -> Vec<NodeId> {
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
                for &n in adj.get(&x).map(|v| v.as_slice()).unwrap_or(&[]) {
                    if seen.insert(n) {
                        stack.push(n);
                    }
                }
            }
            let mut out: Vec<NodeId> = seen.into_iter().collect();
            out.sort_unstable();
            out
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

    /// Sorted members, for comparison against the model.
    fn members_of(f: &ScopedForest, scope: ScopeId, node: NodeId, cap: usize) -> Vec<NodeId> {
        let m = f.members(scope, node, cap).expect("the index is on");
        assert!(!m.is_truncated(), "cap {cap} was not meant to bite");
        let mut v = m.into_nodes();
        let before = v.len();
        v.sort_unstable();
        v.dedup();
        assert_eq!(before, v.len(), "members must not repeat a node");
        v
    }

    /// The membership question, graded against BFS over the same edges — in
    /// every scope, after every edge. This is invariant I1 restated for sets
    /// rather than for representatives.
    #[test]
    fn members_match_the_reference_model() {
        let mut rng = StdRng::seed_from_u64(0x11E11BE12);
        const NODES: u64 = 60;
        const SCOPES: [ScopeId; 4] = [1, 2, 3, 4];

        for round in 0..6 {
            let f = ScopedForest::new().tracking_members();
            let mut model = Model {
                global_edges: vec![],
                scope_edges: vec![],
            };
            for step in 0..200 {
                let u = rng.random_range(0..NODES);
                let v = rng.random_range(0..NODES);
                if rng.random_range(0..100) < 25 {
                    f.apply(&global_edge(u, v));
                    model.global_edges.push((u, v));
                } else {
                    let scope = SCOPES[rng.random_range(0..SCOPES.len())];
                    f.apply(&scoped_edge(u, v, &[scope]));
                    model.scope_edges.push((scope, u, v));
                }
                if step % 10 != 0 {
                    continue;
                }
                for _ in 0..4 {
                    let a = rng.random_range(0..NODES);
                    let s = if rng.random_range(0..4) == 0 {
                        GLOBAL_SCOPE
                    } else {
                        SCOPES[rng.random_range(0..SCOPES.len())]
                    };
                    assert_eq!(
                        members_of(&f, s, a, NODES as usize),
                        model.component(s, a),
                        "round {round} step {step}: scope {s} members({a}) diverged"
                    );
                }
            }

            // And the same after a snapshot/hydrate cycle, which is the only
            // other way the index gets built.
            let snap = f.snapshot();
            let f2 = ScopedForest::new().tracking_members();
            f2.hydrate(&snap);
            for a in 0..NODES {
                for s in [GLOBAL_SCOPE, 1, 2, 3, 4] {
                    assert_eq!(
                        members_of(&f2, s, a, NODES as usize),
                        model.component(s, a),
                        "round {round}: hydrated scope {s} members({a}) diverged"
                    );
                }
            }
        }
    }

    /// A node nobody has ever mentioned is its own component, and saying so is
    /// different from saying nothing — the empty answer would be indexed-but-
    /// unreachable, which is the bug this design's `None` exists for.
    #[test]
    fn an_untouched_node_is_a_singleton() {
        let f = ScopedForest::new().tracking_members();
        f.apply(&global_edge(1, 2));
        assert_eq!(members_of(&f, GLOBAL_SCOPE, 9999, 10), vec![9999]);
        assert_eq!(members_of(&f, 7, 9999, 10), vec![9999]);
    }

    /// The cap is the whole reason this returns an enum. `Truncated` must hand
    /// back exactly `cap` members, every one of them real.
    #[test]
    fn a_component_over_the_cap_truncates_to_real_members() {
        let f = ScopedForest::new().tracking_members();
        for i in 1..100u64 {
            f.apply(&global_edge(0, i));
        }
        let all = f.members(GLOBAL_SCOPE, 50, 1000).unwrap();
        assert!(!all.is_truncated());
        assert_eq!(all.nodes().len(), 100);

        let capped = f.members(GLOBAL_SCOPE, 50, 10).unwrap();
        assert!(capped.is_truncated(), "100 members do not fit in 10");
        assert_eq!(capped.nodes().len(), 10, "exactly the cap, not one more");
        for &m in capped.nodes() {
            assert_eq!(
                f.scope_root(GLOBAL_SCOPE, m),
                0,
                "truncation must not invent members"
            );
        }

        // Exactly at the cap is complete, not truncated: the off-by-one here
        // would tell an operator a whole component was a hub.
        assert!(!f.members(GLOBAL_SCOPE, 50, 100).unwrap().is_truncated());
        assert!(f.members(GLOBAL_SCOPE, 50, 99).unwrap().is_truncated());
    }

    /// A scoped component spans several shared components, and the overlay only
    /// holds their roots. Walking one level would return the roots and call it
    /// the membership.
    #[test]
    fn a_scoped_walk_descends_through_the_shared_tier() {
        let f = ScopedForest::new().tracking_members();
        // Two shared components, roots 10 and 30.
        f.apply(&global_edge(10, 11));
        f.apply(&global_edge(10, 12));
        f.apply(&global_edge(30, 31));
        // Scope 5 glues them together; nothing else sees that.
        f.apply(&scoped_edge(11, 31, &[5]));

        assert_eq!(members_of(&f, 5, 12, 100), vec![10, 11, 12, 30, 31]);
        assert_eq!(members_of(&f, GLOBAL_SCOPE, 12, 100), vec![10, 11, 12]);
        assert_eq!(members_of(&f, 6, 31, 100), vec![30, 31]);
    }

    /// The case that would catch keying the index on the root instead of on the
    /// parent: a global merge moves the root *downward* after the links were
    /// recorded, and everything above must still list completely.
    #[test]
    fn a_component_whose_root_moved_still_lists_completely() {
        let f = ScopedForest::new().tracking_members();
        for i in 501..510u64 {
            f.apply(&global_edge(500, i));
        }
        assert_eq!(f.scope_root(GLOBAL_SCOPE, 505), 500);
        // A much smaller id joins; the root drops from 500 to 3.
        f.apply(&global_edge(505, 3));
        assert_eq!(f.scope_root(GLOBAL_SCOPE, 505), 3);

        let mut expected: Vec<NodeId> = (500..510).collect();
        expected.push(3);
        expected.sort_unstable();
        assert_eq!(members_of(&f, GLOBAL_SCOPE, 509, 100), expected);
        assert_eq!(members_of(&f, GLOBAL_SCOPE, 3, 100), expected);
    }

    /// Off by default, and a forest that was not tracking says so rather than
    /// reporting every component as a singleton.
    #[test]
    fn without_the_index_there_is_no_answer() {
        let f = ScopedForest::new();
        f.apply(&global_edge(1, 2));
        assert!(!f.tracks_members());
        assert!(f.members(GLOBAL_SCOPE, 1, 10).is_none());
        assert_eq!(f.scope_root(GLOBAL_SCOPE, 2), 1, "queries are unaffected");
    }

    #[test]
    fn visibility_normalize_folds_global_scope_id() {
        let v = Visibility::Scoped(smallvec![GLOBAL_SCOPE, 3]);
        assert_eq!(v.normalize(), Visibility::Global);
    }
}
