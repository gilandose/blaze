//! A routing base assembled from a **base layer plus a chain of deltas**
//! (design 001), all mmap'd, presented as one [`RoutingBase`].
//!
//! # Why this exists
//!
//! Before this, every flush rewrote the whole routing map. At the target
//! profile that is ~32 GB per minute of Puffin payload for ~3 MB of actual
//! change, and a fold — the step that keeps the memtable from growing without
//! bound — stalled ingest for as long as it took to rewrite the base (measured
//! 2.95 s for 125 MB; minutes at 50 GB). Writing the memtable as a *new layer*
//! instead makes both costs O(change): a fold appends, it does not rewrite.
//!
//! # The property that makes lookups cheap: layer keys are disjoint
//!
//! Every layer is produced by resolving through everything beneath it, so a
//! layer's keys are always *composed roots of the layers below* — nodes those
//! layers store no parent for. Two consequences:
//!
//! 1. At most one layer holds a parent for a given node, so the first hit while
//!    scanning layers is the only hit; there is nothing to reconcile.
//! 2. Following that parent can only ever continue in a *newer* layer. So a
//!    resolution walks each layer at most once — `O(layers)` binary searches
//!    bounded by the compaction trigger, not `O(layers²)`.
//!
//! Roots also only ever decrease (canonical lowest-id-wins), so a newer layer
//! always refines an older answer downward: applying layers in order is
//! last-writer-wins by sequence, and that is exactly right.

use std::sync::Arc;

use crate::core::base::{BaseStats, RoutingBase, ScopeList};
use crate::core::{NodeId, ScopeId};
use crate::storage::base::PuffinBase;

/// Base layer plus deltas, oldest first.
#[derive(Debug)]
pub struct LayeredBase {
    /// `layers[0]` is the base; the rest are deltas in commit order. Never
    /// empty.
    layers: Vec<Arc<PuffinBase>>,
}

impl LayeredBase {
    /// A single base layer with no deltas.
    pub fn new(base: Arc<PuffinBase>) -> Self {
        Self { layers: vec![base] }
    }

    /// Base plus deltas in commit order.
    pub fn from_layers(layers: Vec<Arc<PuffinBase>>) -> anyhow::Result<Self> {
        anyhow::ensure!(!layers.is_empty(), "a layered base needs at least a base");
        Ok(Self { layers })
    }

    /// This base with `delta` appended as the newest layer. Cheap: the existing
    /// mappings are shared, nothing is copied or rewritten.
    pub fn pushed(&self, delta: Arc<PuffinBase>) -> Self {
        let mut layers = self.layers.clone();
        layers.push(delta);
        Self { layers }
    }

    /// Deltas layered on the base.
    pub fn delta_count(&self) -> usize {
        self.layers.len() - 1
    }

    /// The layers in `range` as a stack of their own — what a tiered merge reads.
    ///
    /// Merging a *subset* is sound, and the argument is worth spelling out
    /// because it is what tiering rests on. Write `M` for the merge of
    /// `L_i..=L_j`. Then:
    ///
    /// - **Order is preserved.** `M` takes position `i` in
    ///   `[L_0..L_i-1, M, L_j+1..]`, and `M` is by construction equivalent to
    ///   `L_i..=L_j`, so every resolution visits the same layers in the same
    ///   order and reaches the same answer.
    /// - **Keys stay disjoint.** `M`'s keys are the union of `L_i..=L_j`'s, which
    ///   were already disjoint from each other and from every other layer's.
    /// - **Resolution still advances monotonically.** A value in `M` is a
    ///   composed root of `L_i..=L_j`, so it is some `L_m` value with `m <= j`,
    ///   and an `L_m` value has no parent in `L_0..=L_m` — hence none in
    ///   `L_0..=L_i-1`. Continuing at `i + 1` after a hit in `M` therefore cannot
    ///   skip a parent.
    ///
    /// The third point is the one that would bite, and it is why a merged run may
    /// be spliced back in at the position it came from rather than having to go
    /// on top of the stack.
    pub fn slice(&self, range: std::ops::Range<usize>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            range.end <= self.layers.len(),
            "merge range {range:?} runs past the {} layers in the stack",
            self.layers.len()
        );
        Self::from_layers(self.layers[range].to_vec())
    }

    /// This stack with `range` replaced by the single layer that merged it.
    pub fn spliced(
        &self,
        range: std::ops::Range<usize>,
        merged: Arc<PuffinBase>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            range.start < range.end && range.end <= self.layers.len(),
            "splice range {range:?} does not name layers of the {} in the stack",
            self.layers.len()
        );
        let mut layers = Vec::with_capacity(self.layers.len() - range.len() + 1);
        layers.extend_from_slice(&self.layers[..range.start]);
        layers.push(merged);
        layers.extend_from_slice(&self.layers[range.end..]);
        Self::from_layers(layers)
    }

    pub fn layers(&self) -> usize {
        self.layers.len()
    }

    /// The `i`th layer, oldest first. Storage-side compaction merges the
    /// layers' registry runs directly, which needs positional access.
    pub fn layer(&self, i: usize) -> &PuffinBase {
        &self.layers[i]
    }

    /// Resolve `node` through the layers using `probe` to read one layer.
    ///
    /// Starts at `from` and only ever moves forward, which is sound because a
    /// value found in layer `i` was a composed root of layers `0..=i` and so
    /// cannot have a parent in any of them.
    fn resolve(
        &self,
        node: NodeId,
        mut probe: impl FnMut(&PuffinBase, NodeId) -> Option<NodeId>,
    ) -> Option<NodeId> {
        let mut cur = node;
        let mut from = 0usize;
        while from < self.layers.len() {
            let mut step = None;
            for (i, layer) in self.layers.iter().enumerate().skip(from) {
                if let Some(parent) = probe(layer, cur) {
                    step = Some((i, parent));
                    break; // keys are disjoint: the first hit is the only hit
                }
            }
            match step {
                Some((i, parent)) if parent != cur => {
                    cur = parent;
                    from = i + 1;
                }
                _ => break,
            }
        }
        (cur != node).then_some(cur)
    }
}

impl RoutingBase for LayeredBase {
    fn shared_parent(&self, node: NodeId) -> Option<NodeId> {
        self.resolve(node, |layer, x| layer.shared_parent(x))
    }

    fn overlay_parent(&self, scope: ScopeId, node: NodeId) -> Option<NodeId> {
        self.resolve(node, |layer, x| layer.overlay_parent(scope, x))
    }

    fn scopes_for_root(&self, root: NodeId) -> ScopeList {
        // A scope that keyed overlay state on this root in *any* layer still
        // has to be notified when it is absorbed, so this is a union.
        let mut out = ScopeList::new();
        for layer in &self.layers {
            for scope in layer.scopes_for_root(root) {
                if !out.contains(&scope) {
                    out.push(scope);
                }
            }
        }
        out
    }

    fn scopes(&self) -> Vec<ScopeId> {
        let mut out: Vec<ScopeId> = self.layers.iter().flat_map(|l| l.scopes()).collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    fn for_each_shared_pair(&self, f: &mut dyn FnMut(NodeId, NodeId)) {
        self.merge(
            |l| l.shared_len(),
            |l, i| l.shared_pair_at(i),
            |x| self.shared_parent(x),
            f,
        );
    }

    fn for_each_overlay_pair(&self, scope: ScopeId, f: &mut dyn FnMut(NodeId, NodeId)) {
        self.merge(
            |l| l.overlay_len(scope),
            |l, i| l.overlay_pair_at(scope, i),
            |x| self.overlay_parent(scope, x),
            f,
        );
    }

    fn stats(&self) -> BaseStats {
        let mut out = BaseStats::default();
        for layer in &self.layers {
            let s = layer.stats();
            out.sequence = out.sequence.max(s.sequence);
            out.shared_pairs += s.shared_pairs;
            out.overlay_pairs += s.overlay_pairs;
            out.mapped_bytes += s.mapped_bytes;
            out.index_bytes += s.index_bytes;
            // A layer with no overlay state at all would otherwise mask that
            // the ones below it have some.
            out.registry_indexed |= s.registry_indexed;
        }
        out.scopes = self.scopes().len() as u64;
        out
    }
}

impl LayeredBase {
    /// Stream every key across all layers exactly once, in ascending order,
    /// with its fully resolved value — the compaction read.
    ///
    /// A k-way merge over cursors rather than a concatenate-and-sort, so heap
    /// stays O(layers) no matter how much state is being compacted. Keys are
    /// disjoint across layers, so no deduplication is needed; only the *value*
    /// may need resolving, since a later layer can have re-rooted it without
    /// mentioning the key.
    fn merge(
        &self,
        len: impl Fn(&PuffinBase) -> usize,
        pair_at: impl Fn(&PuffinBase, usize) -> (NodeId, NodeId),
        resolve: impl Fn(NodeId) -> Option<NodeId>,
        f: &mut dyn FnMut(NodeId, NodeId),
    ) {
        let lens: Vec<usize> = self.layers.iter().map(|l| len(l)).collect();
        let mut cursors = vec![0usize; self.layers.len()];
        loop {
            // Layer count is bounded by the compaction trigger (tens), so a
            // linear scan for the minimum beats a heap's bookkeeping.
            let mut pick: Option<(usize, NodeId, NodeId)> = None;
            for (i, layer) in self.layers.iter().enumerate() {
                if cursors[i] >= lens[i] {
                    continue;
                }
                let (k, v) = pair_at(layer, cursors[i]);
                if pick.is_none_or(|(_, best, _)| k < best) {
                    pick = Some((i, k, v));
                }
            }
            let Some((i, k, v)) = pick else { return };
            cursors[i] += 1;
            let root = resolve(v).unwrap_or(v);
            if root != k {
                f(k, root);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ForestSnapshot;
    use crate::storage::codec::WriteOptions;
    use crate::storage::{codec, puffin};
    use std::collections::BTreeMap;

    /// Write a layer and map it, the way the flusher does.
    fn layer(
        dir: &std::path::Path,
        name: &str,
        snap: &ForestSnapshot,
        seq: u64,
    ) -> Arc<PuffinBase> {
        let bytes = puffin::write(
            &codec::snapshot_to_blobs(snap, seq, WriteOptions::default()),
            BTreeMap::new(),
        );
        let path = dir.join(format!("{name}.puffin"));
        std::fs::write(&path, &bytes).unwrap();
        Arc::new(PuffinBase::open(&path).unwrap())
    }

    fn snap(
        global: &[(NodeId, NodeId)],
        scopes: &[(ScopeId, Vec<(NodeId, NodeId)>)],
    ) -> ForestSnapshot {
        ForestSnapshot {
            global: global.to_vec(),
            scopes: scopes.to_vec(),
        }
    }

    /// The case the disjointness argument exists for: a delta re-roots a node
    /// the base already had a parent for, *without mentioning* the nodes
    /// pointing at it. Resolution has to keep walking into newer layers.
    #[test]
    fn a_delta_reroots_the_base_without_restating_it() {
        let dir = tempfile::tempdir().unwrap();
        // Base: 500 -> 105, 900 -> 105 (105 is the root).
        let base = layer(dir.path(), "base", &snap(&[(500, 105), (900, 105)], &[]), 1);
        // Delta: 105 absorbed into a new lower root 7. Nothing says 500 -> 7.
        let d1 = layer(dir.path(), "d1", &snap(&[(105, 7)], &[]), 2);
        let l = LayeredBase::from_layers(vec![base, d1]).unwrap();

        assert_eq!(l.shared_parent(105), Some(7));
        assert_eq!(l.shared_parent(500), Some(7), "must follow 500 -> 105 -> 7");
        assert_eq!(l.shared_parent(900), Some(7));
        assert_eq!(l.shared_parent(7), None, "7 is the root");
        assert_eq!(l.shared_parent(4242), None);
        assert_eq!(l.delta_count(), 1);
    }

    /// Roots only decrease, so a node re-rooted in several successive deltas
    /// must resolve to the newest (lowest) one.
    #[test]
    fn repeated_rerooting_resolves_to_the_newest_root() {
        let dir = tempfile::tempdir().unwrap();
        let base = layer(dir.path(), "base", &snap(&[(900, 500)], &[]), 1);
        let d1 = layer(dir.path(), "d1", &snap(&[(500, 105)], &[]), 2);
        let d2 = layer(dir.path(), "d2", &snap(&[(105, 40)], &[]), 3);
        let d3 = layer(dir.path(), "d3", &snap(&[(40, 3)], &[]), 4);
        let l = LayeredBase::from_layers(vec![base, d1, d2, d3]).unwrap();

        for node in [900, 500, 105, 40] {
            assert_eq!(l.shared_parent(node), Some(3), "node {node}");
        }
        assert_eq!(l.shared_parent(3), None);
        assert_eq!(l.stats().sequence, 4);
    }

    /// Compaction reads the layers as one sorted stream of resolved pairs, so
    /// the result must be exactly what a single flattened base would hold.
    #[test]
    fn merge_emits_every_key_once_sorted_and_resolved() {
        let dir = tempfile::tempdir().unwrap();
        let base = layer(
            dir.path(),
            "base",
            &snap(&[(900, 500), (700, 500), (12, 4)], &[(7, vec![(500, 9)])]),
            1,
        );
        let d1 = layer(
            dir.path(),
            "d1",
            &snap(
                &[(500, 105), (33, 4)],
                &[(7, vec![(9, 2)]), (8, vec![(6, 1)])],
            ),
            2,
        );
        let d2 = layer(dir.path(), "d2", &snap(&[(105, 40)], &[]), 3);
        let l = LayeredBase::from_layers(vec![base, d1, d2]).unwrap();

        let mut shared = Vec::new();
        l.for_each_shared_pair(&mut |k, v| shared.push((k, v)));
        assert_eq!(
            shared,
            vec![(12, 4), (33, 4), (105, 40), (500, 40), (700, 40), (900, 40)],
            "keys ascending, each once, values fully resolved"
        );

        assert_eq!(l.scopes(), vec![7, 8]);
        let mut ov = Vec::new();
        l.for_each_overlay_pair(7, &mut |k, v| ov.push((k, v)));
        assert_eq!(ov, vec![(9, 2), (500, 2)], "overlay resolves across layers");
        let mut ov8 = Vec::new();
        l.for_each_overlay_pair(8, &mut |k, v| ov8.push((k, v)));
        assert_eq!(ov8, vec![(6, 1)]);

        // Merge-joining a layer that has no table for a scope must not skip the
        // layers that do.
        let mut none = Vec::new();
        l.for_each_overlay_pair(999, &mut |k, v| none.push((k, v)));
        assert!(none.is_empty());
    }

    #[test]
    fn registry_and_scopes_union_across_layers() {
        let dir = tempfile::tempdir().unwrap();
        let base = layer(dir.path(), "base", &snap(&[], &[(7, vec![(50, 9)])]), 1);
        let d1 = layer(dir.path(), "d1", &snap(&[], &[(8, vec![(50, 4)])]), 2);
        let l = LayeredBase::from_layers(vec![base, d1]).unwrap();

        let mut scopes = l.scopes_for_root(50).to_vec();
        scopes.sort_unstable();
        assert_eq!(scopes, vec![7, 8], "both layers reference root 50");
        assert!(l.scopes_for_root(12_345).is_empty());
    }

    #[test]
    fn rejects_an_empty_layer_list() {
        assert!(LayeredBase::from_layers(vec![]).is_err());
    }

    /// A merged run spliced back in at its old position must resolve exactly as
    /// the layers it replaced did — including when the value it hands back has a
    /// parent in a layer *above* the splice.
    ///
    /// This is the case the monotonic-advance argument is for. Merging `L0..=L1`
    /// gives `900 -> 105`, and 105 is re-rooted twice more above. Resolution has to
    /// continue at the layer just past the splice: stopping there loses the newer
    /// roots, and restarting from the bottom would rescan layers the merge already
    /// composed.
    #[test]
    fn a_spliced_merge_resolves_like_the_layers_it_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let l0 = layer(dir.path(), "l0", &snap(&[(900, 500)], &[]), 1);
        let l1 = layer(dir.path(), "l1", &snap(&[(500, 105)], &[]), 2);
        let l2 = layer(dir.path(), "l2", &snap(&[(105, 40)], &[]), 3);
        let l3 = layer(dir.path(), "l3", &snap(&[(40, 3)], &[]), 4);
        let flat = LayeredBase::from_layers(vec![l0, l1, l2.clone(), l3.clone()]).unwrap();

        // What merging L0..=L1 produces: every key resolved through those two.
        let sub = flat.slice(0..2).unwrap();
        let mut merged_pairs = Vec::new();
        sub.for_each_shared_pair(&mut |k, v| merged_pairs.push((k, v)));
        assert_eq!(merged_pairs, vec![(500, 105), (900, 105)]);
        let merged = layer(
            dir.path(),
            "merged",
            &snap(&merged_pairs, &[]),
            2, // inherits the span of what it subsumed
        );

        let tiered = flat.spliced(0..2, merged).unwrap();
        assert_eq!(tiered.layers(), 3, "two layers became one");
        for node in [900, 500, 105, 40] {
            assert_eq!(
                tiered.shared_parent(node),
                flat.shared_parent(node),
                "node {node} resolved differently after the splice"
            );
            assert_eq!(tiered.shared_parent(node), Some(3));
        }
        assert_eq!(tiered.shared_parent(3), None);

        // A splice in the *middle* keeps the layers on both sides in place, so
        // resolution has to enter the merged run from below and leave it upward.
        let sub = flat.slice(1..3).unwrap();
        let mut mid_pairs = Vec::new();
        sub.for_each_shared_pair(&mut |k, v| mid_pairs.push((k, v)));
        assert_eq!(mid_pairs, vec![(105, 40), (500, 40)]);
        let mid_merged = layer(dir.path(), "mid", &snap(&mid_pairs, &[]), 3);

        let mid = flat.spliced(1..3, mid_merged).unwrap();
        assert_eq!(mid.layers(), 3);
        for node in [900, 500, 105, 40] {
            assert_eq!(
                mid.shared_parent(node),
                Some(3),
                "node {node} must still walk L0 -> merged -> L3"
            );
        }
    }

    #[test]
    fn a_slice_or_splice_past_the_stack_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let base = layer(dir.path(), "base", &snap(&[(900, 500)], &[]), 1);
        let l = LayeredBase::new(base.clone());

        assert!(l.slice(0..2).is_err(), "range past the end");
        assert!(l.slice(0..0).is_err(), "empty slice has no base");
        assert!(l.spliced(0..0, base.clone()).is_err(), "empty splice range");
        assert!(l.spliced(0..2, base).is_err(), "range past the end");
    }
}
