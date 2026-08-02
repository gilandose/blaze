//! Design 011's member index, exercised across the base/memtable boundary.
//!
//! The unit tests in `core::scoped` grade `members` against BFS over the same
//! edges, but they run all-RAM: every merge edge is in the memtable, and the
//! downward walk never leaves it. That is the easy half. Everything interesting
//! about this index is on disk —
//!
//! - a component is split across a *stack* of runs, so children of one node
//!   live in a layer the walk has to keep scanning after the first hit;
//! - a node folded out to a run and then re-rooted in the memtable has to be
//!   reachable from a root the run has never heard of;
//! - a run written without `--member-index` makes the whole stack unable to
//!   answer, and must say so rather than under-reporting;
//! - tiered compaction rewrites runs, and the index has to survive that or the
//!   query disappears the first time a merge fires.
//!
//! So these drive real Puffin files through the real fold and compaction paths.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use blaze::core::{
    EdgeEvent, GLOBAL_SCOPE, Members, NodeId, RoutingBase, ScopeId, ScopedForest, Visibility,
};
use blaze::storage::codec::{self, WriteOptions};
use blaze::storage::{LayeredBase, PuffinBase, RegistryEncoding, compact_layers, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

fn indexed() -> WriteOptions {
    WriteOptions {
        member_index: true,
        ..Default::default()
    }
}

fn global(u: NodeId, v: NodeId) -> EdgeEvent {
    EdgeEvent {
        src: u,
        dst: v,
        visibility: Visibility::Global,
        event_time_ms: 0,
        props: None,
    }
}

fn scoped(u: NodeId, v: NodeId, scope: ScopeId) -> EdgeEvent {
    EdgeEvent {
        src: u,
        dst: v,
        visibility: Visibility::Scoped(smallvec::smallvec![scope]),
        event_time_ms: 0,
        props: None,
    }
}

fn write_layer(dir: &std::path::Path, name: &str, blobs: &[puffin::Blob]) -> Arc<PuffinBase> {
    let bytes = puffin::write(blobs, std::collections::BTreeMap::new());
    let path = dir.join(format!("{name}.puffin"));
    std::fs::write(&path, &bytes).unwrap();
    Arc::new(PuffinBase::open(&path).unwrap())
}

/// Fold the forest's memtable out as a new delta layer, exactly as the flusher
/// does, and return the grown stack.
fn fold(
    forest: &ScopedForest,
    dir: &std::path::Path,
    name: &str,
    seq: u64,
    layers: Vec<Arc<PuffinBase>>,
    opts: WriteOptions,
) -> Vec<Arc<PuffinBase>> {
    let mut writer = codec::BlobWriter::new(seq, opts);
    let dir = dir.to_path_buf();
    let name = name.to_string();
    forest
        .fold_delta(&mut writer, |w, _prev| {
            let layer = write_layer(&dir, &name, &w.finish());
            let mut next = layers;
            next.push(layer);
            let base: Arc<dyn RoutingBase> =
                Arc::new(LayeredBase::from_layers(next.clone()).unwrap());
            anyhow::Ok((base, next))
        })
        .unwrap()
}

/// BFS components over the edges actually applied — the same oracle the unit
/// tests use, kept here so the on-disk paths are graded against edges rather
/// than against another DSU.
#[derive(Default)]
struct Model {
    global: Vec<(NodeId, NodeId)>,
    scoped: Vec<(ScopeId, NodeId, NodeId)>,
}

impl Model {
    fn component(&self, scope: ScopeId, u: NodeId) -> Vec<NodeId> {
        let mut adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let add = |a: NodeId, b: NodeId, adj: &mut HashMap<NodeId, Vec<NodeId>>| {
            adj.entry(a).or_default().push(b);
            adj.entry(b).or_default().push(a);
        };
        for &(a, b) in &self.global {
            add(a, b, &mut adj);
        }
        for &(s, a, b) in &self.scoped {
            if s == scope {
                add(a, b, &mut adj);
            }
        }
        let mut seen = HashSet::from([u]);
        let mut stack = vec![u];
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
}

fn members(f: &ScopedForest, scope: ScopeId, node: NodeId, cap: usize) -> Vec<NodeId> {
    let m = f.members(scope, node, cap).expect("the stack is indexed");
    assert!(!m.is_truncated(), "cap {cap} was not meant to bite");
    let mut v = m.into_nodes();
    let n = v.len();
    v.sort_unstable();
    v.dedup();
    assert_eq!(n, v.len(), "members must not repeat a node");
    v
}

/// Five folds, so a component's merge edges are scattered across five runs plus
/// whatever is still in the memtable, and every answer graded against BFS.
#[test]
fn a_layered_stack_lists_components_exactly() {
    const NODES: u64 = 400;
    const SCOPES: [ScopeId; 3] = [1, 2, 3];
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(0x0B11_1DEA);

    let forest = ScopedForest::new().tracking_members();
    let mut model = Model::default();
    let mut layers: Vec<Arc<PuffinBase>> = Vec::new();

    for round in 0..5u64 {
        // Sparse enough that components stay small and plentiful, which is the
        // regime this index is for; one giant component would test one walk.
        for _ in 0..120 {
            let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            if rng.random_range(0..100) < 30 {
                forest.apply(&global(u, v));
                model.global.push((u, v));
            } else {
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                forest.apply(&scoped(u, v, s));
                model.scoped.push((s, u, v));
            }
        }
        layers = fold(
            &forest,
            dir.path(),
            &format!("layer-{round}"),
            round + 1,
            layers,
            indexed(),
        );

        // A handful of edges applied *after* the fold, so every check below
        // spans the base/memtable boundary rather than a freshly drained one.
        for _ in 0..15 {
            let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            forest.apply(&global(u, v));
            model.global.push((u, v));
        }

        for node in (0..NODES).step_by(7) {
            for &scope in &[GLOBAL_SCOPE, 1, 2, 3] {
                assert_eq!(
                    members(&forest, scope, node, NODES as usize),
                    model.component(scope, node),
                    "round {round}: scope {scope} members({node}) diverged over {} layers",
                    layers.len()
                );
            }
        }
    }
    assert_eq!(layers.len(), 5, "one run per fold");

    // A cold worker sees only the runs; the memtable half of every answer is
    // gone, so this is the check that the *written* index is complete on its
    // own for everything committed. Re-fold the tail first so nothing is left
    // in the heap.
    let layers = fold(&forest, dir.path(), "tail", 6, layers, indexed());
    let cold = ScopedForest::with_base(Arc::new(LayeredBase::from_layers(layers).unwrap()))
        .tracking_members();
    for node in (0..NODES).step_by(3) {
        for &scope in &[GLOBAL_SCOPE, 1, 2, 3] {
            assert_eq!(
                members(&cold, scope, node, NODES as usize),
                model.component(scope, node),
                "cold start: scope {scope} members({node}) diverged"
            );
        }
    }
}

/// Tiering merges runs. If the merged run drops the index, the stack stops
/// being able to answer at all — silently, from the operator's point of view,
/// because nothing else about the run changes.
#[test]
fn compaction_carries_the_index_through() {
    const NODES: u64 = 300;
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(0xC0117AC7);

    let forest = ScopedForest::new().tracking_members();
    let mut model = Model::default();
    let mut layers: Vec<Arc<PuffinBase>> = Vec::new();
    for round in 0..4u64 {
        for _ in 0..150 {
            let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            if rng.random_range(0..100) < 40 {
                forest.apply(&global(u, v));
                model.global.push((u, v));
            } else {
                let s = 1 + rng.random_range(0..3u32);
                forest.apply(&scoped(u, v, s));
                model.scoped.push((s, u, v));
            }
        }
        layers = fold(
            &forest,
            dir.path(),
            &format!("l{round}"),
            round + 1,
            layers,
            indexed(),
        );
    }
    // Graded against the pre-compaction stack rather than against BFS: the
    // property under test is that compaction preserves answers. BFS covers what
    // the answers should be, in the test above.
    let before = LayeredBase::from_layers(layers.clone()).unwrap();
    let reference = ScopedForest::with_base(Arc::new(before)).tracking_members();

    let stack = LayeredBase::from_layers(layers).unwrap();
    let (blobs, _) = compact_layers(
        &stack,
        99,
        WriteOptions {
            member_index: true,
            registry: RegistryEncoding::Blocked,
            ..Default::default()
        },
    );
    let merged = write_layer(dir.path(), "merged", &blobs);
    assert!(
        merged.has_member_index(),
        "the compacted run must carry the index it was asked for"
    );
    let after = ScopedForest::with_base(Arc::new(LayeredBase::new(merged))).tracking_members();

    for node in 0..NODES {
        for scope in [GLOBAL_SCOPE, 1, 2, 3] {
            let mut a = reference.members(scope, node, 1000).unwrap().into_nodes();
            let mut b = after.members(scope, node, 1000).unwrap().into_nodes();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(
                a, b,
                "scope {scope} members({node}) changed under compaction"
            );
        }
    }

    // And the flag still means something: compacting without it drops the blob,
    // so the stack can no longer answer.
    let (plain, _) = compact_layers(&stack, 100, WriteOptions::default());
    let bare = write_layer(dir.path(), "bare", &plain);
    assert!(!bare.has_member_index());
    let unindexed = ScopedForest::with_base(Arc::new(LayeredBase::new(bare))).tracking_members();
    assert!(
        unindexed.members(GLOBAL_SCOPE, 1, 10).is_none(),
        "an unindexed run must refuse, not under-report"
    );
}

/// One unindexed run in the stack is enough to make the whole thing
/// unanswerable, and that is the intended behaviour: the members it holds are
/// unreachable, and a short list would be indistinguishable from a small
/// component.
#[test]
fn a_mixed_stack_refuses_rather_than_under_reporting() {
    let dir = tempfile::tempdir().unwrap();
    let forest = ScopedForest::new().tracking_members();
    for i in 1..40u64 {
        forest.apply(&global(0, i));
    }
    let layers = fold(&forest, dir.path(), "indexed", 1, Vec::new(), indexed());
    let full = ScopedForest::with_base(Arc::new(LayeredBase::from_layers(layers.clone()).unwrap()))
        .tracking_members();
    assert_eq!(members(&full, GLOBAL_SCOPE, 20, 100).len(), 40);

    // A second fold that forgot the flag.
    for i in 40..60u64 {
        forest.apply(&global(0, i));
    }
    let mixed = fold(
        &forest,
        dir.path(),
        "plain",
        2,
        layers,
        WriteOptions::default(),
    );
    let stack = LayeredBase::from_layers(mixed).unwrap();
    assert!(!stack.has_member_index(), "one bare run poisons the stack");
    let f = ScopedForest::with_base(Arc::new(stack)).tracking_members();
    assert!(f.members(GLOBAL_SCOPE, 20, 100).is_none());
    assert_eq!(
        f.scope_root(GLOBAL_SCOPE, 55),
        0,
        "and every other query is unaffected"
    );
}

/// The truncation contract, on disk: exactly `cap` members, all real, and the
/// caller is told the set is partial. This is the answer for anything past the
/// percolation threshold, which is most of a production graph.
#[test]
fn a_hub_truncates_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let forest = ScopedForest::new().tracking_members();
    for i in 1..500u64 {
        forest.apply(&global(0, i));
    }
    let layers = fold(&forest, dir.path(), "hub", 1, Vec::new(), indexed());
    // Half in the run, half still in the memtable, so truncation has to happen
    // across both halves of the walk.
    for i in 500..800u64 {
        forest.apply(&global(0, i));
    }
    let _ = layers;

    let capped = forest.members(GLOBAL_SCOPE, 700, 50).unwrap();
    assert!(matches!(capped, Members::Truncated(_)));
    assert_eq!(capped.nodes().len(), 50);
    for &m in capped.nodes() {
        assert_eq!(forest.scope_root(GLOBAL_SCOPE, m), 0, "member {m} is real");
    }
    assert!(
        !forest
            .members(GLOBAL_SCOPE, 700, 800)
            .unwrap()
            .is_truncated()
    );
    assert_eq!(
        forest
            .members(GLOBAL_SCOPE, 700, 800)
            .unwrap()
            .nodes()
            .len(),
        800
    );
}
