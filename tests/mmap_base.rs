//! Disk-backed routing base: mmap'd committed state + in-memory memtable.
//!
//! These tests drive the real path — flush to an object store, cache and mmap
//! the Puffin snapshot, then serve and mutate through the composed
//! base+memtable forest — and check every answer against a BFS reference
//! model (invariants I1/I2/I6 from docs/design).

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as StorePath;

use blaze::core::{EdgeEvent, GLOBAL_SCOPE, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, SnapshotCatalog, open_base_from_catalog};

fn global_edge(src: u64, dst: u64) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: Visibility::Global,
        event_time_ms: 1_700_000_000_000,
        props: None,
    }
}

fn scoped_edge(src: u64, dst: u64, scopes: &[u32]) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: Visibility::Scoped(scopes.iter().copied().collect()),
        event_time_ms: 1_700_000_000_000,
        props: None,
    }
}

/// Reference model: per-scope BFS over (global ∪ scope) edges; the canonical
/// root is the component minimum.
#[derive(Default)]
struct RefModel {
    global_edges: Vec<(u64, u64)>,
    scope_edges: Vec<(u32, u64, u64)>,
}

impl RefModel {
    fn component(&self, scope: u32, u: u64) -> std::collections::HashSet<u64> {
        let mut adj: std::collections::HashMap<u64, Vec<u64>> = std::collections::HashMap::new();
        let add = |a: u64, b: u64, adj: &mut std::collections::HashMap<u64, Vec<u64>>| {
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
        let mut seen = std::collections::HashSet::from([u]);
        let mut stack = vec![u];
        while let Some(x) = stack.pop() {
            for &n in adj.get(&x).map(|v| v.as_slice()).unwrap_or(&[]) {
                if seen.insert(n) {
                    stack.push(n);
                }
            }
        }
        seen
    }

    fn connected(&self, scope: u32, u: u64, v: u64) -> bool {
        u == v || self.component(scope, u).contains(&v)
    }

    fn component_min(&self, scope: u32, u: u64) -> u64 {
        self.component(scope, u).into_iter().min().unwrap()
    }
}

struct Warehouse {
    store: Arc<dyn ObjectStore>,
    catalog: Arc<SnapshotCatalog>,
    prefix: StorePath,
    _dir: tempfile::TempDir,
    cache: tempfile::TempDir,
    /// The worker's layer stack, carried between ticks.
    held: std::sync::Mutex<Option<blaze::storage::LocalLayers>>,
}

impl Warehouse {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let prefix = StorePath::from("graph/edges");
        let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
        Self {
            store,
            catalog,
            prefix,
            _dir: dir,
            cache: tempfile::tempdir().unwrap(),
            held: std::sync::Mutex::new(None),
        }
    }

    /// Flush a forest's state as a committed snapshot (leader path), never
    /// folding — so the memtable is left exactly as the test built it.
    async fn commit(&self, forest: Arc<ScopedForest>, first_offset: u64, events: &[EdgeEvent]) {
        self.flush(forest, first_offset, events, u64::MAX, true)
            .await;
    }

    /// As `commit`, but folding the memtable into a fresh local base whenever
    /// it holds `fold_after` links or more.
    async fn commit_folding(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
    ) {
        self.flush(forest, first_offset, events, fold_after, true)
            .await;
    }

    async fn flush(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
    ) {
        self.flush_layered(forest, first_offset, events, fold_after, leader, usize::MAX)
            .await;
    }

    /// One flush tick. The layer stack is carried across calls in `held`, the
    /// way a long-lived worker's flusher carries its own — otherwise every tick
    /// would look like a fresh worker and never produce a delta.
    async fn flush_layered(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
        max_delta_layers: usize,
    ) {
        let buffer = Arc::new(EdgeBuffer::new());
        for (i, e) in events.iter().enumerate() {
            buffer.append(first_offset + i as u64, e);
        }
        let flusher = Flusher {
            forest,
            buffer,
            store: self.store.clone(),
            catalog: self.catalog.clone(),
            elector: Arc::new(StaticElector(leader)),
            table_prefix: self.prefix.clone(),
            worker_id: if leader {
                "test-leader"
            } else {
                "test-follower"
            }
            .into(),
            base_dir: Some(self.cache.path().to_path_buf()),
            fold_after_links: fold_after,
            max_delta_layers,
            layers: parking_lot::Mutex::new(self.held.lock().unwrap().take()),
        };
        flusher.tick().await.unwrap();
        *self.held.lock().unwrap() = flusher.layers.lock().take();
    }

    /// Cold-start a base-backed forest from the latest committed snapshot.
    async fn open_base_backed(&self) -> (Arc<ScopedForest>, u64) {
        let (base, watermark, local) =
            open_base_from_catalog(&self.store, &self.catalog, self.cache.path())
                .await
                .unwrap()
                .expect("a committed snapshot");
        *self.held.lock().unwrap() = Some(local);
        (Arc::new(ScopedForest::with_base(base)), watermark)
    }

    async fn latest_meta(&self) -> blaze::storage::SnapshotMeta {
        self.catalog.latest().await.unwrap().expect("a snapshot")
    }

    async fn puffin_len(&self, path: &str) -> usize {
        use object_store::ObjectStoreExt;
        self.store
            .get(&StorePath::from(path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .len()
    }

    async fn delete_snapshot_meta(&self, sequence: u64) {
        use object_store::ObjectStoreExt;
        let path = self
            .prefix
            .clone()
            .join("metadata")
            .join(format!("snap-{sequence:012}.json"));
        ObjectStoreExt::delete(&*self.store, &path).await.unwrap();
    }
}

/// The case that breaks every shortcut: a scope's overlay state lives *only*
/// in the mmap'd base, and then a shared root it references gets absorbed.
/// The fix-up has to be discovered through the base's registry index and
/// applied to composed overlay operands.
#[tokio::test]
async fn merge_of_base_resident_root_still_notifies_scope() {
    let wh = Warehouse::new();

    // Cycle 1 (all RAM): one shared merge and one scope-7 edge.
    let first = [global_edge(10, 11), scoped_edge(1, 2, &[7])];
    let ram = Arc::new(ScopedForest::new());
    for e in &first {
        ram.apply(e);
    }
    wh.commit(ram, 1, &first).await;

    // Cycle 2: cold start with the snapshot mmap'd; memtable is empty.
    let (forest, watermark) = wh.open_base_backed().await;
    assert_eq!(watermark, 2);
    let stats = forest.base_stats().expect("base attached");
    assert_eq!(stats.shared_pairs, 1); // 11 -> 10
    assert_eq!(stats.overlay_pairs, 1); // scope 7: 2 -> 1
    assert!(stats.registry_indexed);

    // Served straight from disk.
    assert!(forest.connected(7, 1, 2));
    assert!(forest.connected(GLOBAL_SCOPE, 10, 11));
    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 11), 10);
    assert!(!forest.connected(8, 1, 2), "scope 8 must not see scope 7");

    // Now absorb base-resident shared root 1 into a *lower* new root 0. The
    // scope-7 overlay class {1,2} exists only in the base, so this only works
    // if the base registry is consulted and the overlay operands composed.
    forest.apply(&global_edge(1, 0));

    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 1), 0);
    assert!(
        forest.connected(7, 2, 0),
        "scope 7 must follow node 2 -> 1 -> 0 across the base/memtable boundary"
    );
    assert_eq!(forest.scope_root(7, 2), 0, "canonical root must be the min");
    assert!(
        !forest.connected(8, 2, 0),
        "the scope-7 edge must stay invisible to scope 8"
    );
    assert!(!forest.connected(GLOBAL_SCOPE, 2, 0));

    // Compaction from a base-backed forest must emit the *composed* state, so
    // the next base subsumes both layers and a fresh worker agrees.
    let more = [global_edge(1, 0)];
    wh.commit(forest.clone(), 3, &more).await;
    let (fresh, _) = wh.open_base_backed().await;
    assert!(fresh.connected(7, 2, 0));
    assert_eq!(fresh.scope_root(7, 2), 0);
    assert!(!fresh.connected(8, 2, 0));
    assert!(fresh.connected(GLOBAL_SCOPE, 10, 11));
    // ...and the new base really did absorb the old one.
    let fresh_stats = fresh.base_stats().unwrap();
    assert!(fresh_stats.shared_pairs >= stats.shared_pairs);
    assert!(fresh_stats.overlay_pairs >= stats.overlay_pairs);
}

/// Randomized state driven through repeated compaction cycles, each cycle
/// serving from an mmap'd base and mutating in the memtable on top of it.
#[tokio::test]
async fn base_backed_forest_matches_model_across_cycles() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const NODES: u64 = 90;
    const SCOPES: [u32; 3] = [1, 2, 3];
    const CYCLES: usize = 3;
    const PER_CYCLE: usize = 120;

    let mut rng = StdRng::seed_from_u64(0x5EED_BA5E);
    let wh = Warehouse::new();
    let mut model = RefModel::default();
    let mut offset = 1u64;

    // Cycle 0 seeds the first base from an all-RAM forest.
    let mut forest = Arc::new(ScopedForest::new());

    for cycle in 0..CYCLES {
        let mut batch = Vec::with_capacity(PER_CYCLE);
        for _ in 0..PER_CYCLE {
            let u = rng.random_range(0..NODES);
            let v = rng.random_range(0..NODES);
            let event = if rng.random_range(0..100) < 30 {
                model.global_edges.push((u, v));
                global_edge(u, v)
            } else {
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                model.scope_edges.push((s, u, v));
                scoped_edge(u, v, &[s])
            };
            forest.apply(&event);
            batch.push(event);
        }

        // Every answer must match the model *before* compaction too — this is
        // the composed base+memtable path once cycle >= 1.
        for _ in 0..250 {
            let a = rng.random_range(0..NODES);
            let b = rng.random_range(0..NODES);
            let s = if rng.random_range(0..4) == 0 {
                GLOBAL_SCOPE
            } else {
                SCOPES[rng.random_range(0..SCOPES.len())]
            };
            assert_eq!(
                forest.connected(s, a, b),
                model.connected(s, a, b),
                "cycle {cycle}: live connectivity diverged in scope {s} ({a},{b})"
            );
            assert_eq!(
                forest.scope_root(s, a),
                model.component_min(s, a),
                "cycle {cycle}: live root({a}) in scope {s} is not the component min"
            );
        }

        wh.commit(forest.clone(), offset, &batch).await;
        offset += batch.len() as u64;

        // Cold start on the freshly committed base and re-verify from disk.
        let (next, watermark) = wh.open_base_backed().await;
        assert_eq!(watermark, offset - 1);
        let stats = next.base_stats().expect("base attached");
        assert!(stats.mapped_bytes > 0);
        assert!(stats.registry_indexed);

        for _ in 0..250 {
            let a = rng.random_range(0..NODES);
            let b = rng.random_range(0..NODES);
            let s = if rng.random_range(0..4) == 0 {
                GLOBAL_SCOPE
            } else {
                SCOPES[rng.random_range(0..SCOPES.len())]
            };
            assert_eq!(
                next.connected(s, a, b),
                model.connected(s, a, b),
                "cycle {cycle}: mmap base connectivity diverged in scope {s} ({a},{b})"
            );
            assert_eq!(
                next.scope_root(s, a),
                model.component_min(s, a),
                "cycle {cycle}: mmap base root({a}) in scope {s} is not the component min"
            );
        }

        // Continue on the base-backed forest: the next cycle's merges land in
        // the memtable *on top of* the mapped base.
        forest = next;
    }
}

/// Compaction reads the base as a stream and merge-joins it against the
/// memtable. That merge is where a composed forest can silently drop or
/// duplicate a key, so assert the streamed blobs equal the ones built from a
/// fully materialized snapshot — over state that spans both tiers.
#[tokio::test]
async fn streamed_compaction_matches_the_materialized_snapshot() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    let wh = Warehouse::new();
    let mut rng = StdRng::seed_from_u64(0x571EA1);
    let mut events = Vec::new();
    let ram = Arc::new(ScopedForest::new());
    for _ in 0..600 {
        let e = if rng.random_range(0..100) < 30 {
            global_edge(rng.random_range(0..300), rng.random_range(0..300))
        } else {
            scoped_edge(
                rng.random_range(0..300),
                rng.random_range(0..300),
                &[rng.random_range(1..=25u32)],
            )
        };
        ram.apply(&e);
        events.push(e);
    }
    wh.commit(ram, 1, &events).await;

    // Now half in the base, half in the memtable: keys interleave, and some
    // memtable merges absorb roots the base still stores pairs for.
    let (forest, _) = wh.open_base_backed().await;
    for _ in 0..400 {
        let e = if rng.random_range(0..100) < 40 {
            global_edge(rng.random_range(0..300), rng.random_range(0..300))
        } else {
            scoped_edge(
                rng.random_range(0..300),
                rng.random_range(0..300),
                &[rng.random_range(1..=25u32)],
            )
        };
        forest.apply(&e);
    }

    let streamed = blaze::storage::codec::compact_to_blobs(&forest, 2);
    let collected = blaze::storage::codec::snapshot_to_blobs(&forest.snapshot(), 2);
    let fields = |bs: &[blaze::storage::puffin::Blob]| {
        bs.iter()
            .map(|b| (b.blob_type.clone(), b.properties.clone(), b.data.to_vec()))
            .collect::<Vec<_>>()
    };
    assert!(streamed.len() > 10, "expected many scope blobs");
    assert_eq!(fields(&streamed), fields(&collected));

    // And each shared key appears exactly once despite living in both tiers.
    let snap = forest.snapshot();
    let mut keys: Vec<u64> = snap.global.iter().map(|(k, _)| *k).collect();
    let unique = {
        let mut k = keys.clone();
        k.sort_unstable();
        k.dedup();
        k
    };
    keys.sort_unstable();
    assert_eq!(keys, unique, "compaction emitted a duplicate shared key");
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "output must be sorted"
    );
}

/// The memtable has to be foldable into a fresh base **while the worker runs**.
/// Compaction alone only reads the forest, so without a fold a cold start's
/// small memtable is an initial condition rather than a steady state, and the
/// process drifts back to heap-resident. Drive many folds over randomized
/// state and require every answer to keep matching the model.
#[tokio::test]
async fn repeated_folds_bound_the_memtable_without_changing_answers() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const NODES: u64 = 400;
    const SCOPES: [u32; 3] = [1, 2, 3];
    const ROUNDS: usize = 6;
    const PER_ROUND: usize = 150;
    const FOLD_AFTER: u64 = 40;

    let mut rng = StdRng::seed_from_u64(0xF01D);
    let wh = Warehouse::new();
    let mut model = RefModel::default();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    let mut peak_memtable = 0u64;
    let mut expected_folds = 0u64;

    for round in 0..ROUNDS {
        let mut batch = Vec::with_capacity(PER_ROUND);
        for _ in 0..PER_ROUND {
            let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            let event = if rng.random_range(0..100) < 30 {
                model.global_edges.push((u, v));
                global_edge(u, v)
            } else {
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                model.scope_edges.push((s, u, v));
                scoped_edge(u, v, &[s])
            };
            forest.apply(&event);
            batch.push(event);
        }
        // Links saturate once a scope's nodes are all one component, so a late
        // round may legitimately not reach the trigger. Predict from the same
        // signal the flusher uses rather than assuming one fold per round.
        let pending = forest.memtable_links();
        peak_memtable = peak_memtable.max(pending);
        if pending >= FOLD_AFTER {
            expected_folds += 1;
        }

        wh.commit_folding(forest.clone(), offset, &batch, FOLD_AFTER)
            .await;
        offset += batch.len() as u64;

        // The fold happened in place: this is the *same* forest object the
        // queries below and the next round's writes go through.
        let stats = forest.stats();
        assert_eq!(stats.folds, expected_folds, "round {round}: fold not taken");
        if pending >= FOLD_AFTER {
            assert_eq!(
                forest.memtable_links(),
                0,
                "round {round}: fold must leave the memtable empty"
            );
            assert!(stats.base_shared_pairs + stats.base_overlay_pairs > 0);
        }

        for _ in 0..300 {
            let (a, b) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            let s = if rng.random_range(0..4) == 0 {
                GLOBAL_SCOPE
            } else {
                SCOPES[rng.random_range(0..SCOPES.len())]
            };
            assert_eq!(
                forest.connected(s, a, b),
                model.connected(s, a, b),
                "round {round}: connectivity diverged after fold in scope {s} ({a},{b})"
            );
            assert_eq!(
                forest.scope_root(s, a),
                model.component_min(s, a),
                "round {round}: root({a}) in scope {s} is not the component min after fold"
            );
        }
    }

    // The point of the exercise: heap stayed bounded by the fold trigger while
    // the state behind it grew round after round.
    assert!(
        peak_memtable > FOLD_AFTER,
        "the trigger was never exercised"
    );
    let final_stats = forest.stats();
    assert!(
        expected_folds >= 3,
        "expected several folds over {ROUNDS} rounds, got {expected_folds}"
    );
    assert_eq!(final_stats.folds, expected_folds);
    assert!(
        final_stats.base_shared_pairs + final_stats.base_overlay_pairs > peak_memtable,
        "the base should now hold more than the memtable ever did"
    );
}

/// Followers serve from the same structures and grow at the same rate, so
/// folding only on the leader would relocate the leak rather than fix it. A
/// follower must fold and must still commit nothing.
#[tokio::test]
async fn followers_fold_but_never_commit() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut events = Vec::new();
    for i in 0..80u64 {
        let e = global_edge(i, i + 1);
        forest.apply(&e);
        events.push(e);
    }
    assert_eq!(forest.memtable_links(), 80);

    wh.flush(forest.clone(), 1, &events, 10, false).await;

    assert_eq!(forest.stats().folds, 1, "a follower must still fold");
    assert_eq!(forest.memtable_links(), 0);
    assert!(
        wh.catalog.latest().await.unwrap().is_none(),
        "a follower must not commit a snapshot"
    );
    // ...and it still answers over the whole chain, now from its local base.
    assert!(forest.connected(GLOBAL_SCOPE, 0, 80));
    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 80), 0);
}

/// A fold rewrites the base under live queries. Readers are never blocked, so
/// assert directly that concurrent lookups see no intermediate state — every
/// answer is either the pre-fold or the post-fold one, and those are equal.
#[tokio::test]
async fn queries_see_no_torn_state_while_folding() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut events = Vec::new();
    for i in 0..400u64 {
        // Two long chains, so a torn read would be obvious: any node's root is
        // the chain head, and the answer must never be an intermediate node.
        let e = global_edge(i, i + 1);
        forest.apply(&e);
        events.push(e);
        let e = scoped_edge(10_000 + i, 10_001 + i, &[9]);
        forest.apply(&e);
        events.push(e);
    }

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let f = forest.clone();
            std::thread::spawn(move || {
                for _ in 0..20_000 {
                    // Global chain 0..=400: every node's root is 0.
                    assert_eq!(f.scope_root(GLOBAL_SCOPE, 400), 0);
                    // Scope 9 additionally sees 10_000..=10_400, a component
                    // disjoint from the global chain, so its min is 10_000.
                    assert_eq!(f.scope_root(9, 10_400), 10_000);
                    assert_eq!(f.scope_root(9, 400), 0);
                    assert!(f.connected(9, 10_000, 10_400));
                    assert!(!f.connected(GLOBAL_SCOPE, 0, 10_000));
                    assert!(!f.connected(9, 0, 10_000));
                }
            })
        })
        .collect();

    wh.commit_folding(forest.clone(), 1, &events, 10).await;
    for r in readers {
        r.join()
            .expect("a reader observed torn state during the fold");
    }
    assert_eq!(forest.stats().folds, 1);
    assert_eq!(forest.memtable_links(), 0);
}

/// The point of delta snapshots: successive flushes commit only what changed,
/// so a tick's Puffin payload is a fraction of the base — and a cold start
/// still reconstructs identical topology from base + chain.
#[tokio::test]
async fn flushes_commit_deltas_not_whole_bases() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const NODES: u64 = 3_000;
    let mut rng = StdRng::seed_from_u64(0xDE17A);
    let wh = Warehouse::new();
    let mut model = RefModel::default();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;

    let batch = |rng: &mut StdRng, model: &mut RefModel, n: usize| {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
            let e = if rng.random_range(0..100) < 30 {
                model.global_edges.push((u, v));
                global_edge(u, v)
            } else {
                let s = rng.random_range(1..=12u32);
                model.scope_edges.push((s, u, v));
                scoped_edge(u, v, &[s])
            };
            forest.apply(&e);
            out.push(e);
        }
        out
    };

    // Tick 1 has no base to layer over, so it must be a full base.
    let first = batch(&mut rng, &mut model, 4_000);
    wh.flush_layered(forest.clone(), offset, &first, u64::MAX, true, 60)
        .await;
    offset += first.len() as u64;
    let base_meta = wh.latest_meta().await;
    assert_eq!(base_meta.base_sequence, 1, "first commit is its own base");
    assert_eq!(base_meta.delta_chain_len, 0);
    let base_bytes = wh.puffin_len(&base_meta.puffin_path).await;

    // Subsequent ticks add small deltas on top.
    let mut delta_bytes = Vec::new();
    for tick in 2..=5u64 {
        let more = batch(&mut rng, &mut model, 60);
        wh.flush_layered(forest.clone(), offset, &more, u64::MAX, true, 60)
            .await;
        offset += more.len() as u64;
        let meta = wh.latest_meta().await;
        assert_eq!(meta.sequence, tick);
        assert_eq!(meta.base_sequence, 1, "tick {tick} must layer on base 1");
        assert_eq!(meta.delta_chain_len, tick - 1);
        delta_bytes.push(wh.puffin_len(&meta.puffin_path).await);
    }

    // A delta covering 60 events must be far smaller than a base covering
    // thousands. This is the entire cost argument, so assert it rather than
    // trusting it.
    let biggest = *delta_bytes.iter().max().unwrap();
    assert!(
        biggest * 10 < base_bytes,
        "delta {biggest} B should be an order of magnitude under base {base_bytes} B"
    );

    // Cold start over base + 4 deltas must agree with the model everywhere.
    let (cold, watermark) = wh.open_base_backed().await;
    assert_eq!(watermark, offset - 1);
    for _ in 0..400 {
        let (a, b) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
        let s = if rng.random_range(0..4) == 0 {
            GLOBAL_SCOPE
        } else {
            rng.random_range(1..=12u32)
        };
        assert_eq!(
            cold.connected(s, a, b),
            model.connected(s, a, b),
            "chain cold start diverged in scope {s} ({a},{b})"
        );
        assert_eq!(
            cold.scope_root(s, a),
            model.component_min(s, a),
            "chain cold start root({a}) in scope {s} is not the component min"
        );
    }

    // RAM mode replays the same chain by applying deltas in sequence order.
    let ram = Arc::new(ScopedForest::new());
    let ram_watermark = blaze::storage::hydrate_from_catalog(&ram, &wh.store, &wh.catalog)
        .await
        .unwrap();
    assert_eq!(ram_watermark, watermark);
    for _ in 0..400 {
        let (a, b) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
        let s = rng.random_range(1..=12u32);
        assert_eq!(ram.connected(s, a, b), model.connected(s, a, b));
        assert_eq!(ram.scope_root(s, a), model.component_min(s, a));
    }
}

/// Compaction has to fire on the layer trigger and reset the chain, or lookups
/// pay an unbounded layer scan and cold starts an unbounded fetch.
#[tokio::test]
async fn compaction_fires_on_the_layer_trigger_and_resets_the_chain() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    // Base at 1, deltas at 2 and 3, then tick 4 must compact.
    const MAX_LAYERS: usize = 3;

    let mut sequences = Vec::new();
    for tick in 0..5u64 {
        let events: Vec<EdgeEvent> = (0..20)
            .map(|i| global_edge(tick * 100 + i, tick * 100 + i + 1))
            .collect();
        for e in &events {
            forest.apply(e);
        }
        wh.flush_layered(forest.clone(), offset, &events, u64::MAX, true, MAX_LAYERS)
            .await;
        offset += events.len() as u64;
        let meta = wh.latest_meta().await;
        sequences.push((meta.sequence, meta.base_sequence, meta.delta_chain_len));
    }

    assert_eq!(
        sequences,
        vec![
            (1, 1, 0), // no base yet -> base
            (2, 1, 1), // delta
            (3, 1, 2), // delta
            (4, 4, 0), // chain would hit MAX_LAYERS -> compact
            (5, 4, 1), // delta on the new base
        ],
        "chain must reset when compaction fires"
    );

    // Every chain up to here is still readable, and the compacted base really
    // did absorb the deltas below it.
    let (cold, _) = wh.open_base_backed().await;
    for tick in 0..5u64 {
        assert!(
            cold.connected(GLOBAL_SCOPE, tick * 100, tick * 100 + 20),
            "tick {tick}'s chain was lost across compaction"
        );
        assert_eq!(cold.scope_root(GLOBAL_SCOPE, tick * 100 + 20), tick * 100);
    }
    // Only base + one delta are mapped after the reset.
    assert_eq!(cold.base_stats().unwrap().sequence, 5);
}

/// A worker that folded locally without committing must not then commit a
/// delta: the committed chain would be missing those layers, and a cold start
/// would silently reconstruct incomplete topology.
#[tokio::test]
async fn a_worker_with_local_only_layers_commits_a_base_not_a_delta() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());

    // Establish a committed base.
    let first: Vec<EdgeEvent> = (0..30u64).map(|i| global_edge(i, i + 1)).collect();
    for e in &first {
        forest.apply(e);
    }
    wh.flush_layered(forest.clone(), 1, &first, u64::MAX, true, 60)
        .await;
    assert_eq!(wh.latest_meta().await.base_sequence, 1);

    // Now fold as a *follower*: a local layer that the catalog never saw.
    let hidden: Vec<EdgeEvent> = (0..30u64)
        .map(|i| global_edge(1_000 + i, 1_001 + i))
        .collect();
    for e in &hidden {
        forest.apply(e);
    }
    wh.flush_layered(forest.clone(), 31, &hidden, 1, false, 60)
        .await;
    assert_eq!(forest.stats().folds, 2, "the follower folded locally");
    assert_eq!(
        wh.latest_meta().await.sequence,
        1,
        "a follower must not commit"
    );

    // Becoming leader, the next commit must be a full base — not a delta whose
    // chain is missing the local-only layer.
    let more: Vec<EdgeEvent> = (0..5u64)
        .map(|i| global_edge(2_000 + i, 2_001 + i))
        .collect();
    for e in &more {
        forest.apply(e);
    }
    wh.flush_layered(forest.clone(), 61, &more, u64::MAX, true, 60)
        .await;
    let meta = wh.latest_meta().await;
    assert_eq!(meta.sequence, 2);
    assert_eq!(
        meta.base_sequence, 2,
        "must self-describe as a base after local-only folds"
    );
    assert_eq!(meta.delta_chain_len, 0);

    // The proof that matters: a cold start sees the state that only ever
    // existed in the local-only layer.
    let (cold, _) = wh.open_base_backed().await;
    assert!(
        cold.connected(GLOBAL_SCOPE, 1_000, 1_030),
        "local-only state was lost"
    );
    assert!(cold.connected(GLOBAL_SCOPE, 0, 30));
    assert!(cold.connected(GLOBAL_SCOPE, 2_000, 2_005));
}

/// A hole in the chain must fail loudly (I5). Skipping a delta would serve
/// stale topology as though it were current, which is worse than not starting.
#[tokio::test]
async fn a_missing_chain_link_is_a_hard_error() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    for tick in 0..3u64 {
        let events: Vec<EdgeEvent> = (0..10)
            .map(|i| global_edge(tick * 50 + i, tick * 50 + i + 1))
            .collect();
        for e in &events {
            forest.apply(e);
        }
        wh.flush_layered(forest.clone(), offset, &events, u64::MAX, true, 60)
            .await;
        offset += events.len() as u64;
    }
    assert_eq!(wh.latest_meta().await.delta_chain_len, 2);

    // Delete the middle link's metadata and read from a cache-cold worker.
    wh.delete_snapshot_meta(2).await;
    let cold = tempfile::tempdir().unwrap();
    let err = open_base_from_catalog(&wh.store, &wh.catalog, cold.path())
        .await
        .expect_err("a hole in the chain must not be silently skipped");
    assert!(
        err.to_string().contains("missing snapshot 2"),
        "error should name the missing link, got: {err}"
    );
}

/// A base-backed forest holds only post-snapshot merges in the heap: that is
/// the whole point of the disk tier.
#[tokio::test]
async fn memtable_stays_small_after_compaction() {
    let wh = Warehouse::new();
    let ram = Arc::new(ScopedForest::new());
    let mut events = Vec::new();
    for i in 0..200u64 {
        let e = global_edge(i, i + 1);
        ram.apply(&e);
        events.push(e);
    }
    let ram_links = ram.stats().global_links;
    assert_eq!(ram_links, 200);
    wh.commit(ram, 1, &events).await;

    let (forest, _) = wh.open_base_backed().await;
    let stats = forest.stats();
    assert_eq!(
        stats.global_links, 0,
        "a freshly opened base must leave the memtable empty"
    );
    assert_eq!(stats.base_shared_pairs, 200);
    // Still answers over the full 201-node chain, from disk.
    assert!(forest.connected(GLOBAL_SCOPE, 0, 200));
    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 200), 0);

    // One new merge => exactly one memtable link.
    forest.apply(&global_edge(200, 500));
    assert_eq!(forest.stats().global_links, 1);
    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 500), 0);
}
