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
use blaze::storage::{Flusher, RunSet, SnapshotCatalog, open_base_from_catalog};

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
    /// Likewise an in-flight merge, so a detached one survives to be adopted.
    held_merge: std::sync::Mutex<Option<blaze::storage::PendingMerge>>,
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
            held_merge: std::sync::Mutex::new(None),
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

    /// As `flush_tiered`, with tiering off: no level ever fills, so the only
    /// merge trigger is the depth ceiling. That is what the flat policy did, so
    /// tests written against it keep asserting exactly what they did before.
    async fn flush_layered(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
        max_delta_layers: usize,
    ) {
        self.flush_tiered(
            forest,
            first_offset,
            events,
            fold_after,
            leader,
            max_delta_layers,
            usize::MAX,
        )
        .await;
    }

    /// One flush tick. The layer stack is carried across calls in `held`, the
    /// way a long-lived worker's flusher carries its own — otherwise every tick
    /// would look like a fresh worker and never produce a delta.
    #[allow(clippy::too_many_arguments)]
    async fn flush_tiered(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
        max_delta_layers: usize,
        tier_fanout: usize,
    ) {
        self.flush_full(
            forest,
            first_offset,
            events,
            fold_after,
            leader,
            max_delta_layers,
            tier_fanout,
            true,
        )
        .await;
    }

    /// One flush tick with merges detached, the production default: a merge
    /// started here lands in a later tick.
    #[allow(clippy::too_many_arguments)]
    async fn flush_detached(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        max_delta_layers: usize,
        tier_fanout: usize,
    ) {
        self.flush_full(
            forest,
            first_offset,
            events,
            u64::MAX,
            true,
            max_delta_layers,
            tier_fanout,
            false,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_full(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
        max_delta_layers: usize,
        tier_fanout: usize,
        inline_merges: bool,
    ) {
        self.flush_all(
            forest,
            first_offset,
            events,
            fold_after,
            leader,
            max_delta_layers,
            tier_fanout,
            inline_merges,
            blaze::storage::DEFAULT_FILTER_BITS,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_all(
        &self,
        forest: Arc<ScopedForest>,
        first_offset: u64,
        events: &[EdgeEvent],
        fold_after: u64,
        leader: bool,
        max_delta_layers: usize,
        tier_fanout: usize,
        inline_merges: bool,
        filter_bits: usize,
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
            tier_fanout,
            filter_bits,
            inline_merges,
            layers: parking_lot::Mutex::new(self.held.lock().unwrap().take()),
            pending_merge: parking_lot::Mutex::new(self.held_merge.lock().unwrap().take()),
        };
        flusher.tick().await.unwrap();
        *self.held.lock().unwrap() = flusher.layers.lock().take();
        *self.held_merge.lock().unwrap() = flusher.pending_merge.lock().take();
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

    /// Rewrite every committed snapshot the way an earlier build wrote them —
    /// `runs` absent entirely, so `#[serde(default)]` has to carry it — leaving
    /// only `base_sequence` + `delta_chain_len` to describe the state.
    async fn downgrade_to_base_plus_chain(&self, through: u64) {
        use object_store::{ObjectStoreExt, PutPayload};
        for sequence in 1..=through {
            let path = self
                .prefix
                .clone()
                .join("metadata")
                .join(format!("snap-{sequence:012}.json"));
            let bytes = self.store.get(&path).await.unwrap().bytes().await.unwrap();
            let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let obj = json.as_object_mut().unwrap();
            obj.remove("runs");
            // An old writer emitted no version either, and leaving one behind
            // would make the snapshot claim a run set it does not carry — which
            // the reader is now right to reject.
            obj.remove("format_version");
            let payload = PutPayload::from(serde_json::to_vec(&json).unwrap());
            ObjectStoreExt::put(&*self.store, &path, payload)
                .await
                .unwrap();
        }
    }

    async fn delete_puffin(&self, path: &str) {
        use object_store::ObjectStoreExt;
        ObjectStoreExt::delete(&*self.store, &StorePath::from(path))
            .await
            .unwrap();
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

    let streamed = blaze::storage::codec::compact_to_blobs(
        &forest,
        2,
        blaze::storage::filter::DEFAULT_FILTER_BITS,
    );
    let collected = blaze::storage::codec::snapshot_to_blobs(
        &forest.snapshot(),
        2,
        blaze::storage::filter::DEFAULT_FILTER_BITS,
    );
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
            // Tick 4 hits MAX_LAYERS, so it merges at sequence 4 and then still
            // commits its batch at 5 rather than holding it a whole interval.
            (5, 4, 1),
            (6, 4, 2), // delta on the merged run
        ],
        "chain must reset when compaction fires"
    );

    // Every chain up to here is still readable, and the merged run really did
    // absorb the deltas below it.
    let (cold, _) = wh.open_base_backed().await;
    for tick in 0..5u64 {
        assert!(
            cold.connected(GLOBAL_SCOPE, tick * 100, tick * 100 + 20),
            "tick {tick}'s chain was lost across compaction"
        );
        assert_eq!(cold.scope_root(GLOBAL_SCOPE, tick * 100 + 20), tick * 100);
    }
    // The merged run plus the two deltas committed after it.
    assert_eq!(cold.base_stats().unwrap().sequence, 6);
}

/// Every commit must describe its state as a run set whose spans are dense and
/// contiguous from the first sequence to its own.
///
/// This is the property the format exists for, and the one that breaks quietly.
/// Runs resolve oldest-span-first, so a gap means some sequence's state belongs
/// to no run, and an overlap means two runs claim it — either way resolution
/// order stops being total and the disjoint-keys argument (a run's keys are
/// composed roots of every run with an earlier span) no longer holds. Nothing
/// else notices: lookups would still return *an* answer.
///
/// The interesting tick is the merge, where the output has to inherit the span
/// of everything it subsumed instead of taking the sequence it was committed at.
#[tokio::test]
async fn run_spans_stay_dense_across_a_compaction_cycle() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    // Base at 1, deltas at 2 and 3, then tick 4 must merge.
    const MAX_LAYERS: usize = 3;

    let mut shapes = Vec::new();
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
        let RunSet::Runs(runs) = meta.run_set().unwrap() else {
            panic!("tick {tick} committed no run set");
        };

        assert_eq!(
            runs.first().unwrap().min_sequence,
            1,
            "tick {tick}: the run set must reach back to the first sequence"
        );
        assert_eq!(
            runs.last().unwrap().max_sequence,
            meta.sequence,
            "tick {tick}: the newest run must cover this commit"
        );
        for pair in runs.windows(2) {
            assert!(
                pair[0].adjacent_to(&pair[1]),
                "tick {tick}: {}..{} and {}..{} are not adjacent",
                pair[0].min_sequence,
                pair[0].max_sequence,
                pair[1].min_sequence,
                pair[1].max_sequence
            );
        }
        // Every run names a distinct object; a repeated path would mean two runs
        // resolving the same bytes at different positions in the stack.
        let mut paths: Vec<&str> = runs.iter().map(|r| r.path.as_str()).collect();
        paths.sort_unstable();
        let distinct = paths.len();
        paths.dedup();
        assert_eq!(paths.len(), distinct, "tick {tick}: duplicate run path");

        shapes.push((
            meta.sequence,
            runs.iter()
                .map(|r| (r.level, r.min_sequence, r.max_sequence))
                .collect::<Vec<_>>(),
        ));
    }

    assert_eq!(
        shapes,
        vec![
            // No stack yet, so tick 1 folds a self-contained run — but it
            // subsumes nothing, so it is still level 0.
            (1, vec![(0, 1, 1)]),
            (2, vec![(0, 1, 1), (0, 2, 2)]),
            (3, vec![(0, 1, 1), (0, 2, 2), (0, 3, 3)]),
            // The merge inherits 1..=3 and extends over its own commit at 4, so
            // the set stays dense with no run left behind at it. The tick then
            // carries on and commits its batch at 5, which is why the sequence
            // jumps: a merge no longer costs a whole interval of watermark.
            (5, vec![(1, 1, 4), (0, 5, 5)]),
            (6, vec![(1, 1, 4), (0, 5, 5), (0, 6, 6)]),
        ],
        "run levels and spans across a merge"
    );
}

/// Compaction merges the *committed layer files*, taking no union lock and
/// never reading the forest — so the memtable it runs alongside must still be
/// there afterwards, holding everything that arrived during the merge.
///
/// This is the observable difference from the in-memory compactor, which drains
/// the memtable as part of the same locked step. Getting it wrong loses every
/// event applied since the last fold, so assert it directly rather than relying
/// on another test noticing the gap.
#[tokio::test]
async fn storage_side_compaction_preserves_the_memtable() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    const MAX_LAYERS: usize = 3;

    // Ticks 0..3 build base + 2 deltas.
    for tick in 0..3u64 {
        let events: Vec<EdgeEvent> = (0..10)
            .map(|i| global_edge(tick * 100 + i, tick * 100 + i + 1))
            .collect();
        for e in &events {
            forest.apply(e);
        }
        wh.flush_layered(forest.clone(), offset, &events, u64::MAX, true, MAX_LAYERS)
            .await;
        offset += events.len() as u64;
    }
    assert_eq!(forest.memtable_links(), 0, "each tick folded");

    // Now apply events that exist *only* in the memtable, then trigger the
    // compaction tick. Nothing has folded them into a layer.
    let orphans: Vec<EdgeEvent> = (0..10).map(|i| global_edge(9000 + i, 9001 + i)).collect();
    for e in &orphans {
        forest.apply(e);
    }
    let pending = forest.memtable_links();
    assert_eq!(pending, 10, "10 merges live only in the memtable");
    let folds_before = forest.stats().folds;

    wh.flush_layered(forest.clone(), offset, &orphans, u64::MAX, true, MAX_LAYERS)
        .await;

    // The merge is its own commit at sequence 4, carrying no data: it covers the
    // runs it read, not the memtable.
    let merge = wh.catalog.get(4).await.unwrap();
    assert!(merge.data_files.is_empty(), "a merge commits no data");
    let RunSet::Runs(merged) = merge.run_set().unwrap() else {
        panic!("the merge must publish a run set");
    };
    assert_eq!(
        merged
            .iter()
            .map(|r| (r.level, r.min_sequence, r.max_sequence))
            .collect::<Vec<_>>(),
        vec![(1, 1, 4)],
        "the merge subsumed the whole chain into one run"
    );

    // The tick then *continues* and commits this batch at sequence 5, so exactly
    // one fold happened — the ordinary end-of-tick one, after the merge, never
    // as part of it. Two folds, or a memtable already empty at merge time, would
    // mean the merge had swept it.
    let meta = wh.latest_meta().await;
    assert_eq!(
        (meta.sequence, meta.base_sequence, meta.delta_chain_len),
        (5, 4, 1),
        "the batch follows the merge rather than waiting a whole interval"
    );
    assert_eq!(
        forest.stats().folds,
        folds_before + 1,
        "storage-side compaction must not fold; only the tick's own fold may"
    );
    assert!(
        !meta.data_files.is_empty(),
        "the merge tick must not hold the batch back"
    );
    // The orphan merges reached storage in that delta rather than being lost,
    // which is what would happen if the merge had drained the memtable and then
    // lost its commit race.
    assert_eq!(pending, 10);

    // Both halves still answer: the compacted base, and the preserved memtable.
    for tick in 0..3u64 {
        assert!(forest.connected(GLOBAL_SCOPE, tick * 100, tick * 100 + 10));
    }
    assert!(
        forest.connected(GLOBAL_SCOPE, 9000, 9010),
        "memtable-only state was lost by the compaction"
    );
    assert_eq!(forest.scope_root(GLOBAL_SCOPE, 9010), 9000);
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

/// Runs must promote through levels, and every answer must survive it.
///
/// This is tiering itself. With a fanout of 3: three L0 runs merge into one L1,
/// three L1s into one L2, and each merge takes a *contiguous subset* of the stack
/// and splices the result back where it came from. That splice is the load-bearing
/// part — a merged run resolves at the position its inputs held, not on top — and
/// getting it wrong reorders resolution, which still returns plausible answers.
///
/// So the shape is asserted tick by tick, and then a cold start built purely from
/// the committed run set is checked against the live forest that applied every
/// event. Edges are chained within a tick and disjoint across ticks, so each tick
/// forms one component and a reordered stack shows up as a wrong root.
#[tokio::test]
async fn runs_promote_through_levels_without_changing_answers() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    const FANOUT: usize = 3;
    const TICKS: u64 = 14;

    let mut shapes = Vec::new();
    for tick in 0..TICKS {
        let events: Vec<EdgeEvent> = (0..8)
            .map(|i| global_edge(tick * 100 + i, tick * 100 + i + 1))
            .collect();
        for e in &events {
            forest.apply(e);
        }
        wh.flush_tiered(
            forest.clone(),
            offset,
            &events,
            u64::MAX,
            true,
            usize::MAX, // no depth ceiling: the level policy is what is under test
            FANOUT,
        )
        .await;
        offset += events.len() as u64;

        let meta = wh.latest_meta().await;
        let RunSet::Runs(runs) = meta.run_set().unwrap() else {
            panic!("tick {tick} committed no run set");
        };
        // Whatever the policy did, the set still has to be dense and cover this
        // commit — a partial merge leaves runs above it that must stay adjacent.
        assert_eq!(runs.first().unwrap().min_sequence, 1);
        assert_eq!(runs.last().unwrap().max_sequence, meta.sequence);
        for pair in runs.windows(2) {
            assert!(
                pair[0].adjacent_to(&pair[1]),
                "tick {tick}: spans stopped being dense"
            );
        }
        shapes.push(
            runs.iter()
                .map(|r| (r.level, r.min_sequence, r.max_sequence))
                .collect::<Vec<_>>(),
        );
    }

    assert_eq!(
        shapes,
        vec![
            vec![(0, 1, 1)],
            vec![(0, 1, 1), (0, 2, 2)],
            vec![(0, 1, 1), (0, 2, 2), (0, 3, 3)],
            // Three L0s are due. The merge takes sequence 4 and covers its own
            // commit; the tick then commits its batch as a delta at 5.
            vec![(1, 1, 4), (0, 5, 5)],
            vec![(1, 1, 4), (0, 5, 5), (0, 6, 6)],
            vec![(1, 1, 4), (0, 5, 5), (0, 6, 6), (0, 7, 7)],
            // A *subset* merge: the L1 below is untouched and keeps its position.
            vec![(1, 1, 4), (1, 5, 8), (0, 9, 9)],
            vec![(1, 1, 4), (1, 5, 8), (0, 9, 9), (0, 10, 10)],
            vec![(1, 1, 4), (1, 5, 8), (0, 9, 9), (0, 10, 10), (0, 11, 11)],
            vec![(1, 1, 4), (1, 5, 8), (1, 9, 12), (0, 13, 13)],
            // Three L1s now qualify, so they promote to one L2. Note the run
            // above the merge takes the span extension: it did not move, but it
            // is now the newest, so it is the one that covers sequence 14.
            vec![(2, 1, 12), (0, 13, 14), (0, 15, 15)],
            vec![(2, 1, 12), (0, 13, 14), (0, 15, 15), (0, 16, 16)],
            vec![(2, 1, 12), (1, 13, 17), (0, 18, 18)],
            vec![(2, 1, 12), (1, 13, 17), (0, 18, 18), (0, 19, 19)],
        ],
        "levels must promote and merges must splice in place"
    );
    assert!(
        shapes.iter().all(|s| s.len() <= 5),
        "run count must stay bounded as commits accumulate"
    );

    // A cold start assembled from the run set alone must answer identically to
    // the forest that applied every event.
    let cold = tempfile::tempdir().unwrap();
    let (base, _, local) = open_base_from_catalog(&wh.store, &wh.catalog, cold.path())
        .await
        .unwrap()
        .expect("a committed snapshot");
    assert_eq!(base.layers(), 4, "the L2 run, an L1, and two L0 deltas");
    assert!(local.fully_committed());
    let cold = Arc::new(ScopedForest::with_base(base));
    for tick in 0..TICKS {
        for i in 0..=8u64 {
            assert_eq!(
                cold.scope_root(GLOBAL_SCOPE, tick * 100 + i),
                forest.scope_root(GLOBAL_SCOPE, tick * 100 + i),
                "tick {tick} node {i} diverged after promotion"
            );
        }
        assert!(
            cold.connected(GLOBAL_SCOPE, tick * 100, tick * 100 + 8),
            "tick {tick}'s component was lost"
        );
        // Components must not have bled into each other across merges.
        if tick + 1 < TICKS {
            assert!(!cold.connected(GLOBAL_SCOPE, tick * 100, (tick + 1) * 100));
        }
    }
}

/// A merge must run in the background while folds and commits carry on, and its
/// output must splice in at the position its inputs held even though the stack
/// grew underneath it.
///
/// This is what the run-set format was built for, and the observable is sharp: a
/// detached merge is **published at a sequence later than the span it covers**.
/// `base_sequence` + `delta_chain_len` cannot express that at all — there is no
/// sequence at which "a base plus everything after it" describes a run that
/// covers 1..=3 sitting below a delta committed at 4.
///
/// The reason it matters is the memtable. Awaiting a merge inside the tick meant
/// nothing folded for its duration, so the memtable grew unbounded by the fold
/// trigger for however many minutes a high-level merge took.
#[tokio::test]
async fn a_merge_runs_detached_while_folds_carry_on() {
    let wh = Warehouse::new();
    let forest = Arc::new(ScopedForest::new());
    let mut offset = 1u64;
    const MAX_LAYERS: usize = 3;

    // Three ticks build three runs; the fourth finds the ceiling reached.
    for tick in 0..4u64 {
        let events: Vec<EdgeEvent> = (0..10)
            .map(|i| global_edge(tick * 100 + i, tick * 100 + i + 1))
            .collect();
        for e in &events {
            forest.apply(e);
        }
        wh.flush_detached(forest.clone(), offset, &events, MAX_LAYERS, usize::MAX)
            .await;
        offset += events.len() as u64;
    }

    // Tick 4 started the merge and did *not* wait for it: it folded and committed
    // its batch at sequence 4 regardless. That is the property — under the old
    // inline behaviour this commit could not have happened until the merge was
    // done.
    let during = wh.latest_meta().await;
    assert_eq!(during.sequence, 4);
    assert!(
        !during.data_files.is_empty(),
        "the tick that started the merge must still commit its batch"
    );
    assert_eq!(
        forest.memtable_links(),
        0,
        "the fold must have drained the memtable while the merge ran"
    );
    let RunSet::Runs(runs) = during.run_set().unwrap() else {
        panic!("expected a run set");
    };
    assert_eq!(
        runs.len(),
        4,
        "the merge has not landed yet, so all four runs are still listed"
    );

    // The next tick adopts it.
    let events: Vec<EdgeEvent> = (0..10).map(|i| global_edge(900 + i, 901 + i)).collect();
    for e in &events {
        forest.apply(e);
    }
    wh.flush_detached(forest.clone(), offset, &events, MAX_LAYERS, usize::MAX)
        .await;

    // Sequence 5 is the merge, 6 the batch that followed it.
    let merge = wh.catalog.get(5).await.unwrap();
    assert!(merge.data_files.is_empty(), "a merge commits no data");
    let RunSet::Runs(after) = merge.run_set().unwrap() else {
        panic!("expected a run set");
    };
    assert_eq!(
        after
            .iter()
            .map(|r| (r.level, r.min_sequence, r.max_sequence))
            .collect::<Vec<_>>(),
        // The merged run covers 1..=3 and sits *below* the delta committed at 4
        // while it was running — which then takes the span extension over 5.
        vec![(1, 1, 3), (0, 4, 5)],
        "the merge must splice in below the runs committed while it ran"
    );
    assert!(
        after[0].max_sequence < merge.sequence,
        "a detached merge is published later than the span it covers, which is \
         exactly what base_sequence + delta_chain_len cannot express"
    );

    // Everything still answers, from a cold start built only from the run set.
    let cold = tempfile::tempdir().unwrap();
    let (base, _, _) = open_base_from_catalog(&wh.store, &wh.catalog, cold.path())
        .await
        .unwrap()
        .expect("a committed snapshot");
    let cold = Arc::new(ScopedForest::with_base(base));
    for tick in 0..4u64 {
        assert!(
            cold.connected(GLOBAL_SCOPE, tick * 100, tick * 100 + 10),
            "tick {tick}'s component was lost across a detached merge"
        );
    }
    assert!(cold.connected(GLOBAL_SCOPE, 900, 910));
    for tick in 0..4u64 {
        for i in 0..=10u64 {
            assert_eq!(
                cold.scope_root(GLOBAL_SCOPE, tick * 100 + i),
                forest.scope_root(GLOBAL_SCOPE, tick * 100 + i),
                "tick {tick} node {i} diverged"
            );
        }
    }
}

/// Runs written with no membership filters must answer identically, because the
/// filter is an optimisation and nothing more.
///
/// This is the low-memory escape hatch. Filters are the only thing a disk-backed
/// base holds in RAM that scales with *total state* and cannot be evicted — the
/// memtable is bounded by a trigger and the mapping is clean file-backed pages —
/// so at billions of links they are the floor, and `--filter-bits 0` is how you
/// trade query latency for it.
///
/// The property that makes this safe is that a filter only ever answers "probe
/// the table" or "definitely absent". Removing it removes the second answer, so
/// every probe falls through to the binary search that a false positive would
/// have cost anyway. A run written at any setting stays readable by any build,
/// which is checked here by mixing settings within one stack.
#[tokio::test]
async fn runs_written_without_filters_answer_identically() {
    let with = Warehouse::new();
    let without = Warehouse::new();
    let f_with = Arc::new(ScopedForest::new());
    let f_without = Arc::new(ScopedForest::new());
    let mut offset = 1u64;

    // Enough keys per run that bits-per-key actually changes the block count —
    // below ~64 keys every setting rounds up to the one-block minimum and the
    // sizes are identical for uninteresting reasons.
    for tick in 0..6u64 {
        let mut events: Vec<EdgeEvent> = (0..400)
            .map(|i| global_edge(tick * 1000 + i, tick * 1000 + i + 1))
            .collect();
        events.push(scoped_edge(tick * 1000, tick * 1000 + 50, &[7, 9]));
        for e in &events {
            f_with.apply(e);
            f_without.apply(e);
        }
        with.flush_layered(f_with.clone(), offset, &events, u64::MAX, true, 3)
            .await;
        // Alternate 0 and 4 bits, so the stack mixes filtered and unfiltered runs
        // and a merge has to read across both.
        with_bits(&without, f_without.clone(), offset, &events, tick).await;
        offset += events.len() as u64;
    }

    // Both catalogs cold-start, and the unfiltered one really did write no filter
    // blobs on the runs that asked for none.
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let (base_a, _, _) = open_base_from_catalog(&with.store, &with.catalog, dir_a.path())
        .await
        .unwrap()
        .expect("a committed snapshot");
    let (base_b, _, _) = open_base_from_catalog(&without.store, &without.catalog, dir_b.path())
        .await
        .unwrap()
        .expect("a committed snapshot");
    assert!(
        blaze::core::RoutingBase::stats(&*base_b).index_bytes
            < blaze::core::RoutingBase::stats(&*base_a).index_bytes,
        "dropping filters must actually shrink the in-RAM index: {} vs {}",
        blaze::core::RoutingBase::stats(&*base_b).index_bytes,
        blaze::core::RoutingBase::stats(&*base_a).index_bytes
    );

    let cold_a = Arc::new(ScopedForest::with_base(base_a));
    let cold_b = Arc::new(ScopedForest::with_base(base_b));
    for tick in 0..6u64 {
        for i in 0..=400u64 {
            let node = tick * 1000 + i;
            assert_eq!(
                cold_b.scope_root(GLOBAL_SCOPE, node),
                cold_a.scope_root(GLOBAL_SCOPE, node),
                "node {node} diverged without filters"
            );
            assert_eq!(cold_b.scope_root(7, node), cold_a.scope_root(7, node));
        }
        // Misses are the case filters exist for, so check them explicitly.
        let absent = 9_000_000 + tick;
        assert_eq!(cold_b.scope_root(GLOBAL_SCOPE, absent), absent);
        assert_eq!(cold_b.scope_root(9, absent), absent);
    }
}

/// Alternate filter settings tick by tick, so one stack holds runs written both
/// ways.
async fn with_bits(
    wh: &Warehouse,
    forest: Arc<ScopedForest>,
    offset: u64,
    events: &[EdgeEvent],
    tick: u64,
) {
    // Mostly none, with one 4-bit run so the stack mixes settings and a merge has
    // to read across both.
    let bits = if tick == 3 { 4 } else { 0 };
    wh.flush_all(
        forest,
        offset,
        events,
        u64::MAX,
        true,
        3,
        usize::MAX,
        true,
        bits,
    )
    .await;
}

/// A catalog written before snapshots carried run sets must cold-start with no
/// migration step, mapping the same layers and answering identically.
///
/// This is the upgrade path — a running deployment's history is all in the old
/// format — and it is the only thing exercising the synthesised branch, since
/// every commit this build makes populates `runs`.
#[tokio::test]
async fn a_pre_run_set_catalog_still_cold_starts() {
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
    let expected: Vec<u64> = (0..3u64)
        .map(|tick| forest.scope_root(GLOBAL_SCOPE, tick * 50 + 10))
        .collect();

    wh.downgrade_to_base_plus_chain(3).await;
    assert!(
        matches!(
            wh.latest_meta().await.run_set().unwrap(),
            RunSet::SequencesOnly(chain) if chain == (1..=3)
        ),
        "the downgraded catalog must read as a base plus a chain"
    );

    let cold = tempfile::tempdir().unwrap();
    let (base, watermark, local) = open_base_from_catalog(&wh.store, &wh.catalog, cold.path())
        .await
        .unwrap()
        .expect("a committed snapshot");
    assert_eq!(base.layers(), 3, "the whole chain must be mapped");
    assert_eq!(watermark, 30);
    // Synthesised runs are single-sequence, so a delta may still be layered on
    // top — an old catalog must not force the next tick into a full merge.
    assert!(local.fully_committed());

    let cold = Arc::new(ScopedForest::with_base(base));
    for (tick, want) in expected.iter().enumerate() {
        assert_eq!(
            cold.scope_root(GLOBAL_SCOPE, tick as u64 * 50 + 10),
            *want,
            "tick {tick} diverged after reading an old-format catalog"
        );
    }
}

/// A missing run must fail loudly (I5). Skipping one would serve stale topology
/// as though it were current, which is worse than not starting.
///
/// Where that guarantee lives moved when snapshots started carrying run sets, and
/// both halves are pinned here. A cold start now learns every run's path from the
/// newest snapshot alone, so an intermediate snapshot JSON is no longer
/// load-bearing and losing one costs nothing — a strict improvement over walking
/// the chain, which needed every link's JSON to find the next path. The *layer
/// objects* are what state actually lives in, and losing one of those is still
/// fatal.
#[tokio::test]
async fn a_missing_run_is_a_hard_error() {
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
    let latest = wh.latest_meta().await;
    assert_eq!(latest.delta_chain_len, 2);
    let RunSet::Runs(runs) = latest.run_set().unwrap() else {
        panic!("the run set is what this test is about");
    };
    assert_eq!(runs.len(), 3);

    // The middle snapshot's metadata is no longer needed to find its run.
    wh.delete_snapshot_meta(2).await;
    let cold = tempfile::tempdir().unwrap();
    let (base, _, _) = open_base_from_catalog(&wh.store, &wh.catalog, cold.path())
        .await
        .expect("a lost intermediate snapshot JSON must not break a cold start")
        .expect("a committed snapshot");
    assert_eq!(base.layers(), 3, "every run must still be mapped");

    // The run's *object*, though, is where the state is.
    wh.delete_puffin(&runs[1].path).await;
    let colder = tempfile::tempdir().unwrap();
    let err = open_base_from_catalog(&wh.store, &wh.catalog, colder.path())
        .await
        .expect_err("a missing run must not be silently skipped");
    assert!(
        err.to_string().contains("dsu-000000000002"),
        "error should name the missing run, got: {err}"
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
