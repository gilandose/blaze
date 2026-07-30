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
        }
    }

    /// Flush a forest's state as a committed snapshot (leader path).
    async fn commit(&self, forest: Arc<ScopedForest>, first_offset: u64, events: &[EdgeEvent]) {
        let buffer = Arc::new(EdgeBuffer::new());
        for (i, e) in events.iter().enumerate() {
            buffer.append(first_offset + i as u64, e);
        }
        let flusher = Flusher {
            forest,
            buffer,
            store: self.store.clone(),
            catalog: self.catalog.clone(),
            elector: Arc::new(StaticElector(true)),
            table_prefix: self.prefix.clone(),
            worker_id: "test-leader".into(),
        };
        flusher.tick().await.unwrap();
    }

    /// Cold-start a base-backed forest from the latest committed snapshot.
    async fn open_base_backed(&self) -> (Arc<ScopedForest>, u64) {
        let (base, watermark) =
            open_base_from_catalog(&self.store, &self.catalog, self.cache.path())
                .await
                .unwrap()
                .expect("a committed snapshot");
        (Arc::new(ScopedForest::with_base(base)), watermark)
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
