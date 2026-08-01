//! Emit `scope, node, root` for an edge list, so an outside oracle can check it.
//!
//! Every correctness test in this repo so far grades blaze against something
//! blaze's authors wrote — a BFS reference model, an in-memory compactor, an
//! earlier version of the same encoder. Those catch regressions well and shared
//! misconceptions not at all. This exists so `scipy.sparse.csgraph` can grade it
//! instead, on published graphs, from an implementation with no idea this
//! project exists.
//!
//! # What is being claimed
//!
//! For a scope `s`, `scope_root(s, n)` must be **the lowest node id in `n`'s
//! connected component of (global edges ∪ edges visible to `s`)**. Not "some
//! stable representative" — the exact minimum, which is what makes the answer
//! comparable across workers and across restarts. So the oracle does not have to
//! compare partitions up to relabelling: it can compute the expected root for
//! every node and check value for value.
//!
//! # Two modes, because the interesting bugs are not in the DSU
//!
//! `memory` runs the in-heap forest only. `storage` drives a real `Flusher`, so
//! the same answers have to survive being folded into runs, tier-merged,
//! compacted, and resolved back through a layered stack of mmap'd files. The
//! second is where a wrong answer would actually come from.
//!
//! Input is `src dst scope` per line, scope `0` meaning global. Output is
//! `scope node root` for scopes `0..=scopes`, whether or not an edge mentions
//! them — a scope holding no edges of its own must still see the global
//! components, and asking only about scopes that appear in the input would never
//! check that. Driven by `tools/cc_oracle.py`, which is where the datasets and
//! the comparison live.
//!
//! Run: `cargo run --release --example oracle -- <edges> <out> <mode> <scopes>`

use blaze::core::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, SnapshotCatalog};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::collections::BTreeSet;
use std::io::{BufWriter, Write};
use std::sync::Arc;

struct Input {
    edges: Vec<(NodeId, NodeId, ScopeId)>,
    nodes: BTreeSet<NodeId>,
    scopes: BTreeSet<ScopeId>,
}

fn read_edges(path: &str) -> Input {
    let text = std::fs::read_to_string(path).expect("read edge list");
    let mut edges = Vec::new();
    let mut nodes = BTreeSet::new();
    let mut scopes = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut f = line.split_whitespace();
        let src: NodeId = f.next().expect("src").parse().expect("src is a u64");
        let dst: NodeId = f.next().expect("dst").parse().expect("dst is a u64");
        let scope: ScopeId = f.next().expect("scope").parse().expect("scope is a u32");
        nodes.insert(src);
        nodes.insert(dst);
        if scope != GLOBAL_SCOPE {
            scopes.insert(scope);
        }
        edges.push((src, dst, scope));
    }
    Input {
        edges,
        nodes,
        scopes,
    }
}

fn event(src: NodeId, dst: NodeId, scope: ScopeId) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: if scope == GLOBAL_SCOPE {
            Visibility::Global
        } else {
            Visibility::Scoped([scope].into_iter().collect())
        },
        event_time_ms: 0,
        props: None,
    }
}

/// Apply every edge to a bare in-heap forest.
fn run_memory(input: &Input) -> Arc<ScopedForest> {
    let forest = Arc::new(ScopedForest::new());
    for &(src, dst, scope) in &input.edges {
        forest.apply(&event(src, dst, scope));
    }
    forest
}

/// Apply every edge through a real flush loop, so the answers have to survive
/// folding, tiered merges, compaction and layered resolution over mmap'd runs.
///
/// The flusher is ticked between batches rather than on a timer: this is a
/// correctness harness, and a wall-clock schedule would make which edges landed
/// in which run depend on how fast the machine is.
async fn run_storage(input: &Input) -> (Arc<ScopedForest>, usize, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let forest = Arc::new(ScopedForest::new());
    let buffer = Arc::new(EdgeBuffer::new());

    let flusher = Flusher {
        forest: forest.clone(),
        buffer: buffer.clone(),
        store,
        catalog,
        elector: Arc::new(StaticElector(true)),
        table_prefix: prefix,
        worker_id: "oracle".into(),
        base_dir: Some(cache.path().to_path_buf()),
        fold_after_links: u64::MAX,
        // Deliberately small: a shallow ceiling and a low fanout mean this run
        // merges often, which is the point — every tier level and the compaction
        // path get exercised on a few hundred thousand edges rather than needing
        // a soak to reach them.
        max_delta_layers: 6,
        tier_fanout: 3,
        write: Default::default(),
        inline_merges: true,
        layers: parking_lot::Mutex::new(None),
        pending_merge: parking_lot::Mutex::new(None),
    };

    // Enough batches that runs accumulate and merges fire repeatedly.
    let batch = (input.edges.len() / 40).max(1);
    for (offset, chunk) in input.edges.chunks(batch).enumerate() {
        for (i, &(src, dst, scope)) in chunk.iter().enumerate() {
            let e = event(src, dst, scope);
            forest.apply(&e);
            buffer.append((offset * batch + i) as u64, &e);
        }
        flusher.tick().await.unwrap();
    }
    flusher.wait_for_merge().await;
    flusher.tick().await.unwrap();

    let runs = flusher
        .layers
        .lock()
        .as_ref()
        .map(|l| l.runs.len())
        .unwrap_or(0);
    (forest, runs, cache)
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let edges_path = args.next().expect("usage: oracle <edges> <out> <mode>");
    let out_path = args.next().expect("usage: oracle <edges> <out> <mode>");
    let mode = args.next().unwrap_or_else(|| "memory".into());
    let n_scopes: ScopeId = args
        .next()
        .map(|s| s.parse().expect("scope count is a u32"))
        .unwrap_or(0);

    let input = read_edges(&edges_path);
    eprintln!(
        "{} edges, {} nodes, {} scopes, mode {mode}",
        input.edges.len(),
        input.nodes.len(),
        input.scopes.len()
    );

    let (forest, note, _hold) = match mode.as_str() {
        "memory" => (run_memory(&input), "in-heap forest".to_string(), None),
        "storage" => {
            let (forest, runs, cache) = run_storage(&input).await;
            (forest, format!("{runs} committed runs"), Some(cache))
        }
        other => panic!("unknown mode {other:?} (memory, storage)"),
    };
    eprintln!("resolved through: {note}");

    // Every node in every scope, plus the global view. Written sorted so a diff
    // between two modes is readable.
    let out = std::fs::File::create(&out_path).expect("create output");
    let mut out = BufWriter::new(out);
    let mut scopes: BTreeSet<ScopeId> = input.scopes.clone();
    scopes.extend(1..=n_scopes);
    let mut scopes: Vec<ScopeId> = scopes.into_iter().collect();
    scopes.insert(0, GLOBAL_SCOPE);
    for scope in scopes {
        for &node in &input.nodes {
            writeln!(out, "{scope} {node} {}", forest.scope_root(scope, node)).unwrap();
        }
    }
    out.flush().unwrap();
    eprintln!("wrote {out_path}");
}
