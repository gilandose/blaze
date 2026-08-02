//! What does design 011 actually cost, and what does it buy?
//!
//! `--member-index` is off by default on a size argument (+77% of run bytes,
//! measured by `examples/registry_shape`). That is only half the question. The
//! other half is speed, in three places, and none of them had a number:
//!
//! 1. **The union path.** Tracking adds one `DashMap` write per merge. Merges
//!    are the ingest path, so this is the cost every deployment that turns the
//!    flag on pays continuously, whether or not anyone ever calls `members`.
//! 2. **The query.** A downward walk is O(members) lookups against
//!    `scope_root`'s O(depth). The design claims "sub-millisecond for a small
//!    component"; the interesting number is where that stops being true.
//! 3. **The base/memtable split.** A walk that stays in the heap and a walk that
//!    crosses into an mmap'd run are different workloads. The second is what a
//!    real deployment runs, and it pays a binary search per node, because there
//!    is no filter over the member index's keys.
//!
//! # Four things this harness does deliberately, each because it got them wrong
//!
//! **Density is swept, not sampled.** The whole design rests on components being
//! small, and the cap exists for when they are not. A single density flatters —
//! below the percolation threshold (~1.8 links/node) every component is a
//! handful of nodes and the walk looks free; above it, one component is most of
//! the graph and every query truncates. Both are real, so both are here.
//!
//! **Mappings are warmed before they are timed.** The first version measured the
//! global sweep against a cold mapping and the tenant sweep against the pages
//! that sweep had just faulted in, and reported the tenant scope as 5x faster.
//! It is not; that was the page cache.
//!
//! **The ingest A/B is best-of-N with the arms interleaved.** A single sample of
//! each is not enough to claim a ratio. One run of this harness landed on a busy
//! machine and reported the *tracked* arm 74% faster than the plain one, which
//! is impossible; another reported -55% where a quiet machine says -21%.
//! Interleaving cancels drift and the minimum rejects contention, since a
//! CPU-bound loop cannot run faster than its own best observed time.
//!
//! **The in-heap arm is measured before the fold.** `fold_delta` drains the
//! memtable and installs the base, so a handle held across it is no longer
//! in-heap. A first version kept one and labelled the result "in-heap" when the
//! forest had become disk-backed underneath — which is why its two ceiling rows
//! came out suspiciously identical.
//!
//! Tunables via env: `LINKS`, `SCOPES`, `GLOBAL_PCT`, `PROBES`, `CAP`.
//!
//! Run: `cargo run --release --example member_bench`

use blaze::core::{EdgeEvent, GLOBAL_SCOPE, NodeId, RoutingBase, ScopedForest, Visibility};
use blaze::storage::codec::WriteOptions;
use blaze::storage::{LayeredBase, PuffinBase, codec, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One deterministic edge stream, so both arms of the ingest comparison see
/// exactly the same merges in the same order.
fn edges(links: u64, nodes: u64, scopes: u32, global_pct: u32) -> Vec<EdgeEvent> {
    let mut rng = StdRng::seed_from_u64(0x0011_BE0C);
    (0..links)
        .map(|_| EdgeEvent {
            src: rng.random_range(0..nodes),
            dst: rng.random_range(0..nodes),
            visibility: if rng.random_range(0..100u32) < global_pct {
                Visibility::Global
            } else {
                Visibility::Scoped(smallvec::smallvec![1 + rng.random_range(0..scopes)])
            },
            event_time_ms: 0,
            props: None,
        })
        .collect()
}

fn ingest(events: &[EdgeEvent], tracking: bool) -> (Arc<ScopedForest>, f64) {
    let forest = Arc::new(if tracking {
        ScopedForest::new().tracking_members()
    } else {
        ScopedForest::new()
    });
    let t = Instant::now();
    for e in events {
        forest.apply(e);
    }
    (forest, t.elapsed().as_secs_f64())
}

/// Best-of-`reps`, arms interleaved, returning `(plain links/s, tracked links/s)`.
///
/// `build` makes a fresh forest for one arm; alternating which arm runs first
/// keeps a monotonic drift (thermal, allocator warm-up) off the same arm every
/// time, and taking the minimum rejects contention.
fn ab(reps: usize, n: u64, mut run: impl FnMut(bool) -> f64) -> (f64, f64) {
    let (mut plain, mut tracked) = (f64::MAX, f64::MAX);
    for rep in 0..reps {
        let order = if rep % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for tracking in order {
            let secs = run(tracking);
            let slot = if tracking { &mut tracked } else { &mut plain };
            *slot = slot.min(secs);
        }
    }
    (n as f64 / plain, n as f64 / tracked)
}

/// Fold everything out to a Puffin run and serve it from a *fresh* forest with
/// an empty memtable, which is what a `--routing-base disk` worker answers from
/// once it has drained.
fn fold_to_disk(forest: &ScopedForest, path: &std::path::Path) -> Arc<ScopedForest> {
    let mut w = codec::BlobWriter::new(
        1,
        WriteOptions {
            member_index: true,
            ..Default::default()
        },
    );
    let owned = path.to_path_buf();
    forest
        .fold_delta(&mut w, |w, _| {
            std::fs::write(&owned, puffin::write(&w.finish(), BTreeMap::new()))?;
            let base: Arc<dyn RoutingBase> =
                Arc::new(LayeredBase::new(Arc::new(PuffinBase::open(&owned)?)));
            anyhow::Ok((base, ()))
        })
        .unwrap();
    Arc::new(
        ScopedForest::with_base(Arc::new(LayeredBase::new(Arc::new(
            PuffinBase::open(path).unwrap(),
        ))))
        .tracking_members(),
    )
}

struct Bucket {
    label: &'static str,
    us: Vec<f64>,
    total_members: u64,
    truncated: u64,
}

fn bucket_of(n: usize) -> (usize, &'static str) {
    match n {
        1 => (0, "1 (singleton)"),
        2..=9 => (1, "2-9"),
        10..=99 => (2, "10-99"),
        100..=999 => (3, "100-999"),
        1_000..=9_999 => (4, "1k-10k"),
        _ => (5, ">=10k"),
    }
}

/// Time `members` over `probes` nodes, bucketed by the size of the answer.
/// Warms the same probe sequence first.
fn sweep(forest: &ScopedForest, scope: u32, nodes: u64, probes: usize, cap: usize) -> Vec<Bucket> {
    let mut buckets: BTreeMap<usize, Bucket> = BTreeMap::new();
    for pass in 0..2 {
        let mut rng = StdRng::seed_from_u64(0xBEEF_0011);
        buckets.clear();
        for _ in 0..probes {
            let node = rng.random_range(0..nodes);
            let t = Instant::now();
            let found = forest.members(scope, node, cap).expect("indexed");
            let us = t.elapsed().as_secs_f64() * 1e6;
            if pass == 0 {
                continue; // warm-up
            }
            let n = found.nodes().len();
            let (i, label) = bucket_of(n);
            let b = buckets.entry(i).or_insert_with(|| Bucket {
                label,
                us: Vec::new(),
                total_members: 0,
                truncated: 0,
            });
            b.us.push(us);
            b.total_members += n as u64;
            b.truncated += found.is_truncated() as u64;
        }
    }
    buckets.into_values().collect()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

/// The comparator: what a plain component lookup costs on the same state, warmed
/// the same way.
fn root_latency(forest: &ScopedForest, scope: u32, nodes: u64, probes: usize) -> f64 {
    let mut out = 0.0;
    for _ in 0..2 {
        let mut rng = StdRng::seed_from_u64(0xBEEF_0011);
        let t = Instant::now();
        let mut sink: NodeId = 0;
        for _ in 0..probes {
            sink ^= forest.scope_root(scope, rng.random_range(0..nodes));
        }
        std::hint::black_box(sink);
        out = t.elapsed().as_secs_f64() * 1e6 / probes as f64;
    }
    out
}

/// What one request costs when the caller asks for the largest answer the API
/// will give. Past percolation this is what every hub query does, and it is the
/// number to size a rate limit against.
///
/// Its own probe sequence, reset per call, so the two arms ask about the same
/// nodes. 300 probes rather than 64: p99 of 64 samples is the maximum, which is
/// one scheduler hiccup rather than a tail.
fn ceiling(f: &ScopedForest, nodes: u64, label: &str) {
    let mut rng = StdRng::seed_from_u64(0x0000_CE11);
    let mut us: Vec<f64> = Vec::new();
    let mut size = 0usize;
    for _ in 0..300 {
        let node = rng.random_range(0..nodes);
        let t = Instant::now();
        let found = f.members(GLOBAL_SCOPE, node, 10_000).expect("indexed");
        us.push(t.elapsed().as_secs_f64() * 1e6);
        size = size.max(found.nodes().len());
    }
    us.sort_by(|a, c| a.partial_cmp(c).unwrap());
    println!(
        "  ceiling cap=10000, {label:<12} p50 {:.0} us, p99 {:.0} us, largest answer {size}",
        pct(&us, 0.50),
        pct(&us, 0.99)
    );
}

fn table(title: &str, root_us: f64, buckets: &mut [Bucket]) {
    println!("\n  {title}   (scope_root comparator: {root_us:.2} us)");
    println!(
        "  {:<14} {:>8} {:>10} {:>10} {:>10} {:>7}",
        "answer size", "probes", "p50 us", "p99 us", "mean size", "trunc"
    );
    for b in buckets.iter_mut() {
        b.us.sort_by(|a, c| a.partial_cmp(c).unwrap());
        println!(
            "  {:<14} {:>8} {:>10.2} {:>10.2} {:>10.1} {:>7}",
            b.label,
            b.us.len(),
            pct(&b.us, 0.50),
            pct(&b.us, 0.99),
            b.total_members as f64 / b.us.len() as f64,
            b.truncated
        );
    }
}

fn main() {
    let links: u64 = env("LINKS", 2_000_000);
    let scopes: u32 = env("SCOPES", 3_000);
    let global_pct: u32 = env("GLOBAL_PCT", 30);
    // The API's *default* cap, not its ceiling. Past percolation every probe
    // walks the full cap, so the ceiling here would measure how long it takes to
    // enumerate 10 000 nodes 2 000 times rather than what serving costs. The
    // ceiling gets its own single-shot measurement per density.
    let probes: usize = env("PROBES", 2_000);
    let cap: usize = env("CAP", 1_000);
    let dir = tempfile::tempdir().unwrap();

    println!("{links} links / {scopes} scopes / {global_pct}% global / cap {cap}");

    // 1. The union path, measured twice — because the all-RAM number on its own
    // overstates it. With no base attached the DSU *is* the whole cost of
    // `apply`, so a second map write is a large fraction of it. A
    // `--routing-base disk` worker also probes the mapping on every merge, and
    // that term dominates, so the same absolute cost is a much smaller share.
    let events = edges(links, links * 2, scopes, global_pct);
    let (plain, tracked) = ab(3, links, |t| ingest(&events, t).1);
    println!(
        "\ningest, no base       {plain:.0} links/s plain, {tracked:.0} tracked   ({:+.1}%)",
        100.0 * (tracked / plain - 1.0)
    );

    // Fold that state out to a run, then ingest a second, disjoint stream on top
    // of it — what a running worker does between folds.
    let base_path = dir.path().join("ingest-base.puffin");
    fold_to_disk(&ingest(&events, true).0, &base_path);
    drop(events);
    let more = edges(links / 4, links * 2, scopes, global_pct);
    let (plain, tracked) = ab(3, links / 4, |tracking| {
        let base: Arc<dyn RoutingBase> = Arc::new(LayeredBase::new(Arc::new(
            PuffinBase::open(&base_path).unwrap(),
        )));
        let forest = ScopedForest::with_base(base);
        let forest = if tracking {
            forest.tracking_members()
        } else {
            forest
        };
        // Warm the mapping so this measures the probe, not the first fault.
        for e in more.iter().take(1000) {
            forest.apply(e);
        }
        let t = Instant::now();
        for e in &more {
            forest.apply(e);
        }
        t.elapsed().as_secs_f64()
    });
    println!(
        "ingest, mmap'd base   {plain:.0} links/s plain, {tracked:.0} tracked   ({:+.1}%)",
        100.0 * (tracked / plain - 1.0)
    );
    drop(more);

    // 2 and 3. Query latency across the percolation threshold (~1.8 links/node).
    for lpn in [0.5f64, 1.5, 2.5, 5.0] {
        let nodes = (links as f64 / lpn) as u64;
        let events = edges(links, nodes, scopes, global_pct);
        let (forest, _) = ingest(&events, true);
        drop(events);
        println!(
            "\n=== {lpn} links/node ({nodes} nodes){}",
            if lpn < 1.8 {
                "  — below percolation"
            } else {
                "  — giant component"
            }
        );

        let root_us = root_latency(&forest, GLOBAL_SCOPE, nodes, probes);
        table(
            "in-heap, global",
            root_us,
            &mut sweep(&forest, GLOBAL_SCOPE, nodes, probes, cap),
        );
        let root_us = root_latency(&forest, 1, nodes, probes);
        table(
            "in-heap, tenant scope 1 (two-level walk)",
            root_us,
            &mut sweep(&forest, 1, nodes, probes, cap),
        );
        ceiling(&forest, nodes, "in-heap");

        let path = dir.path().join(format!("bench-{lpn}.puffin"));
        let disk = fold_to_disk(&forest, &path);
        drop(forest);
        println!(
            "\n  folded to {:.1} MB; memtable empty, every child lookup hits the mapping",
            std::fs::metadata(&path).unwrap().len() as f64 / 1e6
        );
        let root_us = root_latency(&disk, GLOBAL_SCOPE, nodes, probes);
        table(
            "mmap'd run, global",
            root_us,
            &mut sweep(&disk, GLOBAL_SCOPE, nodes, probes, cap),
        );
        let root_us = root_latency(&disk, 1, nodes, probes);
        table(
            "mmap'd run, tenant scope 1",
            root_us,
            &mut sweep(&disk, 1, nodes, probes, cap),
        );
        ceiling(&disk, nodes, "mmap'd run");
    }
}
