//! Does detaching the merge actually keep the memtable bounded?
//!
//! The claim behind `--inline-merges` being off by default: awaiting a merge
//! inside the flush tick means nothing folds for its duration, so the memtable
//! grows unbounded by the fold trigger for however long the merge takes. Running
//! it on a blocking thread should keep folds draining while it works.
//!
//! That is a claim about *concurrency*, so this drives the real `Flusher` — real
//! object store, real catalog, real tiering policy — with a separate thread
//! ingesting continuously, and reports what each policy does to the two things
//! that matter: how large the memtable gets, and how long a tick blocks.
//!
//! Both arms ingest **the same links at the same rate**, which matters more than
//! it sounds: a first version let the ingest thread run free for a fixed number
//! of ticks, so the arm with longer ticks also got more wall time to ingest, and
//! the arm under test was handed 47% more data than its comparator. With the rate
//! and the total fixed, peak memtable becomes a direct reading of stall duration —
//! it is exactly `rate x seconds the tick failed to fold`.
//!
//! Tunables via env: `LINKS`, `RATE`, `TICK_MS`, `SCOPES`, `NODES`, `MAX_LAYERS`,
//! `FANOUT`.
//!
//! Run: `cargo run --release --example detached_merge`

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, SnapshotCatalog};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Run {
    links: u64,
    ingest_s: f64,
    peak_memtable: u64,
    tick_ms: Vec<f64>,
    merges: u64,
    wall_s: f64,
}

async fn drive(
    inline_merges: bool,
    links: u64,
    rate: u64,
    tick_ms: u64,
    nodes: u64,
    scopes: u32,
) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let forest = Arc::new(ScopedForest::new());
    let buffer = Arc::new(EdgeBuffer::new());

    let stop = Arc::new(AtomicBool::new(false));
    let ingested = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));

    // Ingest at a fixed rate until the budget is spent, so both arms see
    // identical input. A stalled tick then shows up directly: the memtable it
    // feeds keeps filling at `rate` and nothing drains it.
    let done = Arc::new(AtomicBool::new(false));
    let ingest = {
        let (forest, buffer, stop, ingested, done) = (
            forest.clone(),
            buffer.clone(),
            stop.clone(),
            ingested.clone(),
            done.clone(),
        );
        std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(0xD37A);
            let mut offset = 0u64;
            const BATCH: u64 = 1_000;
            let batch_interval = Duration::from_secs_f64(BATCH as f64 / rate as f64);
            let mut next = Instant::now();
            while offset < links && !stop.load(Ordering::Relaxed) {
                for _ in 0..BATCH {
                    let visibility = if rng.random_range(0..100u32) < 30 {
                        Visibility::Global
                    } else {
                        Visibility::Scoped(
                            (0..1 + rng.random_range(0..3u32))
                                .map(|_| 1 + rng.random_range(0..scopes))
                                .collect(),
                        )
                    };
                    let e = EdgeEvent {
                        src: rng.random_range(0..nodes),
                        dst: rng.random_range(0..nodes),
                        visibility,
                        event_time_ms: 0,
                        props: None,
                    };
                    forest.apply(&e);
                    buffer.append(offset, &e);
                    offset += 1;
                }
                ingested.fetch_add(BATCH, Ordering::Relaxed);
                next += batch_interval;
                if let Some(d) = next.checked_duration_since(Instant::now()) {
                    std::thread::sleep(d);
                }
            }
            done.store(true, Ordering::Relaxed);
        })
    };
    let ingest_started = Instant::now();

    // Sample the memtable off the tick's own thread: in the inline case the peak
    // happens *during* a tick, so sampling between ticks would miss it entirely.
    let sampler = {
        let (forest, stop, peak) = (forest.clone(), stop.clone(), peak.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(forest.memtable_links(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };

    let flusher = Flusher {
        forest: forest.clone(),
        buffer: buffer.clone(),
        store: store.clone(),
        catalog: catalog.clone(),
        elector: Arc::new(StaticElector(true)),
        table_prefix: prefix,
        worker_id: "perf".into(),
        base_dir: Some(cache.path().to_path_buf()),
        fold_after_links: u64::MAX, // a leader folds every tick regardless
        max_delta_layers: env("MAX_LAYERS", 12),
        tier_fanout: env("FANOUT", 4),
        inline_merges,
        layers: parking_lot::Mutex::new(None),
        pending_merge: parking_lot::Mutex::new(None),
    };

    let started = Instant::now();
    let mut tick_times = Vec::new();
    let mut merges = 0u64;
    let mut last_seq = 0u64;
    // Tick on a fixed schedule, the way the real flush loop does, so a slow tick
    // eats into the next interval instead of pushing the whole timetable back.
    // Sleeping *between* ticks would hand the inline arm extra wall time to ingest
    // in, which is the same confound as letting the thread run free.
    let mut ticker = tokio::time::interval(Duration::from_millis(tick_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut drain = 0;
    while drain < 3 {
        if done.load(Ordering::Relaxed) {
            drain += 1;
        }
        ticker.tick().await;
        let t = Instant::now();
        flusher.tick().await.unwrap();
        tick_times.push(t.elapsed().as_secs_f64() * 1000.0);
        if let Some(latest) = catalog.latest().await.unwrap() {
            // A merge takes a sequence of its own, so the catalog jumps by two.
            if latest.sequence > last_seq + 1 {
                merges += 1;
            }
            last_seq = latest.sequence;
        }
    }
    let ingest_elapsed = ingest_started.elapsed().as_secs_f64();
    // Let any in-flight merge finish rather than counting it as free.
    flusher.wait_for_merge().await;
    let wall_s = started.elapsed().as_secs_f64();

    stop.store(true, Ordering::Relaxed);
    ingest.join().unwrap();
    sampler.join().unwrap();

    Run {
        links: ingested.load(Ordering::Relaxed),
        ingest_s: ingest_elapsed,
        peak_memtable: peak.load(Ordering::Relaxed),
        tick_ms: tick_times,
        merges,
        wall_s,
    }
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i]
}

#[tokio::main]
async fn main() {
    let links: u64 = env("LINKS", 3_000_000);
    let rate: u64 = env("RATE", 400_000);
    let tick_ms: u64 = env("TICK_MS", 250);
    let nodes: u64 = env("NODES", 4_000_000);
    let scopes: u32 = env("SCOPES", 3_000);

    println!(
        "{}M links at {}k/s, tick every {tick_ms}ms, {scopes} scopes, fanout {}, ceiling {}\n",
        links / 1_000_000,
        rate / 1_000,
        env("FANOUT", 4),
        env("MAX_LAYERS", 12)
    );
    println!(
        "{:<10} {:>7} {:>6} {:>8} {:>15} {:>10} {:>10} {:>10} {:>8}",
        "policy",
        "links",
        "ticks",
        "ingest/s",
        "peak memtable",
        "p50 tick",
        "p99 tick",
        "max tick",
        "merges"
    );

    for (name, inline) in [("inline", true), ("detached", false)] {
        let mut r = drive(inline, links, rate, tick_ms, nodes, scopes).await;
        r.tick_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "{:<10} {:>6.1}M {:>6} {:>7.0}k {:>15} {:>8.0}ms {:>8.0}ms {:>8.0}ms {:>8}",
            name,
            r.links as f64 / 1e6,
            r.tick_ms.len(),
            r.links as f64 / r.ingest_s / 1e3,
            r.peak_memtable,
            pct(&r.tick_ms, 0.50),
            pct(&r.tick_ms, 0.99),
            r.tick_ms.last().copied().unwrap_or(0.0),
            r.merges,
        );
        let _ = r.wall_s;
    }
}
