//! Quick throughput measurement of the two hot paths:
//! ingest (DSU apply + Arrow buffer append) and lock-free queries.
//!
//! Run with: cargo run --release --example throughput

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ingest::EdgeBuffer;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

const EVENTS: usize = 2_000_000;
const NODES: u64 = 1_000_000;
const SCOPES: u32 = 3_000;
const GLOBAL_PCT: u8 = 15;

fn make_events(n: usize) -> Vec<EdgeEvent> {
    let mut rng = StdRng::seed_from_u64(0xB1A2E);
    (0..n)
        .map(|_| {
            let visibility = if rng.random_range(0..100u8) < GLOBAL_PCT {
                Visibility::Global
            } else if rng.random_range(0..100u8) < 5 {
                Visibility::Scoped(smallvec::smallvec![
                    rng.random_range(1..=SCOPES),
                    rng.random_range(1..=SCOPES)
                ])
            } else {
                Visibility::Scoped(smallvec::smallvec![rng.random_range(1..=SCOPES)])
            };
            EdgeEvent {
                src: rng.random_range(0..NODES),
                dst: rng.random_range(0..NODES),
                visibility,
                event_time_ms: 1_700_000_000_000,
                props: Some(r#"{"weight":42}"#.to_string()),
            }
        })
        .collect()
}

fn main() {
    let events = make_events(EVENTS);
    let forest = Arc::new(ScopedForest::new());
    let buffer = Arc::new(EdgeBuffer::new());

    // --- Ingest hot path: DSU apply only ---
    let t = Instant::now();
    for e in &events {
        forest.apply(e);
    }
    let dsu_secs = t.elapsed().as_secs_f64();
    println!(
        "DSU apply:            {:>10.0} events/s  ({} events in {:.2}s)",
        EVENTS as f64 / dsu_secs,
        EVENTS,
        dsu_secs
    );

    // --- Arrow buffer append (separate pass to isolate cost) ---
    let t = Instant::now();
    for (i, e) in events.iter().enumerate() {
        buffer.append(i as u64, e);
    }
    let buf_secs = t.elapsed().as_secs_f64();
    println!(
        "Arrow buffer append:  {:>10.0} events/s  ({:.2}s)",
        EVENTS as f64 / buf_secs,
        buf_secs
    );
    println!(
        "Combined ingest est:  {:>10.0} events/s",
        EVENTS as f64 / (dsu_secs + buf_secs)
    );

    // --- Seal (rotate to sorted RecordBatch) ---
    let t = Instant::now();
    let seg = buffer.seal_active().expect("segment");
    println!(
        "Seal+sort {} rows: {:.0}ms",
        seg.batch.num_rows(),
        t.elapsed().as_secs_f64() * 1000.0
    );

    // --- Query path: single thread ---
    let mut rng = StdRng::seed_from_u64(7);
    const QUERIES: usize = 2_000_000;
    let t = Instant::now();
    let mut acc = 0u64;
    for _ in 0..QUERIES {
        let s = rng.random_range(0..=SCOPES);
        let u = rng.random_range(0..NODES);
        let v = rng.random_range(0..NODES);
        acc += forest.connected(s, u, v) as u64;
    }
    let q_secs = t.elapsed().as_secs_f64();
    println!(
        "Queries (1 thread):   {:>10.0} conn-checks/s  ({} hits)",
        QUERIES as f64 / q_secs,
        acc
    );

    // --- Query path: query threads on all-but-one core, with concurrent
    // ingest of fresh events (the deployment guidance: leave the single
    // writer a core instead of oversubscribing) ---
    let query_threads = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1) as u64)
        .unwrap_or(3);
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let more = make_events(500_000);
    std::thread::scope(|scope| {
        for tid in 0..query_threads {
            let forest = forest.clone();
            let stop = stop.clone();
            let total = total.clone();
            scope.spawn(move || {
                let mut rng = StdRng::seed_from_u64(100 + tid);
                let mut n = 0u64;
                let mut sink = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..1024 {
                        let s = rng.random_range(0..=SCOPES);
                        let u = rng.random_range(0..NODES);
                        let v = rng.random_range(0..NODES);
                        sink += forest.connected(s, u, v) as u64;
                    }
                    n += 1024;
                }
                std::hint::black_box(sink);
                total.fetch_add(n, Ordering::Relaxed);
            });
        }
        let t = Instant::now();
        for e in &more {
            forest.apply(e);
        }
        let ingest_secs = t.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        println!(
            "Ingest w/ {query_threads} query threads: {:>10.0} events/s",
            more.len() as f64 / ingest_secs
        );
        // Threads joined at scope exit; measure wall time for query rate.
        let wall = t.elapsed().as_secs_f64();
        std::hint::black_box(wall);
    });
    let stats = forest.stats();
    println!(
        "Queries ({query_threads} threads, concurrent ingest): ~{:.1}M conn-checks total",
        total.load(Ordering::Relaxed) as f64 / 1e6
    );
    println!(
        "Final state: {} global links, {} scope links, {} scopes, {} fixups",
        stats.global_links, stats.scope_links, stats.active_scopes, stats.merge_fixups
    );
}
