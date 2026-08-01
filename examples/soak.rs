//! Long run at fixed graph density, reporting every sizing constant as state grows.
//!
//! Two things this is for.
//!
//! **Does anything drift?** Every constant in `docs/TUNING.md` was measured at 2M
//! links. A constant that holds at 2M and not at 200M is worse than no constant,
//! so this re-derives all of them at checkpoints across a long run through the
//! real `Flusher` — folds, tiered merges, detached compaction and commits all
//! exercised, not simulated.
//!
//! **Is `scopes_per_root` a steady state?** It decides whether restructuring the
//! registry is worth doing. Grouping entries by root costs `12 + 4k` against a
//! flat `12k`, so at the 3.05 measured at 2M links it is a 1.51x saving, but at
//! k = 10 it would be 2.3x. Components coalesce as a graph grows, which should
//! concentrate scopes onto fewer roots and push k up — if that happens, the
//! design decision changes.
//!
//! Density is held fixed while state grows, because `pairs_per_link` is strongly
//! density-dependent (it peaks at the percolation threshold) and letting it drift
//! would confound every other column. Default 1.8 links/node matches the
//! production profile this was sized against.
//!
//! `GLOBAL_PCT` is the sharpest knob here. A global edge merges once in the
//! shared tier and then has to notify every scope keyed on the roots it joined,
//! so its cost scales with scopes-per-root; a scoped edge touches exactly one
//! overlay and notifies nobody. Setting it to 0 removes `apply_global` from the
//! write path entirely.
//!
//! Tunables via env: `LINKS`, `LINKS_PER_NODE`, `SCOPES`, `CHECKPOINT`,
//! `MAX_LAYERS`, `FANOUT`, `MIN_DISK_GB`, `GLOBAL_PCT`.
//!
//! Run: `cargo run --release --example soak`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
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

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<f64>().ok())
        })
        .unwrap_or(0.0)
        / 1024.0
}

fn disk_free_gb(path: &std::path::Path) -> f64 {
    std::process::Command::new("df")
        .args(["-k", "--output=avail"])
        .arg(path)
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .nth(1)
                .and_then(|l| l.trim().parse::<f64>().ok())
        })
        .map(|kb| kb / 1024.0 / 1024.0)
        .unwrap_or(f64::INFINITY)
}

/// Registry shape across every mapped run: entries, distinct roots, and the
/// share of entries sitting on roots with `k >= 2`, which is the only part
/// grouping can compress.
fn registry_shape(layers: &blaze::storage::LayeredBase) -> (u64, u64, f64) {
    let (mut entries, mut roots, mut in_multi) = (0u64, 0u64, 0u64);
    for i in 0..layers.layers() {
        let (mut last, mut k) = (None, 0u64);
        let close = |k: u64, roots: &mut u64, in_multi: &mut u64| {
            if k > 0 {
                *roots += 1;
                if k >= 2 {
                    *in_multi += k;
                }
            }
        };
        for (root, _) in layers.layer(i).registry_iter() {
            if last == Some(root) {
                k += 1;
            } else {
                close(k, &mut roots, &mut in_multi);
                last = Some(root);
                k = 1;
            }
            entries += 1;
        }
        close(k, &mut roots, &mut in_multi);
    }
    (
        entries,
        roots,
        if entries == 0 {
            0.0
        } else {
            in_multi as f64 / entries as f64
        },
    )
}

#[tokio::main]
async fn main() {
    let links: u64 = env("LINKS", 50_000_000);
    let per_node: f64 = env("LINKS_PER_NODE", 1.8);
    let scopes: u32 = env("SCOPES", 3_000);
    let checkpoint: u64 = env("CHECKPOINT", 5_000_000);
    let min_disk: f64 = env("MIN_DISK_GB", 3.0);
    let global_pct: u32 = env("GLOBAL_PCT", 30);
    let nodes = ((links as f64) / per_node) as u64;

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

    let ingest = {
        let (forest, buffer, stop, ingested) = (
            forest.clone(),
            buffer.clone(),
            stop.clone(),
            ingested.clone(),
        );
        std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(0x50A11);
            let mut offset = 0u64;
            while offset < links && !stop.load(Ordering::Relaxed) {
                for _ in 0..10_000 {
                    let visibility = if rng.random_range(0..100u32) < global_pct {
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
                    buffer.append(0, offset, &e);
                    offset += 1;
                }
                ingested.fetch_add(10_000, Ordering::Relaxed);
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
        worker_id: "soak".into(),
        stream: None,
        base_dir: Some(cache.path().to_path_buf()),
        fold_after_links: u64::MAX,
        max_delta_layers: env("MAX_LAYERS", 12),
        tier_fanout: env("FANOUT", 10),
        write: Default::default(),
        inline_merges: false,
        layers: parking_lot::Mutex::new(None),
        pending_merge: parking_lot::Mutex::new(None),
    };

    println!(
        "{}M links at {per_node} links/node ({}M nodes), {scopes} scopes, {global_pct}% global, fanout {}, ceiling {}\n",
        links / 1_000_000,
        nodes / 1_000_000,
        env("FANOUT", 10),
        env("MAX_LAYERS", 12)
    );
    println!(
        "{:>7} {:>7} {:>6} {:>8} {:>7} {:>6} {:>7} {:>8} {:>6} {:>5} {:>7} {:>6}",
        "Mlinks",
        "Mpairs",
        "pr/lnk",
        "regEntry",
        "Mroots",
        "k",
        "multi%",
        "base MB",
        "B/pair",
        "runs",
        "look us",
        "RSS MB"
    );

    let started = Instant::now();
    let mut next_report = checkpoint;
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut qrng = StdRng::seed_from_u64(0xA71E);

    loop {
        ticker.tick().await;
        flusher.tick().await.unwrap();
        let done = ingested.load(Ordering::Relaxed);

        if done >= next_report || done >= links {
            next_report = done + checkpoint;
            let held = flusher.layers.lock();
            let Some(local) = held.as_ref() else { continue };
            let base = local.base.clone();
            let runs = local.runs.len();
            drop(held);

            let st = base.stats();
            let pairs = st.shared_pairs + st.overlay_pairs;
            let (entries, roots, multi) = registry_shape(&base);

            const Q: usize = 20_000;
            let t = Instant::now();
            let mut sink = 0u64;
            for _ in 0..Q {
                sink += forest.scope_root(
                    1 + qrng.random_range(0..scopes),
                    qrng.random_range(0..nodes),
                );
            }
            std::hint::black_box(sink);

            println!(
                "{:>7.1} {:>7.1} {:>6.2} {:>8.1}M {:>7.2} {:>6.2} {:>6.0}% {:>8.0} {:>6.1} {:>5} {:>7.2} {:>6.0}",
                done as f64 / 1e6,
                pairs as f64 / 1e6,
                pairs as f64 / done.max(1) as f64,
                entries as f64 / 1e6,
                roots as f64 / 1e6,
                entries as f64 / roots.max(1) as f64,
                100.0 * multi,
                st.mapped_bytes as f64 / 1e6,
                st.mapped_bytes as f64 / pairs.max(1) as f64,
                runs,
                t.elapsed().as_secs_f64() * 1e6 / Q as f64,
                rss_mb(),
            );

            let free = disk_free_gb(cache.path());
            if free < min_disk {
                println!("\nstopping: {free:.1} GB disk left, below the {min_disk} GB guard");
                break;
            }
        }
        if done >= links {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    ingest.join().unwrap();
    println!("\nwall {:.1} min", started.elapsed().as_secs_f64() / 60.0);
}
