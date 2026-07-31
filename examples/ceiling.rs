//! Drive the disk-backed engine until something gives, and print what.
//!
//! This runs the real steady-state loop — ingest, fold the memtable out as a
//! delta layer, compact the chain from storage when it grows — and reports the
//! quantities that are supposed to stay flat (resident memory, ingest rate,
//! lookup latency) alongside the ones that are supposed to grow (state, base
//! bytes, compaction duration). The point is to find where a prediction breaks
//! rather than to hit a number.
//!
//! Tunables via env: `LINKS`, `SCOPES`, `FOLD_LINKS`, `MAX_LAYERS`, `CHECKPOINT`.
//!
//! Run: `cargo run --release --example ceiling`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::WriteOptions;
use blaze::storage::{LayeredBase, PuffinBase, codec, compact_layers, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn rss_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    s.lines()
        .find(|l| l.starts_with("VmRSS"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
        / 1024.0
}

/// Bytes free on the filesystem holding `path`, so the run can stop before it
/// fills the disk rather than dying mid-write.
fn disk_free_gb(path: &std::path::Path) -> f64 {
    let out = std::process::Command::new("df")
        .args(["-k", "--output=avail"])
        .arg(path)
        .output()
        .ok();
    out.and_then(|o| {
        String::from_utf8_lossy(&o.stdout)
            .lines()
            .nth(1)
            .and_then(|l| l.trim().parse::<f64>().ok())
    })
    .map(|kb| kb / 1024.0 / 1024.0)
    .unwrap_or(f64::INFINITY)
}

struct Stack {
    base: Arc<LayeredBase>,
    paths: Vec<PathBuf>,
}

fn main() {
    let links: u64 = env("LINKS", 120_000_000);
    let scopes: u32 = env("SCOPES", 3_000);
    let fold_links: u64 = env("FOLD_LINKS", 4_000_000);
    let max_layers: usize = env("MAX_LAYERS", 8);
    let checkpoint: u64 = env("CHECKPOINT", 4_000_000);
    // Node space wide enough that components keep forming rather than
    // saturating into one giant blob, which would stop producing new links.
    let nodes: u64 = links * 2;

    let dir =
        PathBuf::from(std::env::var("CEILING_DIR").unwrap_or_else(|_| "/tmp/blaze-ceiling".into()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    println!(
        "target {links} links / {scopes} scopes / fold at {fold_links} / compact at {max_layers} layers\n\
         disk free {:.1} GB, dir {}\n",
        disk_free_gb(&dir),
        dir.display()
    );
    println!(
        "{:>7} {:>6} {:>6} {:>5} {:>8} {:>8} {:>8} {:>7} {:>8} {:>7}",
        "Mlinks",
        "RSS",
        "baseGB",
        "lyrs",
        "ingst/s",
        "look_us",
        "fold_ms",
        "foldMB",
        "cmpct_s",
        "diskGB"
    );

    let mut rng = StdRng::seed_from_u64(0xCE111);
    let mut qrng = StdRng::seed_from_u64(7);
    let forest = ScopedForest::new();
    let mut stack: Option<Stack> = None;
    let mut seq = 0u64;
    let mut done = 0u64;
    let mut last_fold_ms;
    let mut last_fold_mb;
    let mut last_compact_s = 0.0;
    let mut ingest_elapsed = 0.0;
    let mut ingest_since = 0u64;
    let started = Instant::now();

    let mut depth_at_ingest;
    while done < links {
        // --- ingest a slice ---
        // Depth *now* is what this slice's writes pay for: `apply` resolves its
        // operands through the composed path, so ingest scans layers too.
        depth_at_ingest = stack.as_ref().map(|s| s.base.layers()).unwrap_or(0);
        let slice = fold_links.min(links - done);
        let t = Instant::now();
        for _ in 0..slice {
            let visibility = if rng.random_range(0..100u8) < 15 {
                Visibility::Global
            } else {
                Visibility::Scoped(smallvec::smallvec![rng.random_range(1..=scopes)])
            };
            forest.apply(&EdgeEvent {
                src: rng.random_range(0..nodes),
                dst: rng.random_range(0..nodes),
                visibility,
                event_time_ms: 0,
                props: None,
            });
        }
        ingest_elapsed += t.elapsed().as_secs_f64();
        ingest_since += slice;
        done += slice;

        // --- fold, or compact if the chain is long enough ---
        let should_compact = stack
            .as_ref()
            .is_some_and(|s| s.base.layers() >= max_layers);

        if should_compact {
            let s = stack.take().unwrap();
            seq += 1;
            let t = Instant::now();
            let (blobs, cstats) = compact_layers(&s.base, seq, WriteOptions::default());
            let bytes = puffin::write(&blobs, BTreeMap::new());
            let path = dir.join(format!("base-{seq:06}.puffin"));
            std::fs::write(&path, &bytes).unwrap();
            let flat = Arc::new(LayeredBase::new(Arc::new(PuffinBase::open(&path).unwrap())));
            forest.swap_base(flat.clone());
            last_compact_s = t.elapsed().as_secs_f64();
            for old in &s.paths {
                let _ = std::fs::remove_file(old);
            }
            println!(
                "  -> compacted {} layers in {:.1}s: {} shared + {} overlay pairs, \
                 {} registry ({} corrections, {} moved-root set), {:.2} GB",
                s.base.layers(),
                last_compact_s,
                cstats.shared_pairs,
                cstats.overlay_pairs,
                cstats.registry_entries,
                cstats.registry_corrections,
                cstats.moved_roots,
                bytes.len() as f64 / 1e9
            );
            stack = Some(Stack {
                base: flat,
                paths: vec![path],
            });
        }

        seq += 1;
        let path = dir.join(format!("layer-{seq:06}.puffin"));
        let t = Instant::now();
        let mut writer = codec::BlobWriter::new(seq, WriteOptions::default());
        let prev = stack.as_ref().map(|s| s.base.clone());
        let written = if let Some(prev_base) = prev {
            forest
                .fold_delta(&mut writer, |w, _| {
                    let bytes = puffin::write(&w.finish(), BTreeMap::new());
                    std::fs::write(&path, &bytes)?;
                    let mapped = Arc::new(PuffinBase::open(&path)?);
                    let next = Arc::new(prev_base.pushed(mapped));
                    anyhow::Ok((next.clone() as Arc<dyn RoutingBase>, (bytes.len(), next)))
                })
                .unwrap()
        } else {
            // First layer: no base to append to, so this one *is* the base.
            forest
                .compact_and_fold(&mut writer, |w| {
                    let bytes = puffin::write(&w.finish(), BTreeMap::new());
                    std::fs::write(&path, &bytes)?;
                    let mapped = Arc::new(PuffinBase::open(&path)?);
                    let next = Arc::new(LayeredBase::new(mapped));
                    anyhow::Ok((next.clone() as Arc<dyn RoutingBase>, (bytes.len(), next)))
                })
                .unwrap()
        };
        last_fold_ms = t.elapsed().as_secs_f64() * 1000.0;
        last_fold_mb = written.0 as f64 / 1e6;
        let mut paths = stack.map(|s| s.paths).unwrap_or_default();
        paths.push(path);
        stack = Some(Stack {
            base: written.1,
            paths,
        });

        // --- checkpoint ---
        if done.is_multiple_of(checkpoint) || done >= links {
            let st = stack.as_ref().unwrap();
            let bs = st.base.stats();
            // Warm lookup latency over the live composed path.
            const Q: usize = 20_000;
            let t = Instant::now();
            let mut sink = 0u64;
            for _ in 0..Q {
                sink +=
                    forest.scope_root(qrng.random_range(0..=scopes), qrng.random_range(0..nodes));
            }
            std::hint::black_box(sink);
            let look_us = t.elapsed().as_secs_f64() * 1e6 / Q as f64;
            let base_gb: f64 = st
                .paths
                .iter()
                .filter_map(|p| std::fs::metadata(p).ok())
                .map(|m| m.len() as f64)
                .sum::<f64>()
                / 1e9;
            let free = disk_free_gb(&dir);
            println!(
                "{:>7.1} {:>5.0}M {:>6.2} {:>5} {:>8.0} {:>8.2} {:>8.0} {:>8.1} {:>8.1} {:>7.1}",
                done as f64 / 1e6,
                rss_mb(),
                base_gb,
                depth_at_ingest,
                ingest_since as f64 / ingest_elapsed.max(1e-9),
                look_us,
                last_fold_ms,
                last_fold_mb,
                last_compact_s,
                free
            );
            let _ = bs;
            ingest_elapsed = 0.0;
            ingest_since = 0;
            // Stop before filling the disk: a compaction needs room for the old
            // and new base at once.
            if free < 2.0 * base_gb + 1.0 {
                println!(
                    "\nSTOP: {free:.1} GB free is not enough headroom for another \
                     compaction of a {base_gb:.2} GB base"
                );
                break;
            }
        }
    }

    println!(
        "\ndone: {:.1}M links in {:.1} min",
        done as f64 / 1e6,
        started.elapsed().as_secs_f64() / 60.0
    );
    let _ = std::fs::remove_dir_all(&dir);
}
