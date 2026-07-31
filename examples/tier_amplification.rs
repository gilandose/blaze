//! Measure the claim tiering exists for: total merge work, flat versus tiered.
//!
//! Design 006 says flat base+delta compaction is O(N²) in total state — per-pair
//! cost is constant while each cycle merges linearly more — and that size-tiered
//! compaction turns that into O(N log N). That is an arithmetic argument, so this
//! runs both policies over identical ingest through the real fold and merge code
//! and reports what each actually moved.
//!
//! The headline number is **pairs merged per link ingested**: write amplification.
//! Under a flat policy it should grow with N; under tiering it should settle near
//! a constant of roughly one rewrite per level.
//!
//! Tunables via env: `LINKS`, `SCOPES`, `FOLD_LINKS`, `MAX_LAYERS`, `FANOUT`.
//!
//! Run: `cargo run --release --example tier_amplification`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::WriteOptions;
use blaze::storage::{LayeredBase, PuffinBase, codec, compact_layers, pick_merge, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Run {
    path: PathBuf,
    level: u8,
}

enum Policy {
    /// Rewrite the entire stack once it reaches the ceiling — what the flat
    /// base+delta compactor did, and the O(N²) baseline.
    ///
    /// Note this is *not* the same as running `pick_merge` with an unreachable
    /// fanout: that falls through to the depth-ceiling branch, which merges the
    /// lowest stretch rather than everything, and so is already tiered.
    Flat { ceiling: usize },
    /// Size-tiered: merge `fanout` runs at the lowest full level.
    Tiered { fanout: usize, ceiling: usize },
}

impl Policy {
    fn pick(&self, levels: &[u8]) -> Option<std::ops::Range<usize>> {
        match *self {
            Policy::Flat { ceiling } => (levels.len() >= ceiling).then_some(0..levels.len()),
            Policy::Tiered { fanout, ceiling } => pick_merge(levels, fanout, ceiling),
        }
    }
}

#[derive(Default)]
struct Totals {
    merges: u64,
    pairs_merged: u64,
    bytes_written: u64,
    merge_secs: f64,
    /// Deepest stack seen, which is what lookups and ingest pay for.
    max_runs: usize,
    /// Largest single merge, which is what bounds a stall and peak disk headroom.
    biggest_merge_pairs: u64,
}

/// Ingest `links` links, folding every `fold_links`, merging by `policy`.
fn drive(
    dir: &Path,
    links: u64,
    nodes: u64,
    scopes: u32,
    fold_links: u64,
    policy: &Policy,
) -> Totals {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    let forest = Arc::new(ScopedForest::new());
    let mut rng = StdRng::seed_from_u64(0xB1A2E);
    let mut totals = Totals::default();
    let mut stack: Option<(Arc<LayeredBase>, Vec<Run>)> = None;
    let mut seq = 0u64;
    let mut done = 0u64;

    while done < links {
        // --- ingest a fold's worth ---
        let batch = fold_links.min(links - done);
        for _ in 0..batch {
            let src = rng.random_range(0..nodes);
            let dst = rng.random_range(0..nodes);
            let visibility = if rng.random_range(0..100u32) < 30 {
                Visibility::Global
            } else {
                Visibility::Scoped(
                    (0..1 + rng.random_range(0..3u32))
                        .map(|_| 1 + rng.random_range(0..scopes))
                        .collect(),
                )
            };
            forest.apply(&EdgeEvent {
                src,
                dst,
                visibility,
                event_time_ms: 0,
                props: None,
            });
        }
        done += batch;

        // --- merge whatever the policy picks, before folding on top ---
        while let Some(range) = stack
            .as_ref()
            .and_then(|(_, runs)| policy.pick(&runs.iter().map(|r| r.level).collect::<Vec<_>>()))
        {
            let (base, runs) = stack.as_ref().expect("picked from a stack");
            seq += 1;
            let level = runs[range.clone()].iter().map(|r| r.level).max().unwrap() + 1;
            let path = dir.join(format!("run-{seq:06}-L{level}.puffin"));
            let sub = base.slice(range.clone()).unwrap();

            let t = Instant::now();
            let (blobs, cstats) = compact_layers(&sub, seq, WriteOptions::default());
            let bytes = puffin::write(&blobs, BTreeMap::new());
            std::fs::write(&path, &bytes).unwrap();
            let secs = t.elapsed().as_secs_f64();

            let pairs = cstats.shared_pairs + cstats.overlay_pairs;
            totals.merges += 1;
            totals.pairs_merged += pairs;
            totals.bytes_written += bytes.len() as u64;
            totals.merge_secs += secs;
            totals.biggest_merge_pairs = totals.biggest_merge_pairs.max(pairs);

            let mapped = Arc::new(PuffinBase::open(&path).unwrap());
            let next = Arc::new(base.spliced(range.clone(), mapped).unwrap());
            forest.swap_base(next.clone());
            let mut kept: Vec<Run> = Vec::with_capacity(runs.len() - range.len() + 1);
            for r in &runs[..range.start] {
                kept.push(Run {
                    path: r.path.clone(),
                    level: r.level,
                });
            }
            kept.push(Run { path, level });
            for r in &runs[range.end..] {
                kept.push(Run {
                    path: r.path.clone(),
                    level: r.level,
                });
            }
            let subsumed: Vec<PathBuf> = runs[range].iter().map(|r| r.path.clone()).collect();
            stack = Some((next, kept));
            for old in subsumed {
                let _ = std::fs::remove_file(old);
            }
        }

        // --- fold the memtable out as a new L0 run ---
        seq += 1;
        let path = dir.join(format!("run-{seq:06}-L0.puffin"));
        let mut writer = codec::BlobWriter::new(seq, WriteOptions::default());
        let prev = stack.as_ref().map(|(b, _)| b.clone());
        let folded = if let Some(base) = prev {
            forest
                .fold_delta(&mut writer, |w, _| {
                    let bytes = puffin::write(&w.finish(), BTreeMap::new());
                    std::fs::write(&path, &bytes)?;
                    let mapped = Arc::new(PuffinBase::open(&path)?);
                    let next = Arc::new(base.pushed(mapped));
                    anyhow::Ok((next.clone() as Arc<dyn RoutingBase>, (bytes.len(), next)))
                })
                .unwrap()
        } else {
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
        totals.bytes_written += folded.0 as u64;
        let mut runs = stack.map(|(_, r)| r).unwrap_or_default();
        runs.push(Run { path, level: 0 });
        totals.max_runs = totals.max_runs.max(runs.len());
        stack = Some((folded.1, runs));
    }

    totals
}

fn main() {
    let links: u64 = env("LINKS", 8_000_000);
    let scopes: u32 = env("SCOPES", 3_000);
    let fold_links: u64 = env("FOLD_LINKS", 250_000);
    let max_runs: usize = env("MAX_LAYERS", 8);
    let fanout: usize = env("FANOUT", 4);
    let nodes: u64 = links * 2;

    let dir = PathBuf::from(std::env::var("TIER_DIR").unwrap_or_else(|_| "/tmp/blaze-tier".into()));

    println!(
        "{links} links, {} folds, {scopes} scopes | flat ceiling {max_runs} runs, tier fanout {fanout}\n",
        links / fold_links
    );
    println!(
        "{:<10} {:>7} {:>12} {:>9} {:>10} {:>9} {:>7} {:>12}",
        "policy", "merges", "pairs merged", "amp", "GB written", "merge s", "runs", "biggest"
    );

    // Every arm holds the same depth ceiling, so this compares what each policy
    // costs to *write* at a matched read cost. Comparing at different depths
    // proves nothing: the flat policy can always cut its write cost by carrying
    // more runs, which is exactly the trade tiering is meant to break.
    let arms: Vec<(String, Policy)> = vec![
        ("flat".into(), Policy::Flat { ceiling: max_runs }),
        (
            format!("tier T={fanout}"),
            Policy::Tiered {
                fanout,
                ceiling: max_runs,
            },
        ),
    ];
    for (name, policy) in &arms {
        let t = drive(
            &dir.join(name.replace(' ', "-")),
            links,
            nodes,
            scopes,
            fold_links,
            policy,
        );
        println!(
            "{:<10} {:>7} {:>12} {:>8.2}x {:>10.2} {:>9.1} {:>7} {:>12}",
            name,
            t.merges,
            t.pairs_merged,
            // Pairs rewritten per link ingested: the write-amplification number.
            t.pairs_merged as f64 / links as f64,
            t.bytes_written as f64 / 1e9,
            t.merge_secs,
            t.max_runs,
            t.biggest_merge_pairs,
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
