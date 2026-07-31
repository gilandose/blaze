//! What is actually in the registry, and what would restructuring it save?
//!
//! The registry maps a shared root to the scopes holding overlay state keyed on
//! it, so a global merge notifies only those scopes instead of broadcasting to
//! thousands. Design 006 measured it at 55% of base bytes and proposed grouping
//! entries by root. That trade is not obviously good: the flat form costs 12
//! bytes per `(root, scope)`, and a grouped form costs `12 + 4k` for a root with
//! `k` scopes — so it *loses* at `k = 1` and only wins from `k = 2`.
//!
//! Which means the saving is entirely decided by the distribution of scopes per
//! root, and that is a property of the graph, not of the format. This measures
//! it rather than assuming it.
//!
//! Tunables via env: `LINKS`, `NODES`, `SCOPES`, `SCOPES_PER_EDGE`.
//!
//! Run: `cargo run --release --example registry_shape`

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::storage::{PuffinBase, codec, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let links: u64 = env("LINKS", 2_000_000);
    let nodes: u64 = env("NODES", 4_000_000);
    let scopes: u32 = env("SCOPES", 3_000);
    let per_edge: u32 = env("SCOPES_PER_EDGE", 3);

    let forest = ScopedForest::new();
    let mut rng = StdRng::seed_from_u64(0xEE9157);
    for _ in 0..links {
        let visibility = if rng.random_range(0..100u32) < 30 {
            Visibility::Global
        } else {
            Visibility::Scoped(
                (0..1 + rng.random_range(0..per_edge))
                    .map(|_| 1 + rng.random_range(0..scopes))
                    .collect(),
            )
        };
        forest.apply(&EdgeEvent {
            src: rng.random_range(0..nodes),
            dst: rng.random_range(0..nodes),
            visibility,
            event_time_ms: 0,
            props: None,
        });
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("base.puffin");
    let blobs = codec::compact_to_blobs(&forest, 1, blaze::storage::DEFAULT_FILTER_BITS);
    std::fs::write(&path, puffin::write(&blobs, BTreeMap::new())).unwrap();
    let base = PuffinBase::open(&path).unwrap();

    // Registry entries arrive sorted by (root, scope), so a root's scopes are a
    // contiguous run and counting them is a single pass.
    let n = base.registry_len();
    let mut per_root: Vec<u32> = Vec::new();
    let mut i = 0;
    while i < n {
        let (root, _) = base.registry_at(i);
        let mut k = 0u32;
        while i < n && base.registry_at(i).0 == root {
            k += 1;
            i += 1;
        }
        per_root.push(k);
    }

    let roots = per_root.len() as u64;
    let entries = n as u64;
    let flat_bytes = 8 + entries * 12;
    // Grouped: 8-byte header, then per root an 8-byte root, 4-byte count, and a
    // 4-byte scope each.
    let grouped_bytes = 8 + roots * 12 + entries * 4;

    let mut hist: BTreeMap<&str, u64> = BTreeMap::new();
    for &k in &per_root {
        let bucket = match k {
            1 => "k=1",
            2 => "k=2",
            3..=4 => "k=3-4",
            5..=8 => "k=5-8",
            9..=16 => "k=9-16",
            17..=64 => "k=17-64",
            65..=256 => "k=65-256",
            _ => "k>256",
        };
        *hist.entry(bucket).or_default() += 1;
    }

    let stats = blaze::core::RoutingBase::stats(&base);
    let base_bytes = stats.mapped_bytes;
    let pairs = stats.shared_pairs + stats.overlay_pairs;

    println!(
        "{links} links / {nodes} nodes / {scopes} scopes / up to {per_edge} scopes per edge\n"
    );
    println!("pairs                {pairs}");
    println!("registry entries     {entries}");
    println!("distinct roots       {roots}");
    println!(
        "mean scopes/root     {:.2}",
        entries as f64 / roots.max(1) as f64
    );
    println!("\nbase file            {:.1} MB", base_bytes as f64 / 1e6);
    println!(
        "registry (flat)      {:.1} MB   {:.0}% of base",
        flat_bytes as f64 / 1e6,
        100.0 * flat_bytes as f64 / base_bytes as f64
    );
    println!(
        "registry (grouped)   {:.1} MB   {:.0}% of base   -> {:.2}x smaller, base {:.0}% smaller",
        grouped_bytes as f64 / 1e6,
        100.0 * grouped_bytes as f64 / base_bytes as f64,
        flat_bytes as f64 / grouped_bytes as f64,
        100.0 * (flat_bytes - grouped_bytes) as f64 / base_bytes as f64
    );

    println!("\nscopes per root:");
    for (bucket, count) in &hist {
        let share = 100.0 * *count as f64 / roots.max(1) as f64;
        // Entries, not roots, is what the format cost scales with.
        println!("  {bucket:<9} {count:>10} roots  ({share:.1}%)");
    }
}
