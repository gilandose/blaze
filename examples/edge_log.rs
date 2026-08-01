//! Generate an edge log to feed `blaze --edge-log`.
//!
//! Writes newline-delimited JSON `EdgeEvent`s, one per line, which is the format
//! the file log consumes and the format a Kafka topic dump lands in. The offset a
//! record gets is its line number, so the file *is* the offset assignment — which
//! is the property being stood in for.
//!
//! Tunables via env: `LINKS`, `LINKS_PER_NODE`, `SCOPES`, `GLOBAL_PCT`,
//! `MULTI_SCOPE_PCT`, `SEED`, `OUT`.
//!
//! ```text
//! LINKS=5000000 OUT=edges.ndjson cargo run --release --example edge_log
//! blaze --edge-log edges.ndjson --edge-log-follow false
//! ```
//!
//! `--edge-log-follow false` makes that second command a batch load: it ingests
//! the file, flushes, and then idles serving queries. Leaving follow on is the
//! streaming shape — append to the file and the worker picks it up, the same way
//! it would pick up a produce to a topic.

use blaze::core::{EdgeEvent, Visibility};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::io::{BufWriter, Write};
use std::str::FromStr;
use std::time::Instant;

fn env<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() -> anyhow::Result<()> {
    let links: u64 = env("LINKS", 1_000_000);
    let per_node: f64 = env("LINKS_PER_NODE", 1.8);
    let scopes: u32 = env("SCOPES", 3_000);
    let global_pct: u32 = env("GLOBAL_PCT", 30);
    let multi_scope_pct: u32 = env("MULTI_SCOPE_PCT", 20);
    let seed: u64 = env("SEED", 42);
    let out: String = env("OUT", "edges.ndjson".to_string());
    let nodes = ((links as f64) / per_node).max(1.0) as u64;

    println!(
        "{links} links over {nodes} nodes ({per_node} links/node), {scopes} scopes, \
         {global_pct}% global -> {out}"
    );

    let mut rng = StdRng::seed_from_u64(seed);
    let mut w = BufWriter::with_capacity(1 << 20, std::fs::File::create(&out)?);
    let start = Instant::now();

    for i in 0..links {
        let visibility = if rng.random_range(0..100u32) < global_pct {
            Visibility::Global
        } else {
            let s1 = rng.random_range(1..=scopes);
            if rng.random_range(0..100u32) < multi_scope_pct {
                Visibility::Scoped(smallvec::smallvec![s1, rng.random_range(1..=scopes)])
            } else {
                Visibility::Scoped(smallvec::smallvec![s1])
            }
        };
        let event = EdgeEvent {
            src: rng.random_range(0..nodes),
            dst: rng.random_range(0..nodes),
            visibility,
            event_time_ms: 1_700_000_000_000 + i as i64,
            props: None,
        };
        serde_json::to_writer(&mut w, &event)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;

    let secs = start.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(&out)?.len();
    println!(
        "wrote {links} records / {:.1} MB in {secs:.1}s ({:.0}k rec/s)",
        bytes as f64 / 1e6,
        links as f64 / secs / 1e3,
    );
    Ok(())
}
