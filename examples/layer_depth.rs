//! Isolate the cost of **layer depth** from the cost of state size.
//!
//! The ceiling run showed ingest falling as the layer stack grew, but state grew
//! at the same time, so the two were confounded. Here every variant reaches the
//! *same* total state and differs only in how many layers it was folded into,
//! which is the comparison that actually attributes the cost.
//!
//! Both paths are measured, because `apply` resolves its operands through the
//! composed path just as a query does — so depth is a write cost as well as a
//! read one, which is easy to miss.
//!
//! Run: `cargo run --release --example layer_depth`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::{LayeredBase, PuffinBase, codec, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

const STATE: u64 = 8_000_000;
const PROBE: u64 = 1_000_000;
const SCOPES: u32 = 3_000;
const NODES: u64 = 16_000_000;

fn event(rng: &mut StdRng) -> EdgeEvent {
    let visibility = if rng.random_range(0..100u8) < 15 {
        Visibility::Global
    } else {
        Visibility::Scoped(smallvec::smallvec![rng.random_range(1..=SCOPES)])
    };
    EdgeEvent {
        src: rng.random_range(0..NODES),
        dst: rng.random_range(0..NODES),
        visibility,
        event_time_ms: 0,
        props: None,
    }
}

fn main() {
    let dir = std::path::PathBuf::from("/tmp/blaze-depth");
    println!(
        "{STATE} links of state in every variant, then {PROBE} more measured.\n\
         Only the number of layers it was folded into differs.\n"
    );
    println!(
        "{:>6} {:>10} {:>10} {:>9} {:>9} {:>9}",
        "layers", "ingest/s", "vs 1 layer", "look_us", "vs 1 layer", "RSS_MB"
    );

    let mut baseline_ingest = 0.0;
    let mut baseline_look = 0.0;

    for depth in [1usize, 2, 4, 8, 16] {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same seed everywhere, so all variants hold identical state.
        let mut rng = StdRng::seed_from_u64(0xDEE9);
        let forest = ScopedForest::new();
        let mut stack: Option<Arc<LayeredBase>> = None;
        let per = STATE / depth as u64;

        for layer in 0..depth {
            for _ in 0..per {
                forest.apply(&event(&mut rng));
            }
            let path = dir.join(format!("l{layer}.puffin"));
            let mut w =
                codec::BlobWriter::new(layer as u64 + 1, blaze::storage::DEFAULT_FILTER_BITS);
            let next = match stack.take() {
                None => forest
                    .compact_and_fold(&mut w, |w| {
                        let bytes = puffin::write(&w.finish(), BTreeMap::new());
                        std::fs::write(&path, &bytes)?;
                        let b = Arc::new(LayeredBase::new(Arc::new(PuffinBase::open(&path)?)));
                        anyhow::Ok((b.clone() as Arc<dyn RoutingBase>, b))
                    })
                    .unwrap(),
                Some(prev) => forest
                    .fold_delta(&mut w, |w, _| {
                        let bytes = puffin::write(&w.finish(), BTreeMap::new());
                        std::fs::write(&path, &bytes)?;
                        let b = Arc::new(prev.pushed(Arc::new(PuffinBase::open(&path)?)));
                        anyhow::Ok((b.clone() as Arc<dyn RoutingBase>, b))
                    })
                    .unwrap(),
            };
            stack = Some(next);
        }
        let layers = stack.as_ref().unwrap().layers();
        assert_eq!(layers, depth);

        // Measured slice: ingest against this depth.
        let t = Instant::now();
        for _ in 0..PROBE {
            forest.apply(&event(&mut rng));
        }
        let ingest = PROBE as f64 / t.elapsed().as_secs_f64();

        // Lookups over the composed path at this depth.
        let mut q = StdRng::seed_from_u64(5);
        const Q: usize = 200_000;
        let t = Instant::now();
        let mut sink = 0u64;
        for _ in 0..Q {
            sink += forest.scope_root(q.random_range(0..=SCOPES), q.random_range(0..NODES));
        }
        std::hint::black_box(sink);
        let look_us = t.elapsed().as_secs_f64() * 1e6 / Q as f64;

        let rss = std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find(|l| l.starts_with("VmRSS"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            / 1024.0;

        if depth == 1 {
            baseline_ingest = ingest;
            baseline_look = look_us;
        }
        println!(
            "{:>6} {:>10.0} {:>9.2}x {:>9.2} {:>9.2}x {:>9.0}",
            layers,
            ingest,
            baseline_ingest / ingest,
            look_us,
            look_us / baseline_look,
            rss
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
