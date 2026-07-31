//! What a delta layer actually costs when change is spread thinly across many
//! scopes.
//!
//! One Puffin blob per scope is fine for a base, where each blob holds enough
//! pairs to be worth indexing separately. For a *delta* it is a different
//! shape: the footer lists every blob, so per-layer overhead is O(scopes)
//! regardless of how little changed. At ~50 links/s spread over thousands of
//! tenants, a 60s delta touches a handful of pairs per scope — and the
//! bookkeeping can cost more than the data.
//!
//! Run: `cargo run --release --example blob_overhead`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::WriteOptions;
use blaze::storage::{LayeredBase, PuffinBase, codec, puffin};
use std::collections::BTreeMap;
use std::sync::Arc;

fn delta_for(
    scopes: u32,
    pairs_per_scope: u64,
    dir: &std::path::Path,
    tag: &str,
) -> (usize, usize, usize) {
    // A base to layer over, so the fold emits a delta rather than a full base.
    let seed = ScopedForest::new();
    seed.apply(&EdgeEvent {
        src: 1,
        dst: 2,
        visibility: Visibility::Global,
        event_time_ms: 0,
        props: None,
    });
    let mut w = codec::BlobWriter::new(1, WriteOptions::default());
    seed.compact_into(&mut w);
    let base_bytes = puffin::write(&w.finish(), BTreeMap::new());
    let base_path = dir.join(format!("{tag}-base.puffin"));
    std::fs::write(&base_path, &base_bytes).unwrap();
    let base = Arc::new(PuffinBase::open(&base_path).unwrap());

    let forest = ScopedForest::with_base(Arc::new(LayeredBase::new(base.clone())));
    let mut node = 1_000_000u64;
    for scope in 1..=scopes {
        for _ in 0..pairs_per_scope {
            forest.apply(&EdgeEvent {
                src: node,
                dst: node + 1,
                visibility: Visibility::Scoped(smallvec::smallvec![scope]),
                event_time_ms: 0,
                props: None,
            });
            node += 2;
        }
    }

    let mut w = codec::BlobWriter::new(2, WriteOptions::default());
    let (blobs, payload) = forest
        .fold_delta(&mut w, |w, _| {
            let blobs = w.finish();
            let payload: usize = blobs.iter().map(|b| b.data.len()).sum();
            let n = blobs.len();
            let bytes = puffin::write(&blobs, BTreeMap::new());
            let path = dir.join(format!("{tag}-delta.puffin"));
            std::fs::write(&path, &bytes).unwrap();
            let mapped: Arc<dyn RoutingBase> =
                Arc::new(LayeredBase::new(Arc::new(PuffinBase::open(&path).unwrap())));
            anyhow::Ok((mapped, (n, payload, bytes.len())))
        })
        .map(|(n, payload, total)| ((n, total), payload))
        .unwrap();
    (blobs.0, payload, blobs.1)
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    println!(
        "{:>7} {:>6} {:>7} {:>10} {:>10} {:>9}",
        "scopes", "p/scp", "blobs", "payload", "file", "overhead"
    );
    // A 60s delta at 50 links/s is ~3000 links. Spread it three ways.
    for (scopes, per) in [
        (3000u32, 1u64), // worst case: change dusted across every tenant
        (1000, 3),
        (100, 30),
        (10, 300), // best case: concentrated in a few tenants
    ] {
        let tag = format!("s{scopes}p{per}");
        let (blobs, payload, file) = delta_for(scopes, per, dir.path(), &tag);
        println!(
            "{scopes:>7} {per:>6} {blobs:>7} {:>9} B {:>9} B {:>8.0}%",
            payload,
            file,
            (file - payload) as f64 * 100.0 / file as f64
        );
    }
    println!(
        "\npayload = pair bytes the delta actually carries; file = the Puffin \
         object.\noverhead is footer + framing, and it scales with blob count \
         (one per scope),\nnot with how much changed."
    );
}
