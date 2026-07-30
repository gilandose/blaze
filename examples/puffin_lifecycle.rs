//! Demonstrates the Puffin sidecar lifecycle end to end:
//!
//! 1. Ingest edges and run a real leader flush (Parquet + Puffin + catalog
//!    commit) against a local warehouse.
//! 2. Ingest more, flush again — snapshot sequences increment and each Puffin
//!    file carries the *complete* routing map as of its watermark.
//! 3. Parse the newest Puffin file straight from disk: verify magic, list
//!    blobs (global + per-scope), and binary-search a node's routing entry
//!    the way an external reader would.
//! 4. Cold-start a fresh forest from the catalog and show it answers the
//!    same queries.
//!
//! Run with: cargo run --example puffin_lifecycle

use blaze::core::{EdgeEvent, GLOBAL_SCOPE, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, SnapshotCatalog, codec, hydrate_from_catalog, puffin};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

fn edge(src: u64, dst: u64, scopes: &[u32]) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: if scopes.is_empty() {
            Visibility::Global
        } else {
            Visibility::Scoped(scopes.iter().copied().collect())
        },
        event_time_ms: 1_700_000_000_000,
        props: None,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("blaze-puffin-demo-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir)?);
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));

    let forest = Arc::new(ScopedForest::new());
    let buffer = Arc::new(EdgeBuffer::new());
    let flusher = Flusher {
        forest: forest.clone(),
        buffer: buffer.clone(),
        store: store.clone(),
        catalog: catalog.clone(),
        elector: Arc::new(StaticElector(true)),
        table_prefix: prefix,
        worker_id: "demo".into(),
        // This example is about the committed Puffin lifecycle, so keep the
        // forest all-RAM and never fold.
        base_dir: None,
        fold_after_links: u64::MAX,
        max_delta_layers: usize::MAX,
        // No level ever fills and the ceiling is never reached, so nothing merges.
        tier_fanout: usize::MAX,
        layers: parking_lot::Mutex::new(None),
    };

    // --- Cycle 1: a global merge plus tenant-scoped edges ---
    let batch1 = [
        edge(500, 105, &[]), // Graph 500 routes to Graph 105 globally
        edge(1, 2, &[7]),    // visible only to tenant scope 7
        edge(3, 4, &[7]),
        edge(1, 2, &[2999]),
    ];
    for (i, e) in batch1.iter().enumerate() {
        forest.apply(e);
        buffer.append(i as u64 + 1, e);
    }
    flusher.tick().await?;
    let snap1 = catalog.latest().await?.expect("snapshot 1");
    println!(
        "cycle 1 committed: sequence={} watermark={} puffin={}",
        snap1.sequence, snap1.watermark, snap1.puffin_path
    );

    // --- Cycle 2: a global bridge that reroutes scope 7's islands ---
    let batch2 = [edge(1, 3, &[]), edge(900, 105, &[])];
    for (i, e) in batch2.iter().enumerate() {
        forest.apply(e);
        buffer.append(snap1.watermark + i as u64 + 1, e);
    }
    flusher.tick().await?;
    let snap2 = catalog.latest().await?.expect("snapshot 2");
    println!(
        "cycle 2 committed: sequence={} watermark={} puffin={}",
        snap2.sequence, snap2.watermark, snap2.puffin_path
    );
    assert_eq!(snap2.sequence, snap1.sequence + 1);
    assert!(snap2.watermark > snap1.watermark);

    // --- Parse the newest Puffin file like an external reader would ---
    let bytes = store
        .get(&StorePath::from(snap2.puffin_path.clone()))
        .await?
        .bytes()
        .await?;
    println!(
        "\npuffin file: {} bytes, magic {:?} .. {:?}",
        bytes.len(),
        &bytes[..4],
        &bytes[bytes.len() - 4..]
    );
    let blobs = puffin::read(&bytes)?;
    println!("blobs:");
    for blob in &blobs {
        let pairs = u64::from_le_bytes(blob.data[..8].try_into().unwrap());
        println!(
            "  type={:<24} snapshot-id={} scope={:<6} pairs={} bytes={}",
            blob.blob_type,
            blob.snapshot_id,
            blob.properties
                .get(codec::SCOPE_ID_PROP)
                .map(String::as_str)
                .unwrap_or("-"),
            pairs,
            blob.data.len()
        );
    }

    // O(log n) routing lookup inside the sorted global blob, no DSU needed:
    let global = blobs
        .iter()
        .find(|b| b.blob_type == codec::GLOBAL_BLOB_TYPE)
        .expect("global blob");
    let lookup = |node: u64| -> Option<u64> {
        let count = u64::from_le_bytes(global.data[..8].try_into().unwrap()) as usize;
        let (mut lo, mut hi) = (0usize, count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let base = 8 + mid * 16;
            let key = u64::from_le_bytes(global.data[base..base + 8].try_into().unwrap());
            match key.cmp(&node) {
                std::cmp::Ordering::Equal => {
                    return Some(u64::from_le_bytes(
                        global.data[base + 8..base + 16].try_into().unwrap(),
                    ));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    };
    println!(
        "\nrouting via blob binary search: 500 -> {:?}, 900 -> {:?}, 105 -> {:?} (root, absent)",
        lookup(500),
        lookup(900),
        lookup(105)
    );
    assert_eq!(lookup(500), Some(105));
    assert_eq!(lookup(900), Some(105));

    // --- Cold start from the catalog ---
    let fresh = ScopedForest::new();
    let watermark = hydrate_from_catalog(&fresh, &store, &catalog).await?;
    println!("\ncold start hydrated at watermark {watermark}:");
    println!(
        "  global: root(500)={} root(900)={}",
        fresh.scope_root(GLOBAL_SCOPE, 500),
        fresh.scope_root(GLOBAL_SCOPE, 900)
    );
    println!(
        "  scope 7: connected(2,4)={} (via scope edges + cycle-2 global bridge)",
        fresh.connected(7, 2, 4)
    );
    println!(
        "  scope 2999: connected(2,4)={}",
        fresh.connected(2999, 2, 4)
    );
    assert!(fresh.connected(7, 2, 4));
    assert!(!fresh.connected(2999, 2, 4));
    assert_eq!(watermark, snap2.watermark);

    std::fs::remove_dir_all(&dir).ok();
    println!("\nok");
    Ok(())
}
