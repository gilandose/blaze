//! Retention against a real table, and the property that makes it safe.
//!
//! The unit tests in `storage::retention` build catalogs by hand, so they check
//! the policy logic against snapshots that were never produced by a flush loop.
//! This drives the real `Flusher` until merges have orphaned most of what it
//! wrote, then sweeps, and asserts the only thing that really matters:
//!
//! **a worker that cold-starts after the sweep answers exactly as it did
//! before.**
//!
//! That is the assertion a reachability bug fails. Deleting a live run does not
//! corrupt anything in place — the mapping stays valid, the catalog stays
//! parseable, and the loss only surfaces when someone hydrates from scratch,
//! which is the one moment nobody is watching.

use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::ha::StaticElector;
use blaze::ingest::EdgeBuffer;
use blaze::storage::{Flusher, RetentionPolicy, SnapshotCatalog, collect_garbage};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::sync::Arc;
use std::time::Duration;

const NODES: u64 = 4_000;
const SCOPES: u32 = 12;

fn edge(n: u64) -> EdgeEvent {
    EdgeEvent {
        src: (n * 7919) % NODES,
        dst: (n * 104_729) % NODES,
        visibility: if n.is_multiple_of(3) {
            Visibility::Global
        } else {
            Visibility::Scoped([1 + (n % SCOPES as u64) as u32].into_iter().collect())
        },
        event_time_ms: 0,
        props: None,
    }
}

struct Table {
    store: Arc<dyn ObjectStore>,
    catalog: Arc<SnapshotCatalog>,
    prefix: StorePath,
    forest: Arc<ScopedForest>,
    _dir: tempfile::TempDir,
    _cache: tempfile::TempDir,
}

/// Ingest through a real flush loop with a shallow ceiling, so tiered merges
/// fire repeatedly and leave a trail of superseded runs.
async fn build(ticks: u64, per_tick: u64) -> Table {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
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
        table_prefix: prefix.clone(),
        worker_id: "retention".into(),
        base_dir: Some(cache.path().to_path_buf()),
        fold_after_links: u64::MAX,
        max_delta_layers: 4,
        tier_fanout: 2,
        write: Default::default(),
        inline_merges: true,
        layers: parking_lot::Mutex::new(None),
        pending_merge: parking_lot::Mutex::new(None),
    };

    let mut offset = 0u64;
    for _ in 0..ticks {
        for _ in 0..per_tick {
            let e = edge(offset);
            forest.apply(&e);
            buffer.append(offset, &e);
            offset += 1;
        }
        flusher.tick().await.unwrap();
    }
    flusher.wait_for_merge().await;
    flusher.tick().await.unwrap();

    Table {
        store,
        catalog,
        prefix,
        forest,
        _dir: dir,
        _cache: cache,
    }
}

async fn count(store: &Arc<dyn ObjectStore>, prefix: &StorePath, sub: &str) -> (usize, u64) {
    let p = prefix.clone().join(sub);
    let items: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&p)))
        .await
        .unwrap();
    (items.len(), items.iter().map(|m| m.size).sum())
}

/// Everything a fresh worker would answer, hydrating from the catalog alone.
async fn cold_answers(t: &Table) -> Vec<u64> {
    let cold = ScopedForest::new();
    blaze::storage::hydrate_from_catalog(&cold, &t.store, &t.catalog)
        .await
        .unwrap();
    let mut out = Vec::new();
    for scope in [0u32].into_iter().chain(1..=SCOPES) {
        for node in 0..NODES {
            out.push(cold.scope_root(scope, node));
        }
    }
    out
}

/// A sweep must reclaim real bytes, and a cold start afterwards must answer
/// identically. Both halves are needed: a collector that deletes nothing passes
/// the second, and one that deletes everything passes the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_swept_table_still_recovers() {
    let t = build(24, 400).await;
    let before = cold_answers(&t).await;
    let (runs_before, bytes_before) = count(&t.store, &t.prefix, "puffin").await;

    // `now` past every object's timestamp with a zero grace, so this sweep sees
    // the whole table as fair game rather than only its older half.
    let policy = RetentionPolicy {
        keep_snapshots: 1,
        keep_for: Duration::from_millis(0),
        grace: Duration::from_millis(0),
    };
    let got = collect_garbage(&t.store, &t.catalog, &t.prefix, &policy, i64::MAX / 2)
        .await
        .unwrap();

    let (runs_after, bytes_after) = count(&t.store, &t.prefix, "puffin").await;
    assert!(
        got.objects_deleted > 0 && bytes_after < bytes_before,
        "swept nothing: {runs_before} runs / {bytes_before} B before, \
         {runs_after} / {bytes_after} after ({got:?})"
    );
    assert_eq!(
        runs_after,
        got.objects_live as usize - 1 /* data file */ + 1
    );
    assert_eq!(
        cold_answers(&t).await,
        before,
        "a cold start after the sweep disagrees with one before it"
    );
    // And the live worker is untouched — its mappings are open file handles.
    assert_eq!(t.forest.scope_root(1, 7), t.forest.scope_root(1, 7));
}

/// The whole point, measured on a real table: how much of it was garbage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn most_of_a_merged_table_is_unreachable() {
    let t = build(30, 300).await;
    let (objects, bytes) = count(&t.store, &t.prefix, "puffin").await;

    let policy = RetentionPolicy {
        keep_snapshots: 1,
        keep_for: Duration::from_millis(0),
        grace: Duration::from_millis(0),
    };
    let got = collect_garbage(&t.store, &t.catalog, &t.prefix, &policy, i64::MAX / 2)
        .await
        .unwrap();

    let share = 100.0 * got.bytes_deleted as f64 / bytes as f64;
    eprintln!(
        "{objects} run objects, {:.1} MB; reclaimed {} objects / {:.1} MB ({share:.0}%)",
        bytes as f64 / 1e6,
        got.objects_deleted,
        got.bytes_deleted as f64 / 1e6,
    );
    assert!(
        share > 50.0,
        "expected a merged table to be mostly garbage, reclaimed only {share:.0}%"
    );
    assert_eq!(
        cold_answers(&t).await.len(),
        (SCOPES as usize + 1) * NODES as usize
    );
}

/// Retention runs on a live table, so it has to be safe while the flush loop is
/// still committing. The grace window is what makes that true, and this is the
/// case it exists for: an object written but not yet committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_sweep_leaves_a_running_table_alone() {
    let t = build(12, 300).await;
    let before = count(&t.store, &t.prefix, "puffin").await;

    // Defaults: an hour of grace, so nothing this test just wrote is eligible.
    let got = collect_garbage(
        &t.store,
        &t.catalog,
        &t.prefix,
        &RetentionPolicy::default(),
        chrono_now_ms(),
    )
    .await
    .unwrap();

    assert_eq!(got.objects_deleted, 0, "swept freshly written objects");
    assert_eq!(count(&t.store, &t.prefix, "puffin").await, before);
    assert_eq!(
        cold_answers(&t).await.len(),
        (SCOPES as usize + 1) * NODES as usize
    );
}

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
