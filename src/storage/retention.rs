//! Reclaim what no retained snapshot can reach.
//!
//! Nothing in the commit path ever deletes an object, and tiering means the
//! write rate is several times the rate the live set grows: every merge writes a
//! new run and orphans the ones it read. Measured mid-soak, a table with **11
//! live runs was holding 493 objects — 80% of the bytes unreachable**, with the
//! share still climbing. Left alone the bucket grows at roughly the write
//! amplification factor rather than at the rate of the data.
//!
//! This is Iceberg's `expireSnapshots` plus `removeOrphanFiles`, and it borrows
//! their shape because the hazards are the same.
//!
//! # What makes an object live
//!
//! The run set already names exactly what a snapshot needs, so liveness is a
//! reachability question rather than a bookkeeping one — there is no refcount to
//! get wrong. But reachability is **not** per-snapshot:
//!
//! - A `RunSet::Runs` snapshot is self-contained.
//! - A `RunSet::SequencesOnly` snapshot — the pre-run-set format — hydrates by
//!   walking every sequence from its base forward (`committed_runs`), so
//!   retaining it requires retaining that whole chain. Expiring an intermediate
//!   snapshot would leave a retained one unrecoverable.
//!
//! So the retained set is a **closure**, not a suffix.
//!
//! # Why the grace period is not optional
//!
//! A tick writes its data file and its run object, and only then commits the
//! metadata naming them. In that window the objects are live-to-be and reachable
//! from nothing. A sweep that deleted every unreferenced object would delete the
//! in-flight commit's own inputs, and the commit would publish a snapshot
//! pointing at objects that no longer exist — corruption produced *by* the
//! garbage collector, which is the worst way to get it.
//!
//! `grace` is the answer: an object younger than it is never touched, whatever
//! the catalog says about it. It must exceed the longest plausible gap between
//! writing an object and committing it — a slow merge, a retried upload — so it
//! is measured in hours, not seconds.
//!
//! # Expiring a snapshot spends a recovery point
//!
//! Every retained snapshot is a point a worker can be rolled back to. Retention
//! is therefore a durability setting wearing a storage-cost disguise, and the
//! defaults lean toward keeping too much.

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::storage::catalog::{RunSet, SnapshotCatalog, SnapshotMeta};

/// How much history to keep.
#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
    /// Newest snapshots to keep whatever their age. Never less than 1 — the
    /// newest is the one the cluster is serving.
    pub keep_snapshots: usize,
    /// Also keep any snapshot committed within this window.
    pub keep_for: Duration,
    /// Never delete an object younger than this, reachable or not. Protects
    /// commits that are in flight right now.
    pub grace: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            // Ten snapshots at a 60s flush interval is ten minutes of rollback,
            // and ten run sets is a trivial amount of metadata to keep.
            keep_snapshots: 10,
            keep_for: Duration::from_secs(24 * 3600),
            // Comfortably longer than any single merge-and-commit. A merge of a
            // high tier is minutes; an hour leaves room for a retry storm.
            grace: Duration::from_secs(3600),
        }
    }
}

/// What a sweep reclaimed, and what it decided to keep.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Reclaimed {
    pub snapshots_expired: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    pub snapshots_kept: u64,
    pub objects_live: u64,
    /// Unreachable objects left alone because they were inside `grace`.
    pub objects_too_young: u64,
}

/// Delete everything no retained snapshot can reach.
///
/// `now_ms` is passed in rather than read from the clock so a test can place
/// objects on either side of the grace boundary without sleeping.
///
/// Best-effort by design: a delete that fails is logged and retried by the next
/// sweep. Nothing here is required for correctness of the data, only for its
/// size, so a permissions problem on one object must not fail the loop that
/// calls this.
pub async fn collect(
    store: &Arc<dyn ObjectStore>,
    catalog: &SnapshotCatalog,
    prefix: &Path,
    policy: &RetentionPolicy,
    now_ms: i64,
) -> anyhow::Result<Reclaimed> {
    let mut all = list_snapshot_sequences(store, prefix).await?;
    if all.is_empty() {
        return Ok(Reclaimed::default());
    }

    let keep = policy.keep_snapshots.max(1);
    let cutoff_ms = now_ms - policy.keep_for.as_millis() as i64;

    // Seed: the newest `keep`, plus anything inside the time window.
    let newest: Vec<u64> = all.iter().rev().take(keep).copied().collect();
    let mut retained: BTreeSet<u64> = newest.into_iter().collect();
    let mut metas = Vec::new();
    for &sequence in &all {
        let meta = match catalog.get(sequence).await {
            Ok(m) => m,
            Err(e) => {
                // Unreadable metadata means we cannot prove anything it names is
                // dead, so keep the sweep conservative and skip this round.
                warn!(sequence, error = %e, "snapshot unreadable; skipping retention sweep");
                return Ok(Reclaimed::default());
            }
        };
        if meta.committed_at_ms >= cutoff_ms {
            retained.insert(sequence);
        }
        metas.push(meta);
    }

    // Closure: a retained snapshot in the pre-run-set format needs its whole
    // chain, so pulling one in can pull in others.
    let by_sequence: std::collections::HashMap<u64, &SnapshotMeta> =
        metas.iter().map(|m| (m.sequence, m)).collect();
    let mut queue: Vec<u64> = retained.iter().copied().collect();
    while let Some(sequence) = queue.pop() {
        let Some(meta) = by_sequence.get(&sequence) else {
            continue;
        };
        if let Ok(RunSet::SequencesOnly(chain)) = meta.run_set() {
            for needed in chain {
                if retained.insert(needed) {
                    queue.push(needed);
                }
            }
        }
    }

    // Live objects: everything any retained snapshot names.
    let mut live: HashSet<String> = HashSet::new();
    for meta in &metas {
        if !retained.contains(&meta.sequence) {
            continue;
        }
        live.insert(meta.puffin_path.clone());
        for f in &meta.data_files {
            live.insert(f.path.clone());
        }
        // `runs` is read directly rather than through `run_set()` so a snapshot
        // that fails validation still protects the objects it mentions.
        for r in &meta.runs {
            live.insert(r.path.clone());
        }
    }

    let grace_ms = now_ms - policy.grace.as_millis() as i64;
    let mut out = Reclaimed {
        snapshots_kept: retained.len() as u64,
        objects_live: live.len() as u64,
        ..Default::default()
    };

    // Sweep the object prefixes. Metadata is handled separately below, since a
    // snapshot JSON is expired by policy rather than by reachability.
    for sub in ["puffin", "data"] {
        let scope = prefix.clone().join(sub);
        let mut listing = store.list(Some(&scope));
        let mut doomed = Vec::new();
        while let Some(meta) = futures::TryStreamExt::try_next(&mut listing).await? {
            let path = meta.location.to_string();
            if live.contains(&path) {
                continue;
            }
            if meta.last_modified.timestamp_millis() > grace_ms {
                out.objects_too_young += 1;
                continue;
            }
            doomed.push((meta.location, meta.size));
        }
        for (path, size) in doomed {
            match store.delete(&path).await {
                Ok(()) => {
                    out.objects_deleted += 1;
                    out.bytes_deleted += size;
                    debug!(path = %path, "reclaimed unreachable object");
                }
                Err(e) => warn!(path = %path, error = %e, "could not delete"),
            }
        }
    }

    // Expire snapshot metadata last: while a JSON is still present it is keeping
    // its objects alive, and the objects have just been removed. Doing it in
    // this order means a crash midway leaves a snapshot naming deleted objects
    // for one sweep — which `catalog.get` reports honestly — rather than
    // silently unreachable objects nothing will ever collect.
    all.retain(|s| !retained.contains(s));
    for sequence in all {
        let path = snapshot_path(prefix, sequence);
        match store.head(&path).await {
            Ok(m) if m.last_modified.timestamp_millis() > grace_ms => {
                out.objects_too_young += 1;
                continue;
            }
            Err(_) => continue,
            Ok(_) => {}
        }
        match store.delete(&path).await {
            Ok(()) => out.snapshots_expired += 1,
            Err(e) => warn!(sequence, error = %e, "could not expire snapshot"),
        }
    }

    if out.objects_deleted > 0 || out.snapshots_expired > 0 {
        info!(
            snapshots_expired = out.snapshots_expired,
            objects_deleted = out.objects_deleted,
            mb_deleted = out.bytes_deleted / (1024 * 1024),
            snapshots_kept = out.snapshots_kept,
            objects_live = out.objects_live,
            "reclaimed unreachable storage"
        );
    }
    Ok(out)
}

fn snapshot_path(prefix: &Path, sequence: u64) -> Path {
    prefix
        .clone()
        .join("metadata")
        .join(format!("snap-{sequence:012}.json"))
}

/// Every committed sequence, ascending.
async fn list_snapshot_sequences(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> anyhow::Result<Vec<u64>> {
    let meta_prefix = prefix.clone().join("metadata");
    let mut out = Vec::new();
    let mut listing = store.list(Some(&meta_prefix));
    while let Some(meta) = futures::TryStreamExt::try_next(&mut listing).await? {
        if let Some(seq) = meta
            .location
            .filename()
            .and_then(|n| n.strip_prefix("snap-"))
            .and_then(|n| n.strip_suffix(".json"))
            .and_then(|n| n.parse::<u64>().ok())
        {
            out.push(seq);
        }
    }
    out.sort_unstable();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::catalog::{DataFileMeta, RunMeta};
    use object_store::{PutPayload, memory::InMemory};

    /// A `now` later than any wall-clock timestamp the store will stamp on an
    /// object, so a zero grace window really does expose everything. Using a
    /// small constant here does the opposite — `InMemory` dates objects with the
    /// real clock, so "100 hours after the epoch" makes every object look like
    /// it was written in the future and therefore inside any grace window.
    const AFTER_EVERYTHING: i64 = i64::MAX / 2;

    fn prefix() -> Path {
        Path::from("graph/edges")
    }

    fn policy(keep: usize) -> RetentionPolicy {
        RetentionPolicy {
            keep_snapshots: keep,
            keep_for: Duration::from_millis(0),
            grace: Duration::from_millis(0),
        }
    }

    fn run(path: &str, lo: u64, hi: u64) -> RunMeta {
        RunMeta {
            path: path.into(),
            level: 0,
            min_sequence: lo,
            max_sequence: hi,
            pairs: 1,
            bytes: 1,
        }
    }

    fn meta(sequence: u64, runs: Vec<RunMeta>) -> SnapshotMeta {
        SnapshotMeta {
            sequence,
            committed_at_ms: 0,
            watermark: sequence * 10,
            position: crate::core::StreamPosition::single(sequence * 10),
            stream: None,
            data_files: vec![DataFileMeta {
                path: format!("graph/edges/data/part-{sequence}.parquet"),
                rows: 1,
                bytes: 1,
                min_offset: 0,
                max_offset: sequence * 10,
                first_position: Default::default(),
                last_position: Default::default(),
            }],
            puffin_path: runs
                .last()
                .map(|r| r.path.clone())
                .unwrap_or_else(|| format!("graph/edges/puffin/dsu-{sequence}.puffin")),
            committer: "test".into(),
            base_sequence: sequence,
            delta_chain_len: 0,
            format_version: if runs.is_empty() { 0 } else { 1 },
            runs,
        }
    }

    /// Commit `snapshots`, materializing every object they name.
    async fn seed(store: &Arc<dyn ObjectStore>, catalog: &SnapshotCatalog, snaps: &[SnapshotMeta]) {
        for m in snaps {
            for p in std::iter::once(&m.puffin_path)
                .chain(m.runs.iter().map(|r| &r.path))
                .chain(m.data_files.iter().map(|f| &f.path))
            {
                store
                    .put(&Path::from(p.clone()), PutPayload::from_static(b"x"))
                    .await
                    .unwrap();
            }
            catalog.commit(m).await.unwrap();
        }
    }

    async fn objects(store: &Arc<dyn ObjectStore>, sub: &str) -> Vec<String> {
        let p = prefix().join(sub);
        let mut out: Vec<String> =
            futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&p)))
                .await
                .unwrap()
                .into_iter()
                .map(|m| m.location.to_string())
                .collect();
        out.sort();
        out
    }

    fn harness() -> (Arc<dyn ObjectStore>, SnapshotCatalog) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SnapshotCatalog::new(store.clone(), prefix());
        (store, catalog)
    }

    /// The case the whole thing exists for: a stack whose merges have orphaned
    /// most of what was ever written.
    #[tokio::test]
    async fn reclaims_runs_no_retained_snapshot_names() {
        let (store, catalog) = harness();
        // Five folds, then a merge that subsumes all of them into one run.
        let mut snaps: Vec<SnapshotMeta> = (1..=5)
            .map(|s| {
                let runs = (1..=s)
                    .map(|i| run(&format!("graph/edges/puffin/dsu-{i}.puffin"), i, i))
                    .collect();
                meta(s, runs)
            })
            .collect();
        snaps.push(meta(
            6,
            vec![run("graph/edges/puffin/merged-1-5.puffin", 1, 5)],
        ));
        seed(&store, &catalog, &snaps).await;
        assert_eq!(objects(&store, "puffin").await.len(), 6);

        let got = collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();

        assert_eq!(got.snapshots_kept, 1);
        assert_eq!(got.snapshots_expired, 5);
        assert_eq!(
            objects(&store, "puffin").await,
            vec!["graph/edges/puffin/merged-1-5.puffin"],
            "only the merged run is reachable from snapshot 6"
        );
        assert_eq!(objects(&store, "data").await.len(), 1);
    }

    /// Retention must never remove something a retained snapshot names, even
    /// when a *newer* snapshot has stopped naming it.
    #[tokio::test]
    async fn keeps_everything_the_retained_window_can_reach() {
        let (store, catalog) = harness();
        let snaps = vec![
            meta(1, vec![run("graph/edges/puffin/a.puffin", 1, 1)]),
            meta(
                2,
                vec![
                    run("graph/edges/puffin/a.puffin", 1, 1),
                    run("graph/edges/puffin/b.puffin", 2, 2),
                ],
            ),
            meta(3, vec![run("graph/edges/puffin/merged.puffin", 1, 3)]),
        ];
        seed(&store, &catalog, &snaps).await;

        // Keeping two snapshots keeps 2 and 3, so `a` and `b` are still live
        // through snapshot 2 even though snapshot 3 has merged them away.
        collect(&store, &catalog, &prefix(), &policy(2), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(
            objects(&store, "puffin").await,
            vec![
                "graph/edges/puffin/a.puffin",
                "graph/edges/puffin/b.puffin",
                "graph/edges/puffin/merged.puffin",
            ]
        );

        // Narrowing to one snapshot releases them.
        collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(
            objects(&store, "puffin").await,
            vec!["graph/edges/puffin/merged.puffin"]
        );
    }

    /// A pre-run-set snapshot hydrates by walking its whole chain, so retaining
    /// it has to retain the chain. Expiring an intermediate snapshot would leave
    /// the retained one unrecoverable — reachability is a closure, not a suffix.
    #[tokio::test]
    async fn a_legacy_chain_keeps_the_snapshots_it_walks() {
        let (store, catalog) = harness();
        let mut snaps = Vec::new();
        for sequence in 1..=4u64 {
            let mut m = meta(sequence, Vec::new());
            m.base_sequence = 1;
            m.delta_chain_len = sequence - 1;
            m.format_version = 0;
            m.puffin_path = format!("graph/edges/puffin/dsu-{sequence}.puffin");
            snaps.push(m);
        }
        seed(&store, &catalog, &snaps).await;

        let got = collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();

        assert_eq!(
            got.snapshots_kept, 4,
            "snapshot 4 chains back to 1, so all four are load-bearing"
        );
        assert_eq!(got.snapshots_expired, 0);
        assert_eq!(objects(&store, "puffin").await.len(), 4);
    }

    /// The hazard that makes `grace` mandatory: a tick writes its objects and
    /// commits afterwards, so between those two steps its objects are reachable
    /// from nothing at all. Collecting them would make the commit that follows
    /// publish a snapshot naming files that no longer exist.
    #[tokio::test]
    async fn will_not_touch_objects_inside_the_grace_window() {
        let (store, catalog) = harness();
        seed(
            &store,
            &catalog,
            &[meta(1, vec![run("graph/edges/puffin/live.puffin", 1, 1)])],
        )
        .await;
        // An upload that has not been committed yet.
        store
            .put(
                &Path::from("graph/edges/puffin/in-flight.puffin"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();

        let guarded = RetentionPolicy {
            keep_snapshots: 1,
            keep_for: Duration::from_millis(0),
            grace: Duration::from_secs(3600),
        };
        // `now` is the epoch, so every object in this store is "in the future"
        // and therefore inside any grace window.
        let got = collect(&store, &catalog, &prefix(), &guarded, 0)
            .await
            .unwrap();
        assert_eq!(got.objects_deleted, 0);
        assert!(got.objects_too_young >= 1);
        assert!(
            objects(&store, "puffin")
                .await
                .contains(&"graph/edges/puffin/in-flight.puffin".to_string())
        );

        // With no grace, the same orphan is collected.
        let got = collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(got.objects_deleted, 1);
    }

    /// Sweeping twice must reclaim nothing the second time, and must not fail.
    /// A sweep that is not idempotent cannot be run on a timer.
    #[tokio::test]
    async fn a_second_sweep_is_a_no_op() {
        let (store, catalog) = harness();
        let snaps = vec![
            meta(1, vec![run("graph/edges/puffin/a.puffin", 1, 1)]),
            meta(2, vec![run("graph/edges/puffin/b.puffin", 1, 2)]),
        ];
        seed(&store, &catalog, &snaps).await;

        let first = collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert!(first.objects_deleted > 0);
        let second = collect(&store, &catalog, &prefix(), &policy(1), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(second.objects_deleted, 0);
        assert_eq!(second.snapshots_expired, 0);
        assert_eq!(second.snapshots_kept, 1);
    }

    /// The newest snapshot is what the cluster is serving. `keep_snapshots: 0`
    /// is a configuration error, not an instruction to delete everything.
    #[tokio::test]
    async fn never_expires_the_newest_snapshot() {
        let (store, catalog) = harness();
        seed(
            &store,
            &catalog,
            &[meta(1, vec![run("graph/edges/puffin/only.puffin", 1, 1)])],
        )
        .await;

        let got = collect(&store, &catalog, &prefix(), &policy(0), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(got.snapshots_kept, 1);
        assert_eq!(got.snapshots_expired, 0);
        assert_eq!(objects(&store, "puffin").await.len(), 1);
        assert!(catalog.latest().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn an_empty_catalog_is_not_an_error() {
        let (store, catalog) = harness();
        let got = collect(&store, &catalog, &prefix(), &policy(5), AFTER_EVERYTHING)
            .await
            .unwrap();
        assert_eq!(got, Reclaimed::default());
    }
}
