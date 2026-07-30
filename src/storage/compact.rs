//! **Storage-side compaction**: merge a committed layer chain into one base by
//! reading the mmap'd files, with no forest and no union lock involved.
//!
//! This is design 001's recommended variant, and the difference from the
//! in-memory path is the whole point. `ScopedForest::compact_and_fold` streams
//! the live forest under the union lock, so a compaction stalls ingest for as
//! long as it runs — 32 minutes at 2B links. Here the inputs are immutable
//! committed artifacts, so nothing is locked, nothing is stalled, and the job
//! can run wherever it is convenient: the leader's own tick, a follower, or a
//! separate compactor deployment.
//!
//! # The registry, and why it no longer needs an O(state) sort
//!
//! The `root -> scope` registry was the last thing here proportional to total
//! state: entries have to come out sorted by root *across* scopes, and the
//! obvious implementation collects them all and sorts — 41 GB of buffer at 2B
//! links, which OOMs long before anything else does.
//!
//! It is avoidable, because every layer's registry blob is **already sorted**.
//! The only obstacle to merging them directly is that a root recorded in an
//! older layer may since have been absorbed, and re-resolving it moves it
//! downward, which breaks the sort. But the roots that moved are exactly the
//! *shared keys of the newer layers* — bounded by change, not by state. So:
//!
//! 1. k-way merge the layers' registry runs, resolving each root. Entries that
//!    resolve to themselves are already in order; the rest are corrections.
//! 2. Collect only the corrections (small), sort them, and merge the two sorted
//!    streams.
//!
//! Two sequential passes over the registry runs, and heap proportional to the
//! corrections rather than to the registry.

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

use crate::core::{NodeId, RoutingBase, ScopeId};
use crate::storage::codec;
use crate::storage::layered::LayeredBase;
use crate::storage::puffin::Blob;

/// What a compaction produced, for logging and assertions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub shared_pairs: u64,
    pub overlay_pairs: u64,
    pub scopes: u64,
    pub registry_entries: u64,
    /// Registry entries whose root had been absorbed by a newer layer and so
    /// had to be re-sorted. Bounded by change; if this ever approaches
    /// `registry_entries` the bounded-corrections assumption has broken.
    pub registry_corrections: u64,
}

fn table(pairs: impl Iterator<Item = (NodeId, NodeId)>) -> (Bytes, u64) {
    let mut buf = BytesMut::new();
    buf.put_u64_le(0);
    let mut count = 0u64;
    for (k, v) in pairs {
        buf.put_u64_le(k);
        buf.put_u64_le(v);
        count += 1;
    }
    buf[..8].copy_from_slice(&count.to_le_bytes());
    (buf.freeze(), count)
}

/// Merge `layers` into the blob set for a single self-contained base.
///
/// Pure and synchronous: the only inputs are the mappings, so this is the piece
/// worth testing against the in-memory compactor for equality.
pub fn compact_layers(layers: &LayeredBase, sequence: u64) -> (Vec<Blob>, CompactionStats) {
    let seq = sequence as i64;
    let mut stats = CompactionStats::default();
    let blob = |blob_type: &str, data: Bytes, properties: BTreeMap<String, String>| Blob {
        blob_type: blob_type.into(),
        data,
        properties,
        snapshot_id: seq,
        sequence_number: seq,
    };

    let mut blobs = Vec::new();

    // Shared tier: the layered k-way merge already yields every key once, in
    // order, fully resolved.
    let mut shared = BytesMut::new();
    shared.put_u64_le(0);
    layers.for_each_shared_pair(&mut |k, v| {
        shared.put_u64_le(k);
        shared.put_u64_le(v);
        stats.shared_pairs += 1;
    });
    shared[..8].copy_from_slice(&stats.shared_pairs.to_le_bytes());
    blobs.push(blob(
        codec::GLOBAL_BLOB_TYPE,
        shared.freeze(),
        BTreeMap::new(),
    ));

    for scope in layers.scopes() {
        let mut pairs = Vec::new();
        layers.for_each_overlay_pair(scope, &mut |k, v| pairs.push((k, v)));
        if pairs.is_empty() {
            continue;
        }
        let (data, count) = table(pairs.into_iter());
        stats.overlay_pairs += count;
        stats.scopes += 1;
        blobs.push(blob(
            codec::SCOPE_BLOB_TYPE,
            data,
            BTreeMap::from([(codec::SCOPE_ID_PROP.into(), scope.to_string())]),
        ));
    }

    let (registry, reg_stats) = compact_registry(layers);
    stats.registry_entries = reg_stats.0;
    stats.registry_corrections = reg_stats.1;
    if stats.registry_entries > 0 {
        blobs.push(blob(codec::REGISTRY_BLOB_TYPE, registry, BTreeMap::new()));
    }

    (blobs, stats)
}

/// Ordered cursor over one layer's registry run.
struct Run {
    layer: usize,
    at: usize,
    len: usize,
}

/// Merge the layers' registry runs into one sorted, deduped, fully-resolved
/// blob. Returns `(entries, corrections)`.
fn compact_registry(layers: &LayeredBase) -> (Bytes, (u64, u64)) {
    // Pass 1: find the entries whose root moved. Only these break sort order,
    // and there are O(change) of them rather than O(state).
    let mut corrections: Vec<(NodeId, ScopeId)> = Vec::new();
    for_each_registry_entry(layers, &mut |root, scope| {
        let live = layers.shared_parent(root).unwrap_or(root);
        if live != root {
            corrections.push((live, scope));
        }
    });
    corrections.sort_unstable();
    corrections.dedup();
    let correction_count = corrections.len() as u64;

    // Pass 2: merge the in-order entries with the sorted corrections.
    let mut buf = BytesMut::new();
    buf.put_u64_le(0);
    let mut count = 0u64;
    let mut ci = 0usize;
    let mut last: Option<(NodeId, ScopeId)> = None;
    let mut push = |entry: (NodeId, ScopeId), buf: &mut BytesMut, count: &mut u64| {
        // Runs can carry the same (root, scope) in several layers, and a
        // correction can coincide with an in-order entry.
        if last == Some(entry) {
            return;
        }
        last = Some(entry);
        buf.put_u64_le(entry.0);
        buf.put_u32_le(entry.1);
        *count += 1;
    };
    for_each_registry_entry(layers, &mut |root, scope| {
        let live = layers.shared_parent(root).unwrap_or(root);
        if live != root {
            return; // handled as a correction
        }
        while ci < corrections.len() && corrections[ci] <= (live, scope) {
            push(corrections[ci], &mut buf, &mut count);
            ci += 1;
        }
        push((live, scope), &mut buf, &mut count);
    });
    while ci < corrections.len() {
        push(corrections[ci], &mut buf, &mut count);
        ci += 1;
    }

    buf[..8].copy_from_slice(&count.to_le_bytes());
    (buf.freeze(), (count, correction_count))
}

/// Visit every registry entry across all layers in ascending `(root, scope)`
/// order — a k-way merge over the per-layer runs, which are each already
/// sorted, so nothing is buffered.
fn for_each_registry_entry(layers: &LayeredBase, f: &mut dyn FnMut(NodeId, ScopeId)) {
    let mut runs: Vec<Run> = (0..layers.layers())
        .map(|layer| Run {
            layer,
            at: 0,
            len: layers.layer(layer).registry_len(),
        })
        .filter(|r| r.len > 0)
        .collect();
    loop {
        let mut pick: Option<(usize, (NodeId, ScopeId))> = None;
        for (idx, run) in runs.iter().enumerate() {
            if run.at >= run.len {
                continue;
            }
            let entry = layers.layer(run.layer).registry_at(run.at);
            if pick.is_none_or(|(_, best)| entry < best) {
                pick = Some((idx, entry));
            }
        }
        let Some((idx, (root, scope))) = pick else {
            return;
        };
        runs[idx].at += 1;
        f(root, scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EdgeEvent, GLOBAL_SCOPE, ScopedForest, Visibility};
    use crate::storage::{PuffinBase, puffin};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::sync::Arc;

    fn write_layer(dir: &std::path::Path, name: &str, blobs: &[Blob]) -> Arc<PuffinBase> {
        let bytes = puffin::write(blobs, BTreeMap::new());
        let path = dir.join(format!("{name}.puffin"));
        std::fs::write(&path, &bytes).unwrap();
        Arc::new(PuffinBase::open(&path).unwrap())
    }

    /// The test design 001 asks for: storage-side compaction must produce the
    /// same routing answers as compacting the live forest in memory. Built over
    /// randomized state with a real layer chain, because the interesting cases
    /// are roots absorbed several layers after the registry recorded them.
    #[test]
    fn storage_side_compaction_matches_the_in_memory_compactor() {
        const NODES: u64 = 500;
        const SCOPES: [u32; 4] = [1, 2, 3, 4];
        let dir = tempfile::tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(0xC0117AC7);

        // Build a chain the way the flusher does: a base, then deltas, each
        // folded out of the same forest.
        let forest = ScopedForest::new();
        let mut layers: Vec<Arc<PuffinBase>> = Vec::new();
        for round in 0..5u64 {
            for _ in 0..300 {
                let (u, v) = (rng.random_range(0..NODES), rng.random_range(0..NODES));
                let visibility = if rng.random_range(0..100) < 30 {
                    Visibility::Global
                } else {
                    Visibility::Scoped(smallvec::smallvec![
                        SCOPES[rng.random_range(0..SCOPES.len())]
                    ])
                };
                forest.apply(&EdgeEvent {
                    src: u,
                    dst: v,
                    visibility,
                    event_time_ms: 0,
                    props: None,
                });
            }
            let mut writer = codec::BlobWriter::new(round + 1);
            let name = format!("layer-{round}");
            let dir_path = dir.path().to_path_buf();
            let stack = layers.clone();
            forest
                .fold_delta(&mut writer, |w, _prev| {
                    let layer = write_layer(&dir_path, &name, &w.finish());
                    let mut next = stack;
                    next.push(layer);
                    let base: Arc<dyn RoutingBase> =
                        Arc::new(LayeredBase::from_layers(next.clone()).unwrap());
                    anyhow::Ok((base, next))
                })
                .map(|next| layers = next)
                .unwrap();
        }
        assert_eq!(layers.len(), 5, "one base plus four deltas");

        let stack = LayeredBase::from_layers(layers.clone()).unwrap();
        assert_eq!(stack.delta_count(), 4);

        // Storage-side: merge the mappings, no forest, no lock.
        let (blobs, stats) = compact_layers(&stack, 99);
        assert!(stats.shared_pairs > 0 && stats.overlay_pairs > 0);
        assert!(
            stats.registry_corrections < stats.registry_entries,
            "corrections must stay a fraction of the registry, got {}/{}",
            stats.registry_corrections,
            stats.registry_entries
        );
        let compacted = write_layer(dir.path(), "compacted", &blobs);
        let flat = LayeredBase::new(compacted);

        // In-memory: what the old path would have written.
        let reference =
            ScopedForest::with_base(Arc::new(LayeredBase::from_layers(layers.clone()).unwrap()));

        // Every answer must agree, in every scope, for the whole id space.
        let served = ScopedForest::with_base(Arc::new(flat));
        for node in 0..NODES {
            for scope in [GLOBAL_SCOPE, 1, 2, 3, 4, 77] {
                assert_eq!(
                    served.scope_root(scope, node),
                    reference.scope_root(scope, node),
                    "scope {scope} root({node}) diverged after storage-side compaction"
                );
            }
        }

        // ...including the registry, which is what makes a later merge of a
        // base-resident root still notify the right scopes. Compaction
        // *re-resolves* registry roots to live ones, so a stale root correctly
        // ends up with no entries — the invariant is not that entries stay put
        // but that no registration is lost: everything registered on `r` must
        // still be registered on whatever `r` now resolves to.
        let before = LayeredBase::from_layers(layers).unwrap();
        let after = LayeredBase::new(write_layer(dir.path(), "compacted2", &blobs));
        let mut checked = 0usize;
        for root in 0..NODES {
            let live = before.shared_parent(root).unwrap_or(root);
            let got = after.scopes_for_root(live);
            for scope in before.scopes_for_root(root) {
                assert!(
                    got.contains(&scope),
                    "scope {scope} was registered on root {root} (live {live}) \
                     and is missing after compaction"
                );
                checked += 1;
            }
        }
        assert!(
            checked > 100,
            "expected plenty of registrations, saw {checked}"
        );

        // And the compacted registry must not have invented registrations,
        // which a mis-merge could equally do.
        for root in 0..NODES {
            for scope in after.scopes_for_root(root) {
                let originally = (0..NODES).any(|r| {
                    before.shared_parent(r).unwrap_or(r) == root
                        && before.scopes_for_root(r).contains(&scope)
                });
                assert!(
                    originally,
                    "compaction invented a registration of scope {scope} on root {root}"
                );
            }
        }
    }

    /// A compacted base must be flat: one layer answering directly, with the
    /// chain collapsed rather than merely re-stated.
    #[test]
    fn compaction_collapses_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let forest = ScopedForest::new();
        for i in 0..50u64 {
            forest.apply(&EdgeEvent {
                src: i,
                dst: i + 1,
                visibility: Visibility::Global,
                event_time_ms: 0,
                props: None,
            });
        }
        let mut w = codec::BlobWriter::new(1);
        forest.compact_into(&mut w);
        let base = write_layer(dir.path(), "b", &w.finish());
        for n in 1..=50u64 {
            assert_eq!(base.shared_parent(n), Some(0), "chain roots at 0");
        }

        // A delta that re-roots the whole chain onto a new, lower root. Note
        // this has to go through `fold_delta`: `compact_into` emits the *full*
        // composed state, which is a replacement for the stack rather than a
        // layer on top of it, and stacking it would break the disjoint-keys
        // invariant the layered reader depends on.
        let forest2 = ScopedForest::with_base(Arc::new(LayeredBase::new(base.clone())));
        forest2.apply(&EdgeEvent {
            src: 0,
            dst: 0xFFFF_FFFF, // absorbed: 0 is lower, so 0 stays the root
            visibility: Visibility::Global,
            event_time_ms: 0,
            props: None,
        });
        let mut w2 = codec::BlobWriter::new(2);
        let d1 = forest2
            .fold_delta(&mut w2, |w, _prev| {
                let layer = write_layer(dir.path(), "d1", &w.finish());
                let base: Arc<dyn RoutingBase> =
                    Arc::new(LayeredBase::from_layers(vec![base.clone(), layer.clone()]).unwrap());
                anyhow::Ok((base, layer))
            })
            .unwrap();

        let stack = LayeredBase::from_layers(vec![base, d1]).unwrap();
        assert_eq!(stack.delta_count(), 1);
        let (blobs, stats) = compact_layers(&stack, 3);
        // 50 chain nodes plus the newly absorbed one, each stored once — the
        // point being that the delta's keys do not duplicate the base's.
        assert_eq!(stats.shared_pairs, 51);

        let flat = LayeredBase::new(write_layer(dir.path(), "c", &blobs));
        assert_eq!(
            flat.delta_count(),
            0,
            "the chain is collapsed, not restated"
        );
        for n in 1..=50u64 {
            assert_eq!(flat.shared_parent(n), Some(0));
        }
        assert_eq!(flat.shared_parent(0xFFFF_FFFF), Some(0));
        assert_eq!(flat.shared_parent(0), None);
    }
}
