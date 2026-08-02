//! Binary codec for DSU routing maps inside Puffin blobs.
//!
//! Blob payload (`blaze-*-dsu-v1`): `u64 LE` pair count, then `count` pairs
//! of `(u64 node, u64 root)` LE, fully resolved and sorted by node — so a
//! reader can memory-map and binary-search for O(1)-ish topological routing
//! without replaying history.

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

use crate::core::{ForestSnapshot, NodeId, ScopeId, ScopedForest, SnapshotSink};
use crate::storage::filter::BlockedFilter;
use crate::storage::puffin::Blob;
use crate::storage::registry::{RegistryEncoding, RegistryWriter};

/// How a base is encoded. Both knobs are read-compatible in one direction only:
/// a reader understands everything an older writer produced, so a cluster can
/// roll forward without a rewrite, and a base written by a newer worker stays
/// readable by an older one — filters and the registry both degrade to "probe
/// the table" rather than to an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// Bits of membership filter per key; 0 emits no filter blobs at all. See
    /// [`crate::storage::filter::clamp_filter_bits`].
    pub filter_bits: usize,
    /// Which `root -> scopes` encoding to emit. See
    /// [`crate::storage::registry`].
    pub registry: RegistryEncoding,
    /// Emit the parent-ordered member index, so a component can be listed from
    /// any node in it. Off by default: it is the one index whose cost falls on
    /// deployments that may never query it. See design 011.
    pub member_index: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            filter_bits: crate::storage::filter::DEFAULT_FILTER_BITS,
            registry: RegistryEncoding::default(),
            member_index: false,
        }
    }
}

pub const GLOBAL_BLOB_TYPE: &str = "blaze-global-dsu-v1";
pub const SCOPE_BLOB_TYPE: &str = "blaze-scope-dsu-v1";
pub const SCOPE_ID_PROP: &str = "scope-id";
/// `root -> scopes` index so a reader serving the routing map from disk can
/// answer "which tenants keyed overlay state on this shared root?" with one
/// binary search instead of probing every scope blob.
///
/// Two encodings exist and the blob type is the switch between them; see
/// [`crate::storage::registry`].
pub use crate::storage::registry::{
    BLOCKED_BLOB_TYPE as REGISTRY_V2_BLOB_TYPE, FLAT_BLOB_TYPE as REGISTRY_BLOB_TYPE,
};
/// Blocked membership filters over a layer's keys, so a layer that does not hold
/// a key can say so without a binary search. Old readers ignore these blob
/// types, and a layer written without them still resolves correctly — the
/// reader just falls back to searching. See `storage::filter`.
pub const SHARED_FILTER_BLOB_TYPE: &str = "blaze-shared-filter-v1";
pub const OVERLAY_FILTER_BLOB_TYPE: &str = "blaze-overlay-filter-v1";

/// The same pairs a routing table holds, re-sorted by **parent**, so the
/// children of a node are contiguous and a component can be listed by walking
/// down the parent forest. See `docs/design/011-member-index.md`.
///
/// Keyed on the parent rather than the root deliberately: a parent edge is
/// never rewritten once written, only superseded by a fixup further up the
/// chain, whereas the root of a component moves downward as smaller ids join.
/// An index keyed on the root would go stale; this one cannot.
pub const SHARED_MEMBERS_BLOB_TYPE: &str = "blaze-shared-members-v1";
pub const OVERLAY_MEMBERS_BLOB_TYPE: &str = "blaze-overlay-members-v1";

/// Re-sort an encoded `(node, parent)` table by parent, dropping self-edges.
///
/// Built by re-reading the payload already in hand, like the filters, so
/// nothing extra is buffered during the streaming write and the index cannot
/// drift from the table it describes.
///
/// **Self-edges are excluded and that is load-bearing.** A root's own entry
/// maps it to itself; kept, it would make the downward walk revisit the root
/// forever.
fn invert_table(data: &[u8]) -> Bytes {
    let mut pairs: Vec<(NodeId, NodeId)> = table_keys(data)
        .filter(|(node, parent)| node != parent)
        .map(|(node, parent)| (parent, node))
        .collect();
    pairs.sort_unstable();
    let mut out = BytesMut::with_capacity(8 + pairs.len() * 16);
    out.put_u64_le(pairs.len() as u64);
    for (parent, child) in pairs {
        out.put_u64_le(parent);
        out.put_u64_le(child);
    }
    out.freeze()
}

fn encode_pairs(pairs: &[(NodeId, NodeId)]) -> Bytes {
    let mut sorted: Vec<_> = pairs.to_vec();
    sorted.sort_unstable_by_key(|(n, _)| *n);
    let mut out = BytesMut::with_capacity(8 + sorted.len() * 16);
    out.put_u64_le(sorted.len() as u64);
    for (node, root) in sorted {
        out.put_u64_le(node);
        out.put_u64_le(root);
    }
    out.freeze()
}

fn decode_pairs(data: &[u8]) -> anyhow::Result<Vec<(NodeId, NodeId)>> {
    anyhow::ensure!(data.len() >= 8, "dsu blob truncated (no header)");
    let count = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    anyhow::ensure!(
        data.len() == 8 + count * 16,
        "dsu blob length mismatch: header says {count} pairs, payload has {} bytes",
        data.len() - 8
    );
    let mut pairs = Vec::with_capacity(count);
    for i in 0..count {
        let base = 8 + i * 16;
        let node = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
        let root = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
        pairs.push((node, root));
    }
    Ok(pairs)
}

/// `(root, scope)` pairs derived from a snapshot: every overlay member
/// resolved through the snapshot's shared map to its *live* shared root, so
/// future merges of that root notify the right scopes.
pub fn registry_from_snapshot(snap: &ForestSnapshot) -> Vec<(NodeId, ScopeId)> {
    let shared: std::collections::HashMap<NodeId, NodeId> = snap.global.iter().copied().collect();
    let mut out = Vec::new();
    for (scope, pairs) in &snap.scopes {
        for &(a, b) in pairs {
            for member in [a, b] {
                let live = shared.get(&member).copied().unwrap_or(member);
                out.push((live, *scope));
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Encode sorted, deduped entries in `encoding`, returning the blob type to
/// stamp on them. `None` when there is nothing to write.
fn encode_registry(
    entries: &[(NodeId, ScopeId)],
    encoding: RegistryEncoding,
) -> Option<(&'static str, Bytes)> {
    let mut w = RegistryWriter::new(encoding);
    for &(root, scope) in entries {
        w.push(root, scope);
    }
    w.finish()
}

/// Decode a flat (v1) registry blob payload into `(root, scope)` entries.
pub fn decode_registry(data: &[u8]) -> anyhow::Result<Vec<(NodeId, ScopeId)>> {
    anyhow::ensure!(data.len() >= 8, "registry blob truncated (no header)");
    let count = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    anyhow::ensure!(
        data.len() == 8 + count * 12,
        "registry blob length mismatch: header says {count} entries, payload has {} bytes",
        data.len() - 8
    );
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let base = 8 + i * 12;
        let root = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
        let scope = u32::from_le_bytes(data[base + 8..base + 12].try_into().unwrap());
        out.push((root, scope));
    }
    Ok(out)
}

/// Encode a forest snapshot as Puffin blobs: one global blob, one blob per
/// scope with overlay state, and the `root -> scopes` registry index.
pub fn snapshot_to_blobs(snap: &ForestSnapshot, sequence: u64, opts: WriteOptions) -> Vec<Blob> {
    let seq = sequence as i64;
    let mut blobs = Vec::with_capacity(1 + snap.scopes.len());
    blobs.push(Blob {
        blob_type: GLOBAL_BLOB_TYPE.into(),
        data: encode_pairs(&snap.global),
        properties: BTreeMap::new(),
        snapshot_id: seq,
        sequence_number: seq,
    });
    for (scope, pairs) in &snap.scopes {
        blobs.push(Blob {
            blob_type: SCOPE_BLOB_TYPE.into(),
            data: encode_pairs(pairs),
            properties: BTreeMap::from([(SCOPE_ID_PROP.into(), scope.to_string())]),
            snapshot_id: seq,
            sequence_number: seq,
        });
    }
    let registry = registry_from_snapshot(snap);
    if let Some((blob_type, data)) = encode_registry(&registry, opts.registry) {
        blobs.push(Blob {
            blob_type: blob_type.into(),
            data,
            properties: BTreeMap::new(),
            snapshot_id: seq,
            sequence_number: seq,
        });
    }
    // Same filters the streaming writer emits, so both paths produce the same
    // file and a snapshot-built layer is not slower to read than a folded one.
    let overlay_keys: usize = snap.scopes.iter().map(|(_, p)| p.len()).sum();
    for (blob_type, filter) in [
        (
            SHARED_FILTER_BLOB_TYPE,
            BlockedFilter::build_with(
                snap.global.iter().map(|(k, _)| *k),
                snap.global.len(),
                opts.filter_bits,
            ),
        ),
        (
            OVERLAY_FILTER_BLOB_TYPE,
            BlockedFilter::build_with(
                snap.scopes.iter().flat_map(|(s, pairs)| {
                    pairs
                        .iter()
                        .map(move |(k, _)| crate::storage::filter::overlay_key(*s, *k))
                }),
                overlay_keys,
                opts.filter_bits,
            ),
        ),
    ]
    .into_iter()
    .filter_map(|(t, f)| f.map(|f| (t, f)))
    {
        blobs.push(Blob {
            blob_type: blob_type.into(),
            data: filter.encode(),
            properties: BTreeMap::new(),
            snapshot_id: seq,
            sequence_number: seq,
        });
    }
    if opts.member_index {
        // Inverted from the payloads already encoded above, so both write paths
        // produce byte-identical member indexes for the same forest.
        let mut members: Vec<Blob> = Vec::with_capacity(1 + snap.scopes.len());
        for blob in &blobs {
            let (blob_type, properties) = match blob.blob_type.as_str() {
                GLOBAL_BLOB_TYPE => (SHARED_MEMBERS_BLOB_TYPE, BTreeMap::new()),
                SCOPE_BLOB_TYPE => (OVERLAY_MEMBERS_BLOB_TYPE, blob.properties.clone()),
                _ => continue,
            };
            members.push(Blob {
                blob_type: blob_type.into(),
                data: invert_table(&blob.data),
                properties,
                snapshot_id: seq,
                sequence_number: seq,
            });
        }
        blobs.append(&mut members);
    }
    blobs
}

/// Encodes a compacted forest straight into Puffin blob payloads as it is
/// streamed, so the flush path never materializes the state it is writing.
///
/// [`ScopedForest::compact_into`] emits keys in ascending order — exactly the
/// layout the blobs need — so each fixed-stride table is written once, in
/// place, with no intermediate `Vec` of pairs and no sort. The pair-count
/// header is reserved up front and patched in when the table closes.
///
/// The one term still proportional to state is the registry, which must be
/// sorted by root *across* scopes and so is buffered. It is 12 bytes per
/// overlay endpoint; spilling it to an external sort is design 001's job.
pub struct BlobWriter {
    sequence: i64,
    opts: WriteOptions,
    shared: BytesMut,
    shared_pairs: u64,
    /// The scope currently open, its payload, and its pair count.
    scope: Option<(ScopeId, BytesMut, u64)>,
    registry: Vec<(NodeId, ScopeId)>,
    scope_blobs: Vec<Blob>,
}

/// A table payload with its 8-byte count header reserved but not yet known.
fn open_table() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u64_le(0);
    buf
}

/// Walk a `(u64 node, u64 root)` payload. Used to derive filters from a table
/// that has already been encoded, rather than keeping a second copy of the keys.
pub(crate) fn table_keys(data: &[u8]) -> impl Iterator<Item = (NodeId, NodeId)> + '_ {
    let count = if data.len() >= 8 {
        u64::from_le_bytes(data[..8].try_into().unwrap()) as usize
    } else {
        0
    };
    (0..count).map(move |i| {
        let at = 8 + i * 16;
        (
            u64::from_le_bytes(data[at..at + 8].try_into().unwrap()),
            u64::from_le_bytes(data[at + 8..at + 16].try_into().unwrap()),
        )
    })
}

fn close_table(mut buf: BytesMut, count: u64) -> Bytes {
    buf[..8].copy_from_slice(&count.to_le_bytes());
    buf.freeze()
}

impl BlobWriter {
    pub fn new(sequence: u64, opts: WriteOptions) -> Self {
        Self {
            sequence: sequence as i64,
            opts,
            shared: open_table(),
            shared_pairs: 0,
            scope: None,
            registry: Vec::new(),
            scope_blobs: Vec::new(),
        }
    }

    fn blob(&self, blob_type: &str, data: Bytes, properties: BTreeMap<String, String>) -> Blob {
        Blob {
            blob_type: blob_type.into(),
            data,
            properties,
            snapshot_id: self.sequence,
            sequence_number: self.sequence,
        }
    }

    /// Blob order matches [`snapshot_to_blobs`]: shared tier, scopes
    /// ascending, then the registry index, then the membership filters.
    pub fn finish(&mut self) -> Vec<Blob> {
        let mut blobs = Vec::with_capacity(4 + self.scope_blobs.len());
        let shared = close_table(std::mem::take(&mut self.shared), self.shared_pairs);
        // Filters are built by re-reading the payloads already in hand, so
        // nothing extra is buffered and the key sets cannot drift from the
        // tables they describe.
        let shared_filter = BlockedFilter::build_with(
            table_keys(&shared).map(|(k, _)| k),
            self.shared_pairs as usize,
            self.opts.filter_bits,
        );
        blobs.push(self.blob(GLOBAL_BLOB_TYPE, shared, BTreeMap::new()));

        let overlay_keys: usize = self
            .scope_blobs
            .iter()
            .map(|b| table_keys(&b.data).count())
            .sum();
        let overlay_filter = BlockedFilter::build_with(
            self.scope_blobs.iter().flat_map(|b| {
                let scope: ScopeId = b
                    .properties
                    .get(SCOPE_ID_PROP)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                table_keys(&b.data).map(move |(k, _)| crate::storage::filter::overlay_key(scope, k))
            }),
            overlay_keys,
            self.opts.filter_bits,
        );
        blobs.append(&mut self.scope_blobs);

        self.registry.sort_unstable();
        self.registry.dedup();
        if let Some((blob_type, data)) = encode_registry(&self.registry, self.opts.registry) {
            blobs.push(self.blob(blob_type, data, BTreeMap::new()));
        }
        // No filter blobs at all when disabled: the reader treats their absence as
        // "probe the table directly", so a filterless base is readable by any
        // build, just slower on a miss.
        if let Some(f) = shared_filter {
            blobs.push(self.blob(SHARED_FILTER_BLOB_TYPE, f.encode(), BTreeMap::new()));
        }
        if let Some(f) = overlay_filter {
            blobs.push(self.blob(OVERLAY_FILTER_BLOB_TYPE, f.encode(), BTreeMap::new()));
        }
        if self.opts.member_index {
            // Derived from the encoded tables rather than from a second buffer,
            // so the index describes exactly what was written.
            let shared_members = {
                let table = blobs
                    .iter()
                    .find(|b| b.blob_type == GLOBAL_BLOB_TYPE)
                    .expect("shared table was pushed first");
                invert_table(&table.data)
            };
            blobs.push(self.blob(SHARED_MEMBERS_BLOB_TYPE, shared_members, BTreeMap::new()));
            let overlays: Vec<(ScopeId, Bytes)> = blobs
                .iter()
                .filter(|b| b.blob_type == SCOPE_BLOB_TYPE)
                .map(|b| {
                    let scope: ScopeId = b
                        .properties
                        .get(SCOPE_ID_PROP)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    (scope, invert_table(&b.data))
                })
                .collect();
            for (scope, data) in overlays {
                blobs.push(self.blob(
                    OVERLAY_MEMBERS_BLOB_TYPE,
                    data,
                    BTreeMap::from([(SCOPE_ID_PROP.to_string(), scope.to_string())]),
                ));
            }
        }
        blobs
    }
}

impl SnapshotSink for BlobWriter {
    fn shared_pair(&mut self, node: NodeId, root: NodeId) {
        self.shared.put_u64_le(node);
        self.shared.put_u64_le(root);
        self.shared_pairs += 1;
    }

    fn overlay_pair(&mut self, node: NodeId, root: NodeId) {
        if let Some((_, buf, count)) = &mut self.scope {
            buf.put_u64_le(node);
            buf.put_u64_le(root);
            *count += 1;
        }
    }

    fn scope_start(&mut self, scope: ScopeId) {
        self.scope = Some((scope, open_table(), 0));
    }

    fn scope_end(&mut self, _scope: ScopeId) {
        let Some((scope, buf, count)) = self.scope.take() else {
            return;
        };
        // A scope whose overlay resolved to nothing gets no blob at all.
        if count == 0 {
            return;
        }
        let data = close_table(buf, count);
        let props = BTreeMap::from([(SCOPE_ID_PROP.into(), scope.to_string())]);
        let blob = self.blob(SCOPE_BLOB_TYPE, data, props);
        self.scope_blobs.push(blob);
    }

    fn registry_entry(&mut self, root: NodeId, scope: ScopeId) {
        self.registry.push((root, scope));
    }
}

/// Compact `forest` directly into Puffin blobs. Byte-identical to
/// `snapshot_to_blobs(&forest.snapshot(), sequence)`, but without the
/// O(state) intermediate — this is the flush path.
pub fn compact_to_blobs(forest: &ScopedForest, sequence: u64, opts: WriteOptions) -> Vec<Blob> {
    let mut writer = BlobWriter::new(sequence, opts);
    forest.compact_into(&mut writer);
    writer.finish()
}

/// Decode Puffin blobs back into a forest snapshot. Unknown blob types are
/// ignored (forward compatibility).
pub fn blobs_to_snapshot(blobs: &[Blob]) -> anyhow::Result<ForestSnapshot> {
    let mut snap = ForestSnapshot {
        global: vec![],
        scopes: vec![],
    };
    for blob in blobs {
        match blob.blob_type.as_str() {
            GLOBAL_BLOB_TYPE => snap.global = decode_pairs(&blob.data)?,
            SCOPE_BLOB_TYPE => {
                let scope: ScopeId = blob
                    .properties
                    .get(SCOPE_ID_PROP)
                    .ok_or_else(|| anyhow::anyhow!("scope dsu blob missing {SCOPE_ID_PROP}"))?
                    .parse()?;
                snap.scopes.push((scope, decode_pairs(&blob.data)?));
            }
            _ => {}
        }
    }
    snap.scopes.sort_by_key(|(s, _)| *s);
    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::puffin;

    #[test]
    fn snapshot_blob_roundtrip() {
        let snap = ForestSnapshot {
            global: vec![(500, 105), (7, 105)],
            scopes: vec![(3, vec![(9, 1)]), (2999, vec![(4, 2), (8, 2)])],
        };
        let blobs = snapshot_to_blobs(&snap, 12, WriteOptions::default());
        let file = puffin::write(&blobs, BTreeMap::new());
        let parsed = puffin::read(&file).unwrap();
        let back = blobs_to_snapshot(&parsed).unwrap();

        let mut expected_global = snap.global.clone();
        expected_global.sort_unstable();
        let mut got_global = back.global.clone();
        got_global.sort_unstable();
        assert_eq!(got_global, expected_global);
        assert_eq!(back.scopes.len(), 2);
        assert_eq!(back.scopes[0].0, 3);
        assert_eq!(back.scopes[1].0, 2999);
        assert_eq!(back.scopes[1].1, vec![(4, 2), (8, 2)]);
    }

    /// The streaming flush path and the collecting snapshot path must be the
    /// same function. Anything else means a worker's committed base depends on
    /// which code path wrote it.
    #[test]
    fn streaming_and_collecting_paths_agree_byte_for_byte() {
        use crate::core::{EdgeEvent, Visibility};
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(0xC0DEC);
        let forest = ScopedForest::new();
        for _ in 0..2_000 {
            let visibility = if rng.random_range(0..100u8) < 25 {
                Visibility::Global
            } else {
                Visibility::Scoped(smallvec::smallvec![rng.random_range(1..=40u32)])
            };
            forest.apply(&EdgeEvent {
                src: rng.random_range(0..400u64),
                dst: rng.random_range(0..400u64),
                visibility,
                event_time_ms: 0,
                props: None,
            });
        }

        let streamed = compact_to_blobs(&forest, 9, WriteOptions::default());
        let collected = snapshot_to_blobs(&forest.snapshot(), 9, WriteOptions::default());
        let fields = |bs: &[Blob]| -> Vec<(String, BTreeMap<String, String>, Vec<u8>)> {
            bs.iter()
                .map(|b| (b.blob_type.clone(), b.properties.clone(), b.data.to_vec()))
                .collect()
        };
        assert!(streamed.len() > 20, "test should exercise many scope blobs");
        assert_eq!(fields(&streamed), fields(&collected));
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(decode_pairs(&[1, 2, 3]).is_err());
        let mut bad = encode_pairs(&[(1, 2)]).to_vec();
        bad.truncate(bad.len() - 1);
        assert!(decode_pairs(&bad).is_err());
    }
}

#[cfg(test)]
mod member_index_tests {
    use super::*;

    fn table(pairs: &[(NodeId, NodeId)]) -> Bytes {
        encode_pairs(pairs)
    }

    fn decode_inverse(data: &[u8]) -> Vec<(NodeId, NodeId)> {
        table_keys(data).collect()
    }

    /// The inverse is the same pairs with the columns swapped, sorted by parent
    /// so children are contiguous.
    #[test]
    fn inverts_and_groups_children_together() {
        // 2 and 3 both hang off 1; 4 hangs off 2. Plus self-edges for the roots.
        let t = table(&[(1, 1), (2, 1), (3, 1), (4, 2)]);
        let inv = decode_inverse(&invert_table(&t));
        assert_eq!(inv, vec![(1, 2), (1, 3), (2, 4)]);
    }

    /// Self-edges must not survive: a root maps to itself, and keeping that
    /// entry would make a downward walk revisit the root forever.
    #[test]
    fn self_edges_are_dropped() {
        let t = table(&[(7, 7), (9, 9)]);
        let inv = decode_inverse(&invert_table(&t));
        assert!(inv.is_empty(), "a self-edge would loop the walk: {inv:?}");
    }

    /// Both write paths must produce the same index, or a snapshot-built layer
    /// would answer differently from a folded one.
    #[test]
    fn the_two_write_paths_agree() {
        let opts = WriteOptions {
            member_index: true,
            ..Default::default()
        };
        let snap = ForestSnapshot {
            global: vec![(1, 1), (2, 1), (3, 1), (4, 2)],
            scopes: vec![(7, vec![(1, 1), (5, 1)])],
        };
        let from_snapshot = snapshot_to_blobs(&snap, 1, opts);

        let mut w = BlobWriter::new(1, opts);
        for &(n, r) in &snap.global {
            w.shared_pair(n, r);
        }
        for (scope, pairs) in &snap.scopes {
            w.scope_start(*scope);
            for &(n, r) in pairs {
                w.overlay_pair(n, r);
            }
            w.scope_end(*scope);
        }
        let from_stream = w.finish();

        for blob_type in [SHARED_MEMBERS_BLOB_TYPE, OVERLAY_MEMBERS_BLOB_TYPE] {
            let a: Vec<_> = from_snapshot
                .iter()
                .filter(|b| b.blob_type == blob_type)
                .map(|b| (&b.properties, decode_inverse(&b.data)))
                .collect();
            let b: Vec<_> = from_stream
                .iter()
                .filter(|b| b.blob_type == blob_type)
                .map(|b| (&b.properties, decode_inverse(&b.data)))
                .collect();
            assert_eq!(a, b, "{blob_type} differs between write paths");
            assert!(!a.is_empty(), "{blob_type} was not written at all");
        }
    }

    /// Off by default, and off means absent rather than empty — a reader tells
    /// "no index" from "an index with nothing in it".
    #[test]
    fn nothing_is_written_when_the_option_is_off() {
        let snap = ForestSnapshot {
            global: vec![(2, 1)],
            scopes: vec![],
        };
        let blobs = snapshot_to_blobs(&snap, 1, WriteOptions::default());
        assert!(
            !blobs.iter().any(|b| b.blob_type.contains("members")),
            "member index written despite being off"
        );
    }
}
