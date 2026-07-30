//! Binary codec for DSU routing maps inside Puffin blobs.
//!
//! Blob payload (`blaze-*-dsu-v1`): `u64 LE` pair count, then `count` pairs
//! of `(u64 node, u64 root)` LE, fully resolved and sorted by node — so a
//! reader can memory-map and binary-search for O(1)-ish topological routing
//! without replaying history.

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

use crate::core::{ForestSnapshot, NodeId, ScopeId, ScopedForest, SnapshotSink};
use crate::storage::puffin::Blob;

pub const GLOBAL_BLOB_TYPE: &str = "blaze-global-dsu-v1";
pub const SCOPE_BLOB_TYPE: &str = "blaze-scope-dsu-v1";
pub const SCOPE_ID_PROP: &str = "scope-id";
/// `root -> scopes` index so a reader serving the routing map from disk can
/// answer "which tenants keyed overlay state on this shared root?" with one
/// binary search instead of probing every scope blob.
///
/// Payload: `u64 LE` entry count, then `count` × (`u64 LE` root,
/// `u32 LE` scope), sorted by `(root, scope)` — fixed 12-byte stride.
pub const REGISTRY_BLOB_TYPE: &str = "blaze-registry-v1";

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

fn encode_registry(entries: &[(NodeId, ScopeId)]) -> Bytes {
    let mut out = BytesMut::with_capacity(8 + entries.len() * 12);
    out.put_u64_le(entries.len() as u64);
    for &(root, scope) in entries {
        out.put_u64_le(root);
        out.put_u32_le(scope);
    }
    out.freeze()
}

/// Decode a registry blob payload into `(root, scope)` entries.
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
pub fn snapshot_to_blobs(snap: &ForestSnapshot, sequence: u64) -> Vec<Blob> {
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
    if !registry.is_empty() {
        blobs.push(Blob {
            blob_type: REGISTRY_BLOB_TYPE.into(),
            data: encode_registry(&registry),
            properties: BTreeMap::new(),
            snapshot_id: seq,
            sequence_number: seq,
        });
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

fn close_table(mut buf: BytesMut, count: u64) -> Bytes {
    buf[..8].copy_from_slice(&count.to_le_bytes());
    buf.freeze()
}

impl BlobWriter {
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence: sequence as i64,
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
    /// ascending, then the registry index.
    pub fn finish(&mut self) -> Vec<Blob> {
        let mut blobs = Vec::with_capacity(2 + self.scope_blobs.len());
        let shared = close_table(std::mem::take(&mut self.shared), self.shared_pairs);
        blobs.push(self.blob(GLOBAL_BLOB_TYPE, shared, BTreeMap::new()));
        blobs.append(&mut self.scope_blobs);
        self.registry.sort_unstable();
        self.registry.dedup();
        if !self.registry.is_empty() {
            let data = encode_registry(&self.registry);
            blobs.push(self.blob(REGISTRY_BLOB_TYPE, data, BTreeMap::new()));
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
pub fn compact_to_blobs(forest: &ScopedForest, sequence: u64) -> Vec<Blob> {
    let mut writer = BlobWriter::new(sequence);
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
        let blobs = snapshot_to_blobs(&snap, 12);
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

        let streamed = compact_to_blobs(&forest, 9);
        let collected = snapshot_to_blobs(&forest.snapshot(), 9);
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
