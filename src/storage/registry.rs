//! Encodings for the `shared root -> scopes` registry.
//!
//! The registry answers "which tenants keyed overlay state on this shared
//! root?", so a global merge notifies only those scopes rather than
//! broadcasting to thousands. It is the largest single component of a base —
//! measured at 29-50% of bytes depending on scope fan-out — and the flat
//! `(u64 root, u32 scope)` stride it started as spends eight bytes on a root
//! whose gap from its predecessor is usually one.
//!
//! Two encodings live here behind [`RegistryEncoding`]. The blob type is the
//! switch: a Puffin blob is self-describing, so a base can carry either and a
//! reader that recognises neither falls back to rebuilding the mapping in
//! memory. Runs in a layer stack are read independently, so a stack may mix
//! encodings — which is what makes the format changeable without a rewrite.
//!
//! See `docs/design/009-registry-encoding.md`.

use crate::core::{NodeId, ScopeId};
use bytes::{BufMut, Bytes, BytesMut};

/// Default target block size.
///
/// A block is the unit a lookup decodes, so this is the same trade the sparse
/// index makes over the pair tables — larger blocks mean a smaller in-heap index
/// and a longer scan — except that the scan here decodes varints rather than
/// binary-searching a fixed stride, so it is linear and the trade bites harder.
/// Measured at 3.05 scopes per root: 4 KiB blocks cost 8.1 us per hit, 512 bytes
/// cost 1.2 us for 0.4 MB more heap per 14 MB of registry. See
/// `examples/registry_shape`.
pub const BLOCK_BYTES: usize = 512;

/// Which on-disk encoding a writer emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RegistryEncoding {
    /// `blaze-registry-v1`: fixed 12-byte `(u64 root, u32 scope)` stride.
    Flat,
    /// `blaze-registry-v2`: delta-varint records in offset-indexed blocks.
    #[default]
    Blocked,
}

impl std::fmt::Display for RegistryEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Flat => "flat",
            Self::Blocked => "blocked",
        })
    }
}

impl std::str::FromStr for RegistryEncoding {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "flat" | "v1" => Ok(Self::Flat),
            "blocked" | "varint" | "v2" => Ok(Self::Blocked),
            other => Err(format!("unknown registry encoding {other:?}")),
        }
    }
}

/// Append `v` as LEB128.
pub fn put_varint(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

/// Read a LEB128 value starting at `at`, returning it and the next offset.
/// Bounded by ten bytes so a corrupt payload cannot run away.
pub fn get_varint(data: &[u8], at: usize) -> anyhow::Result<(u64, usize)> {
    let (mut v, mut shift, mut i) = (0u64, 0u32, at);
    loop {
        anyhow::ensure!(i < data.len(), "varint runs past the end of the payload");
        let b = data[i];
        i += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok((v, i));
        }
        shift += 7;
        anyhow::ensure!(shift < 64, "varint longer than 64 bits");
    }
}

/// Streaming registry encoder. Entries are pushed in ascending `(root, scope)`
/// order — the order both the compactor's k-way merge and the snapshot writer
/// already produce — and exact duplicates are dropped.
///
/// The blocked encoding buffers one root's scope list at a time, so heap is
/// bounded by the widest fan-out rather than by the registry.
pub struct RegistryWriter {
    encoding: RegistryEncoding,
    entries: u64,

    /// Flat: the payload, built in place.
    flat: BytesMut,

    /// Blocked: the record currently being accumulated.
    root: Option<NodeId>,
    scopes: Vec<ScopeId>,
    /// Blocked: the block currently being filled, and its framing.
    body: BytesMut,
    records: u32,
    first_root: NodeId,
    prev_root: NodeId,
    /// Blocked: closed blocks and their start offsets within the payload.
    blocks: BytesMut,
    offsets: Vec<u64>,
    block_bytes: usize,
}

impl RegistryWriter {
    pub fn new(encoding: RegistryEncoding) -> Self {
        Self::with_block_size(encoding, BLOCK_BYTES)
    }

    /// A writer targeting a non-default block size. Only the writer needs to
    /// know it — a block's extent comes from the offsets in the payload — so
    /// this changes nothing for readers and runs written at different sizes
    /// coexist.
    pub fn with_block_size(encoding: RegistryEncoding, block_bytes: usize) -> Self {
        let mut flat = BytesMut::new();
        if encoding == RegistryEncoding::Flat {
            flat.put_u64_le(0); // count, patched in `finish`
        }
        Self {
            encoding,
            entries: 0,
            flat,
            root: None,
            scopes: Vec::new(),
            body: BytesMut::new(),
            records: 0,
            first_root: 0,
            prev_root: 0,
            blocks: BytesMut::new(),
            offsets: Vec::new(),
            block_bytes,
        }
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    pub fn push(&mut self, root: NodeId, scope: ScopeId) {
        match self.encoding {
            RegistryEncoding::Flat => {
                // The flat form has no grouping to dedup against, so callers
                // own that; a repeat here is written as a repeat.
                self.flat.put_u64_le(root);
                self.flat.put_u32_le(scope);
                self.entries += 1;
            }
            RegistryEncoding::Blocked => {
                if self.root == Some(root) {
                    if self.scopes.last() == Some(&scope) {
                        return;
                    }
                    self.scopes.push(scope);
                } else {
                    self.close_record();
                    self.root = Some(root);
                    self.scopes.clear();
                    self.scopes.push(scope);
                }
                self.entries += 1;
            }
        }
    }

    /// Encode the buffered root and append it to the open block, starting a new
    /// block when it would not fit. A record never spans blocks, so a lookup
    /// that lands on a block decodes whole records and a root's scope list is
    /// always contiguous.
    fn close_record(&mut self) {
        let Some(root) = self.root.take() else { return };

        let mut rec = BytesMut::new();
        put_varint(&mut rec, self.scopes.len() as u64);
        let mut prev = 0u64;
        for &scope in &self.scopes {
            put_varint(&mut rec, scope as u64 - prev);
            prev = scope as u64;
        }

        if self.records > 0 {
            let mut gap = BytesMut::new();
            put_varint(&mut gap, root - self.prev_root);
            if self.body.len() + gap.len() + rec.len() <= self.block_bytes {
                self.body.unsplit(gap);
                self.body.unsplit(rec);
                self.records += 1;
                self.prev_root = root;
                return;
            }
            self.close_block();
        }
        // First record of a block: its root is absolute in the block header, so
        // no gap is written. A record too large for one block gets its own
        // oversized block rather than being split.
        self.first_root = root;
        self.prev_root = root;
        self.body = rec;
        self.records = 1;
    }

    fn close_block(&mut self) {
        if self.records == 0 {
            return;
        }
        self.offsets.push(self.blocks.len() as u64);
        self.blocks.put_u64_le(self.first_root);
        self.blocks.put_u32_le(self.records);
        let body = std::mem::take(&mut self.body);
        self.blocks.unsplit(body);
        self.records = 0;
    }

    /// The payload, or `None` when nothing was pushed.
    ///
    /// Blocked layout:
    /// ```text
    /// u64 LE   entry count
    /// u64 LE   block count B
    /// u64 LE   x (B+1)   block offsets, relative to the first block
    /// blocks   each: u64 LE first root, u32 LE record count, then records
    /// record   [varint root gap]  varint k  varint scope gap x k
    /// ```
    /// The first record of a block omits the root gap — the block header
    /// carries its root absolutely, which is what makes a block decodable
    /// without its predecessors and gives the reader an O(1) first key.
    pub fn finish(mut self) -> Option<(&'static str, Bytes)> {
        if self.entries == 0 {
            return None;
        }
        match self.encoding {
            RegistryEncoding::Flat => {
                self.flat[..8].copy_from_slice(&self.entries.to_le_bytes());
                Some((FLAT_BLOB_TYPE, self.flat.freeze()))
            }
            RegistryEncoding::Blocked => {
                self.close_record();
                self.close_block();
                let blocks = self.blocks.len() as u64;
                let count = self.offsets.len() as u64;
                let mut out =
                    BytesMut::with_capacity(24 + (count as usize + 1) * 8 + blocks as usize);
                out.put_u64_le(self.entries);
                out.put_u64_le(count);
                for &o in &self.offsets {
                    out.put_u64_le(o);
                }
                out.put_u64_le(blocks);
                out.unsplit(self.blocks);
                Some((BLOCKED_BLOB_TYPE, out.freeze()))
            }
        }
    }
}

/// Payload: `u64 LE` entry count, then `count` × (`u64 LE` root, `u32 LE`
/// scope), sorted by `(root, scope)` — fixed 12-byte stride.
pub const FLAT_BLOB_TYPE: &str = "blaze-registry-v1";
/// Delta-varint records in offset-indexed blocks. See [`RegistryWriter::finish`].
pub const BLOCKED_BLOB_TYPE: &str = "blaze-registry-v2";

/// Reader over a [`RegistryEncoding::Blocked`] payload inside a mapping.
///
/// Blocks are variable-length — a root's scope list is never split, so a root
/// with a very wide fan-out gets an oversized block of its own — which is why
/// this carries its own offsets rather than reusing the fixed-stride sparse
/// index the pair tables use. Both offsets and first roots are hoisted to the
/// heap at open: 16 bytes per 4 KiB block, the same 0.4% the sparse index costs,
/// and it means narrowing to a block touches no page of the mapping.
#[derive(Debug)]
pub struct BlockedRegistry {
    /// Offset of the first block within the mapping.
    blocks_at: usize,
    count: usize,
    /// Byte offset of each block relative to `blocks_at`, with a trailing entry
    /// for the end of the last block.
    offsets: Vec<u64>,
    first_roots: Vec<NodeId>,
}

impl BlockedRegistry {
    pub fn parse(data: &[u8], range: std::ops::Range<usize>) -> anyhow::Result<Self> {
        let payload = data
            .get(range.clone())
            .ok_or_else(|| anyhow::anyhow!("registry blob range outside the mapping"))?;
        anyhow::ensure!(
            payload.len() >= 16,
            "blocked registry truncated (no header)"
        );
        let count = u64::from_le_bytes(payload[..8].try_into().unwrap()) as usize;
        let blocks = u64::from_le_bytes(payload[8..16].try_into().unwrap()) as usize;
        let table = 16 + (blocks + 1) * 8;
        anyhow::ensure!(
            payload.len() >= table,
            "blocked registry claims {blocks} blocks but has no room for their offsets"
        );
        let offsets: Vec<u64> = (0..=blocks)
            .map(|i| u64::from_le_bytes(payload[16 + i * 8..24 + i * 8].try_into().unwrap()))
            .collect();
        anyhow::ensure!(
            offsets[blocks] as usize == payload.len() - table,
            "blocked registry length mismatch: offsets end at {}, payload has {} block bytes",
            offsets[blocks],
            payload.len() - table
        );
        anyhow::ensure!(
            offsets.windows(2).all(|w| w[0] < w[1]),
            "blocked registry block offsets are not strictly increasing"
        );

        let blocks_at = range.start + table;
        let mut first_roots = Vec::with_capacity(blocks);
        for i in 0..blocks {
            let at = blocks_at + offsets[i] as usize;
            anyhow::ensure!(
                offsets[i + 1] - offsets[i] >= 12 && at + 12 <= data.len(),
                "blocked registry block {i} is too short for its header"
            );
            first_roots.push(u64::from_le_bytes(data[at..at + 8].try_into().unwrap()));
        }
        anyhow::ensure!(
            first_roots.windows(2).all(|w| w[0] < w[1]),
            "blocked registry first roots are not sorted"
        );
        Ok(Self {
            blocks_at,
            count,
            offsets,
            first_roots,
        })
    }

    /// Entries — `(root, scope)` pairs, not roots.
    pub fn count(&self) -> usize {
        self.count
    }

    pub fn heap_bytes(&self) -> u64 {
        ((self.offsets.len() + self.first_roots.len()) * 8) as u64
    }

    /// All scopes recorded for `root`.
    ///
    /// Binary search the in-heap first roots to pick the one block that can
    /// hold it, then decode records forward. Records are ordered, so the scan
    /// stops as soon as it passes `root` — the cost is half a block on a hit and
    /// less than that on a miss.
    ///
    /// A malformed block yields no scopes rather than a panic: the caller is
    /// `apply_global` deciding whom to notify, and losing a notification is
    /// recoverable where crashing the flush loop is not. `parse` has already
    /// checked the framing, so this is defence in depth.
    pub fn scopes_for(&self, data: &[u8], root: NodeId) -> crate::core::base::ScopeList {
        let after = self.first_roots.partition_point(|&r| r <= root);
        if after == 0 {
            return Default::default();
        }
        self.decode_block(data, after - 1, root).unwrap_or_default()
    }

    fn decode_block(
        &self,
        data: &[u8],
        block: usize,
        want: NodeId,
    ) -> anyhow::Result<crate::core::base::ScopeList> {
        let at = self.blocks_at + self.offsets[block] as usize;
        let end = self.blocks_at + self.offsets[block + 1] as usize;
        let records = u32::from_le_bytes(data[at + 8..at + 12].try_into().unwrap());
        let mut cur = self.first_roots[block];
        let mut i = at + 12;
        for rec in 0..records {
            if rec > 0 {
                let (gap, next) = get_varint(data, i)?;
                cur += gap;
                i = next;
            }
            let (k, next) = get_varint(data, i)?;
            i = next;
            if cur > want {
                return Ok(Default::default());
            }
            let mut scope = 0u64;
            let hit = cur == want;
            let mut out = crate::core::base::ScopeList::new();
            for _ in 0..k {
                let (gap, next) = get_varint(data, i)?;
                i = next;
                scope += gap;
                if hit {
                    out.push(scope as ScopeId);
                }
            }
            if hit {
                return Ok(out);
            }
            anyhow::ensure!(i <= end, "blocked registry record ran past its block");
        }
        Ok(Default::default())
    }

    pub fn iter<'a>(&'a self, data: &'a [u8]) -> BlockedIter<'a> {
        BlockedIter {
            data,
            reg: self,
            block: 0,
            at: 0,
            records: 0,
            root: 0,
            first: true,
            scopes: 0,
            scope: 0,
        }
    }
}

/// Every `(root, scope)` in ascending order — the shape the compactor's k-way
/// merge needs, decoded block by block with nothing buffered.
pub struct BlockedIter<'a> {
    data: &'a [u8],
    reg: &'a BlockedRegistry,
    block: usize,
    at: usize,
    records: u32,
    root: NodeId,
    first: bool,
    scopes: u64,
    scope: u64,
}

impl Iterator for BlockedIter<'_> {
    type Item = (NodeId, ScopeId);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.scopes > 0 {
                let (gap, next) = get_varint(self.data, self.at).ok()?;
                self.at = next;
                self.scope += gap;
                self.scopes -= 1;
                return Some((self.root, self.scope as ScopeId));
            }
            if self.records == 0 {
                if self.block >= self.reg.first_roots.len() {
                    return None;
                }
                let at = self.reg.blocks_at + self.reg.offsets[self.block] as usize;
                self.root = self.reg.first_roots[self.block];
                self.records = u32::from_le_bytes(self.data[at + 8..at + 12].try_into().ok()?);
                self.at = at + 12;
                self.first = true;
                self.block += 1;
                continue;
            }
            if !self.first {
                let (gap, next) = get_varint(self.data, self.at).ok()?;
                self.at = next;
                self.root += gap;
            }
            self.first = false;
            let (k, next) = get_varint(self.data, self.at).ok()?;
            self.at = next;
            self.scopes = k;
            self.scope = 0;
            self.records -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wide enough to need a block to itself, and outside the generated
    /// sequence so it does not collide with one of its roots.
    const WIDE_ROOT: NodeId = 100_001;

    fn entries() -> Vec<(NodeId, ScopeId)> {
        // Dense roots with small gaps, which is what the measurements found,
        // plus one root wide enough to force an oversized block.
        let mut out = Vec::new();
        for root in (0..40_000u64).map(|i| i * 3 + 7) {
            let k = (root % 5) + 1;
            for j in 0..k {
                out.push((root, (1 + j * 137) as ScopeId));
            }
        }
        for scope in 1..3_000u32 {
            out.push((WIDE_ROOT, scope));
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    #[test]
    fn blocked_is_smaller_than_flat_on_dense_roots() {
        let e = entries();
        let mut flat = RegistryWriter::new(RegistryEncoding::Flat);
        let mut blocked = RegistryWriter::new(RegistryEncoding::Blocked);
        for &(r, s) in &e {
            flat.push(r, s);
            blocked.push(r, s);
        }
        let (ft, f) = flat.finish().unwrap();
        let (bt, b) = blocked.finish().unwrap();
        assert_eq!(ft, FLAT_BLOB_TYPE);
        assert_eq!(bt, BLOCKED_BLOB_TYPE);
        assert!(
            b.len() * 2 < f.len(),
            "blocked {} flat {}",
            b.len(),
            f.len()
        );
    }

    #[test]
    fn varints_round_trip_at_the_boundaries() {
        let mut buf = BytesMut::new();
        let cases = [0u64, 1, 127, 128, 16_383, 16_384, u64::MAX];
        for &v in &cases {
            put_varint(&mut buf, v);
        }
        let mut at = 0;
        for &v in &cases {
            let (got, next) = get_varint(&buf, at).unwrap();
            assert_eq!(got, v);
            at = next;
        }
        assert_eq!(at, buf.len());
        assert!(get_varint(&buf, buf.len()).is_err());
    }

    fn blocked(entries: &[(NodeId, ScopeId)]) -> (Bytes, BlockedRegistry) {
        let mut w = RegistryWriter::new(RegistryEncoding::Blocked);
        for &(r, s) in entries {
            w.push(r, s);
        }
        let (_, data) = w.finish().unwrap();
        let reg = BlockedRegistry::parse(&data, 0..data.len()).unwrap();
        (data, reg)
    }

    #[test]
    fn blocked_round_trips_in_order() {
        let e = entries();
        let (data, reg) = blocked(&e);
        assert_eq!(reg.count(), e.len());
        assert_eq!(reg.iter(&data).collect::<Vec<_>>(), e);
    }

    /// The property that matters: for *every* root, present or absent, the
    /// blocked reader must answer exactly what a linear scan of the flat entries
    /// answers. Asserted against the entries themselves rather than a fixture,
    /// so block boundaries are covered by construction — the set spans hundreds
    /// of blocks and includes a root wide enough to need one to itself.
    #[test]
    fn blocked_lookups_match_the_flat_entries() {
        let e = entries();
        let (data, reg) = blocked(&e);
        let mut expected: std::collections::BTreeMap<NodeId, Vec<ScopeId>> = Default::default();
        for &(r, s) in &e {
            expected.entry(r).or_default().push(s);
        }
        // Sampled rather than exhaustive: a lookup decodes half a block, so
        // probing all 40k roots in a debug build is minutes of varints for no
        // extra coverage. Every seventh root plus both neighbours still lands in
        // every block, and the boundary cases have tests of their own below.
        for (&root, scopes) in expected.iter().step_by(7) {
            assert_eq!(reg.scopes_for(&data, root).as_slice(), scopes.as_slice());
            for probe in [root - 1, root + 1] {
                let want = expected.get(&probe).cloned().unwrap_or_default();
                assert_eq!(reg.scopes_for(&data, probe).as_slice(), want.as_slice());
            }
        }
        // Before the first root and past the last.
        assert!(reg.scopes_for(&data, 0).is_empty());
        assert!(reg.scopes_for(&data, u64::MAX).is_empty());
    }

    /// Block edges are where this breaks. A block's first root comes from the
    /// block header rather than from a decoded delta, and its last root is the
    /// one a scan runs off the end looking for.
    #[test]
    fn blocked_finds_the_roots_at_both_edges_of_every_block() {
        let e = entries();
        let (data, reg) = blocked(&e);
        assert!(reg.first_roots.len() > 10, "test needs many blocks");
        let mut expected: std::collections::BTreeMap<NodeId, Vec<ScopeId>> = Default::default();
        for &(r, s) in &e {
            expected.entry(r).or_default().push(s);
        }
        for (b, &first) in reg.first_roots.iter().enumerate() {
            let last = match reg.first_roots.get(b + 1) {
                Some(&next) => *expected.range(..next).next_back().unwrap().0,
                None => *expected.keys().next_back().unwrap(),
            };
            for edge in [first, last] {
                assert_eq!(
                    reg.scopes_for(&data, edge).as_slice(),
                    expected[&edge].as_slice(),
                    "block {b} edge root {edge}"
                );
            }
        }
    }

    /// The widest root gets a block to itself, because a record is never split.
    /// It is also the only root whose scope list exceeds one block, so it is the
    /// case a fixed-stride block layout would have got wrong.
    #[test]
    fn a_root_wider_than_a_block_gets_its_own() {
        let e = entries();
        let (data, reg) = blocked(&e);
        let want: Vec<ScopeId> = e
            .iter()
            .filter(|(r, _)| *r == WIDE_ROOT)
            .map(|(_, s)| *s)
            .collect();
        assert_eq!(want.len(), 2_999);
        assert_eq!(reg.scopes_for(&data, WIDE_ROOT).as_slice(), want.as_slice());
        let b = reg
            .first_roots
            .iter()
            .position(|&r| r == WIDE_ROOT)
            .unwrap();
        assert!(
            reg.offsets[b + 1] - reg.offsets[b] > BLOCK_BYTES as u64,
            "the wide root should have forced an oversized block"
        );
    }

    #[test]
    fn blocked_rejects_a_corrupt_header() {
        let (data, _) = blocked(&entries());
        assert!(BlockedRegistry::parse(&data, 0..8).is_err());
        let mut bad = data.to_vec();
        bad[8] = 0xff; // block count no longer matches the offsets that follow
        assert!(BlockedRegistry::parse(&bad, 0..bad.len()).is_err());
    }

    #[test]
    fn empty_writers_emit_no_blob() {
        assert!(
            RegistryWriter::new(RegistryEncoding::Flat)
                .finish()
                .is_none()
        );
        assert!(
            RegistryWriter::new(RegistryEncoding::Blocked)
                .finish()
                .is_none()
        );
    }
}
