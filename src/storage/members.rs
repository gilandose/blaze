//! Encodings for the parent-ordered **member index** (design 011).
//!
//! The index is the routing table's pairs re-sorted by parent, so the children
//! of a node are contiguous and a component can be listed by walking down the
//! parent forest. It shipped as a fixed 16-byte `(u64 parent, u64 child)`
//! stride, which measured **+77% of run bytes** — the single reason the flag is
//! off by default.
//!
//! That stride is wasteful in exactly the way the registry's was before
//! [009](../../docs/design/009-registry-encoding.md): a repeated key with an
//! ascending value list, where the key's gap from its predecessor is usually
//! small and the count is usually one. So this applies the same treatment —
//! delta-varint records in offset-indexed blocks — and the layout below is
//! deliberately the registry's, record for record.
//!
//! # Why this is a second implementation rather than a shared one
//!
//! `storage::registry` encodes `(u64 root, u32 scope)`; this encodes
//! `(u64 parent, u64 child)`. The writers would generalise cleanly over the
//! value type, but the readers would not: `BlockedRegistry::scopes_for` returns
//! a `SmallVec<[u32; 4]>` and is on `apply_global`'s hot path, while this
//! returns u64 node ids appended to a caller's buffer and is on the query path.
//! Making one generic reader serve both means a trait object or a monomorphised
//! callback in the middle of two different hot loops, to save ~120 lines of
//! straight-line varint code. The duplication is the cheaper trade, and it is
//! recorded here so the next reader knows it was a decision.
//!
//! # What it costs, and what it buys
//!
//! The value gaps here are worse than the registry's. Scopes are u32 and dense,
//! so their gaps are tiny; children are node ids scattered across the whole id
//! space, so a child gap is 4-5 varint bytes rather than 1. The win comes from
//! the *key* side — one parent gap per record instead of eight bytes per pair —
//! and from the count, so it scales with how many parents have several children.
//! See `examples/registry_shape` for the measured figure on a real base.

use bytes::{BufMut, Bytes, BytesMut};

use crate::core::NodeId;
use crate::storage::registry::{BLOCK_BYTES, get_varint, put_varint};

/// Which on-disk encoding a writer emits for the member index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MemberEncoding {
    /// `blaze-*-members-v1`: fixed 16-byte `(u64 parent, u64 child)` stride.
    Flat,
    /// `blaze-*-members-v2`: delta-varint records in offset-indexed blocks.
    #[default]
    Blocked,
}

impl std::fmt::Display for MemberEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Flat => "flat",
            Self::Blocked => "blocked",
        })
    }
}

impl std::str::FromStr for MemberEncoding {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "flat" | "v1" => Ok(Self::Flat),
            "blocked" | "varint" | "v2" => Ok(Self::Blocked),
            other => Err(format!("unknown member index encoding {other:?}")),
        }
    }
}

/// Which tier a member blob describes. Only affects the blob type it is written
/// under; the payload layout is identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Shared,
    Overlay,
}

pub const SHARED_FLAT_BLOB_TYPE: &str = "blaze-shared-members-v1";
pub const SHARED_BLOCKED_BLOB_TYPE: &str = "blaze-shared-members-v2";
pub const OVERLAY_FLAT_BLOB_TYPE: &str = "blaze-overlay-members-v1";
pub const OVERLAY_BLOCKED_BLOB_TYPE: &str = "blaze-overlay-members-v2";

/// Builds one member-index payload from `(parent, child)` pairs pushed in
/// ascending order.
///
/// Callers push in order because the source is an already-sorted inverted
/// table; pushing out of order produces a payload whose blocks are unsearchable
/// rather than an error, which `debug_assert`s catch in tests.
pub struct MemberWriter {
    encoding: MemberEncoding,
    tier: Tier,
    pairs: u64,

    /// Flat: the payload, built in place.
    flat: BytesMut,

    /// Blocked: the record currently being accumulated.
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    /// Blocked: the block currently being filled, and its framing.
    body: BytesMut,
    records: u32,
    first_parent: NodeId,
    prev_parent: NodeId,
    /// Blocked: closed blocks and their start offsets within the payload.
    blocks: BytesMut,
    offsets: Vec<u64>,
    block_bytes: usize,
}

impl MemberWriter {
    pub fn new(encoding: MemberEncoding, tier: Tier) -> Self {
        Self::with_block_size(encoding, tier, BLOCK_BYTES)
    }

    pub fn with_block_size(encoding: MemberEncoding, tier: Tier, block_bytes: usize) -> Self {
        let mut flat = BytesMut::new();
        if encoding == MemberEncoding::Flat {
            flat.put_u64_le(0); // count, patched in `finish`
        }
        Self {
            encoding,
            tier,
            pairs: 0,
            flat,
            parent: None,
            children: Vec::new(),
            body: BytesMut::new(),
            records: 0,
            first_parent: 0,
            prev_parent: 0,
            blocks: BytesMut::new(),
            offsets: Vec::new(),
            block_bytes,
        }
    }

    pub fn pairs(&self) -> u64 {
        self.pairs
    }

    pub fn push(&mut self, parent: NodeId, child: NodeId) {
        debug_assert!(
            parent != child,
            "a self-edge would make the downward walk revisit the root forever"
        );
        match self.encoding {
            MemberEncoding::Flat => {
                self.flat.put_u64_le(parent);
                self.flat.put_u64_le(child);
                self.pairs += 1;
            }
            MemberEncoding::Blocked => {
                if self.parent == Some(parent) {
                    debug_assert!(
                        self.children.last().is_none_or(|&c| c < child),
                        "children must arrive ascending within a parent"
                    );
                    self.children.push(child);
                } else {
                    debug_assert!(
                        self.parent.is_none_or(|p| p < parent),
                        "parents must arrive ascending"
                    );
                    self.close_record();
                    self.parent = Some(parent);
                    self.children.clear();
                    self.children.push(child);
                }
                self.pairs += 1;
            }
        }
    }

    /// Encode the buffered parent and append it to the open block, starting a
    /// new block when it would not fit.
    ///
    /// **A record never spans blocks**, so a lookup that lands on a block finds
    /// the parent's whole child list there — which is what lets `children_of`
    /// decode exactly one block. A parent with a very wide fan-out therefore
    /// gets an oversized block of its own rather than being split.
    fn close_record(&mut self) {
        let Some(parent) = self.parent.take() else {
            return;
        };

        let mut rec = BytesMut::new();
        put_varint(&mut rec, self.children.len() as u64);
        let mut prev = 0u64;
        for &child in &self.children {
            put_varint(&mut rec, child - prev);
            prev = child;
        }

        if self.records > 0 {
            let mut gap = BytesMut::new();
            put_varint(&mut gap, parent - self.prev_parent);
            if self.body.len() + gap.len() + rec.len() <= self.block_bytes {
                self.body.unsplit(gap);
                self.body.unsplit(rec);
                self.records += 1;
                self.prev_parent = parent;
                return;
            }
            self.close_block();
        }
        // First record of a block: its parent is absolute in the block header,
        // so no gap is written.
        self.first_parent = parent;
        self.prev_parent = parent;
        self.body = rec;
        self.records = 1;
    }

    fn close_block(&mut self) {
        if self.records == 0 {
            return;
        }
        self.offsets.push(self.blocks.len() as u64);
        self.blocks.put_u64_le(self.first_parent);
        self.blocks.put_u32_le(self.records);
        let body = std::mem::take(&mut self.body);
        self.blocks.unsplit(body);
        self.records = 0;
    }

    /// The blob type and payload, or `None` when nothing was pushed.
    ///
    /// Blocked layout — the registry's, with u64 values:
    /// ```text
    /// u64 LE   pair count
    /// u64 LE   block count B
    /// u64 LE   x (B+1)   block offsets, relative to the first block
    /// blocks   each: u64 LE first parent, u32 LE record count, then records
    /// record   [varint parent gap]  varint k  varint child gap x k
    /// ```
    /// Child gaps run from zero, so the first child of a record is absolute.
    /// The first record of a block omits the parent gap — the block header
    /// carries it absolutely, which is what makes a block decodable on its own
    /// and gives the reader an O(1) first key to binary-search.
    pub fn finish(mut self) -> Option<(&'static str, Bytes)> {
        if self.pairs == 0 {
            return None;
        }
        let (flat_type, blocked_type) = match self.tier {
            Tier::Shared => (SHARED_FLAT_BLOB_TYPE, SHARED_BLOCKED_BLOB_TYPE),
            Tier::Overlay => (OVERLAY_FLAT_BLOB_TYPE, OVERLAY_BLOCKED_BLOB_TYPE),
        };
        match self.encoding {
            MemberEncoding::Flat => {
                self.flat[..8].copy_from_slice(&self.pairs.to_le_bytes());
                Some((flat_type, self.flat.freeze()))
            }
            MemberEncoding::Blocked => {
                self.close_record();
                self.close_block();
                let blocks = self.blocks.len() as u64;
                let count = self.offsets.len() as u64;
                let mut out =
                    BytesMut::with_capacity(24 + (count as usize + 1) * 8 + blocks as usize);
                out.put_u64_le(self.pairs);
                out.put_u64_le(count);
                for &o in &self.offsets {
                    out.put_u64_le(o);
                }
                out.put_u64_le(blocks);
                out.unsplit(self.blocks);
                Some((blocked_type, out.freeze()))
            }
        }
    }
}

/// Reader over a [`MemberEncoding::Blocked`] payload inside a mapping.
///
/// Blocks are variable-length, so this carries its own offsets rather than
/// reusing the fixed-stride sparse index — and unlike the forward tables, that
/// index is actually *usable* here: block boundaries fall between records, so a
/// parent's children never straddle two blocks. The flat member table has the
/// opposite problem (duplicate keys, so a run of equal keys can start in an
/// earlier block) and has to search the whole table. Narrowing is the second win
/// of this encoding, after the bytes.
#[derive(Debug)]
pub struct BlockedMembers {
    /// Offset of the first block within the mapping.
    blocks_at: usize,
    pairs: usize,
    /// Byte offset of each block relative to `blocks_at`, with a trailing entry
    /// for the end of the last block.
    offsets: Vec<u64>,
    first_parents: Vec<NodeId>,
}

impl BlockedMembers {
    pub fn parse(data: &[u8], range: std::ops::Range<usize>) -> anyhow::Result<Self> {
        let payload = data
            .get(range.clone())
            .ok_or_else(|| anyhow::anyhow!("member blob range outside the mapping"))?;
        anyhow::ensure!(payload.len() >= 16, "blocked member index truncated");
        let pairs = u64::from_le_bytes(payload[..8].try_into().unwrap()) as usize;
        let blocks = u64::from_le_bytes(payload[8..16].try_into().unwrap()) as usize;
        let table = 16 + (blocks + 1) * 8;
        anyhow::ensure!(
            payload.len() >= table,
            "blocked member index claims {blocks} blocks but has no room for their offsets"
        );
        let offsets: Vec<u64> = (0..=blocks)
            .map(|i| u64::from_le_bytes(payload[16 + i * 8..24 + i * 8].try_into().unwrap()))
            .collect();
        anyhow::ensure!(
            offsets[blocks] as usize == payload.len() - table,
            "blocked member index length mismatch: offsets end at {}, payload has {} block bytes",
            offsets[blocks],
            payload.len() - table
        );
        anyhow::ensure!(
            offsets.windows(2).all(|w| w[0] < w[1]),
            "blocked member index block offsets are not strictly increasing"
        );

        let blocks_at = range.start + table;
        let mut first_parents = Vec::with_capacity(blocks);
        for i in 0..blocks {
            let at = blocks_at + offsets[i] as usize;
            anyhow::ensure!(
                offsets[i + 1] - offsets[i] >= 12 && at + 12 <= data.len(),
                "blocked member index block {i} is too short for its header"
            );
            first_parents.push(u64::from_le_bytes(data[at..at + 8].try_into().unwrap()));
        }
        anyhow::ensure!(
            first_parents.windows(2).all(|w| w[0] < w[1]),
            "blocked member index first parents are not sorted"
        );
        Ok(Self {
            blocks_at,
            pairs,
            offsets,
            first_parents,
        })
    }

    pub fn pairs(&self) -> usize {
        self.pairs
    }

    pub fn heap_bytes(&self) -> u64 {
        ((self.offsets.len() + self.first_parents.len()) * 8) as u64
    }

    /// Append the children recorded for `parent`.
    ///
    /// A malformed block appends nothing rather than panicking. The caller is a
    /// membership walk, and under-reporting a component is recoverable where
    /// crashing a query thread is not; `parse` has already checked the framing,
    /// so this is defence in depth.
    pub fn children_of(&self, data: &[u8], parent: NodeId, limit: usize, out: &mut Vec<NodeId>) {
        if limit == 0 {
            return;
        }
        let after = self.first_parents.partition_point(|&p| p <= parent);
        if after == 0 {
            return;
        }
        let _ = self.decode_block(data, after - 1, parent, limit, out);
    }

    fn decode_block(
        &self,
        data: &[u8],
        block: usize,
        want: NodeId,
        limit: usize,
        out: &mut Vec<NodeId>,
    ) -> anyhow::Result<()> {
        let at = self.blocks_at + self.offsets[block] as usize;
        let end = self.blocks_at + self.offsets[block + 1] as usize;
        let records = u32::from_le_bytes(data[at + 8..at + 12].try_into().unwrap());
        let mut cur = self.first_parents[block];
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
                return Ok(());
            }
            let hit = cur == want;
            let mut child = 0u64;
            let mut taken = 0usize;
            for _ in 0..k {
                // Stop *decoding*, not just stop pushing. Decoding the whole
                // record and truncating afterwards is what a first version did,
                // and on a hub root that is half a million varints thrown away —
                // measured slower than the fixed-stride table it replaced.
                if hit && taken >= limit {
                    return Ok(());
                }
                let (gap, next) = get_varint(data, i)?;
                i = next;
                child += gap;
                if hit {
                    out.push(child);
                    taken += 1;
                }
            }
            if hit {
                return Ok(());
            }
            anyhow::ensure!(i <= end, "blocked member record ran past its block");
        }
        Ok(())
    }

    /// Every `(parent, child)` pair in order — the read compaction needs when it
    /// rewrites the index rather than deriving it.
    pub fn for_each(&self, data: &[u8], f: &mut dyn FnMut(NodeId, NodeId)) {
        for block in 0..self.first_parents.len() {
            let at = self.blocks_at + self.offsets[block] as usize;
            let Some(records) = data
                .get(at + 8..at + 12)
                .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
            else {
                return;
            };
            let mut cur = self.first_parents[block];
            let mut i = at + 12;
            for rec in 0..records {
                if rec > 0 {
                    let Ok((gap, next)) = get_varint(data, i) else {
                        return;
                    };
                    cur += gap;
                    i = next;
                }
                let Ok((k, next)) = get_varint(data, i) else {
                    return;
                };
                i = next;
                let mut child = 0u64;
                for _ in 0..k {
                    let Ok((gap, next)) = get_varint(data, i) else {
                        return;
                    };
                    i = next;
                    child += gap;
                    f(cur, child);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(pairs: &[(NodeId, NodeId)], encoding: MemberEncoding) -> (&'static str, Bytes) {
        let mut w = MemberWriter::new(encoding, Tier::Shared);
        for &(p, c) in pairs {
            w.push(p, c);
        }
        w.finish().expect("pairs were pushed")
    }

    fn read_back(pairs: &[(NodeId, NodeId)]) -> Vec<(NodeId, NodeId)> {
        let (blob_type, data) = write(pairs, MemberEncoding::Blocked);
        assert_eq!(blob_type, SHARED_BLOCKED_BLOB_TYPE);
        let m = BlockedMembers::parse(&data, 0..data.len()).unwrap();
        let mut out = Vec::new();
        m.for_each(&data, &mut |p, c| out.push((p, c)));
        out
    }

    #[test]
    fn blocked_round_trips_in_order() {
        let pairs: Vec<(NodeId, NodeId)> = (0..500u64)
            .flat_map(|p| (0..1 + p % 4).map(move |j| (p * 7, p * 7 + 1 + j * 1_000_000)))
            .collect();
        assert_eq!(read_back(&pairs), pairs);
    }

    /// The lookup path, which is the one that matters: every parent must find
    /// exactly its own children and nobody else's.
    #[test]
    fn blocked_lookups_match_what_was_written() {
        let pairs: Vec<(NodeId, NodeId)> = (0..800u64)
            .flat_map(|p| (0..1 + p % 5).map(move |j| (p * 11, 5_000_000 + p * 11 + j * 900_000)))
            .collect();
        let (_, data) = write(&pairs, MemberEncoding::Blocked);
        let m = BlockedMembers::parse(&data, 0..data.len()).unwrap();
        assert!(m.heap_bytes() > 0, "the block index must be in the heap");

        for p in 0..800u64 {
            let want: Vec<NodeId> = pairs
                .iter()
                .filter(|(parent, _)| *parent == p * 11)
                .map(|(_, c)| *c)
                .collect();
            let mut got = Vec::new();
            m.children_of(&data, p * 11, usize::MAX, &mut got);
            assert_eq!(got, want, "children of {}", p * 11);
        }
        // Absent keys, including ones interleaved with present ones and one past
        // the end — the block search must not hand back a neighbour's children.
        for p in [1u64, 3, 7, 12, 100_000] {
            let mut got = Vec::new();
            m.children_of(&data, p, usize::MAX, &mut got);
            assert!(got.is_empty(), "{p} is not a parent, got {got:?}");
        }
    }

    /// A parent with more children than fit in a block gets an oversized block
    /// of its own, because a record must not span blocks — otherwise a lookup
    /// landing on a block would return a truncated child list.
    #[test]
    fn a_wide_parent_is_not_split_across_blocks() {
        let pairs: Vec<(NodeId, NodeId)> = (0..2_000u64).map(|j| (42, 100 + j * 7_777)).collect();
        let (_, data) = write(&pairs, MemberEncoding::Blocked);
        let m = BlockedMembers::parse(&data, 0..data.len()).unwrap();
        let mut got = Vec::new();
        m.children_of(&data, 42, usize::MAX, &mut got);
        assert_eq!(got.len(), 2_000);
        assert_eq!(got, pairs.iter().map(|(_, c)| *c).collect::<Vec<_>>());
    }

    /// The limit must stop the *decode*, not just the push. A hub root's record
    /// is hundreds of thousands of varints, and decoding it to hand back a
    /// thousand children is how a first version of this ended up slower than the
    /// fixed-stride table it replaced.
    #[test]
    fn the_limit_stops_the_decode() {
        let pairs: Vec<(NodeId, NodeId)> = (0..50_000u64).map(|j| (42, 100 + j * 7)).collect();
        let (_, data) = write(&pairs, MemberEncoding::Blocked);
        let m = BlockedMembers::parse(&data, 0..data.len()).unwrap();

        for limit in [0usize, 1, 7, 1_000] {
            let mut got = Vec::new();
            m.children_of(&data, 42, limit, &mut got);
            assert_eq!(got.len(), limit, "limit {limit} not honoured");
            // A prefix of the real children, in order — not an arbitrary subset.
            assert_eq!(
                got,
                pairs
                    .iter()
                    .take(limit)
                    .map(|(_, c)| *c)
                    .collect::<Vec<_>>()
            );
        }

        // Timing is the property that actually matters, and it is the one a
        // "push fewer" implementation would pass while still doing all the work.
        // A limit of 1 must not cost what a limit of 50 000 does.
        let time = |limit: usize| {
            let t = std::time::Instant::now();
            for _ in 0..200 {
                let mut got = Vec::new();
                m.children_of(&data, 42, limit, &mut got);
                std::hint::black_box(&got);
            }
            t.elapsed().as_secs_f64()
        };
        let (small, whole) = (time(1), time(50_000));
        assert!(
            small * 10.0 < whole,
            "a limit of 1 took {small:.6}s against {whole:.6}s for the whole record; \
             the decode is not stopping early"
        );
    }

    #[test]
    fn empty_writers_emit_no_blob() {
        assert!(
            MemberWriter::new(MemberEncoding::Blocked, Tier::Shared)
                .finish()
                .is_none()
        );
        assert!(
            MemberWriter::new(MemberEncoding::Flat, Tier::Overlay)
                .finish()
                .is_none()
        );
    }

    #[test]
    fn the_tier_picks_the_blob_type() {
        let one = |t: Tier, e: MemberEncoding| {
            let mut w = MemberWriter::new(e, t);
            w.push(1, 2);
            w.finish().unwrap().0
        };
        assert_eq!(
            one(Tier::Shared, MemberEncoding::Flat),
            SHARED_FLAT_BLOB_TYPE
        );
        assert_eq!(
            one(Tier::Shared, MemberEncoding::Blocked),
            SHARED_BLOCKED_BLOB_TYPE
        );
        assert_eq!(
            one(Tier::Overlay, MemberEncoding::Flat),
            OVERLAY_FLAT_BLOB_TYPE
        );
        assert_eq!(
            one(Tier::Overlay, MemberEncoding::Blocked),
            OVERLAY_BLOCKED_BLOB_TYPE
        );
    }

    #[test]
    fn blocked_is_smaller_than_flat() {
        // The shape this is for: many parents, most with one or two children.
        let pairs: Vec<(NodeId, NodeId)> = (0..20_000u64)
            .flat_map(|p| (0..1 + p % 3).map(move |j| (p * 3, 40_000_000 + p * 3 + j * 61)))
            .collect();
        let flat = write(&pairs, MemberEncoding::Flat).1.len();
        let blocked = write(&pairs, MemberEncoding::Blocked).1.len();
        assert!(
            blocked * 2 < flat,
            "blocked {blocked} is not at least 2x smaller than flat {flat}"
        );
    }

    #[test]
    fn parse_rejects_a_corrupt_header() {
        let (_, data) = write(&[(1, 2), (1, 3), (5, 9)], MemberEncoding::Blocked);
        assert!(BlockedMembers::parse(&data[..8], 0..8).is_err());
        let mut bad = data.to_vec();
        bad[8..16].copy_from_slice(&9_999u64.to_le_bytes());
        assert!(BlockedMembers::parse(&bad, 0..bad.len()).is_err());
    }
}
