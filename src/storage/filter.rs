//! Blocked membership filters, so a layer that does *not* hold a key can say so
//! without a binary search over the mapping.
//!
//! # Why blocked, specifically
//!
//! A lookup resolves through every layer until it hits, so most probes are
//! misses, and a miss currently costs a narrowed binary search — measured
//! ~0.65 µs, which is what makes depth expensive on all four paths (queries,
//! `apply`, folds, compaction).
//!
//! A *classical* Bloom filter would not help. With k=6 the six bit positions
//! land anywhere in a multi-megabyte array, so a probe is ~6 cache misses —
//! about what the binary search it replaces already costs. The win only appears
//! if all of a key's bits live in **one cache line**, which is what the blocked
//! variant does: one hash picks a 64-byte block, and every bit for that key is
//! set within it. A probe is then a single cache miss.
//!
//! # Sizing
//!
//! 8 bits per key at k=6 gives a false-positive rate around 2-3%, which is the
//! right trade here: a false positive costs exactly the binary search that would
//! have happened anyway, so the filter can be small and sloppy. That works out
//! at **1 byte of heap per key**, or ~3.6% of the pair bytes it covers.
//!
//! # Why this is the memory floor, and the `--filter-bits` escape hatch
//!
//! Filters are the only thing a disk-backed base holds in RAM that scales with
//! *total state* and cannot be reclaimed. The memtable is bounded by a trigger,
//! and the mapping is clean file-backed pages the kernel evicts under pressure —
//! but these are heap, so at billions of links they set the instance size.
//!
//! Measured through the real flusher (`examples/detached_merge`, 2M links, 3000
//! scopes, 1-3 scopes per edge):
//!
//! | bits | index heap | ingest/s |
//! |---|---|---|
//! | 8 | 4044 KB | 88k |
//! | 4 | 2125 KB | 72k |
//! | 0 | 273 KB | 57k |
//!
//! Dropping filters takes the unreclaimable heap down **14.8x** — the 273 KB left
//! is the sparse index, which stays — for about **35% of ingest throughput**. Some
//! of that 35% is second-order: slower ingest stretches the run, which fits more
//! ticks and so more folds competing for the union lock, so treat the direction as
//! certain and the magnitude as indicative.
//!
//! Note the filter cost is per *key*, not per link: at 1.9 bytes per link on this
//! mix, 2B links is ~3.8 GB. Overlay pairs outnumber links whenever edges carry
//! several scopes, so the ratio is workload-shaped.

use bytes::{BufMut, Bytes, BytesMut};

/// Bits set per key. All within one block, so a probe is one cache line.
const K: u32 = 6;
/// One cache line.
const BLOCK_BITS: usize = 512;
const BLOCK_WORDS: usize = BLOCK_BITS / 64;

/// Default bits of filter per key: ~2-3% false positives at K=6, one byte of heap
/// per key.
pub const DEFAULT_FILTER_BITS: usize = 8;

/// Bits per key, or zero to build no filter at all.
///
/// This is the one dial that moves the *unreclaimable* memory floor. Everything
/// else a disk-backed base holds in RAM is either bounded by a trigger (the
/// memtable) or clean file-backed pages the kernel evicts under pressure; filters
/// are heap and scale with total state, so at 2B links they are ~2.3 GB whether
/// or not you have the RAM.
///
/// Lowering it costs query latency, not correctness: a false positive is exactly
/// the binary search that would have happened without a filter. Roughly, at K=6,
/// 8 bits gives ~2-3% false positives, 4 bits ~10-15%, and 0 disables filters so
/// every probe is a binary search.
///
/// **Readers do not need to know this value.** The encoded filter records its own
/// block count and `locate` derives everything from it, so a base written at any
/// setting — including none at all — is readable by any build.
pub fn clamp_filter_bits(bits: usize) -> usize {
    bits.min(64)
}

const MAGIC: u32 = 0x424C_4631; // "BLF1"

/// splitmix64 — cheap, and good enough that consecutive node ids do not collide
/// into the same block.
#[inline]
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Hash for an overlay key, which is a `(scope, node)` pair rather than a node.
#[inline]
pub fn overlay_key(scope: u32, node: u64) -> u64 {
    mix(node ^ ((scope as u64) << 40).wrapping_mul(0x9E37_79B9))
}

#[derive(Debug, Clone)]
pub struct BlockedFilter {
    blocks: Vec<[u64; BLOCK_WORDS]>,
}

impl BlockedFilter {
    /// Build over `keys` at `bits_per_key`, or `None` to build no filter.
    ///
    /// `hint` only sizes the allocation; a wrong hint costs accuracy, never
    /// correctness.
    pub fn build_with(
        keys: impl Iterator<Item = u64>,
        hint: usize,
        bits_per_key: usize,
    ) -> Option<Self> {
        let bits = clamp_filter_bits(bits_per_key);
        if bits == 0 {
            return None;
        }
        Some(Self::build(keys, hint, bits))
    }

    /// Build over `keys`. `hint` only sizes the allocation; a wrong hint costs
    /// accuracy, never correctness.
    pub fn build(keys: impl Iterator<Item = u64>, hint: usize, bits_per_key: usize) -> Self {
        let bits = clamp_filter_bits(bits_per_key).max(1);
        let blocks = (hint * bits).div_ceil(BLOCK_BITS).max(1);
        let mut filter = Self {
            blocks: vec![[0u64; BLOCK_WORDS]; blocks],
        };
        for key in keys {
            let (block, pattern) = filter.locate(key);
            for (w, bits) in pattern.iter().enumerate() {
                filter.blocks[block][w] |= bits;
            }
        }
        filter
    }

    /// `false` means definitely absent; `true` means "worth a real lookup".
    #[inline]
    pub fn may_contain(&self, key: u64) -> bool {
        let (block, pattern) = self.locate(key);
        // One cache line touched, then a handful of ANDs.
        let b = &self.blocks[block];
        pattern
            .iter()
            .enumerate()
            .all(|(w, bits)| b[w] & bits == *bits)
    }

    /// The block a key belongs to, and the bit pattern it sets within it.
    #[inline]
    fn locate(&self, key: u64) -> (usize, [u64; BLOCK_WORDS]) {
        let mut h = mix(key);
        let block = (h >> 32) as usize % self.blocks.len();
        let mut pattern = [0u64; BLOCK_WORDS];
        for _ in 0..K {
            h = mix(h);
            let bit = (h % BLOCK_BITS as u64) as usize;
            pattern[bit / 64] |= 1u64 << (bit % 64);
        }
        (block, pattern)
    }

    pub fn heap_bytes(&self) -> usize {
        self.blocks.len() * BLOCK_WORDS * 8
    }

    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(8 + self.heap_bytes());
        out.put_u32_le(MAGIC);
        out.put_u32_le(self.blocks.len() as u32);
        for block in &self.blocks {
            for word in block {
                out.put_u64_le(*word);
            }
        }
        out.freeze()
    }

    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(data.len() >= 8, "filter blob truncated");
        let magic = u32::from_le_bytes(data[..4].try_into().unwrap());
        anyhow::ensure!(magic == MAGIC, "filter blob has wrong magic {magic:#x}");
        let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let want = 8 + count * BLOCK_WORDS * 8;
        anyhow::ensure!(
            data.len() == want,
            "filter blob length mismatch: header says {count} blocks ({want} bytes), got {}",
            data.len()
        );
        let mut blocks = Vec::with_capacity(count);
        for b in 0..count {
            let mut block = [0u64; BLOCK_WORDS];
            for (w, word) in block.iter_mut().enumerate() {
                let at = 8 + (b * BLOCK_WORDS + w) * 8;
                *word = u64::from_le_bytes(data[at..at + 8].try_into().unwrap());
            }
            blocks.push(block);
        }
        Ok(Self { blocks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_member_is_reported_present() {
        // No false negatives, ever — a miss would make a layer claim it does not
        // hold a key it does hold, and the lookup would skip it.
        let keys: Vec<u64> = (0..50_000).map(|i| i * 7 + 3).collect();
        let f = BlockedFilter::build(keys.iter().copied(), keys.len(), DEFAULT_FILTER_BITS);
        for k in &keys {
            assert!(f.may_contain(*k), "false negative on {k}");
        }
    }

    #[test]
    fn false_positive_rate_is_low_enough_to_be_worth_it() {
        let keys: Vec<u64> = (0..100_000u64).map(|i| i * 3).collect();
        let f = BlockedFilter::build(keys.iter().copied(), keys.len(), DEFAULT_FILTER_BITS);
        // Probe keys that are definitely absent (not multiples of 3).
        let mut fp = 0;
        let trials = 100_000;
        for i in 0..trials {
            let probe = i * 3 + 1;
            if f.may_contain(probe) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(
            rate < 0.06,
            "false positive rate {rate:.3} too high to pay for"
        );
        // ~1 byte per key is the point of the sizing.
        assert!(
            f.heap_bytes() <= keys.len() * 2,
            "filter is larger than sized"
        );
    }

    /// Fewer bits is a smaller filter that lies more often, and zero is no filter
    /// at all. Both must stay *sound* — a false negative would make a layer deny a
    /// key it holds, and the lookup would skip it.
    #[test]
    fn fewer_bits_trade_accuracy_for_heap_and_never_correctness() {
        let keys: Vec<u64> = (0..50_000u64).map(|i| i * 7 + 3).collect();
        let mut sizes = Vec::new();
        for bits in [4usize, 8, 16] {
            let f = BlockedFilter::build_with(keys.iter().copied(), keys.len(), bits)
                .expect("nonzero bits builds a filter");
            for k in &keys {
                assert!(f.may_contain(*k), "false negative at {bits} bits on {k}");
            }
            sizes.push(f.heap_bytes());
        }
        assert!(
            sizes[0] < sizes[1] && sizes[1] < sizes[2],
            "heap must scale with bits per key, got {sizes:?}"
        );

        // Zero means no filter, which the writer turns into no blob and the reader
        // into "probe the table directly".
        assert!(BlockedFilter::build_with(keys.iter().copied(), keys.len(), 0).is_none());
    }

    #[test]
    fn roundtrips_through_bytes() {
        let keys: Vec<u64> = (0..5_000).map(|i| i * 11).collect();
        let f = BlockedFilter::build(keys.iter().copied(), keys.len(), DEFAULT_FILTER_BITS);
        let back = BlockedFilter::decode(&f.encode()).unwrap();
        for k in &keys {
            assert!(back.may_contain(*k));
        }
        assert_eq!(back.heap_bytes(), f.heap_bytes());
    }

    #[test]
    fn rejects_corrupt_blobs() {
        assert!(BlockedFilter::decode(&[1, 2, 3]).is_err());
        let f = BlockedFilter::build([1u64, 2, 3].into_iter(), 3, DEFAULT_FILTER_BITS);
        let mut bad = f.encode().to_vec();
        bad.truncate(bad.len() - 1);
        assert!(BlockedFilter::decode(&bad).is_err());
        let mut wrong_magic = f.encode().to_vec();
        wrong_magic[0] ^= 0xFF;
        assert!(BlockedFilter::decode(&wrong_magic).is_err());
    }

    /// Overlay keys are `(scope, node)`, and two different scopes holding the
    /// same node must not be conflated — that would make one scope's filter
    /// answer for another's membership.
    #[test]
    fn overlay_keys_separate_scopes() {
        let f = BlockedFilter::build(
            (1..=200u32).map(|s| overlay_key(s, 42)),
            200,
            DEFAULT_FILTER_BITS,
        );
        for s in 1..=200u32 {
            assert!(f.may_contain(overlay_key(s, 42)));
        }
        // Scopes well outside the built set should mostly miss.
        let misses = (10_000..10_500u32)
            .filter(|&s| !f.may_contain(overlay_key(s, 42)))
            .count();
        assert!(
            misses > 400,
            "scope is not discriminating: {misses}/500 missed"
        );
    }
}
