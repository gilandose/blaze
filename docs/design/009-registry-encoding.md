# 009 — Registry encoding

The registry maps a shared root to the scopes holding overlay state keyed on it,
so a global merge notifies only those scopes rather than broadcasting to
thousands. It is the largest single component of a base and the last untouched
one.

> **Built: delta-varint in indexed blocks**, as `blaze-registry-v2`. Measured
> **4.8–7.1x** on the registry — a real base goes from **133.6 MB to 81.0 MB**,
> 39% off — for a `scopes_for_root` that costs 1.40 µs against the flat form's
> 0.59 µs. Design 006's proposal, grouping entries by root, measures 1.5–2.3x.
> The difference is that grouping only removes repeated *root* bytes, while delta
> encoding exploits the fact that roots are nearly dense over the node space and
> therefore have gaps of about one.

## What the measurements say

`examples/registry_shape` walks a compacted base and reports the distribution;
`examples/soak` re-derives it as state grows. Both at 3,000 scopes, 1–3 per edge.

| | 2M links | 8M links, denser |
|---|---|---|
| distinct roots | 1,836,713 | 1,884,556 |
| mean scopes/root (`k`) | 3.05 | 9.86 |
| base | 133.6 MB | 764.2 MB |
| registry, flat `(u64, u32)` | 67.1 MB — **50% of base** | 222.9 MB — 29% |
| registry, grouped by root | 44.4 MB — 1.51x | 96.9 MB — 2.30x |
| registry, delta-varint | **13.9 MB — 4.82x** | **31.3 MB — 7.13x** |
| base shrinks by | 40% | 25% |

Three facts from the soak drive the design:

- **`k` grows with scale.** 2.05 → 2.27 → 2.26 → 2.85 → 3.00 across 5M–25M links,
  still climbing. Components coalesce, so scopes concentrate onto fewer roots.
  A format whose saving improves with `k` therefore gets better over time, which
  is the right direction — but grouping's ceiling is `12k / (12 + 4k) → 3x` even
  as `k → ∞`, and it is only at 1.5x where we actually are.
- **Roots are nearly dense over the node space.** 1.88M distinct roots, sorted
  ascending. Consecutive gaps average close to one, so a varint spends **one byte**
  where the flat encoding spends eight. This is the whole win and grouping cannot
  capture any of it.
- **Scopes within a root are sorted and bounded by scope count.** At `k = 3` over
  3,000 scopes the gaps average ~1,000, so two varint bytes each rather than four.

## Design

Replace the flat `(root u64, scope u32)` stride with **delta-varint records in
offset-indexed blocks** (`storage::registry`):

```
payload:
  u64      entry count
  u64      block count B
  u64 xB+1 block offsets, relative to the first block
  blocks

block:
  u64      first root, absolute
  u32      record count
  record   [varint gap from previous root]  varint k  varint scope gap x k
```

The first record of a block omits the root gap, because the block header carries
its root absolutely. That is what makes a block decodable without its
predecessors and gives the reader an O(1) first key.

Blocks are **variable length, with explicit offsets**, not a fixed stride. A
root's scope list is never split across blocks, so a root wide enough to exceed
the target size gets an oversized block to itself — at 3,000 scopes that is a
real case, not a hypothetical. The offsets cost 8 bytes per block in the file and
16 in heap alongside the first roots, which is why the `SparseIndex` the pair
tables use could not be reused: it assumes a fixed stride.

**Block size is the whole latency trade**, because the scan is linear varint
decoding rather than a binary search. Measured at 2M links, `k = 3.05`:

| block | registry | blocks | heap | hit | miss |
|---|---|---|---|---|---|
| 256 B | 15.0 MB | 55,077 | 861 KB | 0.95 µs | 0.76 µs |
| **512 B** | **14.5 MB** | **27,373** | **428 KB** | **1.40 µs** | **1.19 µs** |
| 1 KiB | 14.2 MB | 13,647 | 213 KB | 2.28 µs | 2.12 µs |
| 4 KiB | 14.0 MB | 3,405 | 53 KB | 7.62 µs | 7.32 µs |
| 8 KiB | 14.0 MB | 1,702 | 27 KB | 14.74 µs | 14.44 µs |

Cost is essentially linear in block size — about 1.8 µs per KiB — while the byte
saving flattens out past 1 KiB. **512 bytes is the default**: it takes the whole
compression win and stays within 2.4x of the flat lookup, and the 428 KB of
offsets is 7% on top of the sparse indexes the pair tables already hold.

The first draft of this document guessed "1–2 µs" for 4 KiB blocks. That was
wrong by 4x, and the sweep is here because the cost of decoding varints per byte
is not something worth predicting.

## Cost

`scopes_for_root` goes from 0.59 µs to 1.40 µs on a hit.

**That is a writer-only path.** The only caller is `apply_global`, which runs at
the global-merge rate: ~15/s at the production profile, ~300/s at peak. Even at
peak that is 0.04% of a core. Queries never touch the registry.

Compaction reads the registry end to end, where a sequential varint scan is if
anything friendlier than a strided read.

## Compatibility

The blob type **is** the interface — a Puffin blob is self-describing, and
`PuffinBase::open` already matches on it — so no format-version negotiation is
needed and nothing has to agree cluster-wide. Three consequences:

- A reader that does not recognise `blaze-registry-v2` sees no registry blob and
  takes the existing `build_fallback_registry` path, rebuilding the mapping in
  memory at load time (`registry_falls_back_when_blob_absent`). An older binary
  reads a newer base correctly, just with more heap and a slower start. Better
  than the run-set format's one-way upgrade, and worth keeping.
- Runs are read independently, so a **stack may mix encodings**. A worker that
  starts emitting v2 does not rewrite the v1 runs beneath it; they merge together
  and the merge output is whatever the writer is configured for.
- `--registry-encoding flat` writes the old format from a current binary, which
  is the rollback path if one is ever needed.

`WriteOptions` carries both this and `filter_bits`, since they are the same kind
of knob: per-run, read-compatible in both directions, and changeable without
touching what is already committed.

## Why not just use Parquet

This was measured rather than argued, because the `parquet` crate is already a
dependency and hand-rolling a compression scheme needs justifying. Both columns
were written through a real writer with `DELTA_BINARY_PACKED` and dictionary
encoding off:

| | 2M links, `k` = 3.05 | 8M links, `k` = 9.95 |
|---|---|---|
| flat | 67.1 MB | 183.9 MB |
| delta-varint | **14.0 MB** (4.80x) | **26.1 MB** (7.04x) |
| Parquet | 11.7 MB (5.74x) | 29.7 MB (6.19x) |
| Parquet + zstd | 10.7 MB (6.26x) | 25.6 MB (7.18x) |

They trade places. Parquet wins by 16% at low fan-out, where bit-packing beats
byte-granular varints; the varint form wins by 12% at high fan-out, where storing
`k` once and omitting the repeated root beats approximating that with runs of
zero deltas, and where one wide scope gap widens a whole bit-packed miniblock.
Compressed Parquet is marginally best everywhere, by 2% of the registry — under
1% of the base.

So the size argument does not decide it, and what is left is the reader. Parquet
means decoding a page per lookup, an allocation per lookup, and no mmap-and-serve
— against a format whose entire reader is 150 lines and touches one page. That is
not worth 1% of a base.

**This holds only while the registry is one sorted column read by one caller.**
If the base ever moves to Parquet or Arrow IPC wholesale — design 008's argument,
which is about the pair tables, not this — delete this encoding rather than port
it.

## What this does not fix

The registry is 50% of base at `k = 3` but only 29% at `k = 9.86`, because
overlay pairs grow faster than registry entries as scopes accumulate. So this is
a large one-off win that **shrinks as a share** at higher fan-out. It does not
change any asymptotic, and the overlay tables become the dominant term after it.

## Invariants & tests

- **Round-trip.** `blocked_round_trips_in_order` encodes and re-reads the whole
  table, checking order and count.
- **Agreement with the flat form.** `blocked_lookups_match_the_flat_entries`
  asserts against the entries themselves, for present roots and both their
  neighbours, rather than a fixture.
- **Block edges**, which is where this breaks:
  `blocked_finds_the_roots_at_both_edges_of_every_block` covers the root a block
  header carries and the root a scan runs off the end looking for;
  `a_root_wider_than_a_block_gets_its_own` covers the record that cannot fit.
- **Compaction**, which re-keys entries to live roots while merging runs it did
  not write. `storage_side_compaction_matches_the_in_memory_compactor` runs three
  ways — all-flat, all-blocked, and a **mixed** stack — since the mixed case is
  what a worker actually sees after the encoding changes.
- **Both encodings at the base level.**
  `registry_finds_runs_across_blocks_in_either_encoding` and
  `registry_index_resolves_live_roots` are parameterized, so a reader answering
  differently depending on what it found is a failure.
