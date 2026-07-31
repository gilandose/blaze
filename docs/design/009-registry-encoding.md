# 009 — Registry encoding

The registry maps a shared root to the scopes holding overlay state keyed on it,
so a global merge notifies only those scopes rather than broadcasting to
thousands. It is the largest single component of a base and the last untouched
one.

> **Recommendation: delta-varint in indexed blocks.** Measured **4.8–7.1x** on the
> registry, taking **25–40% off the whole base**. Design 006's proposal — grouping
> entries by root — measures 1.5–2.3x, worth 16–17%. The difference is that
> grouping only removes repeated *root* bytes, while delta encoding exploits the
> fact that roots are nearly dense over the node space and therefore have gaps of
> about one.

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
fixed-size blocks**:

```
block (4 KiB):
  varint  gap from the previous block's last root
  varint  scope count k
  varint  gap from 0 to first scope
  varint  gap to next scope        } k-1 times
  varint  gap to next root
  ...
```

A block boundary restarts the root delta from the block's own first root, so a
block is decodable without reading its predecessors.

**Lookups reuse the existing sparse index.** `SparseIndex` already stores the
first key per 4 KiB block for the pair tables and is the machinery that makes a
narrowed binary search touch one page. The registry gets the same treatment:
binary search the in-heap index to find the block, then scan the block decoding
varints until the root is found or passed.

## Cost

`scopes_for_root` goes from a binary search within a narrowed range to a linear
scan of one block — roughly 512 roots at the measured density. Call it 1–2 µs
against today's ~0.65 µs.

**That is a writer-only path.** The only caller is `apply_global`, which runs at
the global-merge rate: ~15/s at the production profile, ~300/s at peak. Even at
2 µs and peak rate that is 0.06% of a core. Queries never touch the registry.

Compaction reads the registry end to end, where a sequential varint scan is if
anything friendlier than a strided read.

## Compatibility

The new encoding gets its own blob type. A reader that does not recognise it sees
no registry blob and takes the existing `build_fallback_registry` path, rebuilding
the mapping in memory from the overlay tables at load time — already implemented
and tested (`registry_falls_back_when_blob_absent`). So an older binary reads a
newer base correctly, just with more heap and a slower start. That is a better
compatibility story than the run-set format got, and worth keeping.

## Why not just use Parquet

Design 008's stronger criticism applies here: a columnar format would do delta
encoding, run-length encoding and block indexing **automatically**, and we would
delete a hand-written table reader rather than add a second bespoke encoding. The
measured 4.8–7.1x is roughly what Parquet's delta-binary-packed encoding achieves
on sorted integer columns, so this design is best understood as hand-rolling one
column of what a columnar reader gives for free.

The case for doing it by hand anyway is narrow and honest: the registry is one
column with one access pattern, the sparse-index machinery already exists, and
converting the whole base to Parquet is a much larger change that touches the
mmap-and-serve property the disk tier depends on. If the base ever moves to
Parquet or Arrow IPC, this encoding should be deleted rather than ported.

## What this does not fix

The registry is 50% of base at `k = 3` but only 29% at `k = 9.86`, because
overlay pairs grow faster than registry entries as scopes accumulate. So this is
a large one-off win that **shrinks as a share** at higher fan-out. It does not
change any asymptotic, and the overlay tables become the dominant term after it.

## Invariants & tests

- Round-trip: encode then decode any `(root, scopes)` set and get it back exactly.
- `scopes_for_root` must return identical results to the flat encoding for every
  root in a base, including absent roots — assert against the flat implementation
  rather than a fixture.
- Block boundaries are where this breaks: a root whose scope list spans a block,
  and a block whose first root is the one being searched for.
- The randomized model test must pass across a compaction that rewrites the
  registry, since `compact_layers` re-keys entries to live roots.
