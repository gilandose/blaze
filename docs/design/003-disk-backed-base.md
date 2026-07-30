# 003 — Disk-backed routing base (LSM-in-time)

> **Status: implemented** (`--routing-base disk`, `src/storage/base.rs`,
> `src/core/base.rs`). Shipped: mmap'd base with binary-search lookups, the
> composed base+memtable read *and* write paths, a `blaze-registry-v1` index
> so fix-ups against base-resident roots cost one probe, local read-through
> caching from object storage, and compaction that re-emits composed state.
> Deferred: bloom filters over overlay membership (the registry blob covers
> the same need for now) and pruning the memtable at compaction boundaries
> beyond what a fresh base already does. Measured results are in
> ARCHITECTURE.md; note this landed *before* 001/002, so the memtable is
> bounded by flush cadence rather than by delta chains.

## Problem

Even after 002, 2B links costs ~100 GB RAM, and cold start means
downloading and re-inserting ~32 GB of pairs (~10+ minutes). Both costs are
paid for state that is overwhelmingly *cold* — at 3k events/s the hot set
is a tiny fraction of 2B links.

## Design

Reuse the base/delta split from 001 as a two-tier read structure — the same
layered composition the engine already uses twice (scope over global; delta
over base), now layered across the RAM/disk boundary:

- **Base (disk)**: the latest compacted Puffin file, cached on local NVMe
  (read-through from S3), mmap'd. Blobs are sorted `(node, root)` runs;
  lookup is binary search over the mapped range — the exact access path the
  `puffin_lifecycle` example demonstrates, ~10–50 µs on a cold NVMe page,
  ~1 µs when page-cached.
- **Memtable (RAM)**: the in-memory forest holds only links applied since
  the base was compacted, plus the dirty overlay of 001.

### Read path

```text
scope_root(s, x):
  g = mem.shared.find_ro(x)             # memtable first (newest info)
  if g == x:                            # memtable knows nothing about x ->
      g = mem.shared.find_ro(base.shared_parent(x) or x)   # probe base, then
                                                           # refine downward
  ... same two-step within scope s's overlay ...
```

As implemented, one base probe plus one memtable walk is exact — no
re-probing loop. Two facts make that true:

1. **Every mutation composes before writing** (`apply_*`, merge fix-ups and
   `snapshot`), so a memtable key is always a *composed root*: a node the
   base stores no parent for. Hence a node that already has a memtable parent
   cannot also be in the base — the base probe is skippable in that case, and
   a memtable walk can never land on a node needing another base probe.
2. **Roots only decrease** (I2), so the base answer is a valid earlier
   representative and the memtable refines it downward, never the reverse —
   the same stale-root reasoning already proven for scope fix-ups, applied in
   time rather than across tenants.

### Memtable contents & trimming

The memtable must retain any link whose *absence* would change an answer:
every union applied since `base_sequence`. After each compaction the
memtable resets to empty (the new base subsumes it) — so memtable size is
bounded by event rate × compaction interval: at 3k events/s and hourly
compaction, ~11M links ≈ 500 MB with 002 in place. RAM budget becomes
**O(hot window)**, not O(state).

One subtlety: merge-notification registry entries must survive compaction
for roots that live only in the base. Rather than keeping the full registry
in RAM, fix-ups consult the base's scope blobs on demand: a global merge of
root B checks which scopes' base blobs contain B (the per-scope blobs are
sorted; a per-scope bloom filter over members, stored as one more Puffin
blob at compaction time, makes this a no-I/O check in the common miss
case). Registry RAM also becomes O(hot window).

### Restart path

1. Ensure local NVMe copy of the base (download if absent) — mmap it:
   **serving starts here**, seconds after boot, at base-only fidelity.
2. Replay delta chain into the memtable (small), then resume the log from
   the watermark. Full fidelity within seconds; no O(state) hydration ever.

### Failure model

I4 unchanged: NVMe holds only copies of committed S3 objects. Node loss =
re-download on the replacement; corrupted local file = checksum mismatch →
re-download. The mmap is never written except by whole-file replacement on
compaction (atomic rename).

## Cost impact (target profile)

3× `i4i.2xlarge` (64 GB RAM, 1.7 TB NVMe) ≈ $1.5k/mo replaces 3× 256 GB
RAM instances ≈ $3.7k/mo, and restarts drop from ~10 min to ~seconds.
Trade: cold-node lookups go from ~0.5 µs to ~10–50 µs — still well inside
the sub-millisecond SLO.

## Invariants & tests

- I1/I6: randomized model test runs against a forest in mixed base+memtable
  state, including queries whose answers straddle the boundary (node in
  base, root refined in memtable).
- I3: base lookups are reads of an immutable mapping — trivially lock-free.
- New: kill-and-restart test asserting time-to-first-correct-answer and
  that base+delta replay equals the writer's state at the watermark.

## Effort

Depends on 001 (formats) and benefits from 002 (memtable density).
Read-path composition + local cache + atomic swap on compaction: ~3–4 days.
The bloom-filter registry assist can land separately (~1 day) — until then,
keep the full registry in RAM (~16 GB at target, acceptable interim).
