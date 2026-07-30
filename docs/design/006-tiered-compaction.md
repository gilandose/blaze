# 006 — Tiered compaction, and sizing for a backfill-dominated workload

> **Now the top priority.** It replaces "storage-side compaction" (001) at the
> front of the queue, because the recalibrated workload changes which cost
> actually hurts. See "What changed" below.

## The workload, restated

- **~50 links/s average, ~2k/s peak.** That is 4.3M links/day, ~1.6B/year.
- **The bulk of the data is a backfill of historic edges**, not a stream.
- Target scale is billions of links, and the design should not fall over well
  beyond that.

## What changed

Earlier analysis (ARCHITECTURE.md) ranked the fixes as: registry restructure,
then storage-side compaction to remove the ingest stall, then a streaming Puffin
writer. At 3k events/s sustained that was right. At 50/s it is not, for two
reasons.

**A compaction stall is no longer scary.** Queries never take the union lock, so
a stall delays only *ingest*. At 50/s a 30-minute compaction queues ~90k events —
about 5 MB of Arrow buffer — and offsets are the source of truth, so it is a lag
spike, not an outage. Stall *duration* stopped mattering; stall *frequency* and
total write amplification started.

**Layer count explodes precisely because the change rate is low.** The two
triggers we have both misbehave here:

| trigger | behaviour at 50 links/s |
|---|---|
| layer count > 60 (current default) | fires hourly; rewrites a 75 GB base to absorb 7.5 MB of delta |
| delta bytes > 25% of base | fires every ~120 days; by then ~173,000 layers, and every lookup scans them |

Neither is a tuning problem. A single base plus one flat run of deltas has no
setting that is simultaneously cheap to write and cheap to read.

## Design: size-tiered levels

Standard LSM tiering, which is what the layered base was already shaped for.
Each level holds runs roughly `T`× larger than the level below (`T = 10`); when a
level accumulates `T` runs they merge into one run at the next level.

At 50 links/s, with a 60s flush interval and ~126 KB per delta:

| level | run size | runs/day entering | merge cost when it fires |
|---|---|---|---|
| L0 | 126 KB | 1440 | — |
| L1 | 1.26 MB | 144 | 1.26 MB |
| L2 | 12.6 MB | 14 | 12.6 MB |
| L3 | 126 MB | 1.4 | 126 MB |
| L4 | 1.26 GB | every 7 days | 1.26 GB |
| L5 | 12.6 GB | every 70 days | 12.6 GB |

Consequences:

- **Layer count is logarithmic**, ~6–8 runs total instead of thousands, so a
  lookup stays at a handful of probes. This is what keeps the read path inside
  the SLO without bloom filters (which remain a later option).
- **Write amplification is O(log N) per link**, not O(state) per compaction. Each
  link is rewritten ~once per level it passes through: ~6 times over its life,
  versus a full-base rewrite that costs 600,000× that at this change rate.
- **Full-base merges become rare** — L5-and-above events, months apart. That
  largely dissolves 001's storage-side compaction and the 75 GB in-RAM Puffin
  buffer from being blockers: they are still worth fixing, but they stop being on
  the critical path.

Sizing the trigger by *bytes per level* rather than by run count keeps this
correct across change rates: the same configuration that gives 6 levels at 50/s
gives 6 levels at 3k/s, just cycling faster.

### Compatibility

`LayeredBase` already resolves through an ordered stack of runs and k-way merges
them for compaction. Tiering changes only *which* runs get merged and when; the
disjoint-keys property and the resolution loop are unchanged. A run is a run
whether it came from a fold or from an L2→L3 merge.

One real change: today `layers[0]` is privileged as "the base". Under tiering
there is no single base, just runs with levels, so `LocalLayers` needs to carry a
level per run and the catalog needs to list the run set rather than a single
`base_sequence` + chain length.

## Backfill

Measured single-node ingest: **357k links/s** through the DSU, **308k/s** with
Arrow buffering (`examples/throughput.rs`). So 2B links is **~1.8 hours** of
ingest — the backfill is not compute-bound, and no new mechanism is needed to
make it feasible.

Memory during backfill is bounded by the fold trigger, not by the state: set
`--fold-after-links` to ~20M (~3 GB resident) and a 2B backfill folds ~100 times,
writing ~75 GB of runs that tiering then collapses. Total ~2–3 hours on one node
with single-digit GB of RAM.

A dedicated bulk path — sort the historic edges, run external-memory union-find,
emit one finished base — would cut that to perhaps 20–30 minutes, because it
skips per-event DSU work and writes the base exactly once. It is worth building
only if 2 hours actually hurts; note it is a batch tool, not part of the engine.

## Beyond: what limits 10B+

- **The shared tier is inherently single-writer.** A merge can connect any two
  nodes, so union-find cannot be sharded by key range without distributed
  coordination. That is fine here by a wide margin: 2k/s peak is ~0.6% of the
  measured 357k/s write capacity, so one writer serves 10B+ links. The overlay
  tier shards trivially by scope if it ever needs to.
- **Ids.** [002](002-dense-interning.md)'s u32 interning caps at 4.3B nodes and
  must not be used at this target; u64, or packed u48 if the two bytes matter.
- **Delta blob *shape*, which is the sharper problem at a low change rate.**
  One Puffin blob per scope is right for a base and wrong for a delta: the footer
  lists every blob at ~137 bytes, so per-layer overhead is O(scopes) no matter
  how little changed. Measured (`examples/blob_overhead.rs`), a 60s delta at
  50 links/s dusted across 3000 tenants is **74% bookkeeping** — 556 KB carrying
  144 KB of pairs. A single combined overlay blob for delta layers,
  `(scope u32, node u64, root u64)` sorted by `(scope, node)`, is ~4x smaller and
  takes blob count per delta from 3002 to 2. Bases keep per-scope blobs, where
  each is large enough for separate indexing to pay off — a format tier matching
  the compaction tier.
- **The registry is 55% of base bytes** at a flat 12-byte `(root, scope)` stride,
  so grouping by root would save ~30 GB at 2B links and ~125 GB at 10B. Note this
  is now a *space* optimization only: building it no longer needs an O(state)
  sort (see `storage::compact`), and looking it up was always one binary search.
  Disk is the cheapest resource here, so this ranks below the blob-shape fix
  despite the bigger number.
- **Probing per-scope blobs instead of keeping a registry** only works with
  in-RAM membership filters. Without them it is ~3 ms per global merge (3000
  binary searches), which is 2% of a core at the 50/s average but nearly a whole
  core at the 2k/s peak, and worse cold.
- **Disk**: ~375 GB of base at 10B links before the registry fix, ~250 GB after.
  Unremarkable for local NVMe.

Nothing structural stands in the way of 10B; it is tiering plus constant factors.

## Invariants & tests

- I1/I6: the randomized model test runs across a tiering cycle, so a merge that
  promotes runs between levels must not change any answer.
- Disjoint keys must hold for merged runs exactly as for folded ones — assert it
  directly rather than trusting the argument.
- A lookup's probe count must stay bounded by level count as run count grows;
  pin it the way `narrowed_search_reads_at_most_one_block` pins the page bound.
