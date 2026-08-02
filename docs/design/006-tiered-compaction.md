# 006 — Tiered compaction, and sizing for a backfill-dominated workload

> **Shipped, and the exponent is confirmed by measurement.** A 145M-link stress
> run (`examples/ceiling.rs`) showed flat base+delta compaction is **O(N²) in
> total state**: per-pair cost is constant (3.39, 3.44, 3.33 µs across three
> compactions) while each cycle merges linearly more, so the costs sum
> quadratically. Extrapolated to 2B links: **~58 hours of compaction against ~13
> hours of ingest**, i.e. 82% of wall clock.
>
> Size-tiered compaction is now in behind `--tier-fanout`. Measured at a matched
> depth ceiling (`examples/tier_amplification`), doubling the workload doubles the
> flat policy's write amplification while tiering's grows by 1.33x — 8.5x less
> merge work at 500 folds, and widening. See "Measured after shipping".

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
| layer count > 60 (the default when this was written; it is now **24**) | fires hourly; rewrites a 75 GB base to absorb 7.5 MB of delta |
| delta bytes > 25% of base | fires every ~120 days; by then ~173,000 layers, and every lookup scans them |

Neither is a tuning problem. A single base plus one flat run of deltas has no
setting that is simultaneously cheap to write and cheap to read.

## Measured

145M links in 56.2 min on a 4-core / 15 GB / 17 GB-disk box, fold every 5M links,
compact at 8 layers. Stopped by the disk guard, since a compaction needs room for
the old and new base at once.

| # | pairs merged | time | µs/pair | registry | corrections | output |
|---|---|---|---|---|---|---|
| 1 | 40.4M | 136.8 s | 3.39 | 68.0M | 359,223 (0.53%) | 1.46 GB |
| 2 | 76.5M | 263.4 s | 3.44 | 127.5M | 1,000,104 (0.78%) | 2.75 GB |
| 3 | 113.2M | 376.8 s | 3.33 | 187.0M | 1,658,017 (0.89%) | 4.06 GB |

- **The quadratic is the headline.** Linear per-pair cost × linearly growing state
  per cycle = quadratic total. This is the ceiling, and no dial avoids it.
- **Since measured, the constant improved 2.19x** — an identical compaction (same
  5,999,436 shared and 34,412,994 overlay pairs, same 67,999,607 registry entries,
  same 359,223 corrections, same 1.46 GB output) went from **136.8 s to 62.6 s**,
  via a moved-root hash set in place of a per-entry `shared_parent` resolve plus a
  parallel per-scope overlay merge. That changes the constant only: the total is
  still quadratic, so tiering remains the precondition.
- **Effective throughput was 43k links/s** end to end including folds and
  compactions — against 253k/s ingest at one layer, and the 357k/s the DSU
  sustains with no base attached at all.
- **Depth, not size, drives the slowdown**: ingest at depth 7 held at 46.8k →
  44.3k → 47.0k links/s across the three cycles even as state tripled.
- **Bounded corrections hold**, at under 1% — ~20 MB buffered where
  collect-and-sort needed 2.2 GB. But the fraction drifts up (0.53 → 0.78 →
  0.89%) as scopes come to reference more roots each, so it is not a constant.
- **Heap flat, RSS not**: ~0.9 GB anonymous, but 8.4 GB RSS against a 5.4 GB base,
  because compaction reads the base end to end and those clean file-backed pages
  stay resident until evicted. Reclaimable, but it means compaction evicts the
  query working set while it runs. An `MADV_DONTNEED` over the merged range once
  the sweep finishes would bound it.

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
  lookup stays at a handful of probes. Note this bounds depth but does not make
  depth free: with per-layer blocked filters shipped, 8 layers costs 1.98x ingest
  and 3.06x lookups versus one (down from 3.97x and 6.10x without them). Tiering
  and filters solve different halves and both are wanted — filters make each layer
  cheap, tiering stops the count growing without bound.

  The filters also move this design's own arithmetic: total merge work goes as
  `N²/(2·F·L)`, so it is inversely proportional to depth. Measured, **16 layers
  now cost less than 8 did**, which halves the quadratic constant for free before
  tiering changes the exponent at all.
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

## Measured after shipping: the exponent, not just the constant

`examples/tier_amplification` runs both policies over identical ingest through the
real fold and merge code. Both arms hold the **same depth ceiling of 12 runs**,
because comparing at different depths proves nothing — the flat policy can always
cut its write cost by carrying more runs, and breaking that trade is the whole
point. `amp` is pairs rewritten per link ingested.

| folds | policy | merges | pairs merged | amp | GB written | merge s |
|---|---|---|---|---|---|---|
| 250 | flat | 22 | 20.7M | 20.72x | 0.80 | 18.2 |
| 250 | tier T=10 | 43 | 3.7M | **3.72x** | 0.21 | 3.0 |
| 500 | flat | 45 | 84.5M | 42.24x | 3.10 | 83.3 |
| 500 | tier T=10 | 101 | 9.9M | **4.96x** | 0.51 | 9.3 |

- **The exponent changed, which is the claim.** Doubling the fold count doubles
  flat's amplification (20.72 → 42.24, i.e. **2.04x**) — linear in fold count, so
  quadratic in total state. Tiering's rises **1.33x** against a pure-log
  prediction of 1.13x; the excess is the depth ceiling forcing relief merges on
  top of level merges, and it is decisively sublinear either way.
- **The advantage therefore widens with scale**: 5.6x fewer pairs merged at 250
  folds, 8.5x at 500. 500 folds is ~8 hours of 60s ticks; the regime that matters
  is ~500,000 folds a year.
- Tiering does **more** merges (101 vs 45) and each is far smaller — 6x fewer
  bytes written and 9x less merge time. That is the trade working as designed:
  frequent cheap merges instead of rare enormous ones.

Two things this corrected:

- **Larger fanout is better for write amplification, not worse.** Amplification is
  roughly the number of levels, `log_T(F)`, so a *small* fanout means more levels
  and more rewriting: measured, `T=2` cost **13.58x** where `T=10` cost 4.96x on
  the same workload. Fanout trades write cost against depth monotonically —
  `(T-1)·log_T(F)` runs to probe — so pick it for the depth you can afford. The
  default of 10 is in the right place.
- **A first attempt at this measurement was invalid** and is worth recording so
  nobody repeats it. The baseline expressed "never tier" as an unreachable fanout,
  which falls through to the depth-ceiling branch — and that branch merges the
  lowest stretch, not the whole stack. So both arms were tiered, and they scored
  4.98x and 4.96x. Reassuringly consistent, and completely uninformative.

## Shipped

All three pieces are in: the catalog run-set format (`SnapshotMeta.runs`, with
old catalogs synthesising one from `base_sequence` + `delta_chain_len` so there is
no migration), the flush loop expressed in runs, and the policy in
`storage/tier.rs` behind `--tier-fanout`.

The correctness question a subset merge raises is whether the output may be
spliced back **at the position its inputs held**, rather than having to go on top.
It may, and `LayeredBase::slice` carries the argument: order is preserved because
`M` is equivalent to what it replaced at the same position; keys stay disjoint
because `M`'s keys are the union of its inputs'; and resolution still advances
monotonically because a value in `M` is some `L_m` value with `m ≤ j`, which has
no parent in `L_0..=L_m` and hence none below the splice. The third is the one
that would bite — without it, resolution would have to restart from the bottom
after every hit, making lookups O(layers²).

### The upgrade is one-way, and that is deliberate

Old catalogs read on the new binary with no migration — an absent `runs` field
synthesises a run set from `base_sequence` + `delta_chain_len`, and there is a
test for it. **The reverse does not work, and cannot be made to.**

A cold review of the implementation caught this, and it is worth stating exactly
rather than softening. `base_sequence` + `delta_chain_len` describe one base
followed by a contiguous chain. That is not a lossy description of a tiered stack;
it is not a description of one at all, so there is no value of those fields a
pre-run-set reader resolves correctly. Concretely: a merge that lands *below* the
top of the stack keeps the span it subsumed, so it is published at a later
sequence than its span ends. An old reader follows `chain()` to that sequence's
`puffin_path`, gets a different and already-subsumed object, assembles the stack
in the wrong order, and reports two merged components as disconnected. No error —
`#[serde(default)]` makes the new JSON parse cleanly.

Nor can an old reader be made to fail *loudly*. The obvious trick — a
`base_sequence` past `sequence`, giving an empty chain — makes
`open_base_from_catalog` error on an empty layer list, but makes
`hydrate_from_catalog` iterate zero times and return a healthy watermark over an
empty DSU. Silently worse. There is no single value that trips both paths.

So the fields are written best-effort for a chain-shaped stack,
`SnapshotMeta::format_version` marks the boundary, and the operational rule is:
**once a tiered merge has been committed, roll forward, not back.** Nothing in
this build reads the compatibility fields.

What *is* now checked, on every read, is the run set's own shape.
`SnapshotMeta::run_set` is fallible and rejects gaps, overlaps, backwards spans,
and a newest run that does not cover the snapshot's own sequence. That validator
existed as `RunMeta::adjacent_to`, was documented as guarding "the property that
breaks quietly", was asserted in three tests — and was called from no production
path at all until the review pointed it out.

Two smaller decisions worth recording:

- **Spans, not commit order, fix a run's place.** A merged run inherits the span
  of everything it subsumed. It also extends over the sequence that publishes it,
  because a merge commit carries no data files and the previous watermark — it
  adds no routing state — and without the extension the run set leaves a hole at
  its own sequence.
- **The depth ceiling now merges the lowest stretch available**, rather than
  falling back to a full-base rewrite. It stays a bounded merge: any stretch of
  `fanout` or more would have been caught by the level rule first.
- **A merge tick still commits its data batch**, at the sequence after the merge.
  It used to return early, which was tolerable when a merge fired roughly one tick
  in twenty-four; under tiering it fires on ~10% of ticks and up to three in a row,
  so each early return was holding the watermark back for a whole interval — a 3x
  worse worst-case recovery point, bought for a merge that took no lock and
  touched no buffered data. Also from the review.
- **Puffin objects carry a UUID**, as Parquet data files always have. Two workers
  can both believe they hold the lease for a sequence — that is the race
  `CommitOutcome::Conflict` settles — and both upload before either learns who
  won. Sharing a key lets the loser overwrite the winner's object *after* the
  winning commit, leaving a snapshot naming a run composed over a different stack.
  The flat policy healed that within a compaction cycle; a tiered high-level run
  can stay referenced for far longer.

## Backfill — corrected by measurement

An earlier version of this section claimed 2B links in ~1.8 hours, from the
**357k links/s** the DSU sustains with no base attached (`examples/throughput.rs`).
That figure does not survive contact with a layer stack, and the correction is
large.

Measured end to end (`examples/ceiling.rs`, 4-core / 15 GB box, fold every 5M
links, compact at 8 layers): **40M links in ~8.8 minutes including folds — ~76k
links/s effective**, with instantaneous ingest falling 385k → 62k/s as depth went
0 → 7. Extrapolating that rate alone puts 2B links at **~7 hours**, before
counting the compactions a real run of that size would do. Call it most of a day
on one node rather than an afternoon.

Two reasons, both from the depth measurements above:

- Ingest resolves through the stack, so it slows as the stack deepens.
- A fold is O(memtable × layers), not O(memtable) — the observed fold time rose
  6.5 s → 26.5 s for a constant 5M-link memtable.

Memory is still bounded by the fold trigger rather than by state, which was the
main claim and holds: RSS tracked ~2.5 GB at 40M links with a 5M-link trigger.

**This changes the verdict on a dedicated bulk path.** Sorting the historic edges,
running external-memory union-find and emitting one finished base skips per-event
DSU work *and* never builds a layer stack to fight, so it stays near the
layer-free rate throughout. Previously dismissed as "not worth building until 2
hours hurts"; at most-of-a-day it is worth building. It remains a batch tool
rather than part of the engine.

Alternatively, backfill through the streaming path with **depth kept low** (2–4)
and a large fold trigger, accepting more frequent compaction to keep ingest near
its ceiling. That is a tuning choice available today, and worth measuring before
building anything.

## Beyond: what limits 10B+

- **The shared tier is inherently single-writer.** A merge can connect any two
  nodes, so union-find cannot be sharded by key range without distributed
  coordination. That is fine here by a wide margin: 2k/s peak is ~0.6% of the
  measured 357k/s write capacity, so one writer serves 10B+ links. The overlay
  tier shards trivially by scope if it ever needs to.
- **Ids.** (002 is closed as not pursued; kept for the cap it names.)
  [002](002-dense-interning.md)'s u32 interning caps at 4.3B nodes and
  must not be used at this target; u64, or packed u48 if the two bytes matter.
- **The registry inflates the base permanently, and that is a *read* cost.**
  It is 55% of base bytes at a flat 12-byte `(root, scope)` stride — 41 GB of a
  75 GB base at 2B links. Crucially it is **never touched by a query**: the only
  caller of `scopes_for_root` is `apply_global`, so those pages are read by the
  writer alone (~7.5/s average, ~300/s peak) while competing with queries for page
  cache. Grouping by root, or replacing it with per-scope membership filters, takes
  the base to ~34 GB — the difference between not fitting and comfortably fitting
  in cache on a 64 GB box, and roughly halves compaction bytes.
- **Delta blob shape: real, cheap, low impact.** One Puffin blob per scope means
  per-layer overhead is O(scopes) rather than O(change), because the footer lists
  every blob at ~137 bytes. Measured (`examples/blob_overhead.rs`), a 60s delta at
  50 links/s dusted across 3000 tenants is **74% bookkeeping** — 556 KB carrying
  144 KB. The fix is a single combined overlay blob for delta layers,
  `(scope u32, node u64, root u64)` sorted by `(scope, node)`: ~4x smaller, blob
  count per delta from 3002 to 2.

  But note the ceiling on the payoff. Deltas are transient — compaction merges
  them away, and a compacted base's 3002-blob footer is ~450 KB on a 75 GB file.
  So this is 74% of a small number that never compounds: ~800 MB/day of writes
  instead of ~207 MB, and ~38 ms of extra cold-start footer parse at 24 layers.
  It also does not appear during backfill, where each delta is large and
  concentrated (nearer the 1% row of the measured table). It bites only in the
  specific regime of low absolute change spread thinly across many scopes.
- **Probing per-scope blobs instead of keeping a registry** only works with
  in-RAM membership filters. Without them it is ~3 ms per global merge (3000
  binary searches), which is 2% of a core at the 50/s average but nearly a whole
  core at the 2k/s peak, and worse cold.
- **Disk**: ~375 GB of base at 10B links before the registry fix, ~250 GB after.
  Unremarkable for local NVMe.

### Which of these is a read problem, and which a write problem

Worth stating plainly, because it drives the ordering above and is easy to get
backwards:

| | write cost | query latency | cold start |
|---|---|---|---|
| Registry blob | ~50% of delta payload, 55% of base | **none** (writer-only) | none |
| Per-scope blob count | 74% overhead on deltas | **none** per lookup | O(scopes x layers) footer parse |
| Layer count | **~1.3 us per link ingested per layer**, and folds at O(memtable x layers) | **~0.65 us per layer** | O(layers) files |

Only layer count threatens the query SLO, which is why tiering leads. At 50
links/s write amplification does not matter for its own sake — it matters through
what it leaves behind for readers, which is why the registry (permanent, in the
base) outranks blob shape (transient, in deltas) despite the smaller headline
percentage.

Nothing structural stands in the way of 10B; it is tiering plus constant factors.

## Invariants & tests

- I1/I6: the randomized model test runs across a tiering cycle, so a merge that
  promotes runs between levels must not change any answer.
- Disjoint keys must hold for merged runs exactly as for folded ones — assert it
  directly rather than trusting the argument.
- A lookup's probe count must stay bounded by level count as run count grows;
  pin it the way `narrowed_search_reads_at_most_one_block` pins the page bound.
