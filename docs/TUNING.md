# Tuning blaze to a box

How to pick settings for the hardware you have. Design docs explain *why* the
knobs exist; this says what to set them to.

Every constant here was measured on a **4-core / 15 GB dev box** with 3,000 scopes
and 1–3 scopes per edge. Ratios travel; absolute numbers do not. Re-measure on
your hardware with the harnesses at the bottom — it takes minutes.

## The model: what is actually in RAM

Four terms, and only two of them are yours to lose sleep over.

| term | size | reclaimable? |
|---|---|---|
| **Filters** | ~1.07 bytes per **pair** | **no — heap** |
| **Sparse index** | ~0.2% of base bytes (inside the 1.07) | **no — heap** |
| Leader memtable | `flush_interval x arrival_rate x 50 B` | no, but tiny |
| Follower memtable | `fold_after_links x 50 B` | no, but tiny |
| Mapped runs | up to the full base on disk | **yes — clean page cache** |

The last row is why `RSS` misleads. Measured at 15M links, RSS was 719 MB against
a 0.58 GB base — so ~590 MB of it was clean file-backed pages the kernel drops
under pressure. The unreclaimable part was **~30 MB**. On a small box you do not
OOM, you take more page faults; correctness is unaffected and latency degrades
with cache-miss rate.

So there are two budgets:

- **Heap budget** (hard): filters + sparse index + memtable. Exceed it and you OOM.
- **Page-cache budget** (soft): whatever is left. It sets query latency, not
  whether you run.

## Sizing: count pairs, not links

**Size against `(node -> root)` pairs, not edges ingested.** An edge that unions
two nodes already in the same component creates no state at all, so as a graph
saturates an ever-larger fraction of ingest is free. Bytes-per-*link* therefore has
an unbounded denominator and drifts with density; bytes-per-*pair* does not.

Measured at 2M links (`examples/detached_merge`, `--filter-bits 8`), sweeping both
scope shape and graph density:

| workload | pairs/link | base B/link | **base B/pair** | index B/link | **index B/pair** |
|---|---|---|---|---|---|
| 1 scope | 1.09 | 31.5 | 28.8 | 1.15 | **1.06** |
| 1-3 of 100 | 1.93 | 67.1 | 34.7 | 2.06 | **1.07** |
| 1-3 of 3000 | 1.94 | 68.7 | 35.3 | 2.07 | **1.06** |
| 1-3 of 3000, 2x sparser | 1.81 | 66.1 | 36.4 | 1.94 | **1.07** |
| 1-3 of 3000, 5x sparser | 1.74 | 65.0 | 37.3 | 1.86 | **1.07** |

So:

```
index heap  ~= 1.07 bytes per pair       (at --filter-bits 8; halves at 4, ~0.14 at 0)
base disk   ~= 29-37 bytes per pair      (upper end when edges carry several scopes)
pairs       ~= pairs_per_link x links    <- measure this once for your workload
```

**Index heap is flat at 1.07 B/pair across every configuration tested** — which is
the 8 bits per key the filter is sized for, plus the sparse index. The spread in
the per-link column is entirely `pairs/link` moving underneath it.

**Base disk still varies 1.3x**, and the residual tracks scopes-per-root
monotonically (28.8 single-scope, 35.3 multi-scope, 37.3 when sparser spreads roots
further). That is the **registry** — the `(root, scope)` index, which design 006
measured at 55% of base bytes and flagged for restructuring. Pairs do not count it,
so it lands in the residual. Restructuring the registry would both shrink the base
and make this constant.

An earlier version of this guide gave a single 38 B/link figure from one `ceiling`
run. Across configurations that number ranges 31.5 to 74.4, and chasing it as a
per-link constant was the mistake — stack depth does not explain it (sweeping the
ceiling 6/12/24 gives 70.3/69.2/74.4, flat) and neither does per-run Puffin footer
overhead (it scales with scope *count*, which would separate 100 from 3000, and
does not). Changing the denominator does.

Heap floor by pair count, at `--filter-bits 8`:

| pairs | heap floor | at 4 | at 0 |
|---|---|---|---|
| 100M | 0.11 GB | 0.05 GB | 0.014 GB |
| 1B | 1.1 GB | 0.53 GB | 0.14 GB |
| 4B | 4.3 GB | 2.1 GB | 0.56 GB |
| 20B | 21 GB | 11 GB | 2.8 GB |

At ~1.9 pairs/link on a multi-scope workload, 2B links is ~3.8B pairs — so ~4 GB
of heap and ~130 GB of base. Note that is nearly double design 006's 75 GB
estimate, which assumed ~37 B/*link*; validate against your own `pairs/link`
before committing to an instance type.

Plus `leader memtable = interval_s x rate x 50 bytes` — 6 MB at 60 s and 2k/s, so
never the constraint on a writer.

## Worked configurations

**Small box, modest state** — 4 GB RAM, ≤500M links:

```
--routing-base disk --filter-bits 8 --flush-interval-secs 60
--tier-fanout 4 --max-delta-layers 8
```
~1 GB heap (950M pairs at 500M links), leaving ~3 GB of page cache against a
~33 GB base — under 10% resident, so measure your own `pairs/link` — but either
way queries
fault, which is fine if your SLO is milliseconds rather than microseconds. Low
fanout keeps each merge small so it does not evict the working set, and the
measured table shows it also buys ~3x on lookups.

**Small box, large state** — 4 GB RAM, billions of links:

```
--routing-base disk --filter-bits 0 --flush-interval-secs 60
--tier-fanout 4 --max-delta-layers 8
```
`--filter-bits 0` is the only thing that makes this fit: heap drops from ~4 GB to
~0.5 GB at 2B links (3.8B pairs). It costs ~35% ingest throughput (measured 88k → 57k
links/s), which is irrelevant at 50/s and fatal during a backfill — see below.

**Production serving** — 64 GB RAM, 2B links:

```
--routing-base disk --filter-bits 8 --flush-interval-secs 60
--tier-fanout 10 --max-delta-layers 24
```
~4 GB heap (3.8B pairs at 1.07 B/pair), leaving ~59 GB of page cache. Design 006
sized the base at 75 GB assuming ~37 B/*link*; at the measured ~35 B/*pair* and
1.9 pairs/link it is closer to **130 GB**, so ~45% resident rather than ~79%.
Validate `pairs/link` on your own workload before committing to an instance type —
it is the one number everything else scales from.

**Backfill** — whatever box, ingest rate is the objective:

```
--filter-bits 8 --tier-fanout 4 --max-delta-layers 6 --inline-merges
--fold-after-links 5000000
```
Opposite of the serving config on three of four dials, which is the point. Keep
filters (they buy ingest throughput). Keep depth *low*, since ingest resolves
through the stack and pays ~1.3 µs per link per layer. And `--inline-merges` so
ingest cannot outrun compaction and bury you in runs.

## The flags

| flag | default | raise it for | costs |
|---|---|---|---|
| `--filter-bits` | 8 | ingest speed, query latency | heap: ~1.07 B/pair at 8, ~0.14 at 0 |
| `--tier-fanout` | 10 | less rewriting (`amp ≈ log_T(F)`) | depth: `(T-1)·log_T(F)` runs to probe |
| `--max-delta-layers` | 24 | fewer merges | every path pays per run |
| `--flush-interval-secs` | 60 | fewer commits | leader memtable **and** your RPO |
| `--fold-after-links` | 1M | fewer follower folds | follower heap only — see gotcha |
| `--inline-merges` | off | ingest that cannot outrun compaction | tick blocks for the whole merge |
| `--routing-base` | ram | — | `disk` is the low-memory mode |

**The gotcha:** `--fold-after-links` does nothing on a leader. The check is
`!force && links < fold_after_links`, and a leader always passes `force = true`
because it needs a layer to commit. On the writer, memtable size is governed by
`--flush-interval-secs`.

## Measured trade curves

Depth, on identical state (`examples/layer_depth`):

| layers | ingest/s | lookup |
|---|---|---|
| 1 | 216,963 | 1.01 µs |
| 8 | 109,487 | 3.10 µs |
| 16 | 65,850 | 4.90 µs |

≈1.3 µs per link per layer, ≈0.65 µs per lookup per layer.

Filters, through the real flusher at 2M links (`examples/detached_merge`):

| `--filter-bits` | index heap | ingest/s |
|---|---|---|
| 8 | 4044 KB | 88k |
| 4 | 2125 KB | 72k |
| 0 | 273 KB | 57k |

Detached vs inline merges, 3M links at matched schedule:

| | peak memtable | p99 tick | max tick |
|---|---|---|---|
| `--inline-merges` | 661,468 | 1200 ms | 3258 ms |
| default (detached) | 111,168 | 296 ms | 650 ms |

Depth end to end, 3000 scopes at 2M links — note the p99 tick moves *opposite* to
lookup and ingest, because a deeper stack merges less often:

| fanout / ceiling | ingest/s | lookup | p99 tick |
|---|---|---|---|
| 4 / 6 | 96k | 0.88 µs | 351 ms |
| 4 / 12 | 89k | 0.75 µs | 265 ms |
| 10 / 24 (defaults) | 80k | 2.41 µs | 156 ms |

That is the whole trade in one table: the shipped defaults buy the smoothest tick
latency and pay ~3x on lookups and 17% on ingest. At 50 links/s that is the right
corner; during a backfill it is the wrong one.

## Serving and backfill pull opposite ways

Worth stating because three flags invert between them:

| | serving (50/s) | backfill (saturated) |
|---|---|---|
| `--filter-bits` | low is fine — heap matters more | **high** — ingest throughput |
| `--max-delta-layers` | high is fine — 2k/s is 2% of capacity | **low** — depth taxes every link |
| `--inline-merges` | off — keep the memtable bounded | **on** — throttle ingest |

If you run one deployment for both, size for serving and override for the backfill
window.

## What to watch

- **Run count over `--max-delta-layers`.** The flush loop warns. It means merges
  are not keeping up, and nothing else will notice — every path degrades smoothly
  rather than failing.
- **Peak memtable links.** Should track `flush_interval x rate` on a leader. If it
  does not, a tick is stalling.
- **Tick duration p99.** A slow tick is watermark not advancing, i.e. your RPO.
- **Heap versus RSS.** Rising RSS with flat heap is page cache doing its job, not
  a leak.

## Measuring on your own hardware

```bash
# memory floor and tick latency, real Flusher with concurrent ingest
FILTER_BITS=8 LINKS=3000000 RATE=100000 SCOPES=3000 \
  cargo run --release --example detached_merge

# write amplification, flat vs tiered at matched depth
LINKS=2000000 FOLD_LINKS=4000 MAX_LAYERS=12 FANOUT=10 \
  cargo run --release --example tier_amplification

# what depth costs, in isolation
cargo run --release --example layer_depth

# run until something gives; prints RSS, base size, ingest, lookup, compaction
LINKS=40000000 SCOPES=3000 FOLD_LINKS=5000000 MAX_LAYERS=8 \
  cargo run --release --example ceiling
```

`ceiling` predates tiering and uses flat compaction, so its RSS figures are
pessimistic for the shipped policy — it sweeps the whole base each cycle where a
tiered merge touches a subset.
