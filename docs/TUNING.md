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
| **Filters** | ~2.1 bytes per link (~1.2 single-scope) | **no — heap** |
| **Sparse index** | ~0.2% of base bytes | **no — heap** |
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

## Sizing: measure, do not extrapolate

**Only one of these terms is a usable constant.** Measured through the real
flusher at 2M links (`examples/detached_merge`, `--filter-bits 8`):

| scopes/edge | index heap | **B/link (heap)** | base on disk | B/link (disk) |
|---|---|---|---|---|
| 1 scope total | 2249 KB | **1.15** | 60 MB | 31.5 |
| 1–3 of 100 | 4025 KB | **2.06** | 128 MB | 67.1 |
| 1–3 of 3000 | 4044 KB | **2.07** | 131 MB | 68.7 |

**Index heap is predictable**: ~1.2 B/link single-scope, ~2.1 B/link once edges
carry several scopes. It saturates by 100 scopes because what drives it is *scopes
per edge*, not scope count — going 100 → 3000 does not change how many overlay
entries an edge creates. Halves at `--filter-bits 4`; drops to just the sparse
index (~0.14 B/link) at 0.

**Disk bytes per link is not a constant, and an earlier version of this guide
wrongly gave one.** It said 38 B/link, taken from a single `ceiling` run. Measured
across configurations it ranges **31.5 to 74.4**, and the variation is not
explained by stack depth — holding scopes fixed and sweeping the depth ceiling
gives 70.3 / 69.2 / 74.4 at ceilings of 6 / 12 / 24, i.e. flat. It also is not
explained by per-run Puffin footer overhead, which would scale with scope *count*
and so would separate 100 from 3000; it does not. The remaining candidates are
graph density and fold size, and **we do not currently have a model that predicts
it.** Measure your own workload before sizing disk.

Heap floor at `--filter-bits 8`, using 2.1 B/link:

| links | heap floor | at `--filter-bits 4` | at 0 |
|---|---|---|---|
| 100M | 0.21 GB | 0.11 GB | 0.014 GB |
| 500M | 1.0 GB | 0.53 GB | 0.07 GB |
| 2B | 4.2 GB | 2.1 GB | 0.28 GB |
| 10B | 21 GB | 11 GB | 1.4 GB |

Plus `leader memtable = interval_s x rate x 50 bytes` — 6 MB at 60 s and 2k/s, so
never the constraint on a writer.

## Worked configurations

**Small box, modest state** — 4 GB RAM, ≤500M links:

```
--routing-base disk --filter-bits 8 --flush-interval-secs 60
--tier-fanout 4 --max-delta-layers 8
```
1 GB heap, leaving ~3 GB of page cache. Whether that is 15% or 50% of your base
depends on the disk-bytes question above, so measure — but either way queries
fault, which is fine if your SLO is milliseconds rather than microseconds. Low
fanout keeps each merge small so it does not evict the working set, and the
measured table shows it also buys ~3x on lookups.

**Small box, large state** — 4 GB RAM, billions of links:

```
--routing-base disk --filter-bits 0 --flush-interval-secs 60
--tier-fanout 4 --max-delta-layers 8
```
`--filter-bits 0` is the only thing that makes this fit: heap drops from ~4.2 GB
to ~0.28 GB at 2B links. It costs ~35% ingest throughput (measured 88k → 57k
links/s), which is irrelevant at 50/s and fatal during a backfill — see below.

**Production serving** — 64 GB RAM, 2B links:

```
--routing-base disk --filter-bits 8 --flush-interval-secs 60
--tier-fanout 10 --max-delta-layers 24
```
4.2 GB heap, leaving ~59 GB of page cache. Design 006 sized the base at 75 GB for
2B links, which would be ~79% resident — but that figure predates the measurements
above and assumes ~37 B/link, at the bottom of the observed range. If your
workload lands nearer 70 B/link the base is ~140 GB and you are at ~42% resident,
so validate before committing to an instance type.

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
| `--filter-bits` | 8 | ingest speed, query latency | heap: ~2.1 B/link at 8, ~0.14 at 0 |
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
