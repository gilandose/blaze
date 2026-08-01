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

**That last row used to be aspirational.** Building a table's sparse index samples
the first key of every 4 KiB block — one read per *page* — so doing it through the
mapping faulted the whole base in the moment it opened, and pinned it there:
`posix_fadvise(DONTNEED)` cannot reclaim a page any process has mapped. A
"disk-backed" base was therefore fully resident from startup. Indexes are now
built in one pass over the file instead, and a freshly opened 330 MB base sits at
**18% resident** rather than 100%, filling in from the lookups that actually need
it. See `examples/compaction_reader`.

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
```

### Do not derive pairs from links — derive them from nodes

`pairs_per_link` is **not** a workload constant. Sweeping graph density at 3000
scopes:

| links/node | pairs/link | base B/pair | index B/pair |
|---|---|---|---|
| 0.5 | 1.94 | 38.0 | 1.04 |
| 2 | **4.30** | 28.7 | 1.04 |
| 8 | 1.69 | 27.3 | 1.02 |
| 33 | **0.41** | 19.3 | 1.02 |

It swings **10x** and it is **non-monotonic**, peaking around 2 links/node. That
is the percolation threshold: below it most nodes sit in tiny components, at it
the giant component forms and nearly every node acquires a parent, and above it
new links increasingly join nodes that already share a root and create nothing.

State is bounded by **nodes and scope fan-out, never by links**:

```
shared pairs   <= distinct nodes
overlay pairs  <= distinct nodes x scopes per node
pairs          ~= nodes x (1 + scopes_per_node_with_a_distinct_root)
```

So size from your node count. A worked example: 1.1B nodes with a mean of 2.07
scopes per node (median 1) gives ~2.2-3.4B pairs, ~2.3-3.6 GB of heap, and
~63-119 GB of base. Knowing the link count alone would have told you nothing.

**Index heap is flat at 1.02-1.07 B/pair across every configuration tested** — which is
the 8 bits per key the filter is sized for, plus the sparse index. The spread in
the per-link column is entirely `pairs/link` moving underneath it.

**Base disk still varies 1.3x**, and the residual tracks scopes-per-root
monotonically (28.8 single-scope, 35.3 multi-scope, 37.3 when sparser spreads roots
further). That is the **registry** — the `(root, scope)` index. Pairs do not count
it, so it lands in the residual.

The registry is now delta-varint encoded (`--registry-encoding blocked`, the
default), which took a measured base from 133.6 MB to 81.0 MB — **39% off** — so
the base figures below, taken before that change, are conservative by roughly
25-40% depending on scope fan-out. The saving is largest where the residual was
largest, so it shrinks the 1.3x spread as well as the absolute number. Writing
`--registry-encoding flat` restores the old format if a reader needs it; see
`docs/design/009-registry-encoding.md`.

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

Design 006 sized 2B links at 75 GB of base assuming ~37 B/*link*. That framing
does not survive the density sweep above — the same link count spans roughly 20 GB
to 300 GB depending on how many nodes it touches. Size from nodes.

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
| `--edge-log` | unset | **production ingest** — see below | injection is refused while set |
| `--edge-log-batch` | 10 000 | ingest throughput | memtable granularity, nothing else |
| `--registry-encoding` | blocked | — | `flat` is 1.6x the base for a 2.4x faster merge notification |
| `--retention-interval-secs` | 3600 | rarer sweeps | storage grows at the amplification rate — see below |
| `--keep-snapshots` | 10 | more restore points | storage |
| `--keep-snapshots-hours` | 24 | more restore points | storage |
| `--retention-grace-secs` | 3600 | slower merges | storage; **lowering it is the dangerous direction** |
| `--allow-unsafe-commits` | off | **nothing** — see below | correctness, not performance |

**The gotcha:** `--fold-after-links` does nothing on a leader. The check is
`!force && links < fold_after_links`, and a leader always passes `force = true`
because it needs a layer to commit. On the writer, memtable size is governed by
`--flush-interval-secs`.

## Compaction and the page cache

A compaction sweep is the worst possible cache citizen: it reads every page of
every run it merges, exactly once, and never again. Read through the mapping,
all of it stays resident — and because those pages are the most recently touched
in the process, LRU prefers evicting the *query* working set over them. Recency
inversion, not a leak.

Sweeps therefore read through a file descriptor and `posix_fadvise(DONTNEED)`
the range afterwards. Measured on a 330 MB base:

| sweep | resident after | warm lookup |
|---|---|---|
| through the mapping | 330 MB (100%) | 0.32 -> 0.36 µs |
| through an fd | 82 MB (25%) | 0.34 -> 0.37 µs |

The second column is the one to read twice. **The query working set survives.**
Pages a lookup has touched are mapped into the process, and `DONTNEED` cannot
reclaim a mapped page — so the sweep gives back what it read and leaves what
queries are using, which is the behaviour `MADV_COLD` would provide if memmap2
exposed it. Nothing is tunable here and nothing needs to be; it is the default
and the only reason to know about it is if you are reading `mincore` output and
wondering why a compaction no longer moves the number.

## Where edges come in

Two shapes, and the difference is not mainly about speed.

**`POST /v1/edges` (and the gRPC equivalent) mints its own offset.** The worker
holds a counter and hands out the next number. That works, and it is what the
simulator and the tests use, but the numbering is *private to one process*. The
committed watermark is measured in offsets, so a snapshot saying "committed
through 4 001 337" only means something to the worker whose counter produced it.
A replacement worker reading that watermark is interpreting a number from a
sequence it never generated. It happens to work when exactly one worker ever
produces the stream, from an unbroken sequence of events, and it is silently
wrong otherwise.

**`--edge-log` takes the offset from the log.** Kafka assigns it once, at the
broker, and every consumer sees the same number against the same record. Now the
watermark means the same thing everywhere: restart is "resume after it", and the
records replayed are exactly the ones applied but never committed. This is the
shape a production deployment should have, and the reason the flag exists is
correctness before throughput.

The file log stands in for a topic honestly, because it has the properties that
matter: **offset = line number**, assigned by something other than the consumer,
stable across readers and restarts, and replayable by seeking. It does not
simulate broker election, rebalancing, or retention dropping the prefix from
under a slow consumer.

```bash
LINKS=50000000 OUT=edges.ndjson cargo run --release --example edge_log
blaze --edge-log edges.ndjson                      # streaming: follows the tail
blaze --edge-log edges.ndjson --edge-log-follow false   # batch: load, flush, serve
```

**Injection is refused (409 / `FAILED_PRECONDITION`) while `--edge-log` is set.**
An injected edge has no log position, so accepting one means minting an offset
into the space the log is already assigning. The two numberings would collide and
the watermark would stop meaning "everything up to here is in the log". Produce
to the log instead.

### What it costs per edge

| path | per edge | measured |
|---|---|---|
| `--edge-log` | **3.9 µs** | 2M records drained in 7.8s by the release binary |
| `POST /v1/edges` | 510 µs per connection | 0.51 ms p50, 0.86 ms p99, localhost |

Read that as a *structural* difference, not a benchmark of the HTTP server. The
server is fine — half a millisecond, and it scales across connections. But every
edge costs a request, so reaching the log path's rate means roughly 130
concurrent producers doing nothing but posting single edges. The log amortises
the same work over a 10 000-record poll.

The 3.9 µs figure is the ingest path only: `--flush-interval-secs` was set past
the run, so no flush or compaction happened inside the window. It is the ceiling,
not a sustained rate — the soak numbers below are what sustained looks like once
tiering and percolation are in play.

### One partition

blaze tracks a **scalar** watermark, so it consumes one ordered log. Against
Kafka that means a single-partition topic.

Multiple partitions are not blocked by anything fundamental — DSU merges commute,
so partitions would be independently correct — but the watermark would have to
become a map of partition to offset in `SnapshotMeta`, and every comparison
against it a per-partition comparison. Until that exists, the consumer takes one
partition and says so rather than interleaving two and producing a watermark that
means nothing.

## Retention

Nothing on the commit path deletes anything. A tick writes new objects and
publishes a snapshot naming them; the runs the merge consumed are simply no
longer named. So an unswept table does not grow at the rate of your data, it
grows at the *write-amplification* rate — `amp ≈ log_T(F)`, measured 4.96x at
the default fanout. On a real merged table the sweep reclaims **96% of run
bytes**; mid-soak the live figure was 80% and still climbing.

**Retention is on by default** (`--retention-interval-secs 3600`), so a worker
that has been up an hour will start deleting objects. Set it to `0` to keep the
old never-delete behaviour.

What a sweep keeps is a *reachability closure*, not a list:

1. Retain the newest `--keep-snapshots` snapshots, plus every snapshot committed
   within `--keep-snapshots-hours`.
2. Close that set over snapshot parentage — a legacy `SequencesOnly` snapshot
   carries no run set of its own and is only meaningful with the chain behind it,
   so retaining one retains its whole chain.
3. The live object set is every `puffin_path`, `data_files` entry and run named
   by anything in the closure.
4. Delete everything else under the table prefix — objects first, then the
   metadata that referenced them, so a crash mid-sweep leaves unreferenced
   objects rather than a snapshot pointing at nothing.

`--retention-grace-secs` is the safety property and the one number worth
understanding. Between a tick uploading its objects and committing the snapshot
that names them, those objects are reachable from nothing at all. Without a grace
window a concurrent sweep would collect an in-flight commit's own inputs, and the
commit would then publish a snapshot naming files that no longer exist. **The
grace window must exceed your slowest merge-and-commit.** An hour is generous for
a normal table; raise it if merges take longer than that, and understand that
lowering it trades a correctness margin for storage.

The sweep is leader-gated only to avoid duplicated work — it is idempotent and
safe to run from any worker — and runs on its own loop rather than inside the
tick, so it can never slow a commit down. A sweep that fails logs and retries
next period.

The thing to know if you are watching this: deleting a live run corrupts nothing
you can see. The mappings stay valid, the catalog stays parseable, and the loss
surfaces only when a worker hydrates from scratch — the one moment nobody is
watching. `tests/retention.rs` therefore asserts against a cold start rather than
against a byte count.

## The startup preflight

Not a tuning dial, but it is the one flag that can stop a worker booting, so it
belongs here.

Snapshot commits arbitrate between leaders with a single conditional put. If the
object store ignores the precondition, **two workers both succeed, both serve
topology consistent with what they wrote, and nothing downstream can tell** —
there is no error, no warning and no metric that moves. So every worker probes
its own store at startup, under its own bucket and prefix, and refuses to run if
the probe fails.

The probe writes a few dozen small objects under `{table}/_preflight/` and
deletes them again. It checks four things, and the fourth is the one that
matters: a fresh key accepts a conditional put; the same key then refuses one;
the first write is what is actually stored; and **eight simultaneous callers
produce exactly one winner**. A store can pass the first three by serializing
requests per key and still resolve a genuine race by last-write-wins.

If the preflight fails, the message names which property broke and what it means.
The fix is almost always the store, not blaze: an S3-compatible implementation
without `If-None-Match` support, or an older MinIO. `--allow-unsafe-commits`
starts anyway and warns every minute; use it only if you are certain the check is
wrong about your store, and never with more than one worker able to win an
election.

Worth knowing that this is not hypothetical. `s3s-fs`, a real S3 server
implementation, passes the sequential checks and fails the concurrent one —
its `PutObject` tests `path.exists()` and then writes, with nothing in between.

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
| `--filter-bits` | **high, if the base does not fit in cache** — see below | **high** — ingest throughput |
| `--max-delta-layers` | high is fine — 2k/s is 2% of capacity | **low** — depth taxes every link |
| `--inline-merges` | off — keep the memtable bounded | **on** — throttle ingest |

If you run one deployment for both, size for serving and override for the backfill
window.

**One correction to the obvious reading of that table.** Lowering `--filter-bits`
to reclaim heap is only right when the base *fits in page cache*. When it does
not, filters become **more** valuable, not less: they are heap and therefore always
resident, so a lookup probing ~7 runs has 6 of them answered from RAM without
touching the mapping, and the sparse index narrows the survivor to one 4 KiB block.
Drop the filters and all 7 probes can fault. At ~13% residency that is the
difference between roughly one page fault per lookup and seven.

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
