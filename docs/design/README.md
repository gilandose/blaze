# blaze — next-phase designs

Design documents for the next phase of blaze, sized against the **target
production profile**:

| Parameter | Value |
|---|---|
| Tracked nodes (worst case: all participate in links) | ~2B, designed to go well beyond |
| Link rate | **~50/s average, ~2k/s peak** |
| Dominant workload | **backfill of historic edges**, not the live stream |
| Tenant scopes | ~3,000 + global |
| Query SLO | sub-millisecond component lookups |

Three consequences of this profile shape everything below:

1. **Throughput is a non-problem for the live stream, with less margin than
   this once claimed.** 2k/s peak is ~0.6% of the 357k links/s the DSU sustains
   *with no base attached* — but that figure does not survive contact with a
   layer stack. [006](006-tiered-compaction.md#backfill) retracts it: the
   effective rate is **~76k links/s**, so a 2B-link backfill is **~7 hours**, not
   the ~1.8 this section used to claim. One HA group of 3 replicas still suffices
   for the live rate; the sharding question is genuinely open at 10B+, and the
   number that made it look closed was wrong. **If it is ever answered, the
   answer is scope sharding** — see the note below.
2. **State size was the whole problem, and 003 changed its shape.** The 160
   B/link in RAM that motivated most of these designs — ~320 GB at 2B links —
   described a heap-resident DSU. Under [003](003-disk-backed-base.md) pairs live
   in mmap'd runs and the unreclaimable floor is **~1.07 bytes per pair**, so the
   hard budget at 2B links is ~4 GB rather than ~320. What is left is a
   *page-cache* budget, which sets latency rather than whether you run. This is
   why [002](002-dense-interning.md) is closed: it attacked a term that no longer
   dominates.
3. **The low change rate is itself a design constraint.** It makes full-base
   compaction absurdly expensive per unit of change (rewriting 75 GB to absorb
   7.5 MB), which is why [006](006-tiered-compaction.md) — not the stall fixes —
   is the current priority. Stall *duration* is cheap here: queries never take
   the union lock, and 50/s buffers away a 30-minute pause.

### Note: if we shard, we shard on scope

Not a design yet — a direction, recorded so the "sharding is open" line above
has an answer attached rather than sitting as an open question nobody has
thought about.

**The shape.** Partition the *scopes* across shards; every shard also computes
the full global tier. Two properties from the existing model make this nearly
free to reason about:

- `scope_root(s, x)` depends only on `G_global ∪ G_s`, so a shard holding the
  global DSU plus the scopes it owns answers **entirely locally**. No
  cross-shard reads.
- DSU merges commute, so every shard independently applying the same global
  edge stream converges on the identical global DSU with **no coordination**.
  This is the same property that makes multi-partition consumption correct in
  [010](010-stream-position.md).

**Why it is the right axis.** The percolation cliff measured in `examples/soak`
— throughput halving as roots collapsed 28.5M → 16.9M — is driven by `k`, the
mean scopes per root, going 2.90 → 5.07: each global merge notifies every scope
in the registry keyed on the roots it joined. Sharding scopes N ways divides `k`
by N. It attacks the exact term that causes the cliff.

**The ceiling.** Every shard consumes every global edge, so per-shard work is
`g + (1 − g)/N` for a global share `g`, and speedup is capped at **1/g**
regardless of N. Amdahl, with the global tier as the serial fraction:

| global share | ceiling | at N=10 |
|---|---|---|
| 30% (`examples/soak` default) | 3.3x | 2.7x |
| 15% (`SimulatorConfig` default) | 6.7x | 4.4x |
| **2-5% (stated production profile)** | **20-50x** | **6.9-8.5x** |

**So the harnesses are pessimistic, and by a lot.** Both defaults are 3-15x the
global share this is actually expected to see. The percolation cliff, the
effective ingest rate behind the ~7-hour backfill, and every soak number are
therefore measured against a workload considerably harsher than production. The
no-globals arm — **25M links in 4.2 min, ~99k links/s, no cliff** — is much
closer to the real profile than the 30% arm is. Anyone re-measuring should set
`GLOBAL_PCT` to the real share first; the numbers in these docs are a floor, not
an estimate.

**What it costs.** Shared pairs are replicated on every shard. At the measured
~1.94 pairs/link roughly half the state is shared, so total state goes as
`N x 0.5 + 0.5` — about 2.5x at N=4, 5.5x at N=10. Replicate the hot small tier,
partition the large cold one; the usual broadcast-dimension trade.

**What it needs.** Each shard is its own table (own prefix, catalog,
put-if-absent, leader) — no new commit machinery. The stream shape is already
[010](010-stream-position.md)'s: global edges on partitions every shard consumes,
scoped edges partitioned by scope, and per-partition positions already handle a
consumer reading a subset with absent-means-zero covering the rest. Open
problems: scope rebalancing (moving a scope means moving its overlay) and a
scope-to-shard directory for query routing.

The one thing that would have conflicted outright — [005](005-union-tier.md)'s
`all` tier, a view spanning every scope — is **closed**, partly for this reason.
A cross-scope view is exactly what this scheme cannot answer on one shard, so
keeping both would have meant keeping a design at odds with the only scaling
axis available.

## Documents and priority order

Operator-facing sizing and flag guidance is [../TUNING.md](../TUNING.md); the
docs below are the design rationale behind those knobs.

| # | Design | Attacks | Status |
|---|---|---|---|
| [001](001-delta-snapshots.md) | Delta snapshots & compaction | snapshot stall + payload | **implemented** — parts superseded by 006/007 |
| [002](002-dense-interning.md) | Dense id interning | memory (200→~45 B/link) | **not pursued** — superseded by 003 |
| [003](003-disk-backed-base.md) | Disk-backed routing base (LSM) | memory + cold start | **implemented** |
| [004](004-analytics-enrichment.md) | Routing Parquet + DataFusion enrichment | analytics interop | designed — **needs rework** against the run set |
| [005](005-union-tier.md) | Union tier (`all` view) & shared/global naming | semantics gap | **not pursued** |
| [006](006-tiered-compaction.md) | Size-tiered compaction + backfill sizing | write amplification, layer count | **implemented** |
| [007](007-compaction-execution.md) | Where compaction runs (detached / process / deployment) | compaction's cost to serving | **implemented** (detached in-process) |
| [008](008-rocksdb-counterfactual.md) | Could this have been built on RocksDB? | whether the storage tier had to be written | evaluation |
| [009](009-registry-encoding.md) | Registry encoding (delta-varint in indexed blocks) | 25-40% of base bytes | **implemented** |
| [010](010-stream-position.md) | Stream position (per-partition offsets + stream identity) | snapshot metadata cannot describe Kafka | **implemented** |

001, 003, 006, 007, 009 and 010 are in. **002 and 005 are closed**, for
different reasons:

- **002** — 003 removed its premise by moving pairs out of the heap, and the
  ~1.07 B/pair that remains is membership filter sized at 8 bits per *key*, so
  narrowing ids does not touch the term that binds.
- **005** — the `all` view conflicts with the only scaling axis available: a
  cross-scope view is exactly what a scope-sharded deployment cannot answer
  locally. The `Global` → `Shared` rename is dropped separately, as not worth a
  breaking change to the enum, the proto and the REST contract.

**004 is the only design still open**, and it is analytics interop rather than a
scaling change. It needs rework before it is implementable: its writer keys the
routing Parquet on a single base, and 006 removed the privileged base.

Nothing on this list is now load-bearing for scale. The open questions that are
live are measurements and tests rather than designs — see the note on sharding
above, and the two items 006 asks for that do not yet exist (a probe-count bound,
and a re-measure of the per-layer microsecond constant).

Cost impact at the target profile: an all-RAM design would need ~256–512 GB
instances; with 003 shipped it is ~64 GB + NVMe (~$1.5k/mo for 3 replicas). The
instance is sized by page cache and headroom rather than by state — but note
that an earlier version of this paragraph claimed "~300 MB resident at 2B
links", which `ARCHITECTURE.md` retracts by name: that is the *heap*, and RSS
climbs toward `min(base size, available RAM)`. Size the box for the base you
want cached, not for the heap floor.

The base figures throughout 006 and 007 assume **~37 bytes per link**. See
[../TUNING.md](../TUNING.md#sizing-count-pairs-not-links): that framing does not
survive a density sweep, the same link count spans roughly 20 GB to 300 GB, and
the durable unit is the **pair**, not the link. Every figure below derived from
"75 GB at 2B links" inherits the error.

## Invariants every design must preserve

These are load-bearing properties of the merged system; the randomized
model tests enforce most of them and must keep passing:

- **I1 — Scope correctness**: `scope_root(s, x)` equals the BFS component
  minimum over `G_global ∪ G_s`.
- **I2 — Canonical roots**: unions keep the lowest *original u64* graph id
  as root, in both layers. (002 must not compare dense ids.)
- **I3 — Lock-free reads**: the query path takes no lock shared with the
  union path beyond shard read locks.
- **I4 — S3 is the source of truth**: local disk and RAM are caches; losing
  a node loses no committed data.
- **I5 — Exactly-once visibility**: whatever files exist, state becomes
  visible only via the put-if-absent catalog commit; watermarks advance
  monotonically; sequences are dense. Since
  [010](010-stream-position.md), "monotonically" means **per-partition
  dominance**: the watermark is a `StreamPosition`, and the scalar `watermark`
  field is legacy. Enforced by `SnapshotMeta::advances_from`
  (`a_position_that_goes_backwards_is_rejected`).
- **I6 — Cold-start fidelity**: a worker hydrated from the catalog answers
  exactly what the writer answered at the committed watermark, and keeps
  composing with new merges (registry rebuild).
- **I7 — Stream identity** ([010](010-stream-position.md)): a
  snapshot's offsets are only interpretable against the stream they were
  committed against. A worker configured for a different stream refuses to
  start rather than resuming at offsets that mean something else.
  `tests/stream_identity.rs` drives the real binary through both branches;
  `--allow-stream-change` is the override, for a stream renamed or moved with
  its offsets intact.

**On I3.** No test asserts it. `queries_see_no_torn_state_while_folding` checks
that readers never observe a torn answer, which is a correctness property, not
the lock property — `scope_root` simply does not take the union lock, and that
is held by code review rather than by a test. Stated here so the list is not
read as uniformly enforced.
