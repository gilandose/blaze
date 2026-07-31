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

1. **Throughput is a non-problem, by a wide margin.** 2k/s peak is ~0.6% of one
   worker's measured ingest capacity (357k links/s through the DSU, 308k/s with
   Arrow). Partitioning across workers is *not* on this roadmap; one HA group of
   3 replicas suffices indefinitely. A 2B-link backfill is ~1.8 hours.
2. **State size is the whole problem.** At the measured 160 B/link in RAM, 2B
   links is ~320 GB. Every design below attacks state cost, snapshot cost, or
   restart cost.
3. **The low change rate is itself a design constraint.** It makes full-base
   compaction absurdly expensive per unit of change (rewriting 75 GB to absorb
   7.5 MB), which is why [006](006-tiered-compaction.md) — not the stall fixes —
   is the current priority. Stall *duration* is cheap here: queries never take
   the union lock, and 50/s buffers away a 30-minute pause.

## Documents and priority order

Operator-facing sizing and flag guidance is [../TUNING.md](../TUNING.md); the
docs below are the design rationale behind those knobs.

| # | Design | Attacks | Status |
|---|---|---|---|
| [001](001-delta-snapshots.md) | Delta snapshots & compaction | snapshot stall + payload | designed |
| [002](002-dense-interning.md) | Dense id interning | memory (200→~45 B/link) | designed |
| [003](003-disk-backed-base.md) | Disk-backed routing base (LSM) | memory + cold start | **implemented** |
| [004](004-analytics-enrichment.md) | Routing Parquet + DataFusion enrichment | analytics interop | designed |
| [005](005-union-tier.md) | Union tier (`all` view) & shared/global naming | semantics gap | designed |
| [006](006-tiered-compaction.md) | Size-tiered compaction + backfill sizing | write amplification, layer count | **implemented** |
| [007](007-compaction-execution.md) | Where compaction runs (detached / process / deployment) | compaction's cost to serving | **implemented** (detached in-process) |
| [008](008-rocksdb-counterfactual.md) | Could this have been built on RocksDB? | whether the storage tier had to be written | evaluation |
| [009](009-registry-encoding.md) | Registry encoding (delta-varint in indexed blocks) | 25-40% of base bytes | designed — **next** |

006 and 007 are in. Recommended order for what is left: **009** (the registry
encoding, which supersedes the restructure 006 sketched — measured 4.8-7.1x
against that proposal's 1.5-2.3x), then 002 folded in with 005's rename and union
tier, then 004. Note 002's u32 interning caps at 4.3B nodes and must be widened to u64 or a
packed u48 to serve the "well beyond 2B" goal.

Cost impact at the target profile: an all-RAM design would need ~256–512 GB
instances; with 003 shipped it is ~64 GB + NVMe (~$1.5k/mo for 3 replicas), and
measured resident set is ~300 MB at 2B links, so the instance is sized by page
cache and headroom rather than by state.

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
  monotonically; sequences are dense.
- **I6 — Cold-start fidelity**: a worker hydrated from the catalog answers
  exactly what the writer answered at the committed watermark, and keeps
  composing with new merges (registry rebuild).
