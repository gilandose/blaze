# blaze — next-phase designs

Design documents for the next phase of blaze, sized against the **target
production profile**:

| Parameter | Value |
|---|---|
| Tracked nodes (worst case: all participate in links) | ~2B |
| Event rate | ~3k/s peak |
| Tenant scopes | ~3,000 + global |
| Query SLO | sub-millisecond component lookups |

Two consequences of this profile shape everything below:

1. **Throughput is a non-problem.** 3k events/s is ~0.5% of one worker's
   measured ingest capacity (~550k events/s). Partitioning across workers is
   *not* on this roadmap; one HA group of 3 replicas suffices indefinitely.
2. **State size is the whole problem.** At the measured ~200 B/link, 2B links
   is ~400 GB — and a full snapshot would take ~8 minutes under the union
   lock. Every design below attacks state cost, snapshot cost, or restart
   cost.

## Documents and priority order

| # | Design | Attacks | Status |
|---|---|---|---|
| [001](001-delta-snapshots.md) | Delta snapshots & compaction | snapshot stall + payload | designed |
| [002](002-dense-interning.md) | Dense id interning | memory (200→~45 B/link) | designed |
| [003](003-disk-backed-base.md) | Disk-backed routing base (LSM) | memory + cold start | **implemented** |
| [004](004-analytics-enrichment.md) | Routing Parquet + DataFusion enrichment | analytics interop | designed |
| [005](005-union-tier.md) | Union tier (`all` view) & shared/global naming | semantics gap | designed |

Recommended implementation order: **001 and 002 together** (fold in 005's
rename and union tier — same files) (they touch the
same core and are jointly required to fit 2B links on sane hardware), then
003, then 004. Cost impact at the target profile: current design would need
~1 TB-RAM instances (~$9.5k/mo for 3 replicas); after 001+002, 128–256 GB
instances (~$1.9–3.7k/mo); after 003, ~64 GB + NVMe (~$1.5k/mo).

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
