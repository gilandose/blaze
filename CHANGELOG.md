# Changelog

Notable changes, newest first. Versions follow [semver](https://semver.org);
while the major is `0`, a minor bump may change on-disk formats or CLI flags —
each one below says whether it does.

## Unreleased

### Added

- **`forest.member_heap_bytes` in `/v1/stats`** and `PuffinBase::member_index_bytes()`,
  so the member index's RAM cost is visible rather than inferred. The split is
  the point: the written index is evictable page cache, its in-heap block
  offsets and filters are **0.2-3.6% of the mapping** and scale with total state,
  and the memtable merge edges are ~30 B/link but bounded by the fold trigger
  (~30 MB at the default follower trigger, ~3.6 MB on a leader at 2k/s). Sizing
  guidance in `docs/TUNING.md`.

- **Delta-varint member index** (`storage::members`, blob types
  `blaze-shared-members-v2` / `blaze-overlay-members-v2`), the registry's blocked
  layout applied to the parent-ordered pairs. **+34% of run bytes, 7.1 B/pair**,
  down from +77% and 16.1 B/pair for the fixed-stride form. Both encodings stay
  readable and a stack may mix, so the change needed no rewrite.
- **Membership filters over the member index's parent keys**
  (`blaze-*-members-filter-v1`), sized per distinct parent rather than per pair.
  A downward walk probes mostly leaves, and this makes rejecting one a cache line
  instead of a binary search.

- **Member index (design 011), behind `--member-index` (off).** Answers *who else
  is in this component*: `GET /v1/scopes/{scope}/members/{node}?cap=` and
  `BlazeService.GetMembers`, plus `ScopedForest::members`. Two halves — a
  parent-ordered Puffin blob emitted by folds and by tiered compaction, and
  merge-edge tracking in the memtable — walked downward together, capped, and
  returned as `Complete` or `Truncated` so a hub can never be mistaken for a
  small component.
  - **New blob types** `blaze-shared-members-v1` and `blaze-overlay-members-v1`.
    Additive: a reader that does not know them ignores them, and a run written
    without them is unchanged.
  - **A stack containing one unindexed run refuses the query** rather than
    under-reporting. Watch `forest.members_available` in `/v1/stats` — it goes
    false at the next base swap, not at startup.
  - **Not retroactive**: memtable merge edges are recorded as unions happen, so
    the flag needs a restart to take effect.
  - **Costs +77% of run bytes** (+62.6 MB on 81.0 MB, 16.1 B/pair, measured by
    `examples/registry_shape`). The design estimated +20-40% by assuming a
    delta-varint encoding that was not built; that correction is recorded in
    `docs/design/011-member-index.md`.
- `tools/cc_oracle.py` now grades **member sets** against scipy in all three
  modes, not just roots. Strictly stronger: dropping one child per parent leaves
  every root correct and breaks ~60% of the member sets.
- `examples/member_bench` — the cost and latency of `--member-index`, swept
  across the percolation threshold. Ingest **-1.4% to -2.7%** with a mapped base
  (-21% to -31% all-RAM, where the DSU is the whole cost of `apply`); query
  latency linear in the answer at ~0.11 us/member in the heap and ~1.1 us/member
  from a mapped run.

### Fixed

- **The cap bounded the answer but not the work.** In a flattened run a
  component's root has every member as a direct child, and the reader
  materialised all of them before the walk consulted the cap — so a `cap = 1000`
  query on a hub cost O(component) rather than O(cap): **1.1-1.5 ms** on a mapped
  run, tracking graph size. Child fetches now take a budget, and the same query
  is **64 us**, matching what the in-heap walk costs.

  Two wrong budgets are recorded alongside the right one in `Walk::expand`:
  "room remaining" silently drops members when a fetched child is already
  visited (which the two-level scoped walk causes by construction), and
  "decode it all, then truncate" made the blocked encoding *slower than the
  fixed-stride table it replaced* on a hub record.
- **`members` in a tenant scope returned a single member** when the scope's
  overlay was larger than the cap, instead of `cap` members. The seed stage's
  truncation was OR'd into the outer walk before it ran, and the walk stops as
  soon as that flag is set, so it bailed after its first seed. The flag was
  never needed: a truncated seed walk holds exactly `cap + 1` members by
  construction, so feeding it to a walk capped at `cap` truncates correctly on
  its own. Every truncation test queried the global scope, which has one stage;
  `examples/member_bench` found it at 2.5 links/node, 1153 times in one sweep.
  Regression test: `a_scoped_component_over_the_cap_returns_the_whole_cap`.

## [0.1.0] — 2026-08-01

First tagged release. Everything below landed before it; the sections group the
work by what it does rather than by the order it merged.

### The engine

- **Layered scoped DSU.** A shared union-find plus sparse per-scope overlays,
  keyed by shared root, with a registry mapping `root -> {scopes}` so a global
  merge notifies only the scopes that reference it rather than broadcasting to
  thousands. Roots are canonical — lowest original graph id wins, in both layers
  — which is a contract, not just a stable representative.
- **Arrow-native ingest** into a memtable that seals into segments on each tick,
  one row per visible scope so a Parquet reader can prune by `scope_id`.
- **REST and gRPC** serving from the same shared state.

### Storage

- **Iceberg-flavoured layout**: Parquet data files, Puffin DSU sidecars, and an
  immutable snapshot metadata JSON per commit. The commit is a **put-if-absent**,
  which is the sole leader-arbitration primitive.
- **Disk-backed routing base** (`--routing-base disk`): mmap the latest snapshot,
  keep only post-snapshot merges in memory. Restarts serve in seconds and the
  resident set is page cache rather than heap.
- **Size-tiered compaction**, running detached from the ingest path, with write
  amplification `≈ log_T(F)` — measured 4.96x at the default fanout.
- **Delta-varint registry encoding** in offset-indexed blocks
  (`blaze-registry-v2`): 4.8–7.1x smaller on the registry, taking a measured base
  from 133.6 MB to 81.0 MB. The Puffin blob type *is* the interface, so a stack
  may mix encodings and an older reader falls back rather than failing.
- **Compaction sweeps read through a file descriptor** and `posix_fadvise`
  the range afterwards, so a merge gives back what it read and leaves the query
  working set alone. Fixed alongside a larger bug: building the sparse index
  through the mapping faulted the whole base in at open and pinned it there. A
  freshly opened 330 MB base now sits at 18% resident rather than 100%.
- **Retention** (`--retention-interval-secs`, on by default): a reachability
  closure over snapshot parentage, deleting objects before metadata. Measured
  **96% of run bytes reclaimed** on a merged table, with a cold start afterwards
  asserted identical.

### Ingest from a log

- **`--edge-log`** consumes a newline-delimited JSON log where **the log assigns
  the offset**, rather than the worker minting one. This is a correctness change
  before it is a throughput one: the committed watermark is measured in offsets,
  so a locally minted numbering is one only that process can interpret, which is
  exactly what a failover breaks.
- Point it at a **directory** of `partition-<n>.ndjson` and it consumes a
  partitioned stream, checkpointing each partition independently. A topic that
  gains a partition needs no migration — absent means zero, which is where a new
  partition genuinely starts.
- At-least-once redelivery below the committed position is skipped, so rows land
  once. Measured **3.9 µs per edge** against 510 µs per connection over HTTP.
- Injection is refused (409 / `FAILED_PRECONDITION`) while log-backed, because an
  injected edge has no log position.

### Correctness and durability

- **Graded against scipy.** `tools/cc_oracle.py` checks `scope_root` against
  `scipy.sparse.csgraph.connected_components` on published graphs, exact root per
  node per scope. It runs the real binary, restarts it mid-stream, and queries
  back through the API. Runs on every push to `main`, and on pull requests
  carrying the `e2e` label.
- **Mid-tick failure injection**, commit races, and cold-start fidelity.
- **A put-if-absent conformance suite** run at startup against the real bucket and
  prefix, fail-closed. It found a real non-conformance in `s3s-fs`, an
  independent S3 implementation: it passes the sequential checks and fails the
  concurrent one.
- **The leader elector is executed, not just compiled.** The k8s Lease protocol
  is driven against a stand-in API server that implements `resourceVersion`
  optimistic concurrency, covering acquisition, renewal, expiry takeover, and a
  lost race reading as a clean loss.

### Known limits

- **Single-writer per table.** A consumer group splitting partitions across two
  blaze workers is *detected* (the per-partition monotonicity check rejects it at
  commit) but not supported.
- **Throughput is not linear in link count.** At the percolation threshold —
  around 1.8 links/node — the giant component forms, roots collapse, and
  throughput roughly halves. Measured 34.2k links/s early against 15.6k/s at 30M
  links on a 30%-global mix. Size from nodes and scope fan-out, never from links;
  see `docs/TUNING.md`.
- **Linux is the tested platform.** The `posix_fadvise` paths are compiled out
  elsewhere and that fallback has never been executed.
- **No backfill/bulk-load path.** A large historical load goes through the same
  ingest as everything else.
- Designs 001, 002, 004 and 005 remain designed rather than implemented; 002
  (dense id interning) is the next material change to the memory story.
