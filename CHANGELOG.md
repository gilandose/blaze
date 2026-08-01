# Changelog

Notable changes, newest first. Versions follow [semver](https://semver.org);
while the major is `0`, a minor bump may change on-disk formats or CLI flags —
each one below says whether it does.

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
