# 004 — Routing Parquet & DataFusion enrichment

> **Depends on a shape that no longer exists.** This doc keys the routing Parquet
> on a single base (`routing-<base_seq>-<uuid>.parquet`, written "at compaction
> time"), but [006](006-tiered-compaction.md) removed the privileged base — there
> are runs at levels, and full-base merges are months-apart events rather than
> hourly. The writer side needs rethinking against the run set before this is
> implementable. The SQL sketch also predates [010](010-stream-position.md): the
> edge schema now leads with a `partition` column.


## Problem

The Puffin routing blobs are exact and fast for blaze itself, but opaque to
every external engine (custom blob types are container-level interop only).
Analysts need the edge history *with the component id as a column* — e.g.
"roll up all edges by live graph_id for tenant 7" — in Trino, Spark,
DuckDB, or DataFusion.

## Design

### Part 1 — routing table materialization (writer side)

At compaction time (001), additionally emit the base routing map as plain
Parquet:

```text
routing/routing-<base_seq>-<uuid>.parquet
  scope_id: u32      -- 0 = global layer
  node:     u64
  root:     u64      -- canonical lowest graph id (layer-local)
```

Sorted by `(scope_id, node)`, zstd, row group per scope band — the same
pruning story as the edge files. Referenced from `SnapshotMeta` as
`routing_parquet` alongside `puffin_path`. Cost at target profile: ~32 GB
per compaction, hourly; retained for the last K compactions (lifecycle
rule). Cadence and retention configurable; analytics tolerance for staleness
is typically minutes-to-hours, which compaction cadence already matches.

### Part 2 — the enrichment query

The two-layer lookup is two hops of `LEFT JOIN` + `COALESCE`, because
snapshot pairs are fully resolved (depth-1) — no recursion:

```sql
WITH g AS (
  SELECT e.*, COALESCE(gr.root, e.src) AS global_root
  FROM edges e
  LEFT JOIN routing gr
    ON gr.scope_id = 0 AND gr.node = e.src
)
SELECT g.*, COALESCE(sr.root, g.global_root) AS graph_id
FROM g
LEFT JOIN routing sr
  ON sr.scope_id = g.scope_id AND sr.node = g.global_root;
```

This is `overlay(s).find(global.find(x))` expressed relationally; the
`COALESCE` encodes "fall back to global root / self". Runs unchanged on any
engine that reads Parquet.

### Part 3 — blaze-side DataFusion example

`examples/enrich_datafusion.rs`: registers `data/*.parquet` as `edges`,
decodes the snapshot's Puffin blobs into a `MemTable` (skipping Part 1 for
small states), runs the query above, writes `enriched/*.parquet`. Also
demonstrates the UDF alternative: register `graph_id(scope_id, node)`
over a hydrated `ScopedForest` for exactness with one fewer join.

### Snapshot-pairing semantics (the correctness rule)

Two distinct, both-useful semantics — the job must pick one explicitly:

- **`graph_id_at_ingest`**: join edge files against the routing table of
  the *same* catalog snapshot (offset ranges ≤ that snapshot's watermark).
  Reproducible; suitable for point-in-time audits.
- **`graph_id_current`**: join *all* history against the *latest* routing
  table — "where do these edges route today". This is the blueprint's
  O(1)-topological-routing-for-reads promise. Not reproducible over time by
  construction; name the column accordingly.

The example implements both behind a flag; documentation makes the naming
convention normative.

## Invariants & tests

- I2/I6: enrichment output for randomized state equals `scope_root` queries
  against the hydrated forest, row for row (property test over the SQL
  path).
- Determinism: `graph_id_at_ingest` re-runs byte-identically for a fixed
  snapshot.

## Effort

Part 1 (writer emission + metadata + retention): ~1 day, depends on 001's
compaction hook. Parts 2–3 (example + docs + property test): ~1 day;
DataFusion is a dev-dependency-sized addition (or an example-only feature
flag to keep the default build lean).
