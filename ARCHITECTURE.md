# blaze architecture

```mermaid
flowchart LR
    FH[Edge firehose<br/>simulator / Kafka / Kinesis] --> P[Ingest pipeline<br/>single writer]
    API_IN[POST /v1/edges] --> P
    P --> F[ScopedForest<br/>global DSU + scope overlays]
    P --> B[Arrow EdgeBuffer<br/>sealed segments]
    Q[Axum API<br/>lock-free reads] --> F
    FL[Flush loop<br/>every N secs] -->|leader only| OS[(Object storage)]
    B --> FL
    F -->|snapshot| FL
    OS --> C[Snapshot catalog<br/>put-if-absent commit]
    L[K8s Lease election] --> FL
    C -->|hydrate on boot| F
```

Each worker runs four cooperating pieces over shared in-memory state:

1. **Ingest pipeline** (`src/ingest/pipeline.rs`) — a single task drains the
   event channel, assigns monotonic offsets, applies DSU merges, appends rows
   to Arrow builders.
2. **Query API** (`src/api`) — Axum handlers doing lock-free DSU finds; no
   ingest lock is ever taken on the query path.
3. **Flush loop** (`src/storage/flush.rs`) — seals Arrow buffers each tick;
   the leader persists and commits.
4. **Leader election** (`src/ha`) — Kubernetes Lease (feature `k8s`) or
   static assignment.

## The multi-tenant scoping problem

Edges carry visibility: **global** (all tenants) or a small set of the
~3000 **scopes**. Scope `s`'s connectivity is defined over
`G_global ∪ G_s` — so tenant views share the global backbone but never see
each other's private edges.

Naive designs fail at this cardinality:

- *One DSU per scope*: a single global edge must be applied to ~3000
  structures — O(scopes) per global event.
- *Normalize-at-read overlays* (overlay keyed by global roots, resolved
  through `global.find` at query time): subtly wrong — when a global merge
  absorbs root `B` into `A`, overlay state keyed at `B` becomes unreachable
  from lookups that start at the live root `A`.

### Layered DSU with merge notifications (`src/core/scoped.rs`)

```mermaid
flowchart TB
    GE["global edge (u,v)"] -->|"union(u,v)"| G
    SE["scope edge (u,v) @ scope 7"] -->|"union(find_g(u), find_g(v))"| O7
    SE -.->|"register both roots"| R

    subgraph forest ["ScopedForest"]
        G["<b>Global DSU</b><br/>globally-visible edges only<br/>root = lowest graph id"]
        R["<b>Registry</b><br/>global root → {scopes holding<br/>overlay state on it}"]
        subgraph overlays ["sparse per-scope overlays (~3000)"]
            O7["<b>overlay 7</b><br/>elements = global roots<br/>as of insert time"]
            O2999["overlay 2999"]
        end
    end

    G -->|"merge absorbs root B into A"| R
    R -->|"fix-up overlay.union(B,A)<br/>ONLY scopes registered on B<br/>never a 3000-way broadcast"| O7

    subgraph query ["query path (lock-free, read-only)"]
        X["node x"] -->|"global.find_ro"| GR["global root g"]
        GR -->|"overlay(s).find_ro(g)<br/>miss ⇒ g itself"| ROOT["scope_root =<br/>lowest graph id in<br/>scope s's component"]
    end
    G -.-> GR
    O7 -.-> ROOT
```

Reading the diagram: writes enter at the top (global edges touch one
structure; scope edges touch one overlay plus two registry entries), the
registry turns global merges into targeted fix-ups instead of broadcasts,
and queries compose the two layers left-to-right with the overlay lookup
falling back to the global root on a miss.

- **Global DSU**: holds globally-visible edges only.
- **Scope overlay DSU** (sparse, per scope): elements are node ids that were
  *global roots when the scope edge arrived*. A scope edge `(u,v)` unions
  `find_g(u)` and `find_g(v)` in the overlay.
- **Registry** `global_root -> {scopes}`: every overlay insertion registers
  its keys. When a global union absorbs root `B` into `A`, only the scopes
  registered on `B` get a fix-up `overlay.union(B, A)` — recording that the
  two overlay elements now denote the same global component — and `B`'s
  registrations migrate to `A`.

Query composition (lock-free, read-only):

```text
scope_root(s, x) = overlay(s).find_ro(global.find_ro(x))   // falls back to global root
connected(s, u, v) = scope_root(s, u) == scope_root(s, v)
```

**Canonical roots — lowest graph id wins.** Both layers union by minimum id,
so `scope_root` returns the smallest graph id in the component as seen by
that scope: deterministic, and only ever decreasing as merges land (trending
toward stable ids for long-lived graphs). The property survives the overlay
because every global fix-up inserts the new, lower global root as an overlay
element, keeping the overlay class min equal to the true component min. The
randomized test asserts `scope_root == BFS component minimum` on every
check.

Cost model: a global merge pays only for scopes that actually reference the
absorbed root (typically zero or a handful, never a 3000-way broadcast);
scope edges pay O(α) in their own overlay. Correctness is enforced by a
randomized test that checks every answer against a per-scope BFS reference
model, including across snapshot/hydrate cycles.

### Concurrency model

- All **unions** (global, scoped, fix-ups) serialize behind one mutex —
  mutation rate is bounded by ingest, and a mutex uncontended by queries
  costs nanoseconds.
- **Write-path finds** compress via path-halving; halving writes are safe
  under races because a parent pointer is only ever replaced by another
  ancestor. **Query-path finds** (`find_ro`) are read-only walks — taking
  only shard read locks so query load cannot starve the single writer — with
  one repair write if a walk exceeds a depth threshold, capping pathological
  chains.
- Queries racing a multi-step union may observe pre-fix-up state for
  microseconds — the read-your-own-stream guarantee is eventual within a
  tick, which is the right trade for a streaming engine.

## Data model

### Event model (`src/core/types.rs`)

```rust
EdgeEvent {
    src: u64,                 // graph/node id
    dst: u64,
    visibility: Global        // every scope sees it
              | Scoped([u32]) // listed scopes only; [] or containing 0
                              // normalizes to Global; sorted + deduped
    event_time_ms: i64,
    props: Option<String>,    // opaque JSON, carried end to end
}
```

Scope ids are `u32` with `0 = GLOBAL_SCOPE`. Node ids are sparse `u64`;
nothing is paid for ids that never appear in an edge.

### In-memory state

```mermaid
classDiagram
    class ScopedForest {
        global: Dsu
        overlays: DashMap~ScopeId, Dsu~
        registry: DashMap~NodeId, HashSet~ScopeId~~
        union_lock: Mutex
        apply(EdgeEvent)
        scope_root(scope, node) NodeId
        snapshot() ForestSnapshot
        hydrate(ForestSnapshot)
    }
    class Dsu {
        parents: DashMap~NodeId, NodeId~
        find(x) NodeId
        find_ro(x) NodeId
        union(u, v) Option~Merge~
    }
    class EdgeBuffer {
        active: ArrowBuilders
        sealed: Vec~Segment~
        append(offset, EdgeEvent)
        seal_active() Segment
        drop_committed(watermark)
    }
    class Segment {
        batch: RecordBatch
        min_offset: u64
        max_offset: u64
    }
    ScopedForest *-- Dsu : global + per-scope
    EdgeBuffer *-- Segment
```

Key representation choices: a node absent from `parents` is a root (or a
never-seen singleton — identical semantics); overlay elements are node ids
that were global roots at insert time; the registry is the merge-notification
index. `ForestSnapshot` is the neutral exchange form:
`{ global: Vec<(node, root)>, scopes: Vec<(scope_id, Vec<(node, root)>)> }`
with every pair fully resolved (depth 1) at capture time.

### Warehouse layout (object storage)

```text
<table_prefix>/                          e.g. graph/edges/
├── data/part-<seq12>-<uuid>.parquet     edge rows (immutable)
├── puffin/dsu-<seq12>.puffin            routing sidecar (immutable)
└── metadata/snap-<seq12>.json           atomic commit point (put-if-absent)
```

### Edge Parquet schema (one row per event × visible scope)

| column | type | notes |
|---|---|---|
| `offset` | u64 | stream position; replay/dedupe key |
| `src`, `dst` | u64 | edge endpoints |
| `scope_id` | u32 | 0 = global; rows sorted by `(scope_id, src)` for pruning |
| `event_time` | timestamp(ms) | event time |
| `props` | utf8, nullable | opaque JSON |

zstd-compressed, one row group per sealed segment.

### Puffin sidecar (`src/storage/puffin.rs`, `codec.rs`)

```text
container:  "PFA1" | blob₀ … blobₙ | "PFA1" | footer JSON | len:u32 LE | flags:u32 | "PFA1"
blob types: blaze-global-dsu-v1                  (one)
            blaze-scope-dsu-v1 {scope-id: "N"}   (one per active scope)
payload:    count:u64 LE, then count × (node:u64 LE, root:u64 LE)
            sorted by node — binary-searchable in place / via mmap
```

Roots in the payload are canonical (lowest graph id in the component as of
this snapshot). Unknown blob types are ignored on read — the forward-compat
hook the delta design (docs/design/001) extends with `*-delta-v1` types.

### Snapshot metadata (`metadata/snap-*.json`)

```json
{
  "sequence": 42,               // dense, monotonically increasing
  "committed_at_ms": 1753000000000,
  "watermark": 379601,          // highest event offset covered (inclusive)
  "data_files": [ { "path", "rows", "bytes", "min_offset", "max_offset" } ],
  "puffin_path": "graph/edges/puffin/dsu-000000000042.puffin",
  "committer": "worker-pod-abc"
}
```

The watermark is the contract tying the three artifacts together: data
files cover offsets ≤ watermark, the Puffin routing map reflects exactly
those events, and followers prune buffered segments ≤ watermark.

## Ingest and buffering (`src/ingest`)

Events are exploded to one Arrow row per visible scope
(`offset, src, dst, scope_id, event_time, props_json`, global = scope 0), so
Parquet gets a plain `scope_id` column for predicate pushdown. On every
flush tick the active builders rotate into an immutable **sealed segment**
tagged `[min_offset, max_offset]`, sorted by `(scope_id, src)` for row-group
clustering.

Offsets are the replication contract: with a real log (Kafka/Kinesis) they
are the log's offsets and identical on every replica; the built-in simulator
assigns them locally.

## Persistence (`src/storage`)

Every tick, every worker: seal + read `catalog.latest()` + drop sealed
segments `<= watermark` (followers garbage-collect what the leader already
committed). The leader additionally:

1. writes sealed segments as one **Parquet** file
   (`data/part-<seq>-<uuid>.parquet`, zstd, row group per segment);
2. snapshots the forest into a **Puffin** file
   (`puffin/dsu-<seq>.puffin`) — spec-compliant `PFA1` container
   (`src/storage/puffin.rs`), one `blaze-global-dsu-v1` blob plus one
   `blaze-scope-dsu-v1` blob per active scope, each a sorted
   `(node, root)` pair table for O(1) topological routing without replay;
3. commits `metadata/snap-<seq>.json` with **put-if-absent** — the atomic
   publish. A lost race (leadership handoff) leaves harmless orphan files,
   exactly like uncommitted Iceberg data files, and segments re-flush next
   tick: at-least-once writes, exactly-once visibility.

On boot a worker hydrates the forest from the latest snapshot's Puffin blobs
(`hydrate_from_catalog`) and resumes offsets at the committed watermark —
recovery cost is proportional to live topology, not history. The catalog is
deliberately shaped like an Iceberg commit; swapping in an Iceberg REST
catalog changes `SnapshotCatalog` only.

## High availability (`src/ha`)

All replicas ingest and serve. `LeaderElector` decides who commits:
`StaticElector` for standalone/tests, and Lease-based election
(`coordination.k8s.io/v1`, feature `k8s`) renewing at a third of the lease
duration, with expired-lease takeover and `resourceVersion` CAS so two
candidates cannot both win. Failover safety: the catalog's put-if-absent is
the last line of defense even if two workers briefly both believe they lead.

## Capacity planning (measured)

Memory is paid per *tracked link* — a global merge or a scope-overlay union —
never per id: the node space is sparse `u64` and untouched nodes are free.
Measured on the reference workload (15M events, 30M node space, 3000 scopes,
15% global):

- **~200 bytes per tracked link** (DashMap parents + overlay + registry):
  15M links = ~3 GB resident. A 64 GB worker sustains ~250M links; the
  registry's per-root scope sets are the first thing to slim if more is
  needed.
- **Full snapshot cost is the practical per-worker ceiling**, not memory:
  15M links snapshot in ~3.5s *holding the union lock* (ingest stalls),
  encode in ~2.2s off-lock, and produce a ~256 MB Puffin payload. Cost is
  linear, so ~50M links ≈ 12s stall per flush tick — the comfortable
  envelope with 60s full snapshots is **tens of millions of tracked links
  per worker**.
- Beyond that, the format is ready for **delta snapshots**: Puffin blobs are
  sequence-numbered, so a flush can write only pairs changed since the last
  snapshot with periodic full compaction, removing both the stall and the
  rewrite. That, or shard workers by node-id range.

## Disk-backed routing base (`--routing-base disk`)

Committed routing state can be served from an **mmap'd Puffin file on local
disk** instead of the heap, making RAM cost O(hot window) instead of O(state).
The Puffin payloads the flusher already writes are sorted, fixed-stride
`(node, root)` tables, so the same file is directly usable as an on-disk
index: map it once, answer a lookup with a binary search over a byte range.

```mermaid
flowchart LR
    OS[(Object storage<br/>committed Puffin)] -->|read-through cache| NV[["local NVMe<br/>routing-base-N.puffin"]]
    NV -->|mmap, binary search| BASE["<b>base</b> (immutable)<br/>shared + per-scope + registry blobs"]
    ING[Ingest since compaction] --> MEM["<b>memtable</b><br/>ScopedForest DSU maps"]
    BASE --> C{{"composed_root(x) =<br/>memtable.find(base_root(x))"}}
    MEM --> C
    C --> Q[Query API]
    MEM -->|compaction re-emits<br/>composed state| OS
```

**The invariant that makes it exact:** every mutation resolves its operands
through the composed path *before* touching the memtable, so a memtable key is
always a composed root — a node the base stores no parent for. That is why one
base probe plus one memtable walk settles a lookup with no re-probing, and why
a node that already has a memtable parent can skip the base entirely. It also
means merge fix-ups must consult the base: a scope whose overlay class lives
only on disk still has to be notified when a shared root it references is
absorbed, which the base's `blaze-registry-v1` blob (`root -> scopes`, sorted,
binary-searchable) answers in one probe.

### Bounding the cold-page cost: the sparse index

A naive binary search over the mapping is a latency trap at scale. At 2B pairs
it is ~31 probes striding up to ~16 GB, and the first ~26 of them land on
distinct cold pages — ~150–250 µs of page faults for one answer, nearly all of
it spent walking levels small enough to keep in RAM for free.

So each table carries a **sparse in-RAM index**: the first key of every 4 KiB
block of entries. A lookup binary-searches that `Vec` (no faults), narrows to
one block, and only then touches the mapping — over 4 KiB of *contiguous*
bytes, so 1–2 mapped pages regardless of how the payload is aligned inside the
Puffin file. `narrowed_search_reads_at_most_one_block` pins that bound as a
test, because a probe whose base fits in page cache cannot observe it. The
index costs one `u64` per block: 0.2% of the table it indexes, so ~32 MB to
index a 16 GB shared tier, reported as `index_bytes` in `/stats`. The mapping
is also advised `MADV_RANDOM`, since default readahead prefetches pages a
binary search will never visit — except during a compaction sweep, which
re-advises its own byte range `MADV_SEQUENTIAL` for the duration.

### The fold: why a cold start's 51 MB is not a steady state

Compaction only *reads* the forest, so on its own it does nothing for resident
heap: a worker starts with a small memtable and then accumulates for its entire
life. Cold start being cheap is an initial condition, not a bound.

So the flush loop **folds**: compact, write the resulting Puffin file locally,
map it, adopt it as the new base, and drop the memtable — all in one step.
`ScopedForest::compact_and_fold` holds the union lock across the whole thing,
because a two-call version would lose anything applied in between. Folds are
triggered by memtable size (`--fold-after-links`, default 1M) rather than by
the flush clock, since that is the knob that trades write amplification against
resident heap. **Every worker folds, leader or not** — a follower serves from
the same structures and grows at the same rate, so leader-only folding would
relocate the leak rather than fix it. `folds` in `/stats` is the signal: if it
stops advancing while `global_links` climbs, heap use is unbounded again.

The fold replaces the entire tier (base + memtable + registry) behind one
atomic pointer swap rather than clearing the maps in place. That is not a
stylistic choice: `DashMap::clear` is not atomic across shards, so a concurrent
`find_ro` could walk a half-cleared chain and stop at an intermediate node,
returning a non-root as a component id. Under RCU a reader that loaded the
previous generation keeps resolving against the previous base *and* its intact
memtable — which by construction gives the same answers — and that generation
is freed when the last such reader drops it. Queries are never blocked, and
`queries_see_no_torn_state_while_folding` runs four reader threads through a
live fold to keep it that way.

### Folds append; they don't rewrite (design 001)

A fold that rewrites the base costs time and bytes proportional to *total
state* — 3113 ms and 125 MB for a 125 MB base, which extrapolates badly. So a
fold instead writes only what the memtable itself contributes, as a **new layer**
over the mappings already open: measured **~400 ms and 12.5 MB** for the same
300k links. That file is also what the leader commits, so a flush publishes a
delta rather than the whole map.

**The memtable is the dirty set.** Nothing tracks what changed, because every
fold empties the memtable — so its key set already *is* the nodes re-rooted
since the last fold, and each key's composed root is its final value. A separate
dirty-set structure would only add something that can drift out of step with the
data it describes.

`LayeredBase` presents base + deltas as one `RoutingBase`, and lookups stay exact
because **layer keys are disjoint**: every layer is produced by resolving through
everything beneath it, so its keys are composed roots of the layers below. At
most one layer holds a parent for a node, and following that parent can only
continue in a *newer* layer — so a resolution visits each layer at most once.
Compaction k-way merges the layers, which is what keeps the compaction read from
buffering anything.

### What layer depth actually costs

Measured on identical state, varying only the number of layers it was folded
into (`examples/layer_depth.rs`, 8M links / 3000 scopes):

| layers | ingest/s | vs 1 layer | lookup µs | vs 1 layer |
|---|---|---|---|---|
| 1 | 252,969 | 1.00× | 1.22 | 1.00× |
| 2 | 170,318 | 1.49× | 2.12 | 1.73× |
| 4 | 112,005 | 2.26× | 3.85 | 3.16× |
| 8 | 63,733 | 3.97× | 7.44 | 6.10× |
| 16 | 43,643 | 5.80× | 10.79 | 8.84× |

Depth taxes **three** paths, not one, and an earlier draft of this document got
two of them wrong:

- **Queries**: ~+0.65 µs per layer.
- **Ingest**: ~+1.3 µs per link per layer, because `apply` resolves its operands
  through the composed path before touching the memtable. Appending a layer is
  trivial; every write *after* that pays for it. This was previously described as
  "trivial (append)".
- **Folds**: `stream_memtable` resolves every memtable key, so a fold is
  **O(memtable × layers)**, not O(memtable). Observed in the ceiling run as fold
  time rising 6.5 s → 26.5 s across depth 0 → 6 for a constant 5M-link memtable
  and a constant 185 MB of output.

Both per-layer costs are linear in depth, which is what the O(layers) resolution
predicts — but the constant is ~2× what a smaller probe suggested (0.65 vs
0.26 µs), because a wider node space misses cache more.

`--max-delta-layers` is the dial, defaulting to 24. For the production profile
that default is fine: 50 links/s is 0.1% of even the depth-24 ingest capacity, and
~16 µs lookups are far inside the SLO. For a **backfill**, where ingest rate is
the whole point, it is badly wrong — see the backfill note below.

The fix that removes the trade rather than balancing it is a per-layer membership
filter, so a miss costs one probe instead of a binary search per layer. It should
be a *blocked* filter (all of a key's bits in one cache line): a classical bloom
with k=7 over a 6 MB filter is ~7 cache misses, no better than the binary search
it replaces. With that, depth stops mattering much and compaction can be rare —
which is why this now outranks tiering on ratio, though tiering is still needed to
stop depth growing without bound.

A worker whose newest layer was folded **locally but never committed** (any
follower, or a leader that lost a commit race) must not then commit a delta: the
committed chain would be missing those layers, and a cold start would silently
reconstruct incomplete topology. Such a worker compacts and commits a full base
instead — `LocalLayers::committed_through` tracks it and a test pins it.

### Compaction streams; it never materializes

Compaction runs under the union lock, which makes any O(state) allocation there
an ingest stall. So `ScopedForest::compact_into` emits the composed state to a
`SnapshotSink` as an ordered stream — base pairs walked as an ascending
sequential scan, merge-joined against the memtable's (small, sorted) key set —
and `codec::compact_to_blobs` writes each fixed-stride blob in place, patching
in its count header at the end. Nothing per-node is ever collected, and the
output is already in the sorted order the format wants, so no sort either.
`ForestSnapshot` still exists for RAM-mode callers and tests; the flush path
does not build one.

Measured (3M links / 3000 scopes / 113 MB base, release build):

| | all-RAM | mmap base |
|---|---|---|
| Cold start (to first correct answer) | 3284 ms (read+parse+hydrate) | **3.6 ms** (mmap + footer index + sparse index) |
| Resident for committed state | 479 MB | **108 MB** (touched pages; OS-reclaimable) + 0.2 MB index |
| `scope_root` lookups/s (1 thread) | 2.78M (0.36 µs) | 1.30M (0.77 µs) |
| Compaction, heap above the output | +57 MB (3.2M-pair snapshot) | **+0 MB** (streamed) |
| Compaction wall time | 1432 ms (collect) | **756 ms** (stream) |
| Fold 300k links, rewriting the base | n/a | 3113 ms, 125 MB written |
| Fold 300k links, as a delta layer | n/a | **~400 ms, 12.5 MB written** |
| Lookups/s, base only | — | 1.23M (0.81 µs) |
| Lookups/s, base + 3 delta layers | — | 534k (1.87 µs); see the depth table for the attributed per-layer cost |

Cold start is O(blob count) rather than O(pairs), so the gap widens linearly
with state — the reason a multi-gigabyte base serves in milliseconds. Warm
lookups cost roughly 2× a heap lookup and rise to ~10–50 µs on a cold NVMe
page; the sparse index is what keeps that "a page fault", singular. The
compaction intermediate that streaming removes is the same 16 bytes/pair that
would be ~32 GB at 2B links. Durability is unchanged (invariant I4): the local
file is only ever a read-through cache of an object-storage snapshot, written to
a temp path and renamed so a torn download can never be mapped, and the mapping
is read-only.

What still scales with state, named so it is not mistaken for solved:

- **Compaction**, which still rewrites everything, though it no longer holds the
  union lock (`storage::compact` merges the committed files instead).
- **Read *and write* amplification per delta layer** (~0.65 µs per lookup,
  ~1.3 µs per link ingested, and folds at O(memtable × layers)) until per-layer
  membership filters land. Measured above.
- **The Puffin file itself**, buffered in RAM before it is written and
  uploaded. Streaming it to disk and doing a multipart upload would remove
  this; it is only invisible today because a fold already dominates.
- **The sparse index**, ~0.2% of the base — the deliberate price of bounding
  cold-page faults.
- **The registry buffer built during compaction**, 12 bytes per overlay
  endpoint, because it must be sorted by root *across* scopes and so cannot
  stream without an external sort.

## Scaling to billions of links

Extrapolated from the measured per-unit constants above, not estimated:
**1.068 pairs per link**, **35 B/pair** in the base file (16 B pair + ~19 B of
registry), **0.2%** sparse index, **0.91 µs/pair** compaction, **160 B/link** if
held in RAM instead, and **308k links/s** ingest.

Sizing target: **~50 links/s average, ~2k/s peak**, mostly arriving as a backfill
of historic edges.

| links | pairs | base file | process RSS (disk mode) | RSS if all-RAM | compaction, 1 core | registry sort buffer |
|---|---|---|---|---|---|---|
| 100M | 107M | 3.8 GB | ~110 MB | 16 GB | 1.6 min | 2 GB |
| 1B | 1.07B | 38 GB | ~180 MB | 160 GB | 16 min | 20 GB |
| 2B | 2.14B | 75 GB | ~300 MB | 320 GB | 32 min | **41 GB** |

**Memory is genuinely flat.** At 2B links the process holds the sparse index
(150 MB), one flush interval of memtable (~30 MB at 3k events/s), the Arrow
buffer (~20 MB) and the delta layers' indexes (<1 MB) — call it 300 MB against
320 GB for the same state in the heap. Page cache then uses whatever RAM the box
has as a *cache*: the SLO is met at a 0% hit rate (1–2 cold NVMe faults, ~10–100
µs), so cache size buys throughput, not correctness. A 16–32 GB instance serves
2B links.

**Steady-state writes are flat too**, because they scale with change, not state:
7.6 MB and ~0.26 s per tick at 3k events/s, whatever the base weighs — about
11 GB/day of deltas.

**Local disk**: base 75 GB + 24 deltas ≈ 75 GB, but compaction needs the old and
new base at once, so provision ~2×. An `i4i.2xlarge` (1.7 TB NVMe) is ample.

### Where it actually breaks — sized to the real workload

The workload is **~50 links/s average, ~2k/s peak**, with the bulk of the data
arriving as a **backfill of historic edges**. That changes which cost hurts, and
two conclusions an earlier draft of this section got wrong are worth stating
directly:

- **Compaction stall duration is not the problem.** Queries never take the union
  lock, so a stall delays only ingest. At 50/s a 30-minute compaction queues ~90k
  events (~5 MB of Arrow buffer) and offsets are the source of truth — a lag
  spike, not an outage. Frequency and write amplification are what matter.
- **Low change rate is what breaks the layer stack**, not high. Compacting on
  layer count (60) fires hourly and rewrites a 75 GB base to absorb 7.5 MB;
  compacting on delta bytes (25% of base) fires every ~120 days, by which point
  ~173,000 runs are in the stack. No setting fixes both, because a single flat run
  of deltas is the wrong shape.

So the real next step is **[size-tiered compaction](docs/design/006-tiered-compaction.md)**:
runs grow ~10× per level, layer count stays logarithmic (~6–8 runs), and write
amplification is O(log N) per link instead of O(state) per compaction. Full-base
merges become months-apart events, which demotes both remaining O(state) items —
001's storage-side compaction and the in-RAM Puffin buffer — off the critical
path.

**Backfill is already tractable.** Measured single-node ingest is **357k links/s**
through the DSU and **308k/s** with Arrow buffering (`examples/throughput.rs`), so
2B links is **~1.8 hours**. Memory during a backfill is bounded by the fold
trigger rather than by state: `--fold-after-links 20000000` holds ~3 GB and folds
~100 times. Call it 2–3 hours on one node with single-digit GB of RAM.

**Beyond, toward 10B+:** the registry restructure remains the best
saving-to-effort ratio after tiering (55% of base bytes; ~30 GB at 2B, ~125 GB at
10B). Note the shared tier is inherently single-writer — a merge can connect any
two nodes, so union-find cannot be sharded by key range without distributed
coordination — but 2k/s peak is ~0.6% of measured write capacity, so one writer
serves 10B+ comfortably. [Design 002](docs/design/002-dense-interning.md)'s u32
interning caps at 4.3B nodes and must not be used at this target; u64 or a packed
u48. Nothing structural blocks 10B — it is tiering plus constant factors.

## Next phase

Implementation-ready designs live in [docs/design/](docs/design/README.md).
[006 tiered compaction](docs/design/006-tiered-compaction.md) is the current
priority; [001 delta snapshots](docs/design/001-delta-snapshots.md) shipped its
write path and retains storage-side compaction as follow-on work.

## Extension points

- **gRPC** (`src/grpc`): a tonic `BlazeService` over the same `AppState` as
  the Axum API, served on a second port (`--grpc-listen`). The query RPCs stay
  lock-free (`forest.scope_root` / `forest.connected`) and `InjectEdge` feeds
  the same ingest channel as `POST /v1/edges`. The proto is compiled in
  `build.rs` with the pure-Rust `protox` compiler, so no `protoc` is needed.
- **Log sources**: implement a consumer feeding the pipeline channel with
  log-native offsets.
- **Iceberg REST catalog**: replace `SnapshotCatalog` commit/latest.
- **Query-side reads of history**: Parquet files are sorted and
  scope-partitioned; DataFusion can serve historical queries with the Puffin
  routing map providing current component ids.
