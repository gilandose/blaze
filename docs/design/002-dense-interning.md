# 002 — Dense id interning

## Problem

Measured memory is ~200 B per tracked link, dominated by DashMap entry
overhead on sparse `u64` keys and the registry's per-root `HashSet<u32>`.
At the target profile (up to 2B tracked links) that is ~400 GB — forcing
1 TB-RAM instances (~$9.5k/mo for the HA trio). Target: **≤48 B/link**,
fitting 2B links in ≤128–256 GB.

## Design

### Intern table

A one-way, append-only map from external node id to dense internal id:

```rust
struct Interner {
    to_dense: DashMap<NodeId, u32>,   // external u64 -> dense id
    to_ext:   boxcar::Vec<NodeId>,    // dense id -> external u64 (append-only,
                                      // lock-free indexed reads)
}
```

- Interning happens only on the **union path** (single-writer): both
  endpoints of every applied link. The query path *never interns* — an
  unknown node is a singleton; `scope_root` returns it unchanged without
  touching the table.
- `to_ext` is append-only and indexed by dense id, so reads are a bounds
  check + array load.

### Parent storage

Parents become flat arrays indexed by dense id:

```rust
global_parents: boxcar::Vec<AtomicU32>,   // dense parent; SELF sentinel = root
```

4 bytes per tracked node. Overlays: each scope's overlay is sparse relative
to the whole graph, so overlays keep a map — but keyed by dense id
(`DashMap<u32, u32>` or a small open-addressing map), roughly halving
their footprint. Registry: `HashSet<ScopeId>` → sorted `SmallVec<[u32; 2]>`
(measured: the vast majority of roots are referenced by 1–2 scopes; fall
back to a heap set only past a threshold).

### The one rule that must not break: compare external ids

**Canonical min-id union (I2) must compare the original `u64` ids, never
dense ids.** Dense ids are assigned in arrival order and carry no meaning.
Every union direction decision reads `to_ext[a] < to_ext[b]`. This also
preserves cross-worker determinism (replicated global DSUs stay identical
from the same log order even though their intern tables differ). A dedicated
test hammers this: interleave arrivals so dense order inverts external
order, assert roots equal BFS component minima (the existing randomized
model test extended to run on the interned implementation).

### Capacity guard

u32 caps ~4.29B interned nodes per worker — 2x headroom over the 2B target.
Guard, don't wrap: at ~4.0B emit warnings + metric; at capacity, fail the
union path loudly. Widening to packed u40 or u64 dense ids is a contained
change (parent array element type) if ever needed.

### Persistence compatibility

None of this touches storage: Puffin pairs, Parquet, and the APIs remain
external `u64` end to end. Snapshot encode maps dense→external on the way
out; hydration interns on the way in. 001's dirty sets store dense ids
internally and resolve at encode time.

## Memory budget (target profile, 2B links)

| Component | Per link/node | Total |
|---|---|---|
| Intern table (to_dense + to_ext) | ~24 B/node | ~48 GB |
| Global + overlay parents | ~4–12 B | ~8–24 GB |
| Registry (SmallVec) | ~8 B/registered root | ~16 GB |
| **Total** | **~40–48 B/link** | **~90–110 GB** |

Fits 128 GB (tight) or 256 GB (comfortable) instances; with 003 the intern
table's cold majority can also leave RAM.

## Invariants & tests

- I1/I2: full randomized model suite runs against the interned forest;
  plus the dense-order-inversion test above.
- I6: snapshot/hydrate roundtrip through external ids is byte-stable.
- New: memory probe example reruns to verify the ≤48 B/link target;
  capacity guard unit test with a shrunk limit.

## Effort

Core rewrite of `Dsu`/`ScopedForest` internals behind unchanged public
signatures: ~2–3 days including tests and re-benchmarking. Expect an ingest
throughput *gain* (measured pattern: removing map overhead sped unions 51%
when ranks were dropped; flat arrays should repeat that).
