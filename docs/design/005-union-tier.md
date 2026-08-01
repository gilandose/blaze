# 005 — Union tier ("all edges from all scopes") & naming

> **Cost model assumes [002](002-dense-interning.md)**, which was never built, so
> the per-pair figures below are counterfactual. The composition story ("participates
> in 001 and 003 identically") is also dated: state is a tiered run set with
> per-run membership filters and a delta-varint registry
> ([006](006-tiered-compaction.md), [009](009-registry-encoding.md)).


## Problem

"Global" is currently overloaded. The implemented `Visibility::Global` /
scope 0 means *an edge visible to every scope* — a shared backbone that
participates in each tenant's view. The product also wants a second thing
that has been informally called "global": **the union view** — connectivity
over *every* edge from *every* scope, ignoring visibility (an
operator/analytics perspective).

The union view is **not derivable** from the existing two layers: if scope 7
has edge (u,v) and scope 8 has edge (v,w), the union view connects u–w
through v, but that connection exists in neither the shared DSU nor either
overlay. It needs its own structure.

## Naming (normative from here on)

| Term | Meaning | Today's name |
|---|---|---|
| **scope N** | tenant N's view: `shared ∪ scope-N edges` | scope N (unchanged) |
| **shared** | edges visible to every scope; scope id 0 | "global" / `Visibility::Global` |
| **all** | union view: every edge regardless of visibility | *(new)* |

Migration is mechanical: `Visibility::Global` → `Visibility::Shared`,
`GLOBAL_SCOPE` → `SHARED_SCOPE` (still 0), blob type
`blaze-global-dsu-v1` → kept as-is on disk for compatibility, documented as
the shared tier; API accepts `shared` (and `global` as a deprecated alias)
for scope 0, and reserves `all` for the union tier.

## Design

A third DSU tier that is *simpler* than the other two — no overlays, no
registry, no fix-ups, because the union view has no composition:

```rust
struct ScopedForest {
    shared: Dsu,                       // was `global`
    overlays: DashMap<ScopeId, Dsu>,
    registry: ...,
    union_all: Dsu,                    // NEW: every edge's raw endpoints
}
```

- **Apply**: every event additionally performs `union_all.union(src, dst)`
  regardless of visibility, inside the existing union lock. One extra O(α)
  union per event (noise at 3k events/s).
- **Query**: `scope_root("all", x) = union_all.find_ro(x)` — single-layer,
  lock-free. Surfaced as scope name `all` in REST/gRPC.
- **Canonical semantics**: lowest-graph-id-wins applies within the tier, so
  a node's `all`-view root can be **lower** than its root in any single
  scope (it sees more merges). Name query results distinctly
  (`graph_id_all`) so per-scope and union ids are never joined by accident.
- **Persistence**: one more blob type `blaze-all-dsu-v1`, same sorted-pair
  payload; participates in delta snapshots (001) and the disk-backed base
  (003) identically to the other tiers. Hydration/registry rebuild is
  unaffected (the union tier has no registry).
- **Config**: `--union-tier on|off` (default **on** at the target profile;
  the memory delta is the reason anyone would turn it off).

## Cost (target profile, 2B links)

One more parent entry per merged node in the union tier. Post-interning
(002) it **shares the intern table** — the dominant cost — so the increment
is only the parent array + its share of snapshot bytes: roughly **+8–16
B/link in RAM (~+20–30 GB)** and proportionally larger Puffin bases. Within
the envelopes already budgeted in 002/003.

## Invariants & tests

- I1 extended: `scope_root(all, x)` equals the BFS component minimum over
  *all* edges. The randomized model test gains an `all`-view assertion per
  check (the reference model already has every edge; the union component is
  a strict superset of every scope's component — assert that too).
- I2: min-id comparisons on external ids, same as every tier.
- I6: `all` blobs roundtrip through snapshot/hydrate cycles in the
  incremental test.

## Effort

Small: ~half a day plus the rename sweep (types, API scope parsing, docs,
proto comment). Recommended to land the rename *with* 001/002 (they touch
the same files) and the union tier itself in the same PR.
