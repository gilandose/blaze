# 011 — Member index: listing a component from one node

`scope_root(s, x)` answers *which* component a node is in. Nothing answers *who
else is in it*. This proposes a secondary index that does, for **small
components**, behind a flag that is off by default.

> **Implemented**, behind `--member-index` (off). A parent-ordered index emitted
> during the rewrites that already happen — folds and tiered merges — plus a
> merge-edge index in the memtable, so nothing scans and nothing is rebuilt per
> query. Queried by walking the parent tree downward from the root, bounded by a
> hard cap. Off by default: it costs base bytes, and one extra map insert per
> merge, that a deployment which never asks for members should not pay.
>
> `ScopedForest::members`, `GET /v1/scopes/{scope}/members/{node}`,
> `BlazeService.GetMembers`. Graded against scipy in all three
> `tools/cc_oracle.py` modes.

## What makes this harder than an inverse index

The obvious framing is "we store `node → root`, so store `root → [nodes]` too".
That framing is wrong twice.

**First, the stored value is a *parent*, not a root.** `LayeredBase::resolve`
walks a chain:

```rust
Some((i, parent)) if parent != cur => { cur = parent; from = i + 1; }
```

A merge does not rewrite the members of the losing component — it writes **one
fixup pair**, old root to new root, and resolution follows the chain
afterwards. That is what keeps a merge O(1) writes instead of O(component), and
it is why `merge_fixups` is a counter worth watching. The consequence for this
design: what is stored is a **forest of parent pointers scattered across runs**,
so the inverse of it is a **tree**, and listing a component is a *traversal*,
not a range scan.

**Second, roots move downward.** The canonical root is the lowest original id
([I2](README.md#invariants-every-design-must-preserve)), so a component's root
only ever decreases as smaller ids join. Any index keyed on "the root at write
time" goes stale. Keying on the **parent** instead does not: a parent edge, once
written, is never rewritten — only superseded by a later fixup further up the
chain. The parent forest is append-only even though the root is not.

That second point is what makes this cheap. **The index is keyed on the parent
pointer, which is immutable, rather than on the root, which is not.**

## The mechanism

Every fold and every tiered merge already rewrites pairs. Emit a second ordering
while doing it: the same `(node, parent)` pairs sorted by `(parent, node)`,
as its own Puffin blob.

- **No incremental maintenance.** Nothing on the union path changes, and the
  memtable is unaffected. The cost lands entirely in work that already happens.
- **No new durable structure.** It is another blob in a run that is already
  being written, with the same lifecycle, the same retention, and the same
  compaction. Contrast [002](002-dense-interning.md), closed partly because an
  intern table would have needed its own recovery story.
- **The blob type is the interface**, as in [009](009-registry-encoding.md): a
  run without the blob simply has no member index, a reader that does not
  recognise it ignores it, and a stack may mix runs with and without.

### Size — measured, and worse than this design first claimed

The estimate here used to be **+20–40% of base**, reasoning that parent-ordered
pairs are the shape the registry had before [009](009-registry-encoding.md) — a
repeated key with ascending values — and that delta-varint would do to them what
it did to the registry (67.1 MB → 14.5 MB).

It would. It is not what was built. The index ships as a plain fixed-stride
16-byte pair table, so it costs what one costs. `examples/registry_shape` at 2M
links / 4M nodes / 3000 scopes:

| | run bytes |
|---|---|
| without the index | 81.0 MB |
| with it | 143.6 MB |
| **cost** | **+62.6 MB — +77%, 16.1 B/pair** |

Self-edges are dropped, which is why it is 16.1 B/pair and not more, but nothing
else compresses it. The naive number was the real number.

**Delta-varint encoding it is the obvious next step** and would plausibly land it
near the original estimate; `storage::registry::RegistryWriter` is the encoder,
and the blob type makes it a compatible change — a reader that does not recognise
a `v2` member blob simply reports the run as unindexed. Until then, `+77%` is what
a deployment turning this on is agreeing to, and it is why the flag is off.

## The query

The answer includes the queried node and the root: `members(s, x)` contains `x`,
and a node nobody has ever mentioned comes back as a singleton rather than as an
empty set.

```
members(s, x, cap):
    r = scope_root(s, x)
    frontier = [r]
    out = []
    while let Some(k) = frontier.pop():
        if out.len() >= cap: return Truncated(out)
        for child in index.children_of(k):      # range scan, contiguous
            out.push(child)
            frontier.push(child)                # child may have children itself
    return Complete(out)
```

**In a tenant scope it is two of those, not one.** The first draft of this
section had one walk and was wrong. The overlay's *elements are shared roots*,
not nodes — that is the whole layering — so walking the overlay down from
`scope_root(s, x)` yields the set of shared components the scope has glued
together, and each of those still has to be expanded through the shared tree to
reach actual members. One walk would return the component's shared roots and call
it the membership. So:

```
members(s, x, cap) for s != global:
    seeds = walk(overlay[s], scope_root(s, x))   # shared roots, capped at cap+1
    return walk(shared, seeds, cap)              # one visited set across both
```

The overlay walk is capped at `cap + 1` rather than run to completion: every
overlay element is itself a member, so more than `cap` of them already proves
truncation, and stopping there keeps a hub from walking the whole overlay before
the cap is ever consulted.

The visited set is not an optimisation. The same node arrives from several
layers, and from both levels at once — an overlay element is also reachable
through the shared tree beneath it, which is exactly what the merge fix-up
arranges.

A downward walk of the parent tree. Two things keep it cheap:

- **Children are contiguous**, because the index is sorted by parent — so
  expanding one node is a binary search plus a sequential read.
- **Leaves are the common case.** Most members have no children of their own, so
  expanding them is one binary search that finds nothing.

**No filter over the index's keys, though**, and the first version of this
section assumed one. `storage::filter` covers the *forward* tables; nothing
covers the member index, so rejecting a leaf costs a binary search rather than a
cache line. Adding one is the same shape as the filters already emitted and would
be worth measuring against the cap sizes a deployment actually uses. What is not
available is narrowing by the sparse page index: it narrows to the block holding
a key, and this table has **duplicate** keys by construction, so a run of equal
keys can begin in an earlier block. `PairTable::lower_bound` therefore searches
the whole table. Correctness first.

Correctness follows from two properties already established:

- **Keys are disjoint across runs**, so a node has exactly one parent entry and
  appears as a child exactly once across the whole stack. Note the asymmetry:
  disjointness is a property of a *node's parent entry*, not of a *parent's
  children*, so `shared_children` unions across every layer rather than stopping
  at the first hit the way `resolve` does. The same parent collects children in
  several runs as its component grows.
- **Components only ever grow.** A parent edge written at any time is still true
  now, so the walk yields no false members. Every node reachable downward from
  `r` is in `r`'s component, and every member is reachable, because every member
  has a parent chain terminating at `r`.

## The cap is not optional

Past the percolation threshold a component is not small. `examples/soak` measured
roots collapsing **28.5M → 16.9M** as the giant component formed; for any node in
it, "the members" is a large fraction of the graph. No index makes that a query —
it is an export.

So `members` returns `Complete` or `Truncated`, never a promise. A caller that
gets `Truncated` has learned something real (this node is in a hub) and should
not retry with a bigger cap. Bulk export belongs in the analytics path
([004](004-analytics-enrichment.md)) over the Parquet, not on the serving path.

**The cheaper question is usually the one being asked.** "How big is this
component" needs no index at all — a size counter per root, O(1) to maintain on
union. It is not tracked today because union is by min-id rather than by size
(`core::dsu`), and adding it is far cheaper than this design. If sizes turn out
to satisfy the demand, build that instead and stop here.

## Flag

`--member-index` (off). Two halves that behave differently, which is worth being
explicit about because only one of them matches how the other index flags work:

- **The written half** is per-run, like `--filter-bits` and
  `--registry-encoding`. Runs already written keep whatever they were written
  with, and turning it on takes effect from the next fold rather than requiring a
  rewrite. Unlike those two, though, a stack may *not* usefully mix — see the
  refusal above.
- **The memtable half is neither per-run nor retroactive.** Merge edges are
  recorded as unions happen and cannot be reconstructed afterwards, so a worker
  started without the flag answers `None` until it is restarted with it.

Off by default because it is the first index whose cost falls on deployments that
may never use it. Filters pay for themselves on every lookup; a member index pays
for itself only if someone calls `members`. And at +77% of run bytes, that is not
a rounding error.

## Surface

`GET /v1/scopes/{scope}/members/{node}?cap=N` and `BlazeService.GetMembers`.
Default cap 1000, ceiling 10 000 — above that the honest answer is the analytics
path, not a bigger HTTP response. Unanswerable is `501` / `UNIMPLEMENTED`, naming
the flag, rather than an empty list.

## What this does not do

- **Large components.** By construction — see the cap.
- **Ordering.** Members come out in parent-tree order, which is arbitrary. If
  callers want them sorted, they sort.
- **Cheap retrofitting.** The memtable half of the index is built as merges
  happen, so a worker started without `--member-index` cannot answer until it is
  restarted with it. This was the part of the design least worked out, and the
  answer is neither of the two it offered: scanning the memtable's parent map per
  query is O(memtable) on the query path, and folding first is not something a
  reader can ask for. Instead `core::dsu` records **merge edges** —
  `parent -> roots absorbed into it` — as unions happen, one map insert per merge
  and nothing on the read path.

  Deliberately not the inverse of the parent map. A true inverse would also be
  correct, since compression only ever re-points a node at an ancestor, but
  maintaining it means a *removal* on every compressing write, including the one
  `find_ro` makes on the query path — a write where there is currently only a
  read, in the one place [I3](README.md#invariants-every-design-must-preserve) is
  about. Merge edges are append-only: a root is absorbed exactly once, so there
  are no duplicates and no removals, and every non-root is reachable downward
  because its merge edge is recorded and the chain of merge edges above it
  terminates at the root.
- **Cross-scope.** `members(s, x)` is within one scope's view, like every other
  query. An `all` view is closed ([005](005-union-tier.md)).

## Invariants & tests

- Every member returned satisfies `scope_root(s, member) == scope_root(s, x)` —
  gradeable directly against the existing forest, and against
  `tools/cc_oracle.py`'s scipy components, which already know exactly who is in
  each component.
- No duplicates, and no member missed for a component below the cap. The oracle
  makes this exact rather than approximate: scipy's labels give the true member
  set for every node in a published graph.
- A component above the cap returns `Truncated` with exactly `cap` members, and
  every one of them is a real member.
- A run written without the index is still readable — the same compatibility
  property 009's blob types have. But a stack that *mixes* indexed and unindexed
  runs does **not** answer "correctly for the indexed part", which is what this
  section used to say and is the wrong contract: the members in the unindexed run
  are unreachable, and a short list is indistinguishable from a small component.
  `LayeredBase::has_member_index` therefore requires *every* layer, and the query
  refuses. `forest.members_available` in `/v1/stats` is where an operator sees it
  — a fold or a tiered merge that forgets the flag silently disables the query
  from the next base swap, not at startup, so a static config check would not
  catch it. `compact_layers` carries the index through for exactly this reason.
- Fixups: a component whose root changed after the index was written still lists
  completely, because the index is keyed on immutable parent edges rather than on
  the root. This is the test that would catch keying it on the root by mistake.

Where they live: `core::scoped` (graded against the BFS model, every scope, after
every edge), `core::dsu` (compression does not hide a member), `tests/member_index.rs`
(five folds, cold start, tiered compaction, the mixed-stack refusal, truncation
across the base/memtable boundary), `tests/integration.rs` and `tests/grpc.rs`
(the two front ends and their refusals), and `tools/cc_oracle.py` in all three
modes.

**The oracle earns its place here.** Membership is a strictly stronger check than
the roots, and not by a little: an implementation can name the right
representative for every node while losing members, because a lost member still
resolves upward through its parent chain. Dropping one child per parent from
`Dsu::children_of` leaves **all 5086 roots correct in every scope** and breaks
roughly 60% of the member sets. Only a set comparison sees that, which is why the
oracle emits members and not just roots.
