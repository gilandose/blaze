# 011 — Member index: listing a component from one node

`scope_root(s, x)` answers *which* component a node is in. Nothing answers *who
else is in it*. This proposes a secondary index that does, for **small
components**, behind a flag that is off by default.

> **Proposed.** A parent-ordered index emitted during the rewrites that already
> happen — folds and tiered merges — so there is no incremental maintenance and
> nothing new on the union path. Queried by walking the parent tree downward from
> the root, bounded by a hard cap. Off by default: it costs base bytes that a
> deployment which never asks for members should not pay.

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

### Size

Naively this is +16 bytes per pair, doubling the base — but parent-ordered pairs
are exactly the shape the registry had before 009: a repeated key with ascending
values. Delta-varint took that from 67.1 MB to 13.9 MB, **4.8–7.1x**. The same
encoding should apply, putting this nearer **+20–40% of base** than +100%.

**That is an estimate, not a measurement.** It should be measured on a real base
before the flag's default is ever reconsidered — `examples/registry_shape` is the
harness to extend.

## The query

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

A downward walk of the parent tree. Two things keep it cheap:

- **Children are contiguous**, because the index is sorted by parent — so
  expanding one node is a binary search plus a sequential read.
- **Leaves are the common case and are cheap to reject.** Most members have no
  children of their own, and a membership filter over the index's *keys*
  (`storage::filter`, already built) turns that into one cache line rather than a
  binary search. A 100-node component is ~100 filter probes and a handful of
  range scans — sub-millisecond, which is the point.

Correctness follows from two properties already established:

- **Keys are disjoint across runs**, so a node has exactly one parent entry and
  the walk cannot double-count.
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

`--member-index` (off). Per-run, like `--filter-bits` and `--registry-encoding`:
runs already written keep whatever they were written with, a stack may mix, and
turning it on takes effect from the next fold rather than requiring a rewrite.

Off by default because it is the first index whose cost falls on deployments that
may never use it. Filters pay for themselves on every lookup; a member index pays
for itself only if someone calls `members`.

## What this does not do

- **Large components.** By construction — see the cap.
- **Ordering.** Members come out in parent-tree order, which is arbitrary. If
  callers want them sorted, they sort.
- **The memtable.** Pairs not yet folded have no index entry, so a component
  whose members arrived since the last fold is under-reported. Either fold before
  querying, or scan the memtable's parent map directly on the query path — it is
  bounded by the fold trigger and in memory. The second is probably right, and is
  the part of this design least worked out.
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
- A run written without the index is still readable, and a stack mixing indexed
  and unindexed runs answers correctly for the indexed part — the same
  compatibility property 009's blob types have.
- Fixups: a component whose root changed after the index was written still lists
  completely, because the index is keyed on immutable parent edges rather than on
  the root. This is the test that would catch keying it on the root by mistake.
