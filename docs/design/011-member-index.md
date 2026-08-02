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

### Size — measured twice, and the second time it was encoded

The estimate here was **+20-40% of base**, reasoning that parent-ordered pairs
are the shape the registry had before [009](009-registry-encoding.md) — a
repeated key with an ascending value list — and that delta-varint would do to
them what it did to the registry.

It shipped as a plain fixed-stride 16-byte table first, and that measured
**+77%**, so the estimate was recorded as wrong. Then the encoding was actually
built (`storage::members`, deliberately the registry's layout record for
record), which brings it back to where the estimate said. `examples/registry_shape`
at 2M links / 4M nodes / 3000 scopes:

| | run bytes | vs no index |
|---|---|---|
| without the index | 81.0 MB | — |
| flat, 16 B/pair | 143.6 MB | **+77%** |
| blocked delta-varint | 108.5 MB | **+34%**, 7.1 B/pair |

The gain is smaller than the registry's 4.8-7.1x because the *values* here are
worse: scopes are dense u32s so their gaps are one byte, while children are node
ids scattered across the id space, so a child gap is 4-5 varint bytes. What
compresses is the key side — one parent gap per record instead of eight bytes
per pair — plus the count. So the win scales with how many parents have more
than one child, and 2.3x is what this graph gives.

Both encodings stay readable; the blob type is the switch, as everywhere else
here, so a stack may mix and the change needed no rewrite.

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

### Latency, and the thing that actually bounded it

`examples/member_bench` sweeps query latency across the percolation threshold,
in the heap and over a mapped run. Two findings, and the second is the one that
mattered.

**Below percolation it is linear in the answer and cheap.** ~0.1 us per member
from the heap, ~0.14 us from a mapped run; a 187-member component is 26 us from
disk. That is comfortably the "sub-millisecond" the design claimed.

**Past percolation the cap did not bound the work.** In a flattened run a
component's root has *every* member as a direct child, and the reader
materialised all of them before the walk consulted the cap — so a `cap = 1000`
query on a hub cost O(component) and tracked component size rather than the cap.
No filter helps: the root is not a leaf, and the one lookup that dominates is a
hit.

The fix is a **budget** threaded from the walk into every child fetch, honoured
by the flat reader, the blocked reader and the memtable alike:

| `cap = 1000`, past percolation | before | after |
|---|---|---|
| in-heap p50 | 51-85 us | 60 us |
| **mmap'd run p50** | **1.1-1.5 ms** | **64 us** |
| ceiling `cap = 10000`, mmap'd | 1.3-1.5 ms | 583 us |

The mapped run now costs what the heap does, which is the shape to expect once
the work is bounded by the cap rather than by the graph.

Getting the budget right is subtler than it looks, and both wrong versions are
recorded in `Walk::expand`:

- *Room remaining* is wrong. A fetched child may already be in the visited set,
  so it consumes budget without growing the answer, and the children dropped past
  the budget are then lost silently. The two-level scoped walk re-encounters its
  own seeds through the shared tree by construction, so this dropped real
  members.
- *Decode it all and truncate* is wrong for the blocked encoding: a hub record is
  hundreds of thousands of varints, and throwing them away afterwards measured
  **slower than the fixed-stride table it replaced**.

`cap + 1` flat is right, and provably so — see `Walk::expand` for the argument
that it is both sufficient and non-quadratic.

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

**And the cap has to bound the work, not just the answer.** It did not, for the
first two implementations of the reader — see the latency section above. A cap
that bounds only what is returned is a cap in name: the query still reads the
whole component, so the hub case costs what an export costs and merely declines
to hand it over. Every child fetch takes a budget for this reason.

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

### Should it be on by default now?

The follow-up work was done to answer that, and the honest answer is **no, not
yet** — with the reasons written down rather than the conclusion.

What changed: storage +77% → **+34%**, the hub query 1.5 ms → **64 us**, and
ingest was never the problem on a disk-backed worker (**-1.4% to -2.7%**). The
query itself is no longer the argument against.

What has not changed: **+34% of run bytes is paid by every deployment, and only
some of them ever call `members`.** At 2B links that is tens of gigabytes of
object storage and the page cache to match. Filters earn their keep on every
lookup; this earns nothing until somebody asks a question many tables will never
be asked.

The thing that would change the answer is already visible in the numbers. The
member index now costs **7.1 bytes per pair while the forward table it inverts
costs 16** — the index is denser than the data. Applying the same encoding to the
routing tables would save more than the index costs, at which point turning it on
is free relative to today's base and the default should flip. That is the next
piece of work, not this one.

**The ingest cost is small in the configuration that matters.** Tracking adds one
`DashMap` write per merge, which is **-21% to -31%** of an all-RAM ingest loop
(`examples/member_bench`, best-of-3 interleaved; the spread is run-to-run machine
variance, not measurement error). But all-RAM is the configuration where the DSU
*is* the entire cost of `apply`. With a mapped base attached — what
`--routing-base disk` runs — the base probe dominates and the same absolute cost
is **-1.4% to -2.7%**. Size a backfill against the second number.

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
  every one of them is a real member. Swept across *every* cap from 1 to past the
  component size rather than checked at one point — the budget bugs above only
  showed at caps near the component size
  (`every_cap_returns_min_of_cap_and_the_component`).
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
modes, plus `storage::members` for the encoding and `examples/member_bench` for
what any of it costs.

**The benchmark earns its place too.** It found the tenant-scope truncation bug
(1153 wrong answers in one sweep), then the unbounded child fetch, then the
decode-and-truncate regression — three defects that every correctness test
passed, because all three produced *right answers slowly* or right answers with
the wrong count in a case no test constructed. A design whose whole premise is
"this is cheap for small components" needs a harness that measures, not only one
that verifies.

**The oracle earns its place here.** Membership is a strictly stronger check than
the roots, and not by a little: an implementation can name the right
representative for every node while losing members, because a lost member still
resolves upward through its parent chain. Dropping one child per parent from
`Dsu::children_of` leaves **all 5086 roots correct in every scope** and breaks
roughly 60% of the member sets. Only a set comparison sees that, which is why the
oracle emits members and not just roots.
