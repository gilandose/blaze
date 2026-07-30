# 008 — Could this have been built on RocksDB?

Asked after shipping tiering, which is the point at which the question becomes
sharp: most of that work was reimplementing an LSM, and RocksDB is an LSM. Worth
answering honestly rather than defensively, because the answer decides whether the
storage tier is a liability we own forever or a thing that had to be built.

> Short version: **RocksDB would have replaced roughly 1,100 lines of LSM
> machinery and none of the ~1,000 lines that make this a union-find engine.** The
> part that can't be delegated is compaction, which here has to *resolve* rather
> than *deduplicate* — and that is precisely the part RocksDB's extension points
> cannot express.

## What RocksDB would have given us

Not a small list. Mapped against what is actually in this repo:

| ours | RocksDB equivalent | code |
|---|---|---|
| `LayeredBase` — ordered run stack, k-way merge iterator | SSTable levels + `MergingIterator` | 326 |
| `BlockedFilter` — blocked bloom, all bits in one cache line | built-in bloom filters | 163 |
| `SparseIndex` — first key per 4 KiB block | block index | (part of `base.rs`) |
| `tier::pick_merge` — size-tiered policy, fanout, depth ceiling | universal/leveled compaction | 78 |
| `start_merge` / `adopt_merge` — detached background merge | background compaction threads | (part of `flush.rs`) |
| mmap reader, binary search, block layout | table reader | ~534 in `base.rs` |

Call it **~1,100 lines of LSM** we wrote and now maintain, plus the measurement
work behind it: the blocked-filter cache-line argument, the 0.2%-of-table sparse
index sizing, the fanout-vs-depth trade in 006. All of that is solved, tuned, and
battle-tested in RocksDB, and none of it is our competitive advantage.

That is a real cost and it should be stated plainly.

## What RocksDB would not have given us

### Compaction has to resolve, not deduplicate — and this is decisive

An LSM compaction keeps the newest value per key. Ours *re-resolves* every value
through the composed stack, so `900→500`, `500→105`, `105→40`, `40→3` collapses to
`900→3`. Without that collapse, depth grows without bound and every lookup chases
the chain.

RocksDB has two extension points and neither reaches it:

- **Merge operator.** Associative combine over operands for *one* key. Canonical
  lowest-id-wins looks like a perfect fit — `merge(node, parent)` taking the min —
  and it does handle the point update. But a root is *transitive*: knowing the
  lowest parent ever written for `900` does not tell you `900`'s root, because the
  parent has its own parent. The merge operator never sees another key.
- **Compaction filter.** Sees one key-value at a time and may drop or modify it.
  Resolving a value means reading *other* keys mid-compaction, which RocksDB
  explicitly warns against — reading the DB from inside a compaction filter risks
  deadlock and sees inconsistent state.

So `compact_layers` — 456 lines, the largest single piece of logic in the storage
tier — stays ours either way. And it is the piece the whole design rests on: the
disjoint-keys invariant, `moved_roots`, the registry re-keying, the argument in
`LayeredBase::slice` about why a subset merge may be spliced back in place.

The alternative is storing fully-resolved roots so last-write-wins suffices. Then a
single union rewrites every member of the component — O(component) per edge, which
is the exact cost union-find exists to avoid.

### The durable format is the product, not an implementation detail

Committed state here *is* Iceberg-flavoured object storage: Parquet data files,
Puffin sidecars, atomic catalog commits, readable by other tools. RocksDB SSTables
are a local embedded format. Using RocksDB would mean maintaining **both** — its
LSM as the serving cache, and the Puffin/Iceberg representation as the durable
one — with two compaction schedules and two consistency stories to keep aligned.

That inverts the current design, where the mmap'd local runs *are* the committed
Puffin files, downloaded and mapped directly. The read-through cache is free
because the local and remote formats are identical.

### Multi-tenant scoping is a secondary index

Per-scope overlays keyed by shared root, plus a registry mapping shared root →
referencing scopes, so a global merge notifies only the scopes that care instead of
broadcasting to 3,000. Column families or key prefixes give you the storage; the
registry is a secondary index maintained transactionally with the primary, and no
KV store maintains that for you. `compact_layers` re-keys it during every merge.

### The query path

`find_ro` composes the memtable over the run stack with one read-only repair past
depth 8, and takes no lock, so queries never block behind ingest. Over RocksDB
that becomes a snapshot read plus our own overlay resolution — doable, but the
composition logic is ours regardless, and we would lose the direct mmap.

## Verdict

**If the requirement were "a fast embedded KV store with an LSM", RocksDB, without
hesitation.** It isn't. The requirement is a queryable union-find whose durable
form is Iceberg on object storage. The LSM is incidental to that; the resolving
compaction and the output format are the product.

Two things follow, though, and both are concessions:

1. **The ~1,100 lines of LSM are a genuine liability**, not a triumph. They are
   correct and measured, but they are undifferentiated, and every future bug in
   them is ours. If a Rust LSM crate ever exposes a compaction hook that can read
   across runs, revisiting this is worthwhile.
2. **The strongest version of the criticism is not RocksDB-shaped.** It is that
   `base.rs` reimplements a table reader. If we wanted to shed real weight without
   touching the semantics, adopting Parquet or Arrow IPC for the run files — with
   their existing readers, indexes and predicate pushdown — would delete more code
   than RocksDB would, and would keep the durable format Iceberg-native rather than
   introducing a second one. That is a better question than this one, and it is not
   answered here.

Related: [007](007-compaction-execution.md) on where compaction runs. The same
reasoning applies to Flink — it would have made the ingest, checkpointing and
failover machinery trivial and the union-find core harder, because keyed state
cannot express `union(a, b)` across key groups and its state is not externally
queryable. The ingest side is where buying rather than building would genuinely
have paid.
