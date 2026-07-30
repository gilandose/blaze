# 001 — Delta snapshots & compaction

> **Now the top priority, and load-bearing rather than merely nice.** 003
> shipped, which means compaction no longer materializes state (streamed) *and*
> the memtable is folded into a fresh base while the worker runs — so heap is
> bounded. But the fold rewrites the whole base, so it pays the exact cost this
> design removes: measured 2.95 s of ingest stall for a 125 MB base, extrapolating
> to tens of minutes at the target profile. Everything below applies unchanged;
> the delta chain is what turns a fold from O(state) into O(memtable).

## Problem

The flusher writes the **entire** routing map every tick. Measured at 15M
links: ~3.5s of snapshot iteration *holding the union lock* (ingest stalls),
~2.2s encode, ~256 MB Puffin payload. All three are linear in state, so the
target profile (2B links) implies ~8 min stalls and ~32 GB payloads per
minute — untenable. Meanwhile the actual *change* per 60s tick at 3k
events/s is at most ~180k pairs (~3 MB).

## Design

### Dirty tracking (write side)

All mutations already serialize behind the `ScopedForest` union lock, so
dirty tracking is a plain (non-concurrent) structure guarded by the same
lock — zero new synchronization:

```rust
struct DirtySet {
    global: HashSet<NodeId>,          // nodes whose global parent changed
    scopes: HashMap<ScopeId, HashSet<NodeId>>, // overlay members changed
}
```

Every `union` that links `child -> parent` inserts `child` (and, for
overlay fix-ups, the overlay-inserted ids) into the appropriate set. Path
compression writes (halving, `find_ro` repair) do **not** mark dirty — they
change tree shape, not membership, and snapshots resolve pairs fully at
encode time anyway.

`ScopedForest::take_dirty_snapshot()` (under the union lock) swaps the
DirtySet out and resolves only those nodes: `(node, find(node))` per dirty
node. Lock hold time is O(|delta|) — microseconds-to-milliseconds at the
target profile — instead of O(state).

### Blob & catalog format

New blob types alongside the existing ones (readers ignore unknown types —
forward compatible):

- `blaze-global-dsu-delta-v1`, `blaze-scope-dsu-delta-v1` — same sorted
  pair payload as the full blobs; `sequence-number` identifies the commit.
- Full blobs (`blaze-*-dsu-v1`) become **base** blobs, written only by
  compaction.

`SnapshotMeta` gains:

```json
{
  "puffin_path": "puffin/dsu-000000000042.puffin",   // this commit's delta
  "base_sequence": 17,                                // last full snapshot
  "delta_chain_len": 25
}
```

### Hydration (read side)

`hydrate_from_catalog` reads the base Puffin at `base_sequence`, then every
delta Puffin from `base_sequence+1..=sequence` in order, applying pairs in
sequence order. Because pairs are fully resolved *at their commit time* and
roots only ever decrease (canonical min-id), applying a later pair for the
same node simply overwrites with the newer, lower root — last-writer-wins by
sequence is correct. Registry rebuild is unchanged (derived from all pairs).

### Compaction

Triggered when `delta_chain_len > N` (default 60) **or** cumulative delta
bytes exceed a fraction of the base (default 25%). The compactor:

1. takes a full `snapshot()` — this is the one remaining O(state) pause, now
   amortized to ~hourly instead of every tick. (At 2B links that is still
   minutes under the lock; see "large-state compaction" below.)
2. writes base blobs to a new Puffin, commits a snapshot with
   `base_sequence = own sequence`, `delta_chain_len = 0`.
3. Old bases + deltas older than a retention window become GC-eligible.

**Large-state compaction** (needed at the 2B target): instead of
`snapshot()` under the lock, compact *from storage*: read the previous base
+ delta chain (no lock at all), merge them (last-write-wins by sequence,
k-way merge over sorted runs), and write the new base. The union lock is
never taken; the in-memory forest is not involved. This makes compaction a
pure storage job that could even run in a follower or a separate compactor
deployment. This is the recommended implementation — the in-memory variant
is acceptable only as an interim step.

### Interaction with 003 (disk-backed base)

The base/delta split introduced here is exactly the LSM structure 003 mmaps.
Implementing 001 with sorted, size-prefixed pair runs keeps 003 a pure
read-path change.

## Invariants & tests

- I5: sequences stay dense; `base_sequence <= sequence` always; hydration
  must fail loudly on a missing chain file rather than skip.
- I6: extend `incremental_puffin_cycles_stay_correct` so cycles produce
  deltas (and periodically a compaction), still checking every answer
  against the BFS model across cold starts. Add a targeted test: node
  re-rooted in multiple deltas (root decreasing across commits) hydrates to
  the newest root.
- New: storage-side compaction produces byte-identical routing answers to
  in-memory `snapshot()` on randomized state.

## Effort

Dirty tracking + delta write/hydrate: ~1 day. Storage-side compaction +
GC + tests: ~1–2 days.
