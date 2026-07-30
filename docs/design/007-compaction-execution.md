# 007 — Where compaction runs: detached, separate process, or separate deployment

## First, a clarification: "async" here means detached, not async I/O

The merge is `mmap` reads (page faults) plus CPU plus one large sequential write.
None of that benefits from `async` — there is no socket to await, and page faults
are not yieldable. Making compaction `async` in the useful sense means **running
it detached from the flush tick**, so folds and commits continue while it works.
Threads and processes are the tools; `spawn_blocking` (already in place) only
stops it from occupying a runtime worker.

So the real question is *where the work runs*, and there are three shapes.

## The blocker is the catalog format, and it is the same for all three

Compaction is already pure storage-side: it reads immutable committed layer files
and writes one new file. It touches no forest, no memtable, no union lock, no
Arrow buffer. That is what makes any of these deployments possible.

What stops it running detached today is not the execution model but how a
snapshot is described. `SnapshotMeta` says "base at sequence B, plus every delta
in `B+1..=sequence`". A merge that started when the chain was `L0..Lk` finishes
after `Lk+1..Ln` have also been committed, and its output cannot be expressed in
that form:

- Commit it as a base at a later sequence `S` → the chain becomes empty and
  `Lk+1..Ln` are silently dropped. Data loss.
- Commit it as a base at sequence `k` → `k` is already taken, and sequences are
  dense and immutable.

So `SnapshotMeta` has to describe a **set of runs** (or at minimum record which
sequence range the merged base covers), which lets a reader assemble
`[M, Lk+1..Ln]`. Design 006 needs exactly that change for tiering, where there is
no single privileged base at all.

**Once the format lands, all three shapes below work with the merge code
unchanged.** The execution model is a deployment choice; the format is the
engineering.

Worth noting the splice is already proven sound: `M ≡ [L0..Lk]`, and `Lk+1`'s keys
were composed roots of `[L0..Lk]`, hence of `[M]` — so disjointness holds and
`swap_base` accepts it. That argument is in `core/scoped.rs`.

## Shipped: (a), detached in-process

The format landed with tiering, and (a) is now the default — `--inline-merges`
turns it off. `start_merge` spawns onto a blocking thread and returns; a later
tick's `adopt_merge` publishes the result and splices it in. Folds and commits run
throughout, which was the point: awaiting a merge inside the tick meant nothing
drained the memtable for its duration, so the fold trigger stopped bounding
memory for however many minutes a high-level merge took.

Three things this needed that were not obvious up front:

- **The merged run is located by identity, not by remembered indices.** A merge
  outlives the stack it was planned against — folds append below it, and a
  full-stack fold can replace it outright. `adopt_merge` finds its inputs as a
  contiguous window by path and discards the merge if that window is gone, rather
  than splicing over whatever now sits at those positions.
- **`fold` had to stop compacting on depth.** That was the only depth control
  before tiering, and left in place it preempted the policy: with a merge in
  flight the stack still looks deep, so the fold rewrote the whole thing inline
  under the union lock — performing the merge's work synchronously, which is
  precisely what detaching it was meant to avoid. Caught by the first run of the
  detached test, which found one run where it expected four. `fold` now compacts
  only when there is no stack yet or the stack holds a local-only run; depth is
  `pick_merge`'s ceiling branch, and a stack stuck over the ceiling logs a warning
  rather than being papered over.
- **A merge is published later than the span it covers.** That is the observable
  the run-set format exists for and what the test asserts directly: a run covering
  1..=3 committed at sequence 5, sitting below a delta committed at 4 while it
  ran. No value of `base_sequence` + `delta_chain_len` describes that.

Still true and still worth doing: `wait_for_merge` exists for graceful shutdown,
and a process that exits with a merge in flight leaves an orphan file on local
disk that nothing references.

## (a) Detached task, same process

A background task that the tick kicks off and does not await; on completion it
commits and calls `swap_base`.

- **Cost sharing**: same CPU and, critically, the same page cache. Measured, a
  compaction reads the base end to end and leaves it resident — 8.4 GB RSS
  against a 5.4 GB base — so it evicts the query working set while it runs.
- **Local cache**: shared, so nothing is re-downloaded. This is the big practical
  advantage.
- **Ops**: nothing new to deploy.
- **Concurrency**: needs a guard so only one merge runs at a time, and the stack
  splice described above.

## (b) Separate process, same node

A `--role compactor` mode of the same binary, coordinating only through the
catalog and the shared local directory.

- Adds OS-level resource control (cgroup/nice), so compaction cannot starve
  serving of CPU, and an independent failure domain — it can be killed and
  restarted without touching the serving process.
- Does **not** fix page-cache eviction: same kernel, same cache.
- Still shares the local NVMe cache, so still no re-download.
- Two processes writing the same directory is already safe — layer files are
  written to `.partial` and renamed.

Honestly a modest gain over (a): mostly blast radius and resource limits.

## (c) Separate deployment

Its own pod/node, reading the chain from object storage.

- **Removes compaction's CPU *and* page-cache cost from serving nodes entirely.**
  Given the eviction measurement, this is the one real advantage, and it is a
  query-latency advantage rather than a throughput one.
- Can use a different instance shape — compaction wants cores and disk
  bandwidth, not low-latency RAM — and can scale independently, since compaction
  is bursty.
- **Costs a full chain download per compaction.** Without tiering that is the
  whole base: ~75 GB at 2B links, every cycle. With tiering, only the level being
  merged. **So (c) is only economical after 006.**
- Needs a second election so two compactors do not duplicate work. Cheap: reuse
  the existing Lease machinery with a different key. Note correctness does not
  depend on it — put-if-absent already makes the loser discard its merge — so
  this is purely about wasted work.

## Failure modes (all shapes)

Mostly already handled, which is worth stating so nobody re-solves it:

- **Dies mid-merge**: the output only exists at `.partial` and is never renamed,
  so nothing can map a torn file. Already the case.
- **Loses the commit race**: discards the merge and retries next cycle. Already
  implemented.
- **Compactor down for a long time**: the chain grows, the depth tax rises
  (measured ~1.3 µs per link ingested per layer, ~0.65 µs per lookup), and
  serving degrades smoothly rather than failing. That makes **chain length an
  SLI** — it should be alerted on, since nothing else will notice.

## Page-cache pollution, and why it is not a one-line fix

Attempted and backed out, because it is worth recording why. A compaction reads
each page of a layer exactly once and leaves all of them resident — measured,
8.4 GB RSS against a 5.4 GB base — and since those pages are the most recently
touched, LRU prefers to evict the query working set over them. Recency inversion,
not a leak.

- **`MADV_COLD`** is exactly right: deprioritize for reclaim without freeing
  anything or changing an observable byte, so it is safe to issue while other
  threads read the mapping. memmap2 0.9.11 does not expose it.
- **`MADV_DONTNEED`** is exposed, but only through `unsafe
  unchecked_advise_range`, and it is unsound here. Queries hold `&[u8]` borrows
  into the same mapping concurrently with a sweep, and freeing pages under a live
  borrow is UB in Rust's model — even though a read-only mapping of an immutable
  file would refault identical bytes. The precondition cannot be established, so
  the unsafe block cannot be justified.
- **The safe fix is to stop sweeping through the mapping**: have compaction read
  layers with ordinary buffered file I/O and `posix_fadvise(POSIX_FADV_DONTNEED)`
  on the descriptor, which involves no borrows at all. That is a real change to
  the compaction reader rather than an advice call, and it composes well with
  running compaction in a separate process — which would not have the serving
  node's page cache to pollute in the first place.

Note this ranks *below* tiering for the same reason everything else does: once
merges stop touching the whole base, there is far less cache to pollute.

## Recommendation

1. **Tiering plus the run-set format** (006). It is the unlock for every option
   here, and independently it converts O(N²) total merge work into O(N log N).
2. **Then (a), detached in-process, as the default.** With tiering most merges are
   small — L0→L1 is megabytes — so co-location is fine and the shared local cache
   is worth keeping. The page-cache objection largely evaporates once merges stop
   touching the whole base.
3. **Keep (c) as a config option for the regimes where merges are genuinely
   large**: the rare high-level merges, and **backfill**, where compaction is
   continuous and heavy. This mirrors `--max-delta-layers`, which already wants a
   different value while backfilling than while serving — the deployment shape is
   another knob that should differ between those two modes.

(b) is not worth building on its own; it is (a) plus process supervision, and if
isolation matters enough to pay for it, (c) buys strictly more.

One convenient consequence: a separate compactor is the natural home for the
**bulk-load path** too (sort edges, external-memory union-find, emit one base).
Both are batch jobs over object storage that need no forest, so they can be the
same binary and the same deployment.
