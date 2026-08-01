# 010 — Stream position: snapshot metadata that can describe Kafka

`SnapshotMeta.watermark` is a `u64`: the highest event offset a snapshot covers.
Every recovery path, every dedup decision and every buffer eviction compares
against it. It is correct, and it can describe exactly one ordered stream.

> **Built.** The scalar is replaced by a `StreamPosition` — a map of partition to
> highest-applied offset — carried end to end, plus a `StreamId` naming the
> stream and consumer group the offsets belong to. Legacy scalars read as
> `{0: watermark}`, mirroring how `run_set()` already absorbs pre-run-set
> snapshots. The partial order this introduces is confined to *bookkeeping*;
> layer resolution stays totally ordered by catalog sequence and is untouched.
>
> Verified through the real binary: a table consuming three partitions, restarted
> against a directory that has since gained a fourth, resumes the first three
> where they stood and consumes the new one from its beginning — **3300 rows
> committed, exactly the stream, no migration step.**

## Two problems, only one of them obvious

**The obvious one:** a scalar cannot describe more than one partition. Kafka
assigns offsets per partition, and offset 5000 in partition 3 has nothing to do
with offset 5000 in partition 7. Today blaze consumes a single-partition topic
and says so, which is honest but narrow.

**The one worth fixing at the same time:** the watermark is not self-describing
even for one stream. It records a *number* and not what the number is in. A
worker pointed at a different topic — a rename, a staging cluster, a rebuilt
topic that reused the name — resumes at an offset that exists and means something
else entirely. It hydrates cleanly, replays a plausible suffix, and produces a
table that is wrong in a way nothing downstream can detect. `FileLog::seek_after`
catches only the case where the watermark is past the *end* of the log, which is
the lucky half of that failure.

This is the same class of failure as the put-if-absent gap in
[the preflight](../TUNING.md#the-startup-preflight): no error, no warning, and
two internally consistent answers. It gets the same treatment — a check that
fails closed.

## The position type

```rust
/// Highest offset applied per partition, inclusive.
///
/// A partition absent from the map has had nothing applied. Offset 0 is
/// reserved for exactly that meaning, so absent and zero agree.
pub struct StreamPosition { offsets: BTreeMap<PartitionId, u64> }
```

`BTreeMap` rather than `HashMap` so the serialized JSON is deterministic — two
workers committing the same position produce byte-identical metadata, which
matters when the metadata object is written with a conditional put.

### Absent means zero, and that is why there is no partition count

Kafka partitions can be **added but never removed**. A topic that grows from 8 to
16 partitions leaves the first 8 offsets untouched and starts the new 8 at zero.

Recording a partition count at commit time was considered and rejected: with
absent-means-zero, growth needs no metadata at all. A snapshot written before the
expansion simply has no entry for partition 12, a consumer that now sees
partition 12 reads its position as 0, and it consumes from the beginning of that
partition — which is exactly right, because there was nothing in it before.
A count field would add a second source of truth for something the map already
answers, and the two could disagree.

Shrinkage is the case that cannot happen. If a partition present in the committed
position stops being delivered, that is a misconfiguration or a rebalance bug, not
a topology change, and it should be reported rather than absorbed.

## Identity

```rust
pub struct StreamId {
    /// `kafka`, `file`, … — what kind of thing assigned these offsets.
    pub source: String,
    /// Topic name, or the log path.
    pub name: String,
    /// Consumer group (Kafka) or client identity, if any.
    pub group: Option<String>,
}
```

`name` is what makes a mismatch detectable. A worker whose configured stream does
not match the committed one refuses to start, with the same shape and the same
escape hatch as `--allow-unsafe-commits`: fail closed, name the mismatch, and let
an operator who knows better override it. Retopicking is a real operation; doing
it silently is not.

Shipped as `--allow-stream-change`. `stream` is `Option<StreamId>` so a snapshot
written before this existed declines the check rather than failing it.

`group` is recorded because operators reconcile blaze's watermark against the
broker's committed offsets, and **those two numbers are supposed to differ**. The
consumer group's committed offset is a checkpoint the client library advances on
its own cadence; blaze's position is what a snapshot durably covers. The group's
offset normally lags, which is what makes redelivery below the watermark routine
rather than exceptional. Without the group recorded, anyone comparing the two has
to guess which group to look at.

Identity belongs on the snapshot rather than in a table-level config object
because there is no table-level object — snapshots are the only metadata blaze
writes, and the field costs a few dozen bytes on each.

## Where the position has to flow

The scalar appears in ~164 places. They are not 164 decisions; they are six.

| Site | Today | Becomes |
|---|---|---|
| `log::Record` | `offset: u64` | `+ partition: PartitionId` |
| `EdgeBuffer::append` | `(offset, event)` | `(partition, offset, event)`; per-partition min/max |
| `Segment` | `min_offset`, `max_offset` | a `StreamPosition` span |
| Parquet schema | `offset` column | `+ partition` column (`u32`, non-null) |
| `SnapshotMeta` | `watermark: u64` | `position: StreamPosition`, `stream: Option<StreamId>` |
| `DataFileMeta` | `min_offset`, `max_offset` | per-partition spans |
| `Pipeline::apply_batch` | one `last` | per-partition last applied |
| `EdgeBuffer::drop_committed` | `max_offset > watermark` | dominance test, below |
| recovery | `seek_after(watermark)` | `seek(&position)`, per partition |
| Puffin blob metadata | `"watermark"` key | `"position"`, JSON-encoded |

## What changes when the order stops being total

This is the part that needs care, and most of it is narrower than it looks.

Two positions may be **incomparable**: `{0:5, 1:9}` and `{0:9, 1:5}` neither
dominates the other. The scalar comparisons in the current code silently assume a
total order, so each one has to be re-read as a question about *domination*:

```rust
/// Every partition in `other` is at or behind `self`.
fn dominates(&self, other: &StreamPosition) -> bool {
    other.iter().all(|(p, off)| self.get(p) >= off)
}
```

- **Buffer eviction.** `sealed.retain(|s| s.max_offset > watermark)` becomes
  "retain unless the committed position dominates this segment's span". A segment
  touching partitions 0 and 3 is droppable only when *both* are covered. Getting
  this wrong drops uncommitted data on a follower, which surfaces on failover and
  nowhere else.
- **Monotonicity.** Invariant I5 says watermarks advance monotonically. Under a
  partial order that has to mean *each partition* advances: a new position must
  dominate the previous one. Two incomparable successive commits are a bug — most
  likely a rebalance handing partitions to a second consumer — and the commit path
  should reject rather than absorb them. This is a **new check**, and it is the one
  that turns a whole class of split-brain consumption into a loud failure.

And the part that does **not** change:

- **`RunMeta.min_sequence`/`max_sequence` are catalog sequences, not log offsets.**
  Sequences are assigned by the commit path, one per snapshot, and remain totally
  ordered. Layer resolution order, the adjacency rule for merges, and the
  disjoint-keys invariant that makes "first hit while scanning is the only hit"
  true are all defined over sequences. **The partial order does not reach them.**
  This is worth stating loudly because "we made ordering partial" sounds like it
  should threaten exactly that argument, and it does not.

## Why per-partition independence is correct at all

Because DSU merges commute. Union is commutative, associative and idempotent, so
the forest reached by applying a set of edges does not depend on the order they
arrive in — within a partition or across partitions.

Which raises the sharper question: if order does not matter, why is there an
offset at all?

**The offset is bookkeeping for exactly-once buffering, not a serialization
requirement.** It answers "has this record already been written to a data file",
and nothing else. That is why redelivery below the watermark can be skipped
without touching the forest, and why the forest is unharmed when a duplicate
*is* applied — the harm is a duplicate Parquet row. Recognising this narrows the
blast radius of the whole change: partial ordering weakens a bookkeeping
invariant, not a correctness one.

It also means partitions could eventually be consumed by parallel threads. Not
here: the pipeline is single-writer by design, which is what keeps the query path
lock-free, and that constraint is worth more than the parallelism.

## Migration

Same shape as the run-set migration, which is the precedent this codebase already
set and readers already understand.

- `SnapshotMeta::stream_position()` is the only way readers ask. It returns
  `position` when present, and otherwise synthesises `{0: watermark}` from the
  legacy scalar — exactly as `run_set()` falls back to `SequencesOnly`.
- `format_version` bumps to `FORMAT_STREAM_POSITION = 2`.
- **Single-partition writers keep populating `watermark`.** When the position has
  exactly one entry and it is partition 0, the scalar is not an approximation, it
  is the same number — so an older binary reads a newer catalog correctly for the
  only topology it could ever have handled. Rollback works inside the
  single-partition world.
- **Multi-partition is a one-way upgrade**, like tiered run sets. There is no
  value of `watermark` that an old reader resolves correctly, so it is written as
  `0`.

### What the version marker turned out not to be

The first cut bumped `format_version` to `FORMAT_STREAM_POSITION` for any
multi-partition position. That is wrong, and building it made the reason
obvious: **`format_version` is a generation, not a bitfield.** Each level implies
the ones below it, and `>= FORMAT_RUN_SETS` is a *promise that `runs` is
populated* — `run_set()` rejects a snapshot claiming it and listing none. A
RAM-mode worker writes no runs, so version 2 there is a promise it does not keep,
and the first RAM-mode multi-partition commit failed its own validation.

So the bump applies only on top of a run set. A RAM-mode multi-partition snapshot
carries no version marker at all, and that costs less than it looks like. An old
binary ignores `format_version` — that was already the known hole in the run-set
upgrade — so the marker was never protection, only documentation. What genuinely
describes such a snapshot is `position` being present with more than one entry:
self-describing, in the way the Puffin blob types are, and readable by anything
that knows to look.

### Parquet is the easy half

Adding a `partition` column looks like the risky part and is not, because
**blaze never reads its own data files back**. Recovery hydrates from Puffin
sidecars; the Parquet is for external readers and DataFusion. So the column is
purely additive: old files lack it, new files have it, and a reader that unions
both treats the missing column as partition 0 — standard schema evolution, no
rewrite, no migration window.

## Cost

- **Metadata**: a few dozen bytes per snapshot per partition. At 16 partitions and
  a 60s flush interval that is ~500 B per snapshot against data files measured in
  megabytes. Free.
- **Parquet**: 4 bytes per row before encoding. A segment usually spans few
  partitions, so the column is near-constant and RLE flattens it to approximately
  nothing.
- **Ingest**: a map lookup per record instead of a scalar compare, on a path
  measured at 3.9 µs/edge. Expected to be lost in the noise; to be measured, not
  assumed, and a `SmallVec` keyed by partition index is the fallback if a
  `BTreeMap` probe shows up in a profile.
- **Eviction**: dominance over a handful of partitions instead of one compare,
  once per tick.

## What this does not fix

- **Parallel consumption.** One writer thread still applies everything. This
  design makes multi-partition *correct*, not faster.
- **Rebalancing.** blaze is single-writer per table via leader election, so the
  leader must own every partition of the topic. A consumer group that splits
  partitions across two blaze workers produces two divergent tables, and the
  monotonicity check above turns that into an error at commit rather than a
  correctness problem — it detects the situation, it does not support it.
- **Topic retention outrunning the consumer.** If the broker drops a prefix blaze
  has not consumed, the resume offset is gone. Detectable (the seek fails, as the
  file log's already does) but not recoverable without a backfill.
- **Exactly-once *production*.** Nothing here concerns what blaze writes onward.

## Invariants & tests

Restating I5 for a partial order, and adding one:

- **I5 — Exactly-once visibility** (amended): state becomes visible only via the
  put-if-absent catalog commit; **each partition's offset advances monotonically**,
  i.e. every committed position dominates its predecessor; sequences are dense.
- **I7 — Stream identity**: a snapshot's offsets are only interpretable against
  the `StreamId` it was committed with. A worker whose configured stream does not
  match refuses to start.

Tests this needs, beyond porting the existing ones:

1. A legacy scalar snapshot reads as `{0: watermark}` and a worker resumes from it
   unchanged — the migration equivalent of the existing `SequencesOnly` test.
2. A segment spanning two partitions is **not** dropped when only one is
   committed. This is the eviction bug that would otherwise surface only on
   failover.
3. Two incomparable successive positions are rejected at commit.
4. A multi-partition restart mid-stream reaches the same state as an
   uninterrupted run, and commits the same row count — the same pair of
   assertions `tests/log_ingest.rs` uses, since state equality alone proved too
   weak to catch an off-by-one once the graph percolated.
5. Redelivery below the watermark on *one* partition does not suppress a live
   record on another.
6. A stream identity mismatch fails closed, and the escape hatch overrides it.

A multi-partition `FileLog` — a directory of files, one per partition, offset
still the line number — is what makes 2 through 5 testable without a broker.

## What building it turned up

Two bugs the design did not predict, both caught by checks this design added:

- **The committed position has to be cumulative.** The flush loop first published
  the extent of *this tick's* segments, so a tick holding nothing from partition 1
  dropped partition 1 from the position entirely — and the next recovery would
  have replayed it from the beginning. `advances_from` rejected the commit, which
  is exactly the incomparability it was written to catch, arriving from a
  direction nobody had in mind. The position a snapshot publishes is the previous
  one merged with what was just written.
- **The data file's extent is not the snapshot's position.** They were briefly the
  same value. `DataFileMeta` names what its rows *are*, which is only this tick's
  writes; the snapshot names where the whole stream stands.

And one thing the design got wrong on paper, corrected above: the
`format_version` bump. See *What the version marker turned out not to be*.

## Sequencing

Landed in two commits, because the first is mechanical and the second is where
the thinking is:

1. **`StreamPosition` + `StreamId` in the catalog**, with the legacy fallback and
   the migration tests. Nothing produced a multi-partition position yet; the
   single-partition path serialised identically apart from the new fields.
2. **Carry the partition end to end** — record, buffer, Parquet column, dedup,
   eviction, seek — plus `PartitionedLog` and tests 2 through 5.
