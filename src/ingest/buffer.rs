//! Arrow-native edge buffering.
//!
//! Incoming edges append into active Arrow builders (one row per visible
//! scope, so a Parquet reader can prune by the `scope_id` column). On every
//! flush tick the active builders rotate into a *sealed segment* tagged with
//! the offset range it covers. Sealed segments are what the leader persists;
//! followers drop sealed segments once the committed watermark passes them,
//! so a failover never loses or double-writes data.

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use arrow_array::types::TimestampMillisecondType;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parking_lot::Mutex;
use std::sync::Arc;

use crate::core::{EdgeEvent, GLOBAL_SCOPE, PartitionId, StreamPosition, Visibility};

/// Schema of the edge table as written to Parquet.
pub fn edge_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // `partition` before `offset` because together they are the row's
        // identity in the stream, and neither means anything alone. Added by
        // design 010; blaze never reads its own data files back — recovery is
        // from the Puffin sidecars — so this is purely additive for external
        // readers, and a file written without it reads as partition 0.
        Field::new("partition", DataType::UInt32, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
        Field::new("scope_id", DataType::UInt32, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("props", DataType::Utf8, true),
    ]))
}

#[derive(Default)]
struct ActiveBuf {
    partition: UInt32Builder,
    offset: UInt64Builder,
    src: UInt64Builder,
    dst: UInt64Builder,
    scope: UInt32Builder,
    event_time: PrimitiveBuilder<TimestampMillisecondType>,
    props: StringBuilder,
    rows: usize,
    /// Lowest and highest offset seen per partition. Both are needed: `max` is
    /// what decides durability, `min` is what a data file advertises.
    first: StreamPosition,
    last: StreamPosition,
}

/// An immutable, flush-ready chunk of buffered edges.
#[derive(Clone)]
pub struct Segment {
    pub batch: RecordBatch,
    /// Lowest offset this segment holds per partition.
    pub first: StreamPosition,
    /// Highest offset this segment holds per partition — the position a
    /// committed snapshot must **dominate** before the segment is durable.
    pub last: StreamPosition,
}

#[derive(Default)]
pub struct EdgeBuffer {
    active: Mutex<ActiveBuf>,
    sealed: Mutex<Vec<Segment>>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct BufferStats {
    pub active_rows: usize,
    pub sealed_rows: usize,
    pub sealed_segments: usize,
    pub sealed_bytes: usize,
}

impl EdgeBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one event at its position in the stream, exploding multi-scope
    /// visibility into one row per scope (global => single row, scope 0).
    pub fn append(&self, partition: PartitionId, offset: u64, event: &EdgeEvent) {
        let mut buf = self.active.lock();
        // Offsets are one-based, so a zero here means this partition has not
        // contributed to the active buffer yet rather than "starts at zero".
        if buf.first.get(partition) == 0 {
            buf.first.advance(partition, offset);
        }
        buf.last.advance(partition, offset);
        let scopes: smallvec::SmallVec<[u32; 4]> = match event.visibility.clone().normalize() {
            Visibility::Global => smallvec::smallvec![GLOBAL_SCOPE],
            Visibility::Scoped(s) => s,
        };
        for scope in scopes {
            buf.partition.append_value(partition);
            buf.offset.append_value(offset);
            buf.src.append_value(event.src);
            buf.dst.append_value(event.dst);
            buf.scope.append_value(scope);
            buf.event_time.append_value(event.event_time_ms);
            buf.props.append_option(event.props.as_deref());
            buf.rows += 1;
        }
    }

    /// Rotate the active builders into a sealed segment. Rows are sorted by
    /// `(scope_id, src)` so Parquet row groups cluster per tenant and scope
    /// predicates prune well.
    pub fn seal_active(&self) -> Option<Segment> {
        let mut buf = self.active.lock();
        if buf.rows == 0 {
            return None;
        }
        let taken = std::mem::take(&mut *buf);
        drop(buf);

        let ActiveBuf {
            mut partition,
            mut offset,
            mut src,
            mut dst,
            mut scope,
            mut event_time,
            mut props,
            first,
            last,
            ..
        } = taken;

        let batch = RecordBatch::try_new(
            edge_schema(),
            vec![
                Arc::new(partition.finish()),
                Arc::new(offset.finish()),
                Arc::new(src.finish()),
                Arc::new(dst.finish()),
                Arc::new(scope.finish()),
                Arc::new(event_time.finish()),
                Arc::new(props.finish()),
            ],
        )
        .expect("edge batch matches edge schema");

        let sort_cols = vec![
            SortColumn {
                values: batch.column(4).clone(), // scope_id
                options: None,
            },
            SortColumn {
                values: batch.column(2).clone(), // src
                options: None,
            },
        ];
        let indices = lexsort_to_indices(&sort_cols, None).expect("lexsort edge batch");
        let columns = batch
            .columns()
            .iter()
            .map(|c| arrow_select::take::take(c, &indices, None).expect("take sorted"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(edge_schema(), columns).expect("sorted edge batch");

        let segment = Segment { batch, first, last };
        self.sealed.lock().push(segment.clone());
        Some(segment)
    }

    /// Sealed segments currently pending persistence (cheap Arc clones).
    pub fn sealed_segments(&self) -> Vec<Segment> {
        self.sealed.lock().clone()
    }

    /// Drop sealed segments the committed position fully covers. Called by
    /// followers when they observe a new catalog snapshot, and by the leader
    /// after a successful commit.
    ///
    /// **Dominance, not a comparison.** A segment spanning partitions 0 and 3 is
    /// durable only when *both* are committed. Under the old scalar watermark
    /// this was `max_offset > watermark`, which silently assumed a total order;
    /// with a partial one, a position may cover part of a segment and not the
    /// rest. Dropping such a segment loses data a follower still owes, and the
    /// loss surfaces only on failover — the one moment nobody is watching.
    pub fn drop_committed(&self, committed: &StreamPosition) -> usize {
        let mut sealed = self.sealed.lock();
        let before = sealed.len();
        sealed.retain(|s| !committed.dominates(&s.last));
        before - sealed.len()
    }

    pub fn stats(&self) -> BufferStats {
        let active_rows = self.active.lock().rows;
        let sealed = self.sealed.lock();
        BufferStats {
            active_rows,
            sealed_rows: sealed.iter().map(|s| s.batch.num_rows()).sum(),
            sealed_segments: sealed.len(),
            sealed_bytes: sealed.iter().map(|s| s.batch.get_array_memory_size()).sum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, UInt32Array};
    use smallvec::smallvec;

    fn ev(src: u64, dst: u64, vis: Visibility) -> EdgeEvent {
        EdgeEvent {
            src,
            dst,
            visibility: vis,
            event_time_ms: 1_700_000_000_000,
            props: Some(r#"{"w":1}"#.to_string()),
        }
    }

    #[test]
    fn explode_seal_and_prune() {
        let buf = EdgeBuffer::new();
        buf.append(0, 1, &ev(1, 2, Visibility::Global));
        buf.append(0, 2, &ev(3, 4, Visibility::Scoped(smallvec![7, 9])));
        assert_eq!(buf.stats().active_rows, 3);

        let seg = buf.seal_active().expect("segment");
        assert_eq!(seg.batch.num_rows(), 3);
        assert_eq!(seg.first, StreamPosition::single(1));
        assert_eq!(seg.last, StreamPosition::single(2));
        assert_eq!(buf.stats().active_rows, 0);
        assert_eq!(buf.stats().sealed_segments, 1);

        // Sorted by scope: global row (scope 0) first.
        let scopes = seg
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(scopes.values(), &[0, 7, 9]);

        // Nothing to seal when empty.
        assert!(buf.seal_active().is_none());

        buf.append(0, 3, &ev(5, 6, Visibility::Global));
        buf.seal_active();
        assert_eq!(buf.stats().sealed_segments, 2);

        // A position at 2 covers only the first segment.
        assert_eq!(buf.drop_committed(&StreamPosition::single(2)), 1);
        assert_eq!(buf.stats().sealed_segments, 1);
        assert_eq!(buf.drop_committed(&StreamPosition::single(3)), 1);
        assert_eq!(buf.stats().sealed_segments, 0);
    }

    /// The eviction case a scalar watermark could not express.
    ///
    /// A segment spanning two partitions is durable only when **both** are
    /// committed. Under the old `max_offset > watermark` there was one number to
    /// compare and the question could not arise; with a partial order a position
    /// can cover part of a segment and not the rest. Dropping it there loses
    /// data the worker still owes, and — because the mapping stays valid and the
    /// catalog stays parseable — the loss shows up only when someone hydrates
    /// from scratch after a failover.
    #[test]
    fn a_segment_spanning_two_partitions_needs_both_committed() {
        let buf = EdgeBuffer::new();
        buf.append(0, 10, &ev(1, 2, Visibility::Global));
        buf.append(3, 4, &ev(3, 4, Visibility::Global));
        let seg = buf.seal_active().expect("segment");
        assert_eq!(seg.first, [(0, 10), (3, 4)].into_iter().collect());
        assert_eq!(seg.last, [(0, 10), (3, 4)].into_iter().collect());

        // Partition 0 committed well past this segment, partition 3 not at all.
        // A scalar comparison on the larger offset would drop it.
        let one_side: StreamPosition = [(0, 99)].into_iter().collect();
        assert_eq!(
            buf.drop_committed(&one_side),
            0,
            "dropped an undurable segment"
        );
        assert_eq!(buf.stats().sealed_segments, 1);

        // Partition 3 committed but short of offset 4 — still not covered.
        let nearly: StreamPosition = [(0, 99), (3, 3)].into_iter().collect();
        assert_eq!(buf.drop_committed(&nearly), 0);

        // Both covered.
        let both: StreamPosition = [(0, 99), (3, 4)].into_iter().collect();
        assert_eq!(buf.drop_committed(&both), 1);
        assert_eq!(buf.stats().sealed_segments, 0);
    }

    /// Partitions interleave in the active buffer and each keeps its own span,
    /// rather than one global min and max that would claim offsets neither
    /// partition holds.
    #[test]
    fn each_partition_keeps_its_own_span() {
        let buf = EdgeBuffer::new();
        buf.append(1, 100, &ev(1, 2, Visibility::Global));
        buf.append(2, 5, &ev(3, 4, Visibility::Global));
        buf.append(1, 101, &ev(5, 6, Visibility::Global));
        buf.append(2, 9, &ev(7, 8, Visibility::Global));

        let seg = buf.seal_active().expect("segment");
        assert_eq!(seg.first, [(1, 100), (2, 5)].into_iter().collect());
        assert_eq!(seg.last, [(1, 101), (2, 9)].into_iter().collect());

        // And the partition travels with the row into Parquet.
        let parts = seg
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let mut seen: Vec<u32> = parts.values().to_vec();
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 1, 2, 2]);
    }
}
