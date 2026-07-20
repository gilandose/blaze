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

use crate::core::{EdgeEvent, GLOBAL_SCOPE, Visibility};

/// Schema of the edge table as written to Parquet.
pub fn edge_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
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
    offset: UInt64Builder,
    src: UInt64Builder,
    dst: UInt64Builder,
    scope: UInt32Builder,
    event_time: PrimitiveBuilder<TimestampMillisecondType>,
    props: StringBuilder,
    rows: usize,
    min_offset: u64,
    max_offset: u64,
}

/// An immutable, flush-ready chunk of buffered edges.
#[derive(Clone)]
pub struct Segment {
    pub batch: RecordBatch,
    pub min_offset: u64,
    pub max_offset: u64,
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

    /// Append one event under the given log offset, exploding multi-scope
    /// visibility into one row per scope (global => single row, scope 0).
    pub fn append(&self, offset: u64, event: &EdgeEvent) {
        let mut buf = self.active.lock();
        if buf.rows == 0 {
            buf.min_offset = offset;
        }
        buf.max_offset = offset;
        let scopes: smallvec::SmallVec<[u32; 4]> = match event.visibility.clone().normalize() {
            Visibility::Global => smallvec::smallvec![GLOBAL_SCOPE],
            Visibility::Scoped(s) => s,
        };
        for scope in scopes {
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
            mut offset,
            mut src,
            mut dst,
            mut scope,
            mut event_time,
            mut props,
            min_offset,
            max_offset,
            ..
        } = taken;

        let batch = RecordBatch::try_new(
            edge_schema(),
            vec![
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
                values: batch.column(3).clone(), // scope_id
                options: None,
            },
            SortColumn {
                values: batch.column(1).clone(), // src
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

        let segment = Segment {
            batch,
            min_offset,
            max_offset,
        };
        self.sealed.lock().push(segment.clone());
        Some(segment)
    }

    /// Sealed segments currently pending persistence (cheap Arc clones).
    pub fn sealed_segments(&self) -> Vec<Segment> {
        self.sealed.lock().clone()
    }

    /// Drop sealed segments fully covered by the committed watermark. Called
    /// by followers when they observe a new catalog snapshot, and by the
    /// leader after a successful commit.
    pub fn drop_committed(&self, watermark: u64) -> usize {
        let mut sealed = self.sealed.lock();
        let before = sealed.len();
        sealed.retain(|s| s.max_offset > watermark);
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
        buf.append(1, &ev(1, 2, Visibility::Global));
        buf.append(2, &ev(3, 4, Visibility::Scoped(smallvec![7, 9])));
        assert_eq!(buf.stats().active_rows, 3);

        let seg = buf.seal_active().expect("segment");
        assert_eq!(seg.batch.num_rows(), 3);
        assert_eq!((seg.min_offset, seg.max_offset), (1, 2));
        assert_eq!(buf.stats().active_rows, 0);
        assert_eq!(buf.stats().sealed_segments, 1);

        // Sorted by scope: global row (scope 0) first.
        let scopes = seg
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(scopes.values(), &[0, 7, 9]);

        // Nothing to seal when empty.
        assert!(buf.seal_active().is_none());

        buf.append(3, &ev(5, 6, Visibility::Global));
        buf.seal_active();
        assert_eq!(buf.stats().sealed_segments, 2);

        // Watermark 2 covers only the first segment.
        assert_eq!(buf.drop_committed(2), 1);
        assert_eq!(buf.stats().sealed_segments, 1);
        assert_eq!(buf.drop_committed(3), 1);
        assert_eq!(buf.stats().sealed_segments, 0);
    }
}
