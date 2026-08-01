//! The single-writer ingest pipeline.
//!
//! One task applies each edge to the in-memory forest (DSU merges) and appends
//! it to the Arrow buffer, under an offset. Keeping DSU mutation single-writer
//! is what lets the query path stay lock-free.
//!
//! Offsets arrive one of two ways, and the difference matters:
//!
//! - [`apply_batch`](Pipeline::apply_batch) takes offsets **assigned by a log**.
//!   Every consumer of that log sees the same number against the same record, so
//!   a committed watermark means the same thing on every worker and a restart is
//!   just "resume after it".
//! - [`run`](Pipeline::run) **mints** offsets from a local counter for events
//!   arriving over the API, which have no log position. That numbering is private
//!   to one worker: it is only meaningful as long as that worker is the only one
//!   producing it, from an unbroken sequence of events.
//!
//! Mixing the two on one worker would interleave minted and log-assigned offsets
//! in the same space, so the binary picks one — see `--edge-log`, which refuses
//! injection while it is set.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

use crate::core::{EdgeEvent, ScopedForest, StreamPosition};

use super::buffer::EdgeBuffer;
use super::log::Record;

#[derive(Debug, Default)]
pub struct PipelineStats {
    /// Highest offset applied on partition 0, kept as an atomic because the API
    /// reports it on a hot path and a single-partition worker never needs more.
    pub last_offset: AtomicU64,
    /// Highest offset applied per partition (starts at the recovery position,
    /// so this is where the worker stands in the stream, not an
    /// events-since-boot count).
    position: parking_lot::Mutex<StreamPosition>,
}

impl PipelineStats {
    /// Position on partition 0. Retained because a single-partition deployment
    /// is the common case and the API exposes a scalar; prefer
    /// [`position`](Self::position) for anything that must be right on a
    /// multi-partition stream.
    pub fn last_offset(&self) -> u64 {
        self.last_offset.load(Ordering::Relaxed)
    }

    /// Where the worker stands in every partition it has seen.
    pub fn position(&self) -> StreamPosition {
        self.position.lock().clone()
    }
}

pub struct Pipeline {
    pub forest: Arc<ScopedForest>,
    pub buffer: Arc<EdgeBuffer>,
    pub stats: Arc<PipelineStats>,
}

impl Pipeline {
    /// Start the pipeline resuming after `start_offset` on partition 0 — the
    /// single-partition convenience form.
    pub fn new(forest: Arc<ScopedForest>, buffer: Arc<EdgeBuffer>, start_offset: u64) -> Self {
        Self::resuming(forest, buffer, StreamPosition::single(start_offset))
    }

    /// Start the pipeline resuming from a committed position — every partition
    /// picks up where the last snapshot left it.
    pub fn resuming(
        forest: Arc<ScopedForest>,
        buffer: Arc<EdgeBuffer>,
        start: StreamPosition,
    ) -> Self {
        let stats = Arc::new(PipelineStats {
            last_offset: AtomicU64::new(start.get(0)),
            position: parking_lot::Mutex::new(start),
        });
        Self {
            forest,
            buffer,
            stats,
        }
    }

    /// Apply a batch of log records under the offsets the log gave them,
    /// returning the new stream position.
    ///
    /// Records at or below the current position are **skipped, not re-applied**.
    /// A log delivers at least once — a consumer that dies between applying a
    /// record and committing its offset sees that record again — so duplicates
    /// are expected rather than exceptional. Re-applying one would be harmless
    /// for the forest, since a union is idempotent, but it would append a second
    /// copy to the Arrow buffer and so write a duplicate row to Parquet. The
    /// watermark is what makes the dedup exact.
    ///
    /// Gaps are fine: a compacted topic has them, and so does anything that
    /// consumes offsets without producing a record. Going *backwards within one
    /// partition* is not, and is a bug in the source rather than something to
    /// absorb. Records from different partitions may interleave freely — they
    /// are separate ordered logs and nothing orders them against each other.
    pub fn apply_batch(&self, records: &[Record]) -> anyhow::Result<u64> {
        let mut position = self.stats.position.lock();
        // Highest offset seen *in this batch*, per partition, which is what
        // catches a source delivering one partition out of order. Kept apart
        // from `position` because that one is also advanced by dedup skips.
        let mut seen = StreamPosition::new();
        for record in records {
            // Zero is reserved: a position uses it for "nothing applied", so a
            // record numbered zero could not be distinguished from one that was
            // never seen. Sources over a zero-based log add one.
            if record.offset == 0 {
                anyhow::bail!("log delivered offset 0, which is reserved for an empty position");
            }
            let previous = seen.get(record.partition);
            if record.offset <= previous && previous != 0 {
                anyhow::bail!(
                    "log delivered offset {} after {previous} on partition {}: \
                     a partition is an ordered log",
                    record.offset,
                    record.partition,
                );
            }
            seen.advance(record.partition, record.offset);
            if record.offset <= position.get(record.partition) {
                continue; // already applied; at-least-once redelivery
            }
            self.forest.apply(&record.event);
            self.buffer
                .append(record.partition, record.offset, &record.event);
            position.advance(record.partition, record.offset);
        }
        let zero = position.get(0);
        drop(position);
        self.stats.last_offset.store(zero, Ordering::Relaxed);
        Ok(zero)
    }

    /// Drain the channel until it closes, minting offsets locally.
    ///
    /// Minted offsets always land on partition 0: there is no log to say
    /// otherwise, and this numbering is private to one worker anyway.
    pub async fn run(&self, mut rx: mpsc::Receiver<EdgeEvent>) {
        // Modest batching keeps channel overhead off the hot path.
        let mut batch = Vec::with_capacity(256);
        while rx.recv_many(&mut batch, 256).await > 0 {
            for event in batch.drain(..) {
                let offset = self.stats.last_offset.fetch_add(1, Ordering::Relaxed) + 1;
                self.forest.apply(&event);
                self.buffer.append(0, offset, &event);
            }
            self.stats
                .position
                .lock()
                .advance(0, self.stats.last_offset.load(Ordering::Relaxed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Visibility;

    #[tokio::test]
    async fn pipeline_applies_and_buffers() {
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        let pipeline = Pipeline::new(forest.clone(), buffer.clone(), 100);

        let (tx, rx) = mpsc::channel(16);
        let handle = {
            let p = Pipeline {
                forest: pipeline.forest.clone(),
                buffer: pipeline.buffer.clone(),
                stats: pipeline.stats.clone(),
            };
            tokio::spawn(async move { p.run(rx).await })
        };

        tx.send(EdgeEvent {
            src: 500,
            dst: 105,
            visibility: Visibility::Global,
            event_time_ms: 0,
            props: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.await.unwrap();

        assert!(forest.connected(0, 500, 105));
        assert_eq!(pipeline.stats.last_offset(), 101);
        let seg = buffer.seal_active().unwrap();
        // Offsets resume after the recovery watermark.
        assert_eq!(seg.first, StreamPosition::single(101));
        assert_eq!(seg.last, StreamPosition::single(101));
    }
}
