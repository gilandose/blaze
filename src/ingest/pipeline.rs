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

use crate::core::{EdgeEvent, ScopedForest};

use super::buffer::EdgeBuffer;
use super::log::Record;

#[derive(Debug, Default)]
pub struct PipelineStats {
    /// Highest offset assigned so far (starts at the recovery watermark, so
    /// this is the stream position, not an events-since-boot count).
    pub last_offset: AtomicU64,
}

impl PipelineStats {
    /// Current stream position: the last assigned offset, which equals the
    /// recovery watermark until the first event arrives.
    pub fn last_offset(&self) -> u64 {
        self.last_offset.load(Ordering::Relaxed)
    }
}

pub struct Pipeline {
    pub forest: Arc<ScopedForest>,
    pub buffer: Arc<EdgeBuffer>,
    pub stats: Arc<PipelineStats>,
}

impl Pipeline {
    /// Start the pipeline with offsets resuming after `start_offset`
    /// (the committed watermark on recovery; 0 for a fresh worker).
    pub fn new(forest: Arc<ScopedForest>, buffer: Arc<EdgeBuffer>, start_offset: u64) -> Self {
        let stats = Arc::new(PipelineStats {
            last_offset: AtomicU64::new(start_offset),
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
    /// consumes offsets without producing a record. Going *backwards* within a
    /// batch is not, and is a bug in the source rather than something to absorb.
    pub fn apply_batch(&self, records: &[Record]) -> anyhow::Result<u64> {
        let mut last = self.stats.last_offset.load(Ordering::Relaxed);
        let mut seen = 0u64;
        for record in records {
            // Zero is reserved: the watermark uses it for "nothing committed",
            // so a record numbered zero could not be distinguished from one that
            // was never applied. Sources over a zero-based log add one.
            if record.offset == 0 {
                anyhow::bail!("log delivered offset 0, which is reserved for an empty watermark");
            }
            if record.offset <= seen {
                anyhow::bail!(
                    "log delivered offset {} after {seen}: a partition must be \
                     ordered, and blaze consumes exactly one",
                    record.offset
                );
            }
            seen = record.offset;
            if record.offset <= last {
                continue; // already committed; at-least-once redelivery
            }
            self.forest.apply(&record.event);
            self.buffer.append(record.offset, &record.event);
            last = record.offset;
        }
        self.stats.last_offset.store(last, Ordering::Relaxed);
        Ok(last)
    }

    /// Drain the channel until it closes, minting offsets locally.
    pub async fn run(&self, mut rx: mpsc::Receiver<EdgeEvent>) {
        // Modest batching keeps channel overhead off the hot path.
        let mut batch = Vec::with_capacity(256);
        while rx.recv_many(&mut batch, 256).await > 0 {
            for event in batch.drain(..) {
                let offset = self.stats.last_offset.fetch_add(1, Ordering::Relaxed) + 1;
                self.forest.apply(&event);
                self.buffer.append(offset, &event);
            }
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
        assert_eq!((seg.min_offset, seg.max_offset), (101, 101));
    }
}
