//! The single-writer ingest pipeline.
//!
//! One task drains the event channel, assigns monotonically increasing
//! offsets, applies each edge to the in-memory forest (DSU merges), and
//! appends it to the Arrow buffer. Keeping DSU mutation single-writer is what
//! lets the query path stay lock-free; in a log-backed deployment the offset
//! would instead be the log's own offset, giving all replicas an identical
//! numbering.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

use crate::core::{EdgeEvent, ScopedForest};

use super::buffer::EdgeBuffer;

#[derive(Debug, Default)]
pub struct PipelineStats {
    /// Next offset to assign; also the exclusive upper bound of ingested data.
    pub next_offset: AtomicU64,
}

impl PipelineStats {
    pub fn events_ingested(&self) -> u64 {
        self.next_offset.load(Ordering::Relaxed)
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
            next_offset: AtomicU64::new(start_offset),
        });
        Self {
            forest,
            buffer,
            stats,
        }
    }

    /// Drain the channel until it closes.
    pub async fn run(&self, mut rx: mpsc::Receiver<EdgeEvent>) {
        // Modest batching keeps channel overhead off the hot path.
        let mut batch = Vec::with_capacity(256);
        while rx.recv_many(&mut batch, 256).await > 0 {
            for event in batch.drain(..) {
                let offset = self.stats.next_offset.fetch_add(1, Ordering::Relaxed) + 1;
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
        assert_eq!(pipeline.stats.events_ingested(), 101);
        let seg = buffer.seal_active().unwrap();
        // Offsets resume after the recovery watermark.
        assert_eq!((seg.min_offset, seg.max_offset), (101, 101));
    }
}
