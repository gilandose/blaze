//! Ingest from a partitioned, ordered, replayable log.
//!
//! # Why the offset has to come from the log
//!
//! The pipeline used to mint offsets itself, with a `fetch_add` per event. That
//! is fine for a single worker generating its own stream and wrong for
//! everything else, because **the offset is what the committed watermark is
//! measured in**. Two workers reading the same events number them from their own
//! counters; the numbering agrees only as long as both saw exactly the same
//! events from exactly the same starting point. After a failover it does not,
//! and a snapshot that says "committed through 4 001 337" means one thing to the
//! worker that wrote it and something else to the worker that reads it back.
//!
//! A log fixes this by being the authority. Kafka assigns the offset once, at
//! the broker, and every consumer of that partition sees the same number against
//! the same record. The watermark then means the same thing everywhere, restart
//! is "resume after the watermark", and the replayed suffix is exactly the events
//! that were applied but not committed.
//!
//! So [`LogSource`] carries the offset with the record rather than leaving the
//! consumer to invent one.
//!
//! # Why the trait is synchronous
//!
//! Applying an edge is CPU work — two union-finds and an Arrow append — and it
//! is single-writer by design, which is what lets the query path stay lock-free.
//! Running that on the async runtime means the ingest loop competes with query
//! handlers for the same workers and can stall them for as long as a batch takes.
//! The consumer therefore owns a blocking thread and the trait is a plain `fn`.
//!
//! This also happens to be the shape real clients offer: `rdkafka`'s
//! `BaseConsumer::poll(timeout)` is [`LogSource::poll`] with a different name.
//!
//! # Partitions
//!
//! A source may present several partitions, each its own ordered log with its
//! own offsets, and a [`StreamPosition`] checkpoints all of them. Consumption
//! stays **single-writer**: one thread polls every partition and applies what it
//! gets, because that is what keeps the query path lock-free. Partitions buy
//! correctness against a partitioned topic, not parallelism.
//!
//! Nothing orders records of different partitions against each other, and
//! nothing needs to — DSU merges commute, so the forest is the same whatever
//! interleaving arrives. The offset is bookkeeping for exactly-once *buffering*,
//! not a serialization requirement. See design 010.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::core::{EdgeEvent, PartitionId, StreamPosition};
use crate::ingest::Pipeline;

/// One record from the log: the event, and the offset the log gave it.
///
/// Offsets are **one-based and strictly increasing**. Zero is reserved, because
/// the committed watermark spends it on "nothing committed" — a record numbered
/// zero could not be told apart from one that was never applied. A source over a
/// zero-based log, which is to say Kafka, adds one on the way through; that is
/// the entire adaptation.
///
/// Gaps are allowed. A compacted topic has them, and so does anything that
/// consumes offsets without yielding a record.
#[derive(Debug, Clone)]
pub struct Record {
    /// Which ordered log this came from. Single-partition sources use `0`.
    pub partition: PartitionId,
    pub offset: u64,
    pub event: EdgeEvent,
}

/// A single ordered partition that can be replayed from an offset.
///
/// Implementations are consumed from a dedicated blocking thread, one at a time,
/// so they need no interior synchronisation.
pub trait LogSource: Send {
    /// Fetch up to `max` records, in offset order, starting where the last poll
    /// left off.
    ///
    /// An empty batch means "nothing available right now", not end of stream —
    /// the same distinction a Kafka poll timeout makes. Returning empty forever
    /// is how a source at the head of a live topic behaves.
    fn poll(&mut self, max: usize) -> anyhow::Result<Vec<Record>>;

    /// Position every partition so the next [`poll`](Self::poll) returns the
    /// first record after the given position. An absent partition starts at the
    /// beginning of its log.
    ///
    /// This is the recovery primitive: a worker hydrates to the committed
    /// position and seeks to it, and the records it then replays are exactly
    /// the ones that were applied but never committed.
    fn seek(&mut self, position: &StreamPosition) -> anyhow::Result<()>;

    /// Single-partition convenience: seek partition 0 alone.
    fn seek_after(&mut self, offset: u64) -> anyhow::Result<()> {
        self.seek(&StreamPosition::single(offset))
    }

    /// Whether more records may yet arrive. A source over a finite file that is
    /// not being followed reports `false` once it is drained, which is what lets
    /// a batch run terminate instead of spinning.
    fn is_live(&self) -> bool {
        true
    }
}

/// A newline-delimited JSON file consumed as a log.
///
/// One `EdgeEvent` per line, and **the offset is the line number**, counting
/// from one. That is not quite Kafka's numbering — Kafka starts at zero — but it
/// matches the existing watermark convention, where `0` means "nothing committed"
/// rather than "the first record is committed", so the boundary case stays
/// unambiguous.
///
/// The mapping is worth stating plainly because it is the whole reason this is a
/// useful stand-in: line number is an offset that is *stable across readers and
/// across restarts*, assigned by something other than the consumer, and
/// replayable by seeking. Those are the properties of a Kafka offset that blaze
/// depends on, and a file has all of them.
///
/// What it does not simulate: leader election among brokers, rebalancing,
/// retention dropping the prefix out from under a slow consumer, or a producer
/// that can reorder within a partition (Kafka cannot either, without retries and
/// `max.in.flight > 1`).
pub struct FileLog {
    /// Which partition this file *is*. Zero on its own; a
    /// [`PartitionedLog`] assigns one per file.
    partition: PartitionId,
    reader: BufReader<File>,
    /// Offset of the last record returned; the next line is `next + 1`.
    next: u64,
    /// Keep waiting at end of file for a writer to append, rather than
    /// reporting the log drained.
    follow: bool,
    /// Reused between lines so a poll of 10 000 records is one allocation.
    line: String,
}

impl FileLog {
    /// Open a log file, positioned at the beginning.
    pub fn open(path: impl AsRef<Path>, follow: bool) -> anyhow::Result<Self> {
        let file = File::open(path.as_ref())
            .map_err(|e| anyhow::anyhow!("opening edge log {}: {e}", path.as_ref().display()))?;
        Ok(Self {
            partition: 0,
            reader: BufReader::with_capacity(1 << 20, file),
            next: 0,
            follow,
            line: String::new(),
        })
    }

    /// Label this file as a given partition of a larger stream.
    pub fn as_partition(mut self, partition: PartitionId) -> Self {
        self.partition = partition;
        self
    }

    /// Read one line, or `None` at end of file.
    ///
    /// A line that does not end in a newline is a record the writer is still
    /// appending. Reading it would produce a truncated event *and* consume the
    /// bytes, so the complete record could never be read; instead the read is
    /// rewound and the caller sees end of file, which in follow mode means it
    /// tries again in a moment. This is the difference between a tail that works
    /// against a live writer and one that corrupts every record it races.
    fn read_line(&mut self) -> anyhow::Result<Option<&str>> {
        self.line.clear();
        let n = self.reader.read_line(&mut self.line)?;
        if n == 0 {
            return Ok(None);
        }
        if !self.line.ends_with('\n') {
            self.reader.seek_relative(-(n as i64))?;
            return Ok(None);
        }
        Ok(Some(self.line.trim_end_matches(['\n', '\r'])))
    }
}

impl LogSource for FileLog {
    fn poll(&mut self, max: usize) -> anyhow::Result<Vec<Record>> {
        let mut out = Vec::with_capacity(max.min(4096));
        while out.len() < max {
            let Some(line) = self.read_line()? else { break };
            if line.is_empty() {
                // A blank line would break the line-number-is-offset mapping if
                // it were skipped, so it is an error rather than a shrug.
                anyhow::bail!("blank line at offset {}", self.next + 1);
            }
            let event: EdgeEvent = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("offset {}: {e}", self.next + 1))?;
            self.next += 1;
            out.push(Record {
                partition: self.partition,
                offset: self.next,
                event,
            });
        }
        Ok(out)
    }

    fn seek(&mut self, position: &StreamPosition) -> anyhow::Result<()> {
        let offset = position.get(self.partition);
        // A linear scan, because a plain file has no offset index. Kafka does
        // this with a sparse `.index` per segment; the cost here is one pass
        // over the already-consumed prefix at startup, paid once.
        self.reader.seek(SeekFrom::Start(0))?;
        self.next = 0;
        let mut scratch = Vec::new();
        while self.next < offset {
            scratch.clear();
            if self.reader.read_until(b'\n', &mut scratch)? == 0 {
                anyhow::bail!(
                    "cannot resume partition {} at offset {offset}: the log ends at {}. \
                     The committed position is past the end of this log, which \
                     usually means the worker is pointed at a different stream \
                     than the one its snapshots were built from.",
                    self.partition,
                    self.next
                );
            }
            self.next += 1;
        }
        Ok(())
    }

    fn is_live(&self) -> bool {
        self.follow
    }
}

/// Several partitions consumed as one stream.
///
/// Files named `partition-<n>.ndjson` in a directory, each its own ordered log
/// with its own offsets — the shape of a partitioned topic, with none of the
/// broker.
///
/// A poll takes a slice from every partition in turn rather than draining one
/// before starting the next. That is not cosmetic: draining in order would mean
/// a snapshot committed mid-catch-up covers partition 0 completely and the rest
/// not at all, so the *incomparable* positions this design exists to handle
/// would never actually arise in a test. Round-robin makes the interesting case
/// the normal one.
pub struct PartitionedLog {
    parts: Vec<FileLog>,
}

impl PartitionedLog {
    /// Open every `partition-<n>.ndjson` in `dir`, lowest partition first.
    pub fn open_dir(dir: impl AsRef<Path>, follow: bool) -> anyhow::Result<Self> {
        let dir = dir.as_ref();
        let mut found: Vec<(PartitionId, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(dir)
            .map_err(|e| anyhow::anyhow!("reading edge log directory {}: {e}", dir.display()))?
        {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(rest) = name.strip_prefix("partition-")
                && let Some(id) = rest.strip_suffix(".ndjson")
                && let Ok(id) = id.parse::<PartitionId>()
            {
                found.push((id, path));
            }
        }
        anyhow::ensure!(
            !found.is_empty(),
            "no partition-<n>.ndjson files in {}",
            dir.display()
        );
        found.sort_unstable();
        let parts = found
            .into_iter()
            .map(|(id, path)| FileLog::open(path, follow).map(|f| f.as_partition(id)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self { parts })
    }

    pub fn partitions(&self) -> usize {
        self.parts.len()
    }
}

impl LogSource for PartitionedLog {
    fn poll(&mut self, max: usize) -> anyhow::Result<Vec<Record>> {
        let share = max.div_ceil(self.parts.len()).max(1);
        let mut out = Vec::with_capacity(max.min(4096));
        for part in &mut self.parts {
            out.extend(part.poll(share)?);
        }
        Ok(out)
    }

    fn seek(&mut self, position: &StreamPosition) -> anyhow::Result<()> {
        // Each partition reads its own entry, and one absent from the position
        // seeks to the start of its log — which is where it genuinely begins.
        for part in &mut self.parts {
            part.seek(position)?;
        }
        Ok(())
    }

    fn is_live(&self) -> bool {
        self.parts.iter().any(|p| p.is_live())
    }
}

/// So a caller that picks a source at runtime — a directory or a single file —
/// can still hand it to [`consume`], which is generic.
impl LogSource for Box<dyn LogSource> {
    fn poll(&mut self, max: usize) -> anyhow::Result<Vec<Record>> {
        (**self).poll(max)
    }
    fn seek(&mut self, position: &StreamPosition) -> anyhow::Result<()> {
        (**self).seek(position)
    }
    fn is_live(&self) -> bool {
        (**self).is_live()
    }
}

/// How the consumer behaves when the log has nothing to give.
#[derive(Debug, Clone, Copy)]
pub struct ConsumerConfig {
    /// Records per poll. Batching is the point: the HTTP and gRPC paths take one
    /// event per request, so a batch here is what turns ingest from
    /// request-bound into memory-bound.
    pub batch: usize,
    /// How long to wait before polling a live source that returned nothing.
    pub idle: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            batch: 10_000,
            idle: Duration::from_millis(20),
        }
    }
}

/// Drive a source into the pipeline until it drains or `stop` is set.
///
/// Returns the last offset applied. Runs the caller's thread flat out; the
/// intent is `spawn_blocking`, so DSU application never occupies an async
/// worker.
pub fn consume<S: LogSource>(
    mut source: S,
    pipeline: &Pipeline,
    cfg: ConsumerConfig,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<u64> {
    let mut last = pipeline.stats.last_offset();
    while !stop.load(Ordering::Relaxed) {
        let batch = source.poll(cfg.batch)?;
        if batch.is_empty() {
            if !source.is_live() {
                break;
            }
            std::thread::sleep(cfg.idle);
            continue;
        }
        last = pipeline.apply_batch(&batch)?;
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ScopedForest, Visibility};
    use crate::ingest::EdgeBuffer;
    use std::io::Write;

    fn event(src: u64, dst: u64) -> EdgeEvent {
        EdgeEvent {
            src,
            dst,
            visibility: Visibility::Global,
            event_time_ms: 7,
            props: None,
        }
    }

    fn log_file(events: &[EdgeEvent]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for e in events {
            writeln!(f, "{}", serde_json::to_string(e).unwrap()).unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn offsets_are_line_numbers_from_one() {
        let f = log_file(&[event(1, 2), event(3, 4), event(5, 6)]);
        let mut log = FileLog::open(f.path(), false).unwrap();
        let got = log.poll(100).unwrap();
        assert_eq!(
            got.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(got[2].event.src, 5);
        assert!(log.poll(100).unwrap().is_empty());
    }

    #[test]
    fn a_poll_is_capped_and_resumes_where_it_stopped() {
        let all: Vec<_> = (0..10).map(|i| event(i, i + 1)).collect();
        let f = log_file(&all);
        let mut log = FileLog::open(f.path(), false).unwrap();

        let first = log.poll(4).unwrap();
        let second = log.poll(4).unwrap();
        let third = log.poll(4).unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(second.len(), 4);
        assert_eq!(third.len(), 2);
        assert_eq!(first[0].offset, 1);
        assert_eq!(second[0].offset, 5);
        assert_eq!(third[1].offset, 10);
    }

    /// The recovery primitive: seeking to a watermark replays exactly the
    /// records after it, with their original offsets.
    #[test]
    fn seek_after_replays_the_suffix_with_its_original_offsets() {
        let all: Vec<_> = (0..10).map(|i| event(i, i + 1)).collect();
        let f = log_file(&all);
        let mut log = FileLog::open(f.path(), false).unwrap();

        log.seek_after(6).unwrap();
        let got = log.poll(100).unwrap();
        assert_eq!(
            got.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![7, 8, 9, 10]
        );
        // Offsets survive the round trip: the record at offset 7 is the seventh
        // event written, not the first one replayed.
        assert_eq!(got[0].event.src, 6);

        // And seeking to zero is the whole log.
        log.seek_after(0).unwrap();
        assert_eq!(log.poll(100).unwrap().len(), 10);
    }

    /// A watermark past the end of the log is a misconfiguration, not an empty
    /// read: the worker is pointed at a stream its snapshots did not come from.
    #[test]
    fn seeking_past_the_end_is_an_error() {
        let f = log_file(&[event(1, 2)]);
        let mut log = FileLog::open(f.path(), false).unwrap();
        let err = log.seek_after(99).unwrap_err().to_string();
        assert!(err.contains("the log ends at 1"), "{err}");
    }

    /// The case that separates a working tail from one that corrupts records it
    /// races: a half-written line must be left alone until its newline lands.
    #[test]
    fn a_partially_written_record_is_not_consumed_until_complete() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "{}", serde_json::to_string(&event(1, 2)).unwrap()).unwrap();
        let whole = serde_json::to_string(&event(3, 4)).unwrap();
        let (head, tail) = whole.split_at(whole.len() / 2);
        write!(f, "{head}").unwrap();
        f.flush().unwrap();

        let mut log = FileLog::open(f.path(), true).unwrap();
        let first = log.poll(100).unwrap();
        assert_eq!(first.len(), 1, "read a torn record");
        assert!(log.poll(100).unwrap().is_empty());

        // The writer finishes the line; now it reads, whole, at the right offset.
        writeln!(f, "{tail}").unwrap();
        f.flush().unwrap();
        let second = log.poll(100).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].offset, 2);
        assert_eq!(second[0].event.dst, 4);
    }

    /// A follow-mode source keeps returning nothing rather than reporting the
    /// log finished, and picks up what a writer appends after it caught up.
    #[test]
    fn follow_mode_picks_up_appends() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "{}", serde_json::to_string(&event(1, 2)).unwrap()).unwrap();
        f.flush().unwrap();

        let mut log = FileLog::open(f.path(), true).unwrap();
        assert_eq!(log.poll(100).unwrap().len(), 1);
        assert!(log.poll(100).unwrap().is_empty());
        assert!(log.is_live(), "a followed log is never drained");

        writeln!(f, "{}", serde_json::to_string(&event(9, 9)).unwrap()).unwrap();
        f.flush().unwrap();
        let got = log.poll(100).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].offset, 2);
    }

    #[test]
    fn a_malformed_record_names_its_offset() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "{}", serde_json::to_string(&event(1, 2)).unwrap()).unwrap();
        writeln!(f, "{{not json").unwrap();
        f.flush().unwrap();

        let mut log = FileLog::open(f.path(), false).unwrap();
        let err = log.poll(100).unwrap_err().to_string();
        assert!(err.starts_with("offset 2:"), "{err}");
    }

    #[test]
    fn consume_drains_a_finite_log_into_the_pipeline() {
        let all: Vec<_> = (0..50).map(|i| event(i, i + 1)).collect();
        let f = log_file(&all);
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        let pipeline = Pipeline::new(forest.clone(), buffer.clone(), 0);

        let last = consume(
            FileLog::open(f.path(), false).unwrap(),
            &pipeline,
            ConsumerConfig {
                batch: 7,
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

        assert_eq!(last, 50);
        assert_eq!(pipeline.stats.last_offset(), 50);
        // 0-1-2-...-50 is one chain, so everything is connected.
        assert!(forest.connected(0, 0, 50));
    }

    /// Resumption end to end: hydrate to a watermark, seek, consume, and the
    /// buffer holds exactly the suffix — nothing before the watermark is
    /// re-buffered and so nothing is double-written on the next flush.
    #[test]
    fn consuming_after_a_watermark_buffers_only_the_suffix() {
        let all: Vec<_> = (0..20).map(|i| event(i, i + 1)).collect();
        let f = log_file(&all);
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        let pipeline = Pipeline::new(forest.clone(), buffer.clone(), 12);

        let mut log = FileLog::open(f.path(), false).unwrap();
        log.seek_after(12).unwrap();
        consume(
            log,
            &pipeline,
            ConsumerConfig::default(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

        let seg = buffer.seal_active().unwrap();
        assert_eq!(seg.first, StreamPosition::single(13));
        assert_eq!(seg.last, StreamPosition::single(20));
    }
}
