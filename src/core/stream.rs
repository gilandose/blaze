//! Where a worker stands in its input stream.
//!
//! See [design 010](../../docs/design/010-stream-position.md). Two ideas:
//!
//! - A [`StreamPosition`] is a map of partition to highest-applied offset, not a
//!   scalar, because Kafka assigns offsets per partition and offset 5000 in
//!   partition 3 has nothing to do with offset 5000 in partition 7.
//! - A [`StreamId`] names the stream those offsets are in. Offsets without it are
//!   numbers without units: a worker pointed at a renamed or rebuilt topic
//!   resumes at an offset that exists and means something else, hydrates cleanly,
//!   and produces a table that is wrong in a way nothing downstream can detect.
//!
//! The order on positions is **partial** — `{0:5, 1:9}` and `{0:9, 1:5}` neither
//! dominates the other — which matters in exactly two places: deciding when a
//! buffered segment is durable, and checking that a commit advances. It does not
//! reach layer resolution, which is ordered by catalog sequence and stays total.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A partition of the input stream. Single-partition sources use `0`.
pub type PartitionId = u32;

/// Highest offset applied per partition, inclusive.
///
/// A partition absent from the map has had nothing applied, and offset `0` is
/// reserved for precisely that meaning, so absent and zero agree. That is what
/// lets a topic **grow** partitions without any metadata describing the topology:
/// a position written before an expansion simply has no entry for the new
/// partition, which reads as 0, which is where that partition genuinely starts.
///
/// `BTreeMap` rather than `HashMap` so the serialized form is deterministic —
/// two workers committing the same position produce byte-identical metadata, and
/// the metadata object is written with a conditional put.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamPosition {
    offsets: BTreeMap<PartitionId, u64>,
}

impl StreamPosition {
    pub fn new() -> Self {
        Self::default()
    }

    /// A single-partition position, which is what a file log or a
    /// single-partition topic produces.
    pub fn single(offset: u64) -> Self {
        let mut p = Self::new();
        p.advance(0, offset);
        p
    }

    /// Highest offset applied to `partition`; `0` if nothing has been.
    pub fn get(&self, partition: PartitionId) -> u64 {
        self.offsets.get(&partition).copied().unwrap_or(0)
    }

    /// Record that `partition` has been applied through `offset`.
    ///
    /// Takes the maximum rather than overwriting, so a caller that observes
    /// records out of order — or replays a prefix — cannot walk a partition
    /// backwards. Going backwards is the one thing that would let already-durable
    /// records be written a second time.
    pub fn advance(&mut self, partition: PartitionId, offset: u64) {
        let slot = self.offsets.entry(partition).or_insert(0);
        *slot = (*slot).max(offset);
    }

    /// Take the per-partition maximum of both.
    pub fn merge(&mut self, other: &StreamPosition) {
        for (&p, &o) in &other.offsets {
            self.advance(p, o);
        }
    }

    /// Whether every partition in `other` is at or behind `self`.
    ///
    /// This is the comparison that replaces `<=` on the old scalar watermark.
    /// Note it is not a total order: two positions may each fail to dominate the
    /// other, and code that assumes otherwise is assuming a single partition.
    pub fn dominates(&self, other: &StreamPosition) -> bool {
        other.offsets.iter().all(|(p, off)| self.get(*p) >= *off)
    }

    /// Partitions with a recorded offset, ascending.
    pub fn partitions(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.offsets.keys().copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (PartitionId, u64)> + '_ {
        self.offsets.iter().map(|(&p, &o)| (p, o))
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// The scalar equivalent, when there is one.
    ///
    /// `Some` only for a position that is empty or covers partition 0 alone —
    /// the shapes where a `u64` watermark is not an approximation but the same
    /// number. Anything else returns `None`, because there is no scalar that
    /// describes it and inventing one is how a reader ends up confidently wrong.
    pub fn as_scalar(&self) -> Option<u64> {
        match self.offsets.len() {
            0 => Some(0),
            1 => self.offsets.get(&0).copied(),
            _ => None,
        }
    }
}

impl FromIterator<(PartitionId, u64)> for StreamPosition {
    fn from_iter<T: IntoIterator<Item = (PartitionId, u64)>>(iter: T) -> Self {
        let mut p = Self::new();
        for (partition, offset) in iter {
            p.advance(partition, offset);
        }
        p
    }
}

impl std::fmt::Display for StreamPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.offsets.is_empty() {
            return write!(f, "(nothing)");
        }
        let mut first = true;
        for (p, o) in &self.offsets {
            if !first {
                write!(f, ",")?;
            }
            write!(f, "{p}:{o}")?;
            first = false;
        }
        Ok(())
    }
}

/// What stream a set of offsets is measured in.
///
/// Recorded on every snapshot because an offset without it is a number without
/// units. `group` is here because operators reconcile blaze's committed position
/// against the broker's committed offsets, and **those two are supposed to
/// differ** — a consumer group's offset is a checkpoint its client library
/// advances on its own cadence, while blaze's position is what a snapshot
/// durably covers. Without knowing the group, nobody can tell which pair of
/// numbers to compare.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamId {
    /// What kind of thing assigned these offsets: `kafka`, `file`, `api`.
    pub source: String,
    /// Topic name, or the log path.
    pub name: String,
    /// Consumer group or client identity, where the source has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

impl StreamId {
    pub fn new(source: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            name: name.into(),
            group: None,
        }
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Whether offsets committed against `self` may be resumed against `other`.
    ///
    /// Deliberately ignores `group`: which consumer group read the records does
    /// not change what offset 5000 of a topic *is*. Changing groups is a routine
    /// operation and must not invalidate a committed position; changing topics
    /// is not.
    pub fn same_stream(&self, other: &StreamId) -> bool {
        self.source == other.source && self.name == other.name
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.source, self.name)?;
        if let Some(g) = &self.group {
            write!(f, " (group {g})")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_and_zero_agree() {
        let p = StreamPosition::new();
        assert_eq!(p.get(7), 0);
        assert!(p.is_empty());

        // Which is what makes partition growth free: a position written before
        // the topic expanded reads the new partition as "start from the
        // beginning", because that is where it genuinely starts.
        let before: StreamPosition = [(0, 100), (1, 200)].into_iter().collect();
        assert_eq!(before.get(9), 0);
    }

    #[test]
    fn advance_takes_the_maximum() {
        let mut p = StreamPosition::single(50);
        p.advance(0, 20);
        assert_eq!(p.get(0), 50, "a partition must not walk backwards");
        p.advance(0, 70);
        assert_eq!(p.get(0), 70);
    }

    /// The property the whole design rests on: this is a partial order, and two
    /// positions can be mutually incomparable. Anything written as if `<=` were
    /// available is assuming a single partition.
    #[test]
    fn domination_is_partial() {
        let a: StreamPosition = [(0, 5), (1, 9)].into_iter().collect();
        let b: StreamPosition = [(0, 9), (1, 5)].into_iter().collect();
        assert!(!a.dominates(&b));
        assert!(!b.dominates(&a));

        let both: StreamPosition = [(0, 9), (1, 9)].into_iter().collect();
        assert!(both.dominates(&a) && both.dominates(&b));
        assert!(!a.dominates(&both));
        assert!(a.dominates(&a), "domination is reflexive");
    }

    #[test]
    fn a_partition_that_is_absent_counts_as_covered() {
        // Segment spans partition 0 only; the committed position never mentions
        // partition 1, but does not need to.
        let committed = StreamPosition::single(100);
        let segment = StreamPosition::single(90);
        assert!(committed.dominates(&segment));

        // But a segment that *does* touch partition 1 is not covered by it.
        let spans_two: StreamPosition = [(0, 90), (1, 1)].into_iter().collect();
        assert!(!committed.dominates(&spans_two));
    }

    #[test]
    fn merge_is_the_per_partition_maximum() {
        let mut a: StreamPosition = [(0, 5), (1, 9)].into_iter().collect();
        let b: StreamPosition = [(0, 9), (2, 3)].into_iter().collect();
        a.merge(&b);
        assert_eq!(a.get(0), 9);
        assert_eq!(a.get(1), 9);
        assert_eq!(a.get(2), 3);
        assert!(a.dominates(&b));
    }

    /// A scalar exists only where it is exact. Inventing one for a
    /// multi-partition position is how an old reader ends up confidently wrong,
    /// so there is no such value.
    #[test]
    fn only_single_partition_positions_have_a_scalar() {
        assert_eq!(StreamPosition::new().as_scalar(), Some(0));
        assert_eq!(StreamPosition::single(42).as_scalar(), Some(42));
        let two: StreamPosition = [(0, 1), (1, 2)].into_iter().collect();
        assert_eq!(two.as_scalar(), None);
        // Partition 3 alone has no scalar either: the number would be right but
        // the partition it belongs to would be lost.
        let odd: StreamPosition = [(3, 7)].into_iter().collect();
        assert_eq!(odd.as_scalar(), None);
    }

    #[test]
    fn serialization_is_deterministic_and_ordered() {
        let a: StreamPosition = [(2, 3), (0, 1), (1, 2)].into_iter().collect();
        let b: StreamPosition = [(1, 2), (2, 3), (0, 1)].into_iter().collect();
        let ja = serde_json::to_string(&a).unwrap();
        assert_eq!(ja, serde_json::to_string(&b).unwrap());
        assert_eq!(ja, r#"{"0":1,"1":2,"2":3}"#);
        assert_eq!(serde_json::from_str::<StreamPosition>(&ja).unwrap(), a);
    }

    #[test]
    fn changing_consumer_group_does_not_change_the_stream() {
        let a = StreamId::new("kafka", "edges").with_group("blaze-1");
        let b = StreamId::new("kafka", "edges").with_group("blaze-2");
        assert!(
            a.same_stream(&b),
            "a group change must not orphan a position"
        );

        let other_topic = StreamId::new("kafka", "edges-v2");
        assert!(!a.same_stream(&other_topic));
        let other_source = StreamId::new("file", "edges");
        assert!(!a.same_stream(&other_source));
    }
}
