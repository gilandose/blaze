//! Which runs to merge next — size-tiered compaction policy (design 006).
//!
//! # Why the flat policy could not be tuned into working
//!
//! A single base plus one flat run of deltas has no setting that is
//! simultaneously cheap to write and cheap to read. Measured at 145M links, per-
//! pair merge cost is constant while each cycle merges linearly more state, so
//! total merge work is **O(N²)**: extrapolated to 2B links, ~58 hours of
//! compaction against ~13 hours of ingest. Lowering the trigger rewrites the
//! whole base more often; raising it leaves thousands of layers for every lookup
//! to scan.
//!
//! Tiering breaks the coupling by merging a *subset*. Each level holds runs
//! roughly `fanout`× larger than the level below; when a level accumulates
//! `fanout` runs they merge into one run at the next level. A link is then
//! rewritten about once per level it passes through — **O(log N) write
//! amplification per link** instead of O(state) per compaction.
//!
//! # What the policy has to respect
//!
//! Runs resolve oldest-span-first, and the disjoint-keys property holds because a
//! run's keys are composed roots of every run with an earlier span. So a merge may
//! only take a **contiguous** stretch of the stack: leaving a run in the middle of
//! what was merged would move it in the resolution order. Merging a contiguous
//! subset and splicing the result back at the same position changes nothing —
//! [`LayeredBase::slice`](crate::storage::LayeredBase::slice) carries that
//! argument.

use std::ops::Range;

/// Runs per level before they merge into one run at the level above.
///
/// 10 is the standard LSM choice and the one design 006 sized the levels against:
/// at ~126 KB per L0 run it puts a 75 GB base at six levels, so a lookup scans
/// tens of runs rather than thousands, and each link is rewritten ~6 times over
/// its life.
///
/// The direction of the trade is easy to get backwards. Write amplification is
/// roughly the number of levels, `log_T(F)`, so a **larger** fanout rewrites
/// *less* — measured (`examples/tier_amplification`), `T=2` cost 13.58x against
/// `T=10`'s 4.96x on identical ingest. What a large fanout costs is depth:
/// `(T-1)·log_T(F)` runs to probe, since a level holds up to `T-1` runs before it
/// merges. So fanout is chosen for the depth budget, and within that budget
/// bigger is better.
pub const DEFAULT_TIER_FANOUT: usize = 10;

/// Which runs to merge next, as an index range into the stack, oldest first.
///
/// `levels[i]` is the size tier of the `i`th run. `max_runs` is the depth ceiling
/// from `--max-delta-layers`: lookups and ingest both pay per run, so a stack that
/// grows past it should buy depth back even when no level is due.
///
/// `None` means leave the stack alone.
pub fn pick_merge(levels: &[u8], fanout: usize, max_runs: usize) -> Option<Range<usize>> {
    // A fanout below 2 would merge single runs forever, rewriting bytes to
    // achieve nothing.
    let fanout = fanout.max(2);

    // The ordinary case: a level has filled up. Lowest first, because those are
    // the cheapest runs to merge and the ones there are most of.
    if let Some(stretch) = lowest_stretch(levels, fanout) {
        return Some(stretch.start..stretch.start + fanout);
    }

    // No level is due, but the stack is already deeper than lookups should pay
    // for. Merge the lowest stretch we have rather than waiting for one to fill:
    // still a bounded merge — any stretch of `fanout` or more would have been
    // caught above, so this one is smaller than that — unlike the full-base
    // rewrite the flat policy fell back on.
    if levels.len() >= max_runs {
        return lowest_stretch(levels, 2);
    }
    None
}

/// The oldest stretch of at least `min` consecutive runs sharing a level, taking
/// the lowest such level.
///
/// Consecutive is the requirement, not merely same-level: merging runs with
/// another run between them would change that run's place in the resolution
/// order. In practice levels descend monotonically up the stack, so same-level
/// runs are always adjacent — this enforces it rather than trusting it.
fn lowest_stretch(levels: &[u8], min: usize) -> Option<Range<usize>> {
    let mut best: Option<Range<usize>> = None;
    let mut start = 0;
    while start < levels.len() {
        let mut end = start;
        while end < levels.len() && levels[end] == levels[start] {
            end += 1;
        }
        if end - start >= min {
            // Strictly lower, so among equal levels the oldest stretch wins.
            let improves = best
                .as_ref()
                .is_none_or(|b| levels[start] < levels[b.start]);
            if improves {
                best = Some(start..end);
            }
        }
        start = end;
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing to do until a level fills, and then only that level's oldest
    /// `fanout` runs — not the whole stack, which is the entire point.
    #[test]
    fn a_full_level_merges_exactly_fanout_runs() {
        assert_eq!(pick_merge(&[0; 3], 4, 100), None);
        assert_eq!(pick_merge(&[0; 4], 4, 100), Some(0..4));
        // Five L0 runs: merge the four oldest and leave the newest for next time.
        assert_eq!(pick_merge(&[0; 5], 4, 100), Some(0..4));
    }

    /// The lowest level goes first: those runs are smallest and most numerous, so
    /// merging them is what keeps write amplification logarithmic.
    #[test]
    fn the_lowest_full_level_wins() {
        // Four L1s and four L0s both qualify; L0 is cheaper.
        let levels = [1, 1, 1, 1, 0, 0, 0, 0];
        assert_eq!(pick_merge(&levels, 4, 100), Some(4..8));
        // With only three L0s, the L1 cohort is the one that is due.
        let levels = [1, 1, 1, 1, 0, 0, 0];
        assert_eq!(pick_merge(&levels, 4, 100), Some(0..4));
    }

    /// Same-level runs separated by a run of another level must not be merged
    /// together: the one in between would move in the resolution order.
    #[test]
    fn a_stretch_must_be_contiguous() {
        // Two L0s, then an L1, then two more L0s. Four L0s in total, but no
        // stretch of four, so nothing is due.
        assert_eq!(pick_merge(&[0, 0, 1, 0, 0], 4, 100), None);
        // Raising the count only helps if it lands in one stretch.
        assert_eq!(pick_merge(&[0, 0, 1, 0, 0, 0, 0], 4, 100), Some(3..7));
    }

    /// Past the depth ceiling with no level due, merge the lowest pair rather
    /// than waiting. Bounded: a stretch of `fanout` would have been caught first.
    #[test]
    fn the_depth_ceiling_merges_early_rather_than_fully() {
        let levels = [3, 2, 1, 1, 0];
        assert_eq!(pick_merge(&levels, 10, 100), None, "under the ceiling");
        assert_eq!(
            pick_merge(&levels, 10, 5),
            Some(2..4),
            "at the ceiling, take the lowest stretch there is"
        );
    }

    /// A stack of runs at strictly descending levels has no stretch to merge at
    /// all. It takes `fanout^max_runs` L0 runs to reach, so it is unreachable in
    /// practice — but it must return `None` rather than merging something
    /// non-contiguous or falling back to a full rewrite.
    #[test]
    fn a_fully_tiered_stack_over_the_ceiling_is_left_alone() {
        assert_eq!(pick_merge(&[4, 3, 2, 1, 0], 10, 3), None);
    }

    /// A nonsense fanout must not turn into an infinite rewrite loop, where every
    /// merge produces one run and immediately qualifies again.
    #[test]
    fn a_degenerate_fanout_is_clamped() {
        assert_eq!(pick_merge(&[0], 0, 100), None);
        assert_eq!(pick_merge(&[0], 1, 100), None);
        assert_eq!(pick_merge(&[0, 0], 1, 100), Some(0..2));
    }

    #[test]
    fn an_empty_stack_has_nothing_to_merge() {
        assert_eq!(pick_merge(&[], 10, 0), None);
    }
}
