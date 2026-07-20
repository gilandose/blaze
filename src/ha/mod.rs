//! High availability: leader election.
//!
//! Every worker ingests and serves queries; only the leader commits to the
//! catalog. `LeaderElector` abstracts how leadership is decided:
//!
//! - [`StaticElector`]: fixed answer, for standalone mode and tests.
//! - `KubeLeaseElector` (feature `k8s`): coordination.k8s.io Lease-based
//!   election for EKS, with periodic renewal and takeover of expired leases.

#[cfg(feature = "k8s")]
pub mod kube_lease;

use std::sync::atomic::{AtomicBool, Ordering};

pub trait LeaderElector: Send + Sync {
    fn is_leader(&self) -> bool;
}

/// Fixed leadership: standalone workers are always leaders; test followers
/// never are.
pub struct StaticElector(pub bool);

impl LeaderElector for StaticElector {
    fn is_leader(&self) -> bool {
        self.0
    }
}

/// Shared flag an async election loop flips; `is_leader` stays sync and
/// lock-free for the flush path.
#[derive(Default)]
pub struct LeaderFlag(AtomicBool);

impl LeaderFlag {
    pub fn set(&self, leader: bool) {
        self.0.store(leader, Ordering::Release);
    }
}

impl LeaderElector for LeaderFlag {
    fn is_leader(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
