//! In-memory graph state: types, union-find, and the multi-tenant forest.

pub mod base;
pub mod dsu;
pub mod scoped;
pub mod stream;
pub mod types;

pub use base::{BaseStats, RoutingBase, ScopeList};
pub use dsu::Dsu;
pub use scoped::{ForestSnapshot, ForestStats, Members, ScopedForest, SnapshotSink};
pub use stream::{PartitionId, StreamId, StreamPosition};
pub use types::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, Visibility};
