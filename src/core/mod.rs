//! In-memory graph state: types, union-find, and the multi-tenant forest.

pub mod dsu;
pub mod scoped;
pub mod types;

pub use dsu::Dsu;
pub use scoped::{ForestSnapshot, ForestStats, ScopedForest};
pub use types::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, Visibility};
