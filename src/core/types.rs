//! Core identifier types and the edge event model.

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Identifier of a graph node ("graph id" in routing terms — e.g. Graph 500).
pub type NodeId = u64;

/// Identifier of a tenant scope. `GLOBAL_SCOPE` (0) is visible to every tenant.
pub type ScopeId = u32;

/// The global scope: edges visible here participate in every scope's view.
pub const GLOBAL_SCOPE: ScopeId = 0;

/// Which tenants may see an edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Visible to all scopes; merges components in every tenant's view.
    Global,
    /// Visible only to the listed scopes (typically one, occasionally a few).
    Scoped(SmallVec<[ScopeId; 4]>),
}

impl Visibility {
    /// Canonicalize visibility: an empty scope list or one containing
    /// `GLOBAL_SCOPE` means global (matching the API contract), and scope ids
    /// are sorted + deduplicated so repeated ids cannot produce duplicate
    /// rows or unions downstream.
    pub fn normalize(self) -> Visibility {
        match self {
            Visibility::Scoped(s) if s.is_empty() || s.contains(&GLOBAL_SCOPE) => {
                Visibility::Global
            }
            Visibility::Scoped(mut s) => {
                s.sort_unstable();
                s.dedup();
                Visibility::Scoped(s)
            }
            global => global,
        }
    }
}

/// A single edge event from the firehose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeEvent {
    pub src: NodeId,
    pub dst: NodeId,
    pub visibility: Visibility,
    /// Event time in epoch milliseconds.
    pub event_time_ms: i64,
    /// Raw edge properties as a JSON document (kept opaque end to end).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub props: Option<String>,
}
