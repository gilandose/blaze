//! The tonic gRPC serving layer.
//!
//! A second front end over the exact same shared state as the Axum API
//! ([`crate::api::AppState`]). The RPCs mirror the REST semantics one-for-one;
//! the query path (`GetComponent`, `CheckConnected`, `GetStats`) touches only
//! lock-free DSU finds and atomic counters, and `InjectEdge` feeds the same
//! mpsc ingest channel as `POST /v1/edges` — so a client sees identical
//! topology regardless of which protocol it speaks.

use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use crate::api::AppState;
use crate::core::{EdgeEvent, Visibility};

/// Generated prost/tonic types for the `blaze.v1` package.
pub mod pb {
    tonic::include_proto!("blaze.v1");
}

use pb::blaze_service_server::{BlazeService, BlazeServiceServer};

/// Adapts [`AppState`] to the generated `BlazeService` trait. Cloning is cheap
/// (all shared state is behind `Arc`), matching how Axum clones state per
/// request.
#[derive(Clone)]
pub struct GrpcService {
    state: AppState,
}

impl GrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Wrap the service in the generated tonic server ready to add to a router.
    pub fn into_server(self) -> BlazeServiceServer<Self> {
        BlazeServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl BlazeService for GrpcService {
    async fn get_component(
        &self,
        request: Request<pb::GetComponentRequest>,
    ) -> Result<Response<pb::GetComponentResponse>, Status> {
        let req = request.into_inner();
        let root = self.state.forest.scope_root(req.scope, req.node);
        Ok(Response::new(pb::GetComponentResponse {
            scope: req.scope,
            node: req.node,
            root,
        }))
    }

    async fn check_connected(
        &self,
        request: Request<pb::CheckConnectedRequest>,
    ) -> Result<Response<pb::CheckConnectedResponse>, Status> {
        let req = request.into_inner();
        let connected = self.state.forest.connected(req.scope, req.u, req.v);
        Ok(Response::new(pb::CheckConnectedResponse {
            scope: req.scope,
            u: req.u,
            v: req.v,
            connected,
        }))
    }

    /// The REST handler's twin, including the refusal: `UNIMPLEMENTED` rather
    /// than an empty list when the worker has no member index, because a short
    /// list is indistinguishable from a small component.
    async fn get_members(
        &self,
        request: Request<pb::GetMembersRequest>,
    ) -> Result<Response<pb::GetMembersResponse>, Status> {
        let req = request.into_inner();
        let cap = match req.cap as usize {
            0 => crate::api::DEFAULT_MEMBER_CAP,
            n if n > crate::api::MAX_MEMBER_CAP => {
                return Err(Status::invalid_argument(format!(
                    "cap must be at most {}, got {n}",
                    crate::api::MAX_MEMBER_CAP
                )));
            }
            n => n,
        };
        let found = self
            .state
            .forest
            .members(req.scope, req.node, cap)
            .ok_or_else(|| {
                Status::unimplemented(
                    "this worker has no member index: start it with --member-index, and \
                     check forest.members_available in GetStats — a run written without \
                     the flag disables the query for the whole stack",
                )
            })?;
        let truncated = found.is_truncated();
        Ok(Response::new(pb::GetMembersResponse {
            scope: req.scope,
            node: req.node,
            root: self.state.forest.scope_root(req.scope, req.node),
            members: found.into_nodes(),
            truncated,
        }))
    }

    async fn get_stats(
        &self,
        _request: Request<pb::GetStatsRequest>,
    ) -> Result<Response<pb::GetStatsResponse>, Status> {
        let forest = self.state.forest.stats();
        let buffer = self.state.buffer.stats();
        Ok(Response::new(pb::GetStatsResponse {
            worker_id: self.state.worker_id.clone(),
            leader: self.state.elector.is_leader(),
            forest: Some(pb::ForestStats {
                events_applied: forest.events_applied,
                global_merges: forest.global_merges,
                global_links: forest.global_links,
                scope_links: forest.scope_links,
                active_scopes: forest.active_scopes,
                merge_fixups: forest.merge_fixups,
                members_available: forest.members_available,
            }),
            buffer: Some(pb::BufferStats {
                active_rows: buffer.active_rows as u64,
                sealed_rows: buffer.sealed_rows as u64,
                sealed_segments: buffer.sealed_segments as u64,
                sealed_bytes: buffer.sealed_bytes as u64,
            }),
            last_offset: self.state.pipeline_stats.last_offset(),
        }))
    }

    async fn inject_edge(
        &self,
        request: Request<pb::InjectEdgeRequest>,
    ) -> Result<Response<pb::InjectEdgeResponse>, Status> {
        let req = request.into_inner();
        // normalize() folds empty lists and lists containing scope 0 into
        // Global and dedups repeated ids — identical to the REST handler.
        let visibility = Visibility::Scoped(req.scopes.into_iter().collect()).normalize();
        let event = EdgeEvent {
            src: req.src,
            dst: req.dst,
            visibility,
            event_time_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            props: req.props,
        };
        // Refused on a log-backed worker: an injected edge has no log position,
        // so accepting it means minting an offset into the space the log is
        // already assigning. See `AppState::ingest_tx`.
        self.state
            .ingest_tx
            .as_ref()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "this worker ingests from a log; its offsets are assigned there. \
                     Produce to the log instead of injecting.",
                )
            })?
            .send(event)
            .await
            .map_err(|_| Status::unavailable("ingest pipeline is shut down"))?;
        Ok(Response::new(pb::InjectEdgeResponse { accepted: true }))
    }
}
