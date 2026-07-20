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
            }),
            buffer: Some(pb::BufferStats {
                active_rows: buffer.active_rows as u64,
                sealed_rows: buffer.sealed_rows as u64,
                sealed_segments: buffer.sealed_segments as u64,
                sealed_bytes: buffer.sealed_bytes as u64,
            }),
            events_ingested: self.state.pipeline_stats.events_ingested(),
        }))
    }

    async fn inject_edge(
        &self,
        request: Request<pb::InjectEdgeRequest>,
    ) -> Result<Response<pb::InjectEdgeResponse>, Status> {
        let req = request.into_inner();
        // Empty scopes (or a list resolving to global) means global visibility,
        // exactly as the REST handler and `Visibility::normalize` treat it.
        let visibility = if req.scopes.is_empty() {
            Visibility::Global
        } else {
            Visibility::Scoped(req.scopes.into_iter().collect())
        };
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
        self.state
            .ingest_tx
            .send(event)
            .await
            .map_err(|_| Status::unavailable("ingest pipeline is shut down"))?;
        Ok(Response::new(pb::InjectEdgeResponse { accepted: true }))
    }
}
