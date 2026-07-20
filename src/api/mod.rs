//! The embedded real-time query API.
//!
//! Serves component lookups and connectivity checks straight from shared
//! memory — the query path touches only lock-free DSU finds, so latency is
//! dominated by the HTTP stack, not the data structures. Also exposes an
//! edge-injection endpoint that feeds the same pipeline as the firehose.

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::core::{EdgeEvent, GLOBAL_SCOPE, NodeId, ScopeId, ScopedForest, Visibility};
use crate::ha::LeaderElector;
use crate::ingest::{EdgeBuffer, PipelineStats};

#[derive(Clone)]
pub struct AppState {
    pub forest: Arc<ScopedForest>,
    pub buffer: Arc<EdgeBuffer>,
    pub pipeline_stats: Arc<PipelineStats>,
    pub ingest_tx: mpsc::Sender<EdgeEvent>,
    pub elector: Arc<dyn LeaderElector>,
    pub worker_id: String,
    pub started_at: Instant,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/stats", get(stats))
        .route("/v1/scopes/{scope}/components/{node}", get(component))
        .route("/v1/scopes/{scope}/connected", get(connected))
        .route("/v1/edges", post(inject_edge))
        .with_state(state)
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

/// Scopes are addressed as `global` or a numeric tenant id.
fn parse_scope(raw: &str) -> Result<ScopeId, ApiError> {
    if raw.eq_ignore_ascii_case("global") {
        return Ok(GLOBAL_SCOPE);
    }
    raw.parse().map_err(|_| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("invalid scope '{raw}': expected 'global' or a numeric scope id"),
        )
    })
}

async fn health(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "worker_id": s.worker_id,
        "leader": s.elector.is_leader(),
        "uptime_secs": s.started_at.elapsed().as_secs(),
    }))
}

async fn stats(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "worker_id": s.worker_id,
        "leader": s.elector.is_leader(),
        "forest": s.forest.stats(),
        "buffer": s.buffer.stats(),
        "last_offset": s.pipeline_stats.last_offset(),
    }))
}

#[derive(Serialize)]
struct ComponentResponse {
    scope: ScopeId,
    node: NodeId,
    /// The component's canonical id: the lowest graph id it contains, so the
    /// routing target is deterministic (Graph 500 -> Graph 105) and only ever
    /// decreases as merges land.
    root: NodeId,
}

async fn component(
    State(s): State<AppState>,
    AxumPath((scope, node)): AxumPath<(String, NodeId)>,
) -> Result<Json<ComponentResponse>, ApiError> {
    let scope = parse_scope(&scope)?;
    Ok(Json(ComponentResponse {
        scope,
        node,
        root: s.forest.scope_root(scope, node),
    }))
}

#[derive(Deserialize)]
struct ConnectedParams {
    u: NodeId,
    v: NodeId,
}

async fn connected(
    State(s): State<AppState>,
    AxumPath(scope): AxumPath<String>,
    Query(p): Query<ConnectedParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let scope = parse_scope(&scope)?;
    Ok(Json(serde_json::json!({
        "scope": scope,
        "u": p.u,
        "v": p.v,
        "connected": s.forest.connected(scope, p.u, p.v),
    })))
}

#[derive(Deserialize)]
struct InjectEdge {
    src: NodeId,
    dst: NodeId,
    #[serde(default)]
    scopes: Vec<ScopeId>,
    #[serde(default)]
    props: Option<String>,
}

async fn inject_edge(
    State(s): State<AppState>,
    Json(body): Json<InjectEdge>,
) -> Result<StatusCode, ApiError> {
    // normalize() folds empty lists and lists containing scope 0 into
    // Global, and dedups repeated ids — the event enters the pipeline
    // canonical.
    let visibility = Visibility::Scoped(body.scopes.into_iter().collect()).normalize();
    let event = EdgeEvent {
        src: body.src,
        dst: body.dst,
        visibility,
        event_time_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
        props: body.props,
    };
    s.ingest_tx.send(event).await.map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "ingest pipeline is shut down".into(),
        )
    })?;
    Ok(StatusCode::ACCEPTED)
}
