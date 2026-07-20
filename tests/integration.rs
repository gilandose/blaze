//! End-to-end tests: ingest -> flush -> object storage -> hydrate -> API.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use smallvec::smallvec;
use tokio::sync::mpsc;
use tower::ServiceExt;

use blaze::api::{AppState, router};
use blaze::core::{EdgeEvent, GLOBAL_SCOPE, ScopedForest, Visibility};
use blaze::ha::{LeaderElector, StaticElector};
use blaze::ingest::{EdgeBuffer, Pipeline};
use blaze::storage::{Flusher, SnapshotCatalog, hydrate_from_catalog};

fn global_edge(src: u64, dst: u64) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: Visibility::Global,
        event_time_ms: 1_700_000_000_000,
        props: Some(r#"{"kind":"merge"}"#.into()),
    }
}

fn scoped_edge(src: u64, dst: u64, scopes: &[u32]) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: Visibility::Scoped(scopes.iter().copied().collect()),
        event_time_ms: 1_700_000_000_000,
        props: None,
    }
}

fn make_flusher(
    store: Arc<dyn ObjectStore>,
    forest: Arc<ScopedForest>,
    buffer: Arc<EdgeBuffer>,
    leader: bool,
    worker: &str,
) -> (Arc<Flusher>, Arc<SnapshotCatalog>) {
    let prefix = StorePath::from("graph/edges");
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), prefix.clone()));
    let flusher = Arc::new(Flusher {
        forest,
        buffer,
        store,
        catalog: catalog.clone(),
        elector: Arc::new(StaticElector(leader)),
        table_prefix: prefix,
        worker_id: worker.into(),
    });
    (flusher, catalog)
}

#[tokio::test]
async fn leader_flush_commit_and_cold_start_hydration() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let forest = Arc::new(ScopedForest::new());
    let buffer = Arc::new(EdgeBuffer::new());

    // Ingest: a global merge (Graph 500 -> Graph 105) plus tenant-scoped
    // edges, one of them later bridged globally.
    let events = [
        global_edge(500, 105),
        scoped_edge(1, 2, &[7]),
        scoped_edge(3, 4, &[7]),
        scoped_edge(1, 2, &[2999]),
        global_edge(1, 3), // bridges scope 7's islands, invisible to raw scope 2999 pairs
    ];
    for (i, e) in events.iter().enumerate() {
        forest.apply(e);
        buffer.append(i as u64 + 1, e);
    }

    let (flusher, catalog) = make_flusher(
        store.clone(),
        forest.clone(),
        buffer.clone(),
        true,
        "worker-a",
    );
    flusher.tick().await.unwrap();

    // Snapshot committed with the right watermark and one data file.
    let latest = catalog.latest().await.unwrap().unwrap();
    assert_eq!(latest.sequence, 1);
    assert_eq!(latest.watermark, 5);
    assert_eq!(latest.data_files.len(), 1);
    assert_eq!(latest.data_files[0].rows, 5); // one row per (event, scope)
    assert_eq!(latest.committer, "worker-a");

    // Committed segments were dropped; an idle tick commits nothing new.
    assert_eq!(buffer.stats().sealed_segments, 0);
    flusher.tick().await.unwrap();
    assert_eq!(catalog.latest().await.unwrap().unwrap().sequence, 1);

    // Cold start: a fresh worker hydrates DSU state purely from the Puffin
    // sidecar and answers the same connectivity queries.
    let fresh = Arc::new(ScopedForest::new());
    let watermark = hydrate_from_catalog(&fresh, &store, &catalog)
        .await
        .unwrap();
    assert_eq!(watermark, 5);
    assert!(fresh.connected(GLOBAL_SCOPE, 500, 105));
    assert!(fresh.connected(7, 2, 4)); // via scope edges + global bridge
    assert!(fresh.connected(2999, 1, 2));
    assert!(!fresh.connected(2999, 2, 4));
    assert!(!fresh.connected(GLOBAL_SCOPE, 1, 2));

    // And hydrated state keeps composing with *new* global merges.
    fresh.apply(&global_edge(4, 900));
    assert!(fresh.connected(7, 900, 1));
    assert!(!fresh.connected(GLOBAL_SCOPE, 900, 1));
}

#[tokio::test]
async fn follower_never_commits_but_prunes_after_leader() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Leader and follower both ingest the same firehose slice.
    let make_worker = || {
        let forest = Arc::new(ScopedForest::new());
        let buffer = Arc::new(EdgeBuffer::new());
        for (i, e) in [global_edge(10, 20), scoped_edge(30, 40, &[5])]
            .iter()
            .enumerate()
        {
            forest.apply(e);
            buffer.append(i as u64 + 1, e);
        }
        (forest, buffer)
    };
    let (leader_forest, leader_buffer) = make_worker();
    let (follower_forest, follower_buffer) = make_worker();

    let (follower, catalog) = make_flusher(
        store.clone(),
        follower_forest.clone(),
        follower_buffer.clone(),
        false,
        "follower",
    );
    // Follower ticks first: seals, but must not commit.
    follower.tick().await.unwrap();
    assert!(catalog.latest().await.unwrap().is_none());
    assert_eq!(follower_buffer.stats().sealed_segments, 1);

    let (leader, _) = make_flusher(
        store.clone(),
        leader_forest,
        leader_buffer.clone(),
        true,
        "leader",
    );
    leader.tick().await.unwrap();
    let latest = catalog.latest().await.unwrap().unwrap();
    assert_eq!(latest.committer, "leader");

    // Next follower tick observes the commit and garbage-collects its own
    // sealed segments instead of double-writing them.
    follower.tick().await.unwrap();
    assert_eq!(follower_buffer.stats().sealed_segments, 0);
    assert_eq!(catalog.latest().await.unwrap().unwrap().committer, "leader");
}

fn test_app(leader: bool) -> (AppState, mpsc::Receiver<EdgeEvent>) {
    let forest = Arc::new(ScopedForest::new());
    forest.apply(&global_edge(500, 105));
    forest.apply(&scoped_edge(1, 2, &[7]));
    let (tx, rx) = mpsc::channel(16);
    let pipeline = Pipeline::new(forest.clone(), Arc::new(EdgeBuffer::new()), 0);
    let elector: Arc<dyn LeaderElector> = Arc::new(StaticElector(leader));
    (
        AppState {
            forest,
            buffer: pipeline.buffer.clone(),
            pipeline_stats: pipeline.stats.clone(),
            ingest_tx: tx,
            elector,
            worker_id: "api-test".into(),
            started_at: Instant::now(),
        },
        rx,
    )
}

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn api_component_and_connectivity() {
    let (state, _rx) = test_app(true);
    let app = router(state);

    let (status, body) = get_json(app.clone(), "/v1/scopes/global/components/500").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["root"], body["root"].as_u64().unwrap());
    // 500 and 105 route to the same component representative.
    let (_, b105) = get_json(app.clone(), "/v1/scopes/global/components/105").await;
    assert_eq!(body["root"], b105["root"]);

    let (status, body) = get_json(app.clone(), "/v1/scopes/7/connected?u=1&v=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["connected"], serde_json::Value::Bool(true));

    let (_, body) = get_json(app.clone(), "/v1/scopes/8/connected?u=1&v=2").await;
    assert_eq!(body["connected"], serde_json::Value::Bool(false));

    let (status, _) = get_json(app.clone(), "/v1/scopes/bogus/connected?u=1&v=2").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, body) = get_json(app.clone(), "/v1/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["leader"], serde_json::Value::Bool(true));

    let (status, body) = get_json(app, "/v1/stats").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["forest"]["global_merges"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn api_edge_injection_feeds_pipeline() {
    let (state, mut rx) = test_app(false);
    let app = router(state);

    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/edges")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"src": 777, "dst": 888, "scopes": [42, 43]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    let event = rx.recv().await.unwrap();
    assert_eq!(event.src, 777);
    assert_eq!(event.dst, 888);
    assert_eq!(
        event.visibility,
        Visibility::Scoped(smallvec![42u32, 43u32])
    );
}

/// Reference model for the incremental-cycles test: per-scope BFS over
/// (global ∪ scope) edges, with the component-minimum as the canonical root.
struct RefModel {
    global_edges: Vec<(u64, u64)>,
    scope_edges: Vec<(u32, u64, u64)>,
}

impl RefModel {
    fn component(&self, scope: u32, u: u64) -> std::collections::HashSet<u64> {
        let mut adj: std::collections::HashMap<u64, Vec<u64>> = std::collections::HashMap::new();
        let add = |a: u64, b: u64, adj: &mut std::collections::HashMap<u64, Vec<u64>>| {
            adj.entry(a).or_default().push(b);
            adj.entry(b).or_default().push(a);
        };
        for &(a, b) in &self.global_edges {
            add(a, b, &mut adj);
        }
        for &(s, a, b) in &self.scope_edges {
            if s == scope {
                add(a, b, &mut adj);
            }
        }
        let mut seen = std::collections::HashSet::from([u]);
        let mut stack = vec![u];
        while let Some(x) = stack.pop() {
            for &n in adj.get(&x).map(|v| v.as_slice()).unwrap_or(&[]) {
                if seen.insert(n) {
                    stack.push(n);
                }
            }
        }
        seen
    }

    fn connected(&self, scope: u32, u: u64, v: u64) -> bool {
        u == v || self.component(scope, u).contains(&v)
    }

    fn component_min(&self, scope: u32, u: u64) -> u64 {
        self.component(scope, u).into_iter().min().unwrap()
    }
}

/// Randomized state must survive REPEATED flush -> Puffin bytes -> cold-start
/// hydration cycles: each cycle's snapshot is taken from a forest that was
/// itself hydrated the cycle before, sequences increment, watermarks advance,
/// and every answer keeps matching the BFS reference model.
#[tokio::test]
async fn incremental_puffin_cycles_stay_correct() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const NODES: u64 = 80;
    const SCOPES: [u32; 3] = [1, 2, 3];
    const CYCLES: usize = 3;
    const EVENTS_PER_CYCLE: usize = 150;

    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut model = RefModel {
        global_edges: vec![],
        scope_edges: vec![],
    };

    let mut forest = Arc::new(ScopedForest::new());
    let mut offset = 0u64;
    let mut last_sequence = 0u64;
    let mut last_watermark = 0u64;

    for cycle in 0..CYCLES {
        // Each cycle uses a fresh buffer/flusher bound to the current forest
        // (which, after cycle 0, is a hydrated cold-start instance).
        let buffer = Arc::new(EdgeBuffer::new());
        let (flusher, catalog) = make_flusher(
            store.clone(),
            forest.clone(),
            buffer.clone(),
            true,
            &format!("leader-cycle-{cycle}"),
        );

        for _ in 0..EVENTS_PER_CYCLE {
            let u = rng.random_range(0..NODES);
            let v = rng.random_range(0..NODES);
            let event = if rng.random_range(0..100) < 30 {
                model.global_edges.push((u, v));
                global_edge(u, v)
            } else {
                let s = SCOPES[rng.random_range(0..SCOPES.len())];
                model.scope_edges.push((s, u, v));
                scoped_edge(u, v, &[s])
            };
            offset += 1;
            forest.apply(&event);
            buffer.append(offset, &event);
        }

        flusher.tick().await.unwrap();
        let snap = catalog.latest().await.unwrap().unwrap();
        assert_eq!(snap.sequence, last_sequence + 1, "sequence must increment");
        assert!(snap.watermark > last_watermark, "watermark must advance");
        assert_eq!(snap.watermark, offset);
        last_sequence = snap.sequence;
        last_watermark = snap.watermark;

        // Cold start strictly from the committed Puffin bytes.
        let fresh = Arc::new(ScopedForest::new());
        let watermark = hydrate_from_catalog(&fresh, &store, &catalog)
            .await
            .unwrap();
        assert_eq!(watermark, offset);

        for _ in 0..250 {
            let a = rng.random_range(0..NODES);
            let b = rng.random_range(0..NODES);
            let s = if rng.random_range(0..4) == 0 {
                GLOBAL_SCOPE
            } else {
                SCOPES[rng.random_range(0..SCOPES.len())]
            };
            assert_eq!(
                fresh.connected(s, a, b),
                model.connected(s, a, b),
                "cycle {cycle}: hydrated connectivity diverged in scope {s} ({a},{b})"
            );
            assert_eq!(
                fresh.scope_root(s, a),
                model.component_min(s, a),
                "cycle {cycle}: hydrated root({a}) in scope {s} is not the component min"
            );
        }

        // Next cycle continues on the HYDRATED forest, so cycle N+1's
        // snapshot proves a snapshot-of-a-hydrated-forest is faithful.
        forest = fresh;
    }
}
