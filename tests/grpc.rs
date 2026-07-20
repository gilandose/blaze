//! gRPC round-trip tests: a real tonic server bound to a loopback socket,
//! driven by the generated client, over the same shared state as the REST API.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use blaze::api::AppState;
use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::grpc::GrpcService;
use blaze::grpc::pb::blaze_service_client::BlazeServiceClient;
use blaze::grpc::pb::{CheckConnectedRequest, GetComponentRequest, InjectEdgeRequest};
use blaze::ha::{LeaderElector, StaticElector};
use blaze::ingest::{EdgeBuffer, Pipeline};

fn global_edge(src: u64, dst: u64) -> EdgeEvent {
    EdgeEvent {
        src,
        dst,
        visibility: Visibility::Global,
        event_time_ms: 1_700_000_000_000,
        props: None,
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

/// Pre-populated forest with a global merge and a scope-7-only edge, plus a
/// live ingest channel whose receiver is returned to the caller.
fn test_state() -> (AppState, mpsc::Receiver<EdgeEvent>) {
    let forest = Arc::new(ScopedForest::new());
    forest.apply(&global_edge(500, 105));
    forest.apply(&scoped_edge(1, 2, &[7]));
    let (tx, rx) = mpsc::channel(16);
    let pipeline = Pipeline::new(forest.clone(), Arc::new(EdgeBuffer::new()), 0);
    let elector: Arc<dyn LeaderElector> = Arc::new(StaticElector(true));
    (
        AppState {
            forest,
            buffer: pipeline.buffer.clone(),
            pipeline_stats: pipeline.stats.clone(),
            ingest_tx: tx,
            elector,
            worker_id: "grpc-test".into(),
            started_at: Instant::now(),
        },
        rx,
    )
}

/// Bind the tonic service to an ephemeral loopback port, returning a connected
/// client and the server task handle.
async fn serve(
    state: AppState,
) -> (
    BlazeServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
) {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = incoming.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GrpcService::new(state).into_server())
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let client = BlazeServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to grpc server");
    (client, handle)
}

#[tokio::test]
async fn grpc_get_component_and_connected_scope_isolation() {
    let (state, _rx) = test_state();
    let (mut client, server) = serve(state).await;

    // 500 and 105 share a component representative in the global scope.
    let r500 = client
        .get_component(GetComponentRequest {
            scope: 0,
            node: 500,
        })
        .await
        .unwrap()
        .into_inner();
    let r105 = client
        .get_component(GetComponentRequest {
            scope: 0,
            node: 105,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r500.root, r105.root);

    // The scope-7 edge is visible in scope 7 but not scope 8 (isolation).
    let in7 = client
        .check_connected(CheckConnectedRequest {
            scope: 7,
            u: 1,
            v: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(in7.connected);
    let in8 = client
        .check_connected(CheckConnectedRequest {
            scope: 8,
            u: 1,
            v: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!in8.connected);

    server.abort();
}

#[tokio::test]
async fn grpc_inject_edge_reaches_channel() {
    let (state, mut rx) = test_state();
    let (mut client, server) = serve(state).await;

    let ack = client
        .inject_edge(InjectEdgeRequest {
            src: 777,
            dst: 888,
            scopes: vec![42, 43],
            props: Some(r#"{"kind":"link"}"#.into()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ack.accepted);

    let event = rx.recv().await.unwrap();
    assert_eq!(event.src, 777);
    assert_eq!(event.dst, 888);
    assert_eq!(
        event.visibility,
        Visibility::Scoped([42u32, 43u32].into_iter().collect())
    );
    assert_eq!(event.props.as_deref(), Some(r#"{"kind":"link"}"#));

    // An empty scope list injects as global visibility.
    client
        .inject_edge(InjectEdgeRequest {
            src: 1,
            dst: 2,
            scopes: vec![],
            props: None,
        })
        .await
        .unwrap();
    let event = rx.recv().await.unwrap();
    assert_eq!(event.visibility, Visibility::Global);

    server.abort();
}
