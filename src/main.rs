//! blaze worker binary: ingest + API + micro-batch persistence + HA.

use clap::Parser;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::info;

use blaze::api::{AppState, router};
use blaze::core::ScopedForest;
use blaze::grpc::GrpcService;
use blaze::ha::{LeaderElector, StaticElector};
use blaze::ingest::{EdgeBuffer, Pipeline, SimulatorConfig, run_simulator};
use blaze::storage::{Flusher, SnapshotCatalog, hydrate_from_catalog};

#[derive(Parser, Debug)]
#[command(name = "blaze", about = "Multi-tenant streaming graph engine")]
struct Args {
    /// REST API listen address.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// gRPC listen address.
    #[arg(long, default_value = "0.0.0.0:50051")]
    grpc_listen: String,

    /// Warehouse URI: file:///abs/path, s3://bucket/prefix, or memory://
    #[arg(long, default_value = "file://./blaze-warehouse")]
    warehouse: String,

    /// Table prefix inside the warehouse.
    #[arg(long, default_value = "graph/edges")]
    table: String,

    /// Seconds between micro-batch flushes.
    #[arg(long, default_value_t = 60)]
    flush_interval_secs: u64,

    /// Worker identity (defaults to hostname-pid).
    #[arg(long)]
    worker_id: Option<String>,

    /// Run the built-in firehose simulator.
    #[arg(long, default_value_t = false)]
    simulate: bool,

    /// Simulator: events per second.
    #[arg(long, default_value_t = 5_000)]
    sim_rate: u64,

    /// Simulator: node id space.
    #[arg(long, default_value_t = 1_000_000)]
    sim_nodes: u64,

    /// Simulator: number of tenant scopes.
    #[arg(long, default_value_t = 3_000)]
    sim_scopes: u32,

    /// Leader election: "static:true", "static:false", or "k8s" (requires
    /// the k8s build feature).
    #[arg(long, default_value = "static:true")]
    election: String,

    /// Kubernetes namespace for the election lease.
    #[arg(long, default_value = "default")]
    k8s_namespace: String,

    /// Kubernetes Lease name.
    #[arg(long, default_value = "blaze-committer")]
    k8s_lease: String,
}

fn build_store(uri: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    if let Some(path) = uri.strip_prefix("file://") {
        let abs = std::path::absolute(path)?;
        std::fs::create_dir_all(&abs)?;
        Ok(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(abs)?,
        ))
    } else if uri.starts_with("memory://") {
        Ok(Arc::new(object_store::memory::InMemory::new()))
    } else if uri.starts_with("s3://") {
        let bucket = s3_bucket(uri)?;
        let store = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;
        Ok(Arc::new(store))
    } else {
        anyhow::bail!("unsupported warehouse uri '{uri}' (file://, s3://, memory://)")
    }
}

fn s3_bucket(uri: &str) -> anyhow::Result<String> {
    let rest = uri.trim_start_matches("s3://");
    let bucket = rest.split('/').next().unwrap_or_default();
    anyhow::ensure!(!bucket.is_empty(), "s3 uri missing bucket: {uri}");
    Ok(bucket.to_string())
}

fn s3_table_prefix(uri: &str, table: &str) -> StorePath {
    // For s3://bucket/some/prefix, object keys start under some/prefix.
    let rest = uri.trim_start_matches("s3://");
    match rest.split_once('/') {
        Some((_, prefix)) if !prefix.is_empty() => {
            StorePath::from(format!("{}/{}", prefix.trim_end_matches('/'), table))
        }
        _ => StorePath::from(table.to_string()),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let worker_id = args.worker_id.clone().unwrap_or_else(|| {
        format!(
            "{}-{}",
            std::env::var("HOSTNAME").unwrap_or_else(|_| "blaze".into()),
            std::process::id()
        )
    });
    info!(worker_id, warehouse = %args.warehouse, "starting blaze worker");

    let store = build_store(&args.warehouse)?;
    let table_prefix = if args.warehouse.starts_with("s3://") {
        s3_table_prefix(&args.warehouse, &args.table)
    } else {
        StorePath::from(args.table.clone())
    };
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), table_prefix.clone()));

    // In-memory state, hydrated from the latest committed Puffin snapshot.
    let forest = Arc::new(ScopedForest::new());
    let watermark = hydrate_from_catalog(&forest, &store, &catalog).await?;
    let buffer = Arc::new(EdgeBuffer::new());

    // Leader election.
    let elector: Arc<dyn LeaderElector> = match args.election.as_str() {
        "static:true" => Arc::new(StaticElector(true)),
        "static:false" => Arc::new(StaticElector(false)),
        #[cfg(feature = "k8s")]
        "k8s" => {
            let flag = Arc::new(blaze::ha::LeaderFlag::default());
            let cfg = blaze::ha::kube_lease::KubeLeaseConfig {
                namespace: args.k8s_namespace.clone(),
                lease_name: args.k8s_lease.clone(),
                identity: worker_id.clone(),
                lease_duration: Duration::from_secs(15),
            };
            tokio::spawn(blaze::ha::kube_lease::run_election(cfg, flag.clone()));
            flag
        }
        other => anyhow::bail!("unknown election mode '{other}' (static:true, static:false, k8s)"),
    };

    // Ingest pipeline.
    let (ingest_tx, ingest_rx) = mpsc::channel(65_536);
    let pipeline = Pipeline::new(forest.clone(), buffer.clone(), watermark);
    let pipeline_stats = pipeline.stats.clone();
    let pipeline_handle = tokio::spawn(async move { pipeline.run(ingest_rx).await });

    if args.simulate {
        let cfg = SimulatorConfig {
            rate: args.sim_rate,
            nodes: args.sim_nodes,
            scopes: args.sim_scopes,
            ..Default::default()
        };
        info!(?cfg, "starting firehose simulator");
        tokio::spawn(run_simulator(cfg, ingest_tx.clone()));
    }

    // Micro-batch flusher.
    let flusher = Arc::new(Flusher {
        forest: forest.clone(),
        buffer: buffer.clone(),
        store: store.clone(),
        catalog: catalog.clone(),
        elector: elector.clone(),
        table_prefix,
        worker_id: worker_id.clone(),
    });
    let flush_handle = tokio::spawn(
        flusher
            .clone()
            .run(Duration::from_secs(args.flush_interval_secs)),
    );

    // API server.
    let state = AppState {
        forest,
        buffer,
        pipeline_stats,
        ingest_tx,
        elector,
        worker_id,
        started_at: Instant::now(),
    };
    // gRPC serving layer over the same shared state, on its own port and task.
    let grpc_addr = args.grpc_listen.parse()?;
    let grpc_server = tonic::transport::Server::builder()
        .add_service(GrpcService::new(state.clone()).into_server())
        .serve(grpc_addr);
    info!(grpc_listen = %args.grpc_listen, "gRPC listening");
    let mut grpc_handle = tokio::spawn(grpc_server);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    info!(listen = %args.listen, "API listening");
    let server = axum::serve(listener, router(state));

    tokio::select! {
        res = server => res?,
        res = &mut grpc_handle => res??,
        _ = tokio::signal::ctrl_c() => {
            grpc_handle.abort();
            info!("shutting down: attempting final flush");
            flush_handle.abort();
            pipeline_handle.abort();
            if let Err(e) = flusher.tick().await {
                tracing::warn!(error = %e, "final flush failed");
            }
        }
    }
    Ok(())
}
