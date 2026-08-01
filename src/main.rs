//! blaze worker binary: ingest + API + micro-batch persistence + HA.

use clap::Parser;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use blaze::api::{AppState, router};
use blaze::core::ScopedForest;
use blaze::grpc::GrpcService;
use blaze::ha::{LeaderElector, StaticElector};
use blaze::ingest::{EdgeBuffer, LogSource, Pipeline, SimulatorConfig, run_simulator};
use blaze::storage::{Flusher, SnapshotCatalog, hydrate_from_catalog, open_base_from_catalog};

#[derive(Parser, Debug)]
#[command(
    name = "blaze",
    about = "Multi-tenant streaming graph engine",
    // Taken from Cargo.toml, which the release workflow checks against the tag,
    // so the number in a bug report is the number that was built.
    version
)]
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

    /// Where committed routing state is served from: "ram" hydrates the
    /// whole DSU into the heap; "disk" mmaps the latest snapshot from
    /// --data-dir and keeps only post-snapshot merges in memory (bounded by
    /// the compaction window, and restarts serve in seconds).
    #[arg(long, default_value = "ram")]
    routing_base: String,

    /// Local cache directory for the mmap'd routing base (--routing-base disk).
    #[arg(long, default_value = "./blaze-data")]
    data_dir: String,

    /// Fold a follower's memtable into a fresh on-disk layer once it holds this
    /// many links (--routing-base disk only).
    ///
    /// Followers only, and the name now says so because the old one did not. A
    /// leader folds on every tick whatever this is set to: the folded layer is
    /// what it commits, since each snapshot's Puffin sidecar is that fold, and a
    /// commit with no layer would leave that sequence with no routing state to
    /// recover from. Leader fold frequency is therefore pinned to commit
    /// frequency, and --flush-interval-secs is the knob that bounds a leader's
    /// memtable.
    ///
    /// On a follower: lower keeps heap smaller at the cost of rewriting more
    /// often, and the fold stalls that worker's ingest while it runs.
    #[arg(long, default_value_t = blaze::storage::DEFAULT_FOLLOWER_FOLD_AFTER_LINKS)]
    follower_fold_after_links: u64,

    /// Depth ceiling: past this many runs, merge the lowest stretch available
    /// even when no level is due (--routing-base disk only). Higher means rarer
    /// merges, at the cost of more runs to probe per lookup, slower ingest, and
    /// more files to fetch on cold start.
    #[arg(long, default_value_t = blaze::storage::DEFAULT_MAX_DELTA_LAYERS)]
    max_delta_layers: usize,

    /// Runs per size level before they merge into one run at the level above
    /// (--routing-base disk only). This is what keeps total merge work
    /// O(N log N): each link is rewritten about once per level it passes
    /// through, rather than once per whole-base rewrite. Lower means fewer runs
    /// to probe but more levels and so more rewriting per link.
    #[arg(long, default_value_t = blaze::storage::DEFAULT_TIER_FANOUT)]
    tier_fanout: usize,

    /// Await each merge inside the tick that starts it, rather than letting it
    /// run in the background (--routing-base disk only). Off by default, because
    /// a detached merge is what lets folds keep draining the memtable while a
    /// merge runs. Turn it on when ingest must not be able to outrun compaction,
    /// or when each tick should be a complete unit of work.
    #[arg(long, default_value_t = false)]
    inline_merges: bool,

    /// Bits of membership filter per key written into each run, or 0 for none
    /// (--routing-base disk only). This is the one dial that moves the
    /// *unreclaimable* memory floor: everything else a disk-backed base keeps in
    /// RAM is either bounded by a trigger or clean file-backed pages the kernel
    /// can evict, while filters are heap and scale with total state — roughly one
    /// byte per key at the default 8 — measured 1.9 bytes per *link* on a
    /// 3000-scope mix, so ~3.8 GB at 2B links. Lowering it costs
    /// query latency on a miss, never correctness: a false positive is exactly the
    /// binary search that would have happened anyway. 4 bits roughly halves the
    /// heap for a ~10-15% false-positive rate; 0 makes every probe a binary
    /// search. Runs already written keep whatever they were written with.
    #[arg(long, default_value_t = blaze::storage::DEFAULT_FILTER_BITS)]
    filter_bits: usize,

    /// On-disk `root -> scopes` registry encoding for newly written runs:
    /// `blocked` (delta-varint in indexed blocks) or `flat` (a fixed 12-byte
    /// stride).
    ///
    /// `blocked` is 4.8-7.1x smaller on the registry, which is the largest
    /// single component of a base — measured 133.6 MB down to 81.0 MB, 39% off —
    /// at the cost of decoding part of a block per lookup instead of a binary
    /// search: 1.40 us against 0.59. That cost falls only on `apply_global`,
    /// which runs at the global-merge rate; queries never read the registry.
    ///
    /// `flat` exists so a base can be written for a reader that predates the
    /// blocked format. Runs already written keep whatever they were written
    /// with, and a stack may mix the two.
    #[arg(long, default_value_t = Default::default())]
    registry_encoding: blaze::storage::RegistryEncoding,

    /// Start even if the object store fails the put-if-absent preflight.
    ///
    /// The snapshot commit is a conditional put, and it is the *only* thing
    /// standing between two workers that both believe they are the leader and
    /// two divergent histories of the same table. A store that ignores the
    /// precondition produces no error and no warning — both commits succeed,
    /// both workers serve topology consistent with what they wrote, and nothing
    /// downstream can tell. So the check is fail-closed by default.
    ///
    /// Set this only if the preflight is wrong about your store, and expect a
    /// warning every minute for as long as the process runs.
    #[arg(long, default_value_t = false)]
    allow_unsafe_commits: bool,

    /// Resume a table whose snapshots were committed against a different stream.
    ///
    /// Offsets are only meaningful in the stream that assigned them, so by
    /// default a mismatch refuses to start: resuming would seek to an offset
    /// that exists in the new stream and means something else in the old one,
    /// hydrate cleanly, and produce a table nothing downstream can tell is
    /// wrong.
    ///
    /// The legitimate use is a stream that was renamed or moved with its offsets
    /// intact — a topic migration, or a log file given a new path. It is **not**
    /// a way to point a table at different data: for that, start a new table.
    /// Expect a warning every minute for as long as the process runs.
    #[arg(long, default_value_t = false)]
    allow_stream_change: bool,

    /// Seconds between retention sweeps, or 0 to never reclaim storage.
    ///
    /// Nothing in the commit path deletes anything, and tiering writes several
    /// times the bytes it keeps: every merge supersedes the runs it read. A
    /// table left unswept grows at the write-amplification rate rather than at
    /// the rate of the data — measured mid-soak at 80% of bytes unreachable, and
    /// still climbing.
    #[arg(long, default_value_t = 3600)]
    retention_interval_secs: u64,

    /// Snapshots to keep regardless of age.
    ///
    /// Each one is a point a worker can be restored to, so this is a durability
    /// setting as much as a storage one.
    #[arg(long, default_value_t = 10)]
    keep_snapshots: usize,

    /// Also keep every snapshot committed within this many hours.
    #[arg(long, default_value_t = 24)]
    keep_snapshots_hours: u64,

    /// Never reclaim an object younger than this, reachable or not.
    ///
    /// A tick uploads its data and run objects and commits the metadata naming
    /// them afterwards; in between they are reachable from nothing. Without this
    /// window a sweep would collect an in-flight commit's own inputs and the
    /// commit would publish a snapshot naming files that no longer exist. Must
    /// exceed the slowest merge-and-commit, so it is measured in hours.
    #[arg(long, default_value_t = 3600)]
    retention_grace_secs: u64,

    /// Consume edges from a newline-delimited JSON log instead of the API.
    ///
    /// This is the shape a production deployment has: the offset is assigned by
    /// the log, not by the worker, so every consumer numbers the same record the
    /// same way. That is what makes the committed watermark portable — a failover
    /// worker resumes after it and replays exactly the events that were applied
    /// but never committed. A worker that mints its own offsets has a watermark
    /// only it can interpret.
    ///
    /// One line per event, offset = line number from 1.
    ///
    /// A **file** is one partition. A **directory** of `partition-<n>.ndjson` is
    /// a partitioned stream, checkpointed per partition — a snapshot then records
    /// `{"0": 900, "1": 400}` rather than one number, and a topic that gains a
    /// partition needs no migration because an absent partition reads as zero,
    /// which is where it starts. Consumption is single-writer either way.
    ///
    /// Injection over HTTP/gRPC is refused while this is set — those events have
    /// no log position, and minting offsets for them would collide with the log's.
    #[arg(long)]
    edge_log: Option<String>,

    /// Wait at the end of the log for more, rather than stopping when drained.
    ///
    /// On is the streaming deployment. Off turns the worker into a batch loader
    /// that ingests a finite file, flushes, and idles serving queries.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    edge_log_follow: bool,

    /// Records per poll from the edge log.
    ///
    /// The single largest difference between this path and the API: injection is
    /// one event per request, so it is bound by request overhead rather than by
    /// how fast edges can be applied. A batch amortises that to nothing.
    #[arg(long, default_value_t = 10_000)]
    edge_log_batch: usize,

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
            // Stated rather than inherited. This is the default in object_store
            // 0.14, but the entire commit protocol rests on it, and a default
            // that changes under a dependency bump would turn leader arbitration
            // off silently. `verify_conditional_put` then checks that the store
            // on the other end actually honours it.
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
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
    // Version in the first line of the log, so a support question that starts
    // with a log paste does not need a second round trip to establish it.
    info!(
        worker_id,
        version = env!("CARGO_PKG_VERSION"),
        warehouse = %args.warehouse,
        "starting blaze worker"
    );

    let store = build_store(&args.warehouse)?;
    let table_prefix = if args.warehouse.starts_with("s3://") {
        s3_table_prefix(&args.warehouse, &args.table)
    } else {
        StorePath::from(args.table.clone())
    };
    let catalog = Arc::new(SnapshotCatalog::new(store.clone(), table_prefix.clone()));

    // Prove the store can arbitrate commits before we ever try to make one.
    // Cheap (a few dozen small objects, all removed again) and it runs against
    // the real bucket, prefix and credentials rather than a stand-in.
    if let Err(e) = blaze::storage::verify_conditional_put(&store, &table_prefix).await {
        if !args.allow_unsafe_commits {
            return Err(anyhow::Error::new(e).context(
                "object store cannot arbitrate snapshot commits; refusing to start. \
                 Re-run with --allow-unsafe-commits only if you are certain this \
                 check is wrong about your store",
            ));
        }
        error!(
            error = %e,
            "object store failed the put-if-absent preflight and --allow-unsafe-commits \
             is set; two leaders can silently publish divergent histories"
        );
        let reason = e.to_string();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_secs(60));
            every.tick().await;
            loop {
                every.tick().await;
                warn!(reason, "running with unverified commit arbitration");
            }
        });
    }

    // Committed routing state: either hydrated into the heap, or mmap'd from
    // a local cache of the latest Puffin snapshot.
    let base_dir = (args.routing_base == "disk").then(|| std::path::PathBuf::from(&args.data_dir));
    // Layer stack the flusher folds onto, seeded from whatever the catalog
    // already has so the first fold can be a delta rather than a rewrite.
    let mut local_layers = None;
    let (forest, watermark) = match args.routing_base.as_str() {
        "ram" => {
            let forest = Arc::new(ScopedForest::new());
            let watermark = hydrate_from_catalog(&forest, &store, &catalog).await?;
            (forest, watermark)
        }
        "disk" => {
            let dir = base_dir.clone().expect("set for disk mode");
            match open_base_from_catalog(&store, &catalog, &dir).await? {
                Some((base, watermark, local)) => {
                    local_layers = Some(local);
                    (Arc::new(ScopedForest::with_base(base)), watermark)
                }
                None => {
                    info!("no committed snapshot yet; starting with an empty memtable");
                    (Arc::new(ScopedForest::new()), 0)
                }
            }
        }
        other => anyhow::bail!("unknown --routing-base '{other}' (ram, disk)"),
    };
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

    // What stream the committed offsets are measured in. Only a log has an
    // answer: offsets minted locally for API-injected edges are not portable to
    // any other worker, so claiming an identity for them would assert something
    // untrue. See design 010.
    let stream_id = args
        .edge_log
        .as_ref()
        .map(|path| blaze::core::StreamId::new("file", path.clone()));
    if let Some(id) = &stream_id {
        let latest = catalog.latest().await?;
        if let Some(committed) = latest.as_ref().and_then(|s| s.stream.as_ref())
            && !committed.same_stream(id)
        {
            // Fail closed by default. Resuming here would seek to an offset that
            // exists in this stream and means something else in the one the
            // snapshots were built from — it hydrates cleanly and produces a
            // table nothing downstream can tell is wrong.
            if !args.allow_stream_change {
                anyhow::bail!(
                    "this table's snapshots were committed against {committed}, but this \
                     worker is configured for {id}. Offsets from one stream are meaningless \
                     in the other. Point --edge-log at the original stream, start a new \
                     table, or pass --allow-stream-change if the stream was renamed or \
                     moved with its offsets intact."
                );
            }
            error!(
                %committed, new = %id,
                "resuming a table against a different stream because --allow-stream-change \
                 is set; the committed offsets only mean anything if this stream kept them"
            );
            let (was, now) = (committed.to_string(), id.to_string());
            tokio::spawn(async move {
                let mut every = tokio::time::interval(Duration::from_secs(60));
                every.tick().await;
                loop {
                    every.tick().await;
                    warn!(committed = %was, configured = %now, "running against a changed stream");
                }
            });
        }
    }

    // Ingest pipeline. Offsets come either from a log or from a local counter,
    // never both — see `--edge-log`.
    // Resume every partition, not just partition 0. `hydrate_from_catalog`
    // returns the scalar for the RAM path; the committed snapshot is what
    // carries the full position, and reads as `{0: watermark}` when it predates
    // design 010.
    let start_position = match catalog.latest().await? {
        Some(snapshot) => snapshot.stream_position(),
        None => blaze::core::StreamPosition::single(watermark),
    };
    let pipeline = Arc::new(Pipeline::resuming(
        forest.clone(),
        buffer.clone(),
        start_position.clone(),
    ));
    let pipeline_stats = pipeline.stats.clone();
    let stop_ingest = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let (ingest_tx, pipeline_handle) = match &args.edge_log {
        Some(path) => {
            if args.simulate {
                anyhow::bail!(
                    "--simulate and --edge-log both feed the pipeline, and their offsets \
                     would interleave in one space; pick one (generate a log with the \
                     `edge_log` example if you want simulated data through the log path)"
                );
            }
            // A directory is a partitioned stream (`partition-<n>.ndjson`), a
            // file is a single partition. Both consume single-writer.
            let mut log: Box<dyn LogSource> = if std::path::Path::new(path).is_dir() {
                Box::new(blaze::ingest::PartitionedLog::open_dir(
                    path,
                    args.edge_log_follow,
                )?)
            } else {
                Box::new(blaze::ingest::FileLog::open(path, args.edge_log_follow)?)
            };
            // Recovery: skip what the last committed snapshot already covers, so
            // the replayed suffix is exactly the applied-but-uncommitted tail.
            log.seek(&start_position)?;
            info!(
                path,
                watermark,
                follow = args.edge_log_follow,
                batch = args.edge_log_batch,
                "consuming edges from a log"
            );
            let cfg = blaze::ingest::ConsumerConfig {
                batch: args.edge_log_batch,
                ..Default::default()
            };
            let (p, stop) = (pipeline.clone(), stop_ingest.clone());
            // A blocking thread on purpose: applying an edge is CPU work, and
            // leaving it on the runtime lets a batch stall query handlers.
            let handle = tokio::task::spawn_blocking(move || {
                match blaze::ingest::consume(log, &p, cfg, stop) {
                    // The position, not the return value: `consume` reports
                    // partition 0, which on a partitioned stream is one of
                    // several and on its own says almost nothing.
                    Ok(_) => info!(position = %p.stats.position(), "edge log drained"),
                    Err(e) => error!(error = %e, "edge log consumer stopped"),
                }
            });
            (None, handle)
        }
        None => {
            let (tx, rx) = mpsc::channel(65_536);
            if args.simulate {
                let cfg = SimulatorConfig {
                    rate: args.sim_rate,
                    nodes: args.sim_nodes,
                    scopes: args.sim_scopes,
                    ..Default::default()
                };
                info!(?cfg, "starting firehose simulator");
                tokio::spawn(run_simulator(cfg, tx.clone()));
            }
            let p = pipeline.clone();
            (Some(tx), tokio::spawn(async move { p.run(rx).await }))
        }
    };

    // Micro-batch flusher.
    let flusher = Arc::new(Flusher {
        forest: forest.clone(),
        buffer: buffer.clone(),
        store: store.clone(),
        catalog: catalog.clone(),
        elector: elector.clone(),
        table_prefix: table_prefix.clone(),
        worker_id: worker_id.clone(),
        stream: stream_id.clone(),
        base_dir,
        follower_fold_after_links: args.follower_fold_after_links,
        max_delta_layers: args.max_delta_layers,
        tier_fanout: args.tier_fanout,
        write: blaze::storage::WriteOptions {
            filter_bits: args.filter_bits,
            registry: args.registry_encoding,
        },
        inline_merges: args.inline_merges,
        pending_merge: parking_lot::Mutex::new(None),
        layers: parking_lot::Mutex::new(local_layers),
    });
    let flush_handle = tokio::spawn(
        flusher
            .clone()
            .run(Duration::from_secs(args.flush_interval_secs)),
    );

    // Retention. A separate loop rather than part of the tick: reclaiming
    // storage is not on the commit path, must not be able to slow it down, and
    // runs on a far longer period than a flush. Leader-gated only to avoid
    // duplicated work — a sweep is idempotent and safe from any worker.
    if args.retention_interval_secs > 0 {
        let policy = blaze::storage::RetentionPolicy {
            keep_snapshots: args.keep_snapshots,
            keep_for: Duration::from_secs(args.keep_snapshots_hours * 3600),
            grace: Duration::from_secs(args.retention_grace_secs),
        };
        let (store, catalog, prefix, elector) = (
            store.clone(),
            catalog.clone(),
            table_prefix,
            elector.clone(),
        );
        let period = Duration::from_secs(args.retention_interval_secs);
        info!(
            period_secs = args.retention_interval_secs,
            keep_snapshots = policy.keep_snapshots,
            keep_hours = args.keep_snapshots_hours,
            grace_secs = args.retention_grace_secs,
            "retention enabled"
        );
        tokio::spawn(async move {
            let mut every = tokio::time::interval(period);
            // Skip the immediate first tick: a worker that just started has not
            // yet observed the catalog it would be sweeping.
            every.tick().await;
            loop {
                every.tick().await;
                if !elector.is_leader() {
                    continue;
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                if let Err(e) =
                    blaze::storage::collect_garbage(&store, &catalog, &prefix, &policy, now).await
                {
                    warn!(error = %e, "retention sweep failed; will retry next period");
                }
            }
        });
    }

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
            // A log consumer runs on a blocking thread, where `abort` does not
            // interrupt anything; it checks this between batches instead. Setting
            // the flag before aborting covers both ingest shapes.
            stop_ingest.store(true, std::sync::atomic::Ordering::Relaxed);
            pipeline_handle.abort();
            if let Err(e) = flusher.tick().await {
                tracing::warn!(error = %e, "final flush failed");
            }
        }
    }
    Ok(())
}
