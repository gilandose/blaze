//! # blaze
//!
//! A highly concurrent, stateful streaming engine bridging millisecond graph
//! operations and immutable object storage.
//!
//! - **In-memory state** ([`core`]): lock-free-read DSU merges over a global
//!   layer plus ~3000 per-tenant scope overlays; Arrow-buffered raw edges.
//! - **Ingest** ([`ingest`]): single-writer pipeline consuming the edge
//!   firehose, sealing offset-tagged Arrow segments per flush tick.
//! - **Real-time API** ([`api`]): embedded Axum service answering component
//!   lookups and connectivity checks from shared memory.
//! - **gRPC** ([`grpc`]): a tonic front end over the same shared state,
//!   mirroring the REST query and edge-injection semantics.
//! - **Persistence** ([`storage`]): micro-batch flushes to object storage —
//!   Parquet data files plus a Puffin sidecar carrying the DSU routing maps,
//!   made visible by an atomic snapshot-catalog commit.
//! - **HA** ([`ha`]): all workers ingest and serve; a Kubernetes Lease
//!   elects the single committer.

pub mod api;
pub mod core;
pub mod grpc;
pub mod ha;
pub mod ingest;
pub mod storage;
