# blaze

A highly concurrent, stateful Rust streaming engine that bridges millisecond
graph operations and immutable object storage. One worker binary is
simultaneously:

- a **massive-throughput ingestion pipeline** for graph edge events, and
- a **real-time API serving layer** answering sub-millisecond component
  lookups from shared memory,

with periodic micro-batch persistence to S3-compatible object storage in
Iceberg-style layout (Parquet data + Puffin DSU sidecars), and Kubernetes
Lease leader election so a fleet of identical workers commits exactly once.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design, including how
multi-tenant **scope visibility** (~3000 tenant scopes plus a global scope)
is solved with a layered DSU.

## Quick start

```bash
# Run a standalone worker with the built-in firehose simulator:
cargo run --release -- \
  --simulate --sim-rate 20000 --sim-scopes 3000 \
  --warehouse file:///tmp/blaze-warehouse \
  --flush-interval-secs 60

# Health, stats
curl localhost:8080/v1/health
curl localhost:8080/v1/stats

# Component lookup ("where does Graph 500 route?") in the global scope
curl localhost:8080/v1/scopes/global/components/500

# Connectivity as tenant scope 77 sees it
curl "localhost:8080/v1/scopes/77/connected?u=1&v=2"

# Inject an edge visible only to scopes 77 and 78
curl -X POST localhost:8080/v1/edges \
  -H 'content-type: application/json' \
  -d '{"src": 1, "dst": 2, "scopes": [77, 78]}'
```

Restarting against the same warehouse hydrates the in-memory DSU from the
latest committed Puffin snapshot — no event replay needed for topology.

### Ingesting from a log

The API path above mints its own offsets, which is fine for one worker and a
demo. A production deployment reads a log, so that the *log* assigns the offset:
every consumer then numbers the same record the same way, which is what makes the
committed watermark portable across a failover instead of being a private number
only the worker that wrote it can interpret.

```bash
# Generate a log — newline-delimited JSON, offset = line number
LINKS=50000000 OUT=edges.ndjson cargo run --release --example edge_log

# Streaming: follow the tail, the way a consumer follows a topic
cargo run --release -- --edge-log edges.ndjson \
  --warehouse file:///tmp/blaze-warehouse

# Batch: load a finite file, flush, then idle serving queries
cargo run --release -- --edge-log edges.ndjson --edge-log-follow false ...
```

Point `--edge-log` at a **directory** of `partition-<n>.ndjson` files and blaze
consumes them as one partitioned stream, checkpointing each partition
independently — a snapshot records `{"0": 900, "1": 400, "2": 1300}` rather than
a single number. A topic that gains a partition needs no migration: absent means
zero, so the new partition is simply consumed from its beginning. Consumption
stays single-writer throughout; partitions buy correctness against a partitioned
topic, not parallelism.

A restart seeks to the committed position and replays exactly the records that
were applied but never committed; redelivery below it is skipped, so the rows
land once. `--edge-log` refuses `POST /v1/edges` while set, because an injected
edge has no log position and minting one would collide with the log's numbering.
Swapping the file for Kafka is an implementation of the `LogSource` trait —
`poll` and `seek`. See [docs/TUNING.md](docs/TUNING.md#where-edges-come-in) and
[design 010](docs/design/010-stream-position.md).

## Checking the answers against something we did not write

`tools/cc_oracle.py` grades `scope_root` against
`scipy.sparse.csgraph.connected_components` on published graphs — Knuth's
five-letter words (Stanford GraphBase, 1993) and the MUSAE Facebook page-page
network (Rozemberczki et al., 2019). Every other correctness test here compares
blaze to something blaze's authors wrote, which catches regressions well and
shared misconceptions not at all.

```
pip install scipy numpy
python3 tools/cc_oracle.py --dataset facebook --global-share 0.15
```

It checks the exact expected root for every node in every scope, not partitions
up to relabelling, and it runs three ways. `memory` and `storage` are in-process.
`api` is the end-to-end one: it starts the real binary, ingests over HTTP,
**restarts the worker mid-stream** so the rest is answered on top of state
hydrated from the catalog, then queries the roots back through the API.

The `words` dataset is vendored under `tests/data`, so the end-to-end run needs
no network.

In CI it runs on **every push to `main`**, and on a pull request only when the
`e2e` label is applied — it builds in release, starts the binary and restarts it
mid-stream, so it costs minutes where the other jobs cost seconds. Label a pull
request that touches ingest, storage or recovery and it runs before the merge;
everything else is still graded after it lands.

## Routing-state modes

| `--routing-base` | Committed state lives in | Cold start | Use when |
|---|---|---|---|
| `ram` (default) | the heap (hydrated from Puffin) | O(pairs) | state fits comfortably in RAM |
| `disk` | an mmap'd Puffin file under `--data-dir` | O(blobs) — milliseconds | state is large, or fast restarts matter |

```bash
# Serve committed routing state from a local NVMe cache instead of the heap:
blaze --routing-base disk --data-dir /nvme/blaze --warehouse s3://bucket/prefix
```

In `disk` mode the in-memory DSU holds only merges applied since the last
compaction; queries compose the mapped base with that memtable. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the composition rules and measured
cold-start/latency numbers.

**Sizing this to your hardware — including running billions of links on a small
box — is [docs/TUNING.md](docs/TUNING.md).** It covers which memory terms are
reclaimable and which are not, worked configurations from 4 GB up, the measured
trade curve for each flag, and why serving and backfill want opposite settings.

## Warehouse backends

| URI | Backend |
|---|---|
| `file:///path` | local filesystem (dev) |
| `s3://bucket/prefix` | S3 via env credentials (`AWS_*`) |
| `memory://` | in-process, for tests/demos |

## HA deployment (EKS)

Build with the `k8s` feature and run several replicas with
`--election k8s`:

```bash
cargo build --release --features k8s
blaze --election k8s --k8s-namespace graph --k8s-lease blaze-committer
```

All replicas consume the firehose and serve queries; the Lease holder is the
only one that commits Parquet/Puffin snapshots to the catalog. Followers
observe each committed watermark and discard their duplicate buffers.

## API

| Route | Description |
|---|---|
| `GET /v1/health` | liveness + leadership |
| `GET /v1/stats` | forest/buffer/ingest counters |
| `GET /v1/scopes/{scope}/components/{node}` | canonical component id for `node` in `scope` — the lowest graph id in the component (`global` or numeric scope id) |
| `GET /v1/scopes/{scope}/connected?u=&v=` | connectivity check in `scope`'s view |
| `POST /v1/edges` | inject an edge event (`{"src", "dst", "scopes": [..], "props"}`; empty scopes = global). 409 on a `--edge-log` worker |

### gRPC

A tonic `BlazeService` (default `0.0.0.0:50051`, `--grpc-listen`) serves the
same semantics over the same shared, lock-free state: `GetComponent`,
`CheckConnected`, `GetStats`, and `InjectEdge` (scopes as `uint32`, 0 =
global). The proto lives in [`proto/blaze/v1/blaze.proto`](proto/blaze/v1/blaze.proto)
and is compiled at build time with the pure-Rust `protox` codegen — no
`protoc` binary required.

## Development

```bash
cargo test              # unit + integration (includes a randomized model check
                        # of the scoped DSU against a BFS reference)
cargo clippy --all-targets
cargo check --features k8s
```
