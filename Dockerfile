# syntax=docker/dockerfile:1
#
# Multi-stage build for the blaze worker binary.
#
# Build with:
#   docker build -t blaze:latest .
#
# The k8s feature (Kubernetes Lease leader election, src/ha/kube_lease.rs)
# is compiled in so the same image can run either standalone
# (--election static:true) or as part of an EKS fleet (--election k8s).

########################################################################
# Build stage
########################################################################
FROM rust:1.94-bookworm AS builder

WORKDIR /build

# Copy the whole workspace (see .dockerignore for exclusions) so build.rs /
# proto/ / src/ / Cargo.lock are all present regardless of layout changes.
COPY . .

RUN cargo build --release --features k8s --locked

########################################################################
# Runtime stage
########################################################################
# Debian slim tracks the same glibc family as the `rust` build image above,
# which the release binary is dynamically linked against.
FROM debian:bookworm-slim AS runtime

# ca-certificates: TLS to S3-compatible object storage and to the
# Kubernetes API server for Lease election.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin blaze

COPY --from=builder /build/target/release/blaze /usr/local/bin/blaze

USER blaze

# HTTP API (health, stats, edge/query REST routes).
EXPOSE 8080
# gRPC service (tonic, same AppState as the HTTP API).
EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/blaze"]
