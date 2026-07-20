# Deploying blaze to EKS

## Build and push the image

The `Dockerfile` at the repo root builds a release binary with the `k8s`
feature (Kubernetes Lease election, `src/ha/kube_lease.rs`) enabled:

```bash
docker build -t <ECR_REPO>:<TAG> .
docker push <ECR_REPO>:<TAG>
```

## Apply the manifests

`deploy/kubernetes.yaml` has a comment header listing the placeholders to
substitute first: `BUCKET` (the S3 warehouse), `<IMAGE>` (the pushed image
reference above), and `<AWS_ROLE_ARN>` (an IAM role, associated with the
ServiceAccount via IRSA, that can read/write `BUCKET`). blaze never takes
AWS credentials as flags -- `object_store`'s S3 client reads them from the
environment, and IRSA is what populates that environment inside the pod.

```bash
sed -e 's#BUCKET#my-bucket/graph#g' \
    -e 's#<IMAGE>#123456789012.dkr.ecr.us-east-1.amazonaws.com/blaze:latest#g' \
    -e 's#<AWS_ROLE_ARN>#arn:aws:iam::123456789012:role/blaze-s3-access#g' \
    deploy/kubernetes.yaml | kubectl apply -f -
```

This creates the `blaze` namespace, a ServiceAccount, a Role/RoleBinding
scoped to `get`/`create`/`update` on `coordination.k8s.io` Leases (the only
permissions `KubeLeaseElector` needs), a 3-replica Deployment, and a
ClusterIP Service exposing the HTTP API (8080) and gRPC service (50051).

## Leader election and follower pruning in a fleet

Every replica in the Deployment runs the same binary with
`--election k8s --k8s-namespace blaze` and ingests and serves queries
identically -- reads never depend on leadership. What differs is who is
allowed to commit:

- Each pod runs an election loop (`run_election` in
  `src/ha/kube_lease.rs`) against a single Lease object (default name
  `blaze-committer`) in the `blaze` namespace. It tries to acquire the
  Lease if it is absent, expired, or already held by that pod's own
  identity (`$HOSTNAME`, wired up via the `fieldRef: metadata.name` env var
  in the Deployment), renewing at a third of the 15s lease duration. The
  API server's `resourceVersion` optimistic-concurrency check means a
  losing candidate's write is rejected with a 409, so two pods can never
  both believe they hold a given term simultaneously.
- On each flush tick (`--flush-interval-secs 60`), every pod seals its
  Arrow buffers, reads the snapshot catalog's latest committed watermark,
  and drops any of its own sealed segments at or below that watermark --
  this is the "follower pruning": non-leaders discard the data the leader
  already persisted instead of accumulating it forever.
- Only the pod currently holding the Lease additionally writes the Parquet
  data file and Puffin DSU snapshot and commits the catalog pointer via
  put-if-absent. If leadership changes mid-tick (lease expiry, pod
  restart, rolling update), the put-if-absent commit is the final
  arbiter: a stale leader's write loses the race harmlessly and its
  output becomes an orphan file, never visible to readers.
- A newly started pod (fresh rollout, or one that just won leadership)
  hydrates its in-memory DSU from the latest committed Puffin snapshot on
  boot (`hydrate_from_catalog`) and resumes ingestion at the committed
  watermark, so a leadership handoff loses no committed topology.

Net effect: scaling replicas up or down changes query capacity and
election contention, not correctness -- there is exactly one committer at
a time, enforced by the Lease's `resourceVersion` CAS and reinforced by the
catalog's own put-if-absent commit as a last line of defense.
