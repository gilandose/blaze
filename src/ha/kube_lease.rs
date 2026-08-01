//! Kubernetes Lease-based leader election (feature `k8s`).
//!
//! Classic lease protocol over `coordination.k8s.io/v1`:
//! - acquire the lease if it is absent, expired, or already ours;
//! - renew at a third of the lease duration;
//! - rely on the API server's `resourceVersion` optimistic concurrency so two
//!   candidates can never both win a term.

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use super::LeaderFlag;

pub struct KubeLeaseConfig {
    pub namespace: String,
    pub lease_name: String,
    /// This worker's identity (typically the pod name).
    pub identity: String,
    pub lease_duration: Duration,
}

/// Run the election loop forever, flipping `flag` as leadership changes.
///
/// Builds a client from the ambient Kubernetes configuration — in-cluster
/// service account, or a kubeconfig. A worker that cannot reach an API server
/// runs as a permanent follower rather than failing to start, so a
/// misconfigured deployment degrades to "nobody writes" instead of "everybody
/// does".
pub async fn run_election(cfg: KubeLeaseConfig, flag: Arc<LeaderFlag>) {
    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "no kubernetes client available; running as follower");
            return;
        }
    };
    let api: Api<Lease> = Api::namespaced(client, &cfg.namespace);
    run_election_with(api, cfg, flag).await
}

/// The loop itself, over an API handle the caller supplies.
///
/// Split out from [`run_election`] so the protocol can be driven against a
/// stand-in API server. `Client::try_default` reads the ambient environment and
/// cannot be pointed anywhere, which is what had kept this loop — the thing that
/// decides who may write — from ever being executed by a test.
pub async fn run_election_with(api: Api<Lease>, cfg: KubeLeaseConfig, flag: Arc<LeaderFlag>) {
    let renew_every = cfg.lease_duration / 3;
    let mut was_leader = false;
    loop {
        let leader = try_acquire(&api, &cfg).await.unwrap_or_else(|e| {
            warn!(error = %e, "lease acquisition failed");
            false
        });
        if leader != was_leader {
            info!(leader, identity = %cfg.identity, "leadership changed");
            was_leader = leader;
        }
        flag.set(leader);
        tokio::time::sleep(renew_every).await;
    }
}

async fn try_acquire(api: &Api<Lease>, cfg: &KubeLeaseConfig) -> anyhow::Result<bool> {
    let now = MicroTime(Timestamp::now());
    let duration_secs = cfg.lease_duration.as_secs() as i32;

    match api.get_opt(&cfg.lease_name).await? {
        None => {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(cfg.lease_name.clone()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(cfg.identity.clone()),
                    acquire_time: Some(now.clone()),
                    renew_time: Some(now),
                    lease_duration_seconds: Some(duration_secs),
                    lease_transitions: Some(0),
                    ..Default::default()
                }),
            };
            // A racing candidate may create it first; that's a clean loss.
            match api.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e.into()),
            }
        }
        Some(mut lease) => {
            let spec = lease.spec.clone().unwrap_or_default();
            let holder = spec.holder_identity.clone().unwrap_or_default();
            let ours = holder == cfg.identity;
            let expired = match (&spec.renew_time, spec.lease_duration_seconds) {
                (Some(renew), Some(dur)) => {
                    Timestamp::now().as_second() - renew.0.as_second() > dur as i64
                }
                _ => true,
            };
            if !ours && !expired {
                return Ok(false);
            }
            let transitions = spec.lease_transitions.unwrap_or(0) + if ours { 0 } else { 1 };
            lease.spec = Some(LeaseSpec {
                holder_identity: Some(cfg.identity.clone()),
                acquire_time: if ours {
                    spec.acquire_time
                } else {
                    Some(now.clone())
                },
                renew_time: Some(now),
                lease_duration_seconds: Some(duration_secs),
                lease_transitions: Some(transitions),
                ..Default::default()
            });
            // replace() carries resourceVersion: a concurrent update makes
            // this 409 and we simply don't lead this round.
            match api
                .replace(&cfg.lease_name, &PostParams::default(), &lease)
                .await
            {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The election protocol, driven against a stand-in API server.
    //!
    //! This loop decides which worker may write. Everything downstream of it —
    //! the put-if-absent commit, the conformance preflight, the whole
    //! arbitration story — exists to contain the case where it gets the answer
    //! wrong, and until now it had never been executed by anything.
    //!
    //! `Client::try_default` reads the ambient environment, so the seam is
    //! [`run_election_with`]. The server below implements just enough of
    //! `coordination.k8s.io/v1` to be worth testing against, and the part that
    //! matters is **`resourceVersion` optimistic concurrency**: a `PUT` carrying
    //! a stale version is rejected with 409. That single behaviour is what the
    //! comment in `try_acquire` leans on when it claims two candidates cannot
    //! both win a term, and a fake without it would let this file's tests pass
    //! while proving nothing.

    use super::*;
    use crate::ha::LeaderElector;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::{Request, Response, StatusCode};
    use parking_lot::Mutex as PlMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One namespaced Lease, plus the version that guards writes to it.
    #[derive(Default)]
    struct Stored {
        lease: Option<serde_json::Value>,
        version: u64,
    }

    #[derive(Default)]
    struct FakeApi {
        state: PlMutex<Stored>,
        /// Forces the next `PUT` to 409 regardless of what it carries — a
        /// concurrent writer that landed between our GET and our PUT.
        steal_next_put: AtomicUsize,
    }

    impl FakeApi {
        fn get(&self) -> Response<Full<Bytes>> {
            match &self.state.lock().lease {
                Some(lease) => json(StatusCode::OK, lease.clone()),
                None => status(
                    StatusCode::NOT_FOUND,
                    "NotFound",
                    "leases.coordination.k8s.io not found",
                ),
            }
        }

        fn create(&self, mut body: serde_json::Value) -> Response<Full<Bytes>> {
            let mut state = self.state.lock();
            if state.lease.is_some() {
                // Exactly what a real API server does when two candidates both
                // find the lease absent and both try to create it.
                return status(StatusCode::CONFLICT, "AlreadyExists", "already exists");
            }
            state.version += 1;
            body["metadata"]["resourceVersion"] = state.version.to_string().into();
            state.lease = Some(body.clone());
            json(StatusCode::CREATED, body)
        }

        fn replace(&self, mut body: serde_json::Value) -> Response<Full<Bytes>> {
            let mut state = self.state.lock();
            let Some(current) = state.lease.clone() else {
                return status(StatusCode::NOT_FOUND, "NotFound", "not found");
            };
            if self.steal_next_put.load(Ordering::SeqCst) > 0 {
                self.steal_next_put.fetch_sub(1, Ordering::SeqCst);
                return status(
                    StatusCode::CONFLICT,
                    "Conflict",
                    "the object has been modified",
                );
            }
            // The guarantee the whole protocol rests on: a write carrying a
            // version that is no longer current loses.
            let sent = body["metadata"]["resourceVersion"].as_str().unwrap_or("");
            let held = current["metadata"]["resourceVersion"]
                .as_str()
                .unwrap_or("");
            if sent != held {
                return status(
                    StatusCode::CONFLICT,
                    "Conflict",
                    "the object has been modified",
                );
            }
            state.version += 1;
            body["metadata"]["resourceVersion"] = state.version.to_string().into();
            state.lease = Some(body.clone());
            json(StatusCode::OK, body)
        }

        fn holder(&self) -> Option<String> {
            self.state
                .lock()
                .lease
                .as_ref()
                .and_then(|l| l["spec"]["holderIdentity"].as_str().map(|s| s.to_string()))
        }

        fn transitions(&self) -> i64 {
            self.state
                .lock()
                .lease
                .as_ref()
                .and_then(|l| l["spec"]["leaseTransitions"].as_i64())
                .unwrap_or(-1)
        }

        /// Backdate the renew time so the lease reads as expired.
        fn expire(&self) {
            let mut state = self.state.lock();
            if let Some(lease) = state.lease.as_mut() {
                let long_ago = Timestamp::now() - std::time::Duration::from_secs(3600);
                lease["spec"]["renewTime"] = MicroTime(long_ago).0.to_string().into();
            }
            state.version += 1;
            let v = state.version.to_string();
            if let Some(lease) = state.lease.as_mut() {
                lease["metadata"]["resourceVersion"] = v.into();
            }
        }
    }

    fn json(code: StatusCode, body: serde_json::Value) -> Response<Full<Bytes>> {
        Response::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
            .unwrap()
    }

    /// A Kubernetes `Status` failure.
    ///
    /// `reason` is not decoration: `Api::get_opt` turns a 404 into `Ok(None)` by
    /// matching `reason == "NotFound"`, not by looking at the code. A fake that
    /// sends an empty reason makes every absent lease an error instead of an
    /// absence, and the acquire path never runs at all.
    fn status(code: StatusCode, reason: &str, message: &str) -> Response<Full<Bytes>> {
        json(
            code,
            serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": message, "reason": reason, "code": code.as_u16(),
            }),
        )
    }

    /// Serve the fake on an ephemeral port and return an `Api<Lease>` for it.
    async fn spawn(api: Arc<FakeApi>) -> Api<Lease> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let api = api.clone();
                tokio::spawn(async move {
                    let service =
                        hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                            let api = api.clone();
                            async move {
                                let method = req.method().clone();
                                let body = req.into_body().collect().await.unwrap().to_bytes();
                                let parsed = serde_json::from_slice(&body)
                                    .unwrap_or(serde_json::Value::Null);
                                Ok::<_, std::convert::Infallible>(match method {
                                    hyper::Method::GET => api.get(),
                                    hyper::Method::POST => api.create(parsed),
                                    hyper::Method::PUT => api.replace(parsed),
                                    _ => status(
                                        StatusCode::METHOD_NOT_ALLOWED,
                                        "MethodNotAllowed",
                                        "no",
                                    ),
                                })
                            }
                        });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await;
                });
            }
        });

        let config = kube::Config::new(format!("http://{addr}").parse().unwrap());
        let client = Client::try_from(config).unwrap();
        Api::namespaced(client, "blaze")
    }

    fn cfg(identity: &str) -> KubeLeaseConfig {
        KubeLeaseConfig {
            namespace: "blaze".into(),
            lease_name: "blaze-leader".into(),
            identity: identity.into(),
            lease_duration: Duration::from_secs(15),
        }
    }

    /// An empty namespace: the first candidate creates the lease and leads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_candidate_creates_the_lease_and_leads() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;

        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());
        assert_eq!(fake.holder().as_deref(), Some("worker-a"));
        assert_eq!(
            fake.transitions(),
            0,
            "a first acquisition is not a handover"
        );
    }

    /// The case the whole thing exists for: a live lease held by someone else is
    /// not takeable, however often it is asked for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_live_lease_held_by_another_worker_cannot_be_taken() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;
        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());

        for _ in 0..5 {
            assert!(
                !try_acquire(&api, &cfg("worker-b")).await.unwrap(),
                "a challenger took a lease that had not expired"
            );
        }
        assert_eq!(fake.holder().as_deref(), Some("worker-a"));
    }

    /// Renewal keeps the term rather than starting a new one — `leaseTransitions`
    /// is what an operator reads to tell "stable leader" from "flapping".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewing_keeps_the_same_term() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;

        for _ in 0..4 {
            assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());
        }
        assert_eq!(fake.transitions(), 0, "renewal counted as a handover");
    }

    /// A leader that stops renewing must be replaceable, or a crashed pod would
    /// wedge the table read-only forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_lease_is_taken_over_and_counts_a_transition() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;
        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());
        assert!(!try_acquire(&api, &cfg("worker-b")).await.unwrap());

        fake.expire();

        assert!(
            try_acquire(&api, &cfg("worker-b")).await.unwrap(),
            "an expired lease was not taken over; a dead leader would wedge writes"
        );
        assert_eq!(fake.holder().as_deref(), Some("worker-b"));
        assert_eq!(fake.transitions(), 1, "a handover must be counted");
    }

    /// The claim in `try_acquire`'s comment, tested: a `replace` that loses the
    /// optimistic-concurrency check means *not leader this round*, not an error
    /// and not a silent success.
    ///
    /// This is the one that matters. If a 409 were treated as anything other
    /// than a loss, two workers could believe they hold the same term, which is
    /// precisely the situation the snapshot commit's put-if-absent is the last
    /// line of defence against.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn losing_the_resource_version_race_is_a_clean_loss() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;
        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());

        // Someone else writes between our GET and our PUT.
        fake.steal_next_put.store(1, Ordering::SeqCst);
        assert!(
            !try_acquire(&api, &cfg("worker-a")).await.unwrap(),
            "a lost 409 must read as 'not leader', never as success"
        );

        // And the next round recovers: losing a race is not losing the lease.
        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());
    }

    /// The loop, not just one call: the flag follows leadership.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_election_loop_drives_the_leader_flag() {
        let fake = Arc::new(FakeApi::default());
        let api = spawn(fake.clone()).await;

        // Someone else holds it first, so the flag starts false and has to
        // *become* true rather than merely starting that way.
        assert!(try_acquire(&api, &cfg("worker-a")).await.unwrap());

        let flag = Arc::new(LeaderFlag::default());
        let mut cfg_b = cfg("worker-b");
        cfg_b.lease_duration = Duration::from_millis(300); // renews every 100ms
        let handle = tokio::spawn(run_election_with(
            spawn(fake.clone()).await,
            cfg_b,
            flag.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!flag.is_leader(), "took a lease that worker-a still holds");

        fake.expire();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            flag.is_leader(),
            "the loop never picked up the expired lease"
        );
        assert_eq!(fake.holder().as_deref(), Some("worker-b"));

        handle.abort();
    }
}
