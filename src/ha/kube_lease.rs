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
pub async fn run_election(cfg: KubeLeaseConfig, flag: Arc<LeaderFlag>) {
    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "no kubernetes client available; running as follower");
            return;
        }
    };
    let api: Api<Lease> = Api::namespaced(client, &cfg.namespace);
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
