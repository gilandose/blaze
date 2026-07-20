//! Edge event sources.
//!
//! Production deployments plug a log consumer (Kafka / Kinesis) into the
//! pipeline's mpsc channel; this module ships a configurable firehose
//! simulator so the engine runs end to end out of the box, plus the API's
//! injection path uses the same channel.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::core::{EdgeEvent, Visibility};

#[derive(Debug, Clone)]
pub struct SimulatorConfig {
    /// Target events per second.
    pub rate: u64,
    /// Node id space to draw endpoints from.
    pub nodes: u64,
    /// Number of tenant scopes (ids 1..=scopes; 0 is global).
    pub scopes: u32,
    /// Fraction (0-100) of edges that are globally visible.
    pub global_pct: u8,
    /// Fraction (0-100) of scoped edges visible to two scopes.
    pub multi_scope_pct: u8,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        Self {
            rate: 5_000,
            nodes: 1_000_000,
            scopes: 3_000,
            global_pct: 15,
            multi_scope_pct: 5,
            seed: None,
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Generate a firehose of random edge events until the channel closes.
pub async fn run_simulator(cfg: SimulatorConfig, tx: mpsc::Sender<EdgeEvent>) {
    let mut rng = match cfg.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => rand::make_rng(),
    };
    // Emit in 20ms micro-bursts to approximate the target rate without a
    // per-event timer.
    let tick = Duration::from_millis(20);
    let per_tick = (cfg.rate / 50).max(1) as usize;
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        for _ in 0..per_tick {
            let src = rng.random_range(0..cfg.nodes);
            let dst = rng.random_range(0..cfg.nodes);
            let visibility = if rng.random_range(0..100u8) < cfg.global_pct {
                Visibility::Global
            } else {
                let s1 = rng.random_range(1..=cfg.scopes);
                if rng.random_range(0..100u8) < cfg.multi_scope_pct {
                    let s2 = rng.random_range(1..=cfg.scopes);
                    Visibility::Scoped(smallvec::smallvec![s1, s2])
                } else {
                    Visibility::Scoped(smallvec::smallvec![s1])
                }
            };
            let event = EdgeEvent {
                src,
                dst,
                visibility,
                event_time_ms: now_ms(),
                props: Some(format!(r#"{{"weight":{}}}"#, rng.random_range(1..100u32))),
            };
            if tx.send(event).await.is_err() {
                return; // pipeline shut down
            }
        }
    }
}
