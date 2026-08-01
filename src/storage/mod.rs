//! Persistence: Parquet data files, Puffin DSU sidecars, snapshot catalog.

pub mod base;
pub mod catalog;
pub mod codec;
pub mod compact;
pub mod conformance;
pub mod filter;
pub mod flush;
pub mod layered;
pub mod parquet_io;
pub mod puffin;
pub mod registry;
pub mod retention;
pub mod tier;

pub use base::{PuffinBase, RegistryIter, ScanReader};
pub use catalog::{CommitOutcome, DataFileMeta, RunMeta, RunSet, SnapshotCatalog, SnapshotMeta};
pub use codec::WriteOptions;
pub use compact::{CompactionStats, compact_layers};
pub use conformance::{ConformanceError, verify_conditional_put};
pub use filter::{BlockedFilter, DEFAULT_FILTER_BITS};
pub use flush::{
    DEFAULT_FOLLOWER_FOLD_AFTER_LINKS, DEFAULT_MAX_DELTA_LAYERS, Flusher, LocalLayers, LocalRun,
    PendingMerge, hydrate_from_catalog, open_base_from_catalog,
};
pub use layered::LayeredBase;
pub use registry::{RegistryEncoding, RegistryWriter};
pub use retention::{Reclaimed, RetentionPolicy, collect as collect_garbage};
pub use tier::{DEFAULT_TIER_FANOUT, pick_merge};
