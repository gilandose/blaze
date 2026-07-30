//! Persistence: Parquet data files, Puffin DSU sidecars, snapshot catalog.

pub mod base;
pub mod catalog;
pub mod codec;
pub mod compact;
pub mod filter;
pub mod flush;
pub mod layered;
pub mod parquet_io;
pub mod puffin;

pub use base::PuffinBase;
pub use catalog::{CommitOutcome, DataFileMeta, RunMeta, RunSet, SnapshotCatalog, SnapshotMeta};
pub use compact::{CompactionStats, compact_layers};
pub use filter::BlockedFilter;
pub use flush::{
    DEFAULT_FOLD_AFTER_LINKS, DEFAULT_MAX_DELTA_LAYERS, Flusher, LocalLayers, LocalRun,
    hydrate_from_catalog, open_base_from_catalog,
};
pub use layered::LayeredBase;
