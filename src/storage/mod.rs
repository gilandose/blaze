//! Persistence: Parquet data files, Puffin DSU sidecars, snapshot catalog.

pub mod catalog;
pub mod codec;
pub mod flush;
pub mod parquet_io;
pub mod puffin;

pub use catalog::{CommitOutcome, DataFileMeta, SnapshotCatalog, SnapshotMeta};
pub use flush::{Flusher, hydrate_from_catalog};
