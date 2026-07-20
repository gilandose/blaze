//! Firehose consumption and Arrow-native buffering.

pub mod buffer;
pub mod pipeline;
pub mod source;

pub use buffer::{BufferStats, EdgeBuffer, Segment, edge_schema};
pub use pipeline::{Pipeline, PipelineStats};
pub use source::{SimulatorConfig, run_simulator};
