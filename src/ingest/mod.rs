//! Firehose consumption and Arrow-native buffering.

pub mod buffer;
pub mod log;
pub mod pipeline;
pub mod source;

pub use buffer::{BufferStats, EdgeBuffer, Segment, edge_schema};
pub use log::{ConsumerConfig, FileLog, LogSource, Record, consume};
pub use pipeline::{Pipeline, PipelineStats};
pub use source::{SimulatorConfig, run_simulator};
