//! Parquet encoding of sealed Arrow segments.

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::ingest::{Segment, edge_schema};

/// Encode sealed segments into a single Parquet file (one row group per
/// segment, rows already clustered by `scope_id`). Returns the encoded bytes
/// and total row count.
pub fn segments_to_parquet(segments: &[Segment]) -> anyhow::Result<(Vec<u8>, u64)> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, edge_schema(), Some(props))?;
    let mut rows = 0u64;
    for segment in segments {
        writer.write(&segment.batch)?;
        // Explicit row-group boundary per segment keeps offset ranges
        // aligned with row groups for debuggability.
        writer.flush()?;
        rows += segment.batch.num_rows() as u64;
    }
    writer.close()?;
    Ok((out, rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EdgeEvent, Visibility};
    use crate::ingest::EdgeBuffer;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn parquet_roundtrip() {
        let buf = EdgeBuffer::new();
        for i in 0..10u64 {
            buf.append(
                i,
                &EdgeEvent {
                    src: i,
                    dst: i + 1,
                    visibility: Visibility::Global,
                    event_time_ms: 1000 + i as i64,
                    props: None,
                },
            );
        }
        let seg = buf.seal_active().unwrap();
        let (bytes, rows) = segments_to_parquet(&[seg]).unwrap();
        assert_eq!(rows, 10);

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let read_rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(read_rows, 10);
    }
}
