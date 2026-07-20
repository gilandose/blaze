//! A minimal, spec-compliant implementation of the Iceberg **Puffin** file
//! format (<https://iceberg.apache.org/puffin-spec/>), used here as the
//! sidecar that carries the serialized DSU routing maps next to the Parquet
//! data files.
//!
//! Layout:
//!
//! ```text
//! Magic (PFA1) | blob₀ | blob₁ | … | Magic | footer payload (JSON)
//!            | payload size (u32 LE) | flags (u32 LE) | Magic
//! ```
//!
//! Blobs are opaque byte ranges; the footer's JSON `FileMetadata` records
//! each blob's type, offset, length, and string properties.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const MAGIC: [u8; 4] = *b"PFA1";

#[derive(Debug, Error)]
pub enum PuffinError {
    #[error("file too small to be a puffin file")]
    TooSmall,
    #[error("bad magic at {0}")]
    BadMagic(&'static str),
    #[error("footer payload is not valid metadata json: {0}")]
    BadFooter(#[from] serde_json::Error),
    #[error("blob range {offset}+{length} out of bounds (file len {file_len})")]
    BlobOutOfBounds {
        offset: u64,
        length: u64,
        file_len: usize,
    },
    #[error("unsupported footer flags {0:#x} (compressed footers not supported)")]
    UnsupportedFlags(u32),
}

/// One blob to be written into (or read from) a Puffin file.
#[derive(Debug, Clone)]
pub struct Blob {
    pub blob_type: String,
    pub data: Bytes,
    pub properties: BTreeMap<String, String>,
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlobMetadata {
    #[serde(rename = "type")]
    blob_type: String,
    fields: Vec<i32>,
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    sequence_number: i64,
    offset: u64,
    length: u64,
    #[serde(
        rename = "compression-codec",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    compression_codec: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileMetadata {
    blobs: Vec<BlobMetadata>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, String>,
}

/// Serialize blobs into a complete Puffin file.
pub fn write(blobs: &[Blob], file_properties: BTreeMap<String, String>) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(&MAGIC);

    let mut metas = Vec::with_capacity(blobs.len());
    for blob in blobs {
        let offset = out.len() as u64;
        out.put_slice(&blob.data);
        metas.push(BlobMetadata {
            blob_type: blob.blob_type.clone(),
            fields: vec![],
            snapshot_id: blob.snapshot_id,
            sequence_number: blob.sequence_number,
            offset,
            length: blob.data.len() as u64,
            compression_codec: None,
            properties: blob.properties.clone(),
        });
    }

    let footer = FileMetadata {
        blobs: metas,
        properties: file_properties,
    };
    let payload = serde_json::to_vec(&footer).expect("footer metadata serializes");

    out.put_slice(&MAGIC);
    out.put_slice(&payload);
    out.put_u32_le(payload.len() as u32);
    out.put_u32_le(0); // flags: uncompressed footer
    out.put_slice(&MAGIC);
    out.freeze()
}

/// Parse a Puffin file back into its blobs.
pub fn read(data: &[u8]) -> Result<Vec<Blob>, PuffinError> {
    let n = data.len();
    if n < MAGIC.len() * 3 + 8 {
        return Err(PuffinError::TooSmall);
    }
    if data[..4] != MAGIC {
        return Err(PuffinError::BadMagic("file start"));
    }
    if data[n - 4..] != MAGIC {
        return Err(PuffinError::BadMagic("file end"));
    }
    let flags = u32::from_le_bytes(data[n - 8..n - 4].try_into().unwrap());
    if flags != 0 {
        return Err(PuffinError::UnsupportedFlags(flags));
    }
    let payload_len = u32::from_le_bytes(data[n - 12..n - 8].try_into().unwrap()) as usize;
    let payload_end = n - 12;
    let payload_start = payload_end
        .checked_sub(payload_len)
        .ok_or(PuffinError::TooSmall)?;
    let magic_start = payload_start.checked_sub(4).ok_or(PuffinError::TooSmall)?;
    if data[magic_start..payload_start] != MAGIC {
        return Err(PuffinError::BadMagic("footer start"));
    }
    let footer: FileMetadata = serde_json::from_slice(&data[payload_start..payload_end])?;

    footer
        .blobs
        .into_iter()
        .map(|meta| {
            let start = meta.offset as usize;
            let end = start + meta.length as usize;
            if end > n {
                return Err(PuffinError::BlobOutOfBounds {
                    offset: meta.offset,
                    length: meta.length,
                    file_len: n,
                });
            }
            Ok(Blob {
                blob_type: meta.blob_type,
                data: Bytes::copy_from_slice(&data[start..end]),
                properties: meta.properties,
                snapshot_id: meta.snapshot_id,
                sequence_number: meta.sequence_number,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let blobs = vec![
            Blob {
                blob_type: "blaze-global-dsu-v1".into(),
                data: Bytes::from_static(b"hello"),
                properties: BTreeMap::from([("k".into(), "v".into())]),
                snapshot_id: 42,
                sequence_number: 42,
            },
            Blob {
                blob_type: "blaze-scope-dsu-v1".into(),
                data: Bytes::from_static(b"world!"),
                properties: BTreeMap::from([("scope-id".into(), "17".into())]),
                snapshot_id: 42,
                sequence_number: 42,
            },
        ];
        let file = write(
            &blobs,
            BTreeMap::from([("created-by".into(), "blaze".into())]),
        );

        // Structural spot-checks against the spec.
        assert_eq!(&file[..4], b"PFA1");
        assert_eq!(&file[file.len() - 4..], b"PFA1");

        let parsed = read(&file).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].blob_type, "blaze-global-dsu-v1");
        assert_eq!(parsed[0].data.as_ref(), b"hello");
        assert_eq!(parsed[1].data.as_ref(), b"world!");
        assert_eq!(parsed[1].properties["scope-id"], "17");
        assert_eq!(parsed[0].snapshot_id, 42);
    }

    #[test]
    fn rejects_garbage() {
        assert!(read(b"nope").is_err());
        assert!(read(b"PFA1xxxxxxxxxxxxxxxxxxxxPFA1").is_err());
    }

    #[test]
    fn empty_blob_list_roundtrips() {
        let file = write(&[], BTreeMap::new());
        assert!(read(&file).unwrap().is_empty());
    }
}
