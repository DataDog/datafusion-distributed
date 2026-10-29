use super::MaterializedStore;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::{reader::FileReader, writer::FileWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;
use std::io::Cursor;

/// Explicit ordered partition contents; empty is a successfully empty partition.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct PartitionManifest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) segments: Vec<Segment>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Segment {
    #[prost(string, tag = "1")]
    pub(crate) key: String,
    #[prost(uint64, tag = "2")]
    pub(crate) length: u64,
    #[prost(uint64, tag = "3")]
    pub(crate) rows: u64,
    #[prost(fixed64, tag = "4")]
    pub(crate) checksum: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct AttemptManifest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) partitions: Vec<PartitionManifest>,
}

pub(crate) async fn write_partition(
    store: &MaterializedStore,
    prefix: &str,
    mut stream: SendableRecordBatchStream,
) -> Result<PartitionManifest> {
    let mut segments = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        // Reject oversized input before allocating its IPC representation.
        if batch.get_array_memory_size() > store.max_segment_bytes {
            return exec_err!("Record batch exceeds materialized segment memory limit");
        }
        let rows = batch.num_rows() as u64;
        let bytes = SpawnedTask::spawn_blocking(move || -> Result<Bytes> {
            let mut writer = FileWriter::try_new(Vec::new(), &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
            Ok(writer.into_inner()?.into())
        })
        .join_unwind()
        .await
        .map_err(|error| exec_datafusion_err!("Materialized encoding task failed: {error}"))??;
        let key = format!("{prefix}/{}.arrow", segments.len());
        let length = bytes.len() as u64;
        let checksum = checksum(&bytes);
        store.put(&key, bytes).await?;
        segments.push(Segment {
            key,
            length,
            rows,
            checksum,
        });
    }
    Ok(PartitionManifest { segments })
}

pub(crate) async fn read_segment(
    store: &MaterializedStore,
    segment: &Segment,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if segment.length > store.max_segment_bytes as u64 {
        return exec_err!("Materialized segment exceeds configured read limit");
    }
    let bytes = store.store.get(&segment.key).await?;
    if bytes.len() as u64 != segment.length || checksum(&bytes) != segment.checksum {
        return exec_err!("Materialized segment integrity mismatch: {}", segment.key);
    }
    let rows = segment.rows;
    SpawnedTask::spawn_blocking(move || -> Result<RecordBatch> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
        if reader.schema() != schema || reader.num_batches() != 1 {
            return exec_err!("Invalid materialized segment schema or batch count");
        }
        let Some(batch) = reader.next() else {
            return exec_err!("Missing materialized record batch");
        };
        let batch = batch?;
        if batch.num_rows() as u64 != rows {
            return exec_err!("Materialized segment row count mismatch");
        }
        Ok(batch)
    })
    .join_unwind()
    .await
    .map_err(|error| exec_datafusion_err!("Materialized decoding task failed: {error}"))?
}

// FNV-1a is a fixed, version-independent integrity check, not an authentication
// mechanism. Storage access control remains the application's responsibility.
pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
