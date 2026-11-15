use crate::IcebergFileIOCodec;
use crate::common::reader::scan_reader;
use crate::file_io_codec::FileIOCodec;
use iceberg_datafusion::read::{EagerIcebergDataSource, IcebergDataSourceBuilder};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_datafusion_err, internal_err};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::proto_error;
use iceberg::io::StorageFactory;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use prost::Message;

use crate::proto::generated::iceberg as pb;

/// Physical plan codec for [`EagerIcebergDataSource`].
#[derive(Debug, Clone)]
pub struct EagerIcebergCodec {
    iceberg_runtime: iceberg::Runtime,
    file_io_codec: FileIOCodec,
}

impl EagerIcebergCodec {
    /// Creates a codec using the storage and runtime configured on this process.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self {
            iceberg_runtime,
            file_io_codec: FileIOCodec::new(storage_factory),
        }
    }

    /// Overrides file IO encoding and decoding while retaining the standard plan codec.
    /// For session registration, use [`crate::IcebergIntegrationOptions::file_io_codec`].
    pub fn with_file_io_codec(mut self, codec: Arc<dyn IcebergFileIOCodec>) -> Self {
        self.file_io_codec.custom = Some(codec);
        self
    }
}

impl Default for EagerIcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

impl PhysicalExtensionCodec for EagerIcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "EagerIcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }
        let source = pb::AssignedScan::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode EagerIcebergDataSource: {error}"))
        })?;
        let config = source
            .config
            .ok_or_else(|| proto_error("Missing Iceberg scan config"))?;
        let schema = config
            .schema
            .ok_or_else(|| proto_error("EagerIcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            config.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("EagerIcebergDataSource is missing its partitioning"))?;
        let fetch = config
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let file_io = self.file_io_codec.decode(
            config.storage_properties,
            config.custom_file_io.as_deref(),
            ctx,
        )?;
        if source.partitions.is_empty() || source.partitions.len() != partitioning.partition_count()
        {
            return internal_err!("Iceberg assigned files do not match scan partitioning");
        }
        let groups = source
            .partitions
            .into_iter()
            .map(|group| {
                group
                    .tasks
                    .into_iter()
                    .map(|bytes| {
                        rmp_serde::from_slice(&bytes).map_err(|error| {
                            exec_datafusion_err!("Invalid assigned Iceberg file: {error}")
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let reader = scan_reader(file_io.clone(), self.iceberg_runtime.clone(), ctx);
        Ok(DataSourceExec::from_data_source(
            IcebergDataSourceBuilder::from_planned_files(schema, groups, file_io, reader)
                .with_fetch(fetch)
                .build_planned(ctx)?,
        ))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return internal_err!(
                "expected DataSourceExec wrapping EagerIcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<EagerIcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping EagerIcebergDataSource");
        };
        let partitions = (0..source.output_partitioning().partition_count())
            .map(|partition| {
                let tasks = source
                    .planned_partition_files(partition)?
                    .map(|file| {
                        rmp_serde::to_vec_named(file).map_err(|error| {
                            exec_datafusion_err!("Cannot encode assigned Iceberg file: {error}")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(pb::FileGroup { tasks })
            })
            .collect::<Result<Vec<_>>>()?;
        let proto = pb::AssignedScan {
            config: Some(self.encode_config(source, proto_converter)?),
            partitions,
        };
        proto.encode(buf).map_err(|error| {
            proto_error(format!("failed to encode EagerIcebergDataSource: {error}"))
        })
    }
}

impl EagerIcebergCodec {
    fn encode_config(
        &self,
        source: &EagerIcebergDataSource,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<pb::ScanConfig> {
        let file_io = self.file_io_codec.encode(source.file_io())?;
        Ok(pb::ScanConfig {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                source.schema().as_ref(),
            )?),
            partitioning: Some(serialize_partitioning(
                &source.output_partitioning(),
                self,
                proto_converter,
            )?),
            fetch: source.fetch().map(|value| value as u64),
            storage_properties: file_io.properties,
            custom_file_io: file_io.custom,
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, create_taxi_table};

    use super::*;
    use crate::IcebergExt;

    #[tokio::test]
    async fn rejects_missing_config_and_invalid_assigned_groups() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source = exec
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let codec = EagerIcebergCodec::default();
        let context = TaskContext::default();
        let proto_converter = datafusion_proto::physical_plan::DefaultPhysicalProtoConverter {};
        let config = codec.encode_config(source, &proto_converter)?;
        let missing = pb::AssignedScan {
            config: None,
            partitions: vec![],
        }
        .encode_to_vec();
        assert!(
            codec
                .try_decode(&missing, &[], &context, &proto_converter)
                .is_err()
        );
        let mut malformed =
            vec![pb::FileGroup { tasks: vec![] }; source.output_partitioning().partition_count()];
        malformed[0].tasks.push(vec![0xc1]); // Invalid MessagePack descriptor.
        for partitions in [vec![], malformed] {
            let bytes = pb::AssignedScan {
                config: Some(config.clone()),
                partitions,
            }
            .encode_to_vec();
            assert!(
                codec
                    .try_decode(&bytes, &[], &context, &proto_converter)
                    .is_err()
            );
        }
        Ok(())
    }
}
