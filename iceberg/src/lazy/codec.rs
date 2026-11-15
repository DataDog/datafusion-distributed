use crate::IcebergFileIOCodec;
use crate::common::reader::scan_reader;
use crate::file_io_codec::FileIOCodec;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
use datafusion::datasource::source::DataSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{WorkUnitFeed, WorkUnitFeedProto};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::proto_error;
use iceberg::io::StorageFactory;
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use prost::Message;

use crate::proto::generated::iceberg as pb;
use crate::{FeedTaskSource, IcebergWorkUnitFeed};

/// Physical plan codec for upstream [`IcebergDataSource<FeedTaskSource>`] scans.
#[derive(Debug, Clone)]
pub struct LazyIcebergCodec {
    iceberg_runtime: iceberg::Runtime,
    file_io_codec: FileIOCodec,
}

impl LazyIcebergCodec {
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

impl Default for LazyIcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

impl PhysicalExtensionCodec for LazyIcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "feed-backed IcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }

        let proto = pb::IcebergDataSource::decode(buf).map_err(|error| {
            proto_error(format!(
                "failed to decode feed-backed IcebergDataSource: {error}"
            ))
        })?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("feed-backed IcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let feed = proto.feed.ok_or_else(|| {
            proto_error("feed-backed IcebergDataSource is missing its work-unit feed")
        })?;
        let feed =
            WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(WorkUnitFeedProto { id: feed.id })?;
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("feed-backed IcebergDataSource is missing its partitioning"))?;
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let iceberg_file_io = self.file_io_codec.decode(
            proto.storage_properties,
            proto.custom_file_io.as_deref(),
            ctx,
        )?;

        let reader = scan_reader(iceberg_file_io.clone(), self.iceberg_runtime.clone(), ctx);
        let task_source = FeedTaskSource::new(feed, partitioning.partition_count());
        Ok(DataSourceExec::from_data_source(
            IcebergDataSource::from_task_source(
                schema,
                task_source,
                iceberg_file_io,
                reader,
                fetch,
            )?,
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
                "expected DataSourceExec wrapping a feed-backed IcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec
            .data_source()
            .downcast_ref::<IcebergDataSource<FeedTaskSource>>()
        else {
            return internal_err!(
                "expected DataSourceExec wrapping a feed-backed IcebergDataSource"
            );
        };

        let file_io = self.file_io_codec.encode(source.file_io())?;
        let feed = source.task_source().feed().to_proto();
        let proto = pb::IcebergDataSource {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                source.schema().as_ref(),
            )?),
            feed: Some(pb::WorkUnitFeed { id: feed.id }),
            partitioning: Some(serialize_partitioning(
                &source.output_partitioning(),
                self,
                proto_converter,
            )?),
            fetch: source.fetch().map(|value| value as u64),
            storage_properties: file_io.properties,
            custom_file_io: file_io.custom,
        };
        proto.encode(buf).map_err(|error| {
            proto_error(format!(
                "failed to encode feed-backed IcebergDataSource: {error}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Statistics, stats::Precision};
    use datafusion::datasource::source::DataSource;
    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, create_taxi_table};
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
    use iceberg::table::Table;

    use super::*;
    use crate::IcebergExt;
    use crate::common::df_err;
    use iceberg_datafusion::IcebergStaticTableProvider;

    #[tokio::test]
    async fn roundtrips_data_source_plan() -> Result<()> {
        let taxi = create_taxi_table().await?;
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
            .with_props([
                ("fixture.storage", "roundtrip's value"),
                ("fixture.region", "test-region"),
            ])
            .build();
        let table = Table::builder()
            .metadata(taxi.table().metadata_ref())
            .identifier(taxi.ident().clone())
            .file_io(file_io)
            .runtime(iceberg::Runtime::current())
            .build()
            .map_err(df_err)?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_config_option("datafusion.execution.target_partitions", 4)
            .build()
            .await?;
        let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
        let plan = provider.scan(&harness.state(), None, &[], Some(10)).await?;
        let codec =
            LazyIcebergCodec::new(Arc::new(LocalFsStorageFactory), iceberg::Runtime::current());
        let proto_converter = DefaultPhysicalProtoConverter {};
        let mut buf = Vec::new();
        codec.try_encode(Arc::clone(&plan), &mut buf, &proto_converter)?;
        let decoded_plan =
            codec.try_decode(&buf, &[], &TaskContext::default(), &proto_converter)?;
        let source = iceberg_source(&plan)?;
        let decoded = iceberg_source(&decoded_plan)?;

        assert_eq!(source.schema(), decoded.schema());
        assert_eq!(
            source.output_partitioning().to_string(),
            decoded.output_partitioning().to_string()
        );
        assert_eq!(source.fetch(), decoded.fetch());
        assert_eq!(
            source.task_source().feed().to_proto(),
            decoded.task_source().feed().to_proto()
        );
        assert_eq!(
            source.file_io().config().props(),
            decoded.file_io().config().props()
        );
        let properties = decoded.file_io().config().props();
        assert_eq!(
            properties.get("fixture.storage").map(String::as_str),
            Some("roundtrip's value")
        );
        assert_eq!(
            properties.get("fixture.region").map(String::as_str),
            Some("test-region")
        );
        // Coordinator assignment statistics are not transported. Output estimates
        // still account for the decoded per-partition fetch (4 partitions * 10).
        assert_eq!(
            decoded.unlimited_statistics().as_ref(),
            &Statistics::new_unknown(&decoded.schema())
        );
        assert_eq!(
            decoded.partition_statistics(None)?.num_rows,
            Precision::Inexact(40)
        );
        Ok(())
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Result<&IcebergDataSource<FeedTaskSource>> {
        let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
            return internal_err!("expected a DataSourceExec");
        };
        exec.data_source()
            .downcast_ref::<IcebergDataSource<FeedTaskSource>>()
            .ok_or_else(|| proto_error("expected a feed-backed IcebergDataSource"))
    }
}
