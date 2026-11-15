use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Result, config_err, internal_datafusion_err};
use datafusion::datasource::source::DataSource;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_distributed::WorkUnitFeed;
use iceberg_datafusion::read::{IcebergDataSourceBuilder, IcebergScanFactory};

use crate::common::reader::scan_reader;
use crate::{FeedTaskSource, IcebergWorkUnitFeed};
use iceberg_datafusion::IcebergDataFusionConfig;

/// Distribution-specific scan construction, selected by upstream table providers.
#[derive(Debug)]
pub(crate) struct DistributedIcebergScanFactory {
    pub(crate) runtime: iceberg::Runtime,
}

#[async_trait]
impl IcebergScanFactory for DistributedIcebergScanFactory {
    async fn create_scan(
        &self,
        builder: IcebergDataSourceBuilder,
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn DataSource>> {
        let config = context
            .session_config()
            .options()
            .extensions
            .get::<IcebergDataFusionConfig>()
            .cloned()
            .unwrap_or_default()
            .execution;
        let partitions = context.session_config().target_partitions();
        if partitions == 0 {
            return config_err!("Iceberg scan requires at least one output partition");
        }
        let projected = builder.projection().is_some();
        let runtime = builder
            .runtime()
            .cloned()
            .unwrap_or_else(|| self.runtime.clone());
        let table = builder
            .table()
            .ok_or_else(|| internal_datafusion_err!("Provider scan requires a discovery table"))?;
        let reader = builder
            .reader()
            .cloned()
            .unwrap_or_else(|| scan_reader(table.file_io().clone(), runtime.clone(), &context));
        let builder = builder
            .with_reader(reader)
            .with_runtime(runtime)
            .with_statistics(true)
            .with_column_statistics(config.column_stats_enabled);
        if config.eager_file_planning {
            return Ok(Arc::new(builder.build_eager(context).await?));
        }
        let source = builder.build().await?;
        let feed = WorkUnitFeed::new(IcebergWorkUnitFeed {
            source: source.task_source().clone(),
            projected,
            partitioning: Partitioning::UnknownPartitioning(partitions),
            sync_manager: Default::default(),
        });
        Ok(Arc::new(
            source.with_task_source(FeedTaskSource::new(feed, partitions))?,
        ))
    }
}
