use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion_distributed::WorkUnitFeed;
use futures::StreamExt;
use iceberg::scan::FileScanTaskStream;
use iceberg_datafusion::read::FileScanTaskSource;

use crate::IcebergWorkUnitFeed;
use crate::common::iceberg_err;

/// Supplies DFD's local or remote work-unit feed to the upstream Iceberg source.
///
/// Lazy scans use `iceberg_datafusion::read::IcebergDataSource<FeedTaskSource>`
/// directly. This adapter delivers file tasks; upstream owns file reading, limits,
/// reader metrics, FileIO, and statistics.
#[derive(Debug, Clone)]
pub struct FeedTaskSource {
    feed: WorkUnitFeed<IcebergWorkUnitFeed>,
    partitions: usize,
}

impl FeedTaskSource {
    pub(crate) fn new(feed: WorkUnitFeed<IcebergWorkUnitFeed>, partitions: usize) -> Self {
        Self { feed, partitions }
    }

    /// The feed registered with DFD for local execution or coordinator-to-worker delivery.
    pub fn feed(&self) -> &WorkUnitFeed<IcebergWorkUnitFeed> {
        &self.feed
    }
}

impl FileScanTaskSource for FeedTaskSource {
    fn partition_count(&self) -> usize {
        self.partitions
    }

    fn tasks(&self, partition: usize, context: Arc<TaskContext>) -> Result<FileScanTaskStream> {
        Ok(self
            .feed
            .feed(partition, context)?
            .map(|work_unit| {
                work_unit
                    .and_then(|work_unit| work_unit.into_task())
                    .map_err(iceberg_err)
            })
            .boxed())
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(feed) = self.feed.inner() else {
            return Ok(());
        };
        write!(f, ", table={}", feed.source.table().identifier())?;
        if feed.projected {
            write!(f, ", projection=[{}]", feed.source.projection().join(", "))?;
        }
        if let Some(predicate) = feed.source.predicate() {
            write!(f, ", predicate={predicate}")?;
        }
        Ok(())
    }
}
