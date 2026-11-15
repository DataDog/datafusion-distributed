use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

use crate::eager::assign_files;
use iceberg_datafusion::read::EagerIcebergDataSource;

/// Like DFD's FileScanConfig scale-up handler, build one scan variant per task
/// before routing. This handler chooses each task's byte-balanced file assignment
/// and local groups; upstream preserves them for routing, transport, and execution.
pub(crate) fn iceberg_scale_up(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let exec = ev.plan.downcast_ref::<DataSourceExec>()?;
    let source = exec
        .data_source()
        .downcast_ref::<EagerIcebergDataSource>()?;
    if ev.task_count <= 1 {
        return None;
    }
    Some((|| {
        let assignments = assign_files(
            source.planned_files(),
            ev.task_count,
            source.output_partitioning().partition_count(),
        )?;
        let sources = assignments
            .into_iter()
            .map(|groups| source.with_planned_files(groups))
            .collect::<Result<Vec<_>>>()?;
        let variants = sources
            .into_iter()
            .map(|source| DataSourceExec::from_data_source(source) as Arc<dyn ExecutionPlan>);
        let plan = DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?;
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(plan)))
    })())
}
