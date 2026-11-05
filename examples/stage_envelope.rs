//! Preserve opaque wrappers around statically planned worker producer heads.
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::execution::SessionState;
use datafusion::physical_plan::collect;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
use datafusion_distributed::test_utils::stage_envelope::TestStageEnvelope;
use datafusion_distributed::{
    DistributedConfig, DistributedExec, DistributedExt, WorkerPlanRewriteEvent,
    WorkerPlanRewriteEventResponse, WorkerQueryContext,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let (mut ctx, _workers, _) = start_localhost_context(2, worker_state).await;
    let mut config = DistributedConfig::from_session_config(&ctx.copied_config())?.clone();
    config.rewrite_producer_head_at_execution = false;
    ctx.set_distributed_option_extension(config);
    ctx.set_distributed_metrics_collection(true)?;
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as _)])?;
    let file = register_temp_parquet_table("input", batch.schema(), vec![batch], &ctx).await?;
    let query = "SELECT id % 2 AS bucket, count(*) AS n FROM input GROUP BY id % 2 ORDER BY bucket";
    let plan = ctx.sql(query).await?.create_physical_plan().await?;

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    println!("{}", pretty_format_batches(&batches)?);
    let distributed = plan
        .downcast_ref::<DistributedExec>()
        .ok_or_else(|| exec_datafusion_err!("Expected a distributed example query"))?;
    let metrics = tokio::time::timeout(Duration::from_secs(10), distributed.wait_for_metrics())
        .await
        .map_err(|error| exec_datafusion_err!("{error}"))?
        .ok_or_else(|| exec_datafusion_err!("Expected worker metrics"))?;
    let roots = metrics
        .values()
        .filter(|task| {
            task.pre_order_plan_metrics.first().is_some_and(|root| {
                root.iter().any(|metric| {
                    metric.value().name() == "envelope_executions" && metric.value().as_usize() > 0
                })
            })
        })
        .count();
    println!(
        "Executed envelopes at {roots}/{} worker-task roots",
        metrics.len()
    );
    std::fs::remove_file(file)?;
    Ok(())
}

async fn worker_state(ctx: WorkerQueryContext) -> Result<SessionState> {
    Ok(ctx
        .builder
        .with_distributed_worker_plan_rewrite_handler(|event: WorkerPlanRewriteEvent<'_>| {
            Ok(WorkerPlanRewriteEventResponse::new(Arc::new(
                TestStageEnvelope::new(event.plan, Default::default()),
            )))
        })
        .build())
}
