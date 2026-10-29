//! Run with: cargo run --features integration --example materialized_recovery
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::object_store::{memory::InMemory, path::Path};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::WorkerQueryContext;
use datafusion_distributed::materialized::{
    MaterializedExecution, MaterializedStore, ObjectStoreIntermediateStore, RecoveryAction,
    ReplayPolicy,
};
use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
use datafusion_distributed::test_utils::parquet::register_parquet_tables;
use std::sync::Arc;

// This example executes only the fixed read-only query below against immutable
// repository fixtures. Production policies must validate their own snapshots,
// UDFs and worker hooks; they must not accept arbitrary plans this way.
#[derive(Debug)]
struct FixtureQuery;

impl ReplayPolicy for FixtureQuery {
    fn validate(&self, _: &Arc<dyn ExecutionPlan>, _: &TaskContext) -> Result<()> {
        Ok(())
    }

    fn on_error(&self, _error: &DataFusionError) -> RecoveryAction {
        // Keep unknown/deterministic errors fatal. Applications classify their
        // own structured transport/store failures here.
        RecoveryAction::Fail
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let store = Arc::new(MaterializedStore::new(Arc::new(
        ObjectStoreIntermediateStore::new(Arc::new(InMemory::new()), Path::from("example")),
    )));
    let worker_store = Arc::clone(&store);
    let context = start_configured_in_memory_context(
        3,
        move |mut query: WorkerQueryContext| {
            let store = Arc::clone(&worker_store);
            async move {
                let config = query.builder.config().get_or_insert_default();
                config.set_extension(store);
                config
                    .options_mut()
                    .optimizer
                    .enable_dynamic_filter_pushdown = false;
                Ok::<SessionState, DataFusionError>(query.builder.build())
            }
        },
        |worker| worker,
    )
    .await;
    {
        let state = context.state_ref();
        let mut state = state.write();
        let config = state.config_mut();
        config
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = false;
        config.set_extension(store);
        config.set_extension(Arc::new(MaterializedExecution::new(Arc::new(FixtureQuery))));
    }
    register_parquet_tables(&context).await?;
    context
        .sql("SELECT COUNT(*) AS observations FROM weather")
        .await?
        .show()
        .await?;
    Ok(())
}
