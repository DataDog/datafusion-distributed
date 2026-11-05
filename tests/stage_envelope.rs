#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::execution::SessionState;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::test_utils::stage_envelope::TestStageEnvelope;
    use datafusion_distributed::{
        BroadcastExec, DistributedExec, DistributedExt, WorkerPlanRewriteEvent,
        WorkerPlanRewriteEventResponse, WorkerQueryContext, assert_snapshot,
        register_distributed_stage_envelope,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test]
    async fn flight_workers_keep_envelopes_outermost_while_resizing_exchange_heads() -> Result<()> {
        let executed_heads = Arc::new(Mutex::new(vec![]));
        let session_builder = {
            let executed_heads = Arc::clone(&executed_heads);
            move |ctx: WorkerQueryContext| {
                let executed_heads = Arc::clone(&executed_heads);
                async move {
                    let mut builder = ctx.builder;
                    register_distributed_stage_envelope::<TestStageEnvelope>(
                        builder.config().get_or_insert_default(),
                    );
                    Ok::<SessionState, datafusion::common::DataFusionError>(
                        builder
                            .with_distributed_worker_plan_rewrite_handler(
                                move |event: WorkerPlanRewriteEvent<'_>| {
                                    // Model a stale producer head; the consumer request must restore its actual fanout.
                                    let plan = single_consumer_head(event.plan)?;
                                    Ok(WorkerPlanRewriteEventResponse::new(Arc::new(
                                        TestStageEnvelope::new(plan, Arc::clone(&executed_heads)),
                                    )))
                                },
                            )
                            .build(),
                    )
                }
            }
        };
        let (mut ctx, _workers, _) = start_localhost_context(2, session_builder).await;
        ctx.set_distributed_metrics_collection(true)?;
        ctx.set_distributed_broadcast_joins(true)?;
        let input = input_batch()?;
        let file = register_temp_parquet_table("input", input.schema(), vec![input], &ctx).await?;
        let query = "SELECT a.bucket, sum(a.amount + b.amount) AS total FROM input a JOIN input b ON a.id = b.id GROUP BY a.bucket ORDER BY a.bucket";
        let plan = ctx.sql(query).await?.create_physical_plan().await?;

        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

        let result = pretty_format_batches(&batches)?.to_string();
        assert_snapshot!(result, @r"
        +--------+-------+
        | bucket | total |
        +--------+-------+
        | a      | 40    |
        | b      | 80    |
        +--------+-------+
        ");
        let metrics = tokio::time::timeout(
            Duration::from_secs(10),
            plan.downcast_ref::<DistributedExec>()
                .unwrap()
                .wait_for_metrics(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!metrics.is_empty());
        for task in metrics.values() {
            // Root metrics are reported first. An exchange outside the envelope fails this assertion.
            assert!(
                task.pre_order_plan_metrics[0]
                    .iter()
                    .any(|metric| metric.value().name() == "envelope_executions"
                        && metric.value().as_usize() > 0)
            );
        }
        let heads = executed_heads.lock().unwrap();
        let mut repartitions = 0;
        let mut broadcasts = 0;
        for head in heads.iter() {
            if let Some(exchange) = head.downcast_ref::<RepartitionExec>() {
                repartitions += 1;
                assert!(exchange.properties().partitioning.partition_count() > 1);
                assert!(!exchange.input().is::<RepartitionExec>());
                assert!(!exchange.input().is::<TestStageEnvelope>());
            }
            if let Some(exchange) = head.downcast_ref::<BroadcastExec>() {
                broadcasts += 1;
                assert_eq!(
                    exchange.properties().partitioning.partition_count(),
                    exchange.input().output_partitioning().partition_count() * 2
                );
                assert!(!exchange.input().is::<BroadcastExec>());
                assert!(!exchange.input().is::<TestStageEnvelope>());
            }
        }
        assert!(repartitions > 0);
        assert!(broadcasts > 0);
        std::fs::remove_file(file)?;
        Ok(())
    }

    fn single_consumer_head(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(exchange) = plan.downcast_ref::<RepartitionExec>() {
            let partitioning = match exchange.partitioning() {
                Partitioning::Hash(expressions, _) => Partitioning::Hash(expressions.clone(), 1),
                _ => Partitioning::RoundRobinBatch(1),
            };
            return Ok(Arc::new(RepartitionExec::try_new(
                Arc::clone(exchange.input()),
                partitioning,
            )?));
        }
        if let Some(exchange) = plan.downcast_ref::<BroadcastExec>() {
            return Ok(Arc::new(BroadcastExec::new(
                Arc::clone(exchange.input()),
                1,
            )));
        }
        Ok(plan)
    }

    fn input_batch() -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bucket", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 10, 20])),
            ],
        )?)
    }
}
