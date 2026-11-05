#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::common::assert_contains;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::test_utils::stage_envelope::TestStageEnvelope;
    use datafusion_distributed::{
        BroadcastExec, DefaultSessionBuilder, DistributedConfig, DistributedExec, DistributedExt,
        MappedWorkerSessionBuilderExt, SessionStateBuilderExt, WorkerPlanRewriteEvent,
        WorkerPlanRewriteEventResponse, WorkerSessionBuilder, assert_snapshot,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test]
    async fn disabled_head_rewriting_keeps_opaque_worker_wrappers_outermost() -> Result<()> {
        let executed_heads = Arc::new(Mutex::new(vec![]));
        let session_builder = envelope_workers(Arc::clone(&executed_heads));
        let (mut ctx, _workers, _) = start_localhost_context(2, session_builder).await;
        let mut config = DistributedConfig::from_session_config(&ctx.copied_config())?.clone();
        config.rewrite_producer_head_at_execution = false;
        ctx.set_distributed_option_extension(config);
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

    #[tokio::test]
    async fn default_configuration_still_inserts_worker_producer_heads() -> Result<()> {
        let executed_heads = Arc::new(Mutex::new(vec![]));
        let (ctx, _workers, _) = start_localhost_context(2, envelope_workers(executed_heads)).await;
        assert!(
            DistributedConfig::from_session_config(&ctx.copied_config())?
                .rewrite_producer_head_at_execution
        );
        let input = input_batch()?;
        let file = register_temp_parquet_table("input", input.schema(), vec![input], &ctx).await?;
        let plan = ctx
            .sql("SELECT bucket, count(*) FROM input GROUP BY bucket ORDER BY bucket")
            .await?
            .create_physical_plan()
            .await?;

        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

        let result = pretty_format_batches(&batches)?.to_string();
        assert_snapshot!(result, @r"
        +--------+----------+
        | bucket | count(*) |
        +--------+----------+
        | a      | 2        |
        | b      | 2        |
        +--------+----------+
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
        // With the default enabled, the exchange is inserted outside our opaque wrapper.
        assert!(metrics.values().any(|task| {
            !task.pre_order_plan_metrics[0]
                .iter()
                .any(|metric| metric.value().name() == "envelope_executions")
        }));
        std::fs::remove_file(file)?;
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_planning_requires_runtime_head_rewriting() -> Result<()> {
        let mut distributed = DistributedConfig::default();
        distributed.dynamic_task_count = true;
        distributed.rewrite_producer_head_at_execution = false;
        let config = SessionConfig::new().with_distributed_option_extension(distributed);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_planner()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_batch("input", input_batch()?)?;

        let error = ctx
            .sql("SELECT bucket, count(*) FROM input GROUP BY bucket")
            .await?
            .create_physical_plan()
            .await
            .unwrap_err();

        assert_contains!(
            error.to_string(),
            "rewrite_producer_head_at_execution=false requires dynamic_task_count=false"
        );
        Ok(())
    }

    #[test]
    fn execution_rejects_incompatible_options_before_preparation() {
        let mut distributed = DistributedConfig::default();
        distributed.dynamic_task_count = true;
        distributed.rewrite_producer_head_at_execution = false;
        let config = SessionConfig::new().with_distributed_option_extension(distributed);
        let ctx = SessionContext::new_with_config(config);
        let plan = DistributedExec::new(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));

        let error = match plan.execute(0, ctx.task_ctx()) {
            Ok(_) => panic!("Invalid execution options must fail before preparation"),
            Err(error) => error,
        };

        assert_contains!(
            error.to_string(),
            "rewrite_producer_head_at_execution=false requires dynamic_task_count=false"
        );
    }

    fn envelope_workers(
        executed_heads: Arc<Mutex<Vec<Arc<dyn ExecutionPlan>>>>,
    ) -> impl WorkerSessionBuilder + Clone {
        DefaultSessionBuilder.map(move |builder| {
            let executed_heads = Arc::clone(&executed_heads);
            Ok(builder
                .with_distributed_worker_plan_rewrite_handler(
                    move |event: WorkerPlanRewriteEvent<'_>| {
                        Ok(WorkerPlanRewriteEventResponse::new(Arc::new(
                            TestStageEnvelope::new(event.plan, Arc::clone(&executed_heads)),
                        )))
                    },
                )
                .build())
        })
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
