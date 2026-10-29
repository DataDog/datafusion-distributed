#[cfg(all(test, feature = "integration"))]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{Result, exec_datafusion_err, exec_err};
    use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
    use datafusion::object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::materialized::{
        IntermediateResultStore, MaterializedExecution, MaterializedStore,
        ObjectStoreIntermediateStore, RecoveryAction, ReplayPolicy,
    };
    use datafusion_distributed::test_utils::in_memory_channel_resolver::{
        InMemoryChannelResolver, InMemoryWorkerResolver,
    };
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::test_utils::test_work_unit_feed::{
        RowGeneratorExec, TestWorkUnitFeedExecCodec, TestWorkUnitFeedFunction,
        row_generator_desired_task_count_handler, row_generator_scale_up_leaf_node_handler,
    };
    use datafusion_distributed::{
        DistributedExt, NetworkBroadcastExec, SessionStateBuilderExt, WorkerQueryContext,
        assert_snapshot,
    };
    use futures::TryStreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use uuid::Uuid;

    #[tokio::test]
    async fn materialized_shuffle_aggregation() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(MaterializedExecution::new(Arc::new(
                        FixturePolicy(RecoveryAction::Fail),
                    )))),
            )?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let batches = ctx.sql("SELECT value % 2 AS parity, SUM(value) AS total FROM numbers GROUP BY value % 2 ORDER BY parity").await?.collect().await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 90    |
        | 1      | 100   |
        +--------+-------+
        ");
        assert!(store.puts.load(Ordering::SeqCst) > 1);
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn retries_partial_producer_without_publishing_rows() -> Result<()> {
        let store = Arc::new(FaultStore::new(2, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(2, 0, 2)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let batches = ctx
            .sql("SELECT value % 2 AS parity, SUM(value) AS total FROM numbers GROUP BY value % 2 ORDER BY parity")
            .await?
            .collect()
            .await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 90    |
        | 1      | 100   |
        +--------+-------+
        ");
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn retries_consumer_with_fresh_aggregate_state() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 2));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(2, 0, 2)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let batches = ctx
            .sql("SELECT value % 2 AS parity, SUM(value) AS total FROM numbers GROUP BY value % 2 ORDER BY parity")
            .await?
            .collect()
            .await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 90    |
        | 1      | 100   |
        +--------+-------+
        ");
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn rebuilds_stages_before_exposing_final_results() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 2).with_lost_input());
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RebuildStages,
                        )))
                        .with_retry_limits(0, 1, 1)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let batches = ctx
            .sql("SELECT value % 2 AS parity, SUM(value) AS total FROM numbers GROUP BY value % 2 ORDER BY parity")
            .await?
            .collect()
            .await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 90    |
        | 1      | 100   |
        +--------+-------+
        ");
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn exhausted_retry_budget_returns_error_without_rows() -> Result<()> {
        let store = Arc::new(FaultStore::new(1, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(0, 0, 0),
                    )),
            )?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let plan = ctx
            .sql("SELECT value FROM numbers ORDER BY value")
            .await?
            .create_physical_plan()
            .await?;
        let mut stream = execute_stream(plan, ctx.task_ctx())?;
        assert!(stream.try_next().await.is_err());
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn sealed_work_units_are_not_enumerated_again_on_retry() -> Result<()> {
        // Four journal writes, then fail the second data write.
        let store = Arc::new(FaultStore::new(6, 0));
        let mut ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                2,
                SessionConfig::new()
                    .with_target_partitions(2)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(2, 0, 2)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        ctx.set_distributed_work_unit_feed(|plan: &RowGeneratorExec| Some(&plan.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_desired_task_count_handler(row_generator_desired_task_count_handler);
        ctx.set_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));
        let plan = ctx.sql("SELECT task, COUNT(*) AS count FROM test_work_unit('source', 2, 'rows(1)', 'rows(2)', 'rows(3)', 'rows(4)') GROUP BY task ORDER BY task")
            .await?.create_physical_plan().await?;
        let mut sources = Vec::new();
        plan.apply(|node| {
            if node.downcast_ref::<RowGeneratorExec>().is_some() {
                sources.push(Arc::clone(node));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +------+-------+
        | task | count |
        +------+-------+
        | 0    | 3     |
        | 1    | 7     |
        +------+-------+
        ");
        let units: usize = sources
            .iter()
            .filter_map(|source| source.metrics())
            .filter_map(|metrics| metrics.sum_by_name("work_units_sent"))
            .map(|value| value.as_usize())
            .sum();
        assert_eq!(units, 4);
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn local_query_also_materializes_before_delivery() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                1,
                SessionConfig::new()
                    .with_target_partitions(1)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(MaterializedExecution::new(Arc::new(
                        FixturePolicy(RecoveryAction::Fail),
                    )))),
            )?
            .build(),
        );
        let batches = ctx.sql("SELECT 42 AS answer").await?.collect().await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+
        | answer |
        +--------+
        | 42     |
        +--------+
        ");
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn final_delivery_failure_never_reexecutes_the_query() -> Result<()> {
        // A local-only query writes one final segment; its first read is delivery.
        let store = Arc::new(FaultStore::new(0, 1));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                1,
                SessionConfig::new()
                    .with_target_partitions(1)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(3, 0, 3)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        let plan = ctx
            .sql("SELECT 42 AS answer")
            .await?
            .create_physical_plan()
            .await?;
        let mut stream = execute_stream(plan, ctx.task_ctx())?;
        assert!(stream.try_next().await.is_err());
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn materialized_two_phase_shuffle_aggregation() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(MaterializedExecution::new(Arc::new(
                        FixturePolicy(RecoveryAction::Fail),
                    )))),
            )?
            .with_distributed_two_step_shuffle_fanout_threshold(1)?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let batches = ctx.sql("SELECT value % 2 AS parity, SUM(value) AS total FROM numbers GROUP BY value % 2 ORDER BY parity").await?.collect().await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 90    |
        | 1      | 100   |
        +--------+-------+
        ");
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn materialized_broadcast_has_independent_readers() -> Result<()> {
        let store = Arc::new(FaultStore::new(0, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                3,
                SessionConfig::new()
                    .with_target_partitions(3)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(MaterializedExecution::new(Arc::new(
                        FixturePolicy(RecoveryAction::Fail),
                    )))),
            )?
            .with_distributed_broadcast_joins(true)?
            .build(),
        );
        let file = register_numbers(&ctx).await?;
        let plan = ctx.sql("SELECT n.value % 2 AS parity, SUM(n.value + d.delta) AS total FROM numbers n CROSS JOIN (VALUES (10), (20)) AS d(delta) GROUP BY n.value % 2 ORDER BY parity")
            .await?.create_physical_plan().await?;
        let mut broadcasts = 0;
        plan.apply(|node| {
            if node.downcast_ref::<NetworkBroadcastExec>().is_some() {
                broadcasts += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(broadcasts > 0);
        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +--------+-------+
        | parity | total |
        +--------+-------+
        | 0      | 480   |
        | 1      | 500   |
        +--------+-------+
        ");
        tokio::fs::remove_file(file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn query_deadline_bounds_retry_delays() -> Result<()> {
        let store = Arc::new(FaultStore::new(1, 0));
        let ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                1,
                SessionConfig::new()
                    .with_target_partitions(1)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_retry_limits(3, 0, 3)
                        .with_retry_delay(Duration::from_secs(60))
                        .with_timeout(Duration::from_secs(1)),
                    )),
            )?
            .build(),
        );
        let error = ctx
            .sql("SELECT 42 AS answer")
            .await?
            .collect()
            .await
            .unwrap_err();
        assert_snapshot!(error.strip_backtrace(), @"Execution error: Materialized query deadline exceeded");
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn partially_sealed_feed_is_not_reenumerated() -> Result<()> {
        let store = Arc::new(FaultStore::new(2, 0));
        let mut ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                2,
                SessionConfig::new()
                    .with_target_partitions(2)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RebuildStages,
                        )))
                        .with_retry_limits(3, 3, 6)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        ctx.set_distributed_work_unit_feed(|plan: &RowGeneratorExec| Some(&plan.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_desired_task_count_handler(row_generator_desired_task_count_handler);
        ctx.set_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));
        let error = ctx.sql("SELECT task, COUNT(*) FROM test_work_unit('source', 2, 'rows(1)', 'rows(2)', 'rows(3)', 'rows(4)') GROUP BY task")
            .await?.collect().await.unwrap_err();
        assert_snapshot!(error.strip_backtrace(), @"Execution error: Injected interrupted output write");
        assert_eq!(store.puts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn shared_store_rejects_overwrites_and_isolates_query_cleanup() -> Result<()> {
        let store =
            ObjectStoreIntermediateStore::new(Arc::new(InMemory::new()), Path::from("recovery"));
        let first_query = Uuid::new_v4();
        let second_query = Uuid::new_v4();
        let first_key = format!("{first_query}/segment");
        let second_key = format!("{second_query}/segment");
        store.put(&first_key, Bytes::from_static(b"first")).await?;
        store
            .put(&second_key, Bytes::from_static(b"second"))
            .await?;
        assert!(
            store
                .put(&first_key, Bytes::from_static(b"replacement"))
                .await
                .is_err()
        );
        assert_eq!(store.get(&first_key).await?, Bytes::from_static(b"first"));
        store.remove_query(first_query).await?;
        store.remove_query(first_query).await?;
        assert!(store.get(&first_key).await.is_err());
        assert_eq!(store.get(&second_key).await?, Bytes::from_static(b"second"));
        Ok(())
    }

    #[tokio::test]
    async fn bounded_tasks_overlap_and_retry_without_restarting_successful_siblings() -> Result<()>
    {
        // Four one-partition producers, one failed attempt, then an ungated root write.
        let (store, mut writes) = FaultStore::new(0, 0).with_blocked_writes(5);
        let store = Arc::new(store);
        let mut ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                4,
                SessionConfig::new()
                    .with_target_partitions(1)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_max_concurrent_tasks(2)?
                        .with_retry_limits(3, 0, 2)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        ctx.set_distributed_work_unit_feed(|plan: &RowGeneratorExec| Some(&plan.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_desired_task_count_handler(row_generator_desired_task_count_handler);
        ctx.set_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));
        let plan = ctx.sql("SELECT task FROM test_work_unit('source', 4, 'rows(1)', 'rows(1)', 'rows(1)', 'rows(1)') ORDER BY task")
            .await?.create_physical_plan().await?;
        let stream = execute_stream(plan, ctx.task_ctx())?;
        let result = SpawnedTask::spawn(async move { stream.try_collect::<Vec<_>>().await });

        let first = next_write(&mut writes).await;
        let second = next_write(&mut writes).await;
        assert_ne!(first.key, second.key);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), writes.recv())
                .await
                .is_err()
        );
        // Keep the first producer blocked while later producers finish and retry.
        second.release.send(Ok(())).unwrap();
        let third = next_write(&mut writes).await;
        third
            .release
            .send(exec_err!("Retry this producer"))
            .unwrap();
        let retry = next_write(&mut writes).await;
        assert_ne!(retry.key, third.key);
        retry.release.send(Ok(())).unwrap();
        let fourth = next_write(&mut writes).await;
        fourth.release.send(Ok(())).unwrap();
        // No consumer may read intermediate data before the slow producer commits.
        assert_eq!(store.data_reads.load(Ordering::SeqCst), 0);
        first.release.send(Ok(())).unwrap();
        let batches = tokio::time::timeout(Duration::from_secs(10), result.join_unwind())
            .await
            .expect("query must finish after releasing producers")
            .expect("query task must not panic")?;
        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @r"
        +------+
        | task |
        +------+
        | 0    |
        | 1    |
        | 2    |
        | 3    |
        +------+
        ");
        assert_eq!(store.data_writes.load(Ordering::SeqCst), 6);
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_failures_share_retry_budget_and_cancel_unfinished_attempts() -> Result<()> {
        let (store, mut writes) = FaultStore::new(0, 0).with_blocked_writes(10);
        let store = Arc::new(store);
        let mut ctx = SessionContext::from(
            session_builder(
                Arc::clone(&store),
                4,
                SessionConfig::new()
                    .with_target_partitions(1)
                    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
                    .with_extension(Arc::new(MaterializedStore::new(store.clone())))
                    .with_extension(Arc::new(
                        MaterializedExecution::new(Arc::new(FixturePolicy(
                            RecoveryAction::RetryTask,
                        )))
                        .with_max_concurrent_tasks(2)?
                        .with_retry_limits(3, 0, 1)
                        .with_retry_delay(Duration::ZERO),
                    )),
            )?
            .build(),
        );
        ctx.set_distributed_work_unit_feed(|plan: &RowGeneratorExec| Some(&plan.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_desired_task_count_handler(row_generator_desired_task_count_handler);
        ctx.set_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));
        let plan = ctx.sql("SELECT task FROM test_work_unit('source', 4, 'rows(1)', 'rows(1)', 'rows(1)', 'rows(1)') ORDER BY task")
            .await?.create_physical_plan().await?;
        let stream = execute_stream(plan, ctx.task_ctx())?;
        let result = SpawnedTask::spawn(async move { stream.try_collect::<Vec<_>>().await });

        let first = next_write(&mut writes).await;
        let second = next_write(&mut writes).await;
        first
            .release
            .send(exec_err!("First producer failed"))
            .unwrap();
        let mut retry = next_write(&mut writes).await;
        // The first producer consumed the only query-wide retry. A sibling's
        // failure must terminate the stage and cancel the blocked retry.
        second
            .release
            .send(exec_err!("Second producer failed"))
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), result.join_unwind())
            .await
            .expect("exhausted budget must terminate the query")
            .expect("query task must not panic")
            .unwrap_err();
        assert_snapshot!(error.strip_backtrace(), @"Execution error: Second producer failed");
        tokio::time::timeout(Duration::from_secs(10), retry.release.closed())
            .await
            .expect("unfinished retry must be cancelled");
        assert_eq!(store.data_writes.load(Ordering::SeqCst), 3);
        assert_eq!(store.data_reads.load(Ordering::SeqCst), 0);
        Ok(())
    }

    // This policy is scoped to immutable local test fixtures and built-in SQL
    // operators. Production applications must validate their own snapshots/hooks.
    #[derive(Debug)]
    struct FixturePolicy(RecoveryAction);
    impl ReplayPolicy for FixturePolicy {
        fn validate(&self, _: &Arc<dyn ExecutionPlan>, _: &TaskContext) -> Result<()> {
            Ok(())
        }
        fn on_error(&self, _: &datafusion::common::DataFusionError) -> RecoveryAction {
            self.0
        }
    }

    fn session_builder(
        store: Arc<FaultStore>,
        workers: usize,
        config: SessionConfig,
    ) -> Result<SessionStateBuilder> {
        let channels = InMemoryChannelResolver::from_session_builder(move |query| {
            worker_session(query, Arc::clone(&store))
        });
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_planner()
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(workers))
            .with_distributed_channel_resolver(channels)
            .with_distributed_file_scan_config_bytes_per_partition(1)
    }

    async fn worker_session(
        mut query: WorkerQueryContext,
        store: Arc<FaultStore>,
    ) -> Result<SessionState> {
        let config = query.builder.config().get_or_insert_default();
        config
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = false;
        config.set_extension(Arc::new(MaterializedStore::new(store)));
        Ok(query
            .builder
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
            .build())
    }

    async fn register_numbers(ctx: &SessionContext) -> Result<std::path::PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )?;
        register_temp_parquet_table("numbers", schema, vec![batch], ctx).await
    }

    #[derive(Debug)]
    struct BlockedWrite {
        key: String,
        release: oneshot::Sender<Result<()>>,
    }

    async fn next_write(writes: &mut mpsc::UnboundedReceiver<BlockedWrite>) -> BlockedWrite {
        tokio::time::timeout(Duration::from_secs(10), writes.recv())
            .await
            .expect("producer must reach the store while its sibling is blocked")
            .expect("write observer must remain open")
    }

    #[derive(Debug)]
    struct FaultStore {
        inner: ObjectStoreIntermediateStore,
        objects: Arc<InMemory>,
        lose_input: bool,
        fail_put: usize,
        fail_get: usize,
        puts: AtomicUsize,
        gets: AtomicUsize,
        failures: AtomicUsize,
        data_writes: AtomicUsize,
        data_reads: AtomicUsize,
        blocked_writes: Option<(usize, mpsc::UnboundedSender<BlockedWrite>)>,
    }
    impl FaultStore {
        fn new(fail_put: usize, fail_get: usize) -> Self {
            let objects = Arc::new(InMemory::new());
            Self {
                objects: Arc::clone(&objects),
                lose_input: false,
                inner: ObjectStoreIntermediateStore::new(objects, Path::from("recovery")),
                fail_put,
                fail_get,
                puts: AtomicUsize::new(0),
                gets: AtomicUsize::new(0),
                failures: AtomicUsize::new(0),
                data_writes: AtomicUsize::new(0),
                data_reads: AtomicUsize::new(0),
                blocked_writes: None,
            }
        }
        fn with_blocked_writes(
            mut self,
            count: usize,
        ) -> (Self, mpsc::UnboundedReceiver<BlockedWrite>) {
            let (tx, rx) = mpsc::unbounded_channel();
            self.blocked_writes = Some((count, tx));
            (self, rx)
        }
        fn with_lost_input(mut self) -> Self {
            self.lose_input = true;
            self
        }
    }
    #[async_trait]
    impl IntermediateResultStore for FaultStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
            if self.puts.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_put {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return exec_err!("Injected interrupted output write");
            }
            if key.ends_with(".arrow") {
                let write = self.data_writes.fetch_add(1, Ordering::SeqCst) + 1;
                if let Some((count, tx)) = &self.blocked_writes
                    && write <= *count
                {
                    let (release, wait) = oneshot::channel();
                    tx.send(BlockedWrite {
                        key: key.to_owned(),
                        release,
                    })
                    .map_err(|_| exec_datafusion_err!("Write observer closed"))?;
                    if let Err(error) = wait
                        .await
                        .map_err(|_| exec_datafusion_err!("Write gate closed"))?
                    {
                        self.failures.fetch_add(1, Ordering::SeqCst);
                        return Err(error);
                    }
                }
            }
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes> {
            if key.ends_with(".arrow") {
                self.data_reads.fetch_add(1, Ordering::SeqCst);
            }
            if self.gets.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_get {
                self.failures.fetch_add(1, Ordering::SeqCst);
                if self.lose_input {
                    self.objects
                        .delete(&Path::parse(format!("recovery/{key}"))?)
                        .await?;
                }
                return exec_err!("Injected interrupted input read");
            }
            self.inner.get(key).await
        }
        async fn remove_query(&self, query: Uuid) -> Result<()> {
            self.inner.remove_query(query).await
        }
    }
}
