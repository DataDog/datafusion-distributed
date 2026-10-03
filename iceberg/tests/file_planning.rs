#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{Result, stats::Precision};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{
        collect,
        statistics::{StatisticsArgs, StatisticsContext},
    };
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DistributedExt, DistributedTaskContext, WorkUnitFeedProvider};
    use datafusion_distributed_iceberg::{
        IcebergConfig, IcebergDataSource, test_utils::IcebergTestHarness,
    };

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn physical_planning_caps_scan_tasks_to_pruned_files() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_workers(10)
            .configure_session(|mut state| {
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .execution
                    .target_partitions = 16;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let (plan, _) = harness.query(
            "SELECT pickup_date, COUNT(*) FROM taxi WHERE pickup_date <= DATE '2024-01-11' GROUP BY pickup_date"
        ).await?;
        assert!(plan.contains("planned_files=4"), "{plan}");
        assert!(plan.contains("tasks=4"), "{plan}");
        assert!(
            !plan.contains("tasks=7") && !plan.contains("tasks=10"),
            "{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pruned_file_statistics_are_not_exact_filtered_row_counts() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT * FROM taxi WHERE pickup_date = DATE '2024-01-10'")
            .await?;
        let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
        assert!(matches!(stats.num_rows, Precision::Inexact(rows) if rows <= 25_000));
        Ok(())
    }

    #[tokio::test]
    async fn planned_work_can_be_executed_twice_without_consuming_it() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT pickup_date FROM taxi WHERE pickup_date = DATE '2024-01-10'")
            .await?;
        let first = collect(Arc::clone(&plan), Arc::new(TaskContext::default())).await?;
        let second = collect(plan, Arc::new(TaskContext::default())).await?;
        assert_eq!(
            pretty_format_batches(&first)?.to_string(),
            pretty_format_batches(&second)?.to_string()
        );
        assert_eq!(
            first.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            25_000
        );
        Ok(())
    }

    #[tokio::test]
    async fn file_limit_fails_in_planning_instead_of_returning_partial_results() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.planning_max_files = 2;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state)
            })?
            .build()
            .await?;
        let error = harness
            .physical_plan("SELECT * FROM taxi")
            .await
            .expect_err("seven files exceed the limit");
        assert!(
            error.to_string().contains("planning_max_files=2"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn planned_files_respect_the_query_memory_pool() -> Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(1));
        let runtime = Arc::new(RuntimeEnvBuilder::new().with_memory_pool(pool).build()?);
        let harness = IcebergTestHarness::builder()
            .configure_session(|state| Ok(state.with_runtime_env(runtime)))?
            .build()
            .await?;
        let error = harness
            .physical_plan("SELECT * FROM taxi")
            .await
            .expect_err("file metadata cannot fit in one byte");
        assert!(
            error.to_string().contains("Iceberg file planning"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn greedy_assignment_reserves_and_releases_scratch_after_file_discovery() -> Result<()> {
        const CAPACITY: usize = 1024 * 1024;
        // Seven files / one task fit the retained-only reservation, but not construction.
        const HEADROOM: usize = 192;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(CAPACITY));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        );
        let mut iceberg = IcebergConfig::default();
        iceberg.greedy_file_assignment = true;
        iceberg.file_task_affinity = true;
        let mut config = SessionConfig::new();
        config.options_mut().extensions.insert(iceberg);
        let harness = IcebergTestHarness::builder()
            .configure_session(|state| {
                Ok(state
                    .with_config(config.clone())
                    .with_runtime_env(Arc::clone(&runtime)))
            })?
            .build()
            .await?;
        let plan = harness.scan().await?; // File discovery succeeds before restricting headroom.
        let discovered = pool.reserved();
        assert!(discovered > 0);
        let source = plan.downcast_ref::<DataSourceExec>().unwrap();
        let provider = source
            .data_source()
            .downcast_ref::<IcebergDataSource>()
            .unwrap()
            .feed()
            .inner()
            .unwrap();
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(config)
                .with_runtime(runtime),
        );
        let task = DistributedTaskContext {
            task_index: 0,
            task_count: 1,
        };
        let occupied = MemoryConsumer::new("test headroom").register(&pool);
        occupied.try_grow(CAPACITY - discovered - HEADROOM)?;
        let before = pool.reserved();
        let error = provider
            .task_affinity(task, Arc::clone(&ctx))
            .expect_err("scratch must also fit");
        assert!(
            error
                .to_string()
                .contains("Iceberg file assignment scratch"),
            "{error}"
        );
        assert_eq!(
            pool.reserved(),
            before,
            "failure must release partial reservations"
        );

        drop(occupied);
        let hints = provider.task_affinity(task, Arc::clone(&ctx))?;
        assert_eq!(hints.len(), 7);
        let retained = pool.reserved();
        assert!(retained > discovered);
        assert!(
            retained - discovered <= HEADROOM,
            "scratch must be released after construction"
        );
        assert_eq!(hints, provider.task_affinity(task, ctx)?);
        assert_eq!(
            pool.reserved(),
            retained,
            "cached assignments must not reserve again"
        );
        drop(plan);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn pruning_all_files_produces_an_empty_result() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, result) = harness
            .query("SELECT count(*) FROM taxi WHERE pickup_date = DATE '1900-01-01'")
            .await?;
        assert!(!plan.contains("Rows=Exact(175000)"));
        assert!(result.contains("| 0"), "{result}");
        Ok(())
    }
}
