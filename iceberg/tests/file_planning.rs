#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{Result, stats::Precision};
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{
        collect,
        statistics::{StatisticsArgs, StatisticsContext},
    };
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::{IcebergConfig, test_utils::IcebergTestHarness};

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
