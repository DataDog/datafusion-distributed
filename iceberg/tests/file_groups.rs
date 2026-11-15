mod tests {
    use std::sync::Arc;

    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, data_file_name};

    use datafusion_distributed_iceberg::IcebergExt;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;
    use iceberg::arrow::ArrowReaderBuilder;
    use iceberg_datafusion::read::{EagerIcebergDataSource, IcebergDataSourceBuilder};

    #[tokio::test]
    async fn regrouping_a_subset_reads_the_selected_dates() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let files: Vec<_> = source.planned_files().cloned().collect();
        let subset = source.with_planned_files(vec![
            vec![files[1].clone(), files[4].clone()],
            vec![files[5].clone()],
        ])?;
        let files: Vec<_> = subset.planned_files().cloned().collect();
        let regrouped = subset.with_planned_files(vec![
            vec![files[0].clone(), files[2].clone()],
            vec![],
            vec![files[1].clone()],
        ])?;
        insta::assert_snapshot!(execute_groups(regrouped).await?);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_zero_groups_but_accepts_empty_partitions() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        assert!(
            source.with_planned_files(vec![]).is_err(),
            "no partition groups"
        );
        let empty = source.with_planned_files(vec![vec![]])?;
        assert_eq!(empty.planned_files().len(), 0);
        assert_eq!(empty.output_partitioning().partition_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reconstruction_preserves_groups_and_reads_the_selected_dates() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let files: Vec<_> = source.planned_files().cloned().collect();
        let selected = source.with_planned_files(vec![
            vec![files[5].clone(), files[1].clone()],
            vec![files[4].clone()],
        ])?;
        let groups = (0..2)
            .map(|p| Ok(selected.planned_partition_files(p)?.cloned().collect()))
            .collect::<Result<Vec<_>>>()?;
        let context = TaskContext::default();
        let restored = IcebergDataSourceBuilder::from_planned_files(
            Arc::clone(source.schema()),
            groups,
            source.file_io().clone(),
            ArrowReaderBuilder::new(source.file_io().clone(), iceberg::Runtime::current()).build(),
        )
        .build_planned(&context)?;
        insta::assert_snapshot!(execute_groups(restored).await?);
        Ok(())
    }

    #[tokio::test]
    async fn replacement_assignments_have_independent_reservations() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let context = TaskContext::default();
        let retained = IcebergDataSourceBuilder::from_planned_files(
            Arc::clone(source.schema()),
            vec![source.planned_files().cloned().collect()],
            source.file_io().clone(),
            ArrowReaderBuilder::new(source.file_io().clone(), iceberg::Runtime::current()).build(),
        )
        .build_planned(&context)?;
        let reserved = context.memory_pool().reserved();
        assert!(reserved > 0);
        let subset = retained
            .with_planned_files(vec![vec![retained.planned_files().next().unwrap().clone()]])?;
        let subset_reserved = context.memory_pool().reserved() - reserved;
        assert!(subset_reserved > 0);
        drop(retained);
        assert_eq!(context.memory_pool().reserved(), subset_reserved);
        drop(subset);
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    /// Execute the supplied source and show actual dates/counts alongside its groups.
    async fn execute_groups(source: EagerIcebergDataSource) -> Result<String> {
        let groups = describe_groups(&source)?;
        let ctx = SessionContext::new();
        let batches = collect(DataSourceExec::from_data_source(source), ctx.task_ctx()).await?;
        ctx.register_table("selected", ctx.read_batches(batches)?.into_view())?;
        let counts = ctx.sql(
            "SELECT pickup_date, COUNT(*) AS rows FROM selected GROUP BY pickup_date ORDER BY pickup_date"
        ).await?.collect().await?;
        Ok(format!("{groups}\n\n{}", pretty_format_batches(&counts)?))
    }

    fn describe_groups(source: &EagerIcebergDataSource) -> Result<String> {
        let mut lines = Vec::new();
        for partition in 0..source.output_partitioning().partition_count() {
            let files = source
                .planned_partition_files(partition)?
                .map(|file| data_file_name(file.data_file_path()).to_owned())
                .collect::<Vec<_>>();
            lines.push(format!("Partition {partition}: [{}]", files.join(", ")));
        }
        Ok(lines.join("\n"))
    }
}
