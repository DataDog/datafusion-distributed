mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Result, exec_datafusion_err};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::{FeedTaskSource, IcebergExt, IcebergIntegrationOptions};
    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, IcebergTestHarness, create_taxi_table,
    };
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg_datafusion::read::EagerIcebergDataSource;
    use iceberg_datafusion::read::IcebergDataSource;
    use iceberg_datafusion::{
        IcebergCatalogTableProvider, IcebergDataFusionConfig, IcebergStaticTableProvider,
        to_datafusion_error,
    };
    use tokio::runtime::Builder as RuntimeBuilder;

    #[tokio::test]
    async fn ordinary_static_provider_uses_distributed_lazy_tasks() -> Result<()> {
        let table = create_taxi_table().await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_workers(2)
            .with_config_option("iceberg.execution.eager_file_planning", false)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let provider =
            IcebergStaticTableProvider::try_new_from_table(table.table().clone()).await?;
        let scan = provider.scan(&harness.state(), None, &[], None).await?;
        assert!(
            scan.downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .is::<IcebergDataSource<FeedTaskSource>>()
        );
        harness
            .context()
            .register_table("taxi", Arc::new(provider))?;
        let (_, rows) = harness
            .query("SELECT MAX(vendor_id) AS vendor FROM taxi")
            .await?;
        insta::assert_snapshot!(rows, @"
        +--------+
        | vendor |
        +--------+
        | 2      |
        +--------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_catalog_provider_refreshes_before_distributed_eager_scan() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = id_table(&fixtures).await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_workers(2)
            .with_config_option("iceberg.execution.eager_file_planning", true)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let provider = IcebergCatalogTableProvider::try_new(
            fixtures.catalog(),
            table.ident().namespace.clone(),
            table.ident().name(),
        )
        .await?;
        harness
            .context()
            .register_table("items", Arc::new(provider))?;
        // The provider was created while the table had no snapshot.
        let batch =
            RecordBatch::try_new(table.schema()?, vec![Arc::new(Int64Array::from(vec![42]))])?;
        table.append().file(batch).commit().await?;
        let provider = harness.context().table_provider("items").await?;
        let scan = provider.scan(&harness.state(), None, &[], None).await?;
        assert!(
            scan.downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .is::<EagerIcebergDataSource>()
        );
        let (_, rows) = harness.query("SELECT id FROM items").await?;
        insta::assert_snapshot!(rows, @"
        +----+
        | id |
        +----+
        | 42 |
        +----+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn integration_preserves_existing_upstream_eager_selection() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|mut state| {
                let mut planning = IcebergDataFusionConfig::default();
                planning.execution.eager_file_planning = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(planning);
                state.with_iceberg_integration(Default::default())
            })
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let scan = harness.scan().await?;
        assert!(
            scan.downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .is::<EagerIcebergDataSource>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn upstream_options_can_be_changed_after_registering_the_integration() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| {
                let mut state = state.with_iceberg_integration(Default::default());
                let mut config = IcebergDataFusionConfig::default();
                config.execution.eager_file_planning = true;
                config.execution.data_file_concurrency_limit = 2;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                state
            })
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let scan = harness.scan().await?;
        assert!(
            scan.downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .is::<EagerIcebergDataSource>()
        );
        assert_eq!(
            harness
                .state()
                .config_options()
                .extensions
                .get::<IcebergDataFusionConfig>()
                .unwrap()
                .execution
                .data_file_concurrency_limit,
            2
        );
        let batches = harness
            .context()
            .sql("SELECT vendor_id FROM taxi LIMIT 19")
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            19
        );
        Ok(())
    }

    #[tokio::test]
    async fn static_distributed_writes_fail_without_changing_the_destination() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let table = id_table(&fixtures).await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_workers(2)
            .with_config_option("distributed.dynamic_task_count", false)
            .build()
            .await?;
        let provider = IcebergCatalogTableProvider::try_new(
            fixtures.catalog(),
            table.ident().namespace.clone(),
            table.ident().name(),
        )
        .await?;
        harness
            .context()
            .register_table("items", Arc::new(provider))?;
        let before = destination_state(&fixtures, &table).await?;
        let error = harness
            .query("INSERT INTO items VALUES (1), (2)")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Distributed Iceberg writes are not supported"),
            "{error}"
        );
        assert_eq!(destination_state(&fixtures, &table).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_distributed_writes_fail_without_changing_the_destination() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let table = id_table(&fixtures).await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_workers(2)
            .with_config_option("distributed.dynamic_task_count", true)
            .build()
            .await?;
        let provider = IcebergCatalogTableProvider::try_new(
            fixtures.catalog(),
            table.ident().namespace.clone(),
            table.ident().name(),
        )
        .await?;
        harness
            .context()
            .register_table("items", Arc::new(provider))?;
        let before = destination_state(&fixtures, &table).await?;
        let error = harness
            .query("INSERT INTO items VALUES (1), (2)")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Distributed Iceberg writes are not supported"),
            "{error}"
        );
        assert_eq!(destination_state(&fixtures, &table).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn catalog_writes_remain_available_without_distributed_planning() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let table = id_table(&fixtures).await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .build()
            .await?;
        let provider = IcebergCatalogTableProvider::try_new(
            fixtures.catalog(),
            table.ident().namespace.clone(),
            table.ident().name(),
        )
        .await?;
        harness
            .context()
            .register_table("items", Arc::new(provider))?;
        let before = destination_state(&fixtures, &table).await?;
        harness.query("INSERT INTO items VALUES (1), (2)").await?;
        let after = destination_state(&fixtures, &table).await?;
        assert_ne!(after.0, before.0);
        assert!(!after.1.is_empty());
        let (_, rows) = harness.query("SELECT id FROM items ORDER BY id").await?;
        insta::assert_snapshot!(rows, @"
        +----+
        | id |
        +----+
        | 1  |
        | 2  |
        +----+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn session_context_installation_registers_the_upstream_scan_hook() -> Result<()> {
        let harness = IcebergTestHarness::builder().build().await?;
        let mut context = harness.context().clone();
        context.set_iceberg_integration(Default::default());
        let table = create_taxi_table().await?;
        let provider =
            IcebergStaticTableProvider::try_new_from_table(table.table().clone()).await?;
        let plan = provider.scan(&context.state(), None, &[], None).await?;
        assert!(
            plan.downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .is::<IcebergDataSource<FeedTaskSource>>()
        );
        Ok(())
    }

    #[test]
    fn worker_readers_do_not_use_the_coordinator_provider_runtime() -> Result<()> {
        let caller = RuntimeBuilder::new_current_thread().enable_all().build()?;
        // Kept alive but never driven: work incorrectly scheduled here cannot finish.
        let coordinator = RuntimeBuilder::new_current_thread().enable_all().build()?;
        let workers = RuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        caller.block_on(async {
            let worker_runtime = iceberg::Runtime::new(&workers);
            let table = create_taxi_table().await?;
            let harness = IcebergTestHarness::builder()
                .with_session_extension(move |state| {
                    state.with_iceberg_integration(IcebergIntegrationOptions {
                        iceberg_runtime: worker_runtime.clone(),
                        ..Default::default()
                    })
                })
                .with_workers(2)
                .with_config_option("iceberg.execution.eager_file_planning", false)
                .configure_session(|state| {
                    state.with_distributed_file_scan_config_bytes_per_partition(1)
                })?
                .build()
                .await?;
            let provider = IcebergStaticTableProvider::try_new_from_table(table.table().clone())
                .await?
                .with_runtime(iceberg::Runtime::new(&coordinator));
            harness
                .context()
                .register_table("taxi", Arc::new(provider))?;
            let (_, rows) = tokio::time::timeout(
                Duration::from_secs(10),
                harness.query("SELECT MAX(vendor_id) AS vendor FROM taxi"),
            )
            .await
            .map_err(|error| {
                exec_datafusion_err!("worker reader used an unavailable runtime: {error}")
            })??;
            insta::assert_snapshot!(rows, @"
            +--------+
            | vendor |
            +--------+
            | 2      |
            +--------+
            ");
            Ok(())
        })
    }

    /// An empty table with one required long column named `id`.
    async fn id_table(fixtures: &FixtureCatalog) -> Result<FixtureTable> {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .map_err(to_datafusion_error)?;
        let ident = TableIdent::from_strs(["default", "items"]).map_err(to_datafusion_error)?;
        fixtures.table(ident, schema).create().await
    }

    async fn destination_state(
        fixtures: &FixtureCatalog,
        table: &FixtureTable,
    ) -> Result<(Option<i64>, Vec<PathBuf>)> {
        let fresh = fixtures
            .catalog()
            .load_table(table.ident())
            .await
            .map_err(to_datafusion_error)?;
        let path = fresh.metadata().location().strip_prefix("file://").unwrap();
        let mut files = Vec::new();
        parquet_files(&Path::new(path).join("data"), &mut files)?;
        files.sort();
        Ok((fresh.metadata().current_snapshot_id(), files))
    }

    fn parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        if path.exists() {
            for entry in std::fs::read_dir(path)? {
                let path = entry?.path();
                if path.is_dir() {
                    parquet_files(&path, files)?;
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
                {
                    files.push(path);
                }
            }
        }
        Ok(())
    }
}
