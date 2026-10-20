use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{
    DistributedCodec, DistributedConfig, DistributedExt, DistributedMetricsFormat,
    SessionStateBuilderExt, WorkerQueryContext, display_plan_ascii,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use iceberg::MetadataLocation;
use iceberg::spec::{
    DataFile, MAIN_BRANCH, ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot, Summary,
    TableMetadata, TableMetadataBuilder,
};

use crate::FixtureTable;
use crate::error::df_err;

/// A DataFusion session for integration tests, over the table given to its builder, if any.
///
/// The table is registered under its name, e.g. `taxi` for `default.taxi`, with
/// `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`, so a session extension must register
/// an `ICEBERG` table factory. The harness owns the table, so its files live as long as
/// the harness.
pub struct IcebergTestHarness {
    ctx: SessionContext,
    table: Option<FixtureTable>,
}

impl IcebergTestHarness {
    pub fn builder() -> IcebergTestHarnessBuilder {
        IcebergTestHarnessBuilder::default()
    }

    /// The registered table, e.g. to inspect its files.
    pub fn table(&self) -> &FixtureTable {
        self.table.as_ref().expect("the harness has a table")
    }

    /// The session's state, e.g. to read its configuration.
    pub fn state(&self) -> SessionState {
        self.ctx.state()
    }

    pub async fn query(&self, sql: &str) -> Result<(String, String)> {
        let plan = self.physical_plan(sql).await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches = collect(plan, self.ctx.task_ctx()).await?;

        Ok((display, pretty_format_batches(&batches)?.to_string()))
    }

    /// Executes a query and returns its plan with collected per-task metrics.
    pub async fn query_with_metrics(&self, sql: &str) -> Result<(String, String)> {
        let plan = self.physical_plan(sql).await?;
        let batches = collect(Arc::clone(&plan), self.ctx.task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask).await?;
        Ok((
            display_plan_ascii(plan.as_ref(), true),
            pretty_format_batches(&batches)?.to_string(),
        ))
    }

    pub async fn physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx.sql(sql).await?.create_physical_plan().await
    }

    /// Returns the registered table's scan without SQL optimization, including for an
    /// empty table.
    pub async fn scan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(table) = &self.table else {
            return exec_err!("the harness has no table to scan");
        };
        self.ctx
            .table_provider(table.ident().name())
            .await?
            .scan(&self.ctx.state(), None, &[], None)
            .await
    }

    /// Encodes `plan` with the session's codecs and decodes it back, as for a worker.
    pub fn roundtrip_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let task_ctx = self.ctx.task_ctx();
        let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
        let proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        proto.try_into_physical_plan(&task_ctx, &codec)
    }
}

/// Derives the metadata to register a table from, given its own.
type MetadataTransform = Box<dyn FnOnce(&TableMetadata) -> TableMetadata>;

/// Extends the coordinator's and every worker's session, e.g. with a table factory.
type SessionExtension = Arc<dyn Fn(SessionStateBuilder) -> SessionStateBuilder + Send + Sync>;

pub struct IcebergTestHarnessBuilder {
    session_builder: SessionStateBuilder,
    session_extensions: Vec<SessionExtension>,
    config_options: BTreeMap<String, String>,
    table_options: BTreeMap<String, String>,
    table: Option<FixtureTable>,
    metadata: Option<MetadataTransform>,
    data_files: Option<Vec<DataFile>>,
    workers: Option<usize>,
}

impl Default for IcebergTestHarnessBuilder {
    fn default() -> Self {
        Self {
            session_builder: SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_distributed_option_extension(DistributedConfig::default()),
            session_extensions: Vec::new(),
            config_options: BTreeMap::new(),
            table_options: BTreeMap::new(),
            table: None,
            metadata: None,
            data_files: None,
            workers: None,
        }
    }
}

impl IcebergTestHarnessBuilder {
    /// Customizes the coordinator's session builder, retaining defaults unless explicitly
    /// replaced. Session extensions and optional worker wiring are installed during
    /// [`Self::build`].
    pub fn configure_session(
        mut self,
        configure: impl FnOnce(SessionStateBuilder) -> Result<SessionStateBuilder>,
    ) -> Result<Self> {
        self.session_builder = configure(self.session_builder)?;
        Ok(self)
    }

    /// Extends the coordinator's session, and every worker's, with `extension`, e.g. to
    /// register the `ICEBERG` table factory. Extensions run in the order they were added.
    pub fn with_session_extension(
        mut self,
        extension: impl Fn(SessionStateBuilder) -> SessionStateBuilder + Send + Sync + 'static,
    ) -> Self {
        self.session_extensions.push(Arc::new(extension));
        self
    }

    /// Sets the coordinator's session option `key`, e.g. `iceberg.eager_file_planning`, to
    /// `value`, e.g. `true`. Options are set after the session extensions run, so they can
    /// belong to a config extension one of them registers. Later values replace earlier
    /// ones for the same key.
    pub fn with_config_option(mut self, key: impl Into<String>, value: impl Display) -> Self {
        self.config_options.insert(key.into(), value.to_string());
        self
    }

    /// Registers `table` under its name.
    pub fn with_table(mut self, table: FixtureTable) -> Self {
        self.table = Some(table);
        self
    }

    /// Sets a table option; later values replace earlier ones for the same key.
    pub fn with_table_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.table_options.insert(key.into(), value.into());
        self
    }

    /// Registers the table from the metadata `metadata` returns for its own. The
    /// metadata is written next to the table's, so it can refer to the table's manifests
    /// and data files.
    pub fn with_metadata(
        mut self,
        metadata: impl FnOnce(&TableMetadata) -> TableMetadata + 'static,
    ) -> Self {
        self.metadata = Some(Box::new(metadata));
        self
    }

    /// Replaces the current snapshot's manifest with synthetic file entries, creating a
    /// snapshot if needed. Data files are not written and the snapshot's summary totals are
    /// preserved, so tests can tell summary statistics from per-file manifest metrics.
    /// The entries' paths are never opened, so planning-only tests can describe files,
    /// metrics and bounds directly. It applies after [`Self::with_metadata`]'s transform.
    pub fn with_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.data_files = Some(files.into_iter().collect());
        self
    }

    /// Enables distributed planning with logical workers backed by in-memory gRPC.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    /// Builds the harness and runs `sql` on it, returning its plan and formatted rows.
    pub async fn query(self, sql: &str) -> Result<(String, String)> {
        let harness = self.build().await?;
        harness.query(sql).await
    }

    /// Builds the harness and runs `sql` on it, returning its plan with per-task metrics and
    /// its formatted rows.
    pub async fn query_with_metrics(self, sql: &str) -> Result<(String, String)> {
        let harness = self.build().await?;
        harness.query_with_metrics(sql).await
    }

    /// Registers the table, if any, from customized metadata if [`Self::with_metadata`] or
    /// [`Self::with_data_files`] was given.
    pub async fn build(mut self) -> Result<IcebergTestHarness> {
        let table = match (
            self.table.take(),
            self.metadata.take(),
            self.data_files.take(),
        ) {
            (None, None, None) => None,
            (None, _, _) => {
                return exec_err!("table metadata can only be customized for a table");
            }
            (Some(table), None, None) => {
                let location = table.metadata_location()?.to_string();
                Some((table, location))
            }
            (Some(table), metadata, data_files) => {
                let mut customized = match metadata {
                    Some(metadata) => metadata(table.table().metadata()),
                    None => table.table().metadata().clone(),
                };
                if let Some(files) = data_files {
                    customized = replace_manifest_with(&table, customized, files).await?;
                }
                let location = MetadataLocation::from_str(table.metadata_location()?)
                    .map_err(df_err)?
                    .with_next_version();
                customized
                    .write_to(table.table().file_io(), &location)
                    .await
                    .map_err(df_err)?;
                Some((table, location.to_string()))
            }
        };

        let mut state = self.session_builder;
        for extension in &self.session_extensions {
            state = extension(state);
        }
        let options = state.config().get_or_insert_default().options_mut();
        for (key, value) in &self.config_options {
            options.set(key, value)?;
        }
        let state = if let Some(workers) = self.workers {
            let extensions = self.session_extensions;
            let resolver =
                InMemoryChannelResolver::from_session_builder(move |ctx: WorkerQueryContext| {
                    let state = extensions
                        .iter()
                        .fold(ctx.builder, |state, extension| extension(state))
                        .build();
                    async move { Ok(state) }
                });
            state
                .with_distributed_planner()
                .with_distributed_worker_resolver(InMemoryWorkerResolver::new(workers))
                .with_distributed_channel_resolver(resolver)
        } else {
            state
        };
        let ctx = SessionContext::new_with_state(state.build());
        let Some((table, metadata_location)) = table else {
            return Ok(IcebergTestHarness { ctx, table: None });
        };
        let mut statement = format!(
            "CREATE EXTERNAL TABLE {} STORED AS ICEBERG LOCATION '{metadata_location}'",
            table.ident().name()
        );
        if !self.table_options.is_empty() {
            let options = self
                .table_options
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "'{}' '{}'",
                        key.replace('\'', "''"),
                        value.replace('\'', "''")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            statement.push_str(&format!(" OPTIONS ({options})"));
        }
        ctx.sql(&statement).await?.collect().await?;
        Ok(IcebergTestHarness {
            ctx,
            table: Some(table),
        })
    }
}

/// Returns a data file's path relative to its table's `data/` directory, which, unlike
/// the temp dir the table lives in, is the same on every run.
pub fn data_file_name(path: &str) -> &str {
    path.rsplit_once("/data/").map_or(path, |(_, name)| name)
}

/// Writes a manifest of `files` and points the current snapshot's manifest list at it,
/// creating a snapshot when the metadata has none. Snapshot identity and summary are kept.
async fn replace_manifest_with(
    table: &FixtureTable,
    metadata: TableMetadata,
    files: Vec<DataFile>,
) -> Result<TableMetadata> {
    let metadata = if metadata.current_snapshot().is_none() {
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(format!(
                "{}/metadata/synthetic-manifest-list.avro",
                metadata.location()
            ))
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: Default::default(),
            })
            .build();
        metadata
            .into_builder(None)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .map_err(df_err)?
            .build()
            .map_err(df_err)?
            .metadata
    } else {
        metadata
    };
    let snapshot = metadata
        .current_snapshot()
        .expect("snapshot initialized above");
    let file_io = table.table().file_io();
    let manifest_path = format!("{}/metadata/synthetic-manifest.avro", metadata.location());
    for path in [manifest_path.as_str(), snapshot.manifest_list()] {
        if file_io.exists(path).await.map_err(df_err)? {
            file_io.delete(path).await.map_err(df_err)?;
        }
    }
    let mut writer = ManifestWriterBuilder::new(
        file_io.new_output(&manifest_path).map_err(df_err)?,
        Some(snapshot.snapshot_id()),
        Arc::clone(metadata.current_schema()),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    for file in files {
        writer
            .add_file(file, snapshot.sequence_number())
            .map_err(df_err)?;
    }
    let manifest = writer.write_manifest_file().await.map_err(df_err)?;
    let mut list = ManifestListWriter::v2(
        file_io
            .new_output(snapshot.manifest_list())
            .map_err(df_err)?
            .writer()
            .await
            .map_err(df_err)?,
        snapshot.snapshot_id(),
        snapshot.parent_snapshot_id(),
        snapshot.sequence_number(),
    );
    list.add_manifests([manifest].into_iter()).map_err(df_err)?;
    list.close().await.map_err(df_err)?;
    Ok(metadata)
}

/// Starts from `metadata` with its schema, partition spec and properties, but no snapshots.
pub fn without_snapshots(metadata: &TableMetadata) -> TableMetadataBuilder {
    let snapshot_ids: Vec<_> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    metadata
        .clone()
        .into_builder(None)
        .remove_ref(MAIN_BRANCH)
        .remove_snapshots(&snapshot_ids)
}
