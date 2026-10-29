use super::distributed::PreparedPlan;
use super::query_coordinator::QueryCoordinator;
use crate::common::{on_drop_stream, task_ctx_with_extension};
use crate::execution_plans::{
    ChildrenIsolatorUnionExec, DistributedLeafExec, MaterializedPartition, MaterializedReaderExec,
    MaterializedWriterExec, ShuffleMode,
};
use crate::materialized::journal::InputJournal;
use crate::materialized::manifest::{AttemptManifest, PartitionManifest};
use crate::materialized::{MaterializedExecution, MaterializedStore, RecoveryAction};
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::LocalStage;
use crate::work_unit_feed::RemoteWorkUnitFeedRegistry;
use crate::{
    CoordinatorToWorkerMsg, DistributedConfig, DistributedTaskContext, ExecuteTaskRequest,
    LocalWorkerContext, NetworkBoundaryExt, NetworkCoalesceExec, NetworkShuffleExec, ProducerHead,
    Stage, TaskKey, get_distributed_channel_resolver,
};
use datafusion::arrow::array::{Array, BinaryArray};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{
    HashMap, Result, config_err, exec_datafusion_err, exec_err, internal_datafusion_err,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use prost::Message;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::unbounded_channel;
use uuid::Uuid;

/// Starts a query-scoped materialization driver. No output is sent until the
/// complete root manifest has been accepted by this driver.
pub(super) fn execute(
    base_plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    options: Arc<MaterializedExecution>,
    prepared: Arc<OnceLock<PreparedPlan>>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let schema = base_plan.schema();
    let cfg = DistributedConfig::from_task_context(&context)?;
    if cfg.dynamic_task_count {
        return config_err!("Materialized execution requires static task counts");
    }
    if context
        .session_config()
        .options()
        .optimizer
        .enable_dynamic_filter_pushdown
    {
        return config_err!("Materialized execution requires enable_dynamic_filter_pushdown=false");
    }
    options.policy.validate(&base_plan, &context)?;
    let store = MaterializedStore::from_ctx(&context)?;
    let query = Uuid::new_v4();
    let mut output = RecordBatchReceiverStreamBuilder::new(base_plan.schema(), 1);
    let tx = output.tx();
    let retries = MetricBuilder::new(metrics).global_counter("materialized_retries");
    let rebuilds = MetricBuilder::new(metrics).global_counter("materialized_rebuilds");
    let driver_store = Arc::clone(&store);
    output.spawn(async move {
        let mut driver = Driver {
            context,
            options: Arc::clone(&options),
            store: driver_store,
            query,
            journals: HashMap::new(),
            retries_left: options.max_retries,
            retries,
            rebuilds,
        };
        let run = async {
            // Feed enumeration is preparation, not retryable execution. A partial
            // failure here must never invoke a stateful provider again.
            driver.seal_inputs(&base_plan).await?;
            for generation in 0..=options.max_rebuilds {
                let result = driver.run(Arc::clone(&base_plan)).await;
                match result {
                    Ok(reader) => return Ok(reader),
                    Err(error) => {
                        if driver.options.policy.on_error(&error) != RecoveryAction::RebuildStages
                            || generation == options.max_rebuilds
                            || driver.retries_left == 0
                        {
                            return Err(error);
                        }
                        driver.retries_left -= 1;
                        driver.rebuilds.add(1);
                        tokio::time::sleep(options.retry_delay).await;
                    }
                }
            }
            exec_err!("Materialized recovery budget exhausted")
        };
        let reader = tokio::time::timeout(options.timeout, run)
            .await
            .map_err(|_| exec_datafusion_err!("Materialized query deadline exceeded"))??;
        prepared
            .set(PreparedPlan {
                head_stage: Arc::clone(&reader),
                plan_for_viz: Arc::clone(&reader),
            })
            .map_err(|_| internal_datafusion_err!("DistributedExec was already prepared"))?;
        let mut result = reader.execute(0, Arc::clone(&driver.context))?;
        // Delivery never re-executes a query or restarts an already delivered partition.
        while let Some(batch) = result.try_next().await? {
            if tx.send(Ok(batch)).await.is_err() {
                break;
            }
        }
        driver.store.store.remove_query(query).await?;
        Ok(())
    });
    // Drop cannot await storage I/O. Normal completion awaits cleanup above; this
    // cancellation fallback is best effort, backed by the store's orphan TTL.
    // Capture the runtime while it exists so dropping outside it does not panic.
    let runtime = tokio::runtime::Handle::current();
    let stream = on_drop_stream(output.build(), move || {
        runtime.spawn(async move {
            // The store owner must provide orphan expiry if cleanup fails or races
            // a late remote write. No query can consume those abandoned objects.
            let _cleanup = store.store.remove_query(query).await;
        });
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

struct Driver {
    context: Arc<TaskContext>,
    options: Arc<MaterializedExecution>,
    store: Arc<MaterializedStore>,
    query: Uuid,
    journals: HashMap<(usize, usize), InputJournal>,
    retries_left: usize,
    retries: Count,
    rebuilds: Count,
}

impl Driver {
    async fn seal_inputs(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        let mut stages = vec![(usize::MAX, 1, Arc::clone(plan))];
        plan.apply(|node| {
            if let Some(boundary) = node.as_network_boundary() {
                let Stage::Local(stage) = boundary.input_stage() else {
                    return exec_err!("Materialized execution requires unbound stage plans");
                };
                stages.push((stage.num, stage.tasks, Arc::clone(&stage.plan)));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        for (stage, tasks, plan) in stages {
            for task in 0..tasks {
                let key = (stage, task);
                if self.journals.contains_key(&key) {
                    continue;
                }
                let journal = InputJournal::seal(
                    &plan,
                    DistributedTaskContext {
                        task_index: task,
                        task_count: tasks,
                    },
                    &self.context,
                    &self.store,
                    format!("{}/inputs/{stage}/{task}", self.query),
                )
                .await?;
                self.journals.insert(key, journal);
            }
        }
        Ok(())
    }

    async fn run(&mut self, base_plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.prepare_plan(base_plan, 1).await?;
        let manifest = self
            .run_task(usize::MAX, 0, 1, Arc::clone(&plan), true)
            .await?;
        let partitions = manifest
            .partitions
            .into_iter()
            .map(|partition| MaterializedPartition {
                producers: vec![partition],
            })
            .collect();
        Ok(Arc::new(MaterializedReaderExec::try_new(
            partitions,
            Arc::clone(plan.properties()),
        )?))
    }

    fn prepare_plan<'a>(
        &'a mut self,
        plan: Arc<dyn ExecutionPlan>,
        tasks: usize,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(async move {
            if let Some(boundary) = plan.as_network_boundary() {
                let Stage::Local(stage) = boundary.input_stage() else {
                    return exec_err!("Expected an unbound materialized stage");
                };
                let stage_id = stage.num;
                let stage_tasks = stage.tasks;
                let producer = self
                    .prepare_plan(Arc::clone(&stage.plan), stage_tasks)
                    .await?;
                let producer = boundary.producer_head(tasks)?.insert(producer)?;
                let mut manifests = Vec::new();
                for task in 0..stage_tasks {
                    manifests.push(
                        self.run_task(stage_id, task, stage_tasks, Arc::clone(&producer), false)
                            .await?,
                    );
                }
                return make_readers(plan, tasks, &manifests);
            }
            let mut children = Vec::new();
            for (index, child) in plan.children().into_iter().enumerate() {
                let child_tasks =
                    if let Some(union) = plan.downcast_ref::<ChildrenIsolatorUnionExec>() {
                        union.child_task_counts()[index]
                    } else {
                        tasks
                    };
                children.push(self.prepare_plan(Arc::clone(child), child_tasks).await?);
            }
            if children.is_empty() {
                return Ok(plan);
            }
            plan.replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
            )
        })
    }

    async fn run_task(
        &mut self,
        stage: usize,
        task: usize,
        tasks: usize,
        plan: Arc<dyn ExecutionPlan>,
        local: bool,
    ) -> Result<AttemptManifest> {
        let key = (stage, task);
        let journal = self
            .journals
            .get(&key)
            .ok_or_else(|| internal_datafusion_err!("Missing sealed input journal"))?
            .clone();
        for retry in 0..=self.options.max_task_retries {
            let attempt = Uuid::new_v4();
            let prefix = format!("{}/attempts/{attempt}", self.query);
            let writer: Arc<dyn ExecutionPlan> = Arc::new(MaterializedWriterExec::new(
                Arc::clone(&plan),
                prefix.clone(),
            ));
            let attempt_stage = LocalStage {
                query_id: attempt,
                num: stage,
                plan: writer,
                tasks,
                metrics_set: Default::default(),
            };
            let result = attempt_stream(
                attempt_stage,
                task,
                Arc::clone(&self.context),
                journal.clone(),
                Arc::clone(&self.store),
                local,
            )?
            .try_collect::<Vec<_>>()
            .await;
            let result = result.and_then(|batches| {
                if batches.len() != 1 || batches[0].num_rows() != 1 || batches[0].num_columns() != 1
                {
                    return exec_err!("Worker did not return exactly one sealed manifest");
                }
                let Some(column) = batches[0].column(0).as_any().downcast_ref::<BinaryArray>()
                else {
                    return exec_err!("Invalid materialized completion schema");
                };
                if column.is_null(0) {
                    return exec_err!("Null materialized completion");
                }
                let manifest = AttemptManifest::decode(column.value(0)).map_err(|error| {
                    exec_datafusion_err!("Invalid materialized completion: {error}")
                })?;
                if manifest.partitions.len() != plan.properties().partitioning.partition_count() {
                    return exec_err!("Incomplete materialized task output");
                }
                for partition in &manifest.partitions {
                    for segment in &partition.segments {
                        if !segment.key.starts_with(&format!("{prefix}/")) {
                            return exec_err!("Attempt returned an output outside its namespace");
                        }
                    }
                }
                Ok(manifest)
            });
            match result {
                Ok(manifest) => return Ok(manifest),
                Err(error) => {
                    if self.options.policy.on_error(&error) != RecoveryAction::RetryTask
                        || retry == self.options.max_task_retries
                        || self.retries_left == 0
                    {
                        return Err(error);
                    }
                    self.retries_left -= 1;
                    self.retries.add(1);
                    tokio::time::sleep(self.options.retry_delay).await;
                }
            }
        }
        exec_err!("Materialized task retry budget exhausted")
    }
}

fn attempt_stream(
    stage: LocalStage,
    task: usize,
    context: Arc<TaskContext>,
    journal: InputJournal,
    store: Arc<MaterializedStore>,
    local: bool,
) -> Result<SendableRecordBatchStream> {
    let mut output = RecordBatchReceiverStreamBuilder::new(stage.plan.schema(), 1);
    let tx = output.tx();
    let coordinator = Arc::new(QueryCoordinator::new(
        Arc::clone(&context),
        &ExecutionPlanMetricsSet::new(),
        None,
        None,
        &mut output,
    ));
    let guard = coordinator.spawner.end_query_guard();
    output.spawn(async move {
        let _guard = guard;
        let mut stage_coordinator = coordinator.stage_coordinator(&stage);
        let mut stream = if local {
            let specialized = stage_coordinator.task_specialized_plan(task)?;
            let mut feeds = RemoteWorkUnitFeedRegistry::default();
            for declaration in specialized.work_unit_feed_declarations {
                feeds.add(declaration.id, declaration.partitions);
            }
            let (feed_tx, mut feed_rx) = unbounded_channel();
            journal.replay(&store, &feed_tx).await?;
            drop(feed_tx);
            while let Some(message) = feed_rx.recv().await {
                if let CoordinatorToWorkerMsg::WorkUnitBatch(batch) = message {
                    for unit in batch.batch {
                        let sender = feeds
                            .senders
                            .get(&(unit.id, unit.partition))
                            .ok_or_else(|| internal_datafusion_err!("Undeclared replay feed"))?;
                        sender
                            .send(Ok(unit))
                            .map_err(|_| exec_datafusion_err!("Replay feed closed"))?;
                    }
                }
            }
            drop(feeds.senders);
            let context = Arc::new(task_ctx_with_extension(&context, feeds.receivers));
            specialized.plan.execute(0, context)?
        } else {
            let (url, worker_tx, worker_stream) =
                stage_coordinator.init_bidirectional_stream(task).await?;
            stage_coordinator.worker_to_coordinator_task(task, worker_stream);
            journal.replay(&store, &worker_tx).await?;
            // Retain the control sender until the attempt ends, as in streaming execution.
            let resolver = get_distributed_channel_resolver(&context);
            let mut channel = match LocalWorkerContext::from_ctx(&context) {
                Some(worker) if worker.self_url == url => worker.to_worker_channel(),
                _ => resolver.get_worker_client_for_url(&url).await?,
            };
            let request = ExecuteTaskRequest {
                task_key: TaskKey {
                    query_id: stage.query_id,
                    stage_id: stage.num,
                    task_number: task,
                },
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: ProducerHead::None,
            };
            let mut streams = channel
                .execute_task(
                    get_passthrough_headers(context.session_config()),
                    request,
                    ExecutionPlanMetricsSet::new(),
                    &context,
                )
                .await?;
            if streams.len() != 1 {
                return exec_err!("Expected one materialized completion stream");
            }
            let stream = streams.remove(0);
            // init_bidirectional_stream keeps the control stream open until guard drop.
            Box::pin(RecordBatchStreamAdapter::new(
                stage.plan.schema(),
                on_drop_stream(stream, move || drop(worker_tx)),
            )) as SendableRecordBatchStream
        };
        while let Some(batch) = stream.try_next().await? {
            if tx.send(Ok(batch)).await.is_err() {
                break;
            }
        }
        Ok(())
    });
    Ok(output.build())
}

fn make_readers(
    boundary: Arc<dyn ExecutionPlan>,
    tasks: usize,
    manifests: &[AttemptManifest],
) -> Result<Arc<dyn ExecutionPlan>> {
    if tasks == 0 || manifests.is_empty() {
        return exec_err!("Materialized exchange requires producer and consumer tasks");
    }
    let count = boundary.properties().partitioning.partition_count();
    let mut variants: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    for task in 0..tasks {
        let mut partitions = Vec::new();
        for partition in 0..count {
            let mut producers = Vec::new();
            if boundary.downcast_ref::<NetworkCoalesceExec>().is_some() {
                let max_group = manifests.len().div_ceil(tasks);
                let per_task = count / max_group.max(1);
                if per_task == 0 {
                    return exec_err!("Invalid materialized coalesce partition count");
                }
                let group_len =
                    manifests.len() / tasks + usize::from(task < manifests.len() % tasks);
                let group_start =
                    task * (manifests.len() / tasks) + task.min(manifests.len() % tasks);
                let offset = partition / per_task;
                if offset < group_len {
                    producers.push(select_partition(
                        manifests,
                        group_start + offset,
                        partition % per_task,
                    )?);
                }
            } else if let Some(shuffle) = boundary.downcast_ref::<NetworkShuffleExec>() {
                match shuffle.mode {
                    ShuffleMode::Direct => {
                        for producer in 0..manifests.len() {
                            producers.push(select_partition(
                                manifests,
                                producer,
                                task * count + partition,
                            )?);
                        }
                    }
                    ShuffleMode::TwoPhase { .. } => {
                        producers.push(select_partition(manifests, partition, task)?)
                    }
                }
            } else {
                for producer in 0..manifests.len() {
                    producers.push(select_partition(
                        manifests,
                        producer,
                        task * count + partition,
                    )?);
                }
            }
            partitions.push(MaterializedPartition { producers });
        }
        variants.push(Arc::new(MaterializedReaderExec::try_new(
            partitions,
            Arc::clone(boundary.properties()),
        )?));
    }
    Ok(Arc::new(DistributedLeafExec::try_new(boundary, variants)?))
}

fn select_partition(
    manifests: &[AttemptManifest],
    task: usize,
    partition: usize,
) -> Result<PartitionManifest> {
    manifests
        .get(task)
        .and_then(|manifest| manifest.partitions.get(partition))
        .cloned()
        .ok_or_else(|| {
            exec_datafusion_err!("Missing materialized task {task} partition {partition}")
        })
}
