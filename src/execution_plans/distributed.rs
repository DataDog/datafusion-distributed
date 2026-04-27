use crate::common::require_one_child;
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::networking::get_distributed_worker_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{DistributedCodec, tonic_status_to_datafusion_error};
use crate::stage::{ExecutionTask, Stage};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, SetPlanRequest, TaskKey, coordinator_to_worker_msg::Inner,
    worker_to_coordinator_msg,
};
use crate::{
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, WorkerResolver, get_distributed_channel_resolver,
};
use dashmap::DashMap;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, internal_err};
use datafusion::common::{exec_datafusion_err, internal_datafusion_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, Label, MetricBuilder, MetricValue, Time,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use http::Extensions;
use prost::Message;
use rand::Rng;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;

/// Stores the metrics collected from all worker tasks, and notifies waiters when new entries arrive.
#[derive(Debug)]
pub struct MetricsStore {
    pub map: DashMap<TaskKey, Vec<pb::MetricsSet>>,
    count_tx: watch::Sender<usize>,
}

impl MetricsStore {
    fn new() -> (Self, watch::Receiver<usize>) {
        let (count_tx, count_rx) = watch::channel(0);
        (
            Self {
                map: DashMap::new(),
                count_tx,
            },
            count_rx,
        )
    }

    pub fn insert(&self, key: TaskKey, metrics: Vec<pb::MetricsSet>) {
        self.map.insert(key, metrics);
        // Notify waiters that a new entry was inserted.
        self.count_tx.send_modify(|n| *n += 1);
    }
}

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. A random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    pub plan: Arc<dyn ExecutionPlan>,
    pub prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
    pub task_metrics: Arc<MetricsStore>,
    metrics_count_rx: watch::Receiver<usize>,
}

struct PreparedPlan {
    plan: Arc<dyn ExecutionPlan>,
    join_set: JoinSet<Result<()>>,
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let (store, count_rx) = MetricsStore::new();
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            task_metrics: Arc::new(store),
            metrics_count_rx: count_rx,
        }
    }

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel.
    ///
    /// Metrics are delivered asynchronously after query execution completes, so callers that need
    /// complete metrics (e.g. for observability or display) should await this before inspecting
    /// [`Self::task_metrics`] or calling [`rewrite_distributed_plan_with_metrics`].
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub async fn wait_for_metrics(&self) {
        // Walk the ORIGINAL plan (not `prepared_plan`), because `prepare_plan` detaches each
        // NetworkBoundary's input plan (sets `input_stage.plan = None`). After preparation, the
        // sub-stage plans are no longer reachable via `children()`, so a traversal of the
        // prepared plan would only see the top-level boundary and miss every nested stage.
        // The original plan keeps the full stage tree intact, and the per-stage task counts are
        // identical to what `prepare_plan` dispatched.
        let mut expected_keys: Vec<TaskKey> = Vec::new();
        let _ = self.plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                let stage = boundary.input_stage();
                for i in 0..stage.tasks.len() {
                    expected_keys.push(TaskKey {
                        query_id: stage.query_id.as_bytes().to_vec(),
                        stage_id: stage.num as u64,
                        task_number: i as u64,
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if expected_keys.is_empty() {
            return;
        }
        let mut count_rx = self.metrics_count_rx.clone();
        let task_metrics = Arc::clone(&self.task_metrics);
        // Wait until every expected task key is present in the map. We re-check on each
        // notification from count_rx (which fires on every insert) rather than trusting the
        // count, because duplicate inserts would advance the counter without filling a new slot.
        let _ = count_rx
            .wait_for(|_| {
                expected_keys
                    .iter()
                    .all(|key| task_metrics.map.contains_key(key))
            })
            .await;
    }

    /// Returns the plan which is lazily prepared on execute() and actually gets executed.
    /// It is updated on every call to execute(). Returns an error if .execute() has not been called.
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }

    /// Prepares the distributed plan for execution, which implies:
    /// 1. Perform some worker assignation, choosing randomly from the given URLs and assigning one
    ///    URL per task.
    /// 2. Sending the sliced subplans to the assigned URLs. For each URL assigned to a task, a
    ///    network call feeding the subplan is necessary.
    /// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
    ///    become nodes without children and traversing them will not go further down in.
    /// 4. Spawn a background task per worker that waits for the worker to finish and collects
    ///    its metrics into [DistributedExec::task_metrics] via the coordinator channel.
    fn prepare_plan(&self, ctx: &Arc<TaskContext>) -> Result<PreparedPlan> {
        let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;
        let codec = DistributedCodec::new_combined_with_user(ctx.session_config());

        let urls = worker_resolver.get_urls()?;

        // Metric that measures to total sum of bytes worth of subplans sent.
        let plan_bytes_sent = MetricBuilder::new(&self.metrics)
            .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
            .global_counter("plan_bytes_sent");

        // Latency statistics about the network calls issued to the workers for feeding subplans.
        let start = Instant::now();
        let plan_send_latency = Arc::new(LatencyMetric::new(
            "plan_send_latency",
            |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
            &self.metrics,
        ));

        let mut join_set = JoinSet::new();
        let prepared = Arc::clone(&self.plan).transform_up(|plan| {
            // The following logic is just applied on network boundaries.
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };

            let stage = plan.input_stage();
            let Some(input_plan) = &stage.plan else {
                return internal_err!("Plan is not set for stage {}", stage.num);
            };

            // Right now, we assign random workers to tasks. This might change in the future.
            let start_idx = rand::rng().random_range(0..urls.len());

            // This assumes the plan is the same for all the tasks within a stage. This is fine for
            // now, but it should be possible to send different versions of the subplan to the
            // different tasks.
            let bytes = PhysicalPlanNode::try_from_physical_plan(Arc::clone(input_plan), &codec)?
                .encode_to_vec();

            let mut tasks = Vec::with_capacity(stage.tasks.len());
            for i in 0..stage.tasks.len() {
                let url = urls[(start_idx + i) % urls.len()].clone();
                tasks.push(ExecutionTask {
                    url: Some(url.clone()),
                });
                let task_key = TaskKey {
                    query_id: stage.query_id.as_bytes().to_vec(),
                    stage_id: stage.num as _,
                    task_number: i as _,
                };
                let request = SetPlanRequest {
                    plan_proto: bytes.clone(),
                    task_count: stage.tasks.len() as _,
                    task_key: Some(task_key),
                };
                plan_bytes_sent.add(bytes.len());
                let plan_send_latency = Arc::clone(&plan_send_latency);
                let ctx = Arc::clone(ctx);
                let task_metrics_collection = Arc::clone(&self.task_metrics);

                // Sending the plan and waiting for metrics both run in a detached tokio::spawn so
                // they are not cancelled when the output stream is dropped early.
                let (plan_sent_tx, plan_sent_rx) = tokio::sync::oneshot::channel::<Result<()>>();

                tokio::spawn(async move {
                    let result = send_plan_task(ctx, url, request).await;
                    plan_send_latency.record(&start);

                    let mut response_stream = match result {
                        Err(e) => {
                            let _ = plan_sent_tx.send(Err(e));
                            return;
                        }
                        Ok(s) => s,
                    };
                    let _ = plan_sent_tx.send(Ok(()));

                    // The worker sends exactly one WorkerToCoordinatorMsg after all partitions
                    // of the task have finished (or been dropped early), containing collected metrics.
                    while let Some(Ok(msg)) = response_stream.next().await {
                        let Some(worker_to_coordinator_msg::Inner::MetricsCollection(collection)) =
                            msg.inner
                        else {
                            continue;
                        };
                        for task_metrics in collection.tasks {
                            if let Some(task_key) = task_metrics.task_key {
                                task_metrics_collection.insert(task_key, task_metrics.metrics);
                            }
                        }
                    }
                });

                join_set.spawn(async move { plan_sent_rx.await.unwrap_or_else(|_| Ok(())) });
            }

            Ok(Transformed::yes(plan.with_input_stage(Stage {
                query_id: stage.query_id,
                num: stage.num,
                plan: None,
                tasks,
            })?))
        })?;
        Ok(PreparedPlan {
            plan: prepared.data,
            join_set,
        })
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
            metrics: self.metrics.clone(),
            task_metrics: Arc::clone(&self.task_metrics),
            metrics_count_rx: self.metrics_count_rx.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let PreparedPlan { plan, join_set } = self.prepare_plan(&context)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(plan.clone());
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        // Spawn the task that pulls data from child...
        builder.spawn(async move {
            let mut stream = plan.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });
        // ...in parallel to the one that feeds the plan to workers.
        builder.spawn(async move {
            for res in join_set.join_all().await {
                res?;
            }
            Ok(())
        });
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

use crate::worker::generated::worker::WorkerToCoordinatorMsg;
async fn send_plan_task(
    ctx: Arc<TaskContext>,
    url: Url,
    request: SetPlanRequest,
) -> Result<tonic::codec::Streaming<WorkerToCoordinatorMsg>> {
    let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());
    let mut client = channel_resolver.get_worker_client_for_url(&url).await?;

    let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
    headers.extend(get_passthrough_headers(ctx.session_config()));

    let msg = CoordinatorToWorkerMsg {
        inner: Some(Inner::SetPlanRequest(request)),
    };
    let request = Request::from_parts(
        MetadataMap::from_headers(headers),
        Extensions::default(),
        futures::stream::once(async { msg }),
    );

    let response_stream = client
        .coordinator_channel(request)
        .await
        .map_err(|e| {
            tonic_status_to_datafusion_error(&e)
                .unwrap_or_else(|| exec_datafusion_err!("Error sending plan to worker {url}: {e}"))
        })?
        .into_inner();
    Ok(response_stream)
}

/// DataFusion metrics system is pretty limited from an API standpoint. This intermediate struct
/// bridges the gaps that are not satisfied by upstream API for measuring latency.
struct LatencyMetric {
    max: Time,
    avg: Time,
    max_latency_micros: AtomicU64,
    sum_latency_micros: AtomicU64,
    count_latency_micros: AtomicU64,
}

impl Drop for LatencyMetric {
    fn drop(&mut self) {
        self.max.add_duration(Duration::from_micros(
            self.max_latency_micros.load(Ordering::Relaxed),
        ));
        self.avg.add_duration(Duration::from_micros(
            self.sum_latency_micros.load(Ordering::Relaxed)
                / self.count_latency_micros.load(Ordering::Relaxed).max(1),
        ));
    }
}

impl LatencyMetric {
    fn new(
        name: impl Display,
        builder: impl Fn(MetricBuilder) -> MetricBuilder,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let max = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_max").into(),
            time: max.clone(),
        });
        let avg = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_avg").into(),
            time: avg.clone(),
        });
        Self {
            max,
            avg,
            max_latency_micros: AtomicU64::new(0),
            sum_latency_micros: AtomicU64::new(0),
            count_latency_micros: AtomicU64::new(0),
        }
    }

    fn record(&self, start: &Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.max_latency_micros.fetch_max(micros, Ordering::Relaxed);
        self.sum_latency_micros.fetch_add(micros, Ordering::Relaxed);
        self.count_latency_micros.fetch_add(1, Ordering::Relaxed);
    }
}
