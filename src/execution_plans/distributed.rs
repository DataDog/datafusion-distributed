use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::networking::get_distributed_worker_resolver;
use crate::protobuf::DistributedCodec;
use crate::stage::{ExecutionTask, MaybeEncodedPlan, Stage};
use datafusion::common::exec_err;
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, displayable,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use rand::Rng;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use url::Url;

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
}

impl DistributedExec {
    fn debug_enabled() -> bool {
        std::env::var("DD_DF_DISTRIBUTED_DEBUG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
        }
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

    fn prepare_plan(
        &self,
        urls: &[Url],
        codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let prepared = Arc::clone(&self.plan).transform_up(|plan| {
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };

            let mut rng = rand::rng();
            let start_idx = rng.random_range(0..urls.len());

            let stage = plan.input_stage();
            let debug = Self::debug_enabled();
            let stage_plan_for_debug = stage
                .plan
                .decoded()
                .map(|p| displayable(p.as_ref()).indent(false).to_string())
                .unwrap_or_else(|_| "<encoded-stage-plan>".to_string());

            let encoded_plan = stage.plan.to_encoded(codec)?;
            if debug {
                let encoded_len = match &encoded_plan {
                    MaybeEncodedPlan::Encoded(bytes) => bytes.len(),
                    MaybeEncodedPlan::Decoded(_) => 0,
                };
                eprintln!(
                    "[df-distributed][prepare] stage={} query_id={} tasks={} encoded_bytes={encoded_len}\n{}",
                    stage.num,
                    stage.query_id,
                    stage.tasks.len(),
                    stage_plan_for_debug
                );
            }

            let ready_stage = Stage {
                query_id: stage.query_id,
                num: stage.num,
                plan: encoded_plan,
                tasks: stage
                    .tasks
                    .iter()
                    .enumerate()
                    .map(|(i, _)| ExecutionTask {
                        url: Some(urls[(start_idx + i) % urls.len()].clone()),
                    })
                    .collect::<Vec<_>>(),
            };

            Ok(Transformed::yes(plan.with_input_stage(ready_stage)?))
        })?;
        Ok(prepared.data)
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

    fn properties(&self) -> &PlanProperties {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let worker_resolver = get_distributed_worker_resolver(context.session_config())?;
        let codec = DistributedCodec::new_combined_with_user(context.session_config());

        let prepared = self.prepare_plan(&worker_resolver.get_urls()?, &codec)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?;
            *guard = Some(prepared.clone());
        }

        prepared.execute(partition, context)
    }
}
