//! Transparent envelope used to observe the producer head actually executed on a worker.
use crate::common::require_one_child;
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

/// Transparent test envelope reporting root metrics and retaining its executed child plans.
#[derive(Debug)]
pub struct TestStageEnvelope {
    input: Arc<dyn ExecutionPlan>,
    executed_heads: Arc<Mutex<Vec<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl TestStageEnvelope {
    /// Shares observations across rebuilt envelope instances.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        executed_heads: Arc<Mutex<Vec<Arc<dyn ExecutionPlan>>>>,
    ) -> Self {
        Self {
            input,
            executed_heads,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for TestStageEnvelope {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TestStageEnvelope")
    }
}

impl ExecutionPlan for TestStageEnvelope {
    fn name(&self) -> &str {
        "TestStageEnvelope"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            require_one_child(children)?,
            Arc::clone(&self.executed_heads),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.executed_heads
            .lock()
            .unwrap()
            .push(Arc::clone(&self.input));
        MetricBuilder::new(&self.metrics)
            .counter("envelope_executions", partition)
            .add(1);
        self.input.execute(partition, context)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
