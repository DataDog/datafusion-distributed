use crate::execution_plans::SamplerExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Wraps a plan in the internal sampler node for worker setup integration tests.
pub fn wrap_in_sampler(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SamplerExec::new(plan))
}
