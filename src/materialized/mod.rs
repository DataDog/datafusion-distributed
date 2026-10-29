//! Opt-in materialized execution for applications with replay-safe query inputs.
//!
//! Register the same [`MaterializedStore`] in coordinator and worker session configs.
//! Register [`MaterializedExecution`] only on the coordinating session. The application
//! owns source snapshot stability and supplies a [`ReplayPolicy`] that validates the
//! plan and classifies failures. Streaming execution remains the default.

pub(crate) mod journal;
pub(crate) mod manifest;
mod store;

pub use store::{IntermediateResultStore, MaterializedStore, ObjectStoreIntermediateStore};

use datafusion::common::{DataFusionError, Result, config_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

/// Application contract for repeatable, read-only execution.
///
/// Validation runs before any work is dispatched. Implementations must check stable
/// source snapshots and that operators, UDFs, codecs and worker hooks can safely run
/// more than once. A serializable plan alone does not establish replay safety.
pub trait ReplayPolicy: Debug + Send + Sync {
    /// Reject a query whose complete distributed plan is not safe to replay.
    fn validate(&self, plan: &Arc<dyn ExecutionPlan>, context: &TaskContext) -> Result<()>;

    /// Classify an execution failure. Deterministic computation errors should fail.
    /// A lost input must rebuild stages, not retry against the same missing input.
    fn on_error(&self, error: &DataFusionError) -> RecoveryAction;
}

/// Recovery requested by an application's [`ReplayPolicy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecoveryAction {
    /// Return the original error without retrying.
    Fail,
    /// Retry this task with its existing committed inputs and fresh operator state.
    RetryTask,
    /// Rebuild the query's stages from the original sealed work-unit journals.
    /// This conservative recovery also discards completed descendants.
    RebuildStages,
}

/// Coordinator-only session extension enabling materialized query execution.
///
/// Set through `SessionConfig::with_extension`. Workers need [`MaterializedStore`],
/// but do not need the replay policy. All execution remains scoped to one live
/// coordinator. Final output is sealed before any result batch is exposed.
#[derive(Clone, Debug)]
pub struct MaterializedExecution {
    pub(crate) policy: Arc<dyn ReplayPolicy>,
    pub(crate) max_task_retries: usize,
    pub(crate) max_concurrent_tasks: usize,
    pub(crate) max_rebuilds: usize,
    pub(crate) max_retries: usize,
    pub(crate) retry_delay: Duration,
    pub(crate) timeout: Duration,
}

impl MaterializedExecution {
    /// Enable recovery using the supplied explicit replay-safety contract.
    pub fn new(policy: Arc<dyn ReplayPolicy>) -> Self {
        Self {
            policy,
            max_task_retries: 3,
            max_concurrent_tasks: 16,
            max_rebuilds: 2,
            max_retries: 16,
            retry_delay: Duration::from_millis(100),
            timeout: Duration::from_secs(300),
        }
    }

    /// Limit in-flight logical tasks per query (default: 16).
    ///
    /// A task retains its slot during retry delays. Output partitions within a
    /// task are drained concurrently and do not count as separate task slots.
    /// Workers remain subject to the application's routing and admission policy.
    pub fn with_max_concurrent_tasks(mut self, tasks: usize) -> Result<Self> {
        if tasks == 0 {
            return config_err!("Materialized task concurrency must be nonzero");
        }
        self.max_concurrent_tasks = tasks;
        Ok(self)
    }

    /// Bound task retries, complete rebuilds and their combined query-wide total.
    pub fn with_retry_limits(mut self, task: usize, rebuilds: usize, total: usize) -> Self {
        self.max_task_retries = task;
        self.max_rebuilds = rebuilds;
        self.max_retries = total;
        self
    }

    /// Delay between attempts, including complete rebuilds.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Bound preparation and execution, excluding delivery of the committed result.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}
