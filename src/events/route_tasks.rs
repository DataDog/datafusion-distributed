use super::defaults::dial_with_failover;
use crate::events::common::EventHandlerChain;
use crate::{DistributedConfig, TaskKey, WorkerResolver, WorkerToCoordinatorMsg};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::BoxStream;
use std::sync::Arc;
use url::Url;

/// Information supplied when the coordinator assigns one distributed task to a worker.
#[derive(Clone, Copy)]
pub struct RouteTaskEvent<'a> {
    /// The task context active for the query being coordinated.
    pub task_ctx: &'a Arc<TaskContext>,
    /// Metrics attached to the [crate::DistributedExec]
    pub metrics: &'a ExecutionPlanMetricsSet,
    /// [WorkerResolver] in scope.
    pub worker_resolver: &'a dyn WorkerResolver,
    /// Identifier of the task that is getting assigned.
    pub task_key: TaskKey,
    /// Number of tasks of the stage to which the assigned task belongs.
    pub task_count: usize,
    /// The exact plan variant that will be sent for this task.
    pub task_specialized_plan: &'a Arc<dyn ExecutionPlan>,
    /// Establishes a coordinator-to-worker connection for a candidate URL. Users are expected
    /// to call `dialer.dial()` inside their [RouteTaskHandler] implementations for establishing
    /// a connection with the remote worker.
    ///
    /// `dialer.dial()` can be called multiple times over different URLs until one successfully
    /// connects.
    pub dialer: &'a dyn CoordinatorToWorkerDialer,
}

impl RouteTaskEvent<'_> {
    /// Connects to the preferred worker using DFD's normal retry and failover policy.
    ///
    /// Custom placement handlers can select a preferred URL without changing how overload,
    /// retryable connection errors, backoff, or terminal errors are handled. Other registered
    /// workers remain eligible for failover, and retries use the query's distributed settings.
    pub async fn dial_with_failover(&self, preferred: Url) -> Result<RouteTaskEventResponse> {
        dial_with_failover(
            self.dialer,
            preferred,
            self.worker_resolver.get_urls()?,
            self.metrics,
            DistributedConfig::from_task_context(self.task_ctx)?,
        )
        .await
    }
}

/// A worker connection selected by a [`RouteTaskHandler`].
pub struct RouteTaskEventResponse {
    /// The URL of the selected worker. Once this URL is returned, the worker officially starts
    /// being part of the query, and it must commit to drive it to completion. Once a worker URL is
    /// returned, a failure in the worker means the failure of the query.
    pub url: Url,
    /// Messages streamed from the selected worker to the coordinator. This must be the result of
    /// calling `ev.dialer.dial(url).await`, where `dialer` is provided by the [RouteTaskEvent]
    /// definition.
    pub worker_to_coordinator_stream: BoxStream<'static, Result<WorkerToCoordinatorMsg>>,
}

/// Assigns a distributed task to a worker and establishes its connection.
///
/// Handlers are evaluated in registration order, with custom handlers before built-ins. Return
/// `None` to defer to the next handler. To implement retries or failover, call
/// [`RouteTaskEvent::dialer`] multiple times and return the successful response.
#[async_trait]
pub trait RouteTaskHandler: Send + Sync + 'static {
    /// Attempts to assign the task described by the event to a worker.
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>>;
}

#[async_trait]
impl RouteTaskHandler for Arc<dyn RouteTaskHandler> {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        self.as_ref().handle(ev).await
    }
}

pub(crate) type RouteTaskHandlers = EventHandlerChain<dyn RouteTaskHandler>;

impl RouteTaskHandlers {
    pub(crate) async fn handle(ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let handlers = ev
            .task_ctx
            .session_config()
            .get_extension::<RouteTaskHandlers>()?;
        for handler in handlers.iter() {
            if let Some(response) = handler.handle(ev).await {
                return Some(response);
            }
        }
        None
    }
}

// === CoordinatorToWorkerDialer ===

/// Establishes coordinator-to-worker connections for assignment candidates.
///
/// Each call represents an independent connection attempt. A handler may call `dial` multiple
/// times, sequentially or concurrently, when implementing retries or failover.
#[async_trait]
pub trait CoordinatorToWorkerDialer: Send + Sync {
    /// Attempts to connect the task to `url`.
    async fn dial(&self, url: Url) -> Result<RouteTaskEventResponse>;
}

pub(crate) fn new_coordinator_to_worker_dialer<F, Fut>(f: F) -> impl CoordinatorToWorkerDialer
where
    F: Fn(Url) -> Fut + Send + Sync,
    Fut: Future<Output = Result<RouteTaskEventResponse>> + Send,
{
    struct S<F>(F);

    #[async_trait]
    impl<F, Fut> CoordinatorToWorkerDialer for S<F>
    where
        F: Fn(Url) -> Fut + Send + Sync,
        Fut: Future<Output = Result<RouteTaskEventResponse>> + Send,
    {
        async fn dial(&self, url: Url) -> Result<RouteTaskEventResponse> {
            self.0(url).await
        }
    }

    S(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RetryOutcome;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::exec_datafusion_err;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionConfig;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[tokio::test]
    async fn preferred_worker_uses_classified_failover() -> Result<()> {
        let workers = InMemoryWorkerResolver::new(2);
        let urls = workers.get_urls()?;
        let preferred = urls[0].clone();
        let calls = Mutex::new(Vec::new());
        let dialer = new_coordinator_to_worker_dialer(|url: Url| {
            calls.lock().unwrap().push(url.clone());
            let rejected = url == preferred;
            async move {
                if rejected {
                    return Err(RetryOutcome::OtherUrl.tag(exec_datafusion_err!("overloaded")));
                }
                Ok(RouteTaskEventResponse {
                    url,
                    worker_to_coordinator_stream: Box::pin(futures::stream::empty()),
                })
            }
        });
        let context = Arc::new(TaskContext::default().with_session_config(
            SessionConfig::new().with_option_extension(DistributedConfig::default()),
        ));
        let plan = Arc::new(EmptyExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let metrics = ExecutionPlanMetricsSet::new();
        let event = RouteTaskEvent {
            task_ctx: &context,
            metrics: &metrics,
            worker_resolver: &workers,
            task_key: TaskKey {
                query_id: Uuid::nil(),
                stage_id: 0,
                task_number: 0,
            },
            task_count: 1,
            task_specialized_plan: &plan,
            dialer: &dialer,
        };
        let response = event.dial_with_failover(preferred.clone()).await?;
        assert_eq!(response.url, urls[1]);
        assert_eq!(*calls.lock().unwrap(), urls);
        Ok(())
    }
}
