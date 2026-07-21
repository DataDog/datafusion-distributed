use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use http::HeaderMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct WorkerQueryContext {
    pub builder: SessionStateBuilder,
    pub headers: HeaderMap,
}

/// builds a DataFusion's [SessionState] in each query issued to a worker.
#[async_trait]
pub trait WorkerSessionBuilder {
    /// Builds a custom [SessionState] scoped to a single ArrowFlight gRPC call, allowing the
    /// users to provide a customized DataFusion session with things like custom extension codecs,
    /// custom physical optimization rules, UDFs, UDAFs, config extensions, etc...
    ///
    /// Example:
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// # use datafusion::error::DataFusionError;
    /// # use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder, TaskContext};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{DistributedExt, WorkerSessionBuilder, WorkerQueryContext};
    ///
    /// #[derive(Debug)]
    /// struct CustomExecCodec;
    ///
    /// impl PhysicalExtensionCodec for CustomExecCodec {
    ///     fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], ctx: &TaskContext) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    ///         todo!()
    ///     }
    ///
    ///     fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl WorkerSessionBuilder for CustomSessionBuilder {
    ///     async fn build_session_state(&self, ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    ///         Ok(ctx
    ///             .builder
    ///             .with_distributed_user_codec(CustomExecCodec)
    ///             // Add your UDFs, optimization rules, etc...
    ///             .build())
    ///     }
    /// }
    /// ```
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError>;

    /// Runs the synchronous setup for a plan received by the worker.
    ///
    /// The callback decodes the physical plan, applies worker plan hooks, and starts any initial
    /// sampling work. Implementations may override this method to wrap that work with tracing or
    /// resource accounting. An implementation that returns `Ok(())` must invoke `setup` exactly
    /// once and return its result; it may reject setup by returning an error without invoking it.
    fn run_plan_setup(
        &self,
        _session_state: &SessionState,
        setup: &mut dyn FnMut() -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        setup()
    }
}

/// Noop implementation of the [WorkerSessionBuilder]. Used by default if no [WorkerSessionBuilder]
/// is provided while building the Worker.
#[derive(Debug, Clone)]
pub struct DefaultSessionBuilder;

#[async_trait]
impl WorkerSessionBuilder for DefaultSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        Ok(ctx.builder.build())
    }
}

/// Implementation of [WorkerSessionBuilder] for any async function that returns a [Result]
#[async_trait]
impl<F, Fut> WorkerSessionBuilder for F
where
    F: Fn(WorkerQueryContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<SessionState, DataFusionError>> + Send + 'static,
{
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        self(ctx).await
    }
}

pub trait MappedWorkerSessionBuilderExt {
    /// Maps an existing [WorkerSessionBuilder] allowing to add further extensions
    /// to its already built [SessionStateBuilder].
    ///
    /// Useful if there's already a [WorkerSessionBuilder] that needs to be extended
    /// with further capabilities.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use datafusion::execution::SessionStateBuilder;
    /// # use datafusion_distributed::{DefaultSessionBuilder, MappedWorkerSessionBuilderExt};
    ///
    /// let session_builder = DefaultSessionBuilder
    ///     .map(|b: SessionStateBuilder| {
    ///         // Add further things.
    ///         Ok(b.build())
    ///     });
    /// ```
    fn map<F>(self, f: F) -> MappedWorkerSessionBuilder<Self, F>
    where
        Self: Sized,
        F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError>;
}

impl<T: WorkerSessionBuilder> MappedWorkerSessionBuilderExt for T {
    fn map<F>(self, f: F) -> MappedWorkerSessionBuilder<Self, F>
    where
        Self: Sized,
    {
        MappedWorkerSessionBuilder {
            inner: self,
            f: Arc::new(f),
        }
    }
}

pub struct MappedWorkerSessionBuilder<T, F> {
    inner: T,
    f: Arc<F>,
}

impl<T: Clone, F> Clone for MappedWorkerSessionBuilder<T, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            f: self.f.clone(),
        }
    }
}

#[async_trait]
impl<T, F> WorkerSessionBuilder for MappedWorkerSessionBuilder<T, F>
where
    T: WorkerSessionBuilder + Send + Sync + 'static,
    F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError> + Send + Sync,
{
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        let state = self.inner.build_session_state(ctx).await?;
        let builder = SessionStateBuilder::new_from_existing(state);
        (self.f)(builder)
    }

    fn run_plan_setup(
        &self,
        session_state: &SessionState,
        setup: &mut dyn FnMut() -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.inner.run_plan_setup(session_state, setup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::{assert_contains, internal_err};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn default_plan_setup_runs_callback_once() {
        let state = SessionStateBuilder::new().build();
        let mut callback_calls = 0;

        DefaultSessionBuilder
            .run_plan_setup(&state, &mut || {
                callback_calls += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(callback_calls, 1);
    }

    #[test]
    fn default_plan_setup_propagates_callback_error() {
        let state = SessionStateBuilder::new().build();

        let error = DefaultSessionBuilder
            .run_plan_setup(&state, &mut || internal_err!("plan setup failed"))
            .expect_err("the callback error should be returned");

        assert_contains!(error.to_string(), "plan setup failed");
    }

    #[test]
    fn mapped_plan_setup_delegates_to_inner_builder() {
        let wrapper_calls = Arc::new(AtomicUsize::new(0));
        let builder = RecordingSessionBuilder {
            wrapper_calls: Arc::clone(&wrapper_calls),
        }
        .map(|builder| Ok(builder.build()));
        let state = SessionStateBuilder::new().build();
        let mut callback_calls = 0;

        builder
            .run_plan_setup(&state, &mut || {
                callback_calls += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(wrapper_calls.load(Ordering::Relaxed), 1);
        assert_eq!(callback_calls, 1);
    }

    struct RecordingSessionBuilder {
        wrapper_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WorkerSessionBuilder for RecordingSessionBuilder {
        async fn build_session_state(
            &self,
            ctx: WorkerQueryContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(ctx.builder.build())
        }

        fn run_plan_setup(
            &self,
            _session_state: &SessionState,
            setup: &mut dyn FnMut() -> Result<(), DataFusionError>,
        ) -> Result<(), DataFusionError> {
            self.wrapper_calls.fetch_add(1, Ordering::Relaxed);
            setup()
        }
    }
}
