#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::{Result, assert_contains, internal_err};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
    };
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::test_utils::worker_plan_setup::wrap_in_sampler;
    use datafusion_distributed::{
        DistributedExt, Worker, WorkerQueryContext, WorkerSessionBuilder,
    };
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::TryStreamExt;
    use prost::Message;
    use std::cell::Cell;
    use std::fmt::Formatter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    thread_local! {
        static PLAN_SETUP_ACTIVE: Cell<bool> = const { Cell::new(false) };
    }

    #[tokio::test]
    async fn plan_setup_wraps_decode_hooks_and_sampler_kickoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let checks = Arc::new(PlanSetupChecks::default());
        let builder = ScopedSessionBuilder {
            checks: Arc::clone(&checks),
        };
        let mut ctx = start_configured_in_memory_context(3, builder, {
            let checks = Arc::clone(&checks);
            move |mut worker| {
                add_sampler_check_hook(&mut worker, Arc::clone(&checks));
                add_scope_check_hook(&mut worker, Arc::clone(&checks));
                worker
            }
        })
        .await;
        ctx.set_distributed_user_codec(ScopedPassThroughExecCodec { checks: None });

        let _left_file = register_input_table("plan_setup_left", &ctx).await?;
        let plan = ctx
            .sql("SELECT id FROM plan_setup_left WHERE id > 1 ORDER BY id")
            .await?
            .create_physical_plan()
            .await?;
        let plan = plan
            .transform_up(|plan| {
                if plan.children().is_empty() {
                    return Ok(Transformed::yes(Arc::new(ScopedPassThroughExec::new(plan))));
                }
                Ok(Transformed::no(plan))
            })?
            .data;

        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(checks.wrapper_calls.load(Ordering::Relaxed) > 0);
        assert!(checks.decode_checks.load(Ordering::Relaxed) > 0);
        assert_eq!(
            checks.hook_checks.load(Ordering::Relaxed),
            checks.wrapper_calls.load(Ordering::Relaxed) * 2,
        );
        assert!(checks.sampler_kickoff_checks.load(Ordering::Relaxed) > 0);

        Ok(())
    }

    #[tokio::test]
    async fn plan_setup_wrapper_errors_propagate_to_query() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut ctx =
            start_configured_in_memory_context(3, RejectingSessionBuilder, |worker| worker).await;
        ctx.set_distributed_user_codec(ScopedPassThroughExecCodec { checks: None });
        let _left_file = register_input_table("plan_setup_left", &ctx).await?;
        let plan = ctx
            .sql("SELECT id FROM plan_setup_left WHERE id > 1 ORDER BY id")
            .await?
            .create_physical_plan()
            .await?;
        let plan = plan
            .transform_up(|plan| {
                if plan.children().is_empty() {
                    return Ok(Transformed::yes(Arc::new(ScopedPassThroughExec::new(plan))));
                }
                Ok(Transformed::no(plan))
            })?
            .data;

        let error = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await
            .expect_err("worker plan setup should fail");

        assert_contains!(error.to_string(), "worker plan setup rejected");
        Ok(())
    }

    #[derive(Debug, Default)]
    struct PlanSetupChecks {
        wrapper_calls: AtomicUsize,
        decode_checks: AtomicUsize,
        hook_checks: AtomicUsize,
        sampler_kickoff_checks: AtomicUsize,
    }

    #[derive(Clone)]
    struct ScopedSessionBuilder {
        checks: Arc<PlanSetupChecks>,
    }

    #[async_trait]
    impl WorkerSessionBuilder for ScopedSessionBuilder {
        async fn build_session_state(
            &self,
            ctx: WorkerQueryContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_distributed_user_codec(ScopedPassThroughExecCodec {
                    checks: Some(Arc::clone(&self.checks)),
                })
                .build())
        }

        fn run_plan_setup(
            &self,
            _session_state: &SessionState,
            setup: &mut dyn FnMut() -> Result<(), DataFusionError>,
        ) -> Result<(), DataFusionError> {
            self.checks.wrapper_calls.fetch_add(1, Ordering::Relaxed);
            PLAN_SETUP_ACTIVE.with(|active| {
                let previously_active = active.replace(true);
                let result = setup();
                active.set(previously_active);
                result
            })
        }
    }

    #[derive(Clone, Copy)]
    struct RejectingSessionBuilder;

    #[async_trait]
    impl WorkerSessionBuilder for RejectingSessionBuilder {
        async fn build_session_state(
            &self,
            ctx: WorkerQueryContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_distributed_user_codec(ScopedPassThroughExecCodec { checks: None })
                .build())
        }

        fn run_plan_setup(
            &self,
            _session_state: &SessionState,
            _setup: &mut dyn FnMut() -> Result<(), DataFusionError>,
        ) -> Result<(), DataFusionError> {
            internal_err!("worker plan setup rejected")
        }
    }

    fn add_sampler_check_hook(worker: &mut Worker, checks: Arc<PlanSetupChecks>) {
        worker.add_on_plan_hook(move |plan, _session_config| {
            check_plan_setup_active("first plan hook")?;
            checks.hook_checks.fetch_add(1, Ordering::Relaxed);
            let checked_plan: Arc<dyn ExecutionPlan> =
                Arc::new(SamplerKickoffCheckExec::new(plan, Arc::clone(&checks)));
            Ok(wrap_in_sampler(checked_plan))
        });
    }

    fn add_scope_check_hook(worker: &mut Worker, checks: Arc<PlanSetupChecks>) {
        worker.add_on_plan_hook(move |plan, _session_config| {
            check_plan_setup_active("second plan hook")?;
            checks.hook_checks.fetch_add(1, Ordering::Relaxed);
            Ok(plan)
        });
    }

    fn check_plan_setup_active(operation: &str) -> Result<()> {
        PLAN_SETUP_ACTIVE.with(|active| {
            if active.get() {
                Ok(())
            } else {
                internal_err!("{operation} ran outside WorkerSessionBuilder::run_plan_setup")
            }
        })
    }

    async fn register_input_table(
        table_name: &str,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<std::path::PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        register_temp_parquet_table(table_name, schema, vec![batch], ctx).await
    }

    #[derive(Debug)]
    struct ScopedPassThroughExec {
        properties: Arc<PlanProperties>,
        child: Arc<dyn ExecutionPlan>,
    }

    impl ScopedPassThroughExec {
        fn new(child: Arc<dyn ExecutionPlan>) -> Self {
            Self {
                properties: Arc::clone(child.properties()),
                child,
            }
        }
    }

    impl DisplayAs for ScopedPassThroughExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "ScopedPassThroughExec")
        }
    }

    impl ExecutionPlan for ScopedPassThroughExec {
        fn name(&self) -> &str {
            "ScopedPassThroughExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let [child] = children.as_slice() else {
                return internal_err!("ScopedPassThroughExec should have exactly one child");
            };
            Ok(Arc::new(Self::new(Arc::clone(child))))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.child.execute(partition, context)
        }
    }

    #[derive(Debug)]
    struct ScopedPassThroughExecCodec {
        checks: Option<Arc<PlanSetupChecks>>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct ScopedPassThroughExecProto {}

    impl PhysicalExtensionCodec for ScopedPassThroughExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let _node = ScopedPassThroughExecProto::decode(buf)
                .map_err(|error| proto_error(format!("{error}")))?;
            let [input] = inputs else {
                return Err(proto_error(format!(
                    "ScopedPassThroughExec expects one child, got {}",
                    inputs.len()
                )));
            };

            if let Some(checks) = &self.checks {
                check_plan_setup_active("physical extension decode")?;
                checks.decode_checks.fetch_add(1, Ordering::Relaxed);
            }

            Ok(Arc::new(ScopedPassThroughExec::new(Arc::clone(input))))
        }

        fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
            if node.downcast_ref::<ScopedPassThroughExec>().is_none() {
                return Err(proto_error(format!(
                    "expected ScopedPassThroughExec, got {}",
                    node.name()
                )));
            }
            ScopedPassThroughExecProto {}
                .encode(buf)
                .map_err(|error| proto_error(format!("{error}")))
        }
    }

    #[derive(Debug)]
    struct SamplerKickoffCheckExec {
        properties: Arc<PlanProperties>,
        child: Arc<dyn ExecutionPlan>,
        checks: Arc<PlanSetupChecks>,
    }

    impl SamplerKickoffCheckExec {
        fn new(child: Arc<dyn ExecutionPlan>, checks: Arc<PlanSetupChecks>) -> Self {
            Self {
                properties: Arc::clone(child.properties()),
                child,
                checks,
            }
        }
    }

    impl DisplayAs for SamplerKickoffCheckExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "SamplerKickoffCheckExec")
        }
    }

    impl ExecutionPlan for SamplerKickoffCheckExec {
        fn name(&self) -> &str {
            "SamplerKickoffCheckExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let [child] = children.as_slice() else {
                return internal_err!("SamplerKickoffCheckExec should have exactly one child");
            };
            Ok(Arc::new(Self::new(
                Arc::clone(child),
                Arc::clone(&self.checks),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            check_plan_setup_active("sampler kickoff")?;
            self.checks
                .sampler_kickoff_checks
                .fetch_add(1, Ordering::Relaxed);
            self.child.execute(partition, context)
        }
    }
}
