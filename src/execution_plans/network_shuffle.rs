use crate::common::require_one_child;
use crate::execution_plans::common::scale_partitioning;
use crate::stage::Stage;
use crate::worker::WorkerConnectionPool;
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::TaskKey;
use crate::worker::generated::worker::flight_app_metadata;
use crate::{DistributedTaskContext, ExecutionTask, NetworkBoundary};
use dashmap::DashMap;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use uuid::Uuid;

/// [ExecutionPlan] implementation that shuffles data across the network in a distributed context.
///
/// The easiest way of thinking about this node is as a plan [RepartitionExec] node that is
/// capable of fanning out the different produced partitions to different tasks.
/// This allows redistributing data across different tasks in different stages, so that different
/// physical machines can make progress on different non-overlapping sets of data.
///
/// This node allows fanning out of data from N tasks to M tasks, with N and M being arbitrary non-zero
/// positive numbers. Here are some examples of how data can be shuffled in different scenarios:
///
/// # 1 to many
///
/// ```text
/// ┌───────────────────────────┐  ┌───────────────────────────┐ ┌───────────────────────────┐     ■
/// │    NetworkShuffleExec     │  │    NetworkShuffleExec     │ │    NetworkShuffleExec     │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └┬─┬┬─┬┬─┬──────────────────┘  └─────────┬─┬┬─┬┬─┬─────────┘ └──────────────────┬─┬┬─┬┬─┬┘  Stage N+1
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
///  └─┘└─┘└─┘                               └─┘└─┘└─┘                              └─┘└─┘└─┘      │
///   ▲  ▲  ▲                                 ▲  ▲  ▲                                ▲  ▲  ▲       ■
///   └──┴──┴────────────────────────┬──┬──┐  │  │  │  ┌──┬──┬───────────────────────┴──┴──┘
///                                  │  │  │  │  │  │  │  │  │                                     ■
///                                 ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐                                    │
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐                                Stage N
///                                │      RepartitionExec      │                                   │
///                                │         (task 1)          │                                   │
///                                └───────────────────────────┘                                   ■
/// ```
///
/// # many to 1
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkShuffleExec     │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲                                    ■
///   ┌──┬──┬──┬──┬──┬──┬──┬──┬─────┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴────┬──┬──┬──┬──┬──┬──┬──┬──┐
///   │  │  │  │  │  │  │  │  │      │  │  │  │  │  │  │  │  │     │  │  │  │  │  │  │  │  │       ■
///  ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐   ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      │
///  │1││2││3││4││5││6││7││8││9│    │1││2││3││4││5││6││7││8││9│   │1││2││3││4││5││6││7││8││9│      │
/// ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐ ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// # many to many
///
/// ```text
///                    ┌───────────────────────────┐  ┌───────────────────────────┐                ■
///                    │    NetworkShuffleExec     │  │    NetworkShuffleExec     │                │
///                    │         (task 1)          │  │         (task 2)          │                │
///                    └┬─┬┬─┬┬─┬┬─┬───────────────┘  └───────────────┬─┬┬─┬┬─┬┬─┬┘             Stage N+1
///                     │1││2││3││4│                                  │5││6││7││8│                 │
///                     └─┘└─┘└─┘└─┘                                  └─┘└─┘└─┘└─┘                 │
///                     ▲▲▲▲▲▲▲▲▲▲▲▲                                  ▲▲▲▲▲▲▲▲▲▲▲▲                 ■
///     ┌──┬──┬──┬──┬──┬┴┴┼┴┴┼┴┴┴┴┴┴───┬──┬──┬──┬──┬──┬──┬──┬────────┬┴┴┼┴┴┼┴┴┼┴┴┼──┬──┬──┐
///     │  │  │  │  │  │  │  │         │  │  │  │  │  │  │  │        │  │  │  │  │  │  │  │        ■
///    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       │
///    │1││2││3││4││5││6││7││8│       │1││2││3││4││5││6││7││8│      │1││2││3││4││5││6││7││8│       │
/// ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐ ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkShuffleExec] has two implications:
///
/// - Each task in Stage N+1 gathers data from all tasks in Stage N
/// - The total number of partitions across all tasks in Stage N+1 is equal to the
///   number of partitions in a single task in Stage N. (e.g. (1,2,3,4)+(5,6,7,8) = (1,2,3,4,5,6,7,8) )
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    /// metrics_collection is used to collect metrics from child tasks. It is initially
    /// instantiated as an empty [DashMap] (see `try_decode` in `distributed_codec.rs`).
    /// Metrics are populated here via [NetworkCoalesceExec::execute].
    ///
    /// An instance may receive metrics for 0 to N child tasks, where N is the number of tasks in
    /// the stage it is reading from. This is because, by convention, the Worker sends metrics for
    /// a task to the last NetworkCoalesceExec to read from it, which may or may not be this
    /// instance.
    pub(crate) metrics_collection: Arc<DashMap<TaskKey, Vec<pb::MetricsSet>>>,
    /// Whether to use optimized shuffle partitioning (1:1 task-partition mapping)
    pub(crate) optimize_shuffle_partitioning: bool,
}

impl NetworkShuffleExec {
    /// Builds a new [NetworkShuffleExec] in "Pending" state.
    ///
    /// Typically, the `input` to this
    /// node is a [RepartitionExec] with a [Partitioning::Hash] partition scheme.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        task_count: usize,
        input_task_count: usize,
        optimize_shuffle_partitioning: bool,
    ) -> Result<Self, DataFusionError> {
        if !matches!(input.output_partitioning(), Partitioning::Hash(_, _)) {
            return plan_err!("NetworkShuffleExec input must be hash partitioned");
        }

        let transformed = Arc::clone(&input).transform_down(|plan| {
            if let Some(r_exe) = plan.as_any().downcast_ref::<RepartitionExec>() {
                let scaled = if optimize_shuffle_partitioning {
                    Arc::new(RepartitionExec::try_new(
                        require_one_child(r_exe.children())?,
                        r_exe.partitioning().clone(),
                    )?)
                } else {
                    Arc::new(RepartitionExec::try_new(
                        require_one_child(r_exe.children())?,
                        scale_partitioning(r_exe.partitioning(), |p| p * task_count),
                    )?)
                };
                Ok(Transformed::new(scaled, true, TreeNodeRecursion::Stop))
            } else if matches!(plan.output_partitioning(), Partitioning::Hash(_, _)) {
                Ok(Transformed::no(plan))
            } else {
                return plan_err!(
                    "NetworkShuffleExec input must be hash partitioned, but {} is not",
                    plan.name()
                );
            }
        })?;

        let properties = if optimize_shuffle_partitioning {
            let mut props = input.properties().clone();
            props.partitioning = Partitioning::UnknownPartitioning(1);
            props
        } else {
            input.properties().clone()
        };

        Ok(Self {
            input_stage: Stage {
                query_id,
                num,
                plan: Some(transformed.data),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            worker_connections: WorkerConnectionPool::new(input_task_count),
            properties,
            metrics_collection: Default::default(),
            optimize_shuffle_partitioning,
        })
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
        if self.optimize_shuffle_partitioning {
            write!(
                f,
                "[Stage {stage}] => NetworkShuffleExec: output_partitions={partitions}, input_tasks={input_tasks}, optimized=true",
            )
        } else {
            write!(
                f,
                "[Stage {stage}] => NetworkShuffleExec: output_partitions={partitions}, input_tasks={input_tasks}",
            )
        }
    }
}

impl ExecutionPlan for NetworkShuffleExec {
    fn name(&self) -> &str {
        "NetworkShuffleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            Some(v) => vec![v],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut self_clone = self.as_ref().clone();
        self_clone.input_stage.plan = Some(require_one_child(children)?);
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_context = DistributedTaskContext::from_ctx(&context);

        let (partition_offset, partition_range) = if self.optimize_shuffle_partitioning {
            let partition_id = task_context.task_index;
            (partition_id, partition_id..(partition_id + 1))
        } else {
            let partition_count = self.properties.partitioning.partition_count();
            let off = partition_count * task_context.task_index;
            (off, off..(off + partition_count))
        };

        let mut streams = Vec::with_capacity(self.input_stage.tasks.len());
        for input_task_index in 0..self.input_stage.tasks.len() {
            let worker_connection = self.worker_connections.get_or_init_worker_connection(
                &self.input_stage,
                partition_range.clone(),
                input_task_index,
                &context,
            )?;

            let metrics_collection = Arc::clone(&self.metrics_collection);

            let actual_partition = if self.optimize_shuffle_partitioning {
                partition_offset
            } else {
                partition_offset + partition
            };

            let stream = worker_connection.stream_partition(actual_partition, move |meta| {
                if let Some(flight_app_metadata::Content::MetricsCollection(m)) = meta.content {
                    for task_metrics in m.tasks {
                        if let Some(task_key) = task_metrics.task_key {
                            metrics_collection.insert(task_key, task_metrics.metrics);
                        };
                    }
                }
            })?;
            streams.push(stream);
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(streams),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use uuid::Uuid;

    #[test]
    fn test_optimized_mode_sets_single_partition() -> datafusion::common::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let part = Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 8);

        let repartition = Arc::new(RepartitionExec::try_new(
            Arc::new(EmptyExec::new(schema.clone())),
            part.clone(),
        )?);

        let optimized = NetworkShuffleExec::try_new(
            repartition.clone(),
            Uuid::new_v4(),
            1,
            8,
            3,
            true,
        )?;

        assert_eq!(optimized.properties.partitioning.partition_count(), 1);
        assert!(optimized.optimize_shuffle_partitioning);

        let legacy = NetworkShuffleExec::try_new(
            repartition.clone(),
            Uuid::new_v4(),
            1,
            2,
            3,
            false,
        )?;

        assert_eq!(legacy.properties.partitioning.partition_count(), 8);
        assert!(!legacy.optimize_shuffle_partitioning);

        Ok(())
    }

    #[test]
    fn test_optimized_mode_no_partition_inflation() -> datafusion::common::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let part = Partitioning::Hash(vec![Arc::new(Column::new("b", 0))], 4);

        let repartition = Arc::new(RepartitionExec::try_new(
            Arc::new(EmptyExec::new(schema.clone())),
            part.clone(),
        )?);

        let optimized = NetworkShuffleExec::try_new(
            repartition.clone(),
            Uuid::new_v4(),
            1,
            4,
            2,
            true,
        )?;

        let input_plan = optimized.input_stage.plan.as_ref().unwrap();
        assert_eq!(input_plan.output_partitioning().partition_count(), 4);

        let legacy = NetworkShuffleExec::try_new(
            repartition,
            Uuid::new_v4(),
            1,
            4,
            2,
            false,
        )?;

        let input_plan = legacy.input_stage.plan.as_ref().unwrap();
        assert_eq!(input_plan.output_partitioning().partition_count(), 16);

        Ok(())
    }
}
