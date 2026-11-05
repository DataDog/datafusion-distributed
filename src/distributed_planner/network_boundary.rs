use super::stage_envelope::rewrite_stage_head;
use crate::execution_plans::SamplerExec;
use crate::{
    BroadcastExec, MaybeEncoded, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec,
    Stage,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn NetworkBoundary>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;

    /// Defines what head node should the producer stage feeding this [NetworkBoundary]
    /// implementation have. This information is used during planning an executing for ensuring
    /// the head of a stage has the appropriate shape for consumption. Returns an error when that
    /// shape cannot be constructed from the boundary's partitioning.
    fn producer_head(&self, consumer_tasks: usize) -> Result<ProducerHead>;
}

/// Defines what shape should the head node of a stage have upon getting executed. Depending
/// on the [NetworkBoundary] implementation, the stage below should have different head nodes.
#[derive(Clone)]
pub enum ProducerHead {
    /// No specific head node is necessary.
    None,
    /// The head node should be a [BroadcastExec].
    BroadcastExec { output_partitions: usize },
    /// The head node should be a [RepartitionExec].
    RepartitionExec {
        partitioning: MaybeEncoded<Partitioning>,
        /// Whether the producer must preserve its input's output ordering.
        preserve_order: bool,
    },
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        if let Some(node) = self.downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}

impl ProducerHead {
    pub(crate) fn insert_for_session(
        self,
        input: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite_stage_head(input, config, |head| self.insert(head))
    }

    pub(crate) fn insert_sampler_for_session(
        input: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite_stage_head(input, config, Self::insert_sampler)
    }

    pub(crate) fn ensure_decoded(self, schema: SchemaRef, ctx: &TaskContext) -> Result<Self> {
        Ok(match self {
            Self::RepartitionExec {
                partitioning,
                preserve_order,
            } => Self::RepartitionExec {
                partitioning: MaybeEncoded::Decoded(partitioning.decode(schema, ctx)?),
                preserve_order,
            },
            v => v,
        })
    }

    /// Ensures the head of the provided plan complies with the passed [ProducerHead] definition. This
    /// can be called both during planning and lazily at runtime.
    pub(crate) fn insert(self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let input = if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            Arc::clone(r_exec.input())
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            Arc::clone(b_exec.input())
        } else {
            input
        };
        let plan = match self {
            ProducerHead::None => input,
            ProducerHead::BroadcastExec { output_partitions } => {
                let partitions = input.output_partitioning().partition_count();
                Arc::new(BroadcastExec::new(input, output_partitions / partitions))
            }
            ProducerHead::RepartitionExec {
                partitioning,
                preserve_order,
            } => {
                let repartition = RepartitionExec::try_new(input, partitioning.try_decoded()?)?;
                Arc::new(if preserve_order {
                    repartition.with_preserve_order()
                } else {
                    repartition
                })
            }
        };
        Ok(plan)
    }

    /// Injects a [SamplerExec] right below a [RepartitionExec] or [BroadcastExec].
    pub(crate) fn insert_sampler(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            let child = Arc::clone(r_exec.input());
            input.replace_children(
                vec![Arc::new(SamplerExec::new(child))],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            let child = Arc::clone(b_exec.input());
            input.replace_children(
                vec![Arc::new(SamplerExec::new(child))],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        } else {
            Ok(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register_distributed_stage_envelope;
    use crate::test_utils::stage_envelope::TestStageEnvelope;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::assert_contains;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn repartition_is_resized_inside_registered_envelope() -> Result<()> {
        let mut config = SessionConfig::new();
        register_distributed_stage_envelope::<TestStageEnvelope>(&mut config);
        let original = Arc::new(RepartitionExec::try_new(
            input(),
            Partitioning::RoundRobinBatch(4),
        )?) as _;
        let wrapped = Arc::new(TestStageEnvelope::new(original, Default::default())) as _;
        let head = ProducerHead::RepartitionExec {
            partitioning: MaybeEncoded::Decoded(Partitioning::RoundRobinBatch(8)),
            preserve_order: false,
        };

        let result = head.insert_for_session(wrapped, &config)?;

        assert!(result.is::<TestStageEnvelope>());
        assert_eq!(result.output_partitioning().partition_count(), 8);
        let exchange = result.children()[0]
            .downcast_ref::<RepartitionExec>()
            .unwrap();
        assert!(exchange.input().is::<EmptyExec>());
        assert_eq!(exchange.properties().partitioning.partition_count(), 8);
        Ok(())
    }

    #[test]
    fn broadcast_is_resized_inside_registered_envelope() -> Result<()> {
        let mut config = SessionConfig::new();
        register_distributed_stage_envelope::<TestStageEnvelope>(&mut config);
        let original = Arc::new(BroadcastExec::new(input(), 1)) as _;
        let wrapped = Arc::new(TestStageEnvelope::new(original, Default::default())) as _;

        let result = ProducerHead::BroadcastExec {
            output_partitions: 8,
        }
        .insert_for_session(wrapped, &config)?;

        assert!(result.is::<TestStageEnvelope>());
        assert_eq!(result.output_partitioning().partition_count(), 8);
        let exchange = result.children()[0]
            .downcast_ref::<BroadcastExec>()
            .unwrap();
        assert!(exchange.input().is::<EmptyExec>());
        assert_eq!(exchange.properties().partitioning.partition_count(), 8);
        Ok(())
    }

    #[test]
    fn sampler_stays_below_exchange_inside_envelope() -> Result<()> {
        let mut config = SessionConfig::new();
        register_distributed_stage_envelope::<TestStageEnvelope>(&mut config);
        let original = Arc::new(RepartitionExec::try_new(
            input(),
            Partitioning::RoundRobinBatch(4),
        )?) as _;
        let wrapped = Arc::new(TestStageEnvelope::new(original, Default::default())) as _;

        let result = ProducerHead::insert_sampler_for_session(wrapped, &config)?;

        assert!(result.is::<TestStageEnvelope>());
        let exchange = result.children()[0]
            .downcast_ref::<RepartitionExec>()
            .unwrap();
        assert!(exchange.input().is::<SamplerExec>());
        assert_eq!(result.output_partitioning().partition_count(), 4);
        Ok(())
    }

    #[test]
    fn unregistered_wrapper_retains_existing_head_insertion_behavior() -> Result<()> {
        let wrapped = Arc::new(TestStageEnvelope::new(input(), Default::default())) as _;

        let result = ProducerHead::BroadcastExec {
            output_partitions: 8,
        }
        .insert_for_session(wrapped, &SessionConfig::new())?;

        assert!(result.is::<BroadcastExec>());
        assert!(result.children()[0].is::<TestStageEnvelope>());
        Ok(())
    }

    #[test]
    fn registered_envelope_must_have_one_input() {
        let mut config = SessionConfig::new();
        register_distributed_stage_envelope::<EmptyExec>(&mut config);

        let error = ProducerHead::None
            .insert_for_session(input(), &config)
            .unwrap_err();

        assert_contains!(error.to_string(), "Expected exactly 1 children, got 0");
    }

    #[test]
    fn registered_envelope_must_preserve_partition_count() {
        let mut config = SessionConfig::new();
        register_distributed_stage_envelope::<CoalescePartitionsExec>(&mut config);
        let opaque = Arc::new(CoalescePartitionsExec::new(input())) as _;

        let error = ProducerHead::None
            .insert_for_session(opaque, &config)
            .unwrap_err();

        assert_contains!(
            error.to_string(),
            "must preserve its input schema and partition count"
        );
    }

    fn input() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(2))
    }
}
