use crate::common::require_one_child;
use datafusion::common::{HashSet, Result, exec_err};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use datafusion::prelude::SessionConfig;
use std::any::TypeId;
use std::sync::Arc;

#[derive(Clone, Default)]
struct StageEnvelopes(HashSet<TypeId>);

/// Registers a transparent unary stage envelope for producer-head preparation.
///
/// Register the same concrete type on coordinator and worker session configurations. This
/// registration is local and is not serialized. The envelope must forward its child's schema,
/// partitioning, ordering and execution properties, preserve partition indices during execution,
/// and recompute properties when replacing its child. Runtime repartition, broadcast and sampler
/// preparation can then happen beneath the envelope without duplicating exchanges.
///
/// Apply envelopes after distributed planning or in a worker plan rewrite handler. Do not
/// register operators that transform rows or their execution properties. Custom codecs must
/// still be registered separately when envelopes cross the network.
pub fn register_distributed_stage_envelope<T: ExecutionPlan>(config: &mut SessionConfig) {
    let mut envelopes = config
        .get_extension::<StageEnvelopes>()
        .as_deref()
        .cloned()
        .unwrap_or_default();
    envelopes.0.insert(TypeId::of::<T>());
    config.set_extension(Arc::new(envelopes));
}

/// Rewrites the producer head beneath explicitly registered, transparent stage envelopes.
pub(crate) fn rewrite_stage_head(
    mut plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
    rewrite: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(registry) = config.get_extension::<StageEnvelopes>() else {
        return rewrite(plan);
    };
    let mut envelopes = vec![];
    while registry.0.contains(&plan.as_ref().type_id()) {
        let child = require_one_child(plan.children())?;
        validate_envelope(&plan, &child)?;
        envelopes.push(plan);
        plan = child;
    }
    let mut head = rewrite(plan)?;
    for envelope in envelopes.into_iter().rev() {
        let rebuilt = envelope.replace_children(
            vec![Arc::clone(&head)],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        validate_envelope(&rebuilt, &head)?;
        head = rebuilt;
    }
    Ok(head)
}

fn validate_envelope(
    envelope: &Arc<dyn ExecutionPlan>,
    input: &Arc<dyn ExecutionPlan>,
) -> Result<()> {
    if envelope.schema() != input.schema()
        || envelope.output_partitioning().partition_count()
            != input.output_partitioning().partition_count()
    {
        return exec_err!(
            "Registered stage envelope {} must preserve its input schema and partition count",
            envelope.name()
        );
    }
    Ok(())
}
