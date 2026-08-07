use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::PlanProperties;
use std::sync::Arc;

pub(super) fn scale_partitioning_props(
    props: &Arc<PlanProperties>,
    f: impl FnOnce(usize) -> usize,
) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f),
        props.emission_type,
        props.boundedness,
    ))
}

pub(super) fn scale_partitioning(
    partitioning: &Partitioning,
    f: impl FnOnce(usize) -> usize,
) -> Partitioning {
    let partition_count = partitioning.partition_count();
    let scaled_count = f(partition_count);
    match partitioning {
        Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(scaled_count),
        Partitioning::Hash(hash, _) => Partitioning::Hash(hash.clone(), scaled_count),
        Partitioning::Range(range) if scaled_count == partition_count => {
            Partitioning::Range(range.clone())
        }
        Partitioning::Range(_) => {
            // Changing the partition count invalidates the existing split points.
            Partitioning::UnknownPartitioning(scaled_count)
        }
        Partitioning::UnknownPartitioning(_) => Partitioning::UnknownPartitioning(scaled_count),
    }
}
