use std::num::NonZeroUsize;

use datafusion::common::{Result, config_datafusion_err, not_impl_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedConfig,
};
use iceberg_datafusion::read::{EagerIcebergDataSource, IcebergDataSource};
use iceberg_datafusion::write::{IcebergCommitExec, IcebergWriteExec};

use crate::FeedTaskSource;

/// Estimates scan parallelism from selected file bytes, bounded by available file work.
/// Lazy scans continue to use snapshot totals.
pub fn iceberg_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    if ev.plan.is::<IcebergWriteExec>() || ev.plan.is::<IcebergCommitExec>() {
        return Some(not_impl_err!(
            "Distributed Iceberg writes are not supported; use an upstream catalog provider in a session without the distributed planner"
        ));
    }
    let node = ev.plan.downcast_ref::<DataSourceExec>()?.data_source();
    // A reader limit changes output estimates, not the selected file work.
    let statistics = if let Some(source) = node.downcast_ref::<EagerIcebergDataSource>() {
        source.unlimited_statistics()
    } else {
        node.downcast_ref::<IcebergDataSource<FeedTaskSource>>()?
            .unlimited_statistics()
    };
    let total_bytes = *statistics.total_byte_size.get_value()?;
    let config = DistributedConfig::from_session_config(ev.session_config).ok()?;

    Some(
        calculate_task_count(
            total_bytes,
            config.file_scan_config_bytes_per_partition,
            ev.session_config.target_partitions(),
        )
        .map(|count| {
            let count = node
                .downcast_ref::<EagerIcebergDataSource>()
                .map_or(count, |source| {
                    count.min(source.planned_files().count() as f64)
                });
            DesiredTaskCountEventResponse::soft(count)
        }),
    )
}

fn calculate_task_count(
    total_bytes: usize,
    bytes_per_partition: usize,
    target_partitions: usize,
) -> Result<f64> {
    let bytes_per_partition = non_zero_divisor(bytes_per_partition, "bytes per partition")?;
    let target_partitions = non_zero_divisor(target_partitions, "target partitions")?;

    Ok(total_bytes as f64 / bytes_per_partition.get() as f64 / target_partitions.get() as f64)
}

fn non_zero_divisor(value: usize, name: &str) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| config_datafusion_err!("Iceberg {name} must be greater than zero"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_fractional_and_boundary_task_counts() {
        assert_eq!(task_count(24, 4, 3), 2.0);
        assert_eq!(task_count(30, 4, 3), 2.5);
        assert_eq!(task_count(usize::MAX, usize::MAX, 2), 0.5);
        assert_eq!(task_count(usize::MAX, 1, 1), usize::MAX as f64);
    }

    #[test]
    fn zero_bytes_require_zero_tasks() {
        assert_eq!(task_count(0, 1, 1), 0.0);
    }

    #[test]
    fn rejects_zero_divisors() {
        assert!(calculate_task_count(1, 0, 1).is_err());
        assert!(calculate_task_count(1, 1, 0).is_err());
    }

    fn task_count(total_bytes: usize, bytes_per_partition: usize, target_partitions: usize) -> f64 {
        calculate_task_count(total_bytes, bytes_per_partition, target_partitions)
            .expect("test task count should be valid")
    }
}
