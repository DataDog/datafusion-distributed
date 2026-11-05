use std::mem::{size_of, size_of_val};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, FileScanTaskColumnStats, TableScan};
use iceberg::spec::{Datum, PrimitiveLiteral};

use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
/// Shared by task-local sources so their descriptors and reservation have one owner.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    tasks: Vec<FileScanTask>,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    /// Summarize selected file work; predicates and deletes can reduce output rows.
    pub(crate) fn statistics(&self, schema: &SchemaRef, may_filter_rows: bool) -> Statistics {
        let bytes = self.tasks.iter().fold(0usize, |bytes, task| {
            bytes.saturating_add(task.length() as usize)
        });
        let rows = self.tasks.iter().try_fold(0usize, |rows, task| {
            rows.checked_add(usize::try_from(task.record_count()?).ok()?)
        });
        let mut statistics = Statistics::new_unknown(schema);
        statistics.num_rows = match rows {
            Some(0) if self.tasks.is_empty() => Precision::Exact(0),
            Some(rows) if !may_filter_rows => Precision::Exact(rows),
            Some(rows) => Precision::Inexact(rows),
            None => Precision::Absent,
        };
        statistics.total_byte_size = if may_filter_rows {
            Precision::Inexact(bytes)
        } else {
            Precision::Exact(bytes)
        };
        statistics
    }

    fn new(context: &TaskContext) -> Self {
        Self {
            tasks: Vec::new(),
            _reservation: MemoryConsumer::new("Iceberg file planning")
                .register(context.memory_pool()),
        }
    }

    fn push(&mut self, task: FileScanTask) -> Result<()> {
        // Include owned paths/delete references and allow room for Vec capacity.
        self._reservation
            .try_grow(retained_size(&task).saturating_mul(2))?;
        self.tasks.push(task);
        Ok(())
    }

    /// Drops the per-file column metrics retained for statistics, which are not needed to
    /// read the files and would otherwise be serialized along with each task.
    pub(crate) fn drop_column_stats(&mut self) {
        for task in &mut self.tasks {
            let before = retained_size(task);
            drop(task.take_column_stats());
            let after = retained_size(task);
            self._reservation
                .shrink(before.saturating_sub(after).saturating_mul(2));
        }
    }

    pub(crate) fn from_tasks(tasks: Vec<FileScanTask>, context: &TaskContext) -> Result<Self> {
        let mut planned = Self::new(context);
        for task in tasks {
            planned.push(task)?;
        }
        Ok(planned)
    }

    pub(crate) async fn collect(scan: TableScan, context: &TaskContext) -> Result<Self> {
        let mut planned = Self::new(context);
        let mut stream = scan.plan_files().await.map_err(df_err)?;
        while let Some(task) = stream.try_next().await.map_err(df_err)? {
            planned.push(task)?;
        }
        Ok(planned)
    }
}

fn retained_size(task: &FileScanTask) -> usize {
    let mut bytes = size_of::<FileScanTask>()
        + task.data_file_path().len()
        + size_of_val(task.project_field_ids())
        + size_of_val(task.deletes())
        + task.key_metadata().map_or(0, <[u8]>::len);
    if let Some(stats) = task.column_stats() {
        bytes = bytes.saturating_add(column_stats_size(stats));
    }
    for delete in task.deletes() {
        bytes = bytes
            .saturating_add(delete.file_path().len())
            .saturating_add(delete.referenced_data_file().map_or(0, str::len))
            .saturating_add(delete.equality_ids().map_or(0, size_of_val))
            .saturating_add(delete.key_metadata().map_or(0, <[u8]>::len));
    }
    bytes
}

/// Approximate heap size of the metrics a task retains for column statistics.
fn column_stats_size(stats: &FileScanTaskColumnStats) -> usize {
    let counts = [
        stats.column_sizes(),
        stats.value_counts(),
        stats.null_value_counts(),
        stats.nan_value_counts(),
    ]
    .iter()
    .map(|counts| counts.len() * size_of::<(i32, u64)>())
    .sum::<usize>();
    let bounds = [stats.lower_bounds(), stats.upper_bounds()]
        .iter()
        .flat_map(|bounds| bounds.values())
        .map(|bound| {
            size_of::<(i32, Datum)>()
                + match bound.literal() {
                    PrimitiveLiteral::String(value) => value.len(),
                    PrimitiveLiteral::Binary(value) => value.len(),
                    _ => 0,
                }
        })
        .sum::<usize>();
    size_of::<FileScanTaskColumnStats>() + counts + bounds
}
