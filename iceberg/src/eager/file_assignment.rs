use std::cmp::Reverse;
use std::collections::BinaryHeap;

use datafusion::common::{Result, exec_err};
use iceberg::scan::FileScanTask;

/// Assign largest files to the least-loaded distributed task, cycling its local
/// partitions. File identity breaks size ties independently of discovery order.
/// The upstream source executes these explicit groups without regrouping them.
pub(crate) fn assign_files<'a>(
    files: impl Iterator<Item = &'a FileScanTask>,
    task_count: usize,
    partitions_per_task: usize,
) -> Result<Vec<Vec<Vec<FileScanTask>>>> {
    if task_count
        .checked_mul(partitions_per_task)
        .is_none_or(|count| count == 0)
    {
        return exec_err!("Invalid Iceberg assignment partition count");
    }
    let files: Vec<_> = files.collect();
    let mut indices: Vec<_> = (0..files.len()).collect();
    indices.sort_unstable_by_key(|&index| {
        let file = &files[index];
        (
            Reverse(file.length()),
            file.data_file_path(),
            file.start(),
            index,
        )
    });
    // Prefer fewer files, then task index, when byte loads tie.
    let mut loads: BinaryHeap<_> = (0..task_count)
        .map(|task| Reverse((0_u128, 0_usize, task)))
        .collect();
    let mut assignments = vec![vec![Vec::new(); partitions_per_task]; task_count];
    for index in indices {
        let mut least_loaded = loads.peek_mut().expect("validated nonzero task count");
        let Reverse((bytes, count, task)) = &mut *least_loaded;
        assignments[*task][*count % partitions_per_task].push(files[index].clone());
        *bytes += u128::from(files[index].length());
        *count += 1;
    }
    Ok(assignments)
}
