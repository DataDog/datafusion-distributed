use super::MaterializedStore;
use super::manifest::checksum;
use crate::common::{TreeNodeExt, now_ns, task_ctx_with_extension};
use crate::work_unit_feed::WorkUnitFeedRegistry;
use crate::{
    CoordinatorToWorkerMsg, DistributedTaskContext, DistributedWorkUnitFeedContext, MaybeEncoded,
    WorkUnitBatch, WorkUnitMsg,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashSet, Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

/// Sealed once per logical task, retained across every execution attempt.
#[derive(Clone)]
pub(crate) struct InputJournal {
    prefix: String,
    items: Vec<(usize, u64)>,
}

#[derive(Clone, PartialEq, Message)]
struct JournalItem {
    #[prost(bytes, tag = "1")]
    id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    partition: u64,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
}

impl InputJournal {
    pub(crate) async fn seal(
        plan: &Arc<dyn ExecutionPlan>,
        task: DistributedTaskContext,
        context: &Arc<TaskContext>,
        store: &MaterializedStore,
        prefix: String,
    ) -> Result<Self> {
        let registry = context
            .session_config()
            .get_extension::<WorkUnitFeedRegistry>()
            .unwrap_or_default();
        let mut feeds = Vec::new();
        let mut declared = HashSet::new();
        plan.apply_with_dt_ctx(task, |plan, task| {
            if let Some(feed) = registry.get_work_unit_feed(plan) {
                if !declared.insert(feed.id()) {
                    return exec_err!("Duplicate work-unit feed in a materialized task");
                }
                let count = plan.properties().partitioning.partition_count();
                let context = Arc::new(task_ctx_with_extension(
                    context,
                    DistributedWorkUnitFeedContext {
                        fan_out_tasks: task.task_count,
                    },
                ));
                for partition in 0..count {
                    feeds.push((
                        feed.id(),
                        partition,
                        feed.feed(task.task_index * count + partition, Arc::clone(&context))?,
                    ));
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let mut journal = Self {
            prefix,
            items: Vec::new(),
        };
        let mut total_bytes = 0usize;
        for (id, partition, mut feed) in feeds {
            while let Some(unit) = feed.try_next().await? {
                let item = JournalItem {
                    id: id.as_bytes().to_vec(),
                    partition: partition as u64,
                    payload: unit.encode_to_bytes(),
                }
                .encode_to_vec();
                total_bytes = total_bytes.saturating_add(item.len());
                // Existing worker control channels are unbounded. Bound a task's entire
                // journal until the protocol supports acknowledged/backpressured feeds.
                if total_bytes > store.max_segment_bytes {
                    return exec_err!("Work-unit journal exceeds per-task byte limit");
                }
                let integrity = (item.len(), checksum(&item));
                store
                    .put(
                        &format!("{}/{}", journal.prefix, journal.items.len()),
                        item.into(),
                    )
                    .await?;
                journal.items.push(integrity);
            }
        }
        Ok(journal)
    }

    pub(crate) async fn replay(
        &self,
        store: &MaterializedStore,
        tx: &UnboundedSender<CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        for (index, &(length, expected_checksum)) in self.items.iter().enumerate() {
            let bytes = store.store.get(&format!("{}/{index}", self.prefix)).await?;
            if bytes.len() != length || checksum(&bytes) != expected_checksum {
                return exec_err!("Replay journal integrity mismatch");
            }
            let item = JournalItem::decode(bytes)
                .map_err(|error| exec_datafusion_err!("Invalid replay journal: {error}"))?;
            let now = now_ns();
            let msg = CoordinatorToWorkerMsg::WorkUnitBatch(WorkUnitBatch {
                batch: vec![WorkUnitMsg {
                    id: Uuid::from_slice(&item.id)
                        .map_err(|error| exec_datafusion_err!("Invalid replay feed ID: {error}"))?,
                    partition: item.partition as usize,
                    body: MaybeEncoded::Encoded(item.payload),
                    created_timestamp_unix_nanos: now,
                    sent_timestamp_unix_nanos: now,
                    received_timestamp_unix_nanos: 0,
                    processed_timestamp_unix_nanos: 0,
                }],
            });
            if tx.send(msg).is_err() {
                return exec_err!("Worker closed while replaying inputs");
            }
        }
        if tx.send(CoordinatorToWorkerMsg::WorkUnitEos).is_err() {
            return exec_err!("Worker closed before end of inputs");
        }
        Ok(())
    }
}
