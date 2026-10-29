use crate::materialized::MaterializedStore;
use crate::materialized::manifest::{PartitionManifest, read_segment};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct MaterializedPartition {
    #[prost(message, repeated, tag = "1")]
    pub(crate) producers: Vec<PartitionManifest>,
}

/// Reads a fixed accepted input snapshot; never switches attempts while running.
#[derive(Debug)]
pub(crate) struct MaterializedReaderExec {
    pub(crate) partitions: Vec<MaterializedPartition>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl MaterializedReaderExec {
    pub(crate) fn try_new(
        partitions: Vec<MaterializedPartition>,
        properties: Arc<PlanProperties>,
    ) -> Result<Self> {
        if partitions.len() != properties.partitioning.partition_count() {
            return exec_err!("Materialized partition count does not match plan properties");
        }
        Ok(Self {
            partitions,
            properties,
        })
    }
}

impl DisplayAs for MaterializedReaderExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializedReaderExec: partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for MaterializedReaderExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "MaterializedReaderExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("Materialized reader cannot accept children");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(partition_manifest) = self.partitions.get(partition) else {
            return exec_err!("Materialized partition {partition} does not exist");
        };
        let store = MaterializedStore::from_ctx(&context)?;
        let mut streams = Vec::new();
        for producer in &partition_manifest.producers {
            let schema = self.schema();
            let store = Arc::clone(&store);
            let stream = futures::stream::iter(producer.segments.clone()).then(move |segment| {
                let store = Arc::clone(&store);
                let schema = Arc::clone(&schema);
                async move { read_segment(&store, &segment, schema).await }
            });
            streams.push(
                Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream))
                    as SendableRecordBatchStream,
            );
        }
        if streams.len() > 1
            && let Some(ordering) = self.properties.output_ordering()
        {
            let reservation = MemoryConsumer::new("MaterializedReaderExec")
                .register(&context.runtime_env().memory_pool);
            return StreamingMergeBuilder::new()
                .with_streams(streams)
                .with_schema(self.schema())
                .with_expressions(ordering)
                .with_metrics(BaselineMetrics::new(
                    &ExecutionPlanMetricsSet::new(),
                    partition,
                ))
                .with_batch_size(context.session_config().batch_size())
                .with_reservation(reservation)
                .build();
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(streams),
        )))
    }
}
