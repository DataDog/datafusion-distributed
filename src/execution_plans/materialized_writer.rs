use crate::common::require_one_child;
use crate::materialized::MaterializedStore;
use crate::materialized::manifest::{AttemptManifest, write_partition};
use datafusion::arrow::array::BinaryArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use prost::Message;
use std::fmt::Formatter;
use std::sync::Arc;

/// Executes all input partitions and returns only a sealed manifest.
/// The coordinator, not this node, decides whether to accept that manifest.
#[derive(Debug)]
pub(crate) struct MaterializedWriterExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) prefix: String,
    properties: Arc<PlanProperties>,
}

impl MaterializedWriterExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, prefix: String) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "manifest",
            DataType::Binary,
            false,
        )]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            prefix,
            properties,
        }
    }
}

impl DisplayAs for MaterializedWriterExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializedWriterExec: partitions={}",
            self.input.properties().partitioning.partition_count()
        )
    }
}

impl ExecutionPlan for MaterializedWriterExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "MaterializedWriterExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            require_one_child(&children)?,
            self.prefix.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("Materialized writer has only one result partition");
        }
        let store = MaterializedStore::from_ctx(&context)?;
        let mut streams = Vec::new();
        for partition in 0..self.input.properties().partitioning.partition_count() {
            streams.push((
                partition,
                self.input.execute(partition, Arc::clone(&context))?,
            ));
        }
        let prefix = self.prefix.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let future = async move {
            // All repartition outputs must be polled concurrently to avoid deadlock.
            let writes = FuturesUnordered::new();
            for (partition, stream) in streams {
                let store = Arc::clone(&store);
                let prefix = format!("{prefix}/{partition}");
                writes.push(async move {
                    Ok::<_, DataFusionError>((
                        partition,
                        write_partition(&store, &prefix, stream).await?,
                    ))
                });
            }
            let mut partitions = writes.try_collect::<Vec<_>>().await?;
            partitions.sort_by_key(|(partition, _)| *partition);
            let manifest = AttemptManifest {
                partitions: partitions
                    .into_iter()
                    .map(|(_, manifest)| manifest)
                    .collect(),
            };
            let bytes = manifest.encode_to_vec();
            if bytes.len() > store.max_segment_bytes {
                return exec_err!("Materialized manifest exceeds segment limit");
            }
            RecordBatch::try_new(
                output_schema,
                vec![Arc::new(BinaryArray::from(vec![bytes.as_slice()]))],
            )
            .map_err(Into::into)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(future),
        )))
    }
}
