use super::distributed_codec::{
    EquivalenceClassProto, parse_equivalence_properties, serialize_equivalence_group,
};
use crate::common::require_one_child;
use crate::execution_plans::{
    MaterializedPartition, MaterializedReaderExec, MaterializedWriterExec,
};
use crate::materialized::MaterializedStore;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_sort_exprs, parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::{
    serialize_partitioning, serialize_physical_sort_exprs,
};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use prost::Message;
use std::sync::Arc;

const MAGIC: &[u8] = b"DFDM1";

#[derive(Debug)]
pub(crate) struct MaterializedCodec;

impl PhysicalExtensionCodec for MaterializedCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(buf) = buf.strip_prefix(MAGIC) else {
            return plan_err!("Not a materialized plan");
        };
        MaterializedStore::from_ctx(ctx)?;
        let proto = MaterializedProto::decode(buf)
            .map_err(|e| plan_datafusion_err!("Invalid materialized plan: {e}"))?;
        let proto = match proto.node {
            Some(MaterializedNode::Writer(writer)) => {
                return Ok(Arc::new(MaterializedWriterExec::new(
                    require_one_child(inputs)?,
                    writer.prefix,
                )));
            }
            Some(MaterializedNode::Reader(reader)) => reader,
            None => return plan_err!("Missing materialized plan node"),
        };
        if !inputs.is_empty() {
            return plan_err!("Materialized reader cannot have children");
        }
        let schema: Schema = proto
            .schema
            .as_ref()
            .ok_or_else(|| plan_datafusion_err!("Missing materialized schema"))?
            .try_into()?;
        let schema = Arc::new(schema);
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            converter,
        )?
        .ok_or_else(|| plan_datafusion_err!("Missing materialized partitioning"))?;
        let mut equivalence = parse_equivalence_properties(
            proto.equivalence_classes,
            Arc::clone(&schema),
            &decode_ctx,
            converter,
        )?;
        let ordering = parse_physical_sort_exprs(&proto.ordering, &decode_ctx, &schema, converter)?;
        if !ordering.is_empty() {
            equivalence.add_orderings([ordering]);
        }
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(MaterializedReaderExec::try_new(
            proto.partitions,
            properties,
        )?))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let proto = if let Some(writer) = node.downcast_ref::<MaterializedWriterExec>() {
            MaterializedNode::Writer(WriterProto {
                prefix: writer.prefix.clone(),
            })
        } else if let Some(reader) = node.downcast_ref::<MaterializedReaderExec>() {
            let ordering = reader
                .properties
                .output_ordering()
                .map(|ordering| {
                    serialize_physical_sort_exprs(ordering.iter().cloned(), self, converter)
                })
                .transpose()?
                .unwrap_or_default();
            MaterializedNode::Reader(ReaderProto {
                schema: Some(reader.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    &reader.properties.partitioning,
                    self,
                    converter,
                )?),
                ordering,
                equivalence_classes: serialize_equivalence_group(
                    reader.properties.equivalence_properties(),
                    self,
                    converter,
                )?,
                partitions: reader.partitions.clone(),
            })
        } else {
            return plan_err!("Not a materialized plan");
        };
        buf.extend_from_slice(MAGIC);
        MaterializedProto { node: Some(proto) }
            .encode(buf)
            .map_err(|e| plan_datafusion_err!("Cannot encode materialized plan: {e}"))
    }
}

#[derive(Clone, PartialEq, Message)]
struct MaterializedProto {
    #[prost(oneof = "MaterializedNode", tags = "1, 2")]
    node: Option<MaterializedNode>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum MaterializedNode {
    #[prost(message, tag = "1")]
    Writer(WriterProto),
    #[prost(message, tag = "2")]
    Reader(ReaderProto),
}

#[derive(Clone, PartialEq, Message)]
struct WriterProto {
    #[prost(string, tag = "1")]
    prefix: String,
}

#[derive(Clone, PartialEq, Message)]
struct ReaderProto {
    #[prost(message, optional, tag = "2")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "3")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, repeated, tag = "4")]
    ordering: Vec<protobuf::PhysicalSortExprNode>,
    #[prost(message, repeated, tag = "5")]
    equivalence_classes: Vec<EquivalenceClassProto>,
    #[prost(message, repeated, tag = "6")]
    partitions: Vec<MaterializedPartition>,
}
