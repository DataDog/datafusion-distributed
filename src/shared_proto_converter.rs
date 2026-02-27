use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr_with_converter;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct SharedDeserializerState {
    expr_cache: HashMap<u64, Arc<dyn PhysicalExpr>>,
    dynamic_filter_cache: HashMap<u64, Arc<dyn PhysicalExpr>>,
}

/// Converter that preserves dynamic filter identity across multiple independent
/// plan (de)serialization calls when reused.
#[derive(Debug)]
pub(crate) struct SharedPhysicalProtoConverter {
    session_id: u64,
    state: Mutex<SharedDeserializerState>,
}

impl SharedPhysicalProtoConverter {
    pub(crate) fn new() -> Self {
        Self {
            session_id: rand::random(),
            state: Mutex::new(SharedDeserializerState::default()),
        }
    }

    fn hash(&self, ptr: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.session_id.hash(&mut hasher);
        ptr.hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        hasher.finish()
    }
}

impl PhysicalProtoConverterExtension for SharedPhysicalProtoConverter {
    fn proto_to_execution_plan(
        &self,
        ctx: &TaskContext,
        codec: &dyn PhysicalExtensionCodec,
        proto: &PhysicalPlanNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        proto.try_into_physical_plan_with_converter(ctx, codec, self)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        PhysicalPlanNode::try_from_physical_plan_with_converter(Arc::clone(plan), codec, self)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        ctx: &TaskContext,
        input_schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if let Some(expr_id) = proto.expr_id {
            let state = self
                .state
                .lock()
                .map_err(|_| internal_datafusion_err!("shared proto converter state poisoned"))?;
            if let Some(expr) = state.expr_cache.get(&expr_id) {
                return Ok(Arc::clone(expr));
            }
        }

        let mut expr = parse_physical_expr_with_converter(proto, ctx, input_schema, codec, self)?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| internal_datafusion_err!("shared proto converter state poisoned"))?;

        if let Some(dynamic_filter_id) = proto.dynamic_filter_inner_id {
            if let Some(cached_filter) = state.dynamic_filter_cache.get(&dynamic_filter_id) {
                let Some(cached_dynamic_filter) = cached_filter
                    .as_any()
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                else {
                    return internal_err!(
                        "dynamic filter cache returned a non DynamicFilterPhysicalExpr"
                    );
                };

                let dynamic_filter_expr = (expr as Arc<dyn Any + Send + Sync>)
                    .downcast::<DynamicFilterPhysicalExpr>()
                    .map_err(|_| {
                        internal_datafusion_err!(
                            "dynamic_filter_inner_id set but expression is not DynamicFilterPhysicalExpr"
                        )
                    })?;
                expr = Arc::new(dynamic_filter_expr.new_from_source(cached_dynamic_filter)?)
                    as Arc<dyn PhysicalExpr>;
            } else {
                state
                    .dynamic_filter_cache
                    .insert(dynamic_filter_id, Arc::clone(&expr));
            }
        }

        if let Some(expr_id) = proto.expr_id {
            state.expr_cache.insert(expr_id, Arc::clone(&expr));
        }

        Ok(expr)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let mut proto = serialize_physical_expr_with_converter(expr, codec, self)?;
        if let Some(dynamic_filter) = expr.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
            proto.dynamic_filter_inner_id = Some(self.hash(dynamic_filter.inner_id()));
        }

        let ptr = Arc::as_ptr(expr) as *const () as usize as u64;
        proto.expr_id = Some(self.hash(ptr));
        Ok(proto)
    }
}
