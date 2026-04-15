//! Physical left delim join.

use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::join::delim_join::{DelimJoin, DelimKey};
use crate::operator::scan::column_data_scan::MaterializedChunkCollection;
use crate::operator::state::OperatorSinkFinalizeInput;
use crate::operator::state::{
    GlobalSinkState, LocalSinkState, OperatorSinkCombineInput, OperatorSinkInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{SinkCombineResultType, SinkFinalizeType, SinkResultType};

#[derive(Debug)]
pub struct LeftDelimJoin {
    pub base: DelimJoin,
}

impl LeftDelimJoin {
    pub fn new(base: DelimJoin) -> Self {
        Self { base }
    }
}

#[derive(Debug)]
struct LeftDelimJoinGlobalSinkState {
    input_collection: Arc<MaterializedChunkCollection>,
    delim_collection: Arc<MaterializedChunkCollection>,
    seen_keys: Mutex<HashSet<DelimKey>>,
}

impl GlobalSinkState for LeftDelimJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct LeftDelimJoinLocalSinkState {
    duplicate_eliminated_executors: Vec<ExpressionExecutor>,
}

impl LocalSinkState for LeftDelimJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for LeftDelimJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::LeftDelimJoin
    }

    fn types(&self) -> &[paro_common::types::LogicalType] {
        &self.base.types
    }

    fn explain_params(&self) -> Vec<String> {
        self.base.explain_params()
    }

    fn children_count(&self) -> usize {
        2
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        match index {
            0 => Some(self.base.input.as_ref()),
            1 => Some(self.base.join.as_ref()),
            _ => None,
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        match index {
            0 => Some(self.base.input.clone()),
            1 => Some(self.base.join.clone()),
            _ => None,
        }
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn is_source(&self) -> bool {
        // This keeps the root query path from treating the delim join as a sink-only
        // DML root. `build_pipelines()` delegates the actual source to `base.join`.
        true
    }

    fn parallel_sink(&self) -> bool {
        false
    }

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let input_collection = Arc::new(MaterializedChunkCollection::new(
            self.base.input.types().to_vec(),
        ));
        let delim_collection = Arc::new(MaterializedChunkCollection::new(self.base.delim_types()));

        input_collection.reset()?;
        delim_collection.reset()?;

        self.base
            .cached_input_scan
            .set_collection(input_collection.clone())?;
        for scan in &self.base.delim_scans {
            scan.set_collection(delim_collection.clone())?;
        }

        Ok(Box::new(LeftDelimJoinGlobalSinkState {
            input_collection,
            delim_collection,
            seen_keys: Mutex::new(HashSet::new()),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(LeftDelimJoinLocalSinkState {
            duplicate_eliminated_executors: self.base.duplicate_eliminated_executors(),
        }))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<LeftDelimJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid left delim join global sink state".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<LeftDelimJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid left delim join local sink state".to_string())
            })?;

        gstate
            .input_collection
            .append(chunk.deep_copy_with_allocator(chunk.allocator().clone()))?;

        let delim_chunk = self.base.evaluate_delim_chunk(
            ctx,
            chunk,
            &mut lstate.duplicate_eliminated_executors,
        )?;
        let mut seen = gstate
            .seen_keys
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock delim seen-key set: {e}")))?;
        let unique_chunk = self.base.select_new_delim_rows(&delim_chunk, &mut seen);
        drop(seen);

        if unique_chunk.size() > 0 {
            gstate
                .delim_collection
                .append(unique_chunk.deep_copy_with_allocator(unique_chunk.allocator().clone()))?;
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        _input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, _input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        Ok(SinkFinalizeType::Ready)
    }

    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        let child_meta = meta_pipeline.create_child_meta_pipeline(
            current,
            self_arc.clone(),
            MetaPipelineType::Regular,
        );
        child_meta.build(&self.base.input, state);

        let dependency = child_meta.base_pipeline();
        for scan in &self.base.delim_scans {
            if let Some(dependency_id) = scan.dependency_id() {
                state.add_delim_join_dependency(dependency_id, dependency.clone());
            }
        }

        self.base
            .join
            .build_pipelines(&self.base.join, current, meta_pipeline, state);
    }

    fn sink_state_name(&self) -> &str {
        "LeftDelimJoinGlobalSinkState"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::LeftDelimJoin;
    use crate::execution_context::ExecutionContext;
    use crate::operator::join::delim_join::DelimJoin;
    use crate::operator::scan::column_data_scan::PhysicalColumnDataScan;
    use crate::operator::scan::column_data_scan::{
        ColumnDataScanBinding, MaterializedChunkCollection,
    };
    use crate::operator::scan::expression_scan::PhysicalExpressionScan;
    use crate::operator::state::{
        OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
    };
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_scheduler::task::InterruptState;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn build_test_left_delim_join() -> LeftDelimJoin {
        let input = Arc::new(PhysicalExpressionScan::new(
            vec![],
            vec![LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let cached_binding = Arc::new(ColumnDataScanBinding::new(None));
        let delim_binding = Arc::new(ColumnDataScanBinding::new(Some(77)));
        let cached_scan = Arc::new(PhysicalColumnDataScan::with_binding(
            vec![LogicalType::Integer],
            cached_binding.clone(),
        )) as Arc<dyn PhysicalOperator>;
        let delim_scan = Arc::new(PhysicalColumnDataScan::with_binding(
            vec![LogicalType::Integer],
            delim_binding.clone(),
        )) as Arc<dyn PhysicalOperator>;
        let join = Arc::new(crate::operator::join::cross_product::CrossProduct::new(
            cached_scan,
            delim_scan.clone(),
        )) as Arc<dyn PhysicalOperator>;

        LeftDelimJoin::new(DelimJoin::new(
            input,
            join,
            vec![Expression::Reference(ReferenceExpression {
                index: 0,
                return_type: LogicalType::Integer,
            })],
            cached_binding,
            vec![delim_binding],
            vec![LogicalType::Integer, LogicalType::Integer],
        ))
    }

    #[test]
    fn left_delim_join_materializes_input_and_unique_delim_keys() {
        let delim_join = build_test_left_delim_join();

        let session = test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = delim_join.get_global_sink_state(&ctx).expect("global sink");
        let mut lstate = delim_join.get_local_sink_state(&ctx).expect("local sink");
        let interrupt = InterruptState::default();
        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);

        let mut input_chunk = Chunk::initialize(&[LogicalType::Integer], 3);
        input_chunk.set_cardinality(3);
        input_chunk
            .column_mut(0)
            .unwrap()
            .set_value(0, &Value::Integer(1));
        input_chunk
            .column_mut(0)
            .unwrap()
            .set_value(1, &Value::Integer(1));
        input_chunk
            .column_mut(0)
            .unwrap()
            .set_value(2, &Value::Integer(2));

        delim_join
            .sink(&ctx, &input_chunk, &mut sink_input)
            .expect("sink");
        delim_join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("finalize");

        let cached_scan = delim_join
            .base
            .cached_input_scan
            .collection()
            .expect("cached input collection");
        assert_eq!(cached_scan.chunk_count().expect("count"), 1);

        let delim_collection = delim_join.base.delim_scans[0]
            .collection()
            .expect("delim collection");
        let snapshot = delim_collection.snapshot().expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].size(), 2);
        assert_eq!(
            snapshot[0].column(0).unwrap().get_value(0),
            Value::Integer(1)
        );
        assert_eq!(
            snapshot[0].column(0).unwrap().get_value(1),
            Value::Integer(2)
        );

        let scan_op = PhysicalColumnDataScan::with_binding(
            vec![LogicalType::Integer],
            delim_join.base.delim_scans[0].clone(),
        );
        let gsource = scan_op
            .get_global_source_state(&ctx, None)
            .expect("global source");
        let mut lsource = scan_op
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source");
        let mut source_input =
            OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut out = Chunk::initialize(&[LogicalType::Integer], 2);
        let source_result = scan_op
            .get_data(&ctx, &mut out, &mut source_input)
            .expect("get data");

        assert_eq!(
            source_result,
            crate::result_type::SourceResultType::Finished
        );
        assert_eq!(out.size(), 2);
        assert_eq!(out.column(0).unwrap().get_value(0), Value::Integer(1));
        assert_eq!(out.column(0).unwrap().get_value(1), Value::Integer(2));
    }

    #[test]
    fn materialized_chunk_collection_snapshot_clones_chunks() {
        let collection = MaterializedChunkCollection::new(vec![LogicalType::Integer]);
        let mut chunk = Chunk::initialize(&[LogicalType::Integer], 1);
        chunk.set_cardinality(1);
        chunk
            .column_mut(0)
            .unwrap()
            .set_value(0, &Value::Integer(99));
        collection.append(chunk).expect("append");

        let snapshot = collection.snapshot().expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0].column(0).unwrap().get_value(0),
            Value::Integer(99)
        );
    }

    #[test]
    fn left_delim_join_local_state_caches_duplicate_key_executors() {
        let delim_join = build_test_left_delim_join();

        let session = test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let lstate = delim_join.get_local_sink_state(&ctx).expect("local sink");
        let lstate = lstate
            .as_any()
            .downcast_ref::<super::LeftDelimJoinLocalSinkState>()
            .expect("left delim local sink state");

        assert_eq!(lstate.duplicate_eliminated_executors.len(), 1);
    }
}
