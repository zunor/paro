// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical right delim join.

use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::join::delim_join::{DelimJoin, DelimKey};
use crate::operator::join::hash_join::operator::HashJoin;
use crate::operator::join::nested_loop_join::NestedLoopJoin;
use crate::operator::scan::column_data_scan::MaterializedChunkCollection;
use crate::operator::state::{
    GlobalSinkState, LocalSinkState, OperatorSinkCombineInput, OperatorSinkFinalizeInput,
    OperatorSinkInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{SinkCombineResultType, SinkFinalizeType, SinkResultType};

#[derive(Debug)]
pub struct RightDelimJoin {
    pub base: DelimJoin,
}

impl RightDelimJoin {
    pub fn new(base: DelimJoin) -> Self {
        Self { base }
    }

    fn build_wrapped_join_pipelines(
        &self,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        if let Some(hash_join) = self.base.join.as_any().downcast_ref::<HashJoin>() {
            hash_join.build_pipelines_base(&self.base.join, current, meta_pipeline, state, false);
            return;
        }

        if let Some(nested_loop_join) = self.base.join.as_any().downcast_ref::<NestedLoopJoin>() {
            nested_loop_join.join.build_join_pipelines(
                &self.base.join,
                current,
                meta_pipeline,
                state,
                false,
            );
            return;
        }

        panic!(
            "RightDelimJoin only supports wrapped hash/nested-loop comparison joins, got {:?}",
            self.base.join.operator_type()
        );
    }
}

#[derive(Debug)]
struct RightDelimJoinGlobalSinkState {
    delim_collection: Arc<MaterializedChunkCollection>,
    seen_keys: Mutex<HashSet<DelimKey>>,
}

impl GlobalSinkState for RightDelimJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "RightDelimJoinGlobalSinkState"
    }
}

#[derive(Debug)]
struct RightDelimJoinLocalSinkState {
    join_local_state: Box<dyn LocalSinkState>,
    duplicate_eliminated_executors: Vec<ExpressionExecutor>,
}

impl LocalSinkState for RightDelimJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for RightDelimJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::RightDelimJoin
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
        true
    }

    fn parallel_sink(&self) -> bool {
        false
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let delim_collection = Arc::new(MaterializedChunkCollection::new(self.base.delim_types()));
        delim_collection.reset()?;

        for scan in &self.base.delim_scans {
            scan.set_collection(delim_collection.clone())?;
        }

        let join_sink_state: Arc<dyn GlobalSinkState> =
            Arc::from(self.base.join.get_global_sink_state(ctx)?);
        self.base.join.set_sink_state(join_sink_state);

        Ok(Box::new(RightDelimJoinGlobalSinkState {
            delim_collection,
            seen_keys: Mutex::new(HashSet::new()),
        }))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(RightDelimJoinLocalSinkState {
            join_local_state: self.base.join.get_local_sink_state(ctx)?,
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
            .downcast_ref::<RightDelimJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid right delim join global sink state".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RightDelimJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid right delim join local sink state".to_string())
            })?;

        let join_sink_state = self.base.join.sink_state().ok_or_else(|| {
            paro_error::internal("Right delim join requires wrapped join sink state".to_string())
        })?;
        let mut join_input = OperatorSinkInput::new(
            join_sink_state.as_ref(),
            lstate.join_local_state.as_mut(),
            input.interrupt_state,
        );
        self.base.join.sink(ctx, chunk, &mut join_input)?;

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
        ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RightDelimJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid right delim join local sink state".to_string())
            })?;

        let join_sink_state = self.base.join.sink_state().ok_or_else(|| {
            paro_error::internal("Right delim join requires wrapped join sink state".to_string())
        })?;
        let mut join_input = OperatorSinkCombineInput::new(
            join_sink_state.as_ref(),
            lstate.join_local_state.as_mut(),
            input.interrupt_state,
        );
        self.base.join.combine(ctx, &mut join_input)?;

        Ok(SinkCombineResultType::Finished)
    }

    fn prepare_finalize(&self, _gstate: &dyn GlobalSinkState) -> Result<()> {
        let join_sink_state = self.base.join.sink_state().ok_or_else(|| {
            paro_error::internal("Right delim join requires wrapped join sink state".to_string())
        })?;
        self.base.join.prepare_finalize(join_sink_state.as_ref())
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let join_sink_state = self.base.join.sink_state().ok_or_else(|| {
            paro_error::internal("Right delim join requires wrapped join sink state".to_string())
        })?;
        self.base.join.finalize(&OperatorSinkFinalizeInput::new(
            join_sink_state.as_ref(),
            input.interrupt_state,
        ))
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

        self.build_wrapped_join_pipelines(current, meta_pipeline, state);
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
    use super::RightDelimJoin;
    use crate::execution_context::ExecutionContext;
    use crate::operator::join::delim_join::DelimJoin;
    use crate::operator::join::hash_join::operator::HashJoin;
    use crate::operator::scan::column_data_scan::{ColumnDataScanBinding, PhysicalColumnDataScan};
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::scan::expression_scan::PhysicalExpressionScan;
    use crate::operator::state::{
        OperatorSinkCombineInput, OperatorSinkFinalizeInput, OperatorSinkInput,
    };
    use crate::operator::PhysicalOperator;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
    use crate::thread_context::ThreadContext;
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_planner::operator::join::{JoinCondition, JoinType};
    use paro_scheduler::task::InterruptState;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn build_test_right_delim_join() -> RightDelimJoin {
        let input = Arc::new(PhysicalExpressionScan::new(
            vec![],
            vec![LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let delim_binding = Arc::new(ColumnDataScanBinding::new(Some(77)));
        let left_scan = Arc::new(PhysicalColumnDataScan::with_binding(
            vec![LogicalType::Integer],
            delim_binding.clone(),
        )) as Arc<dyn PhysicalOperator>;
        let dummy_right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let join = Arc::new(
            HashJoin::new(
                left_scan,
                dummy_right,
                JoinType::Inner,
                vec![JoinCondition::new(
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                    paro_planner::operator::join::JoinComparisonType::Equal,
                )],
                vec![],
                vec![],
            )
            .expect("hash join should be created"),
        ) as Arc<dyn PhysicalOperator>;

        RightDelimJoin::new(DelimJoin::new(
            input,
            join,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            Arc::new(ColumnDataScanBinding::new(None)),
            vec![delim_binding],
            vec![LogicalType::Integer, LogicalType::Integer],
        ))
    }

    #[test]
    fn right_delim_join_materializes_unique_delim_keys_and_reuses_wrapped_join_sink() {
        let right_delim_join = build_test_right_delim_join();

        let session = test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = right_delim_join
            .get_global_sink_state(&ctx)
            .expect("global sink");
        let mut lstate = right_delim_join
            .get_local_sink_state(&ctx)
            .expect("local sink");
        let interrupt = InterruptState::default();

        let mut chunk = Chunk::initialize(&[LogicalType::Integer], 3);
        chunk.set_cardinality(3);
        chunk
            .column_mut(0)
            .unwrap()
            .set_value(0, &Value::Integer(1));
        chunk
            .column_mut(0)
            .unwrap()
            .set_value(1, &Value::Integer(1));
        chunk
            .column_mut(0)
            .unwrap()
            .set_value(2, &Value::Integer(2));

        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        right_delim_join
            .sink(&ctx, &chunk, &mut sink_input)
            .expect("sink");

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        right_delim_join
            .combine(&ctx, &mut combine_input)
            .expect("combine");
        right_delim_join
            .prepare_finalize(gstate.as_ref())
            .expect("prepare finalize");
        right_delim_join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("finalize");

        assert!(right_delim_join.base.join.sink_state().is_some());

        let delim_collection = right_delim_join.base.delim_scans[0]
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
    }

    #[test]
    fn right_delim_join_registers_dependency_before_wrapped_join_build() {
        let right_delim_join = Arc::new(build_test_right_delim_join()) as Arc<dyn PhysicalOperator>;
        let meta_pipeline = MetaPipeline::new(None, MetaPipelineType::Regular);
        let current = meta_pipeline.base_pipeline();
        let mut state = PipelineBuildState::new();

        right_delim_join.build_pipelines(&right_delim_join, &current, &meta_pipeline, &mut state);

        let dependency = state
            .get_delim_join_dependency(77)
            .cloned()
            .expect("dependency should be registered");
        let deps = current.get_dependencies();
        assert!(deps.iter().any(|dep| Arc::ptr_eq(dep, &dependency)));
        assert!(deps.iter().all(|dep| !Arc::ptr_eq(dep, &current)));
        assert_eq!(
            current
                .source()
                .expect("current pipeline source")
                .operator_type(),
            crate::operator_type::PhysicalOperatorType::ColumnDataScan
        );
    }

    #[test]
    fn right_delim_join_local_state_caches_duplicate_key_executors() {
        let right_delim_join = build_test_right_delim_join();

        let session = test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let lstate = right_delim_join
            .get_local_sink_state(&ctx)
            .expect("local sink");
        let lstate = lstate
            .as_any()
            .downcast_ref::<super::RightDelimJoinLocalSinkState>()
            .expect("right delim local sink state");

        assert_eq!(lstate.duplicate_eliminated_executors.len(), 1);
    }
}
