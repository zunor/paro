// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical cross product (CROSS JOIN): Cartesian product of left and right inputs.
//!
//! The sink materializes the right (build) side; the operator streams the left and
//! emits pairs.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::memory_runtime::{RetainedChunkVec, SharedRetainedObject};
use crate::operator::state::OperatorSinkFinalizeInput;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, LocalSinkState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorState,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorResultType, SinkCombineResultType, SinkFinalizeType, SinkResultType,
};

/// Physical Cross Product operator.
///
/// Produces the Cartesian product of two input relations.
/// The right child is materialized (build side), then for each row from
/// the left child (probe side), all combinations with right rows are emitted.
pub struct CrossProduct {
    /// Left (probe) child operator.
    pub left: Arc<dyn PhysicalOperator>,
    /// Right (build) child operator.
    pub right: Arc<dyn PhysicalOperator>,
    /// Output types (left types + right types).
    pub types: Vec<LogicalType>,
    /// Number of columns from left side.
    pub left_col_count: usize,
    /// Number of columns from right side.
    pub right_col_count: usize,
    /// Stored build-side sink state for the probe pipeline.
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl CrossProduct {
    /// Create a new cross product operator.
    pub fn new(left: Arc<dyn PhysicalOperator>, right: Arc<dyn PhysicalOperator>) -> Self {
        let left_types = left.types().to_vec();
        let right_types = right.types().to_vec();
        let left_col_count = left_types.len();
        let right_col_count = right_types.len();

        let mut types = left_types;
        types.extend(right_types);

        Self {
            left,
            right,
            types,
            left_col_count,
            right_col_count,
            sink_state: Mutex::new(None),
        }
    }

    /// Emit a single row combining probe and build rows.
    fn emit_row(
        &self,
        chunk: &mut Chunk,
        output_idx: usize,
        probe_chunk: &Chunk,
        probe_row: usize,
        build_chunk: &Chunk,
        build_row: usize,
    ) -> Result<()> {
        for col_idx in 0..self.left_col_count {
            let src_col = probe_chunk
                .column(col_idx)
                .ok_or_else(|| paro_error::internal("Probe column not found".to_string()))?;
            let dst_col = chunk
                .column_mut(col_idx)
                .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;
            let value = src_col.get_value(probe_row);
            dst_col.set_value(output_idx, &value);
        }

        for col_idx in 0..self.right_col_count {
            let src_col = build_chunk
                .column(col_idx)
                .ok_or_else(|| paro_error::internal("Build column not found".to_string()))?;
            let dst_col = chunk
                .column_mut(self.left_col_count + col_idx)
                .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;
            let value = src_col.get_value(build_row);
            dst_col.set_value(output_idx, &value);
        }

        Ok(())
    }
}

impl fmt::Debug for CrossProduct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrossProduct")
            .field("left_col_count", &self.left_col_count)
            .field("right_col_count", &self.right_col_count)
            .field("types", &self.types)
            .finish()
    }
}

// ========== States ==========

/// Global sink state for cross product (materialized build side).
#[derive(Debug)]
struct CrossProductGlobalSinkState {
    retained_object: Arc<SharedRetainedObject>,
    /// Materialized chunks from the right (build) side.
    rhs_chunks: Arc<Mutex<RetainedChunkVec>>,
    /// Total row count from right side.
    rhs_row_count: Mutex<usize>,
}

impl GlobalSinkState for CrossProductGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl CrossProductGlobalSinkState {
    fn new(ctx: &ExecutionContext) -> Self {
        let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
        let retained_object = Arc::new(SharedRetainedObject::new(
            "cross_product_rhs",
            owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
        ));
        let retained_owner: Arc<dyn MemoryOwner> = retained_object.clone();
        let memory = MemoryAccountingContext::from_owner(
            retained_owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        Self {
            retained_object,
            rhs_chunks: Arc::new(Mutex::new(RetainedChunkVec::new(memory))),
            rhs_row_count: Mutex::new(0),
        }
    }

    fn row_count(&self) -> Result<usize> {
        self.rhs_row_count
            .lock()
            .map(|count| *count)
            .map_err(|e| paro_error::internal(e.to_string()))
    }
}

/// Local sink state for cross product (build / materialize).
#[derive(Debug)]
struct CrossProductLocalSinkState {
    /// Local chunks collected before combining.
    local_chunks: RetainedChunkVec,
}

impl LocalSinkState for CrossProductLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Thread-local operator state for cross product probe execution.
#[derive(Debug)]
struct CrossProductOperatorState {
    /// Reference to materialized right side chunks.
    rhs_chunks: Arc<Mutex<RetainedChunkVec>>,
    /// Total row count from right side.
    rhs_row_count: usize,
    /// Current row index in probe chunk.
    current_probe_row: usize,
    /// Current chunk index in RHS.
    current_rhs_chunk_idx: usize,
    /// Current row index in current RHS chunk.
    current_rhs_row: usize,
}

impl Default for CrossProductOperatorState {
    fn default() -> Self {
        Self {
            rhs_chunks: Arc::new(Mutex::new(RetainedChunkVec::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ))),
            rhs_row_count: 0,
            current_probe_row: 0,
            current_rhs_chunk_idx: 0,
            current_rhs_row: 0,
        }
    }
}

impl CrossProductOperatorState {
    fn new(rhs_chunks: Arc<Mutex<RetainedChunkVec>>, rhs_row_count: usize) -> Self {
        Self {
            rhs_chunks,
            rhs_row_count,
            ..Default::default()
        }
    }

    fn reset(&mut self) {
        self.current_probe_row = 0;
        self.current_rhs_chunk_idx = 0;
        self.current_rhs_row = 0;
    }
}

impl OperatorState for CrossProductOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn cross_product_local_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for CrossProduct {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CrossProduct
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        vec!["Join Type: CROSS".to_string()]
    }

    fn children_count(&self) -> usize {
        2
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        match index {
            0 => Some(self.left.as_ref()),
            1 => Some(self.right.as_ref()),
            _ => None,
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        match index {
            0 => Some(self.left.clone()),
            1 => Some(self.right.clone()),
            _ => None,
        }
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut sink_state = self.sink_state.lock().unwrap();
        *sink_state = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    fn clear_sink_state(&self) {
        let mut sink_state = self.sink_state.lock().unwrap();
        *sink_state = None;
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        let sink = self
            .sink_state()
            .ok_or_else(|| paro_error::internal("CrossProduct requires sink state".to_string()))?;
        let gstate = sink
            .as_any()
            .downcast_ref::<CrossProductGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid cross product sink state".to_string()))?;
        gstate.retained_object.rebind_reclaimer();
        Ok(Box::new(CrossProductOperatorState::new(
            gstate.rhs_chunks.clone(),
            gstate.row_count()?,
        )))
    }

    // --- Sink (build) ---

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(CrossProductGlobalSinkState::new(ctx)))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(CrossProductLocalSinkState {
            local_chunks: RetainedChunkVec::new(cross_product_local_memory_context(ctx)),
        }))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<CrossProductLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        if chunk.size() > 0 {
            lstate
                .local_chunks
                .push(chunk.try_deep_copy(chunk.allocator().clone())?)?;
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CrossProductGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<CrossProductLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        let mut g_chunks = gstate
            .rhs_chunks
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        let mut g_count = gstate
            .rhs_row_count
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        *g_count += lstate.local_chunks.row_count();
        g_chunks.append_from(&mut lstate.local_chunks)?;

        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let _gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CrossProductGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        Ok(SinkFinalizeType::Ready)
    }

    // --- Operator (probe) ---

    fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<OperatorResultType> {
        let state = state
            .as_any_mut()
            .downcast_mut::<CrossProductOperatorState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid cross product operator state".to_string())
            })?;

        if input.size() == 0 || state.rhs_row_count == 0 {
            state.reset();
            chunk.set_cardinality(0);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let mut output_row_count = 0;
        let capacity = chunk.capacity();

        {
            let rhs_chunks = state
                .rhs_chunks
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let rhs_chunks = rhs_chunks.as_slice();

            while output_row_count < capacity {
                if state.current_rhs_chunk_idx >= rhs_chunks.len() {
                    state.current_probe_row += 1;
                    state.current_rhs_chunk_idx = 0;
                    state.current_rhs_row = 0;

                    if state.current_probe_row >= input.size() {
                        break;
                    }
                    continue;
                }

                let rhs_chunk = &rhs_chunks[state.current_rhs_chunk_idx];
                self.emit_row(
                    chunk,
                    output_row_count,
                    input,
                    state.current_probe_row,
                    rhs_chunk,
                    state.current_rhs_row,
                )?;
                output_row_count += 1;

                state.current_rhs_row += 1;
                if state.current_rhs_row >= rhs_chunk.size() {
                    state.current_rhs_row = 0;
                    state.current_rhs_chunk_idx += 1;
                }
            }
        }

        chunk.set_cardinality(output_row_count);

        if state.current_probe_row >= input.size() {
            state.reset();
            Ok(OperatorResultType::NeedMoreInput)
        } else {
            Ok(OperatorResultType::HaveMoreOutput)
        }
    }

    fn build_pipelines(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        let build_meta = meta_pipeline.create_child_meta_pipeline(
            current,
            op.clone(),
            MetaPipelineType::JoinBuild,
        );
        build_meta.build(&self.right, state);

        self.left
            .build_pipelines(&self.left, current, meta_pipeline, state);
        current.add_operator(op.clone());
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
    use super::{CrossProduct, CrossProductGlobalSinkState};
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use paro_context::StatementContext;
    use paro_scheduler::task::InterruptState;

    use crate::execution_context::ExecutionContext;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::{
        EmptyGlobalOperatorState, GlobalSinkState, OperatorSinkFinalizeInput,
    };
    use crate::operator::PhysicalOperator;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
    use crate::result_type::{OperatorResultType, SinkFinalizeType};
    use crate::thread_context::ThreadContext;

    fn test_session() -> Arc<StatementContext> {
        paro_context::test_support::TestStatementContextBuilder::minimal().build()
    }

    fn create_test_session() -> Arc<StatementContext> {
        test_session()
    }

    fn create_test_cross_product() -> CrossProduct {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        CrossProduct::new(left, right)
    }

    fn create_rhs_chunk(values: &[i32]) -> Chunk {
        Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    values,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn create_string_chunk(values: &[&str]) -> Chunk {
        Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_string_vector_with_allocator(
                    values,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }

    #[test]
    fn execute_emits_cartesian_rows_across_multiple_calls() {
        let cross = create_test_cross_product();
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let sink_state = Arc::new(CrossProductGlobalSinkState::new(&ctx));
        {
            let mut rhs_chunks = sink_state.rhs_chunks.lock().unwrap();
            rhs_chunks.push(create_rhs_chunk(&[10, 11])).unwrap();
            rhs_chunks.push(create_rhs_chunk(&[12])).unwrap();
        }
        *sink_state.rhs_row_count.lock().unwrap() = 3;

        let interrupt = InterruptState::new();
        assert_eq!(
            cross
                .finalize(&OperatorSinkFinalizeInput::new(
                    sink_state.as_ref(),
                    &interrupt,
                ))
                .unwrap(),
            SinkFinalizeType::Ready
        );
        cross.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let mut state = cross.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut output = paro_common::test_utils::test_chunk_with_capacity(cross.types(), 2);
        let gstate = EmptyGlobalOperatorState;

        let mut rows = Vec::new();
        loop {
            let result = cross
                .execute(
                    &ctx,
                    &input,
                    &mut output,
                    &gstate,
                    state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .unwrap();
            for row_idx in 0..output.size() {
                rows.push((
                    output.data[0].get_value(row_idx).to_string(),
                    output.data[1].get_value(row_idx).to_string(),
                ));
            }
            if result == OperatorResultType::NeedMoreInput {
                break;
            }
            assert_eq!(result, OperatorResultType::HaveMoreOutput);
        }

        assert_eq!(
            rows,
            vec![
                ("1".to_string(), "10".to_string()),
                ("1".to_string(), "11".to_string()),
                ("1".to_string(), "12".to_string()),
                ("2".to_string(), "10".to_string()),
                ("2".to_string(), "11".to_string()),
                ("2".to_string(), "12".to_string()),
            ]
        );
    }

    #[test]
    fn execute_with_string_payloads_terminates_when_output_chunk_is_reset_each_round() {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Varchar]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Varchar]))
            as Arc<dyn PhysicalOperator>;
        let cross = CrossProduct::new(left, right);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let sink_state = Arc::new(CrossProductGlobalSinkState::new(&ctx));
        sink_state
            .rhs_chunks
            .lock()
            .unwrap()
            .push(create_string_chunk(&[
                "grace-hopper",
                "linus-torvalds",
                "margaret-hamilton",
            ]))
            .unwrap();
        *sink_state.rhs_row_count.lock().unwrap() = 3;

        let interrupt = InterruptState::new();
        assert_eq!(
            cross
                .finalize(&OperatorSinkFinalizeInput::new(
                    sink_state.as_ref(),
                    &interrupt,
                ))
                .unwrap(),
            SinkFinalizeType::Ready
        );
        cross.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let mut state = cross.get_operator_state(&ctx).unwrap();
        let input = create_string_chunk(&["ada-lovelace", "eve-analyst"]);
        let mut output = paro_common::test_utils::test_chunk_with_capacity(cross.types(), 2);
        let gstate = EmptyGlobalOperatorState;

        let mut rows = Vec::new();
        for _ in 0..8 {
            output
                .try_reset(output.allocator().clone())
                .expect("test chunk reset allocation failed");
            let result = cross
                .execute(
                    &ctx,
                    &input,
                    &mut output,
                    &gstate,
                    state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .unwrap();
            for row_idx in 0..output.size() {
                rows.push((
                    output.data[0].get_string(row_idx).unwrap().to_string(),
                    output.data[1].get_string(row_idx).unwrap().to_string(),
                ));
            }
            if result == OperatorResultType::NeedMoreInput {
                break;
            }
            assert_eq!(result, OperatorResultType::HaveMoreOutput);
        }

        assert_eq!(
            rows,
            vec![
                ("ada-lovelace".to_string(), "grace-hopper".to_string()),
                ("ada-lovelace".to_string(), "linus-torvalds".to_string()),
                ("ada-lovelace".to_string(), "margaret-hamilton".to_string()),
                ("eve-analyst".to_string(), "grace-hopper".to_string()),
                ("eve-analyst".to_string(), "linus-torvalds".to_string()),
                ("eve-analyst".to_string(), "margaret-hamilton".to_string()),
            ]
        );
    }

    #[test]
    fn build_pipelines_creates_rhs_build_pipeline() {
        let cross = Arc::new(create_test_cross_product()) as Arc<dyn PhysicalOperator>;
        let meta_pipeline = MetaPipeline::new(None, MetaPipelineType::Regular);
        let current = meta_pipeline.base_pipeline();
        let mut state = PipelineBuildState::new();

        cross.build_pipelines(&cross, &current, &meta_pipeline, &mut state);

        let children = meta_pipeline.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].meta_type(), MetaPipelineType::JoinBuild);

        let deps = current.get_dependencies();
        assert_eq!(deps.len(), 1);
        assert!(Arc::ptr_eq(&deps[0], &children[0].base_pipeline()));
    }
}
