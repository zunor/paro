// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical nested loop join fallback.

use std::any::Any;
use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_join_condition;
use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::RetainedChunkVec;
use crate::operator::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
    construct_right_outer_scan_result, construct_semi_join_result,
};
use crate::operator::join::outer_join_marker::{OuterJoinMarker, OuterJoinScanState};
use crate::operator::join::physical_join::PhysicalJoin;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorSinkCombineInput, OperatorSinkInput, OperatorSourceInput, OperatorState,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorResultType, SinkCombineResultType, SinkResultType, SourceResultType,
};

#[derive(Debug, Clone, Copy)]
struct RightRowLocation {
    chunk_idx: usize,
    row_idx: usize,
    global_idx: usize,
}

#[derive(Debug)]
struct NestedLoopJoinGlobalSinkState {
    rhs_payload_chunks: Arc<Mutex<RetainedChunkVec>>,
    rhs_condition_chunks: Arc<Mutex<RetainedChunkVec>>,
    rhs_row_count: Mutex<usize>,
    right_outer: Arc<OuterJoinMarker>,
    peak_memory_bytes: AtomicUsize,
}

impl NestedLoopJoinGlobalSinkState {
    fn new(enable_right_outer: bool, memory: MemoryAccountingContext) -> Self {
        Self {
            rhs_payload_chunks: Arc::new(Mutex::new(RetainedChunkVec::new(memory.clone()))),
            rhs_condition_chunks: Arc::new(Mutex::new(RetainedChunkVec::new(memory))),
            rhs_row_count: Mutex::new(0),
            right_outer: Arc::new(OuterJoinMarker::new(enable_right_outer)),
            peak_memory_bytes: AtomicUsize::new(0),
        }
    }

    fn row_count(&self) -> usize {
        *self.rhs_row_count.lock().unwrap()
    }

    fn record_peak(&self, bytes: usize) {
        self.peak_memory_bytes.fetch_max(bytes, Ordering::AcqRel);
    }

    fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes.load(Ordering::Acquire)
    }

    fn current_memory_bytes(&self) -> usize {
        self.rhs_payload_chunks.lock().unwrap().retained_bytes()
            + self.rhs_condition_chunks.lock().unwrap().retained_bytes()
    }
}

impl GlobalSinkState for NestedLoopJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "NestedLoopJoinGlobalSinkState"
    }
}

#[derive(Debug)]
struct NestedLoopJoinLocalSinkState {
    local_payload_chunks: RetainedChunkVec,
    local_condition_chunks: RetainedChunkVec,
    local_row_count: usize,
    right_condition_executors: Vec<ExpressionExecutor>,
}

impl LocalSinkState for NestedLoopJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl NestedLoopJoinLocalSinkState {
    fn memory_usage_bytes(&self) -> usize {
        self.local_payload_chunks.retained_bytes() + self.local_condition_chunks.retained_bytes()
    }
}

#[derive(Debug)]
struct NestedLoopJoinGlobalSourceState {
    rhs_payload_chunks: Arc<Mutex<RetainedChunkVec>>,
    right_outer: Arc<OuterJoinMarker>,
    join_type: JoinType,
}

impl GlobalSourceState for NestedLoopJoinGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct NestedLoopJoinLocalSourceState {
    scan_state: OuterJoinScanState,
}

impl LocalSourceState for NestedLoopJoinLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct NestedLoopJoinOperatorState {
    input_initialized: bool,
    left_condition_chunk: Chunk,
    combined_row_chunk: Chunk,
    left_condition_executors: Vec<ExpressionExecutor>,
    arbitrary_condition_executor: Option<ExpressionExecutor>,
    left_row_idx: usize,
    rhs_chunk_idx: usize,
    rhs_row_idx: usize,
    rhs_global_idx: usize,
    found_match: bool,
    saw_null: bool,
    single_match: Option<RightRowLocation>,
}

impl OperatorState for NestedLoopJoinOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowMatchState {
    Match,
    NoMatch,
    Unknown,
}

/// Physical nested loop join fallback for non-hashable predicates and any joins.
pub struct NestedLoopJoin {
    pub join: PhysicalJoin,
    pub left: Arc<dyn PhysicalOperator>,
    pub right: Arc<dyn PhysicalOperator>,
    pub join_type: JoinType,
    pub comparison_conditions: Vec<JoinCondition>,
    pub arbitrary_condition: Option<Expression>,
    pub types: Vec<LogicalType>,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

fn nested_loop_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        paro_common::allocator::MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}

impl NestedLoopJoin {
    fn validate_join_type(join_type: JoinType) -> Result<()> {
        if matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Outer
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
                | JoinType::RightSemi
                | JoinType::RightAnti
        ) {
            Ok(())
        } else {
            Err(paro_error::not_implemented(format!(
                "{} nested loop join result construction",
                join_type
            )))
        }
    }

    pub fn new_comparison(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        mut conditions: Vec<JoinCondition>,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Self {
        crate::operator::join::physical_comparison_join::PhysicalComparisonJoin::reorder_conditions(
            &mut conditions,
        );
        let join = PhysicalJoin::new(
            left.clone(),
            right.clone(),
            join_type,
            left_projection_map,
            right_projection_map,
        );
        let types = join.types.clone();
        Self {
            join,
            left,
            right,
            join_type,
            comparison_conditions: conditions,
            arbitrary_condition: None,
            types,
            sink_state: Mutex::new(None),
        }
    }

    pub fn new_any(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        condition: Expression,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Self {
        let join = PhysicalJoin::new(
            left.clone(),
            right.clone(),
            join_type,
            left_projection_map,
            right_projection_map,
        );
        let types = join.types.clone();
        Self {
            join,
            left,
            right,
            join_type,
            comparison_conditions: Vec::new(),
            arbitrary_condition: Some(condition),
            types,
            sink_state: Mutex::new(None),
        }
    }

    fn condition_info(&self) -> String {
        if self.comparison_conditions.is_empty() {
            "arbitrary predicate".to_string()
        } else {
            self.comparison_conditions
                .iter()
                .map(format_join_condition)
                .collect::<Vec<_>>()
                .join(" AND ")
        }
    }

    fn uses_right_outer_marker(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn uses_any_condition(&self) -> bool {
        self.arbitrary_condition.is_some()
    }

    fn construct_empty_join_result(&self, input: &Chunk, result: &mut Chunk) -> Result<()> {
        match self.join_type {
            JoinType::Anti => {
                let mut sel =
                    SelectionVector::try_with_capacity(input.size(), input.allocator().clone())?;
                sel.set_len(input.size());
                for idx in 0..input.size() {
                    sel.set(idx, idx);
                }
                construct_anti_join_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    result,
                )?;
            }
            JoinType::Mark => {
                construct_mark_join_result(
                    input,
                    &self.join.left_projection_map,
                    &vec![Some(false); input.size()],
                    result,
                )?;
            }
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                let mut sel =
                    SelectionVector::try_with_capacity(input.size(), input.allocator().clone())?;
                sel.set_len(input.size());
                for idx in 0..input.size() {
                    sel.set(idx, idx);
                }
                construct_left_outer_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    &self.join.right_output_types,
                    result,
                )?;
            }
            _ => {
                result.set_cardinality(0);
            }
        }
        Ok(())
    }

    fn ensure_output_chunk(&self, chunk: &mut Chunk) -> Result<()> {
        if chunk.column_count() == 0 {
            *chunk = Chunk::try_initialize(
                &self.types,
                paro_common::vector::VECTOR_SIZE,
                chunk.allocator().clone(),
            )?;
        }
        Ok(())
    }

    fn ensure_input_state(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        state: &mut NestedLoopJoinOperatorState,
    ) -> Result<()> {
        if state.input_initialized {
            return Ok(());
        }

        state.left_row_idx = 0;
        state.rhs_chunk_idx = 0;
        state.rhs_row_idx = 0;
        state.rhs_global_idx = 0;
        state.found_match = false;
        state.saw_null = false;
        state.single_match = None;

        if !self.comparison_conditions.is_empty() {
            let mut left_columns = Vec::with_capacity(self.comparison_conditions.len());
            for executor in &mut state.left_condition_executors {
                left_columns.push(executor.execute_expression(
                    0,
                    input,
                    None,
                    input.size(),
                    ctx,
                )?);
            }
            let mut left_condition_chunk =
                Chunk::from_arc_vectors(left_columns, input.allocator().clone());
            left_condition_chunk.set_cardinality(input.size());
            state.left_condition_chunk = Self::materialize_chunk(
                &left_condition_chunk,
                &self
                    .comparison_conditions
                    .iter()
                    .map(|condition| condition.left.return_type())
                    .collect::<Vec<_>>(),
            )?;
        }

        state.input_initialized = true;
        Ok(())
    }

    fn reset_input_state(&self, state: &mut NestedLoopJoinOperatorState) {
        state.input_initialized = false;
        state.left_row_idx = 0;
        state.rhs_chunk_idx = 0;
        state.rhs_row_idx = 0;
        state.rhs_global_idx = 0;
        state.found_match = false;
        state.saw_null = false;
        state.single_match = None;
        state.left_condition_chunk.set_cardinality(0);
        state.combined_row_chunk.set_cardinality(0);
    }

    fn advance_right_position(
        &self,
        payload_chunks: &[Chunk],
        state: &mut NestedLoopJoinOperatorState,
    ) {
        if state.rhs_chunk_idx >= payload_chunks.len() {
            return;
        }
        state.rhs_row_idx += 1;
        state.rhs_global_idx += 1;
        while state.rhs_chunk_idx < payload_chunks.len()
            && state.rhs_row_idx >= payload_chunks[state.rhs_chunk_idx].size()
        {
            state.rhs_chunk_idx += 1;
            state.rhs_row_idx = 0;
        }
    }

    fn reset_for_next_left_row(&self, state: &mut NestedLoopJoinOperatorState) {
        state.left_row_idx += 1;
        state.rhs_chunk_idx = 0;
        state.rhs_row_idx = 0;
        state.rhs_global_idx = 0;
        state.found_match = false;
        state.saw_null = false;
        state.single_match = None;
    }

    fn comparison_outcome(
        comparison: JoinComparisonType,
        left: &Value,
        right: &Value,
    ) -> Option<bool> {
        match comparison {
            JoinComparisonType::Equal => {
                (!left.is_null() && !right.is_null()).then_some(left == right)
            }
            JoinComparisonType::NotEqual => {
                (!left.is_null() && !right.is_null()).then_some(left != right)
            }
            JoinComparisonType::LessThan => (!left.is_null() && !right.is_null())
                .then_some(left.partial_cmp(right) == Some(CmpOrdering::Less)),
            JoinComparisonType::GreaterThan => (!left.is_null() && !right.is_null())
                .then_some(left.partial_cmp(right) == Some(CmpOrdering::Greater)),
            JoinComparisonType::LessThanOrEqual => {
                (!left.is_null() && !right.is_null()).then_some(matches!(
                    left.partial_cmp(right),
                    Some(CmpOrdering::Less | CmpOrdering::Equal)
                ))
            }
            JoinComparisonType::GreaterThanOrEqual => (!left.is_null() && !right.is_null())
                .then_some(matches!(
                    left.partial_cmp(right),
                    Some(CmpOrdering::Greater | CmpOrdering::Equal)
                )),
            JoinComparisonType::NotDistinctFrom => Some(
                (left.is_null() && right.is_null())
                    || (!left.is_null() && !right.is_null() && left == right),
            ),
            JoinComparisonType::DistinctFrom => Some(
                (left.is_null() && !right.is_null())
                    || (!left.is_null() && right.is_null())
                    || (!left.is_null() && !right.is_null() && left != right),
            ),
        }
    }

    fn evaluate_comparison_match(
        &self,
        state: &NestedLoopJoinOperatorState,
        rhs_condition_chunks: &[Chunk],
        right: RightRowLocation,
    ) -> RowMatchState {
        let right_chunk = match rhs_condition_chunks.get(right.chunk_idx) {
            Some(chunk) => chunk,
            None => return RowMatchState::NoMatch,
        };

        let mut saw_null = false;
        for (cond_idx, condition) in self.comparison_conditions.iter().enumerate() {
            let left_value =
                state.left_condition_chunk.data[cond_idx].get_value(state.left_row_idx);
            let right_value = right_chunk.data[cond_idx].get_value(right.row_idx);
            match Self::comparison_outcome(condition.comparison, &left_value, &right_value) {
                Some(true) => {}
                Some(false) => return RowMatchState::NoMatch,
                None => saw_null = true,
            }
        }

        if saw_null {
            RowMatchState::Unknown
        } else {
            RowMatchState::Match
        }
    }

    fn populate_combined_row_chunk(
        &self,
        combined_row_chunk: &mut Chunk,
        input: &Chunk,
        left_row: usize,
        right_chunk: &Chunk,
        right_row: usize,
    ) -> Result<()> {
        for col_idx in 0..input.column_count() {
            let source = input.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!("Probe column {} missing for any-join", col_idx))
            })?;
            let target = combined_row_chunk.column_mut(col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Combined chunk probe column {} missing for any-join",
                    col_idx
                ))
            })?;
            target.set_value(0, &source.get_value(left_row));
        }

        let right_offset = input.column_count();
        for col_idx in 0..right_chunk.column_count() {
            let source = right_chunk.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!("Build column {} missing for any-join", col_idx))
            })?;
            let target = combined_row_chunk
                .column_mut(right_offset + col_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Combined chunk build column {} missing for any-join",
                        col_idx
                    ))
                })?;
            target.set_value(0, &source.get_value(right_row));
        }
        combined_row_chunk.set_cardinality(1);
        Ok(())
    }

    fn evaluate_any_match(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        state: &mut NestedLoopJoinOperatorState,
        right_chunk: &Chunk,
        right_row: usize,
    ) -> Result<RowMatchState> {
        self.populate_combined_row_chunk(
            &mut state.combined_row_chunk,
            input,
            state.left_row_idx,
            right_chunk,
            right_row,
        )?;

        let executor = state
            .arbitrary_condition_executor
            .as_mut()
            .ok_or_else(|| paro_error::internal("Any-join executor missing".to_string()))?;
        let result_vec = executor.execute_expression(0, &state.combined_row_chunk, None, 1, ctx)?;
        let value = result_vec.get_value(0);
        if value.is_null() {
            return Ok(RowMatchState::Unknown);
        }
        match value {
            Value::Boolean(true) => Ok(RowMatchState::Match),
            Value::Boolean(false) => Ok(RowMatchState::NoMatch),
            _ => Err(paro_error::internal(
                "Any-join predicate must evaluate to BOOLEAN".to_string(),
            )),
        }
    }

    fn evaluate_match(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        state: &mut NestedLoopJoinOperatorState,
        rhs_condition_chunks: &[Chunk],
        rhs_payload_chunks: &[Chunk],
        right: RightRowLocation,
    ) -> Result<RowMatchState> {
        if self.uses_any_condition() {
            let right_chunk = rhs_payload_chunks.get(right.chunk_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Build chunk {} missing while evaluating any-join",
                    right.chunk_idx
                ))
            })?;
            self.evaluate_any_match(ctx, input, state, right_chunk, right.row_idx)
        } else {
            Ok(self.evaluate_comparison_match(state, rhs_condition_chunks, right))
        }
    }

    fn append_left_projection(
        &self,
        output: &mut Chunk,
        output_row: usize,
        input: &Chunk,
        input_row: usize,
    ) -> Result<()> {
        if self.join.left_projection_map.is_empty() {
            for col_idx in 0..input.column_count() {
                let source = input.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("Probe column {} missing", col_idx))
                })?;
                let target = output.column_mut(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("Output column {} missing", col_idx))
                })?;
                target.set_value(output_row, &source.get_value(input_row));
            }
        } else {
            for (output_col_idx, input_col_idx) in self.join.left_projection_map.iter().enumerate()
            {
                let source = input.column(*input_col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Projected probe column {} missing",
                        input_col_idx
                    ))
                })?;
                let target = output.column_mut(output_col_idx).ok_or_else(|| {
                    paro_error::internal(format!("Output column {} missing", output_col_idx))
                })?;
                target.set_value(output_row, &source.get_value(input_row));
            }
        }
        Ok(())
    }

    fn append_right_projection(
        &self,
        output: &mut Chunk,
        output_row: usize,
        rhs_payload_chunks: &[Chunk],
        right: RightRowLocation,
    ) -> Result<()> {
        let right_chunk = rhs_payload_chunks.get(right.chunk_idx).ok_or_else(|| {
            paro_error::internal(format!("Build chunk {} missing", right.chunk_idx))
        })?;
        let output_offset = self.join.left_output_types.len();
        if self.join.right_projection_map.is_empty() {
            for col_idx in 0..right_chunk.column_count() {
                let source = right_chunk.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("Build column {} missing", col_idx))
                })?;
                let target = output.column_mut(output_offset + col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Output build column {} missing",
                        output_offset + col_idx
                    ))
                })?;
                target.set_value(output_row, &source.get_value(right.row_idx));
            }
        } else {
            for (output_col_idx, input_col_idx) in self.join.right_projection_map.iter().enumerate()
            {
                let source = right_chunk.column(*input_col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Projected build column {} missing",
                        input_col_idx
                    ))
                })?;
                let target = output
                    .column_mut(output_offset + output_col_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Output build column {} missing",
                            output_offset + output_col_idx
                        ))
                    })?;
                target.set_value(output_row, &source.get_value(right.row_idx));
            }
        }
        Ok(())
    }

    fn append_null_right(&self, output: &mut Chunk, output_row: usize) -> Result<()> {
        let output_offset = self.join.left_output_types.len();
        for (idx, typ) in self.join.right_output_types.iter().enumerate() {
            let target = output.column_mut(output_offset + idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Output build column {} missing for NULL fill",
                    output_offset + idx
                ))
            })?;
            target.set_value(output_row, &Value::Null(typ.clone()));
        }
        Ok(())
    }

    fn append_join_match_row(
        &self,
        output: &mut Chunk,
        output_row: usize,
        input: &Chunk,
        input_row: usize,
        rhs_payload_chunks: &[Chunk],
        right: RightRowLocation,
    ) -> Result<()> {
        self.append_left_projection(output, output_row, input, input_row)?;
        self.append_right_projection(output, output_row, rhs_payload_chunks, right)
    }

    fn append_left_only_row(
        &self,
        output: &mut Chunk,
        output_row: usize,
        input: &Chunk,
        input_row: usize,
    ) -> Result<()> {
        self.append_left_projection(output, output_row, input, input_row)
    }

    fn append_unmatched_left_row(
        &self,
        output: &mut Chunk,
        output_row: usize,
        input: &Chunk,
        input_row: usize,
    ) -> Result<()> {
        self.append_left_projection(output, output_row, input, input_row)?;
        self.append_null_right(output, output_row)
    }

    fn append_mark_row(
        &self,
        output: &mut Chunk,
        output_row: usize,
        input: &Chunk,
        input_row: usize,
        marker: Option<bool>,
    ) -> Result<()> {
        self.append_left_projection(output, output_row, input, input_row)?;
        let marker_offset = self.join.left_output_types.len();
        let marker_col = output
            .column_mut(marker_offset)
            .ok_or_else(|| paro_error::internal("MARK join output column missing".to_string()))?;
        match marker {
            Some(value) => {
                marker_col.set_value(output_row, &Value::Boolean(value));
                marker_col.set_null(output_row, false);
            }
            None => {
                marker_col.set_value(output_row, &Value::Boolean(false));
                marker_col.set_null(output_row, true);
            }
        }
        Ok(())
    }

    fn mark_right_match(&self, gsink: &NestedLoopJoinGlobalSinkState, right: RightRowLocation) {
        if self.uses_right_outer_marker() {
            gsink.right_outer.set_match(right.global_idx);
        }
    }

    fn materialize_chunk(chunk: &Chunk, types: &[LogicalType]) -> Result<Chunk> {
        let mut materialized =
            Chunk::try_initialize(types, chunk.size().max(1), chunk.allocator().clone())?;
        materialized.set_cardinality(chunk.size());
        for col_idx in 0..chunk.column_count() {
            let source = chunk
                .column(col_idx)
                .expect("materialized source column must exist");
            let target = materialized
                .column_mut(col_idx)
                .expect("materialized target column must exist");
            for row_idx in 0..chunk.size() {
                target.copy_at(row_idx, source, row_idx);
            }
        }
        Ok(materialized)
    }
}

impl fmt::Debug for NestedLoopJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NestedLoopJoin")
            .field("join_type", &self.join_type)
            .field("comparison_conditions", &self.comparison_conditions.len())
            .field("has_any_condition", &self.arbitrary_condition.is_some())
            .finish()
    }
}

impl PhysicalOperator for NestedLoopJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::NestedLoopJoin
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(sink_state) = sink_state
            .as_any()
            .downcast_ref::<NestedLoopJoinGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        ExplainRuntimeStats {
            spilled: None,
            peak_memory_bytes: Some(sink_state.peak_memory_bytes() as u64),
            temp_storage_bytes: None,
            ..Default::default()
        }
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        vec![
            format!("Join Type: {}", self.join_type),
            format!("Join Condition: {}", self.condition_info()),
        ]
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

    fn is_source(&self) -> bool {
        self.join.is_source()
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        false
    }

    fn parallel_operator(&self) -> bool {
        false
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut sink_state = self.sink_state.lock().unwrap();
        *sink_state = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        let left_condition_types = self
            .comparison_conditions
            .iter()
            .map(|condition| condition.left.return_type())
            .collect::<Vec<_>>();
        let mut combined_types = self.left.types().to_vec();
        combined_types.extend(self.right.types().iter().cloned());

        Ok(Box::new(NestedLoopJoinOperatorState {
            input_initialized: false,
            left_condition_chunk: Chunk::try_init_empty(
                &left_condition_types,
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            combined_row_chunk: Chunk::try_initialize(
                &combined_types,
                1,
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            left_condition_executors: self
                .comparison_conditions
                .iter()
                .map(|condition| ExpressionExecutor::new(&condition.left))
                .collect(),
            arbitrary_condition_executor: self
                .arbitrary_condition
                .as_ref()
                .map(ExpressionExecutor::new),
            left_row_idx: 0,
            rhs_chunk_idx: 0,
            rhs_row_idx: 0,
            rhs_global_idx: 0,
            found_match: false,
            saw_null: false,
            single_match: None,
        }))
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Self::validate_join_type(self.join_type)?;
        Ok(Box::new(NestedLoopJoinGlobalSinkState::new(
            self.uses_right_outer_marker(),
            nested_loop_memory_context(ctx),
        )))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let memory = nested_loop_memory_context(ctx);
        Ok(Box::new(NestedLoopJoinLocalSinkState {
            local_payload_chunks: RetainedChunkVec::new(memory.clone()),
            local_condition_chunks: RetainedChunkVec::new(memory),
            local_row_count: 0,
            right_condition_executors: self
                .comparison_conditions
                .iter()
                .map(|condition| ExpressionExecutor::new(&condition.right))
                .collect(),
        }))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<NestedLoopJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join local sink".to_string())
            })?;

        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }

        lstate
            .local_payload_chunks
            .push(Self::materialize_chunk(chunk, self.right.types())?)?;
        lstate.local_row_count += chunk.size();

        if !self.comparison_conditions.is_empty() {
            let mut condition_columns = Vec::with_capacity(self.comparison_conditions.len());
            for executor in &mut lstate.right_condition_executors {
                condition_columns.push(executor.execute_expression(
                    0,
                    chunk,
                    None,
                    chunk.size(),
                    ctx,
                )?);
            }
            let mut condition_chunk =
                Chunk::from_arc_vectors(condition_columns, chunk.allocator().clone());
            condition_chunk.set_cardinality(chunk.size());
            lstate.local_condition_chunks.push(Self::materialize_chunk(
                &condition_chunk,
                &self
                    .comparison_conditions
                    .iter()
                    .map(|condition| condition.right.return_type())
                    .collect::<Vec<_>>(),
            )?)?;
        }

        if let Some(gstate) = input
            .global_state
            .as_any()
            .downcast_ref::<NestedLoopJoinGlobalSinkState>()
        {
            gstate.record_peak(lstate.memory_usage_bytes());
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
            .downcast_ref::<NestedLoopJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join global sink".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<NestedLoopJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join local sink".to_string())
            })?;

        if lstate.local_row_count > 0 {
            {
                let mut payload_chunks = gstate.rhs_payload_chunks.lock().unwrap();
                payload_chunks.append_from(&mut lstate.local_payload_chunks)?;
            }
            {
                let mut condition_chunks = gstate.rhs_condition_chunks.lock().unwrap();
                condition_chunks.append_from(&mut lstate.local_condition_chunks)?;
            }
            *gstate.rhs_row_count.lock().unwrap() += lstate.local_row_count;
            gstate.right_outer.add_rows(lstate.local_row_count);
            lstate.local_row_count = 0;
            gstate.record_peak(gstate.current_memory_bytes());
        }

        Ok(SinkCombineResultType::Finished)
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Self::validate_join_type(self.join_type)?;

        let create_source_state =
            |sink: &NestedLoopJoinGlobalSinkState| -> Result<Box<dyn GlobalSourceState>> {
                Ok(Box::new(NestedLoopJoinGlobalSourceState {
                    rhs_payload_chunks: Arc::clone(&sink.rhs_payload_chunks),
                    right_outer: sink.right_outer.clone(),
                    join_type: self.join_type,
                }))
            };

        if let Some(sink_state) = sink_state {
            let sink = sink_state
                .as_any()
                .downcast_ref::<NestedLoopJoinGlobalSinkState>()
                .ok_or_else(|| {
                    paro_error::internal(
                        "Invalid nested loop join sink state for source".to_string(),
                    )
                })?;
            return create_source_state(sink);
        }

        let internal_sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("NestedLoopJoin requires sink state for source phase".to_string())
        })?;
        let sink = internal_sink
            .as_any()
            .downcast_ref::<NestedLoopJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join sink state for source".to_string())
            })?;
        create_source_state(sink)
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(NestedLoopJoinLocalSourceState::default()))
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<OperatorResultType> {
        Self::validate_join_type(self.join_type)?;

        let state = state
            .as_any_mut()
            .downcast_mut::<NestedLoopJoinOperatorState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join operator state".to_string())
            })?;

        self.ensure_output_chunk(chunk)?;
        if input.size() == 0 {
            chunk.set_cardinality(0);
            self.reset_input_state(state);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("NestedLoopJoin requires sink state".to_string())
        })?;
        let gsink = sink
            .as_any()
            .downcast_ref::<NestedLoopJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join sink state".to_string())
            })?;
        let rhs_row_count = gsink.row_count();

        if rhs_row_count == 0 {
            if self.join.empty_result_if_rhs_is_empty() {
                chunk.set_cardinality(0);
            } else {
                self.construct_empty_join_result(input, chunk)?;
            }
            self.reset_input_state(state);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        self.ensure_input_state(ctx, input, state)?;

        let rhs_payload_chunks = gsink.rhs_payload_chunks.lock().unwrap();
        let rhs_payload_chunks = rhs_payload_chunks.as_slice();
        let rhs_condition_chunks = gsink.rhs_condition_chunks.lock().unwrap();
        let rhs_condition_chunks = rhs_condition_chunks.as_slice();
        let mut output_count = 0;
        let output_capacity = chunk.capacity();

        while output_count < output_capacity && state.left_row_idx < input.size() {
            match self.join_type {
                JoinType::Inner | JoinType::Right | JoinType::Left | JoinType::Outer => {
                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        if matches!(self.join_type, JoinType::Left | JoinType::Outer)
                            && !state.found_match
                        {
                            self.append_unmatched_left_row(
                                chunk,
                                output_count,
                                input,
                                state.left_row_idx,
                            )?;
                            output_count += 1;
                        }
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    if outcome == RowMatchState::Match {
                        state.found_match = true;
                        self.mark_right_match(gsink, right);
                        self.append_join_match_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            rhs_payload_chunks,
                            right,
                        )?;
                        output_count += 1;
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::Semi => {
                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        if state.found_match {
                            self.append_left_only_row(
                                chunk,
                                output_count,
                                input,
                                state.left_row_idx,
                            )?;
                            output_count += 1;
                        }
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    if outcome == RowMatchState::Match {
                        state.found_match = true;
                        self.append_left_only_row(chunk, output_count, input, state.left_row_idx)?;
                        output_count += 1;
                        self.reset_for_next_left_row(state);
                        continue;
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::Anti => {
                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        if !state.found_match {
                            self.append_left_only_row(
                                chunk,
                                output_count,
                                input,
                                state.left_row_idx,
                            )?;
                            output_count += 1;
                        }
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    if outcome == RowMatchState::Match {
                        state.found_match = true;
                        self.reset_for_next_left_row(state);
                        continue;
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::Mark => {
                    if state.found_match {
                        self.append_mark_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            Some(true),
                        )?;
                        output_count += 1;
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        let marker = if state.saw_null { None } else { Some(false) };
                        self.append_mark_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            marker,
                        )?;
                        output_count += 1;
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    match outcome {
                        RowMatchState::Match => state.found_match = true,
                        RowMatchState::Unknown => state.saw_null = true,
                        RowMatchState::NoMatch => {}
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::Single => {
                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        match state.single_match {
                            Some(right) => {
                                self.append_join_match_row(
                                    chunk,
                                    output_count,
                                    input,
                                    state.left_row_idx,
                                    rhs_payload_chunks,
                                    right,
                                )?;
                            }
                            None => {
                                self.append_unmatched_left_row(
                                    chunk,
                                    output_count,
                                    input,
                                    state.left_row_idx,
                                )?;
                            }
                        }
                        output_count += 1;
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    if outcome == RowMatchState::Match {
                        if state.single_match.is_some() {
                            return Err(paro_error::invalid_input(
                                "More than one row returned by SINGLE join",
                            ));
                        }
                        state.single_match = Some(right);
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::RightSemi | JoinType::RightAnti => {
                    if state.rhs_chunk_idx >= rhs_payload_chunks.len() {
                        self.reset_for_next_left_row(state);
                        continue;
                    }

                    let right = RightRowLocation {
                        chunk_idx: state.rhs_chunk_idx,
                        row_idx: state.rhs_row_idx,
                        global_idx: state.rhs_global_idx,
                    };
                    let outcome = self.evaluate_match(
                        ctx,
                        input,
                        state,
                        rhs_condition_chunks,
                        rhs_payload_chunks,
                        right,
                    )?;
                    if outcome == RowMatchState::Match {
                        self.mark_right_match(gsink, right);
                    }
                    self.advance_right_position(rhs_payload_chunks, state);
                }
                JoinType::Invalid => unreachable!("join type validated above"),
            }
        }

        chunk.set_cardinality(output_count);
        if state.left_row_idx >= input.size() {
            self.reset_input_state(state);
            Ok(OperatorResultType::NeedMoreInput)
        } else if output_count > 0 {
            Ok(OperatorResultType::HaveMoreOutput)
        } else {
            Ok(OperatorResultType::NeedMoreInput)
        }
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        Self::validate_join_type(self.join_type)?;

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<NestedLoopJoinGlobalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join global source".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<NestedLoopJoinLocalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid nested loop join local source".to_string())
            })?;

        let emit_found = match gstate.join_type {
            JoinType::RightSemi => true,
            JoinType::Right | JoinType::Outer | JoinType::RightAnti => false,
            _ => {
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            }
        };

        if chunk.column_count() == 0 {
            *chunk = Chunk::try_initialize(
                &self.types,
                paro_common::vector::VECTOR_SIZE,
                chunk.allocator().clone(),
            )?;
        }

        let mut build_chunk = Chunk::try_initialize(
            self.right.types(),
            paro_common::vector::VECTOR_SIZE,
            chunk.allocator().clone(),
        )?;
        let rhs_payload_chunks = gstate.rhs_payload_chunks.lock().unwrap();
        let count = gstate.right_outer.scan(
            rhs_payload_chunks.as_slice(),
            &mut lstate.scan_state,
            emit_found,
            &mut build_chunk,
        )?;
        if count == 0 {
            chunk.set_cardinality(0);
            return Ok(SourceResultType::Finished);
        }

        let build_sel = SelectionVector::try_incremental(count, chunk.allocator().clone())?;
        match gstate.join_type {
            JoinType::Right | JoinType::Outer => construct_right_outer_scan_result(
                &build_chunk,
                &build_sel,
                count,
                &self.join.left_output_types,
                &self.join.right_projection_map,
                chunk,
            ),
            JoinType::RightSemi | JoinType::RightAnti => construct_semi_join_result(
                &build_chunk,
                &build_sel,
                count,
                &self.join.right_projection_map,
                chunk,
            ),
            _ => unreachable!("source phase only runs for right/full/right-semi/right-anti"),
        }?;

        Ok(SourceResultType::HaveMoreOutput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn build_pipelines(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        self.join
            .build_join_pipelines(op, current, meta_pipeline, state, true);
    }
}

#[cfg(test)]
mod tests {
    use super::{NestedLoopJoin, NestedLoopJoinGlobalSinkState, NestedLoopJoinGlobalSourceState};
    use std::sync::Arc;

    use paro_common::allocator::MemoryTag;
    use paro_common::chunk::Chunk;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        ReferenceExpression,
    };
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
    use paro_planner::operator::ColumnBinding;
    use paro_scheduler::task::InterruptState;

    use crate::execution_context::ExecutionContext;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::{EmptyGlobalOperatorState, GlobalSinkState, OperatorSourceInput};
    use crate::operator::PhysicalOperator;
    use crate::result_type::{OperatorResultType, SourceResultType};
    use crate::thread_context::ThreadContext;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn create_test_session() -> Arc<StatementContext> {
        test_session()
    }

    fn left_ref(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn right_ref(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn comparison_join(join_type: JoinType, comparison: JoinComparisonType) -> NestedLoopJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        NestedLoopJoin::new_comparison(
            left,
            right,
            join_type,
            vec![JoinCondition::new(left_ref(0), right_ref(0), comparison)],
            vec![],
            vec![],
        )
    }

    fn any_join(join_type: JoinType) -> NestedLoopJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let condition = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
        ));
        NestedLoopJoin::new_any(left, right, join_type, condition, vec![], vec![])
    }

    fn set_sink_state(
        join: &NestedLoopJoin,
        payload_chunks: Vec<Chunk>,
        condition_chunks: Vec<Chunk>,
    ) {
        let sink_state = Arc::new(NestedLoopJoinGlobalSinkState::new(
            join.uses_right_outer_marker(),
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        ));
        let row_count = payload_chunks.iter().map(Chunk::size).sum::<usize>();
        {
            let mut payload = sink_state.rhs_payload_chunks.lock().unwrap();
            for chunk in payload_chunks {
                payload.push(chunk).unwrap();
            }
        }
        {
            let mut conditions = sink_state.rhs_condition_chunks.lock().unwrap();
            for chunk in condition_chunks {
                conditions.push(chunk).unwrap();
            }
        }
        *sink_state.rhs_row_count.lock().unwrap() = row_count;
        sink_state.right_outer.add_rows(row_count);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);
    }

    #[test]
    fn left_nested_loop_join_emits_unmatched_left_rows() {
        let join = comparison_join(JoinType::Left, JoinComparisonType::GreaterThan);
        set_sink_state(
            &join,
            vec![Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[2, 7],
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            )],
            vec![Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[2, 7],
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            )],
        );

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5, 1],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 2);
        assert_eq!(output.data[0].get_value(0).to_string(), "5");
        assert_eq!(output.data[1].get_value(0).to_string(), "2");
        assert_eq!(output.data[0].get_value(1).to_string(), "1");
        assert!(output.data[1].is_null(1));
    }

    #[test]
    fn mark_nested_loop_join_returns_null_for_unknown_only_matches() {
        let join = comparison_join(JoinType::Mark, JoinComparisonType::GreaterThan);
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut condition =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 1);
        condition
            .column_mut(0)
            .expect("condition column")
            .set_value(0, &Value::Null(LogicalType::Integer));
        condition.set_cardinality(1);
        set_sink_state(&join, vec![payload], vec![condition]);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert!(output.data[1].is_null(0));
    }

    #[test]
    fn any_join_executes_against_combined_probe_and_build_rows() {
        let join = any_join(JoinType::Inner);
        set_sink_state(
            &join,
            vec![Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[2, 4],
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            )],
            vec![],
        );

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert_eq!(output.data[0].get_value(0).to_string(), "3");
        assert_eq!(output.data[1].get_value(0).to_string(), "2");
    }

    #[test]
    fn nested_loop_state_caches_condition_executors() {
        let join = comparison_join(JoinType::Inner, JoinComparisonType::GreaterThan);
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let operator_state = join.get_operator_state(&ctx).unwrap();
        let operator_state = operator_state
            .as_any()
            .downcast_ref::<super::NestedLoopJoinOperatorState>()
            .unwrap();
        assert_eq!(operator_state.left_condition_executors.len(), 1);
        assert!(operator_state.arbitrary_condition_executor.is_none());

        let local_sink_state = join.get_local_sink_state(&ctx).unwrap();
        let local_sink_state = local_sink_state
            .as_any()
            .downcast_ref::<super::NestedLoopJoinLocalSinkState>()
            .unwrap();
        assert_eq!(local_sink_state.right_condition_executors.len(), 1);

        let any_join = any_join(JoinType::Inner);
        let any_operator_state = any_join.get_operator_state(&ctx).unwrap();
        let any_operator_state = any_operator_state
            .as_any()
            .downcast_ref::<super::NestedLoopJoinOperatorState>()
            .unwrap();
        assert!(any_operator_state.left_condition_executors.is_empty());
        assert!(any_operator_state.arbitrary_condition_executor.is_some());
    }

    #[test]
    fn nested_loop_right_join_source_emits_unmatched_build_rows() {
        let join = comparison_join(JoinType::Right, JoinComparisonType::GreaterThan);
        let sink_state = Arc::new(NestedLoopJoinGlobalSinkState::new(
            true,
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        ));
        {
            let mut payload = sink_state.rhs_payload_chunks.lock().unwrap();
            payload
                .push(Chunk::from_arc_vectors(
                    vec![Arc::new(
                        paro_common::test_utils::test_i32_vector_with_allocator(
                            &[10, 20],
                            paro_common::test_utils::test_allocator(),
                        ),
                    )],
                    paro_common::test_utils::test_allocator(),
                ))
                .unwrap();
        }
        *sink_state.rhs_row_count.lock().unwrap() = 2;
        sink_state.right_outer.add_rows(2);
        sink_state.right_outer.set_match(0);

        let gstate = NestedLoopJoinGlobalSourceState {
            rhs_payload_chunks: Arc::clone(&sink_state.rhs_payload_chunks),
            right_outer: sink_state.right_outer.clone(),
            join_type: JoinType::Right,
        };

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut lstate = join.get_local_source_state(&ctx, &gstate).unwrap();
        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(&gstate, lstate.as_mut(), &interrupt);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let result = join.get_data(&ctx, &mut output, &mut input).unwrap();

        assert_eq!(result, SourceResultType::HaveMoreOutput);
        assert_eq!(output.size(), 1);
        assert!(output.data[0].is_null(0));
        assert_eq!(output.data[1].get_value(0).to_string(), "20");
    }

    #[test]
    fn any_join_column_bindings_resolve_against_full_children() {
        let left = paro_planner::operator::LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                0,
                vec![vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                ))]],
                vec!["l".to_string()],
                vec![LogicalType::Integer],
            ),
        );
        let right = paro_planner::operator::LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                1,
                vec![vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(2),
                    LogicalType::Integer,
                ))]],
                vec!["r".to_string()],
                vec![LogicalType::Integer],
            ),
        );
        let ctx = paro_planner::binder::context::BindContext::new();
        let mut plan = paro_planner::operator::LogicalOperator::Join(
            paro_planner::operator::Join::Any(Box::new(paro_planner::operator::AnyJoin::new(
                JoinType::Inner,
                paro_planner::plan::LogicalPlan::new(&ctx, left),
                paro_planner::plan::LogicalPlan::new(&ctx, right),
                Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                )),
            ))),
        );

        crate::column_binding_resolver::ColumnBindingResolver::resolve(&mut plan)
            .expect("resolver should succeed");
        let paro_planner::operator::LogicalOperator::Join(paro_planner::operator::Join::Any(any)) =
            plan
        else {
            panic!("expected any join");
        };
        let Expression::Comparison(expr) = &any.condition else {
            panic!("expected comparison condition");
        };
        let Expression::Reference(left_ref) = expr.left.as_ref() else {
            panic!("left side should resolve to reference");
        };
        let Expression::Reference(right_ref) = expr.right.as_ref() else {
            panic!("right side should resolve to reference");
        };
        assert_eq!(left_ref.index, 0);
        assert_eq!(right_ref.index, 1);
    }
}
