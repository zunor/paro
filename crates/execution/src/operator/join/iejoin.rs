// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical IE join for dual range predicates.

use std::any::Any;
use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::mem::size_of;
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
use crate::operator::state::ProgressData;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorSinkCombineInput, OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
    OperatorState,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorResultType, SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

#[derive(Debug, Clone, Copy)]
struct RowLocation {
    chunk_idx: usize,
    row_idx: usize,
    global_idx: usize,
}

#[derive(Debug, Clone)]
struct IEJoinConditionIndex {
    key_values: Vec<Value>,
    sorted_global_indices: Vec<usize>,
}

#[derive(Debug)]
struct IEJoinMaterializedRhs {
    payload_chunks: Vec<Chunk>,
    condition_chunks: Vec<Chunk>,
    row_locations: Vec<RowLocation>,
    condition_indexes: Vec<IEJoinConditionIndex>,
    row_count: usize,
}

struct IEJoinGlobalSinkState {
    rhs_payload_chunks: Arc<Mutex<RetainedChunkVec>>,
    rhs_condition_chunks: Arc<Mutex<RetainedChunkVec>>,
    rhs_row_count: Mutex<usize>,
    materialized: Mutex<Option<Arc<IEJoinMaterializedRhs>>>,
    right_outer: Arc<OuterJoinMarker>,
    peak_memory_bytes: AtomicUsize,
}

impl IEJoinGlobalSinkState {
    fn new(enable_right_outer: bool, memory: MemoryAccountingContext) -> Self {
        Self {
            rhs_payload_chunks: Arc::new(Mutex::new(RetainedChunkVec::new(memory.clone()))),
            rhs_condition_chunks: Arc::new(Mutex::new(RetainedChunkVec::new(memory))),
            rhs_row_count: Mutex::new(0),
            materialized: Mutex::new(None),
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
            + self
                .materialized
                .lock()
                .unwrap()
                .as_ref()
                .map(|materialized| materialized.auxiliary_memory_usage_bytes())
                .unwrap_or(0)
    }
}

impl fmt::Debug for IEJoinGlobalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IEJoinGlobalSinkState")
            .field("row_count", &self.row_count())
            .field("materialized", &self.materialized.lock().unwrap().is_some())
            .field("right_outer_enabled", &self.right_outer.enabled())
            .finish()
    }
}

impl GlobalSinkState for IEJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "IEJoinGlobalSinkState"
    }
}

#[derive(Debug)]
struct IEJoinLocalSinkState {
    local_payload_chunks: RetainedChunkVec,
    local_condition_chunks: RetainedChunkVec,
    local_row_count: usize,
    right_condition_executors: Vec<ExpressionExecutor>,
}

impl LocalSinkState for IEJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl IEJoinLocalSinkState {
    fn memory_usage_bytes(&self) -> usize {
        self.local_payload_chunks.retained_bytes() + self.local_condition_chunks.retained_bytes()
    }
}

#[derive(Debug)]
struct IEJoinGlobalSourceState {
    payload_chunks: Arc<Mutex<RetainedChunkVec>>,
    right_outer: Arc<OuterJoinMarker>,
    join_type: JoinType,
}

impl GlobalSourceState for IEJoinGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct IEJoinLocalSourceState {
    scan_state: OuterJoinScanState,
}

impl LocalSourceState for IEJoinLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct IEJoinOperatorState {
    input_initialized: bool,
    row_prepared: bool,
    left_condition_chunk: Chunk,
    left_condition_executors: Vec<ExpressionExecutor>,
    left_row_idx: usize,
    candidate_global_indices: Vec<usize>,
    candidate_pos: usize,
    row_has_unknown: bool,
}

impl OperatorState for IEJoinOperatorState {
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

pub struct IEJoin {
    pub join: PhysicalJoin,
    pub left: Arc<dyn PhysicalOperator>,
    pub right: Arc<dyn PhysicalOperator>,
    pub join_type: JoinType,
    pub conditions: Vec<JoinCondition>,
    pub types: Vec<LogicalType>,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

fn values_memory_usage(values: &Vec<Value>) -> usize {
    values.capacity() * size_of::<Value>()
        + values.iter().map(Value::allocation_size).sum::<usize>()
}

fn iejoin_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        paro_common::allocator::MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}

impl IEJoinMaterializedRhs {
    fn auxiliary_memory_usage_bytes(&self) -> usize {
        self.row_locations.capacity() * size_of::<RowLocation>()
            + self.condition_indexes.capacity() * size_of::<IEJoinConditionIndex>()
            + self
                .condition_indexes
                .iter()
                .map(|index| {
                    values_memory_usage(&index.key_values)
                        + index.sorted_global_indices.capacity() * size_of::<usize>()
                })
                .sum::<usize>()
    }
}

impl IEJoin {
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
                "{} IE join result construction",
                join_type
            )))
        }
    }

    pub fn supports_comparison(comparison: JoinComparisonType) -> bool {
        matches!(
            comparison,
            JoinComparisonType::LessThan
                | JoinComparisonType::LessThanOrEqual
                | JoinComparisonType::GreaterThan
                | JoinComparisonType::GreaterThanOrEqual
        )
    }

    pub fn supports_conditions(conditions: &[JoinCondition]) -> bool {
        conditions.len() == 2
            && conditions
                .iter()
                .all(|condition| Self::supports_comparison(condition.comparison))
    }

    fn validate_conditions(conditions: &[JoinCondition]) -> Result<()> {
        if !Self::supports_conditions(conditions) {
            return Err(paro_error::not_implemented(
                "IEJoin requires exactly two range predicates",
            ));
        }
        Ok(())
    }

    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        conditions: Vec<JoinCondition>,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Result<Self> {
        Self::validate_conditions(&conditions)?;
        let join = PhysicalJoin::new(
            left.clone(),
            right.clone(),
            join_type,
            left_projection_map,
            right_projection_map,
        );
        let types = join.types.clone();
        Ok(Self {
            join,
            left,
            right,
            join_type,
            conditions,
            types,
            sink_state: Mutex::new(None),
        })
    }

    fn condition_info(&self) -> String {
        self.conditions
            .iter()
            .map(format_join_condition)
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn uses_right_outer_marker(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
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
            _ => result.set_cardinality(0),
        }
        Ok(())
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

    fn condition_types(&self, use_left: bool) -> Vec<LogicalType> {
        self.conditions
            .iter()
            .map(|condition| {
                if use_left {
                    condition.left.return_type()
                } else {
                    condition.right.return_type()
                }
            })
            .collect()
    }

    fn ensure_input_state(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        state: &mut IEJoinOperatorState,
    ) -> Result<()> {
        if state.input_initialized {
            return Ok(());
        }

        let mut left_columns = Vec::with_capacity(self.conditions.len());
        for executor in &mut state.left_condition_executors {
            left_columns.push(executor.execute_expression(0, input, None, input.size(), ctx)?);
        }
        let mut left_condition_chunk =
            Chunk::from_arc_vectors(left_columns, input.allocator().clone());
        left_condition_chunk.set_cardinality(input.size());
        state.left_condition_chunk =
            Self::materialize_chunk(&left_condition_chunk, &self.condition_types(true))?;
        state.input_initialized = true;
        state.row_prepared = false;
        state.left_row_idx = 0;
        state.candidate_global_indices.clear();
        state.candidate_pos = 0;
        state.row_has_unknown = false;
        Ok(())
    }

    fn reset_input_state(&self, state: &mut IEJoinOperatorState) {
        state.input_initialized = false;
        state.row_prepared = false;
        state.left_row_idx = 0;
        state.candidate_global_indices.clear();
        state.candidate_pos = 0;
        state.row_has_unknown = false;
        state.left_condition_chunk.set_cardinality(0);
    }

    fn advance_row(&self, state: &mut IEJoinOperatorState) {
        state.left_row_idx += 1;
        state.row_prepared = false;
        state.candidate_global_indices.clear();
        state.candidate_pos = 0;
        state.row_has_unknown = false;
    }

    fn value_cmp(left: &Value, right: &Value) -> CmpOrdering {
        left.partial_cmp(right).unwrap_or(CmpOrdering::Equal)
    }

    fn lower_bound(values: &[Value], target: &Value) -> usize {
        let mut left = 0;
        let mut right = values.len();
        while left < right {
            let mid = left + (right - left) / 2;
            if Self::value_cmp(&values[mid], target).is_lt() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    fn upper_bound(values: &[Value], target: &Value) -> usize {
        let mut left = 0;
        let mut right = values.len();
        while left < right {
            let mid = left + (right - left) / 2;
            if Self::value_cmp(&values[mid], target).is_le() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    fn compute_condition_candidates(
        &self,
        left_value: &Value,
        comparison: JoinComparisonType,
        index: &IEJoinConditionIndex,
    ) -> Vec<usize> {
        if left_value.is_null() || index.key_values.is_empty() {
            return Vec::new();
        }

        let (start, end) = match comparison {
            JoinComparisonType::LessThan => (
                Self::upper_bound(&index.key_values, left_value),
                index.key_values.len(),
            ),
            JoinComparisonType::LessThanOrEqual => (
                Self::lower_bound(&index.key_values, left_value),
                index.key_values.len(),
            ),
            JoinComparisonType::GreaterThan => {
                (0, Self::lower_bound(&index.key_values, left_value))
            }
            JoinComparisonType::GreaterThanOrEqual => {
                (0, Self::upper_bound(&index.key_values, left_value))
            }
            _ => (0, 0),
        };

        let mut candidates = index.sorted_global_indices[start..end].to_vec();
        candidates.sort_unstable();
        candidates
    }

    fn intersect_sorted_global_ids(left: &[usize], right: &[usize]) -> Vec<usize> {
        let mut result = Vec::with_capacity(left.len().min(right.len()));
        let mut left_idx = 0;
        let mut right_idx = 0;
        while left_idx < left.len() && right_idx < right.len() {
            match left[left_idx].cmp(&right[right_idx]) {
                CmpOrdering::Less => left_idx += 1,
                CmpOrdering::Greater => right_idx += 1,
                CmpOrdering::Equal => {
                    result.push(left[left_idx]);
                    left_idx += 1;
                    right_idx += 1;
                }
            }
        }
        result
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

    fn evaluate_row_match(
        &self,
        state: &IEJoinOperatorState,
        condition_chunk: &Chunk,
        right_row: usize,
    ) -> RowMatchState {
        let mut saw_null = false;
        for (cond_idx, condition) in self.conditions.iter().enumerate() {
            let left_value =
                state.left_condition_chunk.data[cond_idx].get_value(state.left_row_idx);
            let right_value = condition_chunk.data[cond_idx].get_value(right_row);
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

    fn mark_row_has_unknown(
        &self,
        state: &IEJoinOperatorState,
        materialized: &IEJoinMaterializedRhs,
    ) -> bool {
        for condition_chunk in &materialized.condition_chunks {
            for right_row in 0..condition_chunk.size() {
                if self.evaluate_row_match(state, condition_chunk, right_row)
                    == RowMatchState::Unknown
                {
                    return true;
                }
            }
        }
        false
    }

    fn prepare_current_row(
        &self,
        state: &mut IEJoinOperatorState,
        materialized: &IEJoinMaterializedRhs,
    ) {
        if state.row_prepared {
            return;
        }

        let mut candidates = Vec::new();
        for (cond_idx, condition) in self.conditions.iter().enumerate() {
            let left_value =
                state.left_condition_chunk.data[cond_idx].get_value(state.left_row_idx);
            let condition_candidates = self.compute_condition_candidates(
                &left_value,
                condition.comparison,
                &materialized.condition_indexes[cond_idx],
            );
            if cond_idx == 0 {
                candidates = condition_candidates;
            } else {
                candidates = Self::intersect_sorted_global_ids(&candidates, &condition_candidates);
            }
            if candidates.is_empty() {
                break;
            }
        }

        state.row_prepared = true;
        state.candidate_pos = 0;
        state.candidate_global_indices = candidates;
        state.row_has_unknown = matches!(self.join_type, JoinType::Mark)
            && state.candidate_global_indices.is_empty()
            && self.mark_row_has_unknown(state, materialized);
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
        payload_chunks: &[Chunk],
        location: RowLocation,
    ) -> Result<()> {
        let right_chunk = payload_chunks.get(location.chunk_idx).ok_or_else(|| {
            paro_error::internal(format!("Build chunk {} missing", location.chunk_idx))
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
                target.set_value(output_row, &source.get_value(location.row_idx));
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
                target.set_value(output_row, &source.get_value(location.row_idx));
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
        payload_chunks: &[Chunk],
        location: RowLocation,
    ) -> Result<()> {
        self.append_left_projection(output, output_row, input, input_row)?;
        self.append_right_projection(output, output_row, payload_chunks, location)
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

    fn build_condition_index(
        condition_chunks: &[Chunk],
        condition_idx: usize,
    ) -> IEJoinConditionIndex {
        let mut pairs = Vec::new();
        let mut global_idx = 0usize;
        for chunk in condition_chunks {
            for row_idx in 0..chunk.size() {
                let value = chunk.data[condition_idx].get_value(row_idx);
                if !value.is_null() {
                    pairs.push((value, global_idx));
                }
                global_idx += 1;
            }
        }
        pairs.sort_by(|(left_value, left_idx), (right_value, right_idx)| {
            Self::value_cmp(left_value, right_value).then(left_idx.cmp(right_idx))
        });
        IEJoinConditionIndex {
            key_values: pairs.iter().map(|(value, _)| value.clone()).collect(),
            sorted_global_indices: pairs.into_iter().map(|(_, idx)| idx).collect(),
        }
    }

    fn materialize_rhs(&self, sink: &IEJoinGlobalSinkState) -> Result<Arc<IEJoinMaterializedRhs>> {
        let mut guard = sink.materialized.lock().unwrap();
        if let Some(materialized) = guard.as_ref() {
            return Ok(materialized.clone());
        }

        let payload_chunks = sink.rhs_payload_chunks.lock().unwrap().clone_chunks();
        let condition_chunks = sink.rhs_condition_chunks.lock().unwrap().clone_chunks();
        if payload_chunks.len() != condition_chunks.len() {
            return Err(paro_error::internal(
                "IEJoin payload/condition chunk count mismatch".to_string(),
            ));
        }

        let mut row_locations = Vec::new();
        let mut global_idx = 0usize;
        for (chunk_idx, chunk) in payload_chunks.iter().enumerate() {
            let condition_chunk = condition_chunks.get(chunk_idx).ok_or_else(|| {
                paro_error::internal(format!("IEJoin condition chunk {} missing", chunk_idx))
            })?;
            if chunk.size() != condition_chunk.size() {
                return Err(paro_error::internal(format!(
                    "IEJoin payload/condition cardinality mismatch at chunk {}",
                    chunk_idx
                )));
            }
            for row_idx in 0..chunk.size() {
                row_locations.push(RowLocation {
                    chunk_idx,
                    row_idx,
                    global_idx,
                });
                global_idx += 1;
            }
        }

        let materialized = Arc::new(IEJoinMaterializedRhs {
            payload_chunks,
            condition_chunks: condition_chunks.clone(),
            row_locations,
            condition_indexes: (0..self.conditions.len())
                .map(|condition_idx| Self::build_condition_index(&condition_chunks, condition_idx))
                .collect(),
            row_count: global_idx,
        });
        *guard = Some(materialized.clone());
        drop(guard);
        sink.record_peak(sink.current_memory_bytes());
        Ok(materialized)
    }

    fn mark_right_match(&self, sink: &IEJoinGlobalSinkState, global_idx: usize) {
        if self.uses_right_outer_marker() {
            sink.right_outer.set_match(global_idx);
        }
    }
}

impl fmt::Debug for IEJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IEJoin")
            .field("join_type", &self.join_type)
            .field("conditions", &self.condition_info())
            .finish()
    }
}

impl PhysicalOperator for IEJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::IEJoin
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(sink_state) = sink_state.as_any().downcast_ref::<IEJoinGlobalSinkState>() else {
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

    fn estimated_cardinality(&self) -> usize {
        self.left.estimated_cardinality()
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
        Ok(Box::new(IEJoinOperatorState {
            input_initialized: false,
            row_prepared: false,
            left_condition_chunk: Chunk::try_init_empty(
                &self.condition_types(true),
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            left_condition_executors: self
                .conditions
                .iter()
                .map(|condition| ExpressionExecutor::new(&condition.left))
                .collect(),
            left_row_idx: 0,
            candidate_global_indices: Vec::new(),
            candidate_pos: 0,
            row_has_unknown: false,
        }))
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Self::validate_join_type(self.join_type)?;
        Self::validate_conditions(&self.conditions)?;
        Ok(Box::new(IEJoinGlobalSinkState::new(
            self.uses_right_outer_marker(),
            iejoin_memory_context(ctx),
        )))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let memory = iejoin_memory_context(ctx);
        Ok(Box::new(IEJoinLocalSinkState {
            local_payload_chunks: RetainedChunkVec::new(memory.clone()),
            local_condition_chunks: RetainedChunkVec::new(memory),
            local_row_count: 0,
            right_condition_executors: self
                .conditions
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
            .downcast_mut::<IEJoinLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin local sink".to_string()))?;

        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }

        lstate
            .local_payload_chunks
            .push(Self::materialize_chunk(chunk, self.right.types())?)?;
        lstate.local_row_count += chunk.size();

        let mut condition_columns = Vec::with_capacity(self.conditions.len());
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
            &self.condition_types(false),
        )?)?;
        if let Some(gstate) = input
            .global_state
            .as_any()
            .downcast_ref::<IEJoinGlobalSinkState>()
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
            .downcast_ref::<IEJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin global sink".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<IEJoinLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin local sink".to_string()))?;

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
            *gstate.materialized.lock().unwrap() = None;
            gstate.record_peak(gstate.current_memory_bytes());
        }

        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, _input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        Ok(SinkFinalizeType::Ready)
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Self::validate_join_type(self.join_type)?;
        Self::validate_conditions(&self.conditions)?;

        let create_source_state =
            |sink: &IEJoinGlobalSinkState| -> Result<Box<dyn GlobalSourceState>> {
                let _materialized = self.materialize_rhs(sink)?;
                Ok(Box::new(IEJoinGlobalSourceState {
                    payload_chunks: Arc::clone(&sink.rhs_payload_chunks),
                    right_outer: sink.right_outer.clone(),
                    join_type: self.join_type,
                }))
            };

        if let Some(sink_state) = sink_state {
            let sink = sink_state
                .as_any()
                .downcast_ref::<IEJoinGlobalSinkState>()
                .ok_or_else(|| {
                    paro_error::internal("Invalid IEJoin sink state for source".to_string())
                })?;
            return create_source_state(sink);
        }

        let internal_sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("IEJoin requires sink state for source phase".to_string())
        })?;
        let sink = internal_sink
            .as_any()
            .downcast_ref::<IEJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid IEJoin sink state for source".to_string())
            })?;
        create_source_state(sink)
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(IEJoinLocalSourceState::default()))
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
        Self::validate_conditions(&self.conditions)?;

        let state = state
            .as_any_mut()
            .downcast_mut::<IEJoinOperatorState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin operator state".to_string()))?;

        self.ensure_output_chunk(chunk)?;
        if input.size() == 0 {
            chunk.set_cardinality(0);
            self.reset_input_state(state);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let sink = self
            .sink_state()
            .ok_or_else(|| paro_error::internal("IEJoin requires sink state".to_string()))?;
        let gsink = sink
            .as_any()
            .downcast_ref::<IEJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin sink state".to_string()))?;
        let materialized = self.materialize_rhs(gsink)?;

        if materialized.row_count == 0 {
            if self.join.empty_result_if_rhs_is_empty() {
                chunk.set_cardinality(0);
            } else {
                self.construct_empty_join_result(input, chunk)?;
            }
            self.reset_input_state(state);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        self.ensure_input_state(ctx, input, state)?;

        let mut output_count = 0;
        let output_capacity = chunk.capacity();

        while output_count < output_capacity && state.left_row_idx < input.size() {
            self.prepare_current_row(state, &materialized);

            match self.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer => {
                    if state.candidate_pos < state.candidate_global_indices.len() {
                        let global_idx = state.candidate_global_indices[state.candidate_pos];
                        let location = materialized.row_locations[global_idx];
                        debug_assert_eq!(location.global_idx, global_idx);
                        self.mark_right_match(gsink, global_idx);
                        self.append_join_match_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            &materialized.payload_chunks,
                            location,
                        )?;
                        state.candidate_pos += 1;
                        output_count += 1;
                    } else {
                        if matches!(self.join_type, JoinType::Left | JoinType::Outer)
                            && state.candidate_global_indices.is_empty()
                        {
                            self.append_unmatched_left_row(
                                chunk,
                                output_count,
                                input,
                                state.left_row_idx,
                            )?;
                            output_count += 1;
                        }
                        self.advance_row(state);
                    }
                }
                JoinType::Semi => {
                    if !state.candidate_global_indices.is_empty() {
                        self.append_left_only_row(chunk, output_count, input, state.left_row_idx)?;
                        output_count += 1;
                    }
                    self.advance_row(state);
                }
                JoinType::Anti => {
                    if state.candidate_global_indices.is_empty() {
                        self.append_left_only_row(chunk, output_count, input, state.left_row_idx)?;
                        output_count += 1;
                    }
                    self.advance_row(state);
                }
                JoinType::Mark => {
                    let marker = if !state.candidate_global_indices.is_empty() {
                        Some(true)
                    } else if state.row_has_unknown {
                        None
                    } else {
                        Some(false)
                    };
                    self.append_mark_row(chunk, output_count, input, state.left_row_idx, marker)?;
                    output_count += 1;
                    self.advance_row(state);
                }
                JoinType::Single => {
                    let match_count = state.candidate_global_indices.len();
                    if match_count > 1 {
                        return Err(paro_error::invalid_input(
                            "More than one row returned by SINGLE join",
                        ));
                    }
                    if match_count == 1 {
                        let global_idx = state.candidate_global_indices[0];
                        let location = materialized.row_locations[global_idx];
                        debug_assert_eq!(location.global_idx, global_idx);
                        self.append_join_match_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            &materialized.payload_chunks,
                            location,
                        )?;
                    } else {
                        self.append_unmatched_left_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                        )?;
                    }
                    output_count += 1;
                    self.advance_row(state);
                }
                JoinType::RightSemi | JoinType::RightAnti => {
                    for global_idx in &state.candidate_global_indices {
                        self.mark_right_match(gsink, *global_idx);
                    }
                    self.advance_row(state);
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
        Self::validate_conditions(&self.conditions)?;

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<IEJoinGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin global source".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<IEJoinLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid IEJoin local source".to_string()))?;

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
        let payload_chunks = gstate.payload_chunks.lock().unwrap();
        let count = gstate.right_outer.scan(
            payload_chunks.as_slice(),
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

    fn get_progress(&self, _gstate: &dyn GlobalSourceState) -> ProgressData {
        ProgressData::invalid()
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
    use super::{
        IEJoin, IEJoinConditionIndex, IEJoinGlobalSinkState, IEJoinMaterializedRhs, RowLocation,
    };
    use std::sync::{Arc, Mutex};

    use paro_common::allocator::MemoryTag;
    use paro_common::chunk::Chunk;
    use paro_common::memory::MemoryAccountingClass;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use paro_context::StatementContext;
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

    use crate::execution_context::ExecutionContext;
    use crate::memory_runtime::RetainedChunkVec;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::{EmptyGlobalOperatorState, GlobalSinkState};
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;

    fn test_session() -> Arc<StatementContext> {
        paro_context::test_support::TestStatementContextBuilder::minimal().build()
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

    fn interval_join(join_type: JoinType) -> IEJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![
            LogicalType::Integer,
            LogicalType::Integer,
        ])) as Arc<dyn PhysicalOperator>;
        IEJoin::new(
            left,
            right,
            join_type,
            vec![
                JoinCondition::new(
                    left_ref(0),
                    right_ref(0),
                    JoinComparisonType::GreaterThanOrEqual,
                ),
                JoinCondition::new(
                    left_ref(0),
                    right_ref(1),
                    JoinComparisonType::LessThanOrEqual,
                ),
            ],
            vec![],
            vec![],
        )
        .expect("IEJoin should construct")
    }

    fn build_condition_indexes(condition_chunks: &[Chunk]) -> Vec<IEJoinConditionIndex> {
        (0..2)
            .map(|condition_idx| {
                let mut pairs = Vec::new();
                let mut global_idx = 0usize;
                for chunk in condition_chunks {
                    for row_idx in 0..chunk.size() {
                        let value = chunk.data[condition_idx].get_value(row_idx);
                        if !value.is_null() {
                            pairs.push((value, global_idx));
                        }
                        global_idx += 1;
                    }
                }
                pairs.sort_by(|(left_value, left_idx), (right_value, right_idx)| {
                    left_value
                        .partial_cmp(right_value)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(left_idx.cmp(right_idx))
                });
                IEJoinConditionIndex {
                    key_values: pairs.iter().map(|(value, _)| value.clone()).collect(),
                    sorted_global_indices: pairs.into_iter().map(|(_, idx)| idx).collect(),
                }
            })
            .collect()
    }

    fn set_materialized_sink_state(
        join: &IEJoin,
        payload_chunks: Vec<Chunk>,
        condition_chunks: Vec<Chunk>,
        enable_right_outer: bool,
    ) {
        let mut row_locations = Vec::new();
        let mut row_count = 0usize;
        for (chunk_idx, chunk) in payload_chunks.iter().enumerate() {
            for row_idx in 0..chunk.size() {
                row_locations.push(RowLocation {
                    chunk_idx,
                    row_idx,
                    global_idx: row_count,
                });
                row_count += 1;
            }
        }

        let mut retained_payload =
            RetainedChunkVec::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
        for chunk in payload_chunks.iter().cloned() {
            retained_payload.push(chunk).unwrap();
        }
        let mut retained_conditions =
            RetainedChunkVec::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
        for chunk in condition_chunks.iter().cloned() {
            retained_conditions.push(chunk).unwrap();
        }

        let sink_state = Arc::new(IEJoinGlobalSinkState {
            rhs_payload_chunks: Arc::new(Mutex::new(retained_payload)),
            rhs_condition_chunks: Arc::new(Mutex::new(retained_conditions)),
            rhs_row_count: Mutex::new(row_count),
            materialized: Mutex::new(Some(Arc::new(IEJoinMaterializedRhs {
                payload_chunks,
                condition_chunks: condition_chunks.clone(),
                row_locations,
                condition_indexes: build_condition_indexes(&condition_chunks),
                row_count,
            }))),
            right_outer: Arc::new(
                crate::operator::join::outer_join_marker::OuterJoinMarker::new(enable_right_outer),
            ),
            peak_memory_bytes: std::sync::atomic::AtomicUsize::new(0),
        });
        if enable_right_outer {
            sink_state.right_outer.add_rows(row_count);
        }
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);
    }

    #[test]
    fn inner_ie_join_intersects_two_ranges() {
        let join = interval_join(JoinType::Inner);
        let payload = Chunk::from_arc_vectors(
            vec![
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 4, 7],
                    paro_common::test_utils::test_allocator(),
                )),
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3, 6, 10],
                    paro_common::test_utils::test_allocator(),
                )),
            ],
            paro_common::test_utils::test_allocator(),
        );
        set_materialized_sink_state(&join, vec![payload.clone()], vec![payload], false);

        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[2, 5, 9],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let session = create_test_session();
        let thread = ThreadContext::new(0, 0);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join
            .get_operator_state(&ctx)
            .expect("operator state should construct");
        let mut output = paro_common::test_utils::test_chunk_with_capacity(join.types(), 8);

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &EmptyGlobalOperatorState::default(),
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("IEJoin execution should succeed");

        assert_eq!(
            result,
            crate::result_type::OperatorResultType::NeedMoreInput
        );
        assert_eq!(output.size(), 3);
        assert_eq!(output.data[0].get_value(0), Value::Integer(2));
        assert_eq!(output.data[1].get_value(0), Value::Integer(1));
        assert_eq!(output.data[2].get_value(0), Value::Integer(3));
        assert_eq!(output.data[0].get_value(1), Value::Integer(5));
        assert_eq!(output.data[1].get_value(1), Value::Integer(4));
        assert_eq!(output.data[2].get_value(1), Value::Integer(6));
        assert_eq!(output.data[0].get_value(2), Value::Integer(9));
        assert_eq!(output.data[1].get_value(2), Value::Integer(7));
        assert_eq!(output.data[2].get_value(2), Value::Integer(10));
    }

    #[test]
    fn single_ie_join_errors_on_multiple_matches() {
        let join = interval_join(JoinType::Single);
        let payload = Chunk::from_arc_vectors(
            vec![
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 4],
                    paro_common::test_utils::test_allocator(),
                )),
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[7, 6],
                    paro_common::test_utils::test_allocator(),
                )),
            ],
            paro_common::test_utils::test_allocator(),
        );
        set_materialized_sink_state(&join, vec![payload.clone()], vec![payload], false);

        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let session = create_test_session();
        let thread = ThreadContext::new(0, 0);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join
            .get_operator_state(&ctx)
            .expect("operator state should construct");
        let mut output = paro_common::test_utils::test_chunk_with_capacity(join.types(), 4);

        let err = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &EmptyGlobalOperatorState::default(),
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect_err("SINGLE IEJoin should reject multiple matches");
        assert!(err
            .to_string()
            .contains("More than one row returned by SINGLE join"));
    }

    #[test]
    fn mark_ie_join_reports_unknown_when_only_null_comparisons_exist() {
        let join = interval_join(JoinType::Mark);
        let payload = Chunk::from_arc_vectors(
            vec![
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 5],
                    paro_common::test_utils::test_allocator(),
                )),
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3, 7],
                    paro_common::test_utils::test_allocator(),
                )),
            ],
            paro_common::test_utils::test_allocator(),
        );
        set_materialized_sink_state(&join, vec![payload.clone()], vec![payload], false);

        let mut input_vec = paro_common::test_utils::test_i32_vector_with_allocator(
            &[0],
            paro_common::test_utils::test_allocator(),
        );
        input_vec.set_null(0, true);
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(input_vec)],
            paro_common::test_utils::test_allocator(),
        );
        let session = create_test_session();
        let thread = ThreadContext::new(0, 0);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join
            .get_operator_state(&ctx)
            .expect("operator state should construct");
        let mut output = paro_common::test_utils::test_chunk_with_capacity(join.types(), 2);

        join.execute(
            &ctx,
            &input,
            &mut output,
            &EmptyGlobalOperatorState::default(),
            state.as_mut(),
            crate::operator::state::test_operator_memory_scope(),
        )
        .expect("MARK IEJoin execution should succeed");

        assert_eq!(output.size(), 1);
        assert!(output.data[1].is_null(0));
    }

    #[test]
    fn iejoin_state_caches_condition_executors() {
        let join = interval_join(JoinType::Inner);
        let session = create_test_session();
        let thread = ThreadContext::new(0, 0);
        let ctx = ExecutionContext::new(session, &thread, None);

        let operator_state = join.get_operator_state(&ctx).unwrap();
        let operator_state = operator_state
            .as_any()
            .downcast_ref::<super::IEJoinOperatorState>()
            .unwrap();
        assert_eq!(operator_state.left_condition_executors.len(), 2);

        let local_sink_state = join.get_local_sink_state(&ctx).unwrap();
        let local_sink_state = local_sink_state
            .as_any()
            .downcast_ref::<super::IEJoinLocalSinkState>()
            .unwrap();
        assert_eq!(local_sink_state.right_condition_executors.len(), 2);
    }
}
