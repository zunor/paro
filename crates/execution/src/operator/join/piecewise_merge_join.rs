// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical piecewise merge join for single range predicates.

use std::any::Any;
use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_join_condition;
use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
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
use crate::sorting::sort::Sort;

#[derive(Debug, Clone, Copy)]
struct SortedRowLocation {
    chunk_idx: usize,
    row_idx: usize,
    global_idx: usize,
}

#[derive(Debug)]
struct PiecewiseMergeJoinMaterializedRhs {
    payload_chunks: Vec<Chunk>,
    key_values: Vec<Value>,
    key_locations: Vec<SortedRowLocation>,
    row_count: usize,
    null_row_count: usize,
}

struct PiecewiseMergeJoinGlobalSinkState {
    sort_state: Box<dyn GlobalSinkState>,
    materialized: Mutex<Option<Arc<PiecewiseMergeJoinMaterializedRhs>>>,
    right_key_executor: Mutex<ExpressionExecutor>,
    right_outer: Arc<OuterJoinMarker>,
}

impl PiecewiseMergeJoinMaterializedRhs {
    fn memory_usage_bytes(&self) -> usize {
        self.payload_chunks.capacity() * size_of::<Chunk>()
            + self
                .payload_chunks
                .iter()
                .map(Chunk::get_allocation_size)
                .sum::<usize>()
            + self.key_values.capacity() * size_of::<Value>()
            + self
                .key_values
                .iter()
                .map(Value::allocation_size)
                .sum::<usize>()
            + self.key_locations.capacity() * size_of::<SortedRowLocation>()
    }
}

impl PiecewiseMergeJoinGlobalSinkState {
    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sort_sink_state) = self
            .sort_state
            .as_any()
            .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };

        let materialized_peak = self
            .materialized
            .lock()
            .unwrap()
            .as_ref()
            .map(|materialized| materialized.memory_usage_bytes() as u64)
            .unwrap_or(0);
        let sort_peak = sort_sink_state.peak_reservation() as u64;

        ExplainRuntimeStats {
            spilled: Some(sort_sink_state.is_external()),
            peak_memory_bytes: Some(sort_peak.max(materialized_peak)),
            temp_storage_bytes: None,
        }
    }
}

impl fmt::Debug for PiecewiseMergeJoinGlobalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let materialized = self.materialized.lock().unwrap();
        f.debug_struct("PiecewiseMergeJoinGlobalSinkState")
            .field("sort_state_name", &self.sort_state.sink_state_name())
            .field("materialized", &materialized.is_some())
            .field("right_outer_enabled", &self.right_outer.enabled())
            .finish()
    }
}

impl GlobalSinkState for PiecewiseMergeJoinGlobalSinkState {
    fn state(&self) -> SinkFinalizeType {
        self.sort_state.state()
    }

    fn max_threads(&self, source_max_threads: usize) -> usize {
        self.sort_state.max_threads(source_max_threads)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "PiecewiseMergeJoinGlobalSinkState"
    }
}

#[derive(Debug, Default)]
struct PiecewiseMergeJoinLocalSinkState {
    sort_state: Option<Box<dyn LocalSinkState>>,
}

impl LocalSinkState for PiecewiseMergeJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct PiecewiseMergeJoinGlobalSourceState {
    payload_chunks: Arc<Vec<Chunk>>,
    right_outer: Arc<OuterJoinMarker>,
    join_type: JoinType,
}

impl GlobalSourceState for PiecewiseMergeJoinGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct PiecewiseMergeJoinLocalSourceState {
    scan_state: OuterJoinScanState,
}

impl LocalSourceState for PiecewiseMergeJoinLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct PiecewiseMergeJoinOperatorState {
    input_initialized: bool,
    row_prepared: bool,
    left_key_chunk: Chunk,
    left_key_executor: ExpressionExecutor,
    left_row_idx: usize,
    range_start: usize,
    range_end: usize,
    range_pos: usize,
    row_has_unknown: bool,
}

impl OperatorState for PiecewiseMergeJoinOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct PiecewiseMergeJoin {
    pub join: PhysicalJoin,
    pub left: Arc<dyn PhysicalOperator>,
    pub right: Arc<dyn PhysicalOperator>,
    pub join_type: JoinType,
    pub condition: JoinCondition,
    pub types: Vec<LogicalType>,
    sort: Arc<Sort>,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl PiecewiseMergeJoin {
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
                "{} piecewise merge join result construction",
                join_type
            )))
        }
    }

    fn validate_condition(condition: &JoinCondition) -> Result<()> {
        if !Self::supports_comparison(condition.comparison) {
            return Err(paro_error::not_implemented(format!(
                "{:?} piecewise merge join comparison",
                condition.comparison
            )));
        }
        Ok(())
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

    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        condition: JoinCondition,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Result<Self> {
        Self::validate_condition(&condition)?;
        let join = PhysicalJoin::new(
            left.clone(),
            right.clone(),
            join_type,
            left_projection_map,
            right_projection_map,
        );
        let types = join.types.clone();
        let orders = vec![OrderByNode {
            expression: condition.right.clone(),
            ascending: true,
            nulls_first: false,
        }];
        let sort = Arc::new(Sort::new(orders, right.types().to_vec(), vec![], false)?);

        Ok(Self {
            join,
            left,
            right,
            join_type,
            condition,
            types,
            sort,
            sink_state: Mutex::new(None),
        })
    }

    fn condition_info(&self) -> String {
        format_join_condition(&self.condition)
    }

    fn uses_right_outer_marker(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn ensure_output_chunk(&self, chunk: &mut Chunk) {
        if chunk.column_count() == 0 {
            *chunk = Chunk::initialize(&self.types, paro_common::vector::VECTOR_SIZE);
        }
    }

    fn construct_empty_join_result(&self, input: &Chunk, result: &mut Chunk) {
        match self.join_type {
            JoinType::Anti => {
                let sel = SelectionVector::incremental(input.size());
                construct_anti_join_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    result,
                );
            }
            JoinType::Mark => {
                construct_mark_join_result(
                    input,
                    &self.join.left_projection_map,
                    &vec![Some(false); input.size()],
                    result,
                );
            }
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                let sel = SelectionVector::incremental(input.size());
                construct_left_outer_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    &self.join.right_output_types,
                    result,
                );
            }
            _ => result.set_cardinality(0),
        }
    }

    fn materialize_chunk(chunk: &Chunk, types: &[LogicalType]) -> Chunk {
        let mut materialized = Chunk::initialize(types, chunk.size().max(1));
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
        materialized
    }

    fn ensure_input_state(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        state: &mut PiecewiseMergeJoinOperatorState,
    ) -> Result<()> {
        if state.input_initialized {
            return Ok(());
        }

        let key_vector =
            state
                .left_key_executor
                .execute_expression(0, input, None, input.size(), ctx)?;
        let mut key_chunk = Chunk::from_arc_vectors(vec![key_vector]);
        key_chunk.set_cardinality(input.size());

        state.left_key_chunk =
            Self::materialize_chunk(&key_chunk, &[self.condition.left.return_type()]);
        state.input_initialized = true;
        state.row_prepared = false;
        state.left_row_idx = 0;
        state.range_start = 0;
        state.range_end = 0;
        state.range_pos = 0;
        state.row_has_unknown = false;
        Ok(())
    }

    fn reset_input_state(&self, state: &mut PiecewiseMergeJoinOperatorState) {
        state.input_initialized = false;
        state.row_prepared = false;
        state.left_row_idx = 0;
        state.range_start = 0;
        state.range_end = 0;
        state.range_pos = 0;
        state.row_has_unknown = false;
        state.left_key_chunk.set_cardinality(0);
    }

    fn advance_row(&self, state: &mut PiecewiseMergeJoinOperatorState) {
        state.left_row_idx += 1;
        state.row_prepared = false;
        state.range_start = 0;
        state.range_end = 0;
        state.range_pos = 0;
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

    fn compute_match_range(
        &self,
        left_value: &Value,
        materialized: &PiecewiseMergeJoinMaterializedRhs,
    ) -> (usize, usize, bool) {
        let saw_unknown = if left_value.is_null() {
            materialized.row_count > 0
        } else {
            materialized.null_row_count > 0
        };

        if left_value.is_null() || materialized.key_values.is_empty() {
            return (0, 0, saw_unknown);
        }

        let (start, end) = match self.condition.comparison {
            JoinComparisonType::LessThan => (
                Self::upper_bound(&materialized.key_values, left_value),
                materialized.key_values.len(),
            ),
            JoinComparisonType::LessThanOrEqual => (
                Self::lower_bound(&materialized.key_values, left_value),
                materialized.key_values.len(),
            ),
            JoinComparisonType::GreaterThan => {
                (0, Self::lower_bound(&materialized.key_values, left_value))
            }
            JoinComparisonType::GreaterThanOrEqual => {
                (0, Self::upper_bound(&materialized.key_values, left_value))
            }
            _ => (0, 0),
        };

        (start, end, saw_unknown)
    }

    fn prepare_current_row(
        &self,
        state: &mut PiecewiseMergeJoinOperatorState,
        materialized: &PiecewiseMergeJoinMaterializedRhs,
    ) {
        if state.row_prepared {
            return;
        }

        let left_value = state.left_key_chunk.data[0].get_value(state.left_row_idx);
        let (start, end, saw_unknown) = self.compute_match_range(&left_value, materialized);
        state.row_prepared = true;
        state.range_start = start;
        state.range_end = end;
        state.range_pos = start;
        state.row_has_unknown = saw_unknown;
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
        location: SortedRowLocation,
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
        location: SortedRowLocation,
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

    fn materialize_sorted_rhs(
        &self,
        ctx: &ExecutionContext,
        sink: &PiecewiseMergeJoinGlobalSinkState,
    ) -> Result<Arc<PiecewiseMergeJoinMaterializedRhs>> {
        let mut guard = sink.materialized.lock().unwrap();
        if let Some(materialized) = guard.as_ref() {
            return Ok(materialized.clone());
        }

        let sort_gstate = self
            .sort
            .get_global_source_state(ctx, sink.sort_state.as_ref())?;
        let mut sort_lstate = self
            .sort
            .get_local_source_state(ctx, sort_gstate.as_ref())?;
        let mut chunk = Chunk::initialize(self.right.types(), paro_common::vector::VECTOR_SIZE);
        let allocator = ctx.allocator(MemoryTag::OrderBy);
        let mut payload_chunks = Vec::new();
        let mut key_values = Vec::new();
        let mut key_locations = Vec::new();
        let mut global_idx = 0usize;
        let mut null_row_count = 0usize;

        loop {
            let result =
                self.sort
                    .get_data(ctx, &mut chunk, sort_gstate.as_ref(), sort_lstate.as_mut())?;
            if chunk.size() > 0 {
                let copied = chunk.deep_copy_with_allocator(allocator.clone());
                let key_vector = sink.right_key_executor.lock().unwrap().execute_expression(
                    0,
                    &copied,
                    None,
                    copied.size(),
                    ctx,
                )?;
                for row_idx in 0..copied.size() {
                    let value = key_vector.get_value(row_idx);
                    if value.is_null() {
                        null_row_count += 1;
                    } else {
                        key_values.push(value);
                        key_locations.push(SortedRowLocation {
                            chunk_idx: payload_chunks.len(),
                            row_idx,
                            global_idx,
                        });
                    }
                    global_idx += 1;
                }
                payload_chunks.push(copied);
            }
            if matches!(result, SourceResultType::Finished) {
                break;
            }
            chunk.reset();
        }

        if sink.right_outer.enabled() {
            sink.right_outer.reset();
            sink.right_outer.add_rows(global_idx);
        }

        let materialized = Arc::new(PiecewiseMergeJoinMaterializedRhs {
            payload_chunks,
            key_values,
            key_locations,
            row_count: global_idx,
            null_row_count,
        });
        *guard = Some(materialized.clone());
        Ok(materialized)
    }

    fn mark_right_match(&self, sink: &PiecewiseMergeJoinGlobalSinkState, global_idx: usize) {
        if self.uses_right_outer_marker() {
            sink.right_outer.set_match(global_idx);
        }
    }
}

impl fmt::Debug for PiecewiseMergeJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PiecewiseMergeJoin")
            .field("join_type", &self.join_type)
            .field("condition", &self.condition_info())
            .finish()
    }
}

impl PhysicalOperator for PiecewiseMergeJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::PiecewiseMergeJoin
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

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(join_sink_state) = sink_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        join_sink_state.runtime_memory_stats()
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(PiecewiseMergeJoinOperatorState {
            input_initialized: false,
            row_prepared: false,
            left_key_chunk: Chunk::init_empty(&[self.condition.left.return_type()]),
            left_key_executor: ExpressionExecutor::new(&self.condition.left),
            left_row_idx: 0,
            range_start: 0,
            range_end: 0,
            range_pos: 0,
            row_has_unknown: false,
        }))
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Self::validate_join_type(self.join_type)?;
        Ok(Box::new(PiecewiseMergeJoinGlobalSinkState {
            sort_state: self.sort.get_global_sink_state(ctx)?,
            materialized: Mutex::new(None),
            right_key_executor: Mutex::new(ExpressionExecutor::new(&self.condition.right)),
            right_outer: Arc::new(OuterJoinMarker::new(self.uses_right_outer_marker())),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(PiecewiseMergeJoinLocalSinkState::default()))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join global sink".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PiecewiseMergeJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join local sink".to_string())
            })?;

        if lstate.sort_state.is_none() {
            lstate.sort_state = Some(self.sort.get_local_sink_state(ctx)?);
        }

        self.sort.sink(
            ctx,
            chunk,
            gstate.sort_state.as_ref(),
            lstate.sort_state.as_mut().unwrap().as_mut(),
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join global sink".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PiecewiseMergeJoinLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join local sink".to_string())
            })?;

        if lstate.sort_state.is_none() {
            return Ok(SinkCombineResultType::Finished);
        }

        self.sort.combine(
            ctx,
            gstate.sort_state.as_ref(),
            lstate.sort_state.as_mut().unwrap().as_mut(),
        )
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join global sink".to_string())
            })?;
        self.sort.finalize(gstate.sort_state.as_ref())
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Self::validate_join_type(self.join_type)?;

        let build_source_state = |sink: &PiecewiseMergeJoinGlobalSinkState| {
            let materialized = self.materialize_sorted_rhs(ctx, sink)?;
            Ok(Box::new(PiecewiseMergeJoinGlobalSourceState {
                payload_chunks: Arc::new(materialized.payload_chunks.clone()),
                right_outer: sink.right_outer.clone(),
                join_type: self.join_type,
            }) as Box<dyn GlobalSourceState>)
        };

        if let Some(sink_state) = sink_state {
            let sink = sink_state
                .as_any()
                .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
                .ok_or_else(|| {
                    paro_error::internal(
                        "Invalid piecewise merge join sink state for source".to_string(),
                    )
                })?;
            return build_source_state(sink);
        }

        let internal_sink = self.sink_state().ok_or_else(|| {
            paro_error::internal(
                "PiecewiseMergeJoin requires sink state for source phase".to_string(),
            )
        })?;
        let sink = internal_sink
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal(
                    "Invalid piecewise merge join sink state for source".to_string(),
                )
            })?;
        build_source_state(sink)
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(PiecewiseMergeJoinLocalSourceState::default()))
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        Self::validate_join_type(self.join_type)?;

        let state = state
            .as_any_mut()
            .downcast_mut::<PiecewiseMergeJoinOperatorState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join operator state".to_string())
            })?;

        self.ensure_output_chunk(chunk);
        if input.size() == 0 {
            chunk.set_cardinality(0);
            self.reset_input_state(state);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("PiecewiseMergeJoin requires sink state".to_string())
        })?;
        let gsink = sink
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join sink state".to_string())
            })?;
        let materialized = self.materialize_sorted_rhs(ctx, gsink)?;

        if materialized.row_count == 0 {
            if self.join.empty_result_if_rhs_is_empty() {
                chunk.set_cardinality(0);
            } else {
                self.construct_empty_join_result(input, chunk);
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
                    if state.range_pos < state.range_end {
                        let location = materialized.key_locations[state.range_pos];
                        self.mark_right_match(gsink, location.global_idx);
                        self.append_join_match_row(
                            chunk,
                            output_count,
                            input,
                            state.left_row_idx,
                            &materialized.payload_chunks,
                            location,
                        )?;
                        state.range_pos += 1;
                        output_count += 1;
                    } else {
                        if matches!(self.join_type, JoinType::Left | JoinType::Outer)
                            && state.range_start == state.range_end
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
                    if state.range_start < state.range_end {
                        self.append_left_only_row(chunk, output_count, input, state.left_row_idx)?;
                        output_count += 1;
                    }
                    self.advance_row(state);
                }
                JoinType::Anti => {
                    if state.range_start == state.range_end {
                        self.append_left_only_row(chunk, output_count, input, state.left_row_idx)?;
                        output_count += 1;
                    }
                    self.advance_row(state);
                }
                JoinType::Mark => {
                    let marker = if state.range_start < state.range_end {
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
                    let match_count = state.range_end - state.range_start;
                    if match_count > 1 {
                        return Err(paro_error::invalid_input(
                            "More than one row returned by SINGLE join",
                        ));
                    }
                    if match_count == 1 {
                        let location = materialized.key_locations[state.range_start];
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
                    for location in &materialized.key_locations[state.range_start..state.range_end]
                    {
                        self.mark_right_match(gsink, location.global_idx);
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

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join global source".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PiecewiseMergeJoinLocalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid piecewise merge join local source".to_string())
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
            *chunk = Chunk::initialize(&self.types, paro_common::vector::VECTOR_SIZE);
        }

        let mut build_chunk =
            Chunk::initialize(self.right.types(), paro_common::vector::VECTOR_SIZE);
        let count = gstate.right_outer.scan(
            gstate.payload_chunks.as_ref(),
            &mut lstate.scan_state,
            emit_found,
            &mut build_chunk,
        )?;
        if count == 0 {
            chunk.set_cardinality(0);
            return Ok(SourceResultType::Finished);
        }

        let build_sel = SelectionVector::incremental(count);
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
        }

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
        PiecewiseMergeJoin, PiecewiseMergeJoinGlobalSinkState, PiecewiseMergeJoinGlobalSourceState,
        PiecewiseMergeJoinMaterializedRhs, SortedRowLocation,
    };
    use std::sync::{Arc, Mutex};

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
    use paro_scheduler::task::InterruptState;

    use crate::execution_context::ExecutionContext;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::{
        EmptyGlobalOperatorState, EmptyGlobalSinkState, GlobalSinkState, OperatorSourceInput,
    };
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

    fn comparison_join(join_type: JoinType, comparison: JoinComparisonType) -> PiecewiseMergeJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        PiecewiseMergeJoin::new(
            left,
            right,
            join_type,
            JoinCondition::new(left_ref(0), right_ref(0), comparison),
            vec![],
            vec![],
        )
        .expect("piecewise merge join should construct")
    }

    fn set_materialized_sink_state(
        join: &PiecewiseMergeJoin,
        payload_chunks: Vec<Chunk>,
        key_values: Vec<Value>,
        key_locations: Vec<SortedRowLocation>,
        row_count: usize,
        null_row_count: usize,
        enable_right_outer: bool,
    ) {
        let sink_state = Arc::new(PiecewiseMergeJoinGlobalSinkState {
            sort_state: Box::new(EmptyGlobalSinkState::default()),
            materialized: Mutex::new(Some(Arc::new(PiecewiseMergeJoinMaterializedRhs {
                payload_chunks,
                key_values,
                key_locations,
                row_count,
                null_row_count,
            }))),
            right_key_executor: Mutex::new(
                crate::expression_executor::executor::ExpressionExecutor::new(
                    &join.condition.right,
                ),
            ),
            right_outer: Arc::new(
                crate::operator::join::outer_join_marker::OuterJoinMarker::new(enable_right_outer),
            ),
        });
        if enable_right_outer {
            sink_state.right_outer.add_rows(row_count);
        }
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);
    }

    #[test]
    fn inner_piecewise_join_scans_sorted_match_suffix() {
        let join = comparison_join(JoinType::Inner, JoinComparisonType::LessThan);
        let payload = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(&[2, 4, 6]))]);
        set_materialized_sink_state(
            &join,
            vec![payload],
            vec![Value::Integer(2), Value::Integer(4), Value::Integer(6)],
            vec![
                SortedRowLocation {
                    chunk_idx: 0,
                    row_idx: 0,
                    global_idx: 0,
                },
                SortedRowLocation {
                    chunk_idx: 0,
                    row_idx: 1,
                    global_idx: 1,
                },
                SortedRowLocation {
                    chunk_idx: 0,
                    row_idx: 2,
                    global_idx: 2,
                },
            ],
            3,
            0,
            false,
        );

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(&[5, 1]))]);
        let mut output = Chunk::new();
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(&ctx, &input, &mut output, &gstate, state.as_mut())
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 4);
        assert_eq!(output.data[0].get_value(0).to_string(), "5");
        assert_eq!(output.data[1].get_value(0).to_string(), "6");
        assert_eq!(output.data[0].get_value(1).to_string(), "1");
        assert_eq!(output.data[1].get_value(1).to_string(), "2");
        assert_eq!(output.data[1].get_value(3).to_string(), "6");
    }

    #[test]
    fn mark_piecewise_join_returns_null_when_only_unknown_matches_exist() {
        let join = comparison_join(JoinType::Mark, JoinComparisonType::LessThan);
        let mut payload = Chunk::initialize(&[LogicalType::Integer], 2);
        payload
            .column_mut(0)
            .unwrap()
            .set_value(0, &Value::Integer(5));
        payload
            .column_mut(0)
            .unwrap()
            .set_value(1, &Value::Null(LogicalType::Integer));
        payload.set_cardinality(2);
        set_materialized_sink_state(
            &join,
            vec![payload],
            vec![Value::Integer(5)],
            vec![SortedRowLocation {
                chunk_idx: 0,
                row_idx: 0,
                global_idx: 0,
            }],
            2,
            1,
            false,
        );

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(&[7]))]);
        let mut output = Chunk::new();
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(&ctx, &input, &mut output, &gstate, state.as_mut())
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert!(output.data[1].is_null(0));
    }

    #[test]
    fn right_piecewise_join_source_emits_unmatched_build_rows() {
        let join = comparison_join(JoinType::Right, JoinComparisonType::LessThan);
        let payload_chunks = Arc::new(vec![Chunk::from_arc_vectors(vec![Arc::new(
            Vector::from_i32(&[10, 20]),
        )])]);
        let marker = Arc::new(crate::operator::join::outer_join_marker::OuterJoinMarker::new(true));
        marker.add_rows(2);
        marker.set_match(0);

        let gstate = PiecewiseMergeJoinGlobalSourceState {
            payload_chunks,
            right_outer: marker,
            join_type: JoinType::Right,
        };

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut lstate = join.get_local_source_state(&ctx, &gstate).unwrap();
        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(&gstate, lstate.as_mut(), &interrupt);
        let mut output = Chunk::new();

        let result = join.get_data(&ctx, &mut output, &mut input).unwrap();

        assert_eq!(result, SourceResultType::HaveMoreOutput);
        assert_eq!(output.size(), 1);
        assert!(output.data[0].is_null(0));
        assert_eq!(output.data[1].get_value(0).to_string(), "20");
    }

    #[test]
    fn piecewise_merge_join_state_caches_key_executors() {
        let join = comparison_join(JoinType::Inner, JoinComparisonType::LessThan);
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let operator_state = join.get_operator_state(&ctx).unwrap();
        let operator_state = operator_state
            .as_any()
            .downcast_ref::<super::PiecewiseMergeJoinOperatorState>()
            .unwrap();
        assert_eq!(operator_state.left_key_executor.expression_count(), 1);

        let global_sink_state = join.get_global_sink_state(&ctx).unwrap();
        let global_sink_state = global_sink_state
            .as_any()
            .downcast_ref::<PiecewiseMergeJoinGlobalSinkState>()
            .unwrap();
        assert_eq!(
            global_sink_state
                .right_key_executor
                .lock()
                .unwrap()
                .expression_count(),
            1
        );
    }
}
