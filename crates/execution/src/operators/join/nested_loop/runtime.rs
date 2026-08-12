// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed nested-loop join probe transform and unmatched emit source.
//!
//! The NLJ probe iterates over materialized build chunks row-by-row, evaluating
//! comparison or arbitrary conditions per (probe_row, build_row) pair. The key
//! performance decisions:
//!
//! - Left condition expressions are evaluated once per input chunk (not per row).
//! - Right condition expressions are evaluated once per materialized build chunk
//!   and cached while probe rows scan that chunk.
//! - Arbitrary conditions use a pre-allocated single-row combined chunk in local
//!   state to avoid per-pair allocation.
//! - Found-bits for right/full/right-semi/right-anti live in the global state as an
//!   atomic bitvector so that unmatched emit can scan after probe completes.

use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorSelection, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{
    JoinComparisonType, JoinCondition, JoinType, MarkJoinSemantics,
};

use crate::expression_executor::compile_comparison_dispatch;
use crate::expression_executor::executor::ExpressionExecutor;

use crate::operators::output::ensure_source_output;
use crate::operators::output::ensure_transform_output;
use crate::runtime::breaker::{FoundBits, HandleRef, MaterializedHandle, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    MaterializedSourceGlobal, NestedLoopJoinProbeTransformLocal, NljUnmatchedSourceLocal,
    SourceGlobal, SourceLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

// ─── Exec struct ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct NestedLoopJoinProbeTransformExec {
    pub handle: HandleRef<MaterializedHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: MarkJoinSemantics,
    pub arbitrary_condition: Option<Expression>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_types: Box<[LogicalType]>,
}

// ─── Global state ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct NljProbeGlobal {
    reader: MaterializedReader,
    pub needs_found_bits: bool,
    found_bits: Mutex<Option<FoundBits>>,
}

impl NljProbeGlobal {
    fn ensure_found_bits(&self, build_chunks: &[Chunk]) {
        if !self.needs_found_bits {
            return;
        }
        let mut bits = self.found_bits.lock();
        if bits.is_none() {
            *bits = Some(self.reader.initialize_found_bits_for_chunks(build_chunks));
        }
    }

    fn found_bits(&self) -> Option<FoundBits> {
        self.found_bits.lock().clone()
    }
}

// ─── Implementation ──────────────────────────────────────────────────────────

impl NestedLoopJoinProbeTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        Ok(TransformGlobal::NestedLoopJoinProbe(Arc::new(
            NljProbeGlobal {
                reader: MaterializedReader::new(handle, "NLJ probe"),
                needs_found_bits: self.uses_found_bits(),
                found_bits: Mutex::new(None),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let left_condition_executors = self
            .conditions
            .iter()
            .map(|c| {
                ExpressionExecutor::with_expressions_for_session(
                    std::slice::from_ref(&c.left),
                    ctx.query.session.as_ref(),
                )
            })
            .collect();
        let right_condition_executors = self
            .conditions
            .iter()
            .map(|c| {
                ExpressionExecutor::with_expressions_for_session(
                    std::slice::from_ref(&c.right),
                    ctx.query.session.as_ref(),
                )
            })
            .collect();
        let arbitrary_condition_executor = self.arbitrary_condition.as_ref().map(|expr| {
            ExpressionExecutor::with_expressions_for_session(
                std::slice::from_ref(expr),
                ctx.query.session.as_ref(),
            )
        });
        Ok(TransformLocal::NestedLoopJoinProbe(
            NestedLoopJoinProbeTransformLocal {
                left_condition_executors,
                right_condition_executors,
                arbitrary_condition_executor,
                ..Default::default()
            },
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        let TransformGlobal::NestedLoopJoinProbe(global) = global else {
            return Err(paro_error::internal("NLJ probe global state mismatch"));
        };
        let TransformLocal::NestedLoopJoinProbe(local) = local else {
            return Err(paro_error::internal("NLJ probe local state mismatch"));
        };
        let build_chunks = global.reader.sealed_chunks()?;

        if input.is_empty() {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if build_chunks.iter().all(Chunk::is_empty) {
            self.emit_empty_build_result(input, output)?;
            return Ok(if output.is_empty() {
                TransformPoll::NeedMoreInput
            } else {
                TransformPoll::Output
            });
        }

        if let Some(poll) =
            self.try_singleton_inner_join(ctx, input, build_chunks.as_ref(), local, output)?
        {
            return Ok(poll);
        }

        if !local.probe_in_progress {
            global.ensure_found_bits(build_chunks.as_ref());
            local.probe_row = 0;
            local.build_chunk = 0;
            local.build_row = 0;
            local.build_global_idx = 0;
            local.probe_in_progress = true;
            local.found_match = false;
            local.saw_null = false;
            local.output_row = 0;
            self.evaluate_left_conditions(ctx, input, local)?;
        }

        let found_bits = global.found_bits();
        self.probe_loop(
            ctx,
            input,
            build_chunks.as_ref(),
            found_bits.as_ref(),
            global,
            local,
            output,
        )
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }

    // ─── Internal ────────────────────────────────────────────────────────────

    fn uses_found_bits(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    /// Execute a one-row comparison build as a vector predicate plus a
    /// broadcast projection. Scalar subqueries commonly produce this shape;
    /// treating them as a general pair-at-a-time nested loop needlessly boxes
    /// every probe value and copies every surviving row.
    fn try_singleton_inner_join(
        &self,
        ctx: &mut OperatorCallContext,
        input: &Chunk,
        build_chunks: &[Chunk],
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<Option<TransformPoll>> {
        if self.join_type != JoinType::Inner
            || self.arbitrary_condition.is_some()
            || self.conditions.len() != 1
            || local.probe_in_progress
        {
            return Ok(None);
        }
        let mut singleton = None;
        for (chunk_idx, chunk) in build_chunks.iter().enumerate() {
            for row_idx in 0..chunk.size() {
                if singleton.replace((chunk_idx, row_idx)).is_some() {
                    return Ok(None);
                }
            }
        }
        let Some((build_chunk_idx, build_row)) = singleton else {
            return Ok(None);
        };

        self.evaluate_left_conditions(ctx, input, local)?;
        self.ensure_right_condition_cache(
            build_chunk_idx,
            &build_chunks[build_chunk_idx],
            ctx,
            local,
        )?;
        let left = local.left_condition_results.first().ok_or_else(|| {
            paro_error::internal("singleton NLJ left condition was not evaluated")
        })?;
        let right = local.right_condition_cache.first().ok_or_else(|| {
            paro_error::internal("singleton NLJ right condition was not evaluated")
        })?;
        if left.logical_type() != right.logical_type() {
            return Ok(None);
        }
        let dispatch =
            compile_comparison_dispatch(left.logical_type(), self.conditions[0].comparison.into());
        let Some(select) = dispatch.select else {
            return Ok(None);
        };
        let right_constant = Vector::try_constant_from_value(
            right.logical_type().clone(),
            right.get_value(build_row),
            input.size(),
            input.allocator().clone(),
        )?;
        let mut scratch = ctx.scratch.expr();
        let selection =
            scratch.selection(input.size(), ctx.query.allocator(MemoryTag::BaseTable))?;
        let selected_count = select(left, &right_constant, None, input.size(), selection)?;
        if selected_count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(Some(TransformPoll::NeedMoreInput));
        }

        let selected = VectorSelection::from(&*selection);
        let mut columns =
            Vec::with_capacity(self.left_projection.len() + self.right_projection.len());
        for &column_idx in &self.left_projection {
            let column = input.data.get(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "singleton NLJ left projection is out of bounds: column={column_idx} count={}",
                    input.column_count()
                ))
            })?;
            columns.push(Arc::new(Vector::try_gather_ref(
                Arc::clone(column),
                selected.clone(),
            )?));
        }
        let build = &build_chunks[build_chunk_idx];
        for &column_idx in &self.right_projection {
            let column = build.data.get(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "singleton NLJ right projection is out of bounds: column={column_idx} count={}",
                    build.column_count()
                ))
            })?;
            columns.push(Arc::new(Vector::try_constant_from_value(
                column.logical_type().clone(),
                column.get_value(build_row),
                selected_count,
                input.allocator().clone(),
            )?));
        }
        *output = Chunk::try_from_arc_vectors_with_cardinality(
            columns,
            selected_count,
            input.allocator().clone(),
        )?;
        debug_assert_eq!(output.types(), self.output_types.as_ref());
        Ok(Some(TransformPoll::Output))
    }

    fn evaluate_left_conditions(
        &self,
        ctx: &OperatorCallContext,
        input: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
    ) -> Result<()> {
        if self.conditions.is_empty() {
            return Ok(());
        }
        local.left_condition_results.clear();
        local.left_condition_results.reserve(self.conditions.len());
        for executor in &mut local.left_condition_executors {
            local
                .left_condition_results
                .push(executor.execute_expression(0, input, None, input.size(), ctx.query)?);
        }
        Ok(())
    }

    fn emit_empty_build_result(&self, input: &Chunk, output: &mut Chunk) -> Result<()> {
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
        match self.join_type {
            JoinType::Anti => {
                emit_projected_left(input, &self.left_projection, output)?;
            }
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                emit_left_with_null_right(
                    input,
                    &self.left_projection,
                    &self.right_output_types,
                    output,
                )?;
            }
            JoinType::Mark => {
                emit_mark_all(input, &self.left_projection, Some(false), output)?;
            }
            _ => {
                output.try_set_cardinality(0)?;
            }
        }
        Ok(())
    }

    fn probe_loop(
        &self,
        ctx: &OperatorCallContext,
        input: &Chunk,
        build_chunks: &[Chunk],
        found_bits: Option<&FoundBits>,
        global: &NljProbeGlobal,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
        loop {
            if local.probe_row >= input.size() {
                local.probe_in_progress = false;
                if local.output_row > 0 {
                    self.flush_output(local, output)?;
                    return Ok(TransformPoll::Output);
                }
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }

            // Advance past exhausted build chunks
            while local.build_chunk < build_chunks.len()
                && local.build_row >= build_chunks[local.build_chunk].size()
            {
                local.build_chunk += 1;
                local.build_row = 0;
            }

            if local.build_chunk >= build_chunks.len() {
                // Exhausted all build rows for this probe row
                self.emit_probe_row_conclusion(input, local, output)?;
                local.probe_row += 1;
                local.build_chunk = 0;
                local.build_row = 0;
                local.build_global_idx = 0;
                local.found_match = false;
                local.saw_null = false;
                local.single_match_found = false;

                if local.output_row >= VECTOR_SIZE {
                    self.flush_output(local, output)?;
                    if local.probe_row >= input.size() {
                        local.probe_in_progress = false;
                        return Ok(TransformPoll::Output);
                    }
                    return Ok(TransformPoll::OutputMore);
                }
                continue;
            }

            let bc = &build_chunks[local.build_chunk];
            let match_result = self.evaluate_row_match(ctx, input, bc, global, local)?;

            match match_result {
                RowMatch::Match => {
                    if let Some(bits) = found_bits {
                        bits.mark(local.build_global_idx);
                    }
                    local.found_match = true;

                    match self.join_type {
                        JoinType::Semi => {
                            self.append_left_only(input, local, output)?;
                            self.skip_to_next_probe_row(local, build_chunks);
                            continue;
                        }
                        JoinType::Anti => {
                            self.skip_to_next_probe_row(local, build_chunks);
                            continue;
                        }
                        JoinType::Single => {
                            if local.single_match_found {
                                return Err(paro_error::internal(
                                    "scalar subquery produced more than one row",
                                ));
                            }
                            local.single_match_found = true;
                            self.append_join_match(input, bc, local, output)?;
                            // Don't skip — keep scanning to detect duplicates
                        }
                        JoinType::Mark => {
                            // Mark true will be emitted at conclusion
                            self.skip_to_next_probe_row(local, build_chunks);
                            continue;
                        }
                        JoinType::RightSemi | JoinType::RightAnti => {
                            // Only tracking found-bits, no probe-side output
                        }
                        _ => {
                            self.append_join_match(input, bc, local, output)?;
                        }
                    }
                }
                RowMatch::Null => {
                    local.saw_null = true;
                }
                RowMatch::NoMatch => {}
            }

            self.advance_right(local, build_chunks);

            if local.output_row >= VECTOR_SIZE {
                self.flush_output(local, output)?;
                return Ok(TransformPoll::OutputMore);
            }
        }
    }

    fn ensure_combined_chunk(
        &self,
        input: &Chunk,
        build_chunk: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
    ) -> Result<()> {
        if local.combined_chunk.is_some() {
            return Ok(());
        }
        let mut types: Vec<LogicalType> =
            Vec::with_capacity(input.column_count() + build_chunk.column_count());
        for col in 0..input.column_count() {
            types.push(input.data[col].logical_type().clone());
        }
        for col in 0..build_chunk.column_count() {
            types.push(build_chunk.data[col].logical_type().clone());
        }
        local.combined_chunk = Some(Chunk::try_initialize(&types, 1, input.allocator().clone())?);
        Ok(())
    }

    fn ensure_right_condition_cache(
        &self,
        build_chunk_idx: usize,
        build_chunk: &Chunk,
        ctx: &OperatorCallContext,
        local: &mut NestedLoopJoinProbeTransformLocal,
    ) -> Result<()> {
        if local.right_cache_chunk_idx == Some(build_chunk_idx) {
            return Ok(());
        }
        local.right_condition_cache.clear();
        for executor in &mut local.right_condition_executors {
            local
                .right_condition_cache
                .push(executor.execute_expression(
                    0,
                    build_chunk,
                    None,
                    build_chunk.size(),
                    ctx.query,
                )?);
        }
        local.right_cache_chunk_idx = Some(build_chunk_idx);
        Ok(())
    }

    fn evaluate_row_match(
        &self,
        ctx: &OperatorCallContext,
        input: &Chunk,
        build_chunk: &Chunk,
        _global: &NljProbeGlobal,
        local: &mut NestedLoopJoinProbeTransformLocal,
    ) -> Result<RowMatch> {
        // Arbitrary condition path (any-join)
        if local.arbitrary_condition_executor.is_some() {
            self.ensure_combined_chunk(input, build_chunk, local)?;
            {
                let combined = local.combined_chunk.as_mut().unwrap();
                for col in 0..input.column_count() {
                    combined
                        .column_mut(col)
                        .unwrap()
                        .set_value(0, &input.data[col].get_value(local.probe_row));
                }
                let right_offset = input.column_count();
                for col in 0..build_chunk.column_count() {
                    combined
                        .column_mut(right_offset + col)
                        .unwrap()
                        .set_value(0, &build_chunk.data[col].get_value(local.build_row));
                }
                combined.try_set_cardinality(1)?;
            }
            let executor = local.arbitrary_condition_executor.as_mut().unwrap();
            let combined = local.combined_chunk.as_mut().unwrap();
            let result_vec = executor.execute_expression(0, combined, None, 1, ctx.query)?;
            let val = result_vec.get_value(0);
            return Ok(match val {
                Value::Boolean(true) => RowMatch::Match,
                Value::Null(_) => RowMatch::Null,
                _ => RowMatch::NoMatch,
            });
        }

        // Comparison condition path — right conditions cached per build chunk
        if self.conditions.is_empty() {
            return Ok(RowMatch::Match);
        }
        self.ensure_right_condition_cache(local.build_chunk, build_chunk, ctx, local)?;

        let mut saw_null = false;
        for (cond_idx, condition) in self.conditions.iter().enumerate() {
            let left_val = local
                .left_condition_results
                .get(cond_idx)
                .map(|v| v.get_value(local.probe_row))
                .unwrap_or(Value::Null(condition.left.return_type()));

            let right_val = local
                .right_condition_cache
                .get(cond_idx)
                .map(|v| v.get_value(local.build_row))
                .unwrap_or(Value::Null(condition.right.return_type()));

            match compare_with_nulls(condition.comparison, &left_val, &right_val) {
                Some(true) => {}
                Some(false) => return Ok(RowMatch::NoMatch),
                None => {
                    if self.mark_condition_null_is_unknown(cond_idx) {
                        saw_null = true;
                    } else {
                        return Ok(RowMatch::NoMatch);
                    }
                }
            }
        }

        Ok(if saw_null {
            RowMatch::Null
        } else {
            RowMatch::Match
        })
    }

    fn mark_condition_null_is_unknown(&self, condition_idx: usize) -> bool {
        match self.mark_semantics {
            MarkJoinSemantics::ThreeValuedFrom(start) => condition_idx >= start,
            MarkJoinSemantics::TwoValued => false,
            MarkJoinSemantics::NotMark => true,
        }
    }

    fn emit_probe_row_conclusion(
        &self,
        input: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        if local.found_match {
            if self.join_type == JoinType::Mark {
                self.append_mark_row(input, local, output, Some(true))?;
            }
            return Ok(());
        }

        // No match found for this probe row
        match self.join_type {
            JoinType::Anti => {
                self.append_left_only(input, local, output)?;
            }
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                self.append_left_null_right(input, local, output)?;
            }
            JoinType::Mark => {
                let marker = if local.saw_null { None } else { Some(false) };
                self.append_mark_row(input, local, output, marker)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn append_join_match(
        &self,
        input: &Chunk,
        build_chunk: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        for (out_col, &in_col) in self.left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(local.probe_row));
        }
        let right_offset = self.left_projection.len();
        for (out_col, &in_col) in self.right_projection.iter().enumerate() {
            output
                .column_mut(right_offset + out_col)
                .unwrap()
                .set_value(row, &build_chunk.data[in_col].get_value(local.build_row));
        }
        local.output_row += 1;
        Ok(())
    }

    fn append_left_only(
        &self,
        input: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        for (out_col, &in_col) in self.left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(local.probe_row));
        }
        local.output_row += 1;
        Ok(())
    }

    fn append_left_null_right(
        &self,
        input: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        for (out_col, &in_col) in self.left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(local.probe_row));
        }
        let right_offset = self.left_projection.len();
        for (out_col, typ) in self.right_output_types.iter().enumerate() {
            let target = output.column_mut(right_offset + out_col).unwrap();
            target.set_value(row, &Value::Null(typ.clone()));
            target.try_set_null(row, true)?;
        }
        local.output_row += 1;
        Ok(())
    }

    fn append_mark_row(
        &self,
        input: &Chunk,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
        marker: Option<bool>,
    ) -> Result<()> {
        let row = local.output_row;
        for (out_col, &in_col) in self.left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(local.probe_row));
        }
        let mark_col = output.column_mut(self.left_projection.len()).unwrap();
        match marker {
            Some(v) => {
                mark_col.set_value(row, &Value::Boolean(v));
                mark_col.try_set_null(row, false)?;
            }
            None => {
                mark_col.set_value(row, &Value::Boolean(false));
                mark_col.try_set_null(row, true)?;
            }
        }
        local.output_row += 1;
        Ok(())
    }

    fn flush_output(
        &self,
        local: &mut NestedLoopJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        output.try_set_cardinality(local.output_row)?;
        local.output_row = 0;
        Ok(())
    }

    fn advance_right(&self, local: &mut NestedLoopJoinProbeTransformLocal, build_chunks: &[Chunk]) {
        local.build_row += 1;
        local.build_global_idx += 1;
        while local.build_chunk < build_chunks.len()
            && local.build_row >= build_chunks[local.build_chunk].size()
        {
            local.build_chunk += 1;
            local.build_row = 0;
        }
    }

    fn skip_to_next_probe_row(
        &self,
        local: &mut NestedLoopJoinProbeTransformLocal,
        build_chunks: &[Chunk],
    ) {
        local.build_chunk = build_chunks.len();
        local.build_row = 0;
    }
}

// ─── Match result ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowMatch {
    Match,
    NoMatch,
    Null,
}

// ─── Comparison with proper NULL semantics ───────────────────────────────────

pub(crate) fn compare_with_nulls(
    comparison: JoinComparisonType,
    left: &Value,
    right: &Value,
) -> Option<bool> {
    use std::cmp::Ordering;

    match comparison {
        JoinComparisonType::NotDistinctFrom => Some(
            (left.is_null() && right.is_null())
                || (!left.is_null() && !right.is_null() && left == right),
        ),
        JoinComparisonType::DistinctFrom => Some(
            (left.is_null() != right.is_null())
                || (!left.is_null() && !right.is_null() && left != right),
        ),
        _ => {
            if left.is_null() || right.is_null() {
                return None;
            }
            Some(match comparison {
                JoinComparisonType::Equal => left == right,
                JoinComparisonType::NotEqual => left != right,
                JoinComparisonType::LessThan => left.partial_cmp(right) == Some(Ordering::Less),
                JoinComparisonType::GreaterThan => {
                    left.partial_cmp(right) == Some(Ordering::Greater)
                }
                JoinComparisonType::LessThanOrEqual => matches!(
                    left.partial_cmp(right),
                    Some(Ordering::Less | Ordering::Equal)
                ),
                JoinComparisonType::GreaterThanOrEqual => matches!(
                    left.partial_cmp(right),
                    Some(Ordering::Greater | Ordering::Equal)
                ),
                JoinComparisonType::NotDistinctFrom | JoinComparisonType::DistinctFrom => {
                    unreachable!()
                }
            })
        }
    }
}

// ─── Helpers for empty-build fast paths ──────────────────────────────────────

fn emit_projected_left(input: &Chunk, left_projection: &[usize], output: &mut Chunk) -> Result<()> {
    for row in 0..input.size() {
        for (out_col, &in_col) in left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(row));
        }
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

fn emit_left_with_null_right(
    input: &Chunk,
    left_projection: &[usize],
    right_output_types: &[LogicalType],
    output: &mut Chunk,
) -> Result<()> {
    let right_offset = left_projection.len();
    for row in 0..input.size() {
        for (out_col, &in_col) in left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(row));
        }
        for (out_col, typ) in right_output_types.iter().enumerate() {
            let target = output.column_mut(right_offset + out_col).unwrap();
            target.set_value(row, &Value::Null(typ.clone()));
            target.try_set_null(row, true)?;
        }
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

fn emit_mark_all(
    input: &Chunk,
    left_projection: &[usize],
    marker: Option<bool>,
    output: &mut Chunk,
) -> Result<()> {
    let mark_col_idx = left_projection.len();
    for row in 0..input.size() {
        for (out_col, &in_col) in left_projection.iter().enumerate() {
            output
                .column_mut(out_col)
                .unwrap()
                .set_value(row, &input.data[in_col].get_value(row));
        }
        let mark_col = output.column_mut(mark_col_idx).unwrap();
        match marker {
            Some(v) => {
                mark_col.set_value(row, &Value::Boolean(v));
                mark_col.try_set_null(row, false)?;
            }
            None => {
                mark_col.set_value(row, &Value::Boolean(false));
                mark_col.try_set_null(row, true)?;
            }
        }
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

// ─── NLJ Unmatched Source ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct NljUnmatchedSourceExec {
    pub handle: HandleRef<MaterializedHandle>,
    pub join_type: JoinType,
    pub left_output_types: Box<[LogicalType]>,
    pub right_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
}

impl NljUnmatchedSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Materialized(Arc::new(
            MaterializedSourceGlobal::new(ctx.handles.get(self.handle)?),
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::NljUnmatched(NljUnmatchedSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::Materialized(global) = global else {
            return Err(paro_error::internal("NLJ unmatched source global mismatch"));
        };
        let SourceLocal::NljUnmatched(local) = local else {
            return Err(paro_error::internal("NLJ unmatched source local mismatch"));
        };
        let build_chunks = global.sealed_chunks()?;
        let found_bits = global.found_bits();

        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let left_null_count = self.left_output_types.len();
        let mut out_row = 0;

        while local.chunk_idx < build_chunks.len() {
            let bc = &build_chunks[local.chunk_idx];
            while local.row_idx < bc.size() {
                let matched = found_bits
                    .as_ref()
                    .is_some_and(|bits| bits.is_marked(local.global_row_idx));
                if !matched {
                    match self.join_type {
                        JoinType::Right | JoinType::Outer => {
                            for (out_col, typ) in self.left_output_types.iter().enumerate() {
                                let target = output.column_mut(out_col).unwrap();
                                target.set_value(out_row, &Value::Null(typ.clone()));
                                target.try_set_null(out_row, true)?;
                            }
                            let right_offset = left_null_count;
                            for (out_col, &in_col) in self.right_projection.iter().enumerate() {
                                let target = output.column_mut(right_offset + out_col).unwrap();
                                target
                                    .set_value(out_row, &bc.data[in_col].get_value(local.row_idx));
                            }
                            out_row += 1;
                        }
                        JoinType::RightSemi => {}
                        JoinType::RightAnti => {
                            for (out_col, &in_col) in self.right_projection.iter().enumerate() {
                                let target = output.column_mut(out_col).unwrap();
                                target
                                    .set_value(out_row, &bc.data[in_col].get_value(local.row_idx));
                            }
                            out_row += 1;
                        }
                        _ => {}
                    }
                } else if self.join_type == JoinType::RightSemi {
                    for (out_col, &in_col) in self.right_projection.iter().enumerate() {
                        let target = output.column_mut(out_col).unwrap();
                        target.set_value(out_row, &bc.data[in_col].get_value(local.row_idx));
                    }
                    out_row += 1;
                }
                local.row_idx += 1;
                local.global_row_idx += 1;

                if out_row >= VECTOR_SIZE {
                    output.try_set_cardinality(out_row)?;
                    return Ok(SourcePoll::Output);
                }
            }
            local.chunk_idx += 1;
            local.row_idx = 0;
        }

        if out_row > 0 {
            output.try_set_cardinality(out_row)?;
            Ok(SourcePoll::Output)
        } else {
            output.try_set_cardinality(0)?;
            Ok(SourcePoll::Finished)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::operator::join::JoinComparisonType;

    #[test]
    fn compare_equal_matching() {
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::Equal,
                &Value::Integer(1),
                &Value::Integer(1)
            ),
            Some(true)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::Equal,
                &Value::Integer(1),
                &Value::Integer(2)
            ),
            Some(false)
        );
    }

    #[test]
    fn compare_null_returns_none_for_standard_comparisons() {
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::Equal,
                &Value::Null(LogicalType::Integer),
                &Value::Integer(1)
            ),
            None
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::LessThan,
                &Value::Integer(1),
                &Value::Null(LogicalType::Integer)
            ),
            None
        );
    }

    #[test]
    fn compare_not_distinct_from_handles_nulls() {
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::NotDistinctFrom,
                &Value::Null(LogicalType::Integer),
                &Value::Null(LogicalType::Integer)
            ),
            Some(true)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::NotDistinctFrom,
                &Value::Null(LogicalType::Integer),
                &Value::Integer(1)
            ),
            Some(false)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::NotDistinctFrom,
                &Value::Integer(1),
                &Value::Integer(1)
            ),
            Some(true)
        );
    }

    #[test]
    fn compare_distinct_from_handles_nulls() {
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::DistinctFrom,
                &Value::Null(LogicalType::Integer),
                &Value::Null(LogicalType::Integer)
            ),
            Some(false)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::DistinctFrom,
                &Value::Null(LogicalType::Integer),
                &Value::Integer(1)
            ),
            Some(true)
        );
    }

    #[test]
    fn compare_range_predicates() {
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::LessThan,
                &Value::Integer(1),
                &Value::Integer(2)
            ),
            Some(true)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::LessThan,
                &Value::Integer(2),
                &Value::Integer(1)
            ),
            Some(false)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::GreaterThanOrEqual,
                &Value::Integer(3),
                &Value::Integer(2)
            ),
            Some(true)
        );
        assert_eq!(
            compare_with_nulls(
                JoinComparisonType::LessThanOrEqual,
                &Value::Integer(2),
                &Value::Integer(2)
            ),
            Some(true)
        );
    }
}
