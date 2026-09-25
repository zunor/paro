// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared window frame kernel for the breaker runtime.

use std::ops::Range;
use std::sync::Arc;

use paro_common::allocator::{Allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::window::WindowFunctionType;
use paro_planner::expression::{
    AggregateType, Expression, OrderByExpression, WindowExpression, WindowFrameBound,
    WindowFrameType,
};

use super::{
    are_peers, value_from_expr, value_is_null_from_expr, value_to_i64, WindowPartition,
    WindowRowKey,
};
use crate::operators::aggregate::aggregate_kernel::{
    destroy_states, finalize_states, initialize_states, update_states, AggregatePayload,
};
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;

/// Materialized frame ranges for every row in one partition.
///
/// Ranges are stored relative to the partition.
pub(super) struct WindowFrameIndex {
    partition: WindowPartition,
    ranges: Vec<Range<usize>>,
}

impl WindowFrameIndex {
    pub(super) fn build(
        chunks: &[Chunk],
        sorted_keys: &[WindowRowKey],
        partition: WindowPartition,
        expr: &WindowExpression,
    ) -> Result<Self> {
        let peer_ranges = (expr.frame.frame_type == WindowFrameType::Range)
            .then(|| build_peer_ranges(chunks, sorted_keys, partition, &expr.orders));
        let mut ranges = Vec::with_capacity(partition.end - partition.start);
        for absolute_idx in partition.start..partition.end {
            let row = absolute_idx - partition.start;
            ranges.push(frame_range(
                chunks,
                &sorted_keys[absolute_idx],
                row,
                partition.end - partition.start,
                peer_ranges
                    .as_ref()
                    .map(|ranges| ranges[row].clone())
                    .unwrap_or(row..row + 1),
                expr,
            )?);
        }
        Ok(Self { partition, ranges })
    }

    pub(super) fn relative_range(&self, absolute_idx: usize) -> Range<usize> {
        self.ranges[absolute_idx - self.partition.start].clone()
    }

    /// A fixed lower bound and monotonically growing upper bound admit delta
    /// updates. This is a property of the evaluated frames, not a function name
    /// or an assumption about ROWS versus RANGE/peer ordering.
    pub(super) fn is_append_only(&self) -> bool {
        self.ranges
            .windows(2)
            .all(|pair| pair[0].start == pair[1].start && pair[0].end <= pair[1].end)
    }
}

/// Row positions needed to navigate one window value expression.
///
/// Indexing non-NULL rows once prevents `IGNORE NULLS` from rescanning every row
/// in large overlapping frames or partition-relative LEAD/LAG lookups. Binary
/// searches locate targets without retaining a boxed copy of every value.
pub(super) struct WindowValueIndex {
    partition: WindowPartition,
    non_null_rows: Vec<usize>,
}

impl WindowValueIndex {
    pub(super) fn build(
        chunks: &[Chunk],
        sorted_keys: &[WindowRowKey],
        partition: WindowPartition,
        expr: &WindowExpression,
    ) -> Result<Self> {
        let child = expr.arguments().first().ok_or_else(|| {
            paro_error::internal("frame value window function requires an argument")
        })?;
        let non_null_rows = if expr.ignore_nulls {
            sorted_keys[partition.start..partition.end]
                .iter()
                .enumerate()
                .filter_map(|(row, key)| {
                    (!value_is_null_from_expr(chunks, key, child)).then_some(row)
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            partition,
            non_null_rows,
        })
    }

    pub(super) fn evaluate(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[WindowRowKey],
        frame: Range<usize>,
        current_row: usize,
        expr: &WindowExpression,
    ) -> Result<Value> {
        let null = || Value::Null(expr.return_type());
        let function_type = expr
            .native_invocation()
            .map(|(function, _)| function.function_type)
            .ok_or_else(|| paro_error::internal("frame value kernel requires a native window"))?;
        let row = match function_type {
            WindowFunctionType::FirstValue => self.row_from_head(&frame, 0, expr.ignore_nulls),
            WindowFunctionType::LastValue => self.row_from_tail(&frame, expr.ignore_nulls),
            WindowFunctionType::NthValue => {
                let Some(offset) = expr.arguments().get(1) else {
                    return Err(paro_error::internal("NTH_VALUE requires an offset"));
                };
                let value = value_from_expr(chunks, &sorted_keys[current_row], offset);
                if value.is_null() {
                    return Ok(null());
                }
                let value = value_to_i64(&value).ok_or_else(|| {
                    paro_error::invalid_input("argument of nth_value must be an integer")
                })?;
                if value <= 0 {
                    return Err(paro_error::invalid_input(
                        "argument of nth_value must be greater than zero",
                    ));
                }
                let nth = usize::try_from(value - 1).map_err(|_| {
                    paro_error::invalid_input(
                        "argument of nth_value is outside the supported range",
                    )
                })?;
                self.row_from_head(&frame, nth, expr.ignore_nulls)
            }
            other => {
                return Err(paro_error::internal(format!(
                    "frame value kernel cannot execute {other}"
                )))
            }
        };
        Ok(row
            .map(|row| {
                value_from_expr(
                    chunks,
                    &sorted_keys[self.partition.start + row],
                    &expr.arguments()[0],
                )
            })
            .unwrap_or_else(null))
    }

    /// Find a LEAD/LAG target relative to the current row.
    ///
    /// A negative offset reverses the function's normal direction. Under
    /// `IGNORE NULLS`, offset zero still addresses the current row, while a
    /// non-zero offset counts only non-NULL argument rows after/before it.
    pub(super) fn row_from_current(
        &self,
        current_row: usize,
        offset: i64,
        function_moves_forward: bool,
        ignore_nulls: bool,
    ) -> Option<usize> {
        let current = current_row.checked_sub(self.partition.start)?;
        let partition_len = self.partition.end - self.partition.start;
        if current >= partition_len {
            return None;
        }
        if offset == 0 {
            return Some(current);
        }

        let distance = usize::try_from(offset.unsigned_abs()).ok()?;
        let moves_forward = function_moves_forward == offset.is_positive();
        if !ignore_nulls {
            return if moves_forward {
                current
                    .checked_add(distance)
                    .filter(|&row| row < partition_len)
            } else {
                current.checked_sub(distance)
            };
        }

        if moves_forward {
            let first = self.non_null_rows.partition_point(|&row| row <= current);
            first
                .checked_add(distance - 1)
                .and_then(|idx| self.non_null_rows.get(idx))
                .copied()
        } else {
            let end = self.non_null_rows.partition_point(|&row| row < current);
            end.checked_sub(distance)
                .and_then(|idx| self.non_null_rows.get(idx))
                .copied()
        }
    }

    fn row_from_head(&self, frame: &Range<usize>, nth: usize, ignore_nulls: bool) -> Option<usize> {
        if !ignore_nulls {
            return frame.start.checked_add(nth).filter(|&row| row < frame.end);
        }

        let first = self.non_null_rows.partition_point(|&row| row < frame.start);
        first
            .checked_add(nth)
            .and_then(|idx| self.non_null_rows.get(idx))
            .copied()
            .filter(|&row| row < frame.end)
    }

    fn row_from_tail(&self, frame: &Range<usize>, ignore_nulls: bool) -> Option<usize> {
        if !ignore_nulls {
            return frame.end.checked_sub(1).filter(|&row| row >= frame.start);
        }

        let end = self.non_null_rows.partition_point(|&row| row < frame.end);
        self.non_null_rows[..end]
            .last()
            .copied()
            .filter(|&row| row >= frame.start)
    }
}

pub(super) fn aggregate_frame_is_partition_constant(expr: &WindowExpression) -> bool {
    expr.frame.covers_whole_partition(!expr.orders.is_empty())
}

pub(super) fn aggregate_window_value(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frame: Range<usize>,
    expr: &WindowExpression,
    allocator: Arc<dyn Allocator>,
) -> Result<Value> {
    let mut value = None;
    visit_append_only_aggregate_frames(
        chunks,
        sorted_keys,
        std::iter::once(frame),
        expr,
        allocator,
        |_, result| {
            value = Some(result);
            Ok(())
        },
    )?;
    value.ok_or_else(|| paro_error::internal("aggregate frame produced no result"))
}

/// Evaluate one append-only frame sequence with the exact bound aggregate ABI.
/// Finalization is observational; only destruction owns the state. The single
/// frame fallback above is also the independent recomputation oracle in tests.
pub(super) fn visit_append_only_aggregate_frames(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frames: impl IntoIterator<Item = Range<usize>>,
    expr: &WindowExpression,
    allocator: Arc<dyn Allocator>,
    mut emit: impl FnMut(usize, Value) -> Result<()>,
) -> Result<()> {
    let aggregate = expr.aggregate_invocation().ok_or_else(|| {
        paro_error::internal("aggregate window kernel received a native invocation")
    })?;
    if aggregate.aggr_type != AggregateType::NonDistinct || !aggregate.order_bys.is_empty() {
        return Err(paro_error::not_implemented(
            "sort-window aggregate kernel supports plain unordered aggregates only",
        ));
    }

    // FILTER is evaluated against the sorted window row domain below. The
    // grouped aggregate object normally stores a payload-column Reference for
    // that filter, but the sort-window fallback has no aggregate payload
    // descriptor: a bound Window may still carry a ColumnRef here. Build the
    // state object from the exact bound kernel while deliberately removing the
    // already-consumed row-selection modifier.
    let mut state_aggregate = aggregate.clone();
    state_aggregate.filter = None;
    let objects = [AggregateObject::from_bound(&state_aggregate)?];
    let layout = AggregateStateLayout::new(&objects)?;
    let word_count = layout.total_size().div_ceil(size_of::<u64>()).max(1);
    let mut state = vec![0u64; word_count];
    let state_ptr = state.as_mut_ptr().cast::<u8>();
    let single_address = pointer_vector(1, state_ptr, allocator.clone())?;
    initialize_states(&layout, &objects, &single_address, 1)?;
    let mut arena = ArenaAllocator::new(allocator.clone());
    let mut input_data =
        AggregateInputData::new(None, &mut arena, AggregateCombineType::PreserveInput);
    let aggregate_inputs = [(0..aggregate.children.len()).collect::<Vec<_>>()];

    let result = (|| {
        let mut address_batch = pointer_vector(VECTOR_SIZE, state_ptr, allocator.clone())?;
        let mut output = Chunk::try_initialize(
            std::slice::from_ref(&aggregate.return_type),
            1,
            allocator.clone(),
        )?;
        // A delta is frequently one row (ROWS CURRENT ROW). Own the bounded
        // writable payload once, not a new set of vectors for every prefix.
        // Reset releases variable-width scratch and restores validity before
        // the next delta; aggregate update may consume but cannot retain it.
        let input_types = aggregate
            .children
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        let mut payload_chunk =
            Chunk::try_initialize(&input_types, VECTOR_SIZE, allocator.clone())?;
        let mut selected_keys = Vec::with_capacity(VECTOR_SIZE);
        let mut previous: Option<Range<usize>> = None;
        for (index, frame) in frames.into_iter().enumerate() {
            if frame.start > frame.end
                || frame.end > sorted_keys.len()
                || previous
                    .as_ref()
                    .is_some_and(|last| last.start != frame.start || last.end > frame.end)
            {
                return Err(paro_error::internal("aggregate frames are not append-only"));
            }
            let start = previous.as_ref().map_or(frame.start, |last| last.end);
            for batch in (start..frame.end).step_by(VECTOR_SIZE) {
                let count = (frame.end - batch).min(VECTOR_SIZE);
                let batch_keys = &sorted_keys[batch..batch + count];
                selected_keys.clear();
                if let Some(filter) = aggregate.filter.as_deref() {
                    selected_keys.extend(batch_keys.iter().copied().filter(|key| {
                        matches!(value_from_expr(chunks, key, filter), Value::Boolean(true))
                    }));
                }
                let input_keys = if aggregate.filter.is_some() {
                    selected_keys.as_slice()
                } else {
                    batch_keys
                };
                if input_keys.is_empty() {
                    continue;
                }
                address_batch.try_set_count(input_keys.len())?;
                payload_chunk.try_reset(allocator.clone())?;
                materialize_aggregate_inputs(chunks, input_keys, aggregate, &mut payload_chunk)?;
                let payload = AggregatePayload {
                    chunk: &payload_chunk,
                    aggregate_inputs: &aggregate_inputs,
                };
                update_states(
                    &objects,
                    &mut input_data,
                    &payload,
                    &address_batch,
                    input_keys.len(),
                )?;
            }
            // Reset validity and variable-width output ownership between peeks;
            // a NULL result in an empty prefix must not poison later values.
            output.try_reset(allocator.clone())?;
            finalize_states(&objects, &mut input_data, &single_address, &mut output, 1)?;
            emit(
                index,
                output
                    .column(0)
                    .expect("aggregate result column")
                    .get_value(0),
            )?;
            previous = Some(frame);
        }
        Ok(())
    })();

    let destroy_result = destroy_states(&objects, &mut input_data, &single_address, 1);
    match (result, destroy_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn pointer_vector(
    count: usize,
    state_ptr: *mut u8,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let mut addresses = Vector::try_new(LogicalType::BigInt, count.max(1), allocator)?;
    addresses.try_set_count(count)?;
    // SAFETY: BIGINT is pointer-width on supported targets and the vector owns
    // capacity for `count` entries. AggregateStateInput reads these bits back
    // as state addresses without dereferencing the logical BIGINT value.
    unsafe {
        let data = addresses.flat_data_mut::<*mut u8>();
        for row in 0..count {
            *data.add(row) = state_ptr;
        }
    }
    Ok(addresses)
}

fn materialize_aggregate_inputs(
    chunks: &[Chunk],
    keys: &[WindowRowKey],
    aggregate: &paro_planner::expression::AggregateExpression,
    payload: &mut Chunk,
) -> Result<()> {
    payload.try_set_cardinality(keys.len())?;
    for (column, child) in aggregate.children.iter().enumerate() {
        let vector = payload.column_mut(column).ok_or_else(|| {
            paro_error::internal("aggregate window payload is missing an argument")
        })?;
        for (row, key) in keys.iter().enumerate() {
            match child {
                Expression::Constant(constant) => vector.set_value(row, &constant.value),
                Expression::Reference(reference) => {
                    let source = chunks
                        .get(key.chunk_idx)
                        .and_then(|chunk| chunk.column(reference.index))
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "aggregate window argument references missing column {}",
                                reference.index
                            ))
                        })?;
                    vector.try_copy_at(row, source, key.row_idx)?;
                }
                Expression::ColumnRef(column) => {
                    let source = chunks
                        .get(key.chunk_idx)
                        .and_then(|chunk| chunk.column(column.binding.column_index))
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "aggregate window argument references missing column {}",
                                column.binding.column_index
                            ))
                        })?;
                    vector.try_copy_at(row, source, key.row_idx)?;
                }
                _ => {
                    return Err(paro_error::internal(
                        "aggregate window argument was not lowered to a direct value",
                    ))
                }
            }
        }
    }
    Ok(())
}

fn frame_range(
    chunks: &[Chunk],
    key: &WindowRowKey,
    row: usize,
    partition_len: usize,
    peer_range: Range<usize>,
    expr: &WindowExpression,
) -> Result<Range<usize>> {
    let start = match &expr.frame.start_bound {
        WindowFrameBound::Unbounded if expr.frame.start_is_preceding => 0,
        WindowFrameBound::Unbounded => partition_len,
        WindowFrameBound::CurrentRow if expr.frame.frame_type == WindowFrameType::Range => {
            peer_range.start
        }
        WindowFrameBound::CurrentRow => row,
        WindowFrameBound::Offset(offset) if expr.frame.frame_type == WindowFrameType::Rows => {
            let offset = frame_offset(chunks, key, offset)?;
            if expr.frame.start_is_preceding {
                row.saturating_sub(offset)
            } else {
                row.saturating_add(offset).min(partition_len)
            }
        }
        WindowFrameBound::Offset(_) => {
            return Err(paro_error::not_implemented(
                "RANGE offset window frames require typed range arithmetic",
            ))
        }
    };
    let end = match &expr.frame.end_bound {
        WindowFrameBound::Unbounded if expr.frame.end_is_preceding => 0,
        WindowFrameBound::Unbounded => partition_len,
        WindowFrameBound::CurrentRow if expr.frame.frame_type == WindowFrameType::Range => {
            peer_range.end
        }
        WindowFrameBound::CurrentRow => row + 1,
        WindowFrameBound::Offset(offset) if expr.frame.frame_type == WindowFrameType::Rows => {
            let offset = frame_offset(chunks, key, offset)?;
            if expr.frame.end_is_preceding {
                if offset > row {
                    0
                } else {
                    row - offset + 1
                }
            } else {
                row.saturating_add(offset)
                    .saturating_add(1)
                    .min(partition_len)
            }
        }
        WindowFrameBound::Offset(_) => {
            return Err(paro_error::not_implemented(
                "RANGE offset window frames require typed range arithmetic",
            ))
        }
    };
    let start = start.min(partition_len);
    let end = end.min(partition_len);
    Ok(if start < end {
        start..end
    } else {
        start..start
    })
}

fn frame_offset(chunks: &[Chunk], key: &WindowRowKey, expr: &Expression) -> Result<usize> {
    let value = value_from_expr(chunks, key, expr);
    if value.is_null() {
        return Err(paro_error::invalid_input(
            "window frame offset must not be null",
        ));
    }
    let offset = value_to_i64(&value)
        .ok_or_else(|| paro_error::invalid_input("window frame offset must be an integer"))?;
    usize::try_from(offset)
        .map_err(|_| paro_error::invalid_input("window frame offset must not be negative"))
}

fn build_peer_ranges(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    orders: &[OrderByExpression],
) -> Vec<Range<usize>> {
    let partition_len = partition.end - partition.start;
    if orders.is_empty() {
        return vec![0..partition_len; partition_len];
    }

    let mut peer_ranges = vec![0..0; partition_len];
    let mut peer_start = 0;
    while peer_start < partition_len {
        let mut peer_end = peer_start + 1;
        while peer_end < partition_len
            && are_peers(
                chunks,
                &sorted_keys[partition.start + peer_start],
                &sorted_keys[partition.start + peer_end],
                orders,
            )
        {
            peer_end += 1;
        }
        for range in &mut peer_ranges[peer_start..peer_end] {
            *range = peer_start..peer_end;
        }
        peer_start = peer_end;
    }
    peer_ranges
}
