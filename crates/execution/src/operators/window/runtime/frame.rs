// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate window frame kernel for the breaker runtime.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{
    Expression, OrderByExpression, WindowExpression, WindowFrameBound, WindowFrameType,
};

use super::{are_peers, value_from_expr, value_to_i64, WindowPartition, WindowRowKey};

pub(super) fn aggregate_frame_is_partition_constant(expr: &WindowExpression) -> bool {
    if expr.frame.frame_type != WindowFrameType::Range || !expr.orders.is_empty() {
        return false;
    }
    let starts_at_partition = matches!(
        (&expr.frame.start_bound, expr.frame.start_is_preceding),
        (WindowFrameBound::Unbounded, true) | (WindowFrameBound::CurrentRow, _)
    );
    let ends_at_partition = matches!(
        (&expr.frame.end_bound, expr.frame.end_is_preceding),
        (WindowFrameBound::Unbounded, false) | (WindowFrameBound::CurrentRow, _)
    );
    starts_at_partition && ends_at_partition
}

pub(super) fn aggregate_window_value(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr: &WindowExpression,
    absolute_idx: usize,
) -> Result<Value> {
    let (frame_start, frame_end) =
        frame_bounds(chunks, sorted_keys, partition, expr, absolute_idx)?;
    match aggregate_frame_function(&expr.function.name) {
        Some(AggregateFrameFunction::Count) => {
            aggregate_count(chunks, sorted_keys, frame_start, frame_end, expr)
        }
        Some(AggregateFrameFunction::Sum) => {
            aggregate_sum(chunks, sorted_keys, frame_start, frame_end, expr)
        }
        Some(AggregateFrameFunction::Avg) => {
            aggregate_avg(chunks, sorted_keys, frame_start, frame_end, expr)
        }
        Some(AggregateFrameFunction::Min) => {
            aggregate_min_max(chunks, sorted_keys, frame_start, frame_end, expr, false)
        }
        Some(AggregateFrameFunction::Max) => {
            aggregate_min_max(chunks, sorted_keys, frame_start, frame_end, expr, true)
        }
        None => Err(paro_error::not_implemented(format!(
            "Aggregate window function '{}' is not supported by the window breaker frame kernel",
            expr.function.name
        ))),
    }
}

pub(super) fn try_aggregate_partition_fast(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr: &WindowExpression,
) -> Result<Option<Vec<Value>>> {
    if expr.frame.frame_type != WindowFrameType::Rows {
        return Ok(None);
    }
    let Some(function) = aggregate_frame_function(&expr.function.name) else {
        return Ok(None);
    };
    if function != AggregateFrameFunction::Count && expr.children.len() != 1 {
        return Ok(None);
    }

    let partition_len = partition.end.saturating_sub(partition.start);
    let mut starts = Vec::with_capacity(partition_len);
    let mut ends = Vec::with_capacity(partition_len);
    for absolute_idx in partition.start..partition.end {
        let (start, end) = frame_bounds(chunks, sorted_keys, partition, expr, absolute_idx)?;
        starts.push(start - partition.start);
        ends.push(end - partition.start);
    }

    match function {
        AggregateFrameFunction::Count if expr.children.is_empty() => {
            Ok(Some(fast_count_star(&starts, &ends)))
        }
        AggregateFrameFunction::Count => {
            let values = partition_argument_values(chunks, sorted_keys, partition, expr)?;
            Ok(Some(fast_count_values(&starts, &ends, &values)))
        }
        AggregateFrameFunction::Sum => {
            let values = partition_argument_values(chunks, sorted_keys, partition, expr)?;
            Ok(fast_sum_values(&starts, &ends, &values, &expr.return_type))
        }
        AggregateFrameFunction::Avg => {
            let values = partition_argument_values(chunks, sorted_keys, partition, expr)?;
            Ok(Some(fast_avg_values(
                &starts,
                &ends,
                &values,
                &expr.return_type,
            )))
        }
        AggregateFrameFunction::Min | AggregateFrameFunction::Max => {
            let values = partition_argument_values(chunks, sorted_keys, partition, expr)?;
            Ok(Some(fast_min_max_values(
                &starts,
                &ends,
                &values,
                &expr.return_type,
                function == AggregateFrameFunction::Max,
            )?))
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AggregateFrameFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

fn aggregate_frame_function(name: &str) -> Option<AggregateFrameFunction> {
    if name.eq_ignore_ascii_case("count") {
        Some(AggregateFrameFunction::Count)
    } else if name.eq_ignore_ascii_case("sum") {
        Some(AggregateFrameFunction::Sum)
    } else if name.eq_ignore_ascii_case("avg") {
        Some(AggregateFrameFunction::Avg)
    } else if name.eq_ignore_ascii_case("min") {
        Some(AggregateFrameFunction::Min)
    } else if name.eq_ignore_ascii_case("max") {
        Some(AggregateFrameFunction::Max)
    } else {
        None
    }
}

fn frame_bounds(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr: &WindowExpression,
    absolute_idx: usize,
) -> Result<(usize, usize)> {
    let row = absolute_idx - partition.start;
    let peer = || peer_bounds(chunks, sorted_keys, partition, absolute_idx, &expr.orders);
    let start = match &expr.frame.start_bound {
        WindowFrameBound::Unbounded if expr.frame.start_is_preceding => partition.start,
        WindowFrameBound::Unbounded => partition.end,
        WindowFrameBound::CurrentRow if expr.frame.frame_type == WindowFrameType::Range => peer().0,
        WindowFrameBound::CurrentRow => absolute_idx,
        WindowFrameBound::Offset(offset) if expr.frame.frame_type == WindowFrameType::Rows => {
            let offset = frame_offset(chunks, &sorted_keys[absolute_idx], offset)?;
            if expr.frame.start_is_preceding {
                partition.start + row.saturating_sub(offset)
            } else {
                (absolute_idx + offset).min(partition.end)
            }
        }
        WindowFrameBound::Offset(_) => {
            return Err(paro_error::not_implemented(
                "RANGE offset window frames require typed range arithmetic",
            ))
        }
    };
    let end = match &expr.frame.end_bound {
        WindowFrameBound::Unbounded if expr.frame.end_is_preceding => partition.start,
        WindowFrameBound::Unbounded => partition.end,
        WindowFrameBound::CurrentRow if expr.frame.frame_type == WindowFrameType::Range => peer().1,
        WindowFrameBound::CurrentRow => absolute_idx + 1,
        WindowFrameBound::Offset(offset) if expr.frame.frame_type == WindowFrameType::Rows => {
            let offset = frame_offset(chunks, &sorted_keys[absolute_idx], offset)?;
            if expr.frame.end_is_preceding {
                partition.start + row.saturating_sub(offset) + 1
            } else {
                (absolute_idx + offset + 1).min(partition.end)
            }
        }
        WindowFrameBound::Offset(_) => {
            return Err(paro_error::not_implemented(
                "RANGE offset window frames require typed range arithmetic",
            ))
        }
    };
    Ok((start.min(partition.end), end.min(partition.end).max(start)))
}

fn partition_argument_values(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr: &WindowExpression,
) -> Result<Vec<Value>> {
    let child = expr
        .children
        .first()
        .ok_or_else(|| paro_error::internal("window aggregate requires one argument"))?;
    Ok(sorted_keys[partition.start..partition.end]
        .iter()
        .map(|key| value_from_expr(chunks, key, child))
        .collect())
}

fn fast_count_star(starts: &[usize], ends: &[usize]) -> Vec<Value> {
    starts
        .iter()
        .zip(ends)
        .map(|(&start, &end)| Value::BigInt(end.saturating_sub(start) as i64))
        .collect()
}

fn fast_count_values(starts: &[usize], ends: &[usize], values: &[Value]) -> Vec<Value> {
    let mut prefix = Vec::with_capacity(values.len() + 1);
    prefix.push(0i64);
    for value in values {
        let next = prefix.last().copied().unwrap_or(0) + i64::from(!value.is_null());
        prefix.push(next);
    }
    starts
        .iter()
        .zip(ends)
        .map(|(&start, &end)| Value::BigInt(prefix[end] - prefix[start]))
        .collect()
}

fn fast_sum_values(
    starts: &[usize],
    ends: &[usize],
    values: &[Value],
    return_type: &LogicalType,
) -> Option<Vec<Value>> {
    let use_float = matches!(return_type, LogicalType::Float | LogicalType::Double)
        || values
            .iter()
            .any(|value| value_to_f64(value).is_some() && value_to_i64(value).is_none());
    let mut counts = Vec::with_capacity(values.len() + 1);
    let mut int_prefix = Vec::with_capacity(values.len() + 1);
    let mut float_prefix = Vec::with_capacity(values.len() + 1);
    counts.push(0usize);
    int_prefix.push(0i128);
    float_prefix.push(0.0f64);
    for value in values {
        let mut count = *counts.last().unwrap_or(&0);
        let mut int_sum = *int_prefix.last().unwrap_or(&0);
        let mut float_sum = *float_prefix.last().unwrap_or(&0.0);
        if let Some(number) = value_to_f64(value) {
            count += 1;
            if use_float {
                float_sum += number;
            } else if let Some(integer) = value_to_i64(value) {
                int_sum += integer as i128;
            } else {
                return None;
            }
        }
        counts.push(count);
        int_prefix.push(int_sum);
        float_prefix.push(float_sum);
    }

    Some(
        starts
            .iter()
            .zip(ends)
            .map(|(&start, &end)| {
                if counts[end] == counts[start] {
                    return Value::Null(return_type.clone());
                }
                number_value_for_type(
                    return_type,
                    int_prefix[end] - int_prefix[start],
                    float_prefix[end] - float_prefix[start],
                    use_float,
                )
            })
            .collect(),
    )
}

fn fast_avg_values(
    starts: &[usize],
    ends: &[usize],
    values: &[Value],
    return_type: &LogicalType,
) -> Vec<Value> {
    let mut counts = Vec::with_capacity(values.len() + 1);
    let mut sums = Vec::with_capacity(values.len() + 1);
    counts.push(0usize);
    sums.push(0.0f64);
    for value in values {
        let mut count = *counts.last().unwrap_or(&0);
        let mut sum = *sums.last().unwrap_or(&0.0);
        if let Some(number) = value_to_f64(value) {
            count += 1;
            sum += number;
        }
        counts.push(count);
        sums.push(sum);
    }
    starts
        .iter()
        .zip(ends)
        .map(|(&start, &end)| {
            let count = counts[end] - counts[start];
            if count == 0 {
                Value::Null(return_type.clone())
            } else {
                Value::Double((sums[end] - sums[start]) / count as f64)
            }
        })
        .collect()
}

fn fast_min_max_values(
    starts: &[usize],
    ends: &[usize],
    values: &[Value],
    return_type: &LogicalType,
    is_max: bool,
) -> Result<Vec<Value>> {
    let sparse = MinMaxSparseTable::new(values, is_max)?;
    starts
        .iter()
        .zip(ends)
        .map(|(&start, &end)| {
            Ok(sparse
                .query(values, start, end)?
                .map(|idx| values[idx].clone())
                .unwrap_or_else(|| Value::Null(return_type.clone())))
        })
        .collect()
}

struct MinMaxSparseTable {
    levels: Vec<Vec<Option<usize>>>,
    logs: Vec<usize>,
    is_max: bool,
}

impl MinMaxSparseTable {
    fn new(values: &[Value], is_max: bool) -> Result<Self> {
        let n = values.len();
        let mut logs = vec![0usize; n + 1];
        for idx in 2..=n {
            logs[idx] = logs[idx / 2] + 1;
        }
        let mut levels = Vec::new();
        levels.push(
            values
                .iter()
                .enumerate()
                .map(|(idx, value)| (!value.is_null()).then_some(idx))
                .collect::<Vec<_>>(),
        );
        let mut width = 1usize;
        while width.saturating_mul(2) <= n {
            let prev = levels.last().expect("sparse table has base level");
            let next_len = n - width * 2 + 1;
            let mut next = Vec::with_capacity(next_len);
            for idx in 0..next_len {
                next.push(better_index(values, prev[idx], prev[idx + width], is_max)?);
            }
            levels.push(next);
            width *= 2;
        }
        Ok(Self {
            levels,
            logs,
            is_max,
        })
    }

    fn query(&self, values: &[Value], start: usize, end: usize) -> Result<Option<usize>> {
        if start >= end {
            return Ok(None);
        }
        let len = end - start;
        let level = self.logs[len];
        let width = 1usize << level;
        better_index(
            values,
            self.levels[level][start],
            self.levels[level][end - width],
            self.is_max,
        )
    }
}

fn better_index(
    values: &[Value],
    left: Option<usize>,
    right: Option<usize>,
    is_max: bool,
) -> Result<Option<usize>> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(idx), None) | (None, Some(idx)) => Ok(Some(idx)),
        (Some(left), Some(right)) => {
            let replace = values[right]
                .partial_cmp(&values[left])
                .map(|ordering| {
                    if is_max {
                        ordering.is_gt()
                    } else {
                        ordering.is_lt()
                    }
                })
                .ok_or_else(|| {
                    paro_error::invalid_input(format!(
                        "window MIN/MAX values are not comparable: left={:?}, right={:?}",
                        values[left], values[right]
                    ))
                })?;
            Ok(Some(if replace { right } else { left }))
        }
    }
}

fn frame_offset(chunks: &[Chunk], key: &WindowRowKey, expr: &Expression) -> Result<usize> {
    let value = value_from_expr(chunks, key, expr);
    value_to_i64(&value)
        .and_then(|value| usize::try_from(value.max(0)).ok())
        .ok_or_else(|| {
            paro_error::not_implemented("window frame offset must be a non-negative integer")
        })
}

fn peer_bounds(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    absolute_idx: usize,
    orders: &[OrderByExpression],
) -> (usize, usize) {
    if orders.is_empty() {
        return (partition.start, partition.end);
    }
    let mut start = absolute_idx;
    while start > partition.start
        && are_peers(
            chunks,
            &sorted_keys[start - 1],
            &sorted_keys[absolute_idx],
            orders,
        )
    {
        start -= 1;
    }
    let mut end = absolute_idx + 1;
    while end < partition.end
        && are_peers(
            chunks,
            &sorted_keys[absolute_idx],
            &sorted_keys[end],
            orders,
        )
    {
        end += 1;
    }
    (start, end)
}

fn aggregate_count(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frame_start: usize,
    frame_end: usize,
    expr: &WindowExpression,
) -> Result<Value> {
    if expr.children.is_empty() {
        return Ok(Value::BigInt((frame_end - frame_start) as i64));
    }
    let child = &expr.children[0];
    let mut count = 0i64;
    for key in &sorted_keys[frame_start..frame_end] {
        if !value_from_expr(chunks, key, child).is_null() {
            count += 1;
        }
    }
    Ok(Value::BigInt(count))
}

fn aggregate_sum(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frame_start: usize,
    frame_end: usize,
    expr: &WindowExpression,
) -> Result<Value> {
    let child = expr
        .children
        .first()
        .ok_or_else(|| paro_error::internal("SUM window aggregate requires one argument"))?;
    let mut seen = false;
    let mut int_sum = 0i128;
    let mut float_sum = 0.0f64;
    let mut use_float = matches!(expr.return_type, LogicalType::Float | LogicalType::Double);
    for key in &sorted_keys[frame_start..frame_end] {
        let value = value_from_expr(chunks, key, child);
        if value.is_null() {
            continue;
        }
        seen = true;
        if let Some(number) = value_to_f64(&value) {
            if use_float {
                float_sum += number;
            } else if let Some(integer) = value_to_i64(&value) {
                int_sum += integer as i128;
            } else {
                use_float = true;
                float_sum += int_sum as f64 + number;
            }
        }
    }
    if !seen {
        return Ok(Value::Null(expr.return_type.clone()));
    }
    Ok(number_value_for_type(
        &expr.return_type,
        int_sum,
        float_sum,
        use_float,
    ))
}

fn aggregate_avg(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frame_start: usize,
    frame_end: usize,
    expr: &WindowExpression,
) -> Result<Value> {
    let child = expr
        .children
        .first()
        .ok_or_else(|| paro_error::internal("AVG window aggregate requires one argument"))?;
    let mut sum = 0.0f64;
    let mut count = 0usize;
    for key in &sorted_keys[frame_start..frame_end] {
        let value = value_from_expr(chunks, key, child);
        if let Some(number) = value_to_f64(&value) {
            sum += number;
            count += 1;
        }
    }
    if count == 0 {
        return Ok(Value::Null(expr.return_type.clone()));
    }
    Ok(Value::Double(sum / count as f64))
}

fn aggregate_min_max(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    frame_start: usize,
    frame_end: usize,
    expr: &WindowExpression,
    is_max: bool,
) -> Result<Value> {
    let child = expr
        .children
        .first()
        .ok_or_else(|| paro_error::internal("MIN/MAX window aggregate requires one argument"))?;
    let mut best: Option<Value> = None;
    for key in &sorted_keys[frame_start..frame_end] {
        let value = value_from_expr(chunks, key, child);
        if value.is_null() {
            continue;
        }
        let replace = best
            .as_ref()
            .and_then(|best| value.partial_cmp(best))
            .map(|ordering| {
                if is_max {
                    ordering.is_gt()
                } else {
                    ordering.is_lt()
                }
            })
            .unwrap_or(true);
        if replace {
            best = Some(value);
        }
    }
    Ok(best.unwrap_or_else(|| Value::Null(expr.return_type.clone())))
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::TinyInt(value) => Some(*value as f64),
        Value::SmallInt(value) => Some(*value as f64),
        Value::Integer(value) => Some(*value as f64),
        Value::BigInt(value) => Some(*value as f64),
        Value::UTinyInt(value) => Some(*value as f64),
        Value::USmallInt(value) => Some(*value as f64),
        Value::UInteger(value) => Some(*value as f64),
        Value::UBigInt(value) => Some(*value as f64),
        Value::Float(value) => Some(*value as f64),
        Value::Double(value) => Some(*value),
        _ => None,
    }
}

fn number_value_for_type(
    ty: &LogicalType,
    int_sum: i128,
    float_sum: f64,
    use_float: bool,
) -> Value {
    if use_float {
        return match ty {
            LogicalType::Float => Value::Float(float_sum as f32),
            LogicalType::Double => Value::Double(float_sum),
            _ => Value::Double(float_sum),
        };
    }
    match ty {
        LogicalType::TinyInt => Value::TinyInt(int_sum as i8),
        LogicalType::SmallInt => Value::SmallInt(int_sum as i16),
        LogicalType::Integer => Value::Integer(int_sum as i32),
        LogicalType::BigInt => Value::BigInt(int_sum as i64),
        LogicalType::UTinyInt => Value::UTinyInt(int_sum as u8),
        LogicalType::USmallInt => Value::USmallInt(int_sum as u16),
        LogicalType::UInteger => Value::UInteger(int_sum as u32),
        LogicalType::UBigInt => Value::UBigInt(int_sum as u64),
        LogicalType::Float => Value::Float(int_sum as f32),
        LogicalType::Double => Value::Double(int_sum as f64),
        _ => Value::BigInt(int_sum as i64),
    }
}
