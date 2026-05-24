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
    let name = expr.function.name.to_ascii_lowercase();
    match name.as_str() {
        "count" => aggregate_count(chunks, sorted_keys, frame_start, frame_end, expr),
        "sum" => aggregate_sum(chunks, sorted_keys, frame_start, frame_end, expr),
        "avg" => aggregate_avg(chunks, sorted_keys, frame_start, frame_end, expr),
        "min" => aggregate_min_max(chunks, sorted_keys, frame_start, frame_end, expr, false),
        "max" => aggregate_min_max(chunks, sorted_keys, frame_start, frame_end, expr, true),
        _ => Err(paro_error::not_implemented(format!(
            "Aggregate window function '{}' is not supported by the window breaker frame kernel",
            expr.function.name
        ))),
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
