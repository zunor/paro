// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Window breaker kernel for the role-specific runtime.
//!
//! Build sinks only retain input chunks on the per-chunk path. The blocking
//! work happens during sink finish, which materializes immutable output chunks
//! for the emit source to scan without touching the shared handle again.

use std::cmp::Ordering;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_function::window::WindowFunctionType;
use paro_planner::expression::{
    Expression, OrderByExpression, WindowExpression, WindowFrameBound, WindowInvocation,
};

use crate::physical::specs::WindowSpec;

mod frame;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WindowRowKey {
    chunk_idx: usize,
    row_idx: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WindowPartition {
    start: usize,
    end: usize,
}

pub fn build_window_output_chunks(
    spec: &WindowSpec,
    input_chunks: &[Chunk],
    allocator: Arc<dyn Allocator>,
) -> Result<Vec<Chunk>> {
    validate_window_spec(spec)?;

    let mut keys = build_row_keys(input_chunks);
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(sort_expr) = spec.expressions.get(sort_expression_index(spec)) {
        if !sort_expr.partitions.is_empty() || !sort_expr.orders.is_empty() {
            keys.sort_by(|left, right| compare_window_order(input_chunks, left, right, sort_expr));
        }
    }

    let partitions = spec
        .expressions
        .get(sort_expression_index(spec))
        .map(|expr| find_partitions(input_chunks, &keys, expr))
        .unwrap_or_else(|| {
            vec![WindowPartition {
                start: 0,
                end: keys.len(),
            }]
        });
    let mut output = WindowOutputBuilder::new(spec, input_chunks, &keys, allocator)?;
    for (expr_idx, expr) in spec.expressions.iter().enumerate() {
        write_expression_results(
            input_chunks,
            &keys,
            &partitions,
            expr_idx,
            expr,
            &mut output,
        )?;
    }
    output.finish()
}

struct WindowOutputBuilder {
    input_width: usize,
    counts: Vec<usize>,
    vectors: Vec<Vec<Vector>>,
    allocator: Arc<dyn Allocator>,
}

impl WindowOutputBuilder {
    fn new(
        spec: &WindowSpec,
        chunks: &[Chunk],
        sorted_keys: &[WindowRowKey],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let chunk_count = sorted_keys.len().div_ceil(VECTOR_SIZE);
        let mut counts = Vec::with_capacity(chunk_count);
        let mut vectors = Vec::with_capacity(chunk_count);

        for chunk_idx in 0..chunk_count {
            let start = chunk_idx * VECTOR_SIZE;
            let count = (sorted_keys.len() - start).min(VECTOR_SIZE);
            counts.push(count);

            let mut chunk_vectors = Vec::with_capacity(spec.output_types.len());
            for (col_idx, ty) in spec.output_types.iter().enumerate() {
                let mut vector = Vector::try_new(ty.clone(), count, allocator.clone())?;
                if col_idx < spec.input_width {
                    for output_row in 0..count {
                        let key = &sorted_keys[start + output_row];
                        let source = &chunks[key.chunk_idx].data[col_idx];
                        vector.try_copy_at(output_row, source, key.row_idx)?;
                    }
                }
                chunk_vectors.push(vector);
            }
            vectors.push(chunk_vectors);
        }

        Ok(Self {
            input_width: spec.input_width,
            counts,
            vectors,
            allocator,
        })
    }

    #[inline(always)]
    fn set_window_value(&mut self, expr_idx: usize, global_row: usize, value: &Value) {
        let chunk_idx = global_row / VECTOR_SIZE;
        let row_idx = global_row % VECTOR_SIZE;
        self.vectors[chunk_idx][self.input_width + expr_idx].set_value(row_idx, value);
    }

    #[inline(always)]
    fn set_window_i64(&mut self, expr_idx: usize, global_row: usize, value: i64) {
        let chunk_idx = global_row / VECTOR_SIZE;
        let row_idx = global_row % VECTOR_SIZE;
        self.vectors[chunk_idx][self.input_width + expr_idx].set_i64(row_idx, value);
    }

    #[inline(always)]
    fn set_window_f64(&mut self, expr_idx: usize, global_row: usize, value: f64) {
        let chunk_idx = global_row / VECTOR_SIZE;
        let row_idx = global_row % VECTOR_SIZE;
        self.vectors[chunk_idx][self.input_width + expr_idx].set_f64(row_idx, value);
    }

    fn finish(self) -> Result<Vec<Chunk>> {
        let Self {
            counts,
            vectors,
            allocator,
            ..
        } = self;
        let mut chunks = Vec::with_capacity(vectors.len());
        for (chunk_idx, mut vectors) in vectors.into_iter().enumerate() {
            let count = counts[chunk_idx];
            for vector in &mut vectors {
                vector.try_set_count(count)?;
            }
            let vectors = vectors.into_iter().map(Arc::new).collect();
            let mut chunk = Chunk::from_arc_vectors(vectors, allocator.clone());
            chunk.try_set_cardinality(count)?;
            chunks.push(chunk);
        }
        Ok(chunks)
    }
}

fn validate_window_spec(spec: &WindowSpec) -> Result<()> {
    let Some(first) = spec.expressions.first() else {
        return Ok(());
    };

    validate_window_expression(first)?;
    for (idx, expr) in spec.expressions.iter().enumerate().skip(1) {
        validate_window_expression(expr)?;
        if !first.has_same_layout(expr) {
            return Err(paro_error::not_implemented(format!(
                "Window breaker requires one partition/order layout per WindowSpec; \
                 expression 0 and expression {idx} use different layouts"
            )));
        }
    }
    Ok(())
}

fn validate_window_expression(expr: &WindowExpression) -> Result<()> {
    expr.verify_bound_contract()?;
    for partition in &expr.partitions {
        validate_direct_value_expression(partition, "window partition")?;
    }
    for order in &expr.orders {
        validate_direct_value_expression(&order.expression, "window order")?;
    }
    for child in expr.arguments() {
        validate_direct_value_expression(child, "window function argument")?;
    }
    if let WindowInvocation::Aggregate(aggregate) = &expr.invocation {
        if let Some(filter) = &aggregate.filter {
            validate_direct_value_expression(filter, "aggregate window filter")?;
        }
        for order in &aggregate.order_bys {
            validate_direct_value_expression(&order.expression, "aggregate argument order")?;
        }
    }
    if let WindowFrameBound::Offset(offset) = &expr.frame.start_bound {
        validate_direct_value_expression(offset, "window frame start")?;
    }
    if let WindowFrameBound::Offset(offset) = &expr.frame.end_bound {
        validate_direct_value_expression(offset, "window frame end")?;
    }
    Ok(())
}

fn validate_direct_value_expression(expr: &Expression, context: &str) -> Result<()> {
    match expr {
        Expression::Constant(_) | Expression::Reference(_) | Expression::ColumnRef(_) => Ok(()),
        _ => Err(paro_error::not_implemented(format!(
            "{context} currently supports direct references and constants only"
        ))),
    }
}

fn build_row_keys(chunks: &[Chunk]) -> Vec<WindowRowKey> {
    let row_count = chunks.iter().map(Chunk::size).sum();
    let mut keys = Vec::with_capacity(row_count);
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        for row_idx in 0..chunk.size() {
            keys.push(WindowRowKey { chunk_idx, row_idx });
        }
    }
    keys
}

fn sort_expression_index(spec: &WindowSpec) -> usize {
    spec.expressions
        .iter()
        .enumerate()
        .max_by_key(|(_, expr)| (expr.partitions.len(), expr.orders.len()))
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

fn compare_window_order(
    chunks: &[Chunk],
    left: &WindowRowKey,
    right: &WindowRowKey,
    expr: &WindowExpression,
) -> Ordering {
    for partition in &expr.partitions {
        let cmp = compare_expression(chunks, left, right, partition, true, false);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    for order in &expr.orders {
        let cmp = compare_order_expression(chunks, left, right, order);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_order_expression(
    chunks: &[Chunk],
    left: &WindowRowKey,
    right: &WindowRowKey,
    order: &OrderByExpression,
) -> Ordering {
    compare_expression(
        chunks,
        left,
        right,
        &order.expression,
        order.ascending,
        order.nulls_first,
    )
}

fn compare_expression(
    chunks: &[Chunk],
    left: &WindowRowKey,
    right: &WindowRowKey,
    expr: &Expression,
    ascending: bool,
    nulls_first: bool,
) -> Ordering {
    let left = value_from_expr(chunks, left, expr);
    let right = value_from_expr(chunks, right, expr);
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let cmp = left.partial_cmp(&right).unwrap_or(Ordering::Equal);
            if ascending {
                cmp
            } else {
                cmp.reverse()
            }
        }
    }
}

fn find_partitions(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    expr: &WindowExpression,
) -> Vec<WindowPartition> {
    if sorted_keys.is_empty() {
        return Vec::new();
    }
    if expr.partitions.is_empty() {
        return vec![WindowPartition {
            start: 0,
            end: sorted_keys.len(),
        }];
    }

    let mut partitions = Vec::new();
    let mut start = 0;
    for idx in 1..sorted_keys.len() {
        if !same_partition(
            chunks,
            &sorted_keys[idx - 1],
            &sorted_keys[idx],
            &expr.partitions,
        ) {
            partitions.push(WindowPartition { start, end: idx });
            start = idx;
        }
    }
    partitions.push(WindowPartition {
        start,
        end: sorted_keys.len(),
    });
    partitions
}

fn same_partition(
    chunks: &[Chunk],
    left: &WindowRowKey,
    right: &WindowRowKey,
    partitions: &[Expression],
) -> bool {
    partitions.iter().all(|expr| {
        let left = value_from_expr(chunks, left, expr);
        let right = value_from_expr(chunks, right, expr);
        if left.is_null() || right.is_null() {
            left.is_null() && right.is_null()
        } else {
            left == right
        }
    })
}

fn are_peers(
    chunks: &[Chunk],
    left: &WindowRowKey,
    right: &WindowRowKey,
    orders: &[OrderByExpression],
) -> bool {
    orders
        .iter()
        .all(|order| compare_order_expression(chunks, left, right, order) == Ordering::Equal)
}

fn write_expression_results(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partitions: &[WindowPartition],
    expr_idx: usize,
    expr: &WindowExpression,
    output: &mut WindowOutputBuilder,
) -> Result<()> {
    for &partition in partitions {
        write_partition_expression_results(chunks, sorted_keys, partition, expr_idx, expr, output)?;
    }
    Ok(())
}

fn write_partition_expression_results(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr_idx: usize,
    expr: &WindowExpression,
    output: &mut WindowOutputBuilder,
) -> Result<()> {
    let partition_size = partition.end - partition.start;
    let Some((function, _)) = expr.native_invocation() else {
        return write_aggregate_results(chunks, sorted_keys, partition, expr_idx, expr, output);
    };
    match function.function_type {
        WindowFunctionType::RowNumber => {
            for absolute_idx in partition.start..partition.end {
                let idx = absolute_idx - partition.start;
                output.set_window_i64(expr_idx, absolute_idx, (idx + 1) as i64);
            }
        }
        WindowFunctionType::Rank => {
            let mut rank = 1i64;
            for idx in 0..partition_size {
                if idx > 0 {
                    let prev = &sorted_keys[partition.start + idx - 1];
                    let current = &sorted_keys[partition.start + idx];
                    if !are_peers(chunks, prev, current, &expr.orders) {
                        rank = (idx + 1) as i64;
                    }
                }
                output.set_window_i64(expr_idx, partition.start + idx, rank);
            }
        }
        WindowFunctionType::DenseRank => {
            let mut rank = 1i64;
            for idx in 0..partition_size {
                if idx > 0 {
                    let prev = &sorted_keys[partition.start + idx - 1];
                    let current = &sorted_keys[partition.start + idx];
                    if !are_peers(chunks, prev, current, &expr.orders) {
                        rank += 1;
                    }
                }
                output.set_window_i64(expr_idx, partition.start + idx, rank);
            }
        }
        WindowFunctionType::PercentRank => {
            if partition_size <= 1 {
                for absolute_idx in partition.start..partition.end {
                    output.set_window_f64(expr_idx, absolute_idx, 0.0);
                }
                return Ok(());
            }
            let mut rank = 1i64;
            for idx in 0..partition_size {
                if idx > 0 {
                    let prev = &sorted_keys[partition.start + idx - 1];
                    let current = &sorted_keys[partition.start + idx];
                    if !are_peers(chunks, prev, current, &expr.orders) {
                        rank = (idx + 1) as i64;
                    }
                }
                output.set_window_f64(
                    expr_idx,
                    partition.start + idx,
                    (rank - 1) as f64 / (partition_size - 1) as f64,
                );
            }
        }
        WindowFunctionType::CumeDist => {
            let mut peer_end = 0usize;
            for idx in 0..partition_size {
                if idx >= peer_end {
                    peer_end = idx + 1;
                    while peer_end < partition_size {
                        let current = &sorted_keys[partition.start + idx];
                        let next = &sorted_keys[partition.start + peer_end];
                        if !are_peers(chunks, current, next, &expr.orders) {
                            break;
                        }
                        peer_end += 1;
                    }
                }
                output.set_window_f64(
                    expr_idx,
                    partition.start + idx,
                    peer_end as f64 / partition_size as f64,
                );
            }
        }
        WindowFunctionType::Ntile => {
            let Some(bucket_count) = ntile_bucket_count(chunks, sorted_keys, partition, expr)?
            else {
                let null = Value::Null(expr.return_type());
                for absolute_idx in partition.start..partition.end {
                    output.set_window_value(expr_idx, absolute_idx, &null);
                }
                return Ok(());
            };
            for absolute_idx in partition.start..partition.end {
                let row = absolute_idx - partition.start;
                output.set_window_i64(
                    expr_idx,
                    absolute_idx,
                    ntile_bucket(row, partition_size, bucket_count) as i64,
                );
            }
        }
        WindowFunctionType::Lead | WindowFunctionType::Lag => {
            write_lead_lag(chunks, sorted_keys, partition, expr_idx, expr, output)?;
        }
        WindowFunctionType::FirstValue
        | WindowFunctionType::LastValue
        | WindowFunctionType::NthValue => {
            write_frame_value_results(chunks, sorted_keys, partition, expr_idx, expr, output)?;
        }
    }
    Ok(())
}

fn write_lead_lag(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr_idx: usize,
    expr: &WindowExpression,
    output: &mut WindowOutputBuilder,
) -> Result<()> {
    let is_lead = expr
        .native_invocation()
        .is_some_and(|(function, _)| function.function_type == WindowFunctionType::Lead);
    let values = frame::WindowValueIndex::build(chunks, sorted_keys, partition, expr)?;

    for absolute_idx in partition.start..partition.end {
        let offset = if let Some(offset) = expr.arguments().get(1) {
            let value = value_from_expr(chunks, &sorted_keys[absolute_idx], offset);
            if value.is_null() {
                let null = Value::Null(expr.return_type());
                output.set_window_value(expr_idx, absolute_idx, &null);
                continue;
            }
            value_to_i64(&value).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "offset argument of {} must be an integer",
                    expr.function_name()
                ))
            })?
        } else {
            1
        };
        let value = if let Some(target) =
            values.row_from_current(absolute_idx, offset, is_lead, expr.ignore_nulls)
        {
            value_argument(chunks, &sorted_keys[partition.start + target], expr)
        } else {
            let default = expr
                .arguments()
                .get(2)
                .map(|child| value_from_expr(chunks, &sorted_keys[absolute_idx], child))
                .unwrap_or_else(|| Value::Null(expr.return_type()));
            default
        };
        output.set_window_value(expr_idx, absolute_idx, &value);
    }
    Ok(())
}

fn write_aggregate_results(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr_idx: usize,
    expr: &WindowExpression,
    output: &mut WindowOutputBuilder,
) -> Result<()> {
    if frame::aggregate_frame_is_partition_constant(expr) {
        let value = frame::aggregate_window_value(
            chunks,
            sorted_keys,
            partition.start..partition.end,
            expr,
            output.allocator.clone(),
        )?;
        for absolute_idx in partition.start..partition.end {
            output.set_window_value(expr_idx, absolute_idx, &value);
        }
        return Ok(());
    }

    // The generic sorted-window fallback deliberately favors one bound
    // aggregate ABI over function-name-specific kernels. It recomputes each
    // frame today; incremental state is a separate aggregate capability, not
    // something the planner may infer from a display name.
    let frames = frame::WindowFrameIndex::build(chunks, sorted_keys, partition, expr)?;
    for absolute_idx in partition.start..partition.end {
        let relative = frames.relative_range(absolute_idx);
        let value = frame::aggregate_window_value(
            chunks,
            sorted_keys,
            (partition.start + relative.start)..(partition.start + relative.end),
            expr,
            output.allocator.clone(),
        )?;
        output.set_window_value(expr_idx, absolute_idx, &value);
    }
    Ok(())
}

fn write_frame_value_results(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr_idx: usize,
    expr: &WindowExpression,
    output: &mut WindowOutputBuilder,
) -> Result<()> {
    let frames = frame::WindowFrameIndex::build(chunks, sorted_keys, partition, expr)?;
    let values = frame::WindowValueIndex::build(chunks, sorted_keys, partition, expr)?;
    for absolute_idx in partition.start..partition.end {
        let value = values.evaluate(
            chunks,
            sorted_keys,
            frames.relative_range(absolute_idx),
            absolute_idx,
            expr,
        )?;
        output.set_window_value(expr_idx, absolute_idx, &value);
    }
    Ok(())
}

fn ntile_bucket_count(
    chunks: &[Chunk],
    sorted_keys: &[WindowRowKey],
    partition: WindowPartition,
    expr: &WindowExpression,
) -> Result<Option<usize>> {
    let argument = expr
        .arguments()
        .first()
        .ok_or_else(|| paro_error::internal("NTILE requires a bucket-count argument"))?;
    let value = value_from_expr(chunks, &sorted_keys[partition.start], argument);
    if value.is_null() {
        return Ok(None);
    }
    let count = value_to_i64(&value)
        .ok_or_else(|| paro_error::invalid_input("argument of ntile must be an integer"))?;
    if count <= 0 {
        return Err(paro_error::invalid_input(
            "argument of ntile must be greater than zero",
        ));
    }
    usize::try_from(count)
        .map(Some)
        .map_err(|_| paro_error::invalid_input("argument of ntile is outside the supported range"))
}

/// Return the one-based SQL NTILE bucket for a zero-based row.
///
/// The remainder belongs to the leading buckets. Keeping the calculation in
/// quotient/remainder form also avoids the `row * bucket_count` overflow of a
/// proportional formula.
fn ntile_bucket(row: usize, row_count: usize, bucket_count: usize) -> usize {
    debug_assert!(row < row_count);
    debug_assert!(bucket_count > 0);

    let smaller_bucket_size = row_count / bucket_count;
    let larger_bucket_count = row_count % bucket_count;
    let larger_bucket_size = smaller_bucket_size + 1;
    let rows_in_larger_buckets = larger_bucket_count * larger_bucket_size;

    if row < rows_in_larger_buckets {
        row / larger_bucket_size + 1
    } else {
        debug_assert!(smaller_bucket_size > 0);
        larger_bucket_count + (row - rows_in_larger_buckets) / smaller_bucket_size + 1
    }
}

fn value_argument(chunks: &[Chunk], key: &WindowRowKey, expr: &WindowExpression) -> Value {
    expr.arguments()
        .first()
        .map(|child| value_from_expr(chunks, key, child))
        .unwrap_or_else(|| Value::Null(expr.return_type()))
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::TinyInt(value) => Some(*value as i64),
        Value::SmallInt(value) => Some(*value as i64),
        Value::Integer(value) => Some(*value as i64),
        Value::BigInt(value) => Some(*value),
        Value::UTinyInt(value) => Some(*value as i64),
        Value::USmallInt(value) => Some(*value as i64),
        Value::UInteger(value) => Some(*value as i64),
        Value::UBigInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn value_is_null_from_expr(chunks: &[Chunk], key: &WindowRowKey, expr: &Expression) -> bool {
    match expr {
        Expression::Constant(constant) => constant.value.is_null(),
        Expression::Reference(reference) => chunks
            .get(key.chunk_idx)
            .and_then(|chunk| chunk.column(reference.index))
            .map(|vector| vector.is_null(key.row_idx))
            .unwrap_or(true),
        Expression::ColumnRef(column) => chunks
            .get(key.chunk_idx)
            .and_then(|chunk| chunk.column(column.binding.column_index))
            .map(|vector| vector.is_null(key.row_idx))
            .unwrap_or(true),
        _ => unreachable!(
            "window runtime received an expression that validation should have rejected: {expr:?}"
        ),
    }
}

fn value_from_expr(chunks: &[Chunk], key: &WindowRowKey, expr: &Expression) -> Value {
    match expr {
        Expression::Constant(constant) => constant.value.clone(),
        Expression::Reference(reference) => chunks
            .get(key.chunk_idx)
            .and_then(|chunk| chunk.get_value(reference.index, key.row_idx))
            .unwrap_or_else(|| Value::Null(reference.return_type.clone())),
        Expression::ColumnRef(column) => chunks
            .get(key.chunk_idx)
            .and_then(|chunk| chunk.get_value(column.binding.column_index, key.row_idx))
            .unwrap_or_else(|| Value::Null(column.return_type.clone())),
        _ => unreachable!(
            "window runtime received an expression that validation should have rejected: {expr:?}"
        ),
    }
}
