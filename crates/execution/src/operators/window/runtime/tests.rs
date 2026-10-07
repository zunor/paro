// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::minmax::get_min_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::window::WindowFunction;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ConstantExpression, Expression, OrderByExpression,
    ReferenceExpression, WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
};
use paro_planner::logical::operator::ColumnBinding;

use super::build_window_output_chunks;
use crate::physical::specs::WindowSpec;

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty).into())
}

fn column_ref(index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(7, index), ty).into())
}

fn int_constant(value: i32) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
    )
}

fn bigint_constant(value: i64) -> Expression {
    Expression::Constant(ConstantExpression::new(Value::BigInt(value), LogicalType::BigInt).into())
}

fn null_bigint_constant() -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Null(LogicalType::BigInt), LogicalType::BigInt).into(),
    )
}

fn rank_over(partition_idx: usize, order_idx: usize) -> WindowExpression {
    WindowExpression::native(
        WindowFunction::rank(),
        Vec::new(),
        vec![reference(partition_idx, LogicalType::Integer)],
        vec![OrderByExpression {
            expression: reference(order_idx, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        WindowFrame::default(),
        false,
    )
}

fn window_spec(expressions: Vec<WindowExpression>) -> WindowSpec {
    window_spec_for_types(
        expressions,
        vec![LogicalType::Integer, LogicalType::Integer],
    )
}

fn window_spec_for_types(
    expressions: Vec<WindowExpression>,
    mut output_types: Vec<LogicalType>,
) -> WindowSpec {
    let input_width = output_types.len();
    output_types.extend(expressions.iter().map(WindowExpression::return_type));
    WindowSpec {
        window_index: 1,
        expressions: expressions.into_boxed_slice(),
        input_width,
        output_names: (0..output_types.len())
            .map(|idx| format!("col{idx}"))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output_types: output_types.into_boxed_slice(),
    }
}

fn rank_input_chunk(order_start: i32, count: usize) -> Chunk {
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        count,
        test_allocator(),
    )
    .expect("input chunk");
    chunk.try_set_cardinality(count).expect("cardinality");
    for row in 0..count {
        chunk.set_value(0, row, &Value::Integer(1)).unwrap();
        chunk
            .set_value(1, row, &Value::Integer(order_start + row as i32))
            .unwrap();
    }
    chunk
}

fn value_order_input_chunk(values: &[Value], orders: &[i32]) -> Chunk {
    assert_eq!(values.len(), orders.len());
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        values.len(),
        test_allocator(),
    )
    .expect("input chunk");
    chunk
        .try_set_cardinality(values.len())
        .expect("cardinality");
    for (row, (value, order)) in values.iter().zip(orders).enumerate() {
        chunk.set_value(0, row, value).unwrap();
        chunk.set_value(1, row, &Value::Integer(*order)).unwrap();
    }
    chunk
}

fn offset_value_input_chunk(offsets: &[Value], values: &[i32]) -> Chunk {
    assert_eq!(offsets.len(), values.len());
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::BigInt, LogicalType::Integer],
        values.len(),
        test_allocator(),
    )
    .expect("input chunk");
    chunk
        .try_set_cardinality(values.len())
        .expect("cardinality");
    for (row, (offset, value)) in offsets.iter().zip(values).enumerate() {
        chunk.set_value(0, row, offset).unwrap();
        chunk.set_value(1, row, &Value::Integer(*value)).unwrap();
    }
    chunk
}

fn value_window(
    function: WindowFunction,
    children: Vec<Expression>,
    frame: WindowFrame,
) -> WindowExpression {
    WindowExpression::native(
        function,
        children,
        Vec::new(),
        vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        frame,
        false,
    )
}

fn ntile_window(bucket_count: Expression) -> WindowExpression {
    WindowExpression::native(
        WindowFunction::ntile(),
        vec![bucket_count],
        Vec::new(),
        vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        WindowFrame::get_default_frame(&WindowFunction::ntile()),
        false,
    )
}

fn rows_frame(
    start_bound: WindowFrameBound,
    start_is_preceding: bool,
    end_bound: WindowFrameBound,
    end_is_preceding: bool,
) -> WindowFrame {
    WindowFrame {
        frame_type: WindowFrameType::Rows,
        start_bound,
        start_is_preceding,
        end_bound,
        end_is_preceding,
    }
}

fn whole_partition_rows_frame() -> WindowFrame {
    rows_frame(
        WindowFrameBound::Unbounded,
        true,
        WindowFrameBound::Unbounded,
        false,
    )
}

#[test]
fn window_breaker_rejects_mixed_partition_order_layouts() {
    let spec = window_spec(vec![rank_over(0, 1), rank_over(1, 0)]);

    let err = build_window_output_chunks(&spec, &[], test_allocator()).unwrap_err();
    assert!(err
        .to_string()
        .contains("requires one partition/order layout"));
}

#[test]
fn window_breaker_writes_aggregate_window_results_directly() {
    let (sum, target_types) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("integer SUM binding");
    assert_eq!(target_types, vec![LogicalType::Integer]);
    let return_type = sum.return_type.clone();
    let aggregate =
        AggregateExpression::new(sum, vec![reference(0, LogicalType::Integer)], return_type);
    let spec = window_spec(vec![WindowExpression::aggregate(
        aggregate,
        vec![reference(1, LogicalType::Integer)],
        Vec::new(),
        WindowFrame::default(),
    )]);
    let mut input = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        3,
        test_allocator(),
    )
    .expect("input chunk");
    input.try_set_cardinality(3).expect("cardinality");
    input.set_value(0, 0, &Value::Integer(10)).unwrap();
    input.set_value(1, 0, &Value::Integer(1)).unwrap();
    input.set_value(0, 1, &Value::Integer(20)).unwrap();
    input.set_value(1, 1, &Value::Integer(1)).unwrap();
    input.set_value(0, 2, &Value::Integer(7)).unwrap();
    input.set_value(1, 2, &Value::Integer(2)).unwrap();

    let output =
        build_window_output_chunks(&spec, &[input], test_allocator()).expect("window output");
    assert_eq!(output.len(), 1);
    let chunk = &output[0];
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column(2).unwrap().get_value(0), Value::BigInt(30));
    assert_eq!(chunk.column(2).unwrap().get_value(1), Value::BigInt(30));
    assert_eq!(chunk.column(2).unwrap().get_value(2), Value::BigInt(7));
}

#[test]
fn window_breaker_executes_zero_argument_aggregate_kernel() {
    let aggregate =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    let spec = window_spec(vec![WindowExpression::aggregate(
        aggregate,
        Vec::new(),
        Vec::new(),
        WindowFrame::default(),
    )]);

    let output = build_window_output_chunks(&spec, &[rank_input_chunk(0, 3)], test_allocator())
        .expect("window output");
    let result = output[0].column(2).expect("count output");
    for row in 0..3 {
        assert_eq!(result.get_value(row), Value::BigInt(3));
    }
}

#[test]
fn window_breaker_uses_bound_decimal_min_kernel() {
    let decimal = LogicalType::Decimal {
        precision: 9,
        scale: 2,
    };
    let (minimum, target_types) = get_min_function()
        .bind(std::slice::from_ref(&decimal))
        .expect("decimal MIN binding");
    assert_eq!(target_types, vec![decimal.clone()]);
    let aggregate = AggregateExpression::new(
        minimum,
        vec![reference(0, decimal.clone())],
        decimal.clone(),
    );
    let spec = window_spec_for_types(
        vec![WindowExpression::aggregate(
            aggregate,
            vec![reference(1, LogicalType::Integer)],
            Vec::new(),
            WindowFrame::default(),
        )],
        vec![decimal.clone(), LogicalType::Integer],
    );
    let mut input = Chunk::try_initialize(
        &[decimal.clone(), LogicalType::Integer],
        5,
        test_allocator(),
    )
    .expect("input chunk");
    input.try_set_cardinality(5).expect("cardinality");
    for (row, (value, partition)) in [
        (Value::Decimal(125, 9, 2), 1),
        (Value::Null(decimal.clone()), 1),
        (Value::Decimal(100, 9, 2), 1),
        (Value::Decimal(700, 9, 2), 2),
        (Value::Decimal(700, 9, 2), 2),
    ]
    .into_iter()
    .enumerate()
    {
        input.set_value(0, row, &value).unwrap();
        input.set_value(1, row, &Value::Integer(partition)).unwrap();
    }

    let output =
        build_window_output_chunks(&spec, &[input], test_allocator()).expect("window output");
    let result = output[0].column(2).unwrap();
    for row in 0..3 {
        assert_eq!(result.get_value(row), Value::Decimal(100, 9, 2));
    }
    for row in 3..5 {
        assert_eq!(result.get_value(row), Value::Decimal(700, 9, 2));
    }
}

#[test]
fn sorted_window_evaluates_ordered_aggregate_frames_with_bound_kernel() {
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("integer SUM binding");
    let aggregate = AggregateExpression::new(
        sum,
        vec![reference(0, LogicalType::Integer)],
        LogicalType::BigInt,
    );
    let expression = WindowExpression::aggregate(
        aggregate,
        Vec::new(),
        vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        WindowFrame::default(),
    );
    let output = build_window_output_chunks(
        &window_spec(vec![expression]),
        &[value_order_input_chunk(
            &[Value::Integer(10), Value::Integer(20), Value::Integer(30)],
            &[1, 2, 3],
        )],
        test_allocator(),
    )
    .expect("ordered aggregate window");
    let result = output[0].column(2).expect("sum output");
    assert_eq!(result.get_value(0), Value::BigInt(10));
    assert_eq!(result.get_value(1), Value::BigInt(30));
    assert_eq!(result.get_value(2), Value::BigInt(60));
}

#[test]
fn append_only_frames_match_independent_recomputation_and_update_each_row_once() {
    use super::{frame, WindowRowKey};
    use paro_common::vector::Vector;
    use paro_function::aggregate::{AggregateInputData, AggregateStateInput};
    thread_local! { static UPDATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
    thread_local! { static INPUT_BUFFERS: std::cell::RefCell<std::collections::BTreeSet<usize>> = const { std::cell::RefCell::new(std::collections::BTreeSet::new()) }; }
    thread_local! { static RESULT_BUFFERS: std::cell::RefCell<std::collections::BTreeSet<usize>> = const { std::cell::RefCell::new(std::collections::BTreeSet::new()) }; }
    unsafe fn count_updates(
        inputs: &[&Vector],
        data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        UPDATED.with(|value| value.set(value.get() + count));
        INPUT_BUFFERS.with(|buffers| {
            buffers.borrow_mut().insert(
                inputs[0]
                    .try_to_view(count)
                    .unwrap()
                    .get_data::<i32>()
                    .unwrap() as usize,
            );
        });
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        (sum.update)(inputs, data, states, count);
    }
    unsafe fn track_finalize_buffer(
        states: &Vector,
        data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) -> paro_common::error::Result<()> {
        RESULT_BUFFERS.with(|buffers| {
            buffers
                .borrow_mut()
                .insert(result.as_slice::<i64>().as_ptr() as usize);
        });
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        (sum.finalize)(states, data, result, count)
    }
    let (mut sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    sum.update = count_updates;
    sum.finalize = track_finalize_buffer;
    let expression = WindowExpression::aggregate(
        AggregateExpression::new(
            sum,
            vec![reference(0, LogicalType::Integer)],
            LogicalType::BigInt,
        ),
        vec![],
        vec![],
        WindowFrame::default(),
    );
    let values: Vec<_> = (0..VECTOR_SIZE)
        .map(|i| {
            if i % 13 == 0 {
                Value::Null(LogicalType::Integer)
            } else {
                Value::Integer(i as i32)
            }
        })
        .collect();
    let chunks = vec![
        value_order_input_chunk(&values, &vec![0; VECTOR_SIZE]),
        value_order_input_chunk(&[Value::Integer(7)], &[1]),
    ];
    let keys: Vec<_> = chunks
        .iter()
        .enumerate()
        .flat_map(|(chunk_idx, chunk)| {
            (0..chunk.size()).map(move |row_idx| WindowRowKey { chunk_idx, row_idx })
        })
        .collect();
    let ranges = vec![0..0, 0..1, 0..3, 0..3, 0..keys.len()];
    let expected: Vec<_> = ranges
        .iter()
        .map(|range| {
            frame::aggregate_window_value(
                &chunks,
                &keys,
                range.clone(),
                &expression,
                test_allocator(),
            )
            .unwrap()
        })
        .collect();
    UPDATED.with(|value| value.set(0));
    INPUT_BUFFERS.with(|buffers| buffers.borrow_mut().clear());
    RESULT_BUFFERS.with(|buffers| buffers.borrow_mut().clear());
    let mut actual = Vec::new();
    frame::visit_append_only_aggregate_frames(
        &chunks,
        &keys,
        ranges,
        &expression,
        test_allocator(),
        |_, value| {
            actual.push(value);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(UPDATED.with(|value| value.get()), keys.len());
    // Chunk's reset contract swaps its active and spare vectors. The number
    // of backing buffers is fixed at two, independent of the frame count.
    assert_eq!(INPUT_BUFFERS.with(|buffers| buffers.borrow().len()), 2);
    // Finalize reuses a uniquely owned result vector even through empty/null
    // prefixes; it does not alternate two Chunk reset buffers.
    assert_eq!(RESULT_BUFFERS.with(|buffers| buffers.borrow().len()), 1);
    assert!(frame::visit_append_only_aggregate_frames(
        &chunks,
        &keys,
        [0..3, 1..4],
        &expression,
        test_allocator(),
        |_, _| Ok(())
    )
    .is_err());
    assert!(frame::visit_append_only_aggregate_frames(
        &chunks,
        &keys,
        [0..3, 0..2],
        &expression,
        test_allocator(),
        |_, _| Ok(())
    )
    .is_err());
}

#[test]
fn append_only_frames_destroy_state_on_output_error() {
    use super::{frame, WindowRowKey};
    use paro_common::vector::Vector;
    use paro_function::aggregate::AggregateInputData;
    thread_local! { static DESTROYED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
    unsafe fn destroy(_: &Vector, _: &AggregateInputData, count: usize) {
        DESTROYED.with(|value| value.set(value.get() + count));
    }
    let (mut sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    sum.destructor = Some(destroy);
    let expression = WindowExpression::aggregate(
        AggregateExpression::new(
            sum,
            vec![reference(0, LogicalType::Integer)],
            LogicalType::BigInt,
        ),
        vec![],
        vec![],
        WindowFrame::default(),
    );
    DESTROYED.with(|value| value.set(0));
    let error = frame::visit_append_only_aggregate_frames(
        &[value_order_input_chunk(&[Value::Integer(1)], &[1])],
        &[WindowRowKey {
            chunk_idx: 0,
            row_idx: 0,
        }],
        std::iter::once(0..1),
        &expression,
        test_allocator(),
        |_, _| Err(paro_common::error::internal("output failed")),
    )
    .unwrap_err();
    assert!(error.to_string().contains("output failed"));
    assert_eq!(DESTROYED.with(|value| value.get()), 1);
}

#[test]
fn append_only_finalize_memory_failure_destroys_state_and_refunds_workspace() {
    use super::{frame, WindowRowKey};
    use crate::memory_runtime::QueryMemoryPool;
    use paro_common::allocator::{Allocator, MemoryTag};
    use paro_common::memory::{MemoryAccountingClass, MemoryDomain, MemoryOwnerAllocator};
    use paro_common::vector::Vector;
    use paro_function::aggregate::AggregateInputData;
    use std::sync::Arc;

    thread_local! { static DESTROYED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
    unsafe fn destroy(_: &Vector, _: &AggregateInputData, count: usize) {
        DESTROYED.with(|value| value.set(value.get() + count));
    }
    unsafe fn failing_finalize(
        _: &Vector,
        _: &AggregateInputData,
        result: &mut Vector,
        _: usize,
    ) -> paro_common::error::Result<()> {
        // Fail through the real result vector allocator after state/update and
        // result scratch have been initialized, not through a synthetic Err.
        result.try_reset_for_execution(1_000_000, result.allocator().clone())
    }
    let (mut sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    sum.finalize = failing_finalize;
    sum.destructor = Some(destroy);
    let expression = WindowExpression::aggregate(
        AggregateExpression::new(
            sum,
            vec![reference(0, LogicalType::Integer)],
            LogicalType::BigInt,
        ),
        vec![],
        vec![],
        WindowFrame::default(),
    );
    let pool = Arc::new(QueryMemoryPool::new(64 * 1024));
    let allocator: Arc<dyn Allocator> = Arc::new(MemoryOwnerAllocator::new(
        test_allocator(),
        pool.clone(),
        MemoryDomain::Host,
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    ));
    DESTROYED.with(|value| value.set(0));
    let error = frame::visit_append_only_aggregate_frames(
        &[value_order_input_chunk(&[Value::Integer(1)], &[1])],
        &[WindowRowKey {
            chunk_idx: 0,
            row_idx: 0,
        }],
        std::iter::once(0..1),
        &expression,
        allocator,
        |_, _| panic!("failed finalization must not emit a result"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(DESTROYED.with(|value| value.get()), 1);
    assert_eq!(pool.issued_bytes(), 0);
    assert_eq!(pool.non_revocable_bytes(), 0);
}

#[test]
fn append_only_workspace_owns_varlen_values_across_resets() {
    use super::{frame, WindowRowKey};
    let ty = LogicalType::Varchar;
    let (function, _) = get_min_function().bind(std::slice::from_ref(&ty)).unwrap();
    let expression = WindowExpression::aggregate(
        AggregateExpression::new(function, vec![reference(0, ty.clone())], ty.clone()),
        vec![],
        vec![],
        WindowFrame::default(),
    );
    let values = [
        Value::Null(ty.clone()),
        Value::Varchar("z".repeat(128)),
        Value::Varchar("a".repeat(256)),
        Value::Null(ty.clone()),
    ];
    let mut chunk = Chunk::try_initialize(&[ty], values.len(), test_allocator()).unwrap();
    chunk.try_set_cardinality(values.len()).unwrap();
    for (row, value) in values.iter().enumerate() {
        chunk.set_value(0, row, value).unwrap();
    }
    let keys = (0..values.len())
        .map(|row_idx| WindowRowKey {
            chunk_idx: 0,
            row_idx,
        })
        .collect::<Vec<_>>();
    let mut actual = Vec::new();
    frame::visit_append_only_aggregate_frames(
        &[chunk],
        &keys,
        (1..=values.len()).map(|end| 0..end),
        &expression,
        test_allocator(),
        |_, value| {
            actual.push(value);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        actual,
        vec![
            values[0].clone(),
            values[1].clone(),
            values[2].clone(),
            values[2].clone()
        ]
    );
}

#[test]
fn append_only_filtered_range_peers_preserve_empty_and_non_null_prefixes() {
    let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    let aggregate = AggregateExpression::new(
        sum,
        vec![reference(0, LogicalType::Integer)],
        LogicalType::BigInt,
    )
    .with_filter(Some(reference(2, LogicalType::Boolean)));
    let expression = WindowExpression::aggregate(
        aggregate,
        vec![],
        vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        WindowFrame::default(),
    );
    let types = vec![
        LogicalType::Integer,
        LogicalType::Integer,
        LogicalType::Boolean,
    ];
    let mut input = Chunk::try_initialize(&types, 5, test_allocator()).unwrap();
    input.try_set_cardinality(5).unwrap();
    for (row, (order, filter)) in [
        (1, Value::Boolean(false)),
        (2, Value::Null(LogicalType::Boolean)),
        (3, Value::Boolean(true)),
        (3, Value::Boolean(true)),
        (4, Value::Boolean(false)),
    ]
    .into_iter()
    .enumerate()
    {
        input.set_value(0, row, &Value::Integer(10)).unwrap();
        input.set_value(1, row, &Value::Integer(order)).unwrap();
        input.set_value(2, row, &filter).unwrap();
    }
    let output = build_window_output_chunks(
        &window_spec_for_types(vec![expression], types),
        &[input],
        test_allocator(),
    )
    .unwrap();
    let result = output[0].column(3).unwrap();
    assert_eq!(result.get_value(0), Value::Null(LogicalType::BigInt));
    assert_eq!(result.get_value(1), Value::Null(LogicalType::BigInt));
    for row in 2..5 {
        assert_eq!(result.get_value(row), Value::BigInt(20));
    }
}

#[test]
fn sorted_window_fallback_applies_aggregate_filter_three_valued_logic() {
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("integer SUM binding");
    let aggregate = AggregateExpression::new(
        sum,
        vec![reference(0, LogicalType::Integer)],
        LogicalType::BigInt,
    )
    // Binder output is a ColumnRef until column binding resolution. The
    // generic window fallback must not mistake it for an aggregate-payload
    // Reference because FILTER is applied directly to the sorted row domain.
    .with_filter(Some(column_ref(2, LogicalType::Boolean)));
    let expression = WindowExpression::aggregate(
        aggregate,
        vec![reference(1, LogicalType::Integer)],
        Vec::new(),
        WindowFrame::default(),
    );
    let mut input = Chunk::try_initialize(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Boolean,
        ],
        4,
        test_allocator(),
    )
    .expect("input chunk");
    input.try_set_cardinality(4).expect("cardinality");
    for (row, (value, filter)) in [
        (10, Value::Boolean(true)),
        (20, Value::Boolean(false)),
        (30, Value::Null(LogicalType::Boolean)),
        (40, Value::Boolean(true)),
    ]
    .into_iter()
    .enumerate()
    {
        input.set_value(0, row, &Value::Integer(value)).unwrap();
        input.set_value(1, row, &Value::Integer(1)).unwrap();
        input.set_value(2, row, &filter).unwrap();
    }

    let spec = window_spec_for_types(
        vec![expression],
        vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Boolean,
        ],
    );
    let output = build_window_output_chunks(&spec, &[input], test_allocator())
        .expect("filtered aggregate window");
    let result = output[0].column(3).expect("sum output");
    for row in 0..4 {
        assert_eq!(result.get_value(row), Value::BigInt(50));
    }
}

#[test]
fn window_breaker_writes_rank_across_output_chunks_directly() {
    let spec = window_spec(vec![rank_over(0, 1)]);
    let output = build_window_output_chunks(
        &spec,
        &[
            rank_input_chunk(0, VECTOR_SIZE),
            rank_input_chunk(VECTOR_SIZE as i32, 2),
        ],
        test_allocator(),
    )
    .expect("window output");

    assert_eq!(output.len(), 2);
    assert_eq!(output[0].column(2).unwrap().get_value(0), Value::BigInt(1));
    assert_eq!(
        output[0].column(2).unwrap().get_value(VECTOR_SIZE - 1),
        Value::BigInt(VECTOR_SIZE as i64)
    );
    assert_eq!(
        output[1].column(2).unwrap().get_value(0),
        Value::BigInt(VECTOR_SIZE as i64 + 1)
    );
    assert_eq!(
        output[1].column(2).unwrap().get_value(1),
        Value::BigInt(VECTOR_SIZE as i64 + 2)
    );
}

#[test]
fn window_breaker_rejects_non_direct_sort_expressions() {
    let spec = window_spec(vec![WindowExpression::native(
        WindowFunction::rank(),
        Vec::new(),
        vec![int_constant(1)],
        vec![OrderByExpression {
            expression: Expression::Window(rank_over(0, 1).into()),
            ascending: true,
            nulls_first: false,
        }],
        WindowFrame::default(),
        false,
    )]);

    let err = build_window_output_chunks(&spec, &[], test_allocator()).unwrap_err();
    assert!(err
        .to_string()
        .contains("window order currently supports direct references"));
}

#[test]
fn window_breaker_rejects_non_direct_frame_offsets() {
    let expression = value_window(
        WindowFunction::last_value(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer)],
        rows_frame(
            WindowFrameBound::Offset(Box::new(Expression::Window(rank_over(0, 1).into()))),
            true,
            WindowFrameBound::CurrentRow,
            false,
        ),
    );

    let err = build_window_output_chunks(&window_spec(vec![expression]), &[], test_allocator())
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("window frame start currently supports direct references"));
}

#[test]
fn frame_value_functions_read_from_each_current_frame() {
    let expressions = vec![
        value_window(
            WindowFunction::last_value(LogicalType::Integer),
            vec![reference(1, LogicalType::Integer)],
            WindowFrame::default(),
        ),
        value_window(
            WindowFunction::first_value(LogicalType::Integer),
            vec![reference(1, LogicalType::Integer)],
            rows_frame(
                WindowFrameBound::Offset(Box::new(int_constant(1))),
                false,
                WindowFrameBound::Offset(Box::new(int_constant(2))),
                false,
            ),
        ),
        value_window(
            WindowFunction::last_value(LogicalType::Integer),
            vec![reference(1, LogicalType::Integer)],
            rows_frame(
                WindowFrameBound::Offset(Box::new(int_constant(1))),
                true,
                WindowFrameBound::Offset(Box::new(int_constant(1))),
                true,
            ),
        ),
        value_window(
            WindowFunction::nth_value(LogicalType::Integer),
            vec![reference(1, LogicalType::Integer), bigint_constant(2)],
            rows_frame(
                WindowFrameBound::CurrentRow,
                false,
                WindowFrameBound::Offset(Box::new(int_constant(2))),
                false,
            ),
        ),
    ];
    let output = build_window_output_chunks(
        &window_spec(expressions),
        &[rank_input_chunk(1, 4)],
        test_allocator(),
    )
    .expect("window output");
    let chunk = &output[0];
    let null = Value::Null(LogicalType::Integer);

    let expected = [
        [
            Value::Integer(1),
            Value::Integer(2),
            null.clone(),
            Value::Integer(2),
        ],
        [
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(1),
            Value::Integer(3),
        ],
        [
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(2),
            Value::Integer(4),
        ],
        [Value::Integer(4), null.clone(), Value::Integer(3), null],
    ];
    for (row, expected_values) in expected.iter().enumerate() {
        for (expr_idx, expected_value) in expected_values.iter().enumerate() {
            assert_eq!(
                chunk.column(2 + expr_idx).unwrap().get_value(row),
                *expected_value,
                "row {row}, expression {expr_idx}"
            );
        }
    }
}

#[test]
fn frame_value_functions_apply_ignore_nulls_inside_the_frame() {
    let mut first_ignore = value_window(
        WindowFunction::first_value(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer)],
        whole_partition_rows_frame(),
    );
    first_ignore.ignore_nulls = true;
    let mut last_ignore = value_window(
        WindowFunction::last_value(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer)],
        whole_partition_rows_frame(),
    );
    last_ignore.ignore_nulls = true;
    let mut nth_ignore = value_window(
        WindowFunction::nth_value(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer), bigint_constant(2)],
        whole_partition_rows_frame(),
    );
    nth_ignore.ignore_nulls = true;

    let output = build_window_output_chunks(
        &window_spec(vec![first_ignore, last_ignore, nth_ignore]),
        &[value_order_input_chunk(
            &[
                Value::Null(LogicalType::Integer),
                Value::Integer(10),
                Value::Integer(20),
                Value::Null(LogicalType::Integer),
            ],
            &[1, 2, 3, 4],
        )],
        test_allocator(),
    )
    .expect("window output");
    let chunk = &output[0];

    for row in 0..4 {
        assert_eq!(chunk.column(2).unwrap().get_value(row), Value::Integer(10));
        assert_eq!(chunk.column(3).unwrap().get_value(row), Value::Integer(20));
        assert_eq!(chunk.column(4).unwrap().get_value(row), Value::Integer(20));
    }
}

#[test]
fn ntile_assigns_remainder_rows_to_leading_buckets() {
    let output = build_window_output_chunks(
        &window_spec(vec![ntile_window(bigint_constant(4))]),
        &[rank_input_chunk(1, 10)],
        test_allocator(),
    )
    .expect("window output");
    let vector = output[0].column(2).unwrap();
    let expected = [1, 1, 1, 2, 2, 2, 3, 3, 4, 4];

    for (row, bucket) in expected.into_iter().enumerate() {
        assert_eq!(vector.get_value(row), Value::BigInt(bucket));
    }
}

#[test]
fn ntile_handles_more_buckets_than_rows_and_null_counts() {
    let output = build_window_output_chunks(
        &window_spec(vec![
            ntile_window(bigint_constant(6)),
            ntile_window(null_bigint_constant()),
        ]),
        &[rank_input_chunk(1, 4)],
        test_allocator(),
    )
    .expect("window output");

    for row in 0..4 {
        assert_eq!(
            output[0].column(2).unwrap().get_value(row),
            Value::BigInt(row as i64 + 1)
        );
        assert_eq!(
            output[0].column(3).unwrap().get_value(row),
            Value::Null(LogicalType::BigInt)
        );
    }
}

#[test]
fn ntile_rejects_non_positive_bucket_counts() {
    for count in [-1, 0] {
        let error = build_window_output_chunks(
            &window_spec(vec![ntile_window(bigint_constant(i64::from(count)))]),
            &[rank_input_chunk(1, 1)],
            test_allocator(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("argument of ntile must be greater than zero"),
            "{error}"
        );
    }
}

#[test]
fn lead_evaluates_offsets_for_each_current_row() {
    let expression = value_window(
        WindowFunction::lead_with_offset(LogicalType::Integer),
        vec![
            reference(1, LogicalType::Integer),
            reference(0, LogicalType::BigInt),
        ],
        whole_partition_rows_frame(),
    );
    let output = build_window_output_chunks(
        &window_spec_for_types(
            vec![expression],
            vec![LogicalType::BigInt, LogicalType::Integer],
        ),
        &[offset_value_input_chunk(
            &[
                Value::BigInt(1),
                Value::BigInt(2),
                Value::BigInt(0),
                Value::Null(LogicalType::BigInt),
            ],
            &[10, 20, 30, 40],
        )],
        test_allocator(),
    )
    .expect("window output");
    let vector = output[0].column(2).unwrap();

    assert_eq!(vector.get_value(0), Value::Integer(20));
    assert_eq!(vector.get_value(1), Value::Integer(40));
    assert_eq!(vector.get_value(2), Value::Integer(30));
    assert_eq!(vector.get_value(3), Value::Null(LogicalType::Integer));
}

#[test]
fn lead_and_lag_apply_ignore_nulls_while_navigating_the_partition() {
    let mut lead = value_window(
        WindowFunction::lead_with_default(LogicalType::Integer),
        vec![
            reference(0, LogicalType::Integer),
            bigint_constant(1),
            int_constant(99),
        ],
        whole_partition_rows_frame(),
    );
    lead.ignore_nulls = true;
    let mut lag = value_window(
        WindowFunction::lag_with_default(LogicalType::Integer),
        vec![
            reference(0, LogicalType::Integer),
            bigint_constant(1),
            int_constant(99),
        ],
        whole_partition_rows_frame(),
    );
    lag.ignore_nulls = true;
    let mut zero_offset = value_window(
        WindowFunction::lead_with_offset(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer), bigint_constant(0)],
        whole_partition_rows_frame(),
    );
    zero_offset.ignore_nulls = true;

    let input_values = [
        Value::Null(LogicalType::Integer),
        Value::Integer(10),
        Value::Null(LogicalType::Integer),
        Value::Integer(20),
    ];
    let output = build_window_output_chunks(
        &window_spec(vec![lead, lag, zero_offset]),
        &[value_order_input_chunk(&input_values, &[1, 2, 3, 4])],
        test_allocator(),
    )
    .expect("window output");
    let expected_lead = [10, 20, 20, 99];
    let expected_lag = [99, 99, 10, 10];

    for row in 0..4 {
        assert_eq!(
            output[0].column(2).unwrap().get_value(row),
            Value::Integer(expected_lead[row])
        );
        assert_eq!(
            output[0].column(3).unwrap().get_value(row),
            Value::Integer(expected_lag[row])
        );
        assert_eq!(
            output[0].column(4).unwrap().get_value(row),
            input_values[row]
        );
    }
}

#[test]
fn rows_frame_offsets_reject_null_and_negative_values() {
    for (offset, expected) in [
        (
            Expression::Constant(
                ConstantExpression::new(Value::Null(LogicalType::Integer), LogicalType::Integer)
                    .into(),
            ),
            "window frame offset must not be null",
        ),
        (int_constant(-1), "window frame offset must not be negative"),
    ] {
        let expression = value_window(
            WindowFunction::last_value(LogicalType::Integer),
            vec![reference(1, LogicalType::Integer)],
            rows_frame(
                WindowFrameBound::Offset(Box::new(offset)),
                true,
                WindowFrameBound::CurrentRow,
                false,
            ),
        );
        let error = build_window_output_chunks(
            &window_spec(vec![expression]),
            &[rank_input_chunk(1, 1)],
            test_allocator(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn borrowed_key_comparison_preserves_value_order_and_null_rules() {
    use paro_common::vector::{SelectionVector, Vector};
    use std::cmp::Ordering;
    use std::sync::Arc;
    for (ty, values) in [
        (
            LogicalType::Varchar,
            vec![
                Value::Varchar("long unicode partition ä".into()),
                Value::Varchar("z".into()),
                Value::Null(LogicalType::Varchar),
            ],
        ),
        (
            LogicalType::Double,
            vec![
                Value::Double(-0.0),
                Value::Double(0.0),
                Value::Double(f64::NAN),
                Value::Double(1.0),
                Value::Null(LogicalType::Double),
            ],
        ),
        (
            LogicalType::Decimal {
                precision: 38,
                scale: 2,
            },
            vec![
                Value::Decimal(-123, 38, 2),
                Value::Decimal(456, 38, 2),
                Value::Null(LogicalType::Decimal {
                    precision: 38,
                    scale: 2,
                }),
            ],
        ),
        (
            LogicalType::Decimal {
                precision: 18,
                scale: 2,
            },
            vec![
                Value::Decimal(-123, 18, 2),
                Value::Decimal(456, 18, 2),
                Value::Null(LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                }),
            ],
        ),
    ] {
        let mut vector = Vector::try_new(ty.clone(), values.len(), test_allocator()).unwrap();
        for (row, value) in values.iter().enumerate() {
            vector.set_value(row, value);
        }
        vector.try_set_count(values.len()).unwrap();
        let mut selection =
            SelectionVector::try_with_capacity(values.len(), test_allocator()).unwrap();
        selection.set_len(values.len());
        for row in 0..values.len() {
            selection.try_set(row, values.len() - 1 - row).unwrap();
        }
        let dict = Vector::try_gather_ref(Arc::new(vector), selection).unwrap();
        for ascending in [true, false] {
            for nulls_first in [true, false] {
                for i in 0..values.len() {
                    for j in 0..values.len() {
                        let a = dict.get_value(i);
                        let b = dict.get_value(j);
                        let expected = match (a.is_null(), b.is_null()) {
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
                            _ => {
                                let cmp = a.partial_cmp(&b).unwrap_or(Ordering::Equal);
                                if ascending {
                                    cmp
                                } else {
                                    cmp.reverse()
                                }
                            }
                        };
                        assert_eq!(
                            super::compare_vector_cells(&dict, i, &dict, j, ascending, nulls_first),
                            expected,
                            "type={ty:?} i={i} j={j} ascending={ascending} nulls_first={nulls_first}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn encoded_window_sort_matches_stable_comparison_for_dictionary_keys_and_peers() {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::vector::SelectionVector;
    use paro_context::StatementCancellation;

    let decimal = LogicalType::Decimal {
        precision: 38,
        scale: 2,
    };
    let types = [LogicalType::Varchar, decimal.clone()];
    let mut chunks = Vec::new();
    for batch in 0..2 {
        let mut chunk = Chunk::try_initialize(&types, VECTOR_SIZE, test_allocator()).unwrap();
        chunk.try_set_cardinality(VECTOR_SIZE).unwrap();
        for row in 0..VECTOR_SIZE {
            let i = batch * VECTOR_SIZE + row;
            let label = match i % 7 {
                0 => Value::Null(LogicalType::Varchar),
                1 => Value::Varchar("".into()),
                2 => Value::Varchar("a".into()),
                3 => Value::Varchar("a\0".into()),
                4 => Value::Varchar("a\0b".into()),
                5 => Value::Varchar("中ä-long-prefix".into()),
                _ => Value::Varchar("中ä-long-prefix-more".into()),
            };
            chunk.set_value(0, row, &label).unwrap();
            chunk
                .set_value(
                    1,
                    row,
                    &if i % 11 == 0 {
                        Value::Null(decimal.clone())
                    } else {
                        Value::Decimal((i % 19) as i128 - 9, 38, 2)
                    },
                )
                .unwrap();
        }
        let mut selection =
            SelectionVector::try_with_capacity(VECTOR_SIZE, test_allocator()).unwrap();
        selection.set_len(VECTOR_SIZE);
        for row in 0..VECTOR_SIZE {
            selection.try_set(row, VECTOR_SIZE - 1 - row).unwrap();
        }
        chunk.try_slice(&selection, VECTOR_SIZE).unwrap();
        chunks.push(chunk);
    }
    let memory = MemoryAccountingContext::detached(
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    );
    let cancel = StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None);
    for ascending in [true, false] {
        for nulls_first in [true, false] {
            let expr = WindowExpression::native(
                WindowFunction::rank(),
                vec![],
                vec![int_constant(42), reference(0, LogicalType::Varchar)],
                vec![
                    OrderByExpression {
                        expression: int_constant(17),
                        ascending: false,
                        nulls_first: true,
                    },
                    OrderByExpression {
                        expression: reference(1, decimal.clone()),
                        ascending,
                        nulls_first,
                    },
                ],
                WindowFrame::default(),
                false,
            );
            let mut expected = super::build_row_keys(&chunks);
            expected.sort_by(|a, b| super::compare_window_order(&chunks, a, b, &expr));
            let mut actual = super::window_metadata(&memory).unwrap();
            actual.try_extend(super::build_row_keys(&chunks)).unwrap();
            assert!(super::sort::try_sort_encoded(&chunks, &mut actual, &expr, &cancel).unwrap());
            assert_eq!(actual.as_slice(), expected);
        }
    }
}

#[test]
fn encoded_window_sort_preserves_complete_grouped_partitions_and_matches_work_oracle() {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::vector::SelectionVector;
    use paro_context::StatementCancellation;

    let types = [
        LogicalType::Varchar,
        LogicalType::Integer,
        LogicalType::Varchar,
        LogicalType::Integer,
    ];
    let groups = [
        Value::Varchar("".into()),
        Value::Varchar("a\0".into()),
        Value::Varchar("中ä-long-prefix".into()),
        Value::Null(LogicalType::Varchar),
    ];
    let group_size = VECTOR_SIZE / 3 + 3;
    let count = groups.len() * 2 * group_size;
    let memory = MemoryAccountingContext::detached(
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    );
    let cancel = StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None);
    for grouped in [true, false] {
        let mut chunks = Vec::new();
        for offset in (0..count).step_by(VECTOR_SIZE) {
            let size = (count - offset).min(VECTOR_SIZE);
            let mut chunk = Chunk::try_initialize(&types, size, test_allocator()).unwrap();
            chunk.try_set_cardinality(size).unwrap();
            // Store backwards physically, then expose a dictionary in logical
            // order. Complete partitions straddle input chunk boundaries.
            for physical in 0..size {
                let row = offset + size - 1 - physical;
                let group = row / group_size;
                let group = if grouped { group } else { 7 - group };
                chunk.set_value(0, physical, &groups[group / 2]).unwrap();
                chunk
                    .set_value(1, physical, &Value::Integer((group % 2) as i32))
                    .unwrap();
                let order = match row % 6 {
                    0 => Value::Null(LogicalType::Varchar),
                    1 => Value::Varchar("".into()),
                    2 => Value::Varchar("a\0".into()),
                    3 => Value::Varchar("中ä-long-prefix".into()),
                    4 => Value::Varchar("中ä-long-prefix-more".into()),
                    _ => Value::Varchar("peer".into()),
                };
                chunk.set_value(2, physical, &order).unwrap();
                chunk
                    .set_value(3, physical, &Value::Integer(row as i32))
                    .unwrap();
            }
            let mut selection = SelectionVector::try_with_capacity(size, test_allocator()).unwrap();
            selection.set_len(size);
            for row in 0..size {
                selection.try_set(row, size - 1 - row).unwrap();
            }
            chunk.try_slice(&selection, size).unwrap();
            chunks.push(chunk);
        }
        for ascending in [true, false] {
            for nulls_first in [true, false] {
                let expression = WindowExpression::native(
                    WindowFunction::rank(),
                    vec![],
                    vec![
                        int_constant(42),
                        reference(0, LogicalType::Varchar),
                        int_constant(17),
                        reference(1, LogicalType::Integer),
                    ],
                    vec![
                        OrderByExpression {
                            expression: int_constant(17),
                            ascending: false,
                            nulls_first: true,
                        },
                        OrderByExpression {
                            expression: reference(2, LogicalType::Varchar),
                            ascending,
                            nulls_first,
                        },
                    ],
                    WindowFrame::default(),
                    false,
                );
                let mut expected_keys = super::build_row_keys(&chunks);
                expected_keys
                    .sort_by(|a, b| super::compare_window_order(&chunks, a, b, &expression));
                let row_number = WindowExpression::native(
                    WindowFunction::row_number(),
                    vec![],
                    expression.partitions.clone(),
                    expression.orders.clone(),
                    expression.frame.clone(),
                    false,
                );
                let spec = window_spec_for_types(vec![expression, row_number], types.to_vec());
                let expected =
                    build_window_output_chunks(&spec, &chunks, test_allocator()).unwrap();
                let mut keys = super::window_metadata(&memory).unwrap();
                keys.try_extend(super::build_row_keys(&chunks)).unwrap();
                let actual =
                    super::evaluate_window_work(&spec, &chunks, keys, test_allocator(), &cancel)
                        .unwrap();
                assert_eq!(actual.keys.as_slice(), expected_keys);
                assert_eq!(actual.partitions.len(), groups.len() * 2);
                assert_eq!(actual.chunks.len(), expected.len());
                for (actual, expected) in actual.chunks.iter().zip(&expected) {
                    assert_eq!(actual.size(), expected.size());
                    for column in 0..spec.output_types.len() {
                        for row in 0..actual.size() {
                            assert_eq!(
                                actual.column(column).unwrap().get_value(row),
                                expected.column(column).unwrap().get_value(row),
                                "grouped={grouped} ascending={ascending} nulls_first={nulls_first} column={column} row={row}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn encoded_window_sort_retains_float_fallback_and_cancellation() {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_context::StatementCancellation;

    let chunks = vec![rank_input_chunk(0, VECTOR_SIZE)];
    let memory = MemoryAccountingContext::detached(
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    );
    let mut keys = super::window_metadata(&memory).unwrap();
    keys.try_extend(super::build_row_keys(&chunks)).unwrap();
    let token = tokio_util::sync::CancellationToken::new();
    let cancel = StatementCancellation::new(token.clone(), None);
    let mut expr = rank_over(0, 1);
    expr.orders[0].expression = reference(1, LogicalType::Double);
    assert!(!super::sort::try_sort_encoded(&chunks, &mut keys, &expr, &cancel).unwrap());
    let original = keys.to_vec();
    token.cancel();
    assert!(super::sort::try_sort_encoded(&chunks, &mut keys, &rank_over(0, 1), &cancel).is_err());
    assert_eq!(keys.as_slice(), original);
}

#[test]
fn encoded_window_sort_memory_failure_falls_back_and_refunds_temporary_keys() {
    use crate::memory_runtime::QueryMemoryPool;
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
    use paro_context::StatementCancellation;
    use std::sync::Arc;

    // Input keys and descriptors fit; the encoded arena exceeds the remaining
    // capacity. Exercise cleanup after a successful scratch allocation.
    let pool = Arc::new(QueryMemoryPool::new(VECTOR_SIZE * 64));
    let memory = MemoryAccountingContext::from_owner(
        pool.clone(),
        MemoryDomain::Host,
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    );
    let input = vec![rank_input_chunk(0, VECTOR_SIZE)];
    let mut keys = super::window_metadata(&memory).unwrap();
    keys.try_extend(super::build_row_keys(&input)).unwrap();
    let cancel = StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None);
    let original = keys.to_vec();
    let before = pool.issued_bytes();
    let before_used = pool.non_revocable_bytes();
    assert!(!super::sort::try_sort_encoded(&input, &mut keys, &rank_over(0, 1), &cancel).unwrap());
    assert_eq!(keys.as_slice(), original);
    assert_eq!(pool.issued_bytes(), before);
    assert_eq!(pool.non_revocable_bytes(), before_used);
    let result = super::evaluate_window_work(
        &window_spec(vec![rank_over(0, 1)]),
        &input,
        keys,
        test_allocator(),
        &cancel,
    )
    .expect("comparison fallback completes with the same memory ceiling");
    assert_eq!(result.keys.as_slice(), original);
    assert_eq!(
        result.chunks[0].column(2).unwrap().get_value(0),
        Value::BigInt(1)
    );
    assert_eq!(
        result.chunks[0]
            .column(2)
            .unwrap()
            .get_value(VECTOR_SIZE - 1),
        Value::BigInt(VECTOR_SIZE as i64)
    );
    drop(result);
    assert_eq!(pool.issued_bytes(), 0);
    assert_eq!(pool.non_revocable_bytes(), 0);
}
