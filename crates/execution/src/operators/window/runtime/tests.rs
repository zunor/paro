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
use paro_planner::operator::ColumnBinding;

use super::build_window_output_chunks;
use crate::physical::specs::WindowSpec;

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn column_ref(index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(7, index), ty))
}

fn int_constant(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn bigint_constant(value: i64) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::BigInt(value),
        LogicalType::BigInt,
    ))
}

fn null_bigint_constant() -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Null(LogicalType::BigInt),
        LogicalType::BigInt,
    ))
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
fn sorted_window_fallback_recomputes_ordered_aggregate_frames_with_bound_kernel() {
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
            expression: Expression::Window(rank_over(0, 1)),
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
            WindowFrameBound::Offset(Box::new(Expression::Window(rank_over(0, 1)))),
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
            Expression::Constant(ConstantExpression::new(
                Value::Null(LogicalType::Integer),
                LogicalType::Integer,
            )),
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
