// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::window::{WindowFunction, WindowFunctionType};
use paro_planner::expression::{
    ConstantExpression, Expression, OrderByExpression, ReferenceExpression, WindowExpression,
    WindowFrame, WindowFrameBound, WindowFrameType,
};

use super::build_window_output_chunks;
use crate::physical::specs::WindowSpec;

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn int_constant(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn null_int_constant() -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Null(LogicalType::Integer),
        LogicalType::Integer,
    ))
}

fn rank_over(partition_idx: usize, order_idx: usize) -> WindowExpression {
    WindowExpression {
        function: WindowFunction::rank(),
        children: Vec::new(),
        partitions: vec![reference(partition_idx, LogicalType::Integer)],
        orders: vec![OrderByExpression {
            expression: reference(order_idx, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        frame: WindowFrame::default(),
        ignore_nulls: false,
        return_type: LogicalType::BigInt,
    }
}

fn window_spec(expressions: Vec<WindowExpression>) -> WindowSpec {
    let mut output_types = vec![LogicalType::Integer, LogicalType::Integer];
    output_types.extend(expressions.iter().map(WindowExpression::return_type));
    WindowSpec {
        window_index: 1,
        expressions: expressions.into_boxed_slice(),
        input_width: 2,
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

fn value_window(
    function: WindowFunction,
    children: Vec<Expression>,
    frame: WindowFrame,
) -> WindowExpression {
    WindowExpression {
        function,
        children,
        partitions: Vec::new(),
        orders: vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        frame,
        ignore_nulls: false,
        return_type: LogicalType::Integer,
    }
}

fn ntile_window(bucket_count: Expression) -> WindowExpression {
    WindowExpression {
        function: WindowFunction::ntile(),
        children: vec![bucket_count],
        partitions: Vec::new(),
        orders: vec![OrderByExpression {
            expression: reference(1, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }],
        frame: WindowFrame::get_default_frame(&WindowFunction::ntile()),
        ignore_nulls: false,
        return_type: LogicalType::BigInt,
    }
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
    let spec = window_spec(vec![WindowExpression {
        function: WindowFunction::new(
            "sum",
            WindowFunctionType::Aggregate,
            vec![LogicalType::Integer],
            LogicalType::Integer,
        ),
        children: vec![reference(0, LogicalType::Integer)],
        partitions: vec![reference(1, LogicalType::Integer)],
        orders: Vec::new(),
        frame: WindowFrame::default(),
        ignore_nulls: false,
        return_type: LogicalType::Integer,
    }]);
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
    assert_eq!(chunk.column(2).unwrap().get_value(0), Value::Integer(30));
    assert_eq!(chunk.column(2).unwrap().get_value(1), Value::Integer(30));
    assert_eq!(chunk.column(2).unwrap().get_value(2), Value::Integer(7));
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
    let spec = window_spec(vec![WindowExpression {
        function: WindowFunction::rank(),
        children: Vec::new(),
        partitions: vec![int_constant(1)],
        orders: vec![OrderByExpression {
            expression: Expression::Window(rank_over(0, 1)),
            ascending: true,
            nulls_first: false,
        }],
        frame: WindowFrame::default(),
        ignore_nulls: false,
        return_type: LogicalType::BigInt,
    }]);

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
            vec![reference(1, LogicalType::Integer), int_constant(2)],
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
        vec![reference(0, LogicalType::Integer), int_constant(2)],
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
        &window_spec(vec![ntile_window(int_constant(4))]),
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
            ntile_window(int_constant(6)),
            ntile_window(null_int_constant()),
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
            &window_spec(vec![ntile_window(int_constant(count))]),
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
            reference(0, LogicalType::Integer),
        ],
        whole_partition_rows_frame(),
    );
    let output = build_window_output_chunks(
        &window_spec(vec![expression]),
        &[value_order_input_chunk(
            &[
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(0),
                Value::Null(LogicalType::Integer),
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
            int_constant(1),
            int_constant(99),
        ],
        whole_partition_rows_frame(),
    );
    lead.ignore_nulls = true;
    let mut lag = value_window(
        WindowFunction::lag_with_default(LogicalType::Integer),
        vec![
            reference(0, LogicalType::Integer),
            int_constant(1),
            int_constant(99),
        ],
        whole_partition_rows_frame(),
    );
    lag.ignore_nulls = true;
    let mut zero_offset = value_window(
        WindowFunction::lead_with_offset(LogicalType::Integer),
        vec![reference(0, LogicalType::Integer), int_constant(0)],
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
        (null_int_constant(), "window frame offset must not be null"),
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
fn aggregate_rows_frame_excludes_rows_before_the_partition() {
    let sum = WindowFunction::new(
        "sum",
        WindowFunctionType::Aggregate,
        vec![LogicalType::Integer],
        LogicalType::Integer,
    );
    let expression = value_window(
        sum,
        vec![reference(1, LogicalType::Integer)],
        rows_frame(
            WindowFrameBound::Offset(Box::new(int_constant(1))),
            true,
            WindowFrameBound::Offset(Box::new(int_constant(1))),
            true,
        ),
    );
    let output = build_window_output_chunks(
        &window_spec(vec![expression]),
        &[rank_input_chunk(1, 4)],
        test_allocator(),
    )
    .expect("window output");
    let vector = output[0].column(2).unwrap();

    assert_eq!(vector.get_value(0), Value::Null(LogicalType::Integer));
    assert_eq!(vector.get_value(1), Value::Integer(1));
    assert_eq!(vector.get_value(2), Value::Integer(2));
    assert_eq!(vector.get_value(3), Value::Integer(3));
}

#[test]
fn aggregate_range_frame_includes_the_current_peer_group() {
    let sum = WindowFunction::new(
        "sum",
        WindowFunctionType::Aggregate,
        vec![LogicalType::Integer],
        LogicalType::Integer,
    );
    let expression = value_window(
        sum,
        vec![reference(1, LogicalType::Integer)],
        WindowFrame::default(),
    );
    let output = build_window_output_chunks(
        &window_spec(vec![expression]),
        &[value_order_input_chunk(
            &[
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
            ],
            &[1, 1, 2, 3],
        )],
        test_allocator(),
    )
    .expect("window output");
    let vector = output[0].column(2).unwrap();

    assert_eq!(vector.get_value(0), Value::Integer(2));
    assert_eq!(vector.get_value(1), Value::Integer(2));
    assert_eq!(vector.get_value(2), Value::Integer(4));
    assert_eq!(vector.get_value(3), Value::Integer(7));
}
