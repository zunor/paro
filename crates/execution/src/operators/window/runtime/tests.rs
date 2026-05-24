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
    WindowFrame,
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
