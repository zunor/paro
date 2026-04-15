// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_execution::execution_context::ExecutionContext;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::thread_context::ThreadContext;
use paro_function::scalar::system::{get_array_length_functions, get_array_to_string_functions};
use paro_function::scalar::vector::get_l2_distance_functions;
use paro_function::scalar::{BoundScalarFunction, ScalarBindInput, ScalarFunctionSet};
use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};

const ROWS: usize = 2048;
const INT_WIDTH: usize = 8;
const FLOAT_WIDTH: usize = 16;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    array_length_input: Chunk,
    array_to_string_input: Chunk,
    l2_distance_input: Chunk,
    array_length_expr: Expression,
    array_to_string_expr: Expression,
    l2_distance_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);

        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), INT_WIDTH);
        let float_array_type = LogicalType::Array(Box::new(LogicalType::Float), FLOAT_WIDTH);
        let delimiter_values = vec![","; ROWS];
        let dim_values = vec![1_i32; ROWS];

        BenchState {
            runtime,
            array_length_input: Chunk::from_vectors(vec![
                int_array_vector(),
                Vector::from_i32(&dim_values),
            ]),
            array_to_string_input: Chunk::from_vectors(vec![
                int_array_vector(),
                Vector::from_strings(&delimiter_values),
            ]),
            l2_distance_input: Chunk::from_vectors(vec![
                embedding_vector(0.0),
                embedding_vector(1.0),
            ]),
            array_length_expr: array_length_expr(array_type.clone()),
            array_to_string_expr: array_to_string_expr(array_type),
            l2_distance_expr: l2_distance_expr(float_array_type),
        }
    })
}

fn bind_function(
    set: ScalarFunctionSet,
    argument_types: Vec<LogicalType>,
    constant_values: Vec<Option<Value>>,
) -> BoundScalarFunction {
    let (function, _) = set.bind(&argument_types).expect("bind benchmark overload");
    function
        .bind(&ScalarBindInput::new(argument_types, constant_values))
        .expect("bind benchmark scalar function")
}

fn int_array_vector() -> Vector {
    let array_type = LogicalType::Array(Box::new(LogicalType::Integer), INT_WIDTH);
    let mut vector = Vector::new_array(array_type, ROWS);
    vector.set_count(ROWS);

    for row in 0..ROWS {
        let values = (0..INT_WIDTH)
            .map(|offset| Value::Integer(row as i32 + offset as i32))
            .collect::<Vec<_>>();
        vector.set_value(row, &Value::Array(values, LogicalType::Integer, INT_WIDTH));
    }

    vector
}

fn embedding_vector(offset: f32) -> Vector {
    let values = (0..ROWS)
        .map(|row| {
            (0..FLOAT_WIDTH)
                .map(|index| row as f32 + index as f32 + offset)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Vector::from_embeddings(&values, FLOAT_WIDTH)
}

fn reference(index: usize, logical_type: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, logical_type))
}

fn array_length_expr(array_type: LogicalType) -> Expression {
    let bound = bind_function(
        get_array_length_functions(),
        vec![array_type.clone(), LogicalType::Integer],
        vec![None, None],
    );
    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference(0, array_type), reference(1, LogicalType::Integer)],
        LogicalType::Integer,
    ))
}

fn array_to_string_expr(array_type: LogicalType) -> Expression {
    let bound = bind_function(
        get_array_to_string_functions(),
        vec![array_type.clone(), LogicalType::Varchar],
        vec![None, None],
    );
    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference(0, array_type), reference(1, LogicalType::Varchar)],
        LogicalType::Varchar,
    ))
}

fn l2_distance_expr(array_type: LogicalType) -> Expression {
    let bound = bind_function(
        get_l2_distance_functions(),
        vec![array_type.clone(), array_type.clone()],
        vec![None, None],
    );
    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference(0, array_type.clone()), reference(1, array_type)],
        LogicalType::Double,
    ))
}

#[divan::bench(sample_count = 10)]
fn array_length_fixed_array(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.array_length_expr);
    let mut result = Vector::with_capacity(LogicalType::Integer, ROWS);

    bencher
        .counter(state.array_length_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.array_length_input,
                    None,
                    state.array_length_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("array_length benchmark should execute");
            divan::black_box(result.get_i32(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn array_to_string_fixed_array(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.array_to_string_expr);
    let mut result = Vector::with_capacity(LogicalType::Varchar, ROWS);

    bencher
        .counter(state.array_to_string_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.array_to_string_input,
                    None,
                    state.array_to_string_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("array_to_string benchmark should execute");
            divan::black_box(result.get_string(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn l2_distance_fixed_array(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.l2_distance_expr);
    let mut result = Vector::with_capacity(LogicalType::Double, ROWS);

    bencher
        .counter(state.l2_distance_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.l2_distance_input,
                    None,
                    state.l2_distance_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("l2_distance benchmark should execute");
            divan::black_box(result.get_f64(ROWS - 1));
        });
}
