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
use paro_function::scalar::cast::{string_casts, BoundCastInfo};
use paro_function::scalar::string::{
    get_length_functions, get_lower_function, get_replace_functions,
};
use paro_function::scalar::{BoundScalarFunction, ScalarBindInput, ScalarFunctionSet};
use paro_planner::expression::{
    CastExpression, ConstantExpression, Expression, FunctionExpression, ReferenceExpression,
};

const ROWS: usize = 2048;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    string_input: Chunk,
    replace_input: Chunk,
    cast_input: Chunk,
    length_expr: Expression,
    lower_expr: Expression,
    replace_expr: Expression,
    cast_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);

        let string_rows = (0..ROWS)
            .map(|row| match row % 3 {
                0 => format!("PREFIX_{row}_HELLO_世界_SUFFIX"),
                1 => format!("MIXED_{row}_PARO_ß_HELLO"),
                _ => format!("ASCII_{row}_HELLO_HELLO"),
            })
            .collect::<Vec<_>>();
        let cast_rows = (0..ROWS)
            .map(|row| (row as i64 - (ROWS as i64 / 2)).to_string())
            .collect::<Vec<_>>();

        let string_refs = string_rows.iter().map(String::as_str).collect::<Vec<_>>();
        let cast_refs = cast_rows.iter().map(String::as_str).collect::<Vec<_>>();

        BenchState {
            runtime,
            string_input: Chunk::from_vectors(vec![Vector::from_strings(&string_refs)]),
            replace_input: Chunk::from_vectors(vec![Vector::from_strings(&string_refs)]),
            cast_input: Chunk::from_vectors(vec![Vector::from_strings(&cast_refs)]),
            length_expr: length_expr(),
            lower_expr: lower_expr(),
            replace_expr: replace_expr(),
            cast_expr: cast_varchar_to_i64_expr(),
        }
    })
}

fn reference_varchar(index: usize) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, LogicalType::Varchar))
}

fn constant_varchar(value: &str) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Varchar(value.to_string()),
        LogicalType::Varchar,
    ))
}

fn bind_function(
    set: ScalarFunctionSet,
    argument_types: Vec<LogicalType>,
    constant_values: Vec<Option<Value>>,
) -> BoundScalarFunction {
    let (function, _) = set
        .bind(&argument_types)
        .expect("bind benchmark function overload");
    function
        .bind(&ScalarBindInput::new(argument_types, constant_values))
        .expect("bind benchmark scalar function")
}

fn length_expr() -> Expression {
    let bound = bind_function(
        get_length_functions(),
        vec![LogicalType::Varchar],
        vec![None],
    );
    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference_varchar(0)],
        LogicalType::BigInt,
    ))
}

fn lower_expr() -> Expression {
    let bound = bind_function(get_lower_function(), vec![LogicalType::Varchar], vec![None]);
    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference_varchar(0)],
        LogicalType::Varchar,
    ))
}

fn replace_expr() -> Expression {
    let bound = bind_function(
        get_replace_functions(),
        vec![
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
        vec![
            None,
            Some(Value::Varchar("HELLO".to_string())),
            Some(Value::Varchar("hello".to_string())),
        ],
    );
    Expression::Function(FunctionExpression::new(
        bound,
        vec![
            reference_varchar(0),
            constant_varchar("HELLO"),
            constant_varchar("hello"),
        ],
        LogicalType::Varchar,
    ))
}

fn cast_varchar_to_i64_expr() -> Expression {
    Expression::Cast(CastExpression::new(
        reference_varchar(0),
        LogicalType::BigInt,
        BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i64>),
        false,
    ))
}

#[divan::bench(sample_count = 10)]
fn length_varchar(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.length_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher.counter(state.string_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.string_input,
                None,
                state.string_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("length benchmark should execute");
        divan::black_box(result.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn lower_varchar(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.lower_expr);
    let mut result = Vector::with_capacity(LogicalType::Varchar, ROWS);
    bencher.counter(state.string_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.string_input,
                None,
                state.string_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("lower benchmark should execute");
        divan::black_box(result.get_string(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn replace_varchar(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.replace_expr);
    let mut result = Vector::with_capacity(LogicalType::Varchar, ROWS);
    bencher.counter(state.replace_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.replace_input,
                None,
                state.replace_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("replace benchmark should execute");
        divan::black_box(result.get_string(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn cast_varchar_to_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.cast_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher.counter(state.cast_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.cast_input,
                None,
                state.cast_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("varchar cast benchmark should execute");
        divan::black_box(result.get_i64(ROWS - 1));
    });
}
