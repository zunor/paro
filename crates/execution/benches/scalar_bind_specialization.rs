// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::runtime::QueryRuntimeContext;
use paro_function::scalar::string::{get_regexp_functions, get_substring_functions};
use paro_function::scalar::{BoundScalarFunction, ScalarBindInput, ScalarFunction};
use paro_planner::expression::{
    ConstantExpression, Expression, FunctionExpression, ReferenceExpression,
};

const ROWS: usize = 2048;

mod support;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: QueryRuntimeContext,
    regexp_input: Chunk,
    substring_input: Chunk,
    regexp_bound: Expression,
    regexp_unbound: Expression,
    substring_bound: Expression,
    substring_unbound: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let runtime = support::query_runtime();

        let regexp_rows = (0..ROWS)
            .map(|row_idx| {
                if row_idx % 2 == 0 {
                    "hello_world".to_string()
                } else {
                    "goodbye_world".to_string()
                }
            })
            .collect::<Vec<_>>();
        let substring_rows = (0..ROWS)
            .map(|row_idx| format!("prefix_{row_idx}_suffix"))
            .collect::<Vec<_>>();
        let regexp_refs = regexp_rows.iter().map(String::as_str).collect::<Vec<_>>();
        let substring_refs = substring_rows
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        BenchState {
            runtime,
            regexp_input: Chunk::from_vectors(
                vec![paro_common::test_utils::test_string_vector_with_allocator(
                    &regexp_refs,
                    paro_common::test_utils::test_allocator(),
                )],
                paro_common::test_utils::test_allocator(),
            ),
            substring_input: Chunk::from_vectors(
                vec![paro_common::test_utils::test_string_vector_with_allocator(
                    &substring_refs,
                    paro_common::test_utils::test_allocator(),
                )],
                paro_common::test_utils::test_allocator(),
            ),
            regexp_bound: regexp_expr(true),
            regexp_unbound: regexp_expr(false),
            substring_bound: substring_expr(true),
            substring_unbound: substring_expr(false),
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

fn constant_bigint(value: i64) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::BigInt(value),
        LogicalType::BigInt,
    ))
}

fn bind_or_default(
    function: ScalarFunction,
    specialized: bool,
    input: ScalarBindInput,
) -> BoundScalarFunction {
    if specialized {
        function.bind(&input).expect("specialize scalar function")
    } else {
        function.into()
    }
}

fn regexp_expr(specialized: bool) -> Expression {
    let set = get_regexp_functions();
    let (function, _) = set
        .bind(&[LogicalType::Varchar, LogicalType::Varchar])
        .expect("bind regexp overload");
    let bound = bind_or_default(
        function,
        specialized,
        ScalarBindInput::new(
            vec![LogicalType::Varchar, LogicalType::Varchar],
            vec![None, Some(Value::Varchar("^hello_world$".to_string()))],
        ),
    );

    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference_varchar(0), constant_varchar("^hello_world$")],
        LogicalType::Boolean,
    ))
}

fn substring_expr(specialized: bool) -> Expression {
    let set = get_substring_functions();
    let (function, _) = set
        .bind(&[
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ])
        .expect("bind substring overload");
    let bound = bind_or_default(
        function,
        specialized,
        ScalarBindInput::new(
            vec![
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ],
            vec![None, Some(Value::BigInt(8)), Some(Value::BigInt(6))],
        ),
    );

    Expression::Function(FunctionExpression::new(
        bound,
        vec![reference_varchar(0), constant_bigint(8), constant_bigint(6)],
        LogicalType::Varchar,
    ))
}

#[divan::bench(sample_count = 10)]
fn regexp_bound_constant_pattern(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.regexp_bound);
    let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, ROWS);
    bencher.counter(state.regexp_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.regexp_input,
                None,
                state.regexp_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("bound regexp benchmark");
        divan::black_box(result.get_bool(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn regexp_unbound_constant_pattern(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.regexp_unbound);
    let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, ROWS);
    bencher.counter(state.regexp_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.regexp_input,
                None,
                state.regexp_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("unbound regexp benchmark");
        divan::black_box(result.get_bool(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn substring_bound_constant_args(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.substring_bound);
    let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::Varchar, ROWS);
    bencher
        .counter(state.substring_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.substring_input,
                    None,
                    state.substring_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("bound substring benchmark");
            divan::black_box(result.get_string(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn substring_unbound_constant_args(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.substring_unbound);
    let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::Varchar, ROWS);
    bencher
        .counter(state.substring_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.substring_input,
                    None,
                    state.substring_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("unbound substring benchmark");
            divan::black_box(result.get_string(ROWS - 1));
        });
}
