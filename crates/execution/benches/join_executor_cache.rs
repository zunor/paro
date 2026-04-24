// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_execution::execution_context::ExecutionContext;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::thread_context::ThreadContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, Expression, ReferenceExpression,
};

const BATCH_ROWS: usize = 2048;
const ANY_MATCH_CALLS: usize = 4096;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    join_key_input: Chunk,
    join_key_exprs: Vec<Expression>,
    any_match_input: Chunk,
    any_match_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);
        let left_values: Vec<i32> = (0..BATCH_ROWS as i32).collect();
        let right_values: Vec<i32> = (0..BATCH_ROWS as i32).rev().collect();

        BenchState {
            runtime,
            join_key_input: Chunk::from_vectors(
                vec![
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &left_values,
                        paro_common::test_utils::test_allocator(),
                    ),
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &right_values,
                        paro_common::test_utils::test_allocator(),
                    ),
                ],
                paro_common::test_utils::test_allocator(),
            ),
            join_key_exprs: vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
            ],
            any_match_input: single_row_chunk(3, 2),
            any_match_expr: Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
            )),
        }
    })
}

fn single_row_chunk(left: i32, right: i32) -> Chunk {
    let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        1,
    );
    chunk.set_cardinality(1);
    chunk
        .column_mut(0)
        .expect("left column")
        .set_value(0, &Value::Integer(left));
    chunk
        .column_mut(1)
        .expect("right column")
        .set_value(0, &Value::Integer(right));
    chunk
}

#[divan::bench(sample_count = 10)]
fn hash_join_keys_cached_executor(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::with_expressions(&state.join_key_exprs);
    let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    bencher
        .counter(state.join_key_input.size())
        .bench_local(|| {
            executor
                .execute_all_into(&state.join_key_input, &state.runtime, &mut output)
                .expect("cached join-key benchmark should execute");
            divan::black_box(output.get_value(1, BATCH_ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn hash_join_keys_new_per_batch(bencher: Bencher) {
    let state = bench_state();
    let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    bencher
        .counter(state.join_key_input.size())
        .bench_local(|| {
            let mut executor = ExpressionExecutor::with_expressions(&state.join_key_exprs);
            executor
                .execute_all_into(&state.join_key_input, &state.runtime, &mut output)
                .expect("per-batch join-key benchmark should execute");
            divan::black_box(output.get_value(1, BATCH_ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn nested_loop_any_cached_executor(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.any_match_expr);
    bencher.counter(ANY_MATCH_CALLS).bench_local(|| {
        for _ in 0..ANY_MATCH_CALLS {
            let result = executor
                .execute_expression(0, &state.any_match_input, None, 1, &state.runtime)
                .expect("cached any-join benchmark should execute");
            divan::black_box(result.get_bool(0));
        }
    });
}

#[divan::bench(sample_count = 10)]
fn nested_loop_any_new_per_row(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ANY_MATCH_CALLS).bench_local(|| {
        for _ in 0..ANY_MATCH_CALLS {
            let mut executor = ExpressionExecutor::new(&state.any_match_expr);
            let result = executor
                .execute_expression(0, &state.any_match_input, None, 1, &state.runtime)
                .expect("per-row any-join benchmark should execute");
            divan::black_box(result.get_bool(0));
        }
    });
}
