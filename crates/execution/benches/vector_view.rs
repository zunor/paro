// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_execution::execution_context::ExecutionContext;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::thread_context::ThreadContext;
use paro_planner::expression::{Expression, ReferenceExpression};

const ROWS: usize = 2048;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    flat: Arc<Vector>,
    constant: Vector,
    sequence: Vector,
    selection: SelectionVector,
    input: Chunk,
    reference_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);
        let values: Vec<i64> = (0..ROWS as i64).collect();
        let selection = SelectionVector::from_indices((0..ROWS as u32).rev().collect());
        let flat = Arc::new(Vector::from_i64(&values));

        BenchState {
            runtime,
            flat: Arc::clone(&flat),
            constant: Vector::constant(LogicalType::BigInt, 7_i64, ROWS),
            sequence: Vector::sequence(11, 3, ROWS),
            selection,
            input: Chunk::from_arc_vectors(vec![flat]),
            reference_expr: Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        }
    })
}

#[divan::bench(sample_count = 12)]
fn flat_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.flat.to_view(ROWS);
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn constant_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.constant.to_view(ROWS);
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn sequence_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.sequence.to_view(ROWS);
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn dictionary_overlay_shared_selection(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let dict = Vector::dictionary(Arc::clone(&state.flat), &state.selection);
        divan::black_box(dict.sel_vector().and_then(|sel| sel.allocation_identity()));
    });
}

#[divan::bench(sample_count = 12)]
fn dictionary_overlay_deep_copy_selection(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let dict = Vector::dictionary(Arc::clone(&state.flat), state.selection.deep_copy());
        divan::black_box(dict.sel_vector().and_then(|sel| sel.allocation_identity()));
    });
}

#[divan::bench(sample_count = 12)]
fn reference_with_selection_overlay(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.reference_expr);
    bencher.counter(ROWS).bench_local(|| {
        let result = executor
            .execute_expression(
                0,
                &state.input,
                Some(&state.selection),
                state.selection.len(),
                &state.runtime,
            )
            .expect("reference overlay benchmark should execute");
        divan::black_box(result.get_i64(ROWS - 1));
    });
}
