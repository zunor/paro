// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::runtime::QueryRuntimeContext;
use paro_planner::expression::{Expression, ReferenceExpression};

const ROWS: usize = 2048;

mod support;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: QueryRuntimeContext,
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
        let runtime = support::query_runtime();
        let values: Vec<i64> = (0..ROWS as i64).collect();
        let selection = paro_common::test_utils::test_selection((0..ROWS as u32).rev().collect());
        let flat = Arc::new(paro_common::test_utils::test_i64_vector_with_allocator(
            &values,
            paro_common::test_utils::test_allocator(),
        ));

        BenchState {
            runtime,
            flat: Arc::clone(&flat),
            constant: paro_common::test_utils::test_constant_with_allocator(
                LogicalType::BigInt,
                7_i64,
                ROWS,
                paro_common::test_utils::test_allocator(),
            ),
            sequence: paro_common::test_utils::test_sequence_with_allocator(
                11,
                3,
                ROWS,
                paro_common::test_utils::test_allocator(),
            ),
            selection,
            input: paro_common::test_utils::test_chunk_from_arc_vectors(vec![flat]),
            reference_expr: Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        }
    })
}

#[divan::bench(sample_count = 12)]
fn flat_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.flat.try_to_view(ROWS).unwrap();
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn constant_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.constant.try_to_view(ROWS).unwrap();
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn sequence_to_view(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let view = state.sequence.try_to_view(ROWS).unwrap();
        divan::black_box(view.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 12)]
fn dictionary_overlay_shared_selection(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let dict =
            paro_common::test_utils::test_dictionary(Arc::clone(&state.flat), &state.selection);
        divan::black_box(dict.sel_vector().and_then(|sel| sel.allocation_identity()));
    });
}

#[divan::bench(sample_count = 12)]
fn dictionary_overlay_deep_copy_selection(bencher: Bencher) {
    let state = bench_state();
    bencher.counter(ROWS).bench_local(|| {
        let dict = paro_common::test_utils::test_dictionary(
            Arc::clone(&state.flat),
            state
                .selection
                .try_deep_copy()
                .expect("selection copy allocation failed"),
        );
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
