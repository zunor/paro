// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::{DictionaryInfo, DictionarySource};
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::runtime::QueryRuntimeContext;
use paro_function::scalar::string::get_length_functions;
use paro_function::scalar::ScalarBindInput;
use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};

const ROWS: usize = 2048;

mod support;
const UNIQUE_VALUES: usize = 64;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: QueryRuntimeContext,
    expr: Expression,
    cache_hit_input: Chunk,
    cache_miss_input: Chunk,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let runtime = support::query_runtime();
        BenchState {
            runtime,
            expr: length_expr(),
            cache_hit_input: storage_dictionary_varchar_input(11),
            cache_miss_input: storage_dictionary_varchar_input(12),
        }
    })
}

fn storage_dictionary_varchar_input(provenance_id: u64) -> Chunk {
    let values: Vec<String> = (0..UNIQUE_VALUES)
        .map(|idx| format!("value_{idx:02}"))
        .collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    let selection: Vec<u32> = (0..ROWS).map(|idx| (idx % UNIQUE_VALUES) as u32).collect();
    let vector = paro_common::test_utils::test_with_dictionary(
        Arc::new(paro_common::test_utils::test_string_vector_with_allocator(
            &refs,
            paro_common::test_utils::test_allocator(),
        )),
        selection,
        DictionaryInfo {
            unique_len: UNIQUE_VALUES,
            provenance_id: Some(provenance_id),
            source: DictionarySource::Storage,
        },
    );
    paro_common::test_utils::test_chunk_from_vectors(vec![vector])
}

fn length_expr() -> Expression {
    let function = get_length_functions()
        .functions
        .into_iter()
        .next()
        .expect("length function should exist")
        .bind(&ScalarBindInput::new(
            vec![LogicalType::Varchar],
            vec![None],
        ))
        .expect("length binding should succeed");
    Expression::Function(FunctionExpression::new(
        function,
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::Varchar,
        ))],
        LogicalType::BigInt,
    ))
}

#[divan::bench(sample_count = 10)]
fn length_storage_dictionary_cache_hit(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.expr);
    let mut output = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, ROWS);

    bencher.counter(ROWS).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.cache_hit_input,
                None,
                state.cache_hit_input.size(),
                &state.runtime,
                &mut output,
            )
            .expect("storage dictionary cache-hit benchmark should execute");
        divan::black_box(output.get_i64(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn length_storage_dictionary_provenance_miss(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.expr);
    let mut output = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, ROWS);
    let mut toggle = false;

    bencher.counter(ROWS).bench_local(|| {
        toggle = !toggle;
        let input = if toggle {
            &state.cache_hit_input
        } else {
            &state.cache_miss_input
        };
        executor
            .execute_into(0, input, None, input.size(), &state.runtime, &mut output)
            .expect("storage dictionary cache-miss benchmark should execute");
        divan::black_box(output.get_i64(ROWS - 1));
    });
}
