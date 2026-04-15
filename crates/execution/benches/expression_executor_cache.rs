use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_execution::execution_context::ExecutionContext;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::thread_context::ThreadContext;
use paro_function::scalar::{FunctionExecContext, ScalarFunction};
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConstantExpression, Expression, FunctionExpression,
    ReferenceExpression,
};

const ROWS: usize = 2048;

fn main() {
    divan::main();
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    input: Chunk,
    projection_expr: Expression,
    predicate_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);
        let values: Vec<i32> = (0..ROWS as i32).collect();
        BenchState {
            runtime,
            input: Chunk::from_vectors(vec![Vector::from_i32(&values)]),
            projection_expr: add_one_expr(),
            predicate_expr: greater_than_expr((ROWS / 2) as i32),
        }
    })
}

fn add_one_function(
    input: &Chunk,
    _runtime: &dyn FunctionExecContext,
    result: &mut Vector,
) -> Result<()> {
    let column = input
        .column(0)
        .expect("projection benchmark expects one column");
    for row_idx in 0..input.size() {
        result.set_i32(
            row_idx,
            column
                .get_i32(row_idx)
                .expect("projection benchmark expects non-null integers")
                + 1,
        );
    }
    Ok(())
}

fn add_one_expr() -> Expression {
    let function = ScalarFunction::new(
        "bench_add_one".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        add_one_function,
    );
    Expression::Function(FunctionExpression::new(
        function,
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::Integer,
        ))],
        LogicalType::Integer,
    ))
}

fn greater_than_expr(value: i32) -> Expression {
    Expression::Comparison(ComparisonExpression::new(
        ComparisonType::GreaterThan,
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        )),
    ))
}

#[divan::bench(sample_count = 10)]
fn projection_cached_executor(bencher: Bencher) {
    let state = bench_state();
    let mut executor =
        ExpressionExecutor::with_expressions(std::slice::from_ref(&state.projection_expr));
    let mut output = Chunk::new();
    bencher.counter(state.input.size()).bench_local(|| {
        executor
            .execute_all_into(&state.input, &state.runtime, &mut output)
            .expect("cached projection benchmark should execute");
        divan::black_box(output.get_value(0, ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn projection_new_per_batch(bencher: Bencher) {
    let state = bench_state();
    let mut output = Chunk::new();
    bencher.counter(state.input.size()).bench_local(|| {
        let mut executor =
            ExpressionExecutor::with_expressions(std::slice::from_ref(&state.projection_expr));
        executor
            .execute_all_into(&state.input, &state.runtime, &mut output)
            .expect("per-batch projection benchmark should execute");
        divan::black_box(output.get_value(0, ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn select_cached_executor(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.predicate_expr);
    let mut selection = SelectionVector::with_capacity(ROWS);
    bencher.counter(state.input.size()).bench_local(|| {
        selection.set_len(ROWS);
        let selected = executor
            .select_into(
                0,
                &state.input,
                state.input.size(),
                &state.runtime,
                &mut selection,
            )
            .expect("cached predicate benchmark should execute");
        divan::black_box(selected);
    });
}

#[divan::bench(sample_count = 10)]
fn select_new_per_batch(bencher: Bencher) {
    let state = bench_state();
    let mut selection = SelectionVector::with_capacity(ROWS);
    bencher.counter(state.input.size()).bench_local(|| {
        selection.set_len(ROWS);
        let mut executor = ExpressionExecutor::new(&state.predicate_expr);
        let selected = executor
            .select_into(
                0,
                &state.input,
                state.input.size(),
                &state.runtime,
                &mut selection,
            )
            .expect("per-batch predicate benchmark should execute");
        divan::black_box(selected);
    });
}
