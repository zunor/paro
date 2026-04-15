use std::sync::{Arc, OnceLock};

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_execution::execution_context::ExecutionContext;
use paro_execution::expression_executor::executor::ExpressionExecutor;
use paro_execution::thread_context::ThreadContext;
use paro_function::scalar::cast::{numeric_casts, BoundCastInfo};
use paro_function::scalar::executor::binary::BinaryExecutor;
use paro_function::scalar::executor::ternary::TernaryExecutor;
use paro_function::scalar::executor::unary::UnaryExecutor;
use paro_function::scalar::executor::{BinaryOperator, TernaryOperator, UnaryOperator};
use paro_function::scalar::{FunctionExecContext, ScalarFunction};
use paro_planner::expression::{
    CastExpression, ComparisonExpression, ComparisonType, Expression, FunctionExpression,
    OperatorExpression, OperatorType, ReferenceExpression,
};

const ROWS: usize = 2048;

fn main() {
    divan::main();
}

struct AbsOp;
impl UnaryOperator<i64, i64> for AbsOp {
    fn operation(input: i64) -> i64 {
        input.abs()
    }
}

struct NotOp;
impl UnaryOperator<bool, bool> for NotOp {
    fn operation(input: bool) -> bool {
        !input
    }
}

struct AddOp;
impl BinaryOperator<i64, i64, i64> for AddOp {
    fn operation(left: i64, right: i64) -> i64 {
        left + right
    }
}

struct BetweenOp;
impl TernaryOperator<i64, i64, i64, bool> for BetweenOp {
    fn operation(value: i64, low: i64, high: i64) -> bool {
        value >= low && value <= high
    }
}

struct BenchState {
    runtime: ExecutionContext<'static>,
    cast_i32_input: Chunk,
    nullable_i32_input: Chunk,
    coalesce_i32_input: Chunk,
    in_i32_input: Chunk,
    unary_i64_input: Chunk,
    unary_bool_input: Chunk,
    binary_i64_input: Chunk,
    ternary_i64_input: Chunk,
    cast_expr: Expression,
    abs_expr: Expression,
    not_expr: Expression,
    add_expr: Expression,
    eq_expr: Expression,
    lt_expr: Expression,
    between_expr: Expression,
    is_null_expr: Expression,
    coalesce_expr: Expression,
    in_small_expr: Expression,
    in_large_expr: Expression,
}

fn bench_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        let runtime = ExecutionContext::new(session, thread, None);

        let unary_i64_values = (0..ROWS)
            .map(|row| row as i64 - (ROWS as i64 / 2))
            .collect::<Vec<_>>();
        let cast_i32_values = (0..ROWS)
            .map(|row| row as i32 - (ROWS as i32 / 2))
            .collect::<Vec<_>>();
        let nullable_i32_values = (0..ROWS)
            .map(|row| if row % 5 == 0 { None } else { Some(row as i32) })
            .collect::<Vec<_>>();
        let coalesce_first = (0..ROWS)
            .map(|row| if row % 4 == 0 { None } else { Some(row as i32) })
            .collect::<Vec<_>>();
        let coalesce_second = (0..ROWS)
            .map(|row| {
                if row % 7 == 0 {
                    None
                } else {
                    Some((row as i32) + 10_000)
                }
            })
            .collect::<Vec<_>>();
        let unary_bool_values = (0..ROWS).map(|row| row % 2 == 0).collect::<Vec<_>>();
        let binary_left = (0..ROWS).map(|row| row as i64).collect::<Vec<_>>();
        let binary_right = (0..ROWS).map(|row| (ROWS - row) as i64).collect::<Vec<_>>();
        let ternary_values = (0..ROWS).map(|row| row as i64).collect::<Vec<_>>();
        let ternary_low = (0..ROWS).map(|row| row as i64 - 1).collect::<Vec<_>>();
        let ternary_high = (0..ROWS).map(|row| row as i64 + 1).collect::<Vec<_>>();

        BenchState {
            runtime,
            cast_i32_input: Chunk::from_vectors(vec![Vector::from_i32(&cast_i32_values)]),
            nullable_i32_input: Chunk::from_vectors(vec![nullable_i32_vector(
                &nullable_i32_values,
            )]),
            coalesce_i32_input: Chunk::from_vectors(vec![
                nullable_i32_vector(&coalesce_first),
                nullable_i32_vector(&coalesce_second),
            ]),
            in_i32_input: Chunk::from_vectors(vec![nullable_i32_vector(&nullable_i32_values)]),
            unary_i64_input: Chunk::from_vectors(vec![Vector::from_i64(&unary_i64_values)]),
            unary_bool_input: Chunk::from_vectors(vec![Vector::from_bool(&unary_bool_values)]),
            binary_i64_input: Chunk::from_vectors(vec![
                Vector::from_i64(&binary_left),
                Vector::from_i64(&binary_right),
            ]),
            ternary_i64_input: Chunk::from_vectors(vec![
                Vector::from_i64(&ternary_values),
                Vector::from_i64(&ternary_low),
                Vector::from_i64(&ternary_high),
            ]),
            cast_expr: cast_i32_to_i64_expr(),
            abs_expr: abs_expr(),
            not_expr: not_expr(),
            add_expr: add_expr(),
            eq_expr: comparison_expr(ComparisonType::Equal),
            lt_expr: comparison_expr(ComparisonType::LessThan),
            between_expr: between_expr(),
            is_null_expr: is_null_expr(),
            coalesce_expr: coalesce_i32_expr(),
            in_small_expr: in_list_i32_expr(&[3, 17, 42, 99]),
            in_large_expr: in_list_i32_expr(&(0..64).collect::<Vec<_>>()),
        }
    })
}

fn reference_bigint(index: usize) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, LogicalType::BigInt))
}

fn reference_i32(index: usize) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
}

fn reference_bool(index: usize) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, LogicalType::Boolean))
}

fn nullable_i32_vector(values: &[Option<i32>]) -> Vector {
    let dense: Vec<i32> = values
        .iter()
        .map(|value| value.unwrap_or_default())
        .collect();
    let mut vector = Vector::from_i32(&dense);
    for (row_idx, value) in values.iter().enumerate() {
        if value.is_none() {
            vector.set_null(row_idx, true);
        }
    }
    vector
}

fn abs_function(
    input: &Chunk,
    _runtime: &dyn FunctionExecContext,
    result: &mut Vector,
) -> Result<()> {
    UnaryExecutor::execute::<i64, i64, AbsOp>(&input.data[0], result, input.size());
    Ok(())
}

fn not_function(
    input: &Chunk,
    _runtime: &dyn FunctionExecContext,
    result: &mut Vector,
) -> Result<()> {
    UnaryExecutor::execute::<bool, bool, NotOp>(&input.data[0], result, input.size());
    Ok(())
}

fn add_function(
    input: &Chunk,
    _runtime: &dyn FunctionExecContext,
    result: &mut Vector,
) -> Result<()> {
    BinaryExecutor::execute::<i64, i64, i64, AddOp>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    );
    Ok(())
}

fn between_function(
    input: &Chunk,
    _runtime: &dyn FunctionExecContext,
    result: &mut Vector,
) -> Result<()> {
    TernaryExecutor::execute::<i64, i64, i64, bool, BetweenOp>(
        &input.data[0],
        &input.data[1],
        &input.data[2],
        result,
        input.size(),
    );
    Ok(())
}

fn abs_expr() -> Expression {
    let function = ScalarFunction::new(
        "bench_abs".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        abs_function,
    );
    Expression::Function(FunctionExpression::new(
        function,
        vec![reference_bigint(0)],
        LogicalType::BigInt,
    ))
}

fn cast_i32_to_i64_expr() -> Expression {
    Expression::Cast(CastExpression::new(
        reference_i32(0),
        LogicalType::BigInt,
        BoundCastInfo::fixed(numeric_casts::int32_to_int64),
        false,
    ))
}

fn not_expr() -> Expression {
    let function = ScalarFunction::new(
        "bench_not".to_string(),
        vec![LogicalType::Boolean],
        LogicalType::Boolean,
        not_function,
    );
    Expression::Function(FunctionExpression::new(
        function,
        vec![reference_bool(0)],
        LogicalType::Boolean,
    ))
}

fn add_expr() -> Expression {
    let function = ScalarFunction::new(
        "bench_add".to_string(),
        vec![LogicalType::BigInt, LogicalType::BigInt],
        LogicalType::BigInt,
        add_function,
    );
    Expression::Function(FunctionExpression::new(
        function,
        vec![reference_bigint(0), reference_bigint(1)],
        LogicalType::BigInt,
    ))
}

fn comparison_expr(comparison_type: ComparisonType) -> Expression {
    Expression::Comparison(ComparisonExpression::new(
        comparison_type,
        reference_bigint(0),
        reference_bigint(1),
    ))
}

fn between_expr() -> Expression {
    let function = ScalarFunction::new(
        "bench_between".to_string(),
        vec![
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
        LogicalType::Boolean,
        between_function,
    );
    Expression::Function(FunctionExpression::new(
        function,
        vec![
            reference_bigint(0),
            reference_bigint(1),
            reference_bigint(2),
        ],
        LogicalType::Boolean,
    ))
}

fn is_null_expr() -> Expression {
    Expression::Operator(OperatorExpression::new(
        OperatorType::IsNull,
        vec![reference_i32(0)],
        LogicalType::Boolean,
    ))
}

fn constant_i32(value: i32) -> Expression {
    Expression::Constant(paro_planner::expression::ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn coalesce_i32_expr() -> Expression {
    Expression::Operator(OperatorExpression::new(
        OperatorType::Coalesce,
        vec![reference_i32(0), reference_i32(1), constant_i32(777)],
        LogicalType::Integer,
    ))
}

fn in_list_i32_expr(values: &[i32]) -> Expression {
    let mut children = Vec::with_capacity(values.len() + 1);
    children.push(reference_i32(0));
    for value in values {
        children.push(constant_i32(*value));
    }
    Expression::Operator(OperatorExpression::new(
        OperatorType::In,
        children,
        LogicalType::Boolean,
    ))
}

#[divan::bench(sample_count = 10)]
fn cast_i32_to_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.cast_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher
        .counter(state.cast_i32_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.cast_i32_input,
                    None,
                    state.cast_i32_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("cast benchmark should execute");
            divan::black_box(result.get_i64(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn abs_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.abs_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher
        .counter(state.unary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.unary_i64_input,
                    None,
                    state.unary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("abs benchmark should execute");
            divan::black_box(result.get_i64(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn not_bool(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.not_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher
        .counter(state.unary_bool_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.unary_bool_input,
                    None,
                    state.unary_bool_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("not benchmark should execute");
            divan::black_box(result.get_bool(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn add_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.add_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher
        .counter(state.binary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.binary_i64_input,
                    None,
                    state.binary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("add benchmark should execute");
            divan::black_box(result.get_i64(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn eq_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.eq_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher
        .counter(state.binary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.binary_i64_input,
                    None,
                    state.binary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("eq benchmark should execute");
            divan::black_box(result.get_bool(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn lt_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.lt_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher
        .counter(state.binary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.binary_i64_input,
                    None,
                    state.binary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("lt benchmark should execute");
            divan::black_box(result.get_bool(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn between_i64(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.between_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher
        .counter(state.ternary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.ternary_i64_input,
                    None,
                    state.ternary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("between benchmark should execute");
            divan::black_box(result.get_bool(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn is_null_i32(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.is_null_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher
        .counter(state.nullable_i32_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.nullable_i32_input,
                    None,
                    state.nullable_i32_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("is null benchmark should execute");
            divan::black_box(result.get_bool(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn coalesce_i32(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.coalesce_expr);
    let mut result = Vector::with_capacity(LogicalType::Integer, ROWS);
    bencher
        .counter(state.coalesce_i32_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.coalesce_i32_input,
                    None,
                    state.coalesce_i32_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("coalesce benchmark should execute");
            divan::black_box(result.get_i32(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn in_small_i32(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.in_small_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher.counter(state.in_i32_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.in_i32_input,
                None,
                state.in_i32_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("small IN benchmark should execute");
        divan::black_box(result.get_bool(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn in_large_i32(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.in_large_expr);
    let mut result = Vector::with_capacity(LogicalType::Boolean, ROWS);
    bencher.counter(state.in_i32_input.size()).bench_local(|| {
        executor
            .execute_into(
                0,
                &state.in_i32_input,
                None,
                state.in_i32_input.size(),
                &state.runtime,
                &mut result,
            )
            .expect("large IN benchmark should execute");
        divan::black_box(result.get_bool(ROWS - 1));
    });
}

#[divan::bench(sample_count = 10)]
fn execute_into_reused_output(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.abs_expr);
    let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
    bencher
        .counter(state.unary_i64_input.size())
        .bench_local(|| {
            executor
                .execute_into(
                    0,
                    &state.unary_i64_input,
                    None,
                    state.unary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("reused-output benchmark should execute");
            divan::black_box(result.get_i64(ROWS - 1));
        });
}

#[divan::bench(sample_count = 10)]
fn execute_into_fresh_output(bencher: Bencher) {
    let state = bench_state();
    let mut executor = ExpressionExecutor::new(&state.abs_expr);
    bencher
        .counter(state.unary_i64_input.size())
        .bench_local(|| {
            let mut result = Vector::with_capacity(LogicalType::BigInt, ROWS);
            executor
                .execute_into(
                    0,
                    &state.unary_i64_input,
                    None,
                    state.unary_i64_input.size(),
                    &state.runtime,
                    &mut result,
                )
                .expect("fresh-output benchmark should execute");
            divan::black_box(result.get_i64(ROWS - 1));
        });
}
