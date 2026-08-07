// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_external::routine::boundary::PlacementClass;
use paro_function::scalar::{ExpressionState, FunctionStability, ScalarFunction};
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConjunctionExpression, ConstantExpression,
    FunctionExpression, OperatorExpression, WindowExpression, WindowFrame, WindowFrameBound,
    WindowFrameType,
};
use paro_planner::operator::{
    AntiJoinMode, ColumnBinding, ComparisonJoin, DelimGet, Get, JoinComparisonType, JoinCondition,
};
use paro_planner::plan::LogicalPlan;

fn plan(ctx: &BindContext, op: LogicalOperator) -> LogicalPlan {
    LogicalPlan::new(ctx, op)
}

fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
    Expression::ColumnRef(ColumnRefExpression {
        binding: paro_planner::operator::ColumnBinding {
            table_index,
            column_index,
        },
        depth: 0,
        return_type: LogicalType::Integer,
    })
}

fn make_constant(value: i32) -> Expression {
    Expression::Constant(ConstantExpression {
        value: paro_common::runtime_value::Value::Integer(value),
        return_type: LogicalType::Integer,
    })
}

fn noop_scalar_execute(
    _input: &Chunk,
    _state: &dyn ExpressionState,
    _result: &mut Vector,
) -> Result<()> {
    Ok(())
}

fn external_call() -> Expression {
    let function = ScalarFunction::new(
        "external_test".to_string(),
        vec![],
        LogicalType::Integer,
        noop_scalar_execute,
    );
    let mut expression = FunctionExpression::new(function, vec![], LogicalType::Integer);
    expression
        .routine_meta
        .as_mut()
        .expect("builtin routine metadata")
        .boundary
        .placement = PlacementClass::External;
    Expression::Function(expression)
}

fn volatile_call() -> Expression {
    let function = ScalarFunction::new(
        "volatile_test".to_string(),
        vec![],
        LogicalType::Integer,
        noop_scalar_execute,
    )
    .with_stability(FunctionStability::Volatile);
    Expression::Function(FunctionExpression::new(
        function,
        vec![],
        LogicalType::Integer,
    ))
}

fn window_with_start_offset(offset: Expression) -> Expression {
    Expression::Window(WindowExpression {
        function: paro_function::window::WindowFunction::row_number(),
        children: vec![],
        partitions: vec![],
        orders: vec![],
        frame: WindowFrame {
            frame_type: WindowFrameType::Rows,
            start_bound: WindowFrameBound::Offset(Box::new(offset)),
            start_is_preceding: true,
            end_bound: WindowFrameBound::CurrentRow,
            end_is_preceding: false,
        },
        ignore_nulls: false,
        return_type: LogicalType::BigInt,
    })
}

fn make_comparison(comp_type: ComparisonType, left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpression::new(comp_type, left, right))
}

fn make_get(table_index: usize) -> LogicalOperator {
    LogicalOperator::Get(Get::new_without_table(
        table_index,
        vec!["col0".to_string(), "col1".to_string()],
        vec![LogicalType::Integer, LogicalType::Varchar],
    ))
}

fn make_delim_join(ctx: &BindContext, join_type: JoinType) -> LogicalOperator {
    let left = plan(ctx, make_get(0));
    let right = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        plan(ctx, make_get(1)),
        plan(
            ctx,
            LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
        ),
        vec![JoinCondition::new(
            make_column_ref(1, 0),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(99, 0),
                LogicalType::Integer,
            )),
            JoinComparisonType::Equal,
        )],
    )));

    let mut join = ComparisonJoin::new(
        join_type,
        left,
        plan(ctx, right),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::Equal,
        )],
    );
    join.duplicate_eliminated_columns = vec![make_column_ref(0, 0)];
    LogicalOperator::Join(Join::Comparison(join))
}

fn contains_empty_result(p: &LogicalPlan) -> bool {
    if p.is_empty_result() {
        return true;
    }
    p.children()
        .iter()
        .any(|child| contains_empty_result(child))
}

#[test]
fn test_filter_extract_bindings() {
    let expr = make_comparison(
        ComparisonType::Equal,
        make_column_ref(0, 0),
        make_constant(5),
    );

    let filter = Filter::new(expr);
    assert!(filter.bindings.contains(&0));
    assert_eq!(filter.bindings.len(), 1);
}

#[test]
fn test_filter_extract_bindings_multiple() {
    let expr = make_comparison(
        ComparisonType::Equal,
        make_column_ref(0, 0),
        make_column_ref(1, 0),
    );

    let filter = Filter::new(expr);
    assert!(filter.bindings.contains(&0));
    assert!(filter.bindings.contains(&1));
    assert_eq!(filter.bindings.len(), 2);
}

#[test]
fn test_filter_extract_bindings_visits_window_frame_offsets() {
    let filter = Filter::new(window_with_start_offset(make_column_ref(7, 0)));

    assert_eq!(filter.bindings, HashSet::from([7]));
}

#[test]
fn test_external_routine_detection_visits_window_frame_offsets() {
    let expression = window_with_start_offset(external_call());

    assert!(expression.contains_external_routine());
}

#[test]
fn test_projection_boundary_check_visits_window_frame_offsets() {
    let ctx = BindContext::new();
    let projection = Projection::new(7, plan(&ctx, make_get(0)), vec![external_call()]);
    let expression = window_with_start_offset(make_column_ref(7, 0));

    assert!(projection_reference_crosses_execution_boundary(
        &projection,
        &expression
    ));
}

#[test]
fn test_pushdown_filter_through_filter() {
    let ctx = BindContext::new();
    let get = make_get(0);
    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0),
        make_constant(5),
    );
    let filter = PlannerFilter::new(plan(&ctx, get), vec![filter_expr]);
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filter should be pushed down to create Filter(Get)
    match result {
        LogicalOperator::Filter(f) => {
            assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
        }
        _ => panic!("Expected Filter operator"),
    }
}

#[test]
fn test_pushdown_through_projection() {
    let ctx = BindContext::new();
    // Create: Filter(Projection(Get))
    // Filter: proj.col0 > 5
    // Projection: [get.col0, get.col1]
    let get = make_get(0);
    let proj = Projection::new(
        1, // projection table index
        plan(&ctx, get),
        vec![make_column_ref(0, 0), make_column_ref(0, 1)],
    );

    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(1, 0), // references projection output
        make_constant(5),
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Projection(proj)),
        vec![filter_expr],
    );
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filter should be pushed through projection
    // Result should be: Projection(Filter(Get))
    match result {
        LogicalOperator::Projection(p) => match p.child.operator {
            LogicalOperator::Filter(f) => {
                assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Filter under Projection"),
        },
        _ => panic!("Expected Projection operator"),
    }
}

#[test]
fn test_filter_stays_above_volatile_projection() {
    let ctx = BindContext::new();
    let projection = Projection::new(1, plan(&ctx, make_get(0)), vec![volatile_call()]);
    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(1, 0),
        make_constant(5),
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Projection(projection)),
        vec![filter_expr],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Filter(filter) = result else {
        panic!("expected evaluation fence above volatile projection");
    };
    assert!(matches!(
        filter.child.operator,
        LogicalOperator::Projection(_)
    ));
}

#[test]
fn test_pushdown_through_cross_product() {
    let ctx = BindContext::new();
    // Create: Filter(Cross(Get0, Get1))
    // Filter: get0.col0 > 5 (only references left side)
    let left = make_get(0);
    let right = make_get(1);
    let cross = CrossProduct::new(plan(&ctx, left), plan(&ctx, right));

    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0), // references left side only
        make_constant(5),
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
        vec![filter_expr],
    );
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filter should be pushed to left side
    // Result should be: Cross(Filter(Get0), Get1)
    match result {
        LogicalOperator::Join(Join::Cross(cp)) => {
            match cp.left.operator {
                LogicalOperator::Filter(f) => {
                    assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Filter on left side"),
            }
            assert!(matches!(cp.right.operator, LogicalOperator::Get(_)));
        }
        _ => panic!("Expected Cross product"),
    }
}

#[test]
fn test_volatile_filter_stays_above_cross_product() {
    let ctx = BindContext::new();
    let cross = CrossProduct::new(plan(&ctx, make_get(0)), plan(&ctx, make_get(1)));
    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0),
        volatile_call(),
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
        vec![filter_expr],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Filter(filter) = result else {
        panic!("expected volatile predicate to remain above cross product");
    };
    assert!(matches!(
        filter.child.operator,
        LogicalOperator::Join(Join::Cross(_))
    ));
}

#[test]
fn test_volatile_inner_join_condition_is_not_converted_to_input_filter() {
    let ctx = BindContext::new();
    let join = ComparisonJoin::new(
        JoinType::Inner,
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            volatile_call(),
            JoinComparisonType::Equal,
        )],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Join(Join::Comparison(join)));

    assert!(matches!(result, LogicalOperator::Join(Join::Comparison(_))));
}

#[test]
fn test_inner_hash_join_shape_survives_nested_filter_pushdown() {
    let ctx = BindContext::new();
    let mut join = ComparisonJoin::new(
        JoinType::Inner,
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::Equal,
        )],
    );
    join.left_projection_map = vec![0];
    join.right_projection_map = vec![0];
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        )],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Join(Join::Comparison(join)) = result else {
        panic!("inner comparison join must remain canonical");
    };
    assert_eq!(join.conditions.len(), 1);
    assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    assert_eq!(join.left_projection_map, vec![0]);
    assert_eq!(join.right_projection_map, vec![0]);
    assert!(matches!(join.left.operator, LogicalOperator::Filter(_)));
}

#[test]
fn test_pushdown_join_filter_stays_above() {
    let ctx = BindContext::new();
    // Create: Filter(Cross(Get0, Get1))
    // Filter: get0.col0 = get1.col0 (references both sides)
    let left = make_get(0);
    let right = make_get(1);
    let cross = CrossProduct::new(plan(&ctx, left), plan(&ctx, right));

    let filter_expr = make_comparison(
        ComparisonType::Equal,
        make_column_ref(0, 0), // references left
        make_column_ref(1, 0), // references right
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
        vec![filter_expr],
    );
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filter referencing both sides should stay above the join
    match result {
        LogicalOperator::Filter(f) => {
            assert!(matches!(
                f.child.operator,
                LogicalOperator::Join(Join::Cross(_))
            ));
        }
        _ => panic!("Expected Filter above Cross product"),
    }
}

#[test]
fn test_or_join_filter_derives_domains_for_both_inputs() {
    let ctx = BindContext::new();
    let cross = CrossProduct::new(plan(&ctx, make_get(0)), plan(&ctx, make_get(1)));
    let branch = |left_value, right_value| {
        Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                make_comparison(
                    ComparisonType::Equal,
                    make_column_ref(0, 0),
                    make_constant(left_value),
                ),
                make_comparison(
                    ComparisonType::Equal,
                    make_column_ref(1, 0),
                    make_constant(right_value),
                ),
            ],
        ))
    };
    let predicate = Expression::Conjunction(ConjunctionExpression::new(
        ConjunctionType::Or,
        vec![branch(1, 2), branch(2, 1)],
    ));
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
        vec![predicate],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Filter(filter) = result else {
        panic!("correlated OR must remain above the join");
    };
    let LogicalOperator::Join(Join::Cross(cross)) = filter.child.operator else {
        panic!("expected cross product");
    };
    let LogicalOperator::Filter(left_filter) = cross.left.operator else {
        panic!("expected implied left domain");
    };
    let LogicalOperator::Filter(right_filter) = cross.right.operator else {
        panic!("expected implied right domain");
    };
    assert_eq!(left_filter.expressions.len(), 1);
    assert_eq!(right_filter.expressions.len(), 1);
    assert!(matches!(
        left_filter.expressions[0],
        Expression::Conjunction(ref conjunction)
            if conjunction.conjunction_type == ConjunctionType::Or
                && conjunction.children.len() == 2
    ));
    assert!(matches!(
        right_filter.expressions[0],
        Expression::Conjunction(ref conjunction)
            if conjunction.conjunction_type == ConjunctionType::Or
                && conjunction.children.len() == 2
    ));
}

#[test]
fn positive_mark_filter_lowers_mark_join_to_semi_join() {
    let ctx = BindContext::new();
    let left = LogicalOperator::Join(Join::Cross(CrossProduct::new(
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
    )));
    let mut join = ComparisonJoin::new(
        JoinType::Mark,
        plan(&ctx, left),
        plan(&ctx, make_get(2)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(2, 0),
            JoinComparisonType::Equal,
        )],
    );
    let mark_index = 90;
    join.mark_index = Some(mark_index);
    let marker = Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(mark_index, 0),
        LogicalType::Boolean,
    ));
    let left_predicate = make_comparison(
        ComparisonType::Equal,
        make_column_ref(0, 0),
        make_column_ref(1, 0),
    );
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![left_predicate, marker],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Join(Join::Comparison(join)) = result else {
        panic!("positive marker filter should become a SEMI join");
    };
    assert_eq!(join.join_type, JoinType::Semi);
    assert_eq!(join.mark_index, None);
    assert_eq!(join.mark_null_condition_start, None);
    assert!(matches!(join.left.operator, LogicalOperator::Filter(_)));
}

#[test]
fn negative_scalar_mark_filter_lowers_to_null_aware_anti_join() {
    let ctx = BindContext::new();
    let mut join = ComparisonJoin::new(
        JoinType::Mark,
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::Equal,
        )],
    );
    let mark_index = 90;
    join.mark_index = Some(mark_index);
    let marker = Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(mark_index, 0),
        LogicalType::Boolean,
    ));
    let not_marker = Expression::Operator(OperatorExpression::new_unary(
        OperatorType::Not,
        marker,
        LogicalType::Boolean,
    ));
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![not_marker],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Join(Join::Comparison(join)) = result else {
        panic!("negative scalar marker should become a null-aware ANTI join");
    };
    assert_eq!(join.join_type, JoinType::Anti);
    assert_eq!(join.anti_join_mode, AntiJoinMode::NullAware);
    assert_eq!(join.mark_index, None);
    assert_eq!(join.mark_null_condition_start, None);
}

#[test]
fn negative_marker_with_null_safe_condition_remains_mark_join() {
    let ctx = BindContext::new();
    let mut join = ComparisonJoin::new(
        JoinType::Mark,
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::NotDistinctFrom,
        )],
    );
    let mark_index = 90;
    join.mark_index = Some(mark_index);
    let marker = Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(mark_index, 0),
        LogicalType::Boolean,
    ));
    let not_marker = Expression::Operator(OperatorExpression::new_unary(
        OperatorType::Not,
        marker,
        LogicalType::Boolean,
    ));
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![not_marker],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Filter(filter) = result else {
        panic!("non-scalar negative marker must remain above the MARK join");
    };
    let LogicalOperator::Join(Join::Comparison(join)) = filter.child.operator else {
        panic!("expected MARK join below negative marker filter");
    };
    assert_eq!(join.join_type, JoinType::Mark);
    assert_eq!(join.anti_join_mode, AntiJoinMode::Regular);
}

#[test]
fn compound_mark_filter_preserves_three_valued_mark_semantics() {
    let ctx = BindContext::new();
    let mut join = ComparisonJoin::new(
        JoinType::Mark,
        plan(&ctx, make_get(0)),
        plan(&ctx, make_get(1)),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::Equal,
        )],
    );
    let mark_index = 90;
    join.mark_index = Some(mark_index);
    let marker = Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(mark_index, 0),
        LogicalType::Boolean,
    ));
    let marker_equals_true = Expression::Comparison(ComparisonExpression::new(
        ComparisonType::Equal,
        marker,
        Expression::Constant(ConstantExpression::new(
            paro_common::runtime_value::Value::Boolean(true),
            LogicalType::Boolean,
        )),
    ));
    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![marker_equals_true],
    );

    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    let LogicalOperator::Filter(filter) = result else {
        panic!("compound marker predicate must remain above the MARK join");
    };
    let LogicalOperator::Join(Join::Comparison(join)) = filter.child.operator else {
        panic!("expected MARK join below marker filter");
    };
    assert_eq!(join.join_type, JoinType::Mark);
}

#[test]
fn test_pushdown_through_order() {
    let ctx = BindContext::new();
    let get = make_get(0);
    let order = paro_planner::operator::Order {
        child: Box::new(plan(&ctx, get)),
        orders: vec![],
        projection_map: Vec::new(),
    };

    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0),
        make_constant(5),
    );
    let filter = PlannerFilter::new(plan(&ctx, LogicalOperator::Order(order)), vec![filter_expr]);
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filter should be pushed through Order
    match result {
        LogicalOperator::Order(o) => match o.child.operator {
            LogicalOperator::Filter(f) => {
                assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Filter under Order"),
        },
        _ => panic!("Expected Order operator"),
    }
}

#[test]
fn test_filter_stays_above_limit() {
    let ctx = BindContext::new();
    let get = make_get(0);
    let limit = paro_planner::operator::Limit::new(plan(&ctx, get), Some(make_constant(10)), None);

    let filter_expr = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0),
        make_constant(5),
    );
    let filter = PlannerFilter::new(plan(&ctx, LogicalOperator::Limit(limit)), vec![filter_expr]);
    let op = LogicalOperator::Filter(filter);

    let mut pushdown = FilterPushdown::new();
    let result = pushdown.rewrite(op);

    // Filtering before LIMIT can select replacement rows, so the predicate must stay above it.
    match result {
        LogicalOperator::Filter(filter) => {
            assert!(matches!(filter.child.operator, LogicalOperator::Limit(_)));
        }
        _ => panic!("Expected Filter above Limit"),
    }
}

#[test]
fn test_pushdown_preserves_delim_join_shape() {
    let ctx = BindContext::new();
    let filter = PlannerFilter::new(
        plan(&ctx, make_delim_join(&ctx, JoinType::Inner)),
        vec![make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        )],
    );
    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    match result {
        LogicalOperator::Join(Join::Comparison(join)) => {
            assert!(!join.duplicate_eliminated_columns.is_empty());
            assert!(matches!(join.left.operator, LogicalOperator::Filter(_)));
            assert!(!contains_empty_result(&join.right));
        }
        _ => panic!("expected delim comparison join"),
    }
}

#[test]
fn test_pushdown_rhs_conflict_materializes_empty_result_inside_delim_subtree() {
    let ctx = BindContext::new();
    let left = plan(&ctx, make_get(0));
    let rhs_base = LogicalOperator::Filter(PlannerFilter::new(
        plan(&ctx, make_get(1)),
        vec![make_comparison(
            ComparisonType::Equal,
            make_column_ref(1, 0),
            make_constant(1),
        )],
    ));
    let rhs = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        plan(&ctx, rhs_base),
        plan(
            &ctx,
            LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
        ),
        vec![JoinCondition::new(
            make_column_ref(1, 0),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(99, 0),
                LogicalType::Integer,
            )),
            JoinComparisonType::Equal,
        )],
    )));
    let mut join = ComparisonJoin::new(
        JoinType::Mark,
        left,
        plan(&ctx, rhs),
        vec![JoinCondition::new(
            make_column_ref(0, 0),
            make_column_ref(1, 0),
            JoinComparisonType::Equal,
        )],
    );
    join.duplicate_eliminated_columns = vec![make_column_ref(0, 0)];

    let filter = PlannerFilter::new(
        plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
        vec![make_comparison(
            ComparisonType::Equal,
            make_column_ref(1, 0),
            make_constant(2),
        )],
    );
    let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

    match result {
        LogicalOperator::Join(Join::Comparison(join)) => {
            assert!(contains_empty_result(&join.right));
        }
        _ => panic!("expected join with empty result in rhs subtree"),
    }
}

#[test]
fn test_split_predicates() {
    // Create: a > 5 AND b < 10
    let left = make_comparison(
        ComparisonType::GreaterThan,
        make_column_ref(0, 0),
        make_constant(5),
    );
    let right = make_comparison(
        ComparisonType::LessThan,
        make_column_ref(0, 1),
        make_constant(10),
    );
    let and_expr = Expression::Conjunction(ConjunctionExpression {
        conjunction_type: ConjunctionType::And,
        children: vec![left, right],
    });

    let predicates = FilterPushdown::split_predicates(and_expr);
    assert_eq!(predicates.len(), 2);
}

#[test]
fn test_get_expression_side() {
    let left_bindings: HashSet<usize> = [0].into_iter().collect();
    let right_bindings: HashSet<usize> = [1].into_iter().collect();

    // Expression referencing only left
    let left_expr = make_column_ref(0, 0);
    assert_eq!(
        FilterPushdown::get_expression_side(&left_expr, &left_bindings, &right_bindings),
        JoinSide::Left
    );

    // Expression referencing only right
    let right_expr = make_column_ref(1, 0);
    assert_eq!(
        FilterPushdown::get_expression_side(&right_expr, &left_bindings, &right_bindings),
        JoinSide::Right
    );

    // Expression referencing both
    let both_expr = make_comparison(
        ComparisonType::Equal,
        make_column_ref(0, 0),
        make_column_ref(1, 0),
    );
    assert_eq!(
        FilterPushdown::get_expression_side(&both_expr, &left_bindings, &right_bindings),
        JoinSide::Both
    );

    // Constant expression
    let const_expr = make_constant(5);
    assert_eq!(
        FilterPushdown::get_expression_side(&const_expr, &left_bindings, &right_bindings),
        JoinSide::None
    );
}
