// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free structural preconditions for planner transformations.

use super::*;

pub(super) fn matches_transformation(
    transformation: PlannerTransformation,
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    use LogicalOperatorType as Op;

    let Some(metadata) = state.metadata.get(&expr.payload) else {
        return false;
    };
    let operator = metadata.operator_type;
    match transformation {
        PlannerTransformation::ExpensivePredicatePlacement => {
            operator == Op::Filter
                && payload_operator(expr, state).is_some_and(|operator| {
                    matches!(operator, LogicalOperator::Filter(filter) if filter.expressions.len() > 1)
                })
        }
        PlannerTransformation::CteInline => operator == Op::MaterializedCTE,
        PlannerTransformation::AggregatePostReduction => {
            operator == Op::MaterializedCTE
                || (matches!(operator, Op::Projection | Op::Filter)
                    && canonical_subtree_operator_count(expr, memo, state, Op::Aggregate) >= 2)
        }
        PlannerTransformation::MarkJoinToSemi => {
            canonical_subtree_expression_any(expr, memo, |candidate| {
                is_positive_consumed_mark_filter(candidate, memo, state)
            })
        }
        PlannerTransformation::JoinElimination => {
            matches!(
                operator,
                Op::Projection | Op::Filter | Op::Aggregate | Op::Limit | Op::Order | Op::TopN
            ) && canonical_subtree_any(expr, memo, state, |candidate| {
                matches!(
                    candidate,
                    LogicalOperator::Join(Join::Comparison(join))
                        if matches!(join.join_type, JoinType::Left | JoinType::Right)
                )
            })
        }
        PlannerTransformation::AggregateJoinPreaggregation => {
            operator == Op::Aggregate && aggregate_over_unaggregated_join(expr, memo, state)
        }
        PlannerTransformation::AggregateJoinSubsumption => {
            operator == Op::Aggregate
                && canonical_subtree_contains(expr, memo, state, |candidate| {
                    candidate == Op::ComparisonJoin
                })
                && canonical_descendants_contain(expr, memo, state, |candidate| {
                    candidate == Op::Aggregate
                })
        }
        PlannerTransformation::AggregateNonNullInput => {
            operator == Op::Aggregate
                && payload_operator(expr, state).is_some_and(|operator| {
                    matches!(operator, LogicalOperator::Aggregate(aggregate)
                        if aggregate.aggregates.iter().any(|expression| {
                            matches!(
                                expression,
                                Expression::Aggregate(aggregate)
                                    if aggregate.children.len() == 1
                                        && aggregate.function.non_null_input_function().is_some()
                            ) || matches!(expression, Expression::Reference(_))
                        }))
                })
        }
        PlannerTransformation::AggregateInputMaterialization => {
            operator == Op::Aggregate
                && payload_operator(expr, state).is_some_and(|operator| {
                    matches!(operator, LogicalOperator::Aggregate(aggregate)
                        if aggregate.aggregates.iter().any(|expression| {
                            matches!(expression, Expression::Aggregate(aggregate)
                                if aggregate.children.iter().any(|child| !child.is_passive_value()))
                        }))
                })
        }
        PlannerTransformation::AggregateDimensionDeferral => {
            operator == Op::Aggregate && aggregate_over_dimension_join(expr, memo, state)
        }
        PlannerTransformation::TopNIntroduction => operator == Op::Limit,
        PlannerTransformation::LimitPushdown => {
            operator == Op::Limit
                && canonical_child_operator(expr, 0, memo, state) == Some(Op::Projection)
        }
        PlannerTransformation::LatePayloadFetch => {
            state.rowset_scan_pushdown
                && matches!(operator, Op::Projection | Op::Aggregate | Op::TopN)
                && canonical_subtree_has_late_payload_shape(expr, memo, state)
        }
        PlannerTransformation::ScalarAggregateWindow => {
            matches!(operator, Op::ComparisonJoin | Op::Projection | Op::Filter)
                && canonical_subtree_has_scalar_aggregate_join(expr, memo, state)
        }
    }
}

fn is_positive_consumed_mark_filter(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    let Some(LogicalOperator::Filter(filter)) = payload_operator(expr, state) else {
        return false;
    };
    let [Expression::ColumnRef(marker)] = filter.expressions.as_slice() else {
        return false;
    };
    let Some(child) = expr
        .key
        .children
        .first()
        .and_then(|child| canonical_expression(*child, memo))
    else {
        return false;
    };
    matches!(payload_operator(child, state),
    Some(LogicalOperator::Join(Join::Comparison(join)))
        if join.join_type == JoinType::Mark
            && marker.depth == 0
            && join.mark_index.is_some_and(|index| {
                marker.binding == ColumnBinding::new(index, 0)
            }))
}

fn canonical_subtree_has_late_payload_shape(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    canonical_subtree_any(expr, memo, state, |operator| match operator {
        LogicalOperator::TopN(_) => false,
        LogicalOperator::Projection(projection) => {
            projection.expressions.iter().any(|expression| {
                matches!(expression, Expression::Function(function)
                    if function.function.predicate_projection.is_some())
            })
        }
        _ => false,
    }) || canonical_subtree_has_topn_projection(expr, memo, state)
        || canonical_subtree_has_selective_projection(expr, memo, state)
}

fn canonical_subtree_has_topn_projection(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    canonical_subtree_expression_any(expr, memo, |candidate| {
        state
            .metadata
            .get(&candidate.payload)
            .is_some_and(|metadata| metadata.operator_type == LogicalOperatorType::TopN)
            && canonical_child_operator(candidate, 0, memo, state)
                == Some(LogicalOperatorType::Projection)
    })
}

fn canonical_subtree_has_selective_projection(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    canonical_subtree_expression_any(expr, memo, |candidate| {
        if state
            .metadata
            .get(&candidate.payload)
            .is_none_or(|metadata| metadata.operator_type != LogicalOperatorType::Projection)
        {
            return false;
        }
        let Some(child) = candidate
            .key
            .children
            .first()
            .and_then(|child| canonical_expression(*child, memo))
        else {
            return false;
        };
        canonical_subtree_contains(child, memo, state, |operator| {
            operator == LogicalOperatorType::Get
        }) && !canonical_subtree_contains(child, memo, state, |operator| {
            matches!(
                operator,
                LogicalOperatorType::ComparisonJoin
                    | LogicalOperatorType::AnyJoin
                    | LogicalOperatorType::CrossProduct
                    | LogicalOperatorType::Aggregate
            )
        })
    })
}

fn canonical_subtree_has_scalar_aggregate_join(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    canonical_subtree_expression_any(expr, memo, |candidate| {
        let Some(LogicalOperator::Join(Join::Comparison(join))) =
            payload_operator(candidate, state)
        else {
            return false;
        };
        if join.join_type != JoinType::Inner {
            return false;
        }
        let Some(right) = candidate
            .key
            .children
            .get(1)
            .and_then(|child| canonical_expression(*child, memo))
        else {
            return false;
        };
        canonical_subtree_operator_count(right, memo, state, LogicalOperatorType::Aggregate) >= 2
    })
}

fn payload_operator<'a>(
    expr: &crate::cascades::memo::LogicalExpr,
    state: &'a PlannerTransformState,
) -> Option<&'a LogicalOperator> {
    state
        .payloads
        .logical
        .get(expr.payload.index())
        .map(|payload| &payload.semantic_template.operator)
}

fn canonical_expression(
    group: GroupId,
    memo: &Memo,
) -> Option<&crate::cascades::memo::LogicalExpr> {
    let group = memo.canonical_group(group);
    let expression = memo.group(group)?.logical_exprs().first().copied()?;
    memo.logical_expr(expression)
}

fn canonical_child_operator(
    expr: &crate::cascades::memo::LogicalExpr,
    child: usize,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Option<LogicalOperatorType> {
    let child = canonical_expression(*expr.key.children.get(child)?, memo)?;
    state
        .metadata
        .get(&child.payload)
        .map(|metadata| metadata.operator_type)
}

fn aggregate_over_unaggregated_join(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    let Some(join) = expr
        .key
        .children
        .first()
        .and_then(|child| canonical_expression(*child, memo))
    else {
        return false;
    };
    state
        .metadata
        .get(&join.payload)
        .is_some_and(|metadata| metadata.operator_type == LogicalOperatorType::ComparisonJoin)
        && canonical_child_operator(join, 1, memo, state) != Some(LogicalOperatorType::Aggregate)
}

fn aggregate_over_dimension_join(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
) -> bool {
    let Some(mut candidate) = expr
        .key
        .children
        .first()
        .and_then(|child| canonical_expression(*child, memo))
    else {
        return false;
    };
    while state
        .metadata
        .get(&candidate.payload)
        .is_some_and(|metadata| metadata.operator_type == LogicalOperatorType::Projection)
    {
        let Some(next) = candidate
            .key
            .children
            .first()
            .and_then(|child| canonical_expression(*child, memo))
        else {
            return false;
        };
        candidate = next;
    }
    state
        .metadata
        .get(&candidate.payload)
        .is_some_and(|metadata| metadata.operator_type == LogicalOperatorType::ComparisonJoin)
        && canonical_child_operator(candidate, 1, memo, state) == Some(LogicalOperatorType::Get)
}

fn canonical_descendants_contain(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
    predicate: impl Fn(LogicalOperatorType) -> bool,
) -> bool {
    canonical_groups_contain(expr.key.children.iter().copied(), memo, state, predicate)
}

fn canonical_subtree_contains(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
    predicate: impl Fn(LogicalOperatorType) -> bool,
) -> bool {
    state
        .metadata
        .get(&expr.payload)
        .is_some_and(|metadata| predicate(metadata.operator_type))
        || canonical_groups_contain(expr.key.children.iter().copied(), memo, state, predicate)
}

fn canonical_groups_contain(
    roots: impl IntoIterator<Item = GroupId>,
    memo: &Memo,
    state: &PlannerTransformState,
    predicate: impl Fn(LogicalOperatorType) -> bool,
) -> bool {
    let mut pending = roots.into_iter().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(group) = pending.pop() {
        let group = memo.canonical_group(group);
        if !visited.insert(group) {
            continue;
        }
        let Some(expr) = canonical_expression(group, memo) else {
            continue;
        };
        if state
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| predicate(metadata.operator_type))
        {
            return true;
        }
        pending.extend(expr.key.children.iter().copied());
    }
    false
}

fn canonical_subtree_operator_count(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
    operator: LogicalOperatorType,
) -> usize {
    let mut count = usize::from(
        state
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.operator_type == operator),
    );
    let mut pending = expr.key.children.to_vec();
    let mut visited = BTreeSet::new();
    while let Some(group) = pending.pop() {
        let group = memo.canonical_group(group);
        if !visited.insert(group) {
            continue;
        }
        let Some(candidate) = canonical_expression(group, memo) else {
            continue;
        };
        count += usize::from(
            state
                .metadata
                .get(&candidate.payload)
                .is_some_and(|metadata| metadata.operator_type == operator),
        );
        pending.extend(candidate.key.children.iter().copied());
    }
    count
}

fn canonical_subtree_any(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    state: &PlannerTransformState,
    predicate: impl Fn(&LogicalOperator) -> bool,
) -> bool {
    canonical_subtree_expression_any(expr, memo, |candidate| {
        payload_operator(candidate, state).is_some_and(&predicate)
    })
}

fn canonical_subtree_expression_any(
    expr: &crate::cascades::memo::LogicalExpr,
    memo: &Memo,
    predicate: impl Fn(&crate::cascades::memo::LogicalExpr) -> bool,
) -> bool {
    if predicate(expr) {
        return true;
    }
    let mut pending = expr.key.children.to_vec();
    let mut visited = BTreeSet::new();
    while let Some(group) = pending.pop() {
        let group = memo.canonical_group(group);
        if !visited.insert(group) {
            continue;
        }
        let Some(candidate) = canonical_expression(group, memo) else {
            continue;
        };
        if predicate(candidate) {
            return true;
        }
        pending.extend(candidate.key.children.iter().copied());
    }
    false
}
