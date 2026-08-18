// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fold a scalar aggregate over an alpha-equivalent input into a grouped aggregate.
//!
//! SQL scalar subqueries are deliberately planned with an explicit
//! `FIRST + COUNT + ErrorIfMultipleRows` wrapper.  For analytical HAVING
//! clauses such as TPC-H Q11 this produces two complete executions of the
//! same input: one grouped `SUM`, and one ungrouped `SUM` used to derive a
//! threshold, which can instead be reduced once from finalized group values.
//!
//! This rewrite is intentionally proof-driven.  Base scans are paired by
//! stable catalog object identity and physical column ids, and only
//! deterministic projections, filters, and clean inner equality joins are
//! admitted between them. The optimizer requires an advertised partial merge; it never infers it
//! name or from a SQL return type.

use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_function::aggregate::distributive::minmax::get_max_function;
use paro_function::aggregate::AggregateAlgebra;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType, ReferenceExpression,
};
use paro_planner::operator::{
    Aggregate, AntiJoinMode, ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinType,
    LogicalOperator, MarkJoinSemantics, MaterializedCTE, PostAggregateReduction, Projection,
};
use paro_planner::plan::LogicalPlan;

pub(crate) mod alpha;

use crate::aggregate::semantic_kernels::aggregate_kernels_equal;
use alpha::AlphaBindings;
/// Replace eligible grouped/scalar sibling plans with one grouped aggregate
/// carrying a hidden post-aggregate reduction.
pub fn optimize_plan(plan: LogicalPlan, bind_context: &BindContext) -> LogicalPlan {
    let cte_rewrite = match &plan.operator {
        LogicalOperator::MaterializedCTE(cte) => recognize_cte_max_reduction(cte, bind_context),
        _ => None,
    };
    if let Some(rewrite) = cte_rewrite {
        // Recognition and mutation stay separately defensive. Preserve a
        // binding-identical fallback so future plan-shape drift declines
        // without turning an optimizer opportunity into a query failure.
        let fallback = paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
            &plan,
            bind_context.shared().as_ref(),
        );
        let LogicalOperator::MaterializedCTE(cte) = plan.operator else {
            return fallback;
        };
        return rewrite_cte_max_reduction(cte, rewrite)
            .map(|rewritten| optimize_plan(rewritten, bind_context))
            .unwrap_or(fallback);
    }
    let plan = plan.map_children(|child| optimize_plan(child, bind_context));
    rewrite_projection(plan, bind_context)
}

fn rewrite_cte_max_reduction(cte: MaterializedCTE, rewrite: CteMaxRewrite) -> Option<LogicalPlan> {
    let grouped_plan = attach_cte_reduction(*cte.cte_query, rewrite.reduction)?;
    let rewritten_child = rewrite_cte_consumer(
        *cte.child,
        cte.cte_index,
        rewrite.main_table_index,
        rewrite.wrapper_binding,
        grouped_plan,
    )?;
    Some(rewritten_child)
}

struct CteMaxRewrite {
    main_table_index: usize,
    wrapper_binding: ColumnBinding,
    reduction: PostAggregateReduction,
}

fn recognize_cte_max_reduction(
    cte: &MaterializedCTE,
    bind_context: &BindContext,
) -> Option<CteMaxRewrite> {
    use paro_planner::binder::ir::CTEMaterialize;

    if cte.materialized != CTEMaterialize::Default
        || cte.ref_count != 2
        || cte.column_names.len() != cte.column_types.len()
    {
        return None;
    }
    let (definition_projection, grouped) = cte_grouped_definition(&cte.cte_query)?;
    if grouped.post_reduction.is_some()
        || !plain_grouped_aggregate(grouped)
        || definition_projection.expressions.len() != grouped.returned_types.len()
        || definition_projection.returned_types != cte.column_types
    {
        return None;
    }
    let grouped_sum = plain_sum(grouped.aggregates.first()?)?;
    if grouped_sum.function.algebra != Some(AggregateAlgebra::Sum)
        || !definition_projection.expressions.iter().all(|expression| {
            matches!(expression, Expression::ColumnRef(column)
            if column.depth == 0
                && grouped
                    .get_column_bindings()
                    .iter()
                    .position(|binding| *binding == column.binding)
                    .is_some_and(|ordinal| {
                        grouped.returned_types.get(ordinal) == Some(&column.return_type)
                    }))
        })
        || !cte_source_is_shareable(&grouped.child)
    {
        return None;
    }
    let references = collect_cte_references(&cte.child, cte.cte_index);
    if references.len() != 2 {
        return None;
    }
    let scalar = find_cte_scalar_max(&cte.child, cte.cte_index)?;
    let main = references
        .into_iter()
        .find(|reference| reference.table_index != scalar.cte_table_index)?;
    if main.column_types != cte.column_types
        || main.table_index == scalar.cte_table_index
        || count_binding_uses(&cte.child, scalar.wrapper_binding)? != 1
    {
        return None;
    }
    let main_value_ordinal =
        map_definition_aggregate_output(definition_projection, grouped, scalar.cte_value_ordinal)?;
    let grouped_value_type = grouped.aggregates.get(main_value_ordinal)?.return_type();
    if grouped_value_type != scalar.reducer.return_type()
        || scalar.scalar_expression.return_type() != scalar.wrapper_type
    {
        return None;
    }
    let reduction_index = bind_context.generate_table_index();
    let predicate = find_and_rebase_cte_predicate(
        &cte.child,
        ColumnBinding::new(main.table_index, scalar.cte_value_ordinal),
        scalar.wrapper_binding,
        ColumnBinding::new(grouped.aggregate_index, main_value_ordinal),
        ColumnBinding::new(reduction_index, 0),
        &grouped_value_type,
    )?;
    let reducer = scalar.reducer.clone().replace_column_ref(&|column| {
        (column.binding == ColumnBinding::new(scalar.cte_table_index, scalar.cte_value_ordinal))
            .then(|| {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(grouped.aggregate_index, main_value_ordinal),
                    grouped_value_type.clone(),
                ))
            })
    });
    let scalar_expression = rebase_scalar_expression(
        scalar.scalar_expression,
        scalar.aggregate_index,
        scalar.reducer.return_type(),
    )?;

    Some(CteMaxRewrite {
        main_table_index: main.table_index,
        wrapper_binding: scalar.wrapper_binding,
        reduction: PostAggregateReduction {
            reduction_index,
            reducers: vec![reducer],
            scalar_expressions: vec![scalar_expression],
            predicate,
        },
    })
}

fn cte_grouped_definition(plan: &LogicalPlan) -> Option<(&Projection, &Aggregate)> {
    let LogicalOperator::Projection(projection) = &plan.operator else {
        return None;
    };
    let LogicalOperator::Aggregate(aggregate) = &projection.child.operator else {
        return None;
    };
    Some((projection, aggregate))
}

fn cte_source_is_shareable(plan: &LogicalPlan) -> bool {
    match &plan.operator {
        LogicalOperator::Get(get) => {
            get.scan_order.is_none()
                && get.runtime_filter_expressions.iter().all(is_movable)
                && get.table.is_some()
        }
        LogicalOperator::Filter(filter) => {
            filter
                .projection_map
                .is_identity(filter.child.types().len())
                && filter.expressions.iter().all(is_movable)
                && cte_source_is_shareable(&filter.child)
        }
        LogicalOperator::Projection(projection) => {
            projection.expressions.iter().all(is_movable)
                && cte_source_is_shareable(&projection.child)
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            clean_inner_join(join)
                && cte_source_is_shareable(&join.left)
                && cte_source_is_shareable(&join.right)
        }
        _ => false,
    }
}

fn map_definition_aggregate_output(
    projection: &Projection,
    aggregate: &Aggregate,
    cte_ordinal: usize,
) -> Option<usize> {
    let Expression::ColumnRef(column) = projection.expressions.get(cte_ordinal)? else {
        return None;
    };
    if column.depth != 0 || column.binding.table_index != aggregate.aggregate_index {
        return None;
    }
    let aggregate_expression = aggregate.aggregates.get(column.binding.column_index)?;
    (column.return_type == aggregate_expression.return_type())
        .then_some(column.binding.column_index)
}

fn collect_cte_references<'a>(
    plan: &'a LogicalPlan,
    cte_index: usize,
) -> Vec<&'a paro_planner::operator::CTERef> {
    let mut references = Vec::new();
    if let LogicalOperator::CTERef(reference) = &plan.operator {
        if reference.cte_index == cte_index {
            references.push(reference);
        }
    }
    for child in plan.children() {
        references.extend(collect_cte_references(child, cte_index));
    }
    references
}

struct CteScalarMax<'a> {
    cte_table_index: usize,
    cte_value_ordinal: usize,
    wrapper_binding: ColumnBinding,
    wrapper_type: LogicalType,
    aggregate_index: usize,
    reducer: &'a Expression,
    scalar_expression: &'a Expression,
}

fn find_cte_scalar_max<'a>(plan: &'a LogicalPlan, cte_index: usize) -> Option<CteScalarMax<'a>> {
    let mut found = None;
    find_cte_scalar_max_inner(plan, cte_index, &mut found)?;
    found
}

fn find_cte_scalar_max_inner<'a>(
    plan: &'a LogicalPlan,
    cte_index: usize,
    found: &mut Option<CteScalarMax<'a>>,
) -> Option<()> {
    if let Some(candidate) = peel_cte_scalar_max(plan, cte_index) {
        if found.is_some() {
            return None;
        }
        *found = Some(candidate);
        return Some(());
    }
    for child in plan.children() {
        find_cte_scalar_max_inner(child, cte_index, found)?;
    }
    Some(())
}

fn peel_cte_scalar_max(plan: &LogicalPlan, cte_index: usize) -> Option<CteScalarMax<'_>> {
    let LogicalOperator::Projection(wrapper_projection) = &plan.operator else {
        return None;
    };
    let wrapper = peel_scalar_wrapper_prefix(wrapper_projection)?;
    let LogicalOperator::Projection(scalar_projection) = &wrapper.aggregate.child.operator else {
        return None;
    };
    if scalar_projection.expressions.len() != 1
        || scalar_projection.returned_types.len() != 1
        || scalar_projection.visible_names.len() != 1
    {
        return None;
    }
    let scalar_expression = &scalar_projection.expressions[0];
    let LogicalOperator::Aggregate(reduction) = &scalar_projection.child.operator else {
        return None;
    };
    if !plain_ungrouped_aggregate(reduction) {
        return None;
    }
    let reducer = reduction.aggregates.first()?;
    let Expression::Aggregate(max) = reducer else {
        return None;
    };
    if max.aggr_type != AggregateType::NonDistinct
        || max.filter.is_some()
        || !max.order_bys.is_empty()
        || max.children.len() != 1
        || max.function.destructor.is_some()
        || !is_movable(reducer)
    {
        return None;
    }
    let Expression::ColumnRef(value) = &max.children[0] else {
        return None;
    };
    let LogicalOperator::CTERef(reference) = &reduction.child.operator else {
        return None;
    };
    if reference.cte_index != cte_index
        || reference.column_names.len() != reference.column_types.len()
        || value.depth != 0
        || value.binding.table_index != reference.table_index
        || reference.column_types.get(value.binding.column_index) != Some(&value.return_type)
        || max.function.arguments.as_slice() != [value.return_type.clone()]
        || max.function.return_type != max.return_type
    {
        return None;
    }
    let (canonical_max, targets) = get_max_function()
        .bind(std::slice::from_ref(&value.return_type))
        .ok()?;
    if targets.as_slice() != [value.return_type.clone()]
        || !aggregate_kernels_equal(
            max,
            &AggregateExpression::new(canonical_max, Vec::new(), max.return_type.clone()),
        )
    {
        return None;
    }
    if !is_movable(scalar_expression)
        || !expression_uses_only_column(
            scalar_expression,
            ColumnBinding::new(reduction.aggregate_index, 0),
            &max.return_type,
        )
        || scalar_expression.return_type() != wrapper.wrapper_type
    {
        return None;
    }

    Some(CteScalarMax {
        cte_table_index: reference.table_index,
        cte_value_ordinal: value.binding.column_index,
        wrapper_binding: wrapper.wrapper_binding,
        wrapper_type: wrapper.wrapper_type,
        aggregate_index: reduction.aggregate_index,
        reducer,
        scalar_expression,
    })
}

struct ScalarWrapperPrefix<'a> {
    wrapper_binding: ColumnBinding,
    wrapper_type: LogicalType,
    aggregate: &'a Aggregate,
}

fn peel_scalar_wrapper_prefix(projection: &Projection) -> Option<ScalarWrapperPrefix<'_>> {
    if projection.expressions.len() != 1
        || projection.returned_types.len() != 1
        || projection.visible_names.len() != 1
    {
        return None;
    }
    let Expression::Operator(checked) = &projection.expressions[0] else {
        return None;
    };
    if checked.operator_type != OperatorType::ErrorIfMultipleRows || checked.children.len() != 2 {
        return None;
    }
    let LogicalOperator::Aggregate(wrapper) = &projection.child.operator else {
        return None;
    };
    if !wrapper.groups.is_empty()
        || !wrapper.grouping_sets.is_empty()
        || !wrapper.grouping_functions.is_empty()
        || wrapper.aggregates.len() != 2
        || wrapper.post_reduction.is_some()
    {
        return None;
    }
    let Expression::Aggregate(first) = &wrapper.aggregates[0] else {
        return None;
    };
    let Expression::Aggregate(count) = &wrapper.aggregates[1] else {
        return None;
    };
    let [Expression::ColumnRef(first_result), Expression::ColumnRef(count_result)] =
        checked.children.as_slice()
    else {
        return None;
    };
    let (canonical_first, _) = get_first_function()
        .bind(std::slice::from_ref(&first.children.first()?.return_type()))
        .ok()?;
    if first.aggr_type != AggregateType::NonDistinct
        || first.filter.is_some()
        || !first.order_bys.is_empty()
        || first.children.len() != 1
        || count.aggr_type != AggregateType::NonDistinct
        || count.filter.is_some()
        || !count.order_bys.is_empty()
        || !count.children.is_empty()
        || !aggregate_kernels_equal(
            first,
            &AggregateExpression::new(canonical_first, Vec::new(), first.return_type.clone()),
        )
        || !aggregate_kernels_equal(
            count,
            &AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt),
        )
        || !is_column(
            first_result,
            ColumnBinding::new(wrapper.aggregate_index, 0),
            &first.return_type,
        )
        || !is_column(
            count_result,
            ColumnBinding::new(wrapper.aggregate_index, 1),
            &LogicalType::BigInt,
        )
        || checked.return_type != first.return_type
    {
        return None;
    }
    Some(ScalarWrapperPrefix {
        wrapper_binding: ColumnBinding::new(projection.table_index, 0),
        wrapper_type: checked.return_type.clone(),
        aggregate: wrapper,
    })
}

fn find_and_rebase_cte_predicate(
    plan: &LogicalPlan,
    main_binding: ColumnBinding,
    wrapper_binding: ColumnBinding,
    aggregate_binding: ColumnBinding,
    reduction_binding: ColumnBinding,
    value_type: &LogicalType,
) -> Option<Expression> {
    let mut found = None;
    find_cte_predicate_inner(
        plan,
        main_binding,
        wrapper_binding,
        aggregate_binding,
        reduction_binding,
        value_type,
        &mut found,
    )?;
    found
}

fn find_cte_predicate_inner(
    plan: &LogicalPlan,
    main_binding: ColumnBinding,
    wrapper_binding: ColumnBinding,
    aggregate_binding: ColumnBinding,
    reduction_binding: ColumnBinding,
    value_type: &LogicalType,
    found: &mut Option<Expression>,
) -> Option<()> {
    if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
        for condition in &join.conditions {
            if condition.comparison != JoinComparisonType::Equal {
                continue;
            }
            let matches = |left: &Expression, right: &Expression| {
                matches!(left, Expression::ColumnRef(column)
                    if is_column(column, main_binding, value_type))
                    && matches!(right, Expression::ColumnRef(column)
                        if is_column(column, wrapper_binding, value_type))
            };
            if matches(&condition.left, &condition.right)
                || matches(&condition.right, &condition.left)
            {
                if found.is_some() {
                    return None;
                }
                *found = Some(Expression::Comparison(
                    paro_planner::expression::ComparisonExpression::new(
                        paro_planner::expression::ComparisonType::Equal,
                        Expression::ColumnRef(ColumnRefExpression::new(
                            aggregate_binding,
                            value_type.clone(),
                        )),
                        Expression::ColumnRef(ColumnRefExpression::new(
                            reduction_binding,
                            value_type.clone(),
                        )),
                    ),
                ));
            }
        }
    }
    for child in plan.children() {
        find_cte_predicate_inner(
            child,
            main_binding,
            wrapper_binding,
            aggregate_binding,
            reduction_binding,
            value_type,
            found,
        )?;
    }
    Some(())
}

fn attach_cte_reduction(
    mut plan: LogicalPlan,
    reduction: PostAggregateReduction,
) -> Option<LogicalPlan> {
    let LogicalOperator::Projection(projection) = &mut plan.operator else {
        return None;
    };
    let LogicalOperator::Aggregate(aggregate) = &mut projection.child.operator else {
        return None;
    };
    aggregate.post_reduction = Some(reduction);
    aggregate.verify_post_reduction().ok()?;
    Some(plan)
}

fn rewrite_cte_consumer(
    mut plan: LogicalPlan,
    cte_index: usize,
    main_table_index: usize,
    wrapper_binding: ColumnBinding,
    grouped_plan: LogicalPlan,
) -> Option<LogicalPlan> {
    let scalar_root_id = find_scalar_wrapper_id(&plan, cte_index)?;
    if !remove_scalar_join(&mut plan, scalar_root_id, wrapper_binding) {
        return None;
    }
    let mut grouped_plan = Some(grouped_plan);
    if !replace_main_cte_ref(&mut plan, cte_index, main_table_index, &mut grouped_plan)
        || grouped_plan.is_some()
    {
        return None;
    }
    Some(plan)
}

fn find_scalar_wrapper_id(
    plan: &LogicalPlan,
    cte_index: usize,
) -> Option<paro_planner::plan::PlanNodeId> {
    let mut ids = Vec::new();
    if peel_cte_scalar_max(plan, cte_index).is_some() {
        ids.push(plan.id);
    }
    for child in plan.children() {
        if let Some(id) = find_scalar_wrapper_id(child, cte_index) {
            ids.push(id);
        }
    }
    (ids.len() == 1).then(|| ids[0])
}

fn remove_scalar_join(
    plan: &mut LogicalPlan,
    scalar_root_id: paro_planner::plan::PlanNodeId,
    wrapper_binding: ColumnBinding,
) -> bool {
    let mut replacement = None;
    if let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator {
        let left_scalar = join.left.id == scalar_root_id;
        let right_scalar = join.right.id == scalar_root_id;
        if left_scalar ^ right_scalar
            && join.join_type == JoinType::Inner
            && join.anti_join_mode == AntiJoinMode::Regular
            && join.mark_index.is_none()
            && join.mark_semantics == MarkJoinSemantics::NotMark
            && join.duplicate_eliminated_columns.is_empty()
            && !join.delim_flipped
            && join
                .left_projection_map
                .is_identity(join.left.types().len())
            && join
                .right_projection_map
                .is_identity(join.right.types().len())
        {
            let scalar_condition_count = join
                .conditions
                .iter()
                .filter(|condition| {
                    expression_mentions_binding(&condition.left, wrapper_binding)
                        || expression_mentions_binding(&condition.right, wrapper_binding)
                })
                .count();
            if scalar_condition_count == 1 {
                join.conditions.retain(|condition| {
                    !expression_mentions_binding(&condition.left, wrapper_binding)
                        && !expression_mentions_binding(&condition.right, wrapper_binding)
                });
                if join.conditions.is_empty() {
                    replacement = Some(if left_scalar {
                        std::mem::replace(
                            &mut *join.right,
                            LogicalPlan::synthetic(LogicalOperator::DummyScan),
                        )
                    } else {
                        std::mem::replace(
                            &mut *join.left,
                            LogicalPlan::synthetic(LogicalOperator::DummyScan),
                        )
                    });
                }
            }
        }
    }
    if let Some(replacement) = replacement {
        *plan = replacement;
        return true;
    }
    let mut removed = false;
    let _ = plan.visit_children_mut(|child| {
        if remove_scalar_join(child, scalar_root_id, wrapper_binding) {
            removed = true;
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    });
    removed
}

fn expression_mentions_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if matches!(node, Expression::ColumnRef(column) if column.binding == binding) {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

fn count_binding_uses(plan: &LogicalPlan, binding: ColumnBinding) -> Option<usize> {
    let mut count_expression = |expression: &Expression| {
        let mut count = 0;
        ExpressionIterator::visit(expression, &mut |node| {
            if matches!(node, Expression::ColumnRef(column) if column.binding == binding) {
                count += 1;
                ExpressionVisitDecision::SkipChildren
            } else {
                ExpressionVisitDecision::Descend
            }
        });
        count
    };
    let local = match &plan.operator {
        LogicalOperator::Get(_) | LogicalOperator::CTERef(_) => 0,
        LogicalOperator::Filter(filter) => {
            filter.expressions.iter().map(&mut count_expression).sum()
        }
        LogicalOperator::Projection(projection) => projection
            .expressions
            .iter()
            .map(&mut count_expression)
            .sum(),
        LogicalOperator::Order(order) => order
            .orders
            .iter()
            .map(|order| count_expression(&order.expression))
            .sum(),
        LogicalOperator::Join(Join::Comparison(join)) => join
            .conditions
            .iter()
            .map(|condition| count_expression(&condition.left) + count_expression(&condition.right))
            .sum(),
        LogicalOperator::Join(Join::Cross(_)) => 0,
        LogicalOperator::Aggregate(aggregate) => aggregate
            .groups
            .iter()
            .chain(&aggregate.aggregates)
            .map(&mut count_expression)
            .sum(),
        _ => return None,
    };
    plan.children().into_iter().try_fold(local, |count, child| {
        count_binding_uses(child, binding).map(|child_count| count + child_count)
    })
}

fn replace_main_cte_ref(
    plan: &mut LogicalPlan,
    cte_index: usize,
    main_table_index: usize,
    grouped_plan: &mut Option<LogicalPlan>,
) -> bool {
    if matches!(&plan.operator, LogicalOperator::CTERef(reference)
        if reference.cte_index == cte_index && reference.table_index == main_table_index)
    {
        let Some(definition) = grouped_plan.take() else {
            return false;
        };
        let bindings = definition.get_column_bindings();
        let types = definition.types();
        *plan = LogicalPlan {
            id: plan.id,
            stats: Default::default(),
            operator: LogicalOperator::Projection(Projection::new(
                main_table_index,
                definition,
                bindings
                    .into_iter()
                    .zip(types)
                    .map(|(binding, ty)| {
                        Expression::ColumnRef(ColumnRefExpression::new(binding, ty))
                    })
                    .collect(),
            )),
        };
        return true;
    }
    let mut replaced = false;
    let _ = plan.visit_children_mut(|child| {
        if replace_main_cte_ref(child, cte_index, main_table_index, grouped_plan) {
            replaced = true;
        }
        std::ops::ControlFlow::Continue(())
    });
    replaced
}

#[derive(Clone, Copy)]
enum GroupedSide {
    Left,
    Right,
}

struct Rewrite {
    grouped_side: GroupedSide,
    reduction: PostAggregateReduction,
}

fn rewrite_projection(plan: LogicalPlan, bind_context: &BindContext) -> LogicalPlan {
    let Some(rewrite) = recognize(&plan, bind_context) else {
        return plan;
    };

    // Recognition and mutation remain separately defensive.  If another
    // rewrite changes the accepted shape in the future, an optimizer pass
    // must decline rather than turn that mismatch into a query-level panic.
    let mut plan = plan;
    let output = match &mut plan.operator {
        LogicalOperator::Projection(output) => output,
        _ => return plan,
    };
    let grouped_slot = match &mut output.child.operator {
        LogicalOperator::Filter(filter) => match &mut filter.child.operator {
            LogicalOperator::Join(Join::Cross(cross)) => match rewrite.grouped_side {
                GroupedSide::Left => &mut cross.left,
                GroupedSide::Right => &mut cross.right,
            },
            _ => return plan,
        },
        _ => return plan,
    };
    if !matches!(grouped_slot.operator, LogicalOperator::Aggregate(_)) {
        return plan;
    }
    let mut grouped_plan = std::mem::replace(
        grouped_slot,
        Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    let LogicalOperator::Aggregate(grouped) = &mut grouped_plan.operator else {
        *grouped_slot = grouped_plan;
        return plan;
    };
    grouped.post_reduction = Some(rewrite.reduction);
    output.child = grouped_plan;
    plan
}

fn recognize(plan: &LogicalPlan, bind_context: &BindContext) -> Option<Rewrite> {
    let LogicalOperator::Projection(output) = &plan.operator else {
        return None;
    };
    if output.expressions.is_empty()
        || output.returned_types.len() != output.expressions.len()
        || output.visible_names.len() != output.expressions.len()
    {
        return None;
    }
    let LogicalOperator::Filter(filter) = &output.child.operator else {
        return None;
    };
    if filter.expressions.len() != 1
        || !filter
            .projection_map
            .is_identity(filter.child.types().len())
    {
        return None;
    }
    let predicate = &filter.expressions[0];
    if !matches!(predicate, Expression::Comparison(_)) || !is_movable(predicate) {
        return None;
    }

    let LogicalOperator::Join(Join::Cross(cross)) = &filter.child.operator else {
        return None;
    };

    let rewrite = recognize_orientation(
        cross.left.as_ref(),
        cross.right.as_ref(),
        predicate,
        GroupedSide::Left,
        bind_context,
    )
    .or_else(|| {
        recognize_orientation(
            cross.right.as_ref(),
            cross.left.as_ref(),
            predicate,
            GroupedSide::Right,
            bind_context,
        )
    })?;
    let grouped = match rewrite.grouped_side {
        GroupedSide::Left => cross.left.as_ref(),
        GroupedSide::Right => cross.right.as_ref(),
    };
    projection_consumes_only(output, grouped).then_some(rewrite)
}

fn projection_consumes_only(projection: &Projection, source: &LogicalPlan) -> bool {
    let available = source
        .get_column_bindings()
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    projection.expressions.iter().all(|expression| {
        if !is_movable(expression) {
            return false;
        }
        let mut valid = true;
        ExpressionIterator::visit(expression, &mut |node| match node {
            Expression::ColumnRef(column) => {
                valid &= column.depth == 0 && available.contains(&column.binding);
                ExpressionVisitDecision::SkipChildren
            }
            Expression::Aggregate(_)
            | Expression::Reference(_)
            | Expression::Subquery(_)
            | Expression::Window(_) => {
                valid = false;
                ExpressionVisitDecision::SkipChildren
            }
            _ => ExpressionVisitDecision::Descend,
        });
        valid
    })
}

fn recognize_orientation(
    grouped_plan: &LogicalPlan,
    scalar_wrapper_plan: &LogicalPlan,
    predicate: &Expression,
    grouped_side: GroupedSide,
    bind_context: &BindContext,
) -> Option<Rewrite> {
    let LogicalOperator::Aggregate(grouped) = &grouped_plan.operator else {
        return None;
    };
    if !plain_grouped_aggregate(grouped) {
        return None;
    }
    let grouped_sum = plain_sum(grouped.aggregates.first()?)?;

    let scalar = peel_scalar_wrapper(scalar_wrapper_plan)?;
    let scalar_sum = plain_sum(scalar.aggregate.aggregates.first()?)?;
    if scalar.scalar_expression.return_type() != scalar.wrapper_type {
        return None;
    }

    let bindings = AlphaBindings::match_sources(&grouped.child, &scalar.aggregate.child)?;
    if !bindings.expressions_equal(
        grouped.aggregates.first()?,
        scalar.aggregate.aggregates.first()?,
    ) {
        return None;
    }
    // Keep the explicit checks close to the algebraic operation even though
    // expression equality above also compares these fields.
    if !aggregate_kernels_equal(grouped_sum, scalar_sum)
        || grouped_sum.return_type != scalar_sum.return_type
        || grouped_sum.function.algebra != Some(AggregateAlgebra::Sum)
    {
        return None;
    }

    let merge = grouped_sum.function.partial_merge_function()?;
    if merge.arguments.as_slice() != [grouped_sum.return_type.clone()]
        || merge.return_type != grouped_sum.return_type
        || merge.destructor.is_some()
    {
        return None;
    }

    let scalar_expression = rebase_scalar_expression(
        scalar.scalar_expression,
        scalar.aggregate.aggregate_index,
        grouped_sum.return_type.clone(),
    )?;

    let reduction_index = bind_context.generate_table_index();
    let predicate = rebase_predicate(
        predicate,
        grouped.aggregate_index,
        scalar.wrapper_binding,
        reduction_index,
        scalar.wrapper_type,
    )?;
    let reducer = Expression::Aggregate(AggregateExpression::new(
        merge,
        vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(grouped.aggregate_index, 0),
            grouped_sum.return_type.clone(),
        ))],
        grouped_sum.return_type.clone(),
    ));

    Some(Rewrite {
        grouped_side,
        reduction: PostAggregateReduction {
            reduction_index,
            reducers: vec![reducer],
            scalar_expressions: vec![scalar_expression],
            predicate,
        },
    })
}

fn plain_grouped_aggregate(aggregate: &Aggregate) -> bool {
    !aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
        && aggregate.groups.iter().all(is_movable)
}

fn plain_ungrouped_aggregate(aggregate: &Aggregate) -> bool {
    aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
}

fn plain_sum(expression: &Expression) -> Option<&AggregateExpression> {
    let Expression::Aggregate(aggregate) = expression else {
        return None;
    };
    (aggregate.aggr_type == AggregateType::NonDistinct
        && aggregate.filter.is_none()
        && aggregate.order_bys.is_empty()
        && aggregate.children.len() == 1
        && aggregate.function.algebra == Some(AggregateAlgebra::Sum)
        && aggregate.children.iter().all(is_movable))
    .then_some(aggregate)
}

struct ScalarBranch<'a> {
    wrapper_binding: ColumnBinding,
    wrapper_type: LogicalType,
    scalar_expression: &'a Expression,
    aggregate: &'a Aggregate,
}

fn peel_scalar_wrapper(plan: &LogicalPlan) -> Option<ScalarBranch<'_>> {
    let LogicalOperator::Projection(wrapper_projection) = &plan.operator else {
        return None;
    };
    if wrapper_projection.expressions.len() != 1
        || wrapper_projection.returned_types.len() != 1
        || wrapper_projection.visible_names.len() != 1
    {
        return None;
    }
    let Expression::Operator(checked) = &wrapper_projection.expressions[0] else {
        return None;
    };
    if checked.operator_type != OperatorType::ErrorIfMultipleRows || checked.children.len() != 2 {
        return None;
    }

    let LogicalOperator::Aggregate(wrapper) = &wrapper_projection.child.operator else {
        return None;
    };
    if !wrapper.groups.is_empty()
        || !wrapper.grouping_sets.is_empty()
        || !wrapper.grouping_functions.is_empty()
        || wrapper.aggregates.len() != 2
        || wrapper.post_reduction.is_some()
    {
        return None;
    }
    let Expression::Aggregate(first) = &wrapper.aggregates[0] else {
        return None;
    };
    let Expression::Aggregate(count) = &wrapper.aggregates[1] else {
        return None;
    };
    if first.aggr_type != AggregateType::NonDistinct
        || first.filter.is_some()
        || !first.order_bys.is_empty()
        || first.children.len() != 1
        || !count.function.arguments.is_empty()
        || count.return_type != LogicalType::BigInt
        || count.aggr_type != AggregateType::NonDistinct
        || count.filter.is_some()
        || !count.order_bys.is_empty()
        || !count.children.is_empty()
    {
        return None;
    }
    let (canonical_first, _) = get_first_function()
        .bind(std::slice::from_ref(&first.children[0].return_type()))
        .ok()?;
    let canonical_first =
        AggregateExpression::new(canonical_first, Vec::new(), first.return_type.clone());
    let canonical_count =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    if !aggregate_kernels_equal(first, &canonical_first)
        || !aggregate_kernels_equal(count, &canonical_count)
    {
        return None;
    }

    let [Expression::ColumnRef(first_result), Expression::ColumnRef(count_result)] =
        checked.children.as_slice()
    else {
        return None;
    };
    if !is_column(
        first_result,
        ColumnBinding::new(wrapper.aggregate_index, 0),
        &first.return_type,
    ) || !is_column(
        count_result,
        ColumnBinding::new(wrapper.aggregate_index, 1),
        &LogicalType::BigInt,
    ) || checked.return_type != first.return_type
    {
        return None;
    }

    let LogicalOperator::Projection(scalar_projection) = &wrapper.child.operator else {
        return None;
    };
    if scalar_projection.expressions.len() != 1
        || scalar_projection.returned_types.len() != 1
        || scalar_projection.visible_names.len() != 1
    {
        return None;
    }
    let scalar_expression = &scalar_projection.expressions[0];
    if !is_movable(scalar_expression) || scalar_expression.return_type() != first.return_type {
        return None;
    }

    let Expression::ColumnRef(first_input) = &first.children[0] else {
        return None;
    };
    if !is_column(
        first_input,
        ColumnBinding::new(scalar_projection.table_index, 0),
        &scalar_expression.return_type(),
    ) {
        return None;
    }

    let LogicalOperator::Aggregate(aggregate) = &scalar_projection.child.operator else {
        return None;
    };
    if !plain_ungrouped_aggregate(aggregate) {
        return None;
    }
    let scalar_sum = plain_sum(aggregate.aggregates.first()?)?;
    // The scalar SELECT may apply a bound expression (Q11 multiplies by a
    // decimal constant), but it may consume only the one aggregate output.
    if !expression_uses_only_column(
        scalar_expression,
        ColumnBinding::new(aggregate.aggregate_index, 0),
        &scalar_sum.return_type,
    ) {
        return None;
    }

    Some(ScalarBranch {
        wrapper_binding: ColumnBinding::new(wrapper_projection.table_index, 0),
        wrapper_type: checked.return_type.clone(),
        scalar_expression,
        aggregate,
    })
}

fn is_column(column: &ColumnRefExpression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    column.depth == 0 && column.binding == binding && &column.return_type == ty
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

fn expression_uses_only_column(
    expression: &Expression,
    binding: ColumnBinding,
    return_type: &LogicalType,
) -> bool {
    let mut saw_column = false;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            saw_column |= is_column(column, binding, return_type);
            valid &= is_column(column, binding, return_type);
            ExpressionVisitDecision::SkipChildren
        }
        Expression::Aggregate(_)
        | Expression::Reference(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    valid && saw_column
}

fn rebase_scalar_expression(
    expression: &Expression,
    aggregate_index: usize,
    reducer_type: LogicalType,
) -> Option<Expression> {
    let binding = ColumnBinding::new(aggregate_index, 0);
    if !expression_uses_only_column(expression, binding, &reducer_type) {
        return None;
    }
    Some(expression.clone().replace_column_ref(&|column| {
        is_column(column, binding, &reducer_type)
            .then(|| Expression::Reference(ReferenceExpression::new(0, reducer_type.clone())))
    }))
}

fn rebase_predicate(
    predicate: &Expression,
    aggregate_index: usize,
    scalar_binding: ColumnBinding,
    reduction_index: usize,
    scalar_type: LogicalType,
) -> Option<Expression> {
    let aggregate_binding = ColumnBinding::new(aggregate_index, 0);
    let mut saw_aggregate = false;
    let mut saw_scalar = false;
    let mut valid = true;
    ExpressionIterator::visit(predicate, &mut |node| match node {
        Expression::ColumnRef(column) => {
            if column.depth != 0 {
                valid = false;
            } else if column.binding == aggregate_binding {
                saw_aggregate = true;
            } else if column.binding == scalar_binding && column.return_type == scalar_type {
                saw_scalar = true;
            } else {
                valid = false;
            }
            ExpressionVisitDecision::SkipChildren
        }
        Expression::Aggregate(_)
        | Expression::Reference(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    if !valid || !saw_aggregate || !saw_scalar {
        return None;
    }
    Some(predicate.clone().replace_column_ref(&|column| {
        (column.binding == scalar_binding).then(|| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(reduction_index, 0),
                scalar_type.clone(),
            ))
        })
    }))
}

fn clean_inner_join(join: &ComparisonJoin) -> bool {
    join.join_type == JoinType::Inner
        && join.anti_join_mode == AntiJoinMode::Regular
        && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join
            .left_projection_map
            .is_identity(join.left.types().len())
        && join
            .right_projection_map
            .is_identity(join.right.types().len())
        && !join.conditions.is_empty()
        && join.conditions.iter().all(|condition| {
            matches!(
                condition.comparison,
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
            ) && is_movable(&condition.left)
                && is_movable(&condition.right)
        })
}

#[cfg(test)]
#[path = "post_reduction_tests.rs"]
mod tests;
