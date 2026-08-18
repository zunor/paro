// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reuse a filtered detail stream for an uncorrelated scalar aggregate.
//!
//! A scalar aggregate joined back to a filtered scan of the same base table
//! normally executes the common scan predicate twice. When the scalar source
//! is exactly the detail source plus additional conjuncts, its value is a
//! complete-partition aggregate with those additional conjuncts as an
//! aggregate FILTER. A full-partition window computes that value while
//! retaining the detail rows, eliminating the second scan and scalar join.

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, ComparisonExpression,
    ConjunctionExpression, ConjunctionType, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType, WindowExpression, WindowFrame, WindowFrameBound,
    WindowFrameType,
};
use paro_planner::operator::{
    Aggregate, AntiJoinMode, ColumnBinding, ComparisonJoin, Filter, Get, Join, JoinType,
    LogicalOperator, MarkJoinSemantics, ProjectionMap, Window,
};
use paro_planner::plan::LogicalPlan;

use crate::aggregate::post_reduction::alpha::AlphaBindings;
use crate::aggregate::semantic_kernels::aggregate_kernels_equal;
use crate::subquery::output_contract::{
    child_output_contracts, OutputContract, RewriteOutputShape,
};

pub fn optimize_plan(plan: LogicalPlan, bind_context: &BindContext) -> Result<LogicalPlan> {
    optimize_node(plan, None, bind_context)
}

fn optimize_node(
    plan: LogicalPlan,
    output_contract: Option<OutputContract>,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let child_contracts = child_output_contracts(
        &plan.operator,
        output_contract.as_ref(),
        RewriteOutputShape::StablePrefix,
    );
    let mut child_ordinal = 0usize;
    let plan = plan.try_map_children(|child| {
        let contract = child_contracts.get(child_ordinal).cloned().flatten();
        child_ordinal += 1;
        optimize_node(child, contract, bind_context)
    })?;
    let Some(rewrite) = recognize(&plan, output_contract.as_ref()) else {
        return Ok(plan);
    };
    apply_rewrite(plan, rewrite, bind_context)
}

struct Rewrite {
    detail_projection: ProjectionMap,
    detail_path: Box<[DetailPathStep]>,
    scalar_binding: ColumnBinding,
    scalar_source_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
}

struct ScalarBranch<'a> {
    wrapper_binding: ColumnBinding,
    scalar_expression: &'a Expression,
    aggregate: &'a AggregateExpression,
    aggregate_index: usize,
    source_filter: &'a Filter,
    source_get: &'a Get,
}

struct DetailBranch<'a> {
    filter: &'a Filter,
    get: &'a Get,
    path: Box<[DetailPathStep]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailPathStep {
    LeftReduction,
    RightReduction,
}

fn recognize(plan: &LogicalPlan, output_contract: Option<&OutputContract>) -> Option<Rewrite> {
    let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
        return None;
    };
    if !plain_inner_join(join) {
        return None;
    }
    recognize_detail_left_scalar_right(join, output_contract?)
}

fn recognize_detail_left_scalar_right(
    join: &ComparisonJoin,
    output_contract: &OutputContract,
) -> Option<Rewrite> {
    let detail_plan = join.left.as_ref();
    let scalar_plan = join.right.as_ref();
    let detail = peel_detail_branch(detail_plan)?;
    let scalar = peel_scalar_branch(scalar_plan)?;
    // The scalar branch is an output suffix, so removing it cannot shift any
    // retained detail ordinal. The ancestor binding contract proves that no
    // consumer observes that suffix.
    if output_contract.references(scalar.wrapper_binding) {
        return None;
    }
    let bindings = AlphaBindings::match_gets(detail.get, scalar.source_get)?;

    let mut matched_scalar = vec![false; scalar.source_filter.expressions.len()];
    for detail_predicate in &detail.filter.expressions {
        let index = scalar
            .source_filter
            .expressions
            .iter()
            .enumerate()
            .position(|(index, scalar_predicate)| {
                !matched_scalar[index]
                    && bindings.expressions_equal(detail_predicate, scalar_predicate)
            })?;
        matched_scalar[index] = true;
    }

    let residual = scalar
        .source_filter
        .expressions
        .iter()
        .enumerate()
        .filter(|(index, _)| !matched_scalar[*index])
        .map(|(_, expression)| bindings.rebase_scalar(expression))
        .collect::<Option<Vec<_>>>()?;

    let Expression::Aggregate(mut aggregate) =
        bindings.rebase_scalar(&Expression::Aggregate(scalar.aggregate.clone()))?
    else {
        return None;
    };
    if let Some(filter) = aggregate.filter.take() {
        aggregate.filter = Some(Box::new(and_predicates(
            std::iter::once(*filter).chain(residual).collect(),
        )?));
    } else if let Some(filter) = and_predicates(residual) {
        aggregate.filter = Some(Box::new(filter));
    }

    let detail_bindings = detail_plan
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    if !aggregate_inputs_available(&aggregate, &detail_bindings) {
        return None;
    }

    let scalar_binding = scalar.wrapper_binding;
    let condition_expressions = join
        .conditions
        .iter()
        .map(|condition| {
            Expression::Comparison(ComparisonExpression::new(
                condition.comparison.into(),
                condition.left.clone(),
                condition.right.clone(),
            ))
        })
        .collect::<Vec<_>>();
    if !condition_expressions.iter().all(is_movable)
        || !condition_expressions
            .iter()
            .any(|expression| expression_mentions_binding(expression, scalar_binding))
        || condition_expressions.iter().any(|expression| {
            expression_has_unexpected_binding(expression, &detail_bindings, scalar_binding)
        })
    {
        return None;
    }

    Some(Rewrite {
        detail_projection: join.left_projection_map.clone(),
        detail_path: detail.path,
        scalar_binding,
        scalar_source_binding: ColumnBinding::new(scalar.aggregate_index, 0),
        scalar_expression: scalar.scalar_expression.clone(),
        aggregate,
    })
}

fn peel_detail_branch(plan: &LogicalPlan) -> Option<DetailBranch<'_>> {
    let mut current = plan;
    let mut path = Vec::new();
    loop {
        match &current.operator {
            LogicalOperator::Filter(filter) => {
                let LogicalOperator::Get(get) = &filter.child.operator else {
                    return None;
                };
                return (!filter.expressions.is_empty()
                    && filter.expressions.iter().all(is_movable))
                .then_some(DetailBranch {
                    filter,
                    get,
                    path: path.into_boxed_slice(),
                });
            }
            LogicalOperator::Join(Join::Comparison(join)) if plain_reduction_carrier(join) => {
                match join.join_type {
                    JoinType::Semi | JoinType::Anti => {
                        path.push(DetailPathStep::LeftReduction);
                        current = join.left.as_ref();
                    }
                    JoinType::RightSemi | JoinType::RightAnti => {
                        path.push(DetailPathStep::RightReduction);
                        current = join.right.as_ref();
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
}

fn plain_reduction_carrier(join: &ComparisonJoin) -> bool {
    matches!(
        join.join_type,
        JoinType::Semi | JoinType::Anti | JoinType::RightSemi | JoinType::RightAnti
    ) && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
}

fn peel_scalar_branch(plan: &LogicalPlan) -> Option<ScalarBranch<'_>> {
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
    if !plain_scalar_wrapper(wrapper) {
        return None;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        wrapper.aggregates.as_slice()
    else {
        return None;
    };
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
    let [Expression::ColumnRef(first_output), Expression::ColumnRef(count_output)] =
        checked.children.as_slice()
    else {
        return None;
    };
    if !is_column(
        first_output,
        ColumnBinding::new(wrapper.aggregate_index, 0),
        &first.return_type,
    ) || !is_column(
        count_output,
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
    if !is_movable(scalar_expression)
        || scalar_expression.return_type() != first.return_type
        || !is_column_expression(
            &first.children[0],
            ColumnBinding::new(scalar_projection.table_index, 0),
            &scalar_expression.return_type(),
        )
    {
        return None;
    }

    let LogicalOperator::Aggregate(reduction) = &scalar_projection.child.operator else {
        return None;
    };
    if !plain_ungrouped_aggregate(reduction)
        || !expression_uses_only_binding(
            scalar_expression,
            ColumnBinding::new(reduction.aggregate_index, 0),
        )
    {
        return None;
    }
    let Expression::Aggregate(aggregate) = &reduction.aggregates[0] else {
        return None;
    };
    if aggregate.aggr_type != AggregateType::NonDistinct
        || aggregate.function.destructor.is_some()
        || !aggregate.order_bys.is_empty()
        || !is_movable(&reduction.aggregates[0])
    {
        return None;
    }
    let LogicalOperator::Filter(source_filter) = &reduction.child.operator else {
        return None;
    };
    let LogicalOperator::Get(source_get) = &source_filter.child.operator else {
        return None;
    };
    if source_filter.expressions.is_empty() || !source_filter.expressions.iter().all(is_movable) {
        return None;
    }

    Some(ScalarBranch {
        wrapper_binding: ColumnBinding::new(wrapper_projection.table_index, 0),
        scalar_expression,
        aggregate,
        aggregate_index: reduction.aggregate_index,
        source_filter,
        source_get,
    })
}

fn plain_inner_join(join: &ComparisonJoin) -> bool {
    join.join_type == paro_planner::operator::JoinType::Inner
        && join.anti_join_mode == AntiJoinMode::Regular
        && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
}

fn plain_scalar_wrapper(wrapper: &Aggregate) -> bool {
    if !wrapper.groups.is_empty()
        || !wrapper.grouping_sets.is_empty()
        || !wrapper.grouping_functions.is_empty()
        || wrapper.aggregates.len() != 2
        || wrapper.post_reduction.is_some()
    {
        return false;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        wrapper.aggregates.as_slice()
    else {
        return false;
    };
    first.aggr_type == AggregateType::NonDistinct
        && first.filter.is_none()
        && first.order_bys.is_empty()
        && first.children.len() == 1
        && count.function.arguments.is_empty()
        && count.return_type == LogicalType::BigInt
        && count.aggr_type == AggregateType::NonDistinct
        && count.filter.is_none()
        && count.order_bys.is_empty()
        && count.children.is_empty()
}

fn plain_ungrouped_aggregate(aggregate: &Aggregate) -> bool {
    aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
}

fn and_predicates(mut predicates: Vec<Expression>) -> Option<Expression> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            predicates,
        ))),
    }
}

fn aggregate_inputs_available(
    aggregate: &AggregateExpression,
    detail_bindings: &HashSet<ColumnBinding>,
) -> bool {
    aggregate
        .children
        .iter()
        .chain(aggregate.filter.iter().map(Box::as_ref))
        .chain(aggregate.order_bys.iter().map(|order| &order.expression))
        .all(|expression| !expression_has_binding_outside(expression, detail_bindings))
}

fn expression_has_binding_outside(
    expression: &Expression,
    allowed: &HashSet<ColumnBinding>,
) -> bool {
    let mut invalid = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if let Expression::ColumnRef(column) = node {
            invalid |= column.depth != 0 || !allowed.contains(&column.binding);
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    invalid
}

fn expression_has_unexpected_binding(
    expression: &Expression,
    detail_bindings: &HashSet<ColumnBinding>,
    scalar_binding: ColumnBinding,
) -> bool {
    let mut invalid = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if let Expression::ColumnRef(column) = node {
            invalid |= column.depth != 0
                || (column.binding != scalar_binding && !detail_bindings.contains(&column.binding));
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    invalid
}

fn expression_mentions_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if matches!(node, Expression::ColumnRef(column) if column.depth == 0 && column.binding == binding)
        {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

fn expression_uses_only_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut saw_binding = false;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            saw_binding |= column.depth == 0 && column.binding == binding;
            valid &= column.depth == 0 && column.binding == binding;
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
    valid && saw_binding
}

fn is_column_expression(expression: &Expression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    matches!(expression, Expression::ColumnRef(column) if is_column(column, binding, ty))
}

fn is_column(column: &ColumnRefExpression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    column.depth == 0 && column.binding == binding && &column.return_type == ty
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

fn apply_rewrite(
    plan: LogicalPlan,
    rewrite: Rewrite,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Join(Join::Comparison(mut join)),
    } = plan
    else {
        return Err(paro_error::internal(
            "scalar aggregate window witness no longer points to a comparison join",
        ));
    };
    let detail = std::mem::replace(
        &mut *join.left,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let window_index = bind_context.generate_table_index();
    let window_binding = ColumnBinding::new(window_index, 0);
    let window_type = rewrite.aggregate.return_type.clone();
    let frame = WindowFrame {
        frame_type: WindowFrameType::Rows,
        start_bound: WindowFrameBound::Unbounded,
        start_is_preceding: true,
        end_bound: WindowFrameBound::Unbounded,
        end_is_preceding: false,
    };
    let scalar = rewrite.scalar_expression.replace_column_ref(&|column| {
        (column.depth == 0 && column.binding == rewrite.scalar_source_binding).then(|| {
            Expression::ColumnRef(ColumnRefExpression::new(
                window_binding,
                window_type.clone(),
            ))
        })
    });
    if !expression_uses_only_binding(&scalar, window_binding) {
        return Err(paro_error::internal(
            "scalar aggregate expression escaped its witnessed output binding",
        ));
    }
    let predicates = join
        .conditions
        .into_iter()
        .map(|condition| {
            Expression::Comparison(ComparisonExpression::new(
                condition.comparison.into(),
                condition.left.replace_column_ref(&|column| {
                    (column.depth == 0 && column.binding == rewrite.scalar_binding)
                        .then(|| scalar.clone())
                }),
                condition.right.replace_column_ref(&|column| {
                    (column.depth == 0 && column.binding == rewrite.scalar_binding)
                        .then(|| scalar.clone())
                }),
            ))
        })
        .collect::<Vec<_>>();
    if predicates
        .iter()
        .any(|expression| expression_mentions_binding(expression, rewrite.scalar_binding))
    {
        return Err(paro_error::internal(
            "scalar aggregate join predicate retained the eliminated scalar binding",
        ));
    }

    let window_expression =
        WindowExpression::aggregate(rewrite.aggregate, Vec::new(), Vec::new(), frame);
    window_expression.verify_bound_contract()?;
    let path_is_empty = rewrite.detail_path.is_empty();
    let mut installed_detail = install_scalar_window(
        detail,
        rewrite.detail_path.as_ref(),
        window_index,
        window_expression,
        predicates,
        path_is_empty.then_some(&rewrite.detail_projection),
        bind_context,
    )?;
    if !path_is_empty {
        let preserved_input_width = installed_detail.preserved_input_width.ok_or_else(|| {
            paro_error::internal(
                "scalar aggregate nested detail installation omitted its width witness",
            )
        })?;
        apply_detail_projection(
            &mut installed_detail.plan,
            &rewrite.detail_projection,
            preserved_input_width,
        )?;
    }
    Ok(LogicalPlan {
        id,
        stats,
        operator: installed_detail.plan.operator,
    })
}

/// Result of installing the window in a detail carrier.
///
/// `output_width` is the exact visible width of `plan`. For a reduction carrier,
/// `preserved_input_width` records the child domain over which the carrier's
/// positional projection map was built. Keeping that value next to the rewrite
/// eliminates the previous cross-function assumption that the window column
/// happened to be hidden by an identity-looking filter projection.
struct InstalledScalarWindow {
    plan: LogicalPlan,
    output_width: usize,
    preserved_input_width: Option<usize>,
}

fn install_scalar_window(
    mut plan: LogicalPlan,
    path: &[DetailPathStep],
    window_index: usize,
    window_expression: WindowExpression,
    predicates: Vec<Expression>,
    output_projection: Option<&ProjectionMap>,
    bind_context: &BindContext,
) -> Result<InstalledScalarWindow> {
    let Some((step, remaining)) = path.split_first() else {
        let detail_width = plan.types().len();
        let detail_stats = plan.stats.clone();
        let mut window = LogicalPlan::new(
            bind_context,
            LogicalOperator::Window(Window::new(window_index, vec![window_expression], plan)),
        );
        window.stats = detail_stats.clone();
        let mut filter = Filter::new(window, predicates);
        filter.projection_map = output_projection.map_or_else(
            || ProjectionMap::new((0..detail_width).collect()),
            |projection| ProjectionMap::new(projection.to_indices(detail_width)),
        );
        let mut result = LogicalPlan::new(bind_context, LogicalOperator::Filter(filter));
        result.stats = detail_stats;
        let output_width = result.types().len();
        return Ok(InstalledScalarWindow {
            plan: result,
            output_width,
            preserved_input_width: None,
        });
    };

    let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
        return Err(paro_error::internal(
            "scalar aggregate detail path no longer points to a comparison join",
        ));
    };
    if !plain_reduction_carrier(join) {
        return Err(paro_error::internal(
            "scalar aggregate detail path no longer points to a clean reduction join",
        ));
    }
    let child = match step {
        DetailPathStep::LeftReduction
            if matches!(join.join_type, JoinType::Semi | JoinType::Anti) =>
        {
            &mut join.left
        }
        DetailPathStep::RightReduction
            if matches!(join.join_type, JoinType::RightSemi | JoinType::RightAnti) =>
        {
            &mut join.right
        }
        _ => {
            return Err(paro_error::internal(
                "scalar aggregate detail path changed preserved side",
            ));
        }
    };
    let owned_child = std::mem::replace(
        child.as_mut(),
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let installed_child = install_scalar_window(
        owned_child,
        remaining,
        window_index,
        window_expression,
        predicates,
        output_projection,
        bind_context,
    )?;
    let preserved_input_width = installed_child.output_width;
    **child = installed_child.plan;
    let output_width = plan.types().len();
    Ok(InstalledScalarWindow {
        plan,
        output_width,
        preserved_input_width: Some(preserved_input_width),
    })
}

fn apply_detail_projection(
    plan: &mut LogicalPlan,
    projection: &ProjectionMap,
    witnessed_child_width: usize,
) -> Result<()> {
    let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
        return Err(paro_error::internal(
            "scalar aggregate nested detail root is not a reduction join",
        ));
    };
    let (actual_child_width, preserved_projection) = match join.join_type {
        JoinType::Semi | JoinType::Anti => (join.left.types().len(), &mut join.left_projection_map),
        JoinType::RightSemi | JoinType::RightAnti => {
            (join.right.types().len(), &mut join.right_projection_map)
        }
        _ => {
            return Err(paro_error::internal(
                "scalar aggregate nested detail root changed join type",
            ));
        }
    };
    if actual_child_width != witnessed_child_width {
        return Err(paro_error::internal(
            "scalar aggregate detail carrier changed its witnessed child width",
        ));
    }
    let existing = preserved_projection.to_indices(witnessed_child_width);
    let requested = projection.to_indices(existing.len());
    let Some(composed) = requested
        .into_iter()
        .map(|index| existing.get(index).copied())
        .collect::<Option<Vec<_>>>()
    else {
        return Err(paro_error::internal(
            "scalar aggregate detail projection escaped the reduction output",
        ));
    };
    *preserved_projection = ProjectionMap::new(composed);
    Ok(())
}
