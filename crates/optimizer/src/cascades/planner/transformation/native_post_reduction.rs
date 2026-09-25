// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the local aggregate post-reduction rewrite.
//!
//! `AggregatePostReduction` is one of the last transformation rules whose
//! normal path still builds an owned tree merely to inspect two scalar
//! aggregate branches.  The matcher exposes a deliberately small, exact
//! shape here: a projection/filter/cross shell, one grouped aggregate over a
//! selected Get, and one scalar FIRST/COUNT wrapper over a second selected Get.
//! The native adapter repeats the owned rule's semantic checks over that shell
//! and publishes the hidden reduction without importing an owned subtree.
//!
//! This is not a new aggregate implementation.  Unsupported source paths,
//! CTE ownership, non-movable expressions, and incomplete contracts return
//! `None` so the existing semantic rule remains authoritative for those
//! shapes.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_function::aggregate::AggregateAlgebra;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType, ReferenceExpression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, Join, LogicalOperator, PostAggregateReduction, Projection,
};

use crate::rewrite::aggregate::post_reduction::alpha::AlphaBindings;
use crate::rewrite::aggregate::semantic_kernels::aggregate_kernels_equal;

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::{boundary, Memo, PatternOperand, PlannerTransformState};

pub(super) fn try_native_aggregate_post_reduction(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let original_layout = layouts
        .get(shell.root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native post reduction has no root layout"))?;
    rewrite_shell(shell, original_layout, state)
}

fn rewrite_shell(
    shell: NativeShell,
    original_layout: paro_planner::operator::LogicalOutputLayout,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let root = shell.root;
    let layouts = shell.layouts()?;
    let mut nodes = shell.nodes.into_vec();

    let Some((mut output, filter_index, cross_index)) = root_shell(&nodes, root) else {
        return Ok(None);
    };
    let LogicalOperator::Filter(filter) = nodes[filter_index].operator.clone() else {
        return Ok(None);
    };
    let filter_input_layout = child_layout(&layouts, &filter.child)?;
    if filter.expressions.len() != 1
        || !filter
            .projection_map
            .is_identity(filter_input_layout.bindings().len())
    {
        return Ok(None);
    }
    let predicate = filter.expressions[0].clone();
    if !matches!(predicate, Expression::Comparison(_)) || !is_movable(&predicate) {
        return Ok(None);
    }

    let LogicalOperator::Join(Join::Cross(cross)) = nodes[cross_index].operator.clone() else {
        return Ok(None);
    };
    let orientations = [
        (cross.left.clone(), cross.right.clone()),
        (cross.right.clone(), cross.left.clone()),
    ];
    let mut selected = None;
    for (grouped_child, scalar_child) in orientations {
        if let Some(rewrite) =
            recognize_orientation(&nodes, grouped_child, scalar_child, &predicate, state)?
        {
            selected = Some(rewrite);
            break;
        }
    }
    let Some(rewrite) = selected else {
        return Ok(None);
    };

    let grouped_layout = child_layout(&layouts, &rewrite.grouped_child)?;
    if !projection_consumes_only(&output, grouped_layout) {
        return Ok(None);
    }

    let grouped_index = match rewrite.grouped_child {
        NativeChild::Node(index) => index,
        NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => return Ok(None),
    };
    let LogicalOperator::Aggregate(grouped) = nodes[grouped_index].operator.clone() else {
        return Ok(None);
    };
    let mut grouped = *grouped;
    grouped.post_reduction = Some(rewrite.reduction);
    nodes[grouped_index].operator = LogicalOperator::Aggregate(Box::new(grouped));
    nodes[grouped_index].source_proofs = Box::new([]);

    output.child = NativeChild::Node(grouped_index);
    nodes[root].operator = LogicalOperator::Projection(output);
    nodes[root].source_proofs = Box::new([]);

    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_layout {
        return Ok(None);
    }
    Ok(Some(shell))
}

fn root_shell(
    nodes: &[NativeNode],
    root: usize,
) -> Option<(Projection<NativeChild>, usize, usize)> {
    let LogicalOperator::Projection(output) = nodes.get(root)?.operator.clone() else {
        return None;
    };
    let NativeChild::Node(filter_index) = output.child.clone() else {
        return None;
    };
    let LogicalOperator::Filter(filter) = nodes.get(filter_index)?.operator.clone() else {
        return None;
    };
    let NativeChild::Node(cross_index) = filter.child else {
        return None;
    };
    matches!(
        nodes.get(cross_index)?.operator,
        LogicalOperator::Join(Join::Cross(_))
    )
    .then_some((output, filter_index, cross_index))
}

struct Rewrite {
    grouped_child: NativeChild,
    reduction: PostAggregateReduction,
}

fn recognize_orientation(
    nodes: &[NativeNode],
    grouped_child: NativeChild,
    scalar_child: NativeChild,
    predicate: &Expression,
    state: &PlannerTransformState,
) -> Result<Option<Rewrite>> {
    let NativeChild::Node(grouped_index) = grouped_child.clone() else {
        return Ok(None);
    };
    let NativeChild::Node(scalar_index) = scalar_child else {
        return Ok(None);
    };
    let LogicalOperator::Aggregate(grouped) = nodes
        .get(grouped_index)
        .ok_or_else(|| paro_error::internal("native post reduction lost grouped node"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if !plain_grouped_aggregate(&grouped) {
        return Ok(None);
    }
    let Some(grouped_sum) = plain_sum(grouped.aggregates.first()) else {
        return Ok(None);
    };
    let scalar = match nodes
        .get(scalar_index)
        .ok_or_else(|| paro_error::internal("native post reduction lost scalar node"))?
        .operator
        .clone()
    {
        LogicalOperator::Projection(projection) => projection,
        _ => return Ok(None),
    };
    let Some(scalar_branch) = peel_scalar_wrapper(nodes, scalar_index, scalar) else {
        return Ok(None);
    };
    let Some(scalar_sum) = plain_sum(scalar_branch.aggregate.aggregates.first()) else {
        return Ok(None);
    };
    if scalar_branch.scalar_expression.return_type() != scalar_branch.wrapper_type {
        return Ok(None);
    }

    let (grouped_get, scalar_get) = (
        direct_get(nodes, &grouped.child),
        direct_get(nodes, &scalar_branch.aggregate.child),
    );
    let (Some(grouped_get), Some(scalar_get)) = (grouped_get, scalar_get) else {
        return Ok(None);
    };
    let Some(bindings) = AlphaBindings::match_gets(grouped_get, scalar_get) else {
        return Ok(None);
    };
    if !bindings.expressions_equal(
        grouped.aggregates.first().ok_or_else(|| {
            paro_error::internal("native post reduction grouped aggregate is empty")
        })?,
        scalar_branch.aggregate.aggregates.first().ok_or_else(|| {
            paro_error::internal("native post reduction scalar aggregate is empty")
        })?,
    ) {
        return Ok(None);
    }
    if !aggregate_kernels_equal(grouped_sum, scalar_sum)
        || grouped_sum.return_type != scalar_sum.return_type
        || grouped_sum.function.algebra != Some(AggregateAlgebra::Sum)
    {
        return Ok(None);
    }

    let Some(merge) = grouped_sum.function.partial_merge_function() else {
        return Ok(None);
    };
    if merge.arguments.as_slice() != [grouped_sum.return_type.clone()]
        || merge.return_type != grouped_sum.return_type
        || merge.destructor.is_some()
    {
        return Ok(None);
    }
    let Some(scalar_expression) = rebase_scalar_expression(
        &scalar_branch.scalar_expression,
        scalar_branch.aggregate_index,
        grouped_sum.return_type.clone(),
    ) else {
        return Ok(None);
    };
    let reduction_index = state.bind_context.generate_table_index();
    let Some(predicate) = rebase_predicate(
        predicate,
        grouped.aggregate_index,
        scalar_branch.wrapper_binding,
        reduction_index,
        scalar_branch.wrapper_type.clone(),
    ) else {
        return Ok(None);
    };
    let reducer = Expression::Aggregate(
        AggregateExpression::new(
            merge,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(grouped.aggregate_index, 0),
                    grouped_sum.return_type.clone(),
                )
                .into(),
            )],
            grouped_sum.return_type.clone(),
        )
        .into(),
    );

    Ok(Some(Rewrite {
        grouped_child,
        reduction: PostAggregateReduction {
            reduction_index,
            reducers: vec![reducer],
            scalar_expressions: vec![scalar_expression],
            predicate,
        },
    }))
}

struct ScalarBranch {
    wrapper_binding: ColumnBinding,
    wrapper_type: LogicalType,
    scalar_expression: Expression,
    aggregate: Aggregate<NativeChild>,
    aggregate_index: usize,
}

fn peel_scalar_wrapper(
    nodes: &[NativeNode],
    _wrapper_index: usize,
    wrapper_projection: Projection<NativeChild>,
) -> Option<ScalarBranch> {
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
    let NativeChild::Node(aggregate_index) = wrapper_projection.child else {
        return None;
    };
    let LogicalOperator::Aggregate(wrapper) = nodes.get(aggregate_index)?.operator.clone() else {
        return None;
    };
    if !plain_scalar_wrapper(&wrapper) {
        return None;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        wrapper.aggregates.as_slice()
    else {
        return None;
    };
    let (canonical_first, _) = get_first_function()
        .bind(std::slice::from_ref(&first.children.first()?.return_type()))
        .ok()?;
    if !aggregate_kernels_equal(
        first,
        &AggregateExpression::new(canonical_first, Vec::new(), first.return_type.clone()),
    ) || !aggregate_kernels_equal(
        count,
        &AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt),
    ) {
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
    let NativeChild::Node(scalar_projection_index) = wrapper.child.clone() else {
        return None;
    };
    let LogicalOperator::Projection(scalar_projection) =
        nodes.get(scalar_projection_index)?.operator.clone()
    else {
        return None;
    };
    if scalar_projection.expressions.len() != 1
        || scalar_projection.returned_types.len() != 1
        || scalar_projection.visible_names.len() != 1
    {
        return None;
    }
    let scalar_expression = scalar_projection.expressions[0].clone();
    if !is_movable(&scalar_expression) || scalar_expression.return_type() != first.return_type {
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
    let NativeChild::Node(reduction_index) = scalar_projection.child else {
        return None;
    };
    let LogicalOperator::Aggregate(reduction) = nodes.get(reduction_index)?.operator.clone() else {
        return None;
    };
    if !plain_ungrouped_aggregate(&reduction) {
        return None;
    }
    let Some(scalar_sum) = plain_sum(reduction.aggregates.first()) else {
        return None;
    };
    if !expression_uses_only_column(
        &scalar_expression,
        ColumnBinding::new(reduction.aggregate_index, 0),
        &scalar_sum.return_type,
    ) {
        return None;
    }
    let aggregate_index = reduction.aggregate_index;
    Some(ScalarBranch {
        wrapper_binding: ColumnBinding::new(wrapper_projection.table_index, 0),
        wrapper_type: checked.return_type.clone(),
        scalar_expression,
        aggregate: *reduction,
        aggregate_index,
    })
}

fn direct_get<'a>(
    nodes: &'a [NativeNode],
    child: &'a NativeChild,
) -> Option<&'a paro_planner::operator::Get> {
    let NativeChild::Node(index) = child else {
        return None;
    };
    match &nodes.get(*index)?.operator {
        LogicalOperator::Get(get) => Some(get),
        _ => None,
    }
}

fn child_layout<'a>(
    layouts: &'a [paro_planner::operator::LogicalOutputLayout],
    child: &'a NativeChild,
) -> Result<&'a paro_planner::operator::LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .ok_or_else(|| paro_error::internal("native post reduction lost child layout")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => Ok(layout),
    }
}

fn projection_consumes_only(
    projection: &Projection<NativeChild>,
    source_layout: &paro_planner::operator::LogicalOutputLayout,
) -> bool {
    let available = source_layout
        .bindings()
        .iter()
        .copied()
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

fn plain_grouped_aggregate(aggregate: &Aggregate<NativeChild>) -> bool {
    !aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
        && aggregate.groups.iter().all(is_movable)
}

fn plain_ungrouped_aggregate(aggregate: &Aggregate<NativeChild>) -> bool {
    aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
}

fn plain_scalar_wrapper(aggregate: &Aggregate<NativeChild>) -> bool {
    if !aggregate.groups.is_empty()
        || !aggregate.grouping_sets.is_empty()
        || !aggregate.grouping_functions.is_empty()
        || aggregate.aggregates.len() != 2
        || aggregate.post_reduction.is_some()
    {
        return false;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        aggregate.aggregates.as_slice()
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

fn plain_sum(expression: Option<&Expression>) -> Option<&AggregateExpression> {
    let Expression::Aggregate(aggregate) = expression? else {
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
        is_column(column, binding, &reducer_type).then(|| {
            Expression::Reference(ReferenceExpression::new(0, reducer_type.clone()).into())
        })
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
            Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(reduction_index, 0),
                    scalar_type.clone(),
                )
                .into(),
            )
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_planner::expression::ComparisonExpression;
    use paro_planner::operator::{CrossProduct, Filter, Get};
    use paro_storage::table::table_factory::TableFactory;

    use crate::cascades::budget::SearchBudget;
    use crate::cascades::planner::transformation::{PlannerTransformation, TransformContext};
    use crate::cascades::planner::MemoBuilder;
    use crate::cascades::rules::TransformationRule;
    use paro_planner::binder::context::BindContext;
    use paro_planner::plan::OwnedLogicalPlan;

    fn table(object_id: u64) -> std::sync::Arc<TableCatalogEntry> {
        let types = vec![LogicalType::BigInt, LogicalType::Integer];
        let storage = std::sync::Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            format!("post_reduction_{object_id}"),
            vec![
                ColumnDefinition::new("key".to_string(), types[0].clone()),
                ColumnDefinition::new("value".to_string(), types[1].clone()),
            ],
        );
        std::sync::Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(object_id), 0)
                .unwrap(),
        )
    }

    fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, index), ty).into())
    }

    fn sum(input: Expression) -> Expression {
        let ty = input.return_type();
        let (function, _) = paro_function::aggregate::distributive::sum::get_sum_function()
            .bind(std::slice::from_ref(&ty))
            .unwrap();
        let return_type = function.return_type.clone();
        Expression::Aggregate(AggregateExpression::new(function, vec![input], return_type).into())
    }

    fn get(table_index: usize, table: std::sync::Arc<TableCatalogEntry>) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            table_index,
            vec!["key".to_string(), "value".to_string()],
            vec![LogicalType::BigInt, LogicalType::Integer],
            table,
        ))))
    }

    fn shape() -> OwnedLogicalPlan {
        let grouped = Aggregate::new(
            30,
            31,
            32,
            get(10, table(92_001)),
            vec![column(10, 0, LogicalType::BigInt)],
            vec![],
            vec![sum(column(10, 1, LogicalType::Integer))],
            vec![],
        );
        let scalar_aggregate = Aggregate::new(
            40,
            41,
            42,
            get(20, table(92_001)),
            vec![],
            vec![],
            vec![sum(column(20, 1, LogicalType::Integer))],
            vec![],
        );
        let scalar_projection = Projection::new(
            43,
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(scalar_aggregate))),
            vec![column(41, 0, LogicalType::BigInt)],
        );
        let (first, _) = get_first_function().bind(&[LogicalType::BigInt]).unwrap();
        let wrapper = Aggregate::new(
            50,
            51,
            52,
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(scalar_projection)),
            vec![],
            vec![],
            vec![
                Expression::Aggregate(
                    AggregateExpression::new(
                        first,
                        vec![column(43, 0, LogicalType::BigInt)],
                        LogicalType::BigInt,
                    )
                    .into(),
                ),
                Expression::Aggregate(
                    AggregateExpression::new(
                        get_count_star_function(),
                        vec![],
                        LogicalType::BigInt,
                    )
                    .into(),
                ),
            ],
            vec![],
        );
        let checked = Expression::Operator(
            paro_planner::expression::OperatorExpression::new(
                OperatorType::ErrorIfMultipleRows,
                vec![
                    column(51, 0, LogicalType::BigInt),
                    column(51, 1, LogicalType::BigInt),
                ],
                LogicalType::BigInt,
            )
            .into(),
        );
        let wrapper_projection = Projection::new(
            53,
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(wrapper))),
            vec![checked],
        );
        let cross =
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(grouped))),
                OwnedLogicalPlan::synthetic(LogicalOperator::Projection(wrapper_projection)),
            ))));
        let filter = Filter::new(
            cross,
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    paro_planner::expression::ComparisonType::GreaterThan,
                    column(31, 0, LogicalType::BigInt),
                    column(53, 0, LogicalType::BigInt),
                )
                .into(),
            )],
        );
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            60,
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter)),
            vec![
                column(30, 0, LogicalType::BigInt),
                column(31, 0, LogicalType::BigInt),
            ],
        )))
    }

    #[test]
    fn native_post_reduction_rewrites_exact_shell() {
        let mut input =
            MemoBuilder::build(shape(), BindContext::new(), SearchBudget::default()).unwrap();
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let state = input.planner_state.read().unwrap();
        let binding = crate::cascades::planner::transformation::matching::scoped_pattern_bindings(
            PlannerTransformation::AggregatePostReduction,
            input.root,
            root_expression,
            &input.memo,
            &state,
            None,
            crate::cascades::budget::BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .expect("exact post-reduction shell");
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            crate::cascades::budget::BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .expect("post-reduction facts");
        let shell =
            try_native_aggregate_post_reduction(&binding.root, context.memo(), &state, &facts)
                .unwrap()
                .expect("native post reduction");
        let LogicalOperator::Projection(output) = shell.root_operator() else {
            panic!("projection root")
        };
        let NativeChild::Node(aggregate) = &output.child else {
            panic!("grouped aggregate child")
        };
        let LogicalOperator::Aggregate(aggregate) = &shell.nodes[*aggregate].operator else {
            panic!("aggregate child")
        };
        assert!(aggregate.post_reduction.is_some());
        assert_eq!(aggregate.post_reduction.as_ref().unwrap().reducers.len(), 1);
    }

    #[test]
    fn production_binding_stages_post_reduction_without_owned_settlement() {
        let mut input =
            MemoBuilder::build(shape(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = {
            let state_read = state.read().unwrap();
            crate::cascades::planner::transformation::matching::scoped_pattern_bindings(
                PlannerTransformation::AggregatePostReduction,
                input.root,
                root_expression,
                &input.memo,
                &state_read,
                None,
                crate::cascades::budget::BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("production post-reduction pattern should match")
        };
        let rule = crate::cascades::planner::transformation::PlannerTransformationRule {
            transformation: PlannerTransformation::AggregatePostReduction,
            planner_state: state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        let state_read = state.read().unwrap();
        let payload = state_read
            .payloads
            .logical
            .get(outputs[0].payload.index())
            .expect("staged post-reduction payload");
        let LogicalOperator::Projection(output) = &payload.semantic_template.operator else {
            panic!("expected projection output")
        };
        assert_eq!(output.expressions.len(), 2);
    }

    #[test]
    fn native_post_reduction_refuses_different_catalog_object() {
        let mut plan = shape();
        let LogicalOperator::Projection(output) = &mut plan.operator else {
            panic!("projection")
        };
        let LogicalOperator::Filter(filter) = &mut output.child.operator else {
            panic!("filter")
        };
        let LogicalOperator::Join(Join::Cross(cross)) = &mut filter.child.operator else {
            panic!("cross")
        };
        let LogicalOperator::Projection(wrapper) = &mut cross.right.operator else {
            panic!("wrapper projection")
        };
        let LogicalOperator::Aggregate(aggregate) = &mut wrapper.child.operator else {
            panic!("wrapper aggregate")
        };
        let LogicalOperator::Projection(scalar) = &mut aggregate.child.operator else {
            panic!("scalar projection")
        };
        let LogicalOperator::Aggregate(reduction) = &mut scalar.child.operator else {
            panic!("reduction")
        };
        reduction.child = Box::new(get(21, table(92_002)));
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let state = input.planner_state.read().unwrap();
        let bindings = crate::cascades::planner::transformation::matching::scoped_pattern_bindings(
            PlannerTransformation::AggregatePostReduction,
            input.root,
            root_expression,
            &input.memo,
            &state,
            None,
            crate::cascades::budget::BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        let binding = bindings
            .bindings
            .first()
            .expect("structural matcher still exposes the authoritative fallback")
            .clone();
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            crate::cascades::budget::BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .expect("catalog mismatch still has a valid boundary contract");
        assert!(
            try_native_aggregate_post_reduction(&binding.root, context.memo(), &state, &facts)
                .unwrap()
                .is_none()
        );
    }
}
