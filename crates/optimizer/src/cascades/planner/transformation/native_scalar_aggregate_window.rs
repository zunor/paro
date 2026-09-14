// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the exact scalar-aggregate/window rewrite.
//!
//! The scalar aggregate rule is intentionally conservative: its generic
//! matcher keeps the detail side opaque while it follows the scalar witness.
//! For the common direct `Filter -> Get` detail shape, a singleton detail
//! alternative, including Projection/Filter ancestors, can be expanded as a bounded Memo shell and rewritten without
//! importing an owned tree.  Any ambiguity, unsupported operator, or missing
//! evidence returns `None` and leaves the authoritative owned rule available.

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;
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
use paro_planner::plan::NodeStats;

use crate::aggregate::post_reduction::alpha::AlphaBindings;
use crate::aggregate::semantic_kernels::aggregate_kernels_equal;

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::{boundary, PatternOperand, PlannerTransformState};

pub(super) fn try_native_scalar_aggregate_window(
    binding: &PatternOperand,
    ctx: &mut super::TransformContext<'_>,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let Some(expanded) = expand_binding(binding, Shape::Join, ctx, state)? else {
        return Ok(None);
    };
    let Some(facts) = boundary::BoundarySnapshot::read(
        ctx,
        state,
        &expanded,
        super::BudgetDimension::RuleWorkPerGroup,
    )?
    else {
        return Ok(None);
    };
    ctx.record_fact_value(facts.binding_value_fingerprint(ctx.memo(), &expanded)?);
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(ctx.memo(), state, &expanded, &facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let result = rewrite_shell(shell, layouts, state)?;
    // Canonical templates expose carriers. The actual target group is the
    // authority for whether the scalar suffix may be removed.
    if let Some(shell) = &result {
        if !super::transformed_layout_matches_group_contract(
            &shell.root_layout()?,
            ctx.group(),
            ctx.memo(),
            state,
        )? {
            return Ok(None);
        }
    }
    Ok(result)
}

#[derive(Clone, Copy)]
enum Shape {
    Boundary,
    Join,
    Detail,
    WrapperProjection,
    WrapperAggregate,
    ScalarProjection,
    Reduction,
    SourceFilter,
    Get,
}

impl Shape {
    fn children(self, operator: &LogicalOperator<()>) -> Option<&'static [Shape]> {
        match (self, operator) {
            (Self::Join, LogicalOperator::Projection(_) | LogicalOperator::Filter(_)) => {
                Some(&[Self::Join])
            }
            (Self::Join, LogicalOperator::Join(Join::Comparison(_))) => {
                Some(&[Self::Detail, Self::WrapperProjection])
            }
            (Self::Detail | Self::SourceFilter, LogicalOperator::Filter(_)) => Some(&[Self::Get]),
            (Self::Detail, LogicalOperator::Join(Join::Comparison(join)))
                if plain_reduction_carrier(join) =>
            {
                if matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
                    Some(&[Self::Detail, Self::Boundary])
                } else {
                    Some(&[Self::Boundary, Self::Detail])
                }
            }
            (Self::WrapperProjection, LogicalOperator::Projection(_)) => {
                Some(&[Self::WrapperAggregate])
            }
            (Self::WrapperAggregate, LogicalOperator::Aggregate(_)) => {
                Some(&[Self::ScalarProjection])
            }
            (Self::ScalarProjection, LogicalOperator::Projection(_)) => Some(&[Self::Reduction]),
            (Self::Reduction, LogicalOperator::Aggregate(_)) => Some(&[Self::SourceFilter]),
            (Self::Get, LogicalOperator::Get(_)) => Some(&[]),
            _ => None,
        }
    }
}

/// Resolve only this finite grammar. Every singleton lookup subscribes to its
/// frontier before testing uniqueness, including negative results. Both
/// traversal and operand construction are charged before allocation.
fn expand_binding(
    operand: &PatternOperand,
    shape: Shape,
    ctx: &mut super::TransformContext<'_>,
    state: &PlannerTransformState,
) -> Result<Option<PatternOperand>> {
    use crate::cascades::rules::PatternRead;
    if !ctx.admit_fact_work(super::BudgetDimension::RuleWorkPerGroup, 1)? {
        return Ok(None);
    }
    if matches!(shape, Shape::Boundary) {
        // The non-preserved side is opaque in this rewrite. Do not accept an
        // expanded subtree that could contain another scalar-window witness:
        // claiming native completeness would then hide its owned peer rewrite.
        return Ok(match operand {
            PatternOperand::Group(_) => Some(operand.clone()),
            PatternOperand::Expression { children, .. } if children.is_empty() => {
                Some(operand.clone())
            }
            _ => None,
        });
    }
    let (group, expression) = match operand {
        PatternOperand::Expression {
            group, expression, ..
        } => (*group, *expression),
        PatternOperand::Group(group) => {
            let group = ctx.memo().canonical_group(*group);
            let read = PatternRead::from_group(ctx.memo(), group)?;
            ctx.record_fact_read(read);
            let Some(group_ref) = ctx.memo().group(group) else {
                return Ok(None);
            };
            let [expression] = group_ref.logical_exprs() else {
                return Ok(None);
            };
            (group, *expression)
        }
    };
    let Some(logical) = ctx.memo().logical_expr(expression) else {
        return Ok(None);
    };
    let Some(payload) = state.payloads.logical.get(logical.payload.index()) else {
        return Ok(None);
    };
    let Some(child_shapes) = shape.children(&payload.semantic_template.operator) else {
        return Ok(None);
    };
    if logical.key.children.len() != child_shapes.len() {
        return Ok(None);
    }
    if let PatternOperand::Expression { children, .. } = operand {
        if children.len() != child_shapes.len() {
            return Ok(None);
        }
    }
    if !ctx.admit_fact_work(
        super::BudgetDimension::RuleWorkPerGroup,
        1 + child_shapes.len(),
    )? {
        return Ok(None);
    }
    let child_groups = ctx
        .memo()
        .logical_expr(expression)
        .ok_or_else(|| paro_error::internal("scalar window expansion lost its expression"))?
        .key
        .children
        .clone();
    let mut result = Vec::with_capacity(child_shapes.len());
    for (slot, child_shape) in child_shapes.iter().copied().enumerate() {
        let fallback = PatternOperand::Group(child_groups[slot]);
        let child = match operand {
            PatternOperand::Expression { children, .. } => &children[slot],
            PatternOperand::Group(_) => &fallback,
        };
        let Some(expanded) = expand_binding(child, child_shape, ctx, state)? else {
            return Ok(None);
        };
        result.push(expanded);
    }
    Ok(Some(PatternOperand::Expression {
        group,
        expression,
        children: result.into_boxed_slice(),
    }))
}

fn rewrite_shell(
    shell: NativeShell,
    layouts: Vec<paro_planner::operator::LogicalOutputLayout>,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let root = shell.root;
    if root + 1 != shell.nodes.len() {
        return Ok(None);
    }
    if matches!(
        shell.root_operator(),
        LogicalOperator::Projection(_) | LogicalOperator::Filter(_)
    ) {
        return rewrite_wrapped_shell(shell, layouts, state);
    }
    let LogicalOperator::Join(Join::Comparison(join)) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if !plain_inner_join(&join) {
        return Ok(None);
    }
    let NativeChild::Node(detail_index) = join.left.clone() else {
        return Ok(None);
    };
    let NativeChild::Node(scalar_index) = join.right.clone() else {
        return Ok(None);
    };
    let Some(rewrite) = recognize(&shell.nodes, &layouts, &join, detail_index, scalar_index)?
    else {
        return Ok(None);
    };

    let detail_layout = layouts
        .get(detail_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost detail layout"))?;
    let detail_width = detail_layout.bindings().len();
    if join.left_projection_map.validate(detail_width).is_err() {
        return Ok(None);
    }
    let detail_projection_indices = join.left_projection_map.to_indices(detail_width);
    let expected_layout = paro_planner::operator::LogicalOutputLayout::new(
        detail_projection_indices
            .iter()
            .filter_map(|index| detail_layout.types().get(*index).cloned())
            .collect(),
        detail_projection_indices
            .iter()
            .filter_map(|index| detail_layout.bindings().get(*index).copied())
            .collect(),
    );
    if expected_layout.len() != detail_projection_indices.len() {
        return Ok(None);
    }
    let window_index = state.bind_context.generate_table_index();
    let window_binding = ColumnBinding::new(window_index, 0);
    let window_type = rewrite.aggregate.return_type.clone();
    let scalar = rewrite.scalar_expression.replace_column_ref(&|column| {
        (column.depth == 0 && column.binding == rewrite.scalar_source_binding).then(|| {
            Expression::ColumnRef(
                ColumnRefExpression::new(window_binding, window_type.clone()).into(),
            )
        })
    });
    if !expression_uses_only_binding(&scalar, window_binding, &window_type) {
        return Ok(None);
    }
    let predicates = join
        .conditions
        .iter()
        .map(|condition| {
            Expression::Comparison(
                ComparisonExpression::new(
                    condition.comparison.into(),
                    condition.left.clone().replace_column_ref(&|column| {
                        (column.depth == 0 && column.binding == rewrite.scalar_binding)
                            .then(|| scalar.clone())
                    }),
                    condition.right.clone().replace_column_ref(&|column| {
                        (column.depth == 0 && column.binding == rewrite.scalar_binding)
                            .then(|| scalar.clone())
                    }),
                )
                .into(),
            )
        })
        .collect::<Vec<_>>();
    if rewrite.scalar_binding != window_binding
        && predicates
            .iter()
            .any(|expression| expression_mentions_binding(expression, rewrite.scalar_binding))
    {
        return Ok(None);
    }

    let frame = WindowFrame {
        frame_type: WindowFrameType::Rows,
        start_bound: WindowFrameBound::Unbounded,
        start_is_preceding: true,
        end_bound: WindowFrameBound::Unbounded,
        end_is_preceding: false,
    };
    let window_expression =
        WindowExpression::aggregate(rewrite.aggregate, Vec::new(), Vec::new(), frame);
    window_expression.verify_bound_contract()?;

    let mut nodes = shell.nodes.into_vec();
    let mut root_node = nodes
        .pop()
        .ok_or_else(|| paro_error::internal("native scalar window lost root node"))?;
    let root_id = root_node.id;
    let root_stats = root_node.stats.clone();
    let has_carriers = !rewrite.detail_path.is_empty();
    let input_filter_width = layouts[rewrite.detail_filter_index].len();
    let mut filter = Filter {
        expressions: predicates,
        child: NativeChild::Node(nodes.len()),
        projection_map: ProjectionMap::new(if has_carriers {
            (0..input_filter_width).collect()
        } else {
            detail_projection_indices.clone()
        }),
    };
    let window_node = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: nodes
            .get(rewrite.detail_filter_index)
            .map(|node| node.stats.clone())
            .unwrap_or_else(NodeStats::default),
        operator: LogicalOperator::Window(Window {
            window_index,
            expressions: vec![window_expression],
            child: NativeChild::Node(rewrite.detail_filter_index),
        }),
        source_proofs: Box::new([]),
    });
    filter.child = NativeChild::Node(window_node);
    root_node.operator = LogicalOperator::Filter(filter);
    root_node.source_proofs = Box::new([]);
    if has_carriers {
        root_node.id = state.bind_context.next_plan_id();
    }
    nodes.push(root_node);
    let mut replacement = nodes.len() - 1;
    // Install before any semi/anti reduction: the scalar aggregate is over
    // the original filtered source, not the surviving reduction rows. Keep
    // each non-preserved sibling as its exact native edge.
    for (carrier_index, left) in rewrite.detail_path.into_iter().rev() {
        let mut carrier_node = nodes[carrier_index].clone();
        let LogicalOperator::Join(Join::Comparison(carrier)) = &mut carrier_node.operator else {
            unreachable!()
        };
        let (child, projection) = if left {
            (&mut carrier.left, &mut carrier.left_projection_map)
        } else {
            (&mut carrier.right, &mut carrier.right_projection_map)
        };
        if carrier_index == detail_index {
            let NativeChild::Node(old_child) = child else {
                return Ok(None);
            };
            let existing = projection.to_indices(layouts[*old_child].len());
            let Some(composed) = detail_projection_indices
                .iter()
                .map(|ordinal| existing.get(*ordinal).copied())
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            *projection = ProjectionMap::new(composed);
            carrier_node.id = root_id;
            carrier_node.stats = root_stats.clone();
        }
        *child = NativeChild::Node(replacement);
        carrier_node.source_proofs = Box::new([]);
        nodes.push(carrier_node);
        replacement = nodes.len() - 1;
    }
    let root = nodes.len() - 1;
    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != expected_layout {
        return Ok(None);
    }
    Ok(Some(shell))
}

/// Keep the selected unary ancestors, but validate their expressions against
/// the rewritten child layout. A removed scalar output cannot be hidden by a
/// projection with an unchanged output type. This operates on native edges,
/// never an exported tree, and invalidates copied ancestor proof lineage.
fn rewrite_wrapped_shell(
    shell: NativeShell,
    mut layouts: Vec<paro_planner::operator::LogicalOutputLayout>,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let expected = layouts
        .get(shell.root)
        .cloned()
        .ok_or_else(|| paro_error::internal("scalar window ancestor lost its output layout"))?;
    let mut join_index = shell.root;
    loop {
        let child = match &shell.nodes[join_index].operator {
            LogicalOperator::Projection(projection) => &projection.child,
            LogicalOperator::Filter(filter) => &filter.child,
            LogicalOperator::Join(Join::Comparison(_)) => break,
            _ => return Ok(None),
        };
        let NativeChild::Node(index) = child else {
            return Ok(None);
        };
        // Expanded shells are postorder. Unary ancestors must be contiguous,
        // so splitting here cannot discard a sibling or a separate witness.
        if index.checked_add(1) != Some(join_index) {
            return Ok(None);
        }
        join_index = *index;
    }
    let mut nodes = shell.nodes.into_vec();
    let ancestors = nodes.split_off(join_index + 1);
    layouts.truncate(join_index + 1);
    let Some(rewritten) = rewrite_shell(
        NativeShell {
            nodes: nodes.into_boxed_slice(),
            root: join_index,
        },
        layouts,
        state,
    )?
    else {
        return Ok(None);
    };
    let mut child_layout = rewritten.root_layout()?;
    let mut child_index = rewritten.root;
    let mut nodes = rewritten.nodes.into_vec();
    for mut ancestor in ancestors {
        let allowed = child_layout.bindings().iter().copied().collect();
        let expressions = match &ancestor.operator {
            LogicalOperator::Projection(projection) => &projection.expressions,
            LogicalOperator::Filter(filter) => &filter.expressions,
            _ => return Ok(None),
        };
        if expressions.iter().any(|expression| {
            let mut unsupported = false;
            ExpressionIterator::visit(expression, &mut |node| {
                if matches!(
                    node,
                    Expression::Reference(_)
                        | Expression::Subquery(_)
                        | Expression::Aggregate(_)
                        | Expression::Window(_)
                ) {
                    unsupported = true;
                    ExpressionVisitDecision::SkipChildren
                } else {
                    ExpressionVisitDecision::Descend
                }
            });
            unsupported || expression_has_binding_outside(expression, &allowed)
        }) {
            return Ok(None);
        }
        match &mut ancestor.operator {
            LogicalOperator::Projection(projection) => {
                projection.child = NativeChild::Node(child_index)
            }
            LogicalOperator::Filter(filter) => filter.child = NativeChild::Node(child_index),
            _ => unreachable!("unary ancestor was checked above"),
        }
        child_layout = ancestor
            .operator
            .output_layout_from_child_refs(&[&child_layout]);
        ancestor.source_proofs = Box::new([]);
        nodes.push(ancestor);
        child_index = nodes.len() - 1;
    }
    if child_layout != expected {
        return Ok(None);
    }
    Ok(Some(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root: child_index,
    }))
}

struct Rewrite {
    detail_filter_index: usize,
    detail_path: Vec<(usize, bool)>,
    scalar_binding: ColumnBinding,
    scalar_source_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
}

fn recognize(
    nodes: &[NativeNode],
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    join: &ComparisonJoin<NativeChild>,
    detail_index: usize,
    scalar_index: usize,
) -> Result<Option<Rewrite>> {
    let mut detail_filter_index = detail_index;
    let mut detail_path = Vec::new();
    loop {
        let Some(node) = nodes.get(detail_filter_index) else {
            return Ok(None);
        };
        let LogicalOperator::Join(Join::Comparison(carrier)) = &node.operator else {
            break;
        };
        if !plain_reduction_carrier(carrier) {
            return Ok(None);
        }
        let left = matches!(carrier.join_type, JoinType::Semi | JoinType::Anti);
        let NativeChild::Node(child) = (if left { &carrier.left } else { &carrier.right }) else {
            return Ok(None);
        };
        detail_path.push((detail_filter_index, left));
        detail_filter_index = *child;
    }
    let LogicalOperator::Filter(detail_filter) = nodes
        .get(detail_filter_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost detail node"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if detail_filter.expressions.is_empty() || !detail_filter.expressions.iter().all(is_movable) {
        return Ok(None);
    }
    let NativeChild::Node(detail_get_index) = detail_filter.child else {
        return Ok(None);
    };
    let LogicalOperator::Get(detail_get) = nodes
        .get(detail_get_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost detail Get"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    let Some(scalar) = peel_scalar_branch(nodes, scalar_index)? else {
        return Ok(None);
    };
    let Some(bindings) = AlphaBindings::match_gets(&detail_get, &scalar.source_get) else {
        return Ok(None);
    };

    let mut matched_scalar = vec![false; scalar.source_filter.expressions.len()];
    for detail_predicate in &detail_filter.expressions {
        let Some(index) = scalar
            .source_filter
            .expressions
            .iter()
            .enumerate()
            .position(|(index, scalar_predicate)| {
                !matched_scalar[index]
                    && bindings.expressions_equal(detail_predicate, scalar_predicate)
            })
        else {
            return Ok(None);
        };
        matched_scalar[index] = true;
    }
    let Some(residual) = scalar
        .source_filter
        .expressions
        .iter()
        .enumerate()
        .filter(|(index, _)| !matched_scalar[*index])
        .map(|(_, expression)| bindings.rebase_scalar(expression))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    let Some(Expression::Aggregate(mut aggregate)) =
        bindings.rebase_scalar(&Expression::Aggregate(scalar.aggregate.clone().into()))
    else {
        return Ok(None);
    };
    if let Some(filter) = aggregate.filter.take() {
        let Some(filter) = and_predicates(std::iter::once(*filter).chain(residual).collect())
        else {
            return Ok(None);
        };
        aggregate.filter = Some(Box::new(filter));
    } else if let Some(filter) = and_predicates(residual) {
        aggregate.filter = Some(Box::new(filter));
    }
    let detail_bindings = layouts
        .get(detail_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost detail layout"))?
        .bindings()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if !aggregate_inputs_available(&aggregate, &detail_bindings) {
        return Ok(None);
    }

    let condition_expressions = join
        .conditions
        .iter()
        .map(|condition| {
            Expression::Comparison(
                ComparisonExpression::new(
                    condition.comparison.into(),
                    condition.left.clone(),
                    condition.right.clone(),
                )
                .into(),
            )
        })
        .collect::<Vec<_>>();
    let scalar_binding = scalar.wrapper_binding;
    if !condition_expressions.iter().all(is_movable)
        || !condition_expressions
            .iter()
            .any(|expression| expression_mentions_binding(expression, scalar_binding))
        || condition_expressions.iter().any(|expression| {
            expression_has_unexpected_binding(expression, &detail_bindings, scalar_binding)
        })
    {
        return Ok(None);
    }
    if join
        .left_projection_map
        .validate(detail_bindings.len())
        .is_err()
    {
        return Ok(None);
    }
    Ok(Some(Rewrite {
        detail_filter_index,
        detail_path,
        scalar_binding,
        scalar_source_binding: ColumnBinding::new(scalar.aggregate_index, 0),
        scalar_expression: scalar.scalar_expression,
        aggregate: aggregate.into_inner(),
    }))
}

fn plain_reduction_carrier<C>(join: &ComparisonJoin<C>) -> bool {
    matches!(
        join.join_type,
        JoinType::Semi | JoinType::Anti | JoinType::RightSemi | JoinType::RightAnti
    ) && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
}

struct ScalarBranch {
    wrapper_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
    aggregate_index: usize,
    source_filter: Filter<NativeChild>,
    source_get: Get,
}

fn peel_scalar_branch(nodes: &[NativeNode], scalar_index: usize) -> Result<Option<ScalarBranch>> {
    let LogicalOperator::Projection(wrapper_projection) = nodes
        .get(scalar_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost scalar projection"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if wrapper_projection.expressions.len() != 1
        || wrapper_projection.returned_types.len() != 1
        || wrapper_projection.visible_names.len() != 1
    {
        return Ok(None);
    }
    let Expression::Operator(checked) = &wrapper_projection.expressions[0] else {
        return Ok(None);
    };
    if checked.operator_type != OperatorType::ErrorIfMultipleRows || checked.children.len() != 2 {
        return Ok(None);
    }
    let NativeChild::Node(wrapper_index) = wrapper_projection.child else {
        return Ok(None);
    };
    let LogicalOperator::Aggregate(wrapper) = nodes
        .get(wrapper_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost wrapper aggregate"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if !plain_scalar_wrapper(&wrapper) {
        return Ok(None);
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        wrapper.aggregates.as_slice()
    else {
        return Ok(None);
    };
    let (canonical_first, _) = get_first_function()
        .bind(std::slice::from_ref(
            &first
                .children
                .first()
                .ok_or_else(|| paro_error::internal("native scalar window FIRST has no input"))?
                .return_type(),
        ))
        .map_err(|_| paro_error::internal("native scalar window cannot bind FIRST"))?;
    if !aggregate_kernels_equal(
        first,
        &AggregateExpression::new(canonical_first, Vec::new(), first.return_type.clone()),
    ) || !aggregate_kernels_equal(
        count,
        &AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt),
    ) {
        return Ok(None);
    }
    let [Expression::ColumnRef(first_output), Expression::ColumnRef(count_output)] =
        checked.children.as_slice()
    else {
        return Ok(None);
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
        return Ok(None);
    }
    let NativeChild::Node(scalar_projection_index) = wrapper.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Projection(scalar_projection) = nodes
        .get(scalar_projection_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost scalar input projection"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if scalar_projection.expressions.len() != 1
        || scalar_projection.returned_types.len() != 1
        || scalar_projection.visible_names.len() != 1
    {
        return Ok(None);
    }
    let scalar_expression = scalar_projection.expressions[0].clone();
    if !is_movable(&scalar_expression)
        || scalar_expression.return_type() != first.return_type
        || !matches!(
            &first.children[0],
            Expression::ColumnRef(column)
                if is_column(
                    column,
                    ColumnBinding::new(scalar_projection.table_index, 0),
                    &scalar_expression.return_type(),
                )
        )
    {
        return Ok(None);
    }
    let NativeChild::Node(reduction_index) = scalar_projection.child else {
        return Ok(None);
    };
    let LogicalOperator::Aggregate(reduction) = nodes
        .get(reduction_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost scalar reduction"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if !plain_ungrouped_aggregate(&reduction) {
        return Ok(None);
    }
    let Expression::Aggregate(aggregate) = reduction
        .aggregates
        .first()
        .ok_or_else(|| paro_error::internal("native scalar window has no scalar aggregate"))?
    else {
        return Ok(None);
    };
    if aggregate.aggr_type != AggregateType::NonDistinct
        || aggregate.function.destructor.is_some()
        || !aggregate.order_bys.is_empty()
        || !is_movable(&reduction.aggregates[0])
        || !expression_uses_only_binding(
            &scalar_expression,
            ColumnBinding::new(reduction.aggregate_index, 0),
            &aggregate.return_type,
        )
    {
        return Ok(None);
    }
    let NativeChild::Node(source_filter_index) = reduction.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Filter(source_filter) = nodes
        .get(source_filter_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost source filter"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if source_filter.expressions.is_empty() || !source_filter.expressions.iter().all(is_movable) {
        return Ok(None);
    }
    let NativeChild::Node(source_get_index) = source_filter.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Get(source_get) = nodes
        .get(source_get_index)
        .ok_or_else(|| paro_error::internal("native scalar window lost source Get"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    Ok(Some(ScalarBranch {
        wrapper_binding: ColumnBinding::new(wrapper_projection.table_index, 0),
        scalar_expression,
        aggregate: aggregate.clone().into_inner(),
        aggregate_index: reduction.aggregate_index,
        source_filter,
        source_get: *source_get,
    }))
}

fn plain_inner_join(join: &ComparisonJoin<NativeChild>) -> bool {
    join.join_type == JoinType::Inner
        && join.anti_join_mode == AntiJoinMode::Regular
        && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
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

fn plain_ungrouped_aggregate(aggregate: &Aggregate<NativeChild>) -> bool {
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
        _ => Some(Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::And, predicates).into(),
        )),
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

fn expression_uses_only_binding(
    expression: &Expression,
    binding: ColumnBinding,
    ty: &LogicalType,
) -> bool {
    let mut saw_binding = false;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            saw_binding |= is_column(column, binding, ty);
            valid &= is_column(column, binding, ty);
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

fn is_column(column: &ColumnRefExpression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    column.depth == 0 && column.binding == binding && &column.return_type == ty
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::ComparisonType;
    use paro_planner::operator::{ComparisonJoin, JoinCondition, Projection};
    use paro_planner::plan::OwnedLogicalPlan;
    use paro_storage::table::table_factory::TableFactory;

    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::transformation::{
        matching, PlannerTransformation, TransformContext,
    };
    use crate::cascades::planner::MemoBuilder;
    use crate::cascades::rules::TransformationRule;

    fn table() -> Arc<TableCatalogEntry> {
        let types = vec![LogicalType::BigInt, LogicalType::Integer];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            "scalar_window_native".to_string(),
            vec![
                ColumnDefinition::new("key".to_string(), types[0].clone()),
                ColumnDefinition::new("value".to_string(), types[1].clone()),
            ],
        );
        Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(93_001), 0)
                .unwrap(),
        )
    }

    fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, index), ty).into())
    }

    fn sum(input: Expression) -> Expression {
        let ty = input.return_type();
        let (function, _) = get_sum_function().bind(std::slice::from_ref(&ty)).unwrap();
        Expression::Aggregate(
            AggregateExpression::new(function.clone(), vec![input], function.return_type.clone())
                .into(),
        )
    }

    fn get(table_index: usize, table: Arc<TableCatalogEntry>) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            table_index,
            vec!["key".to_string(), "value".to_string()],
            vec![LogicalType::BigInt, LogicalType::Integer],
            table,
        ))))
    }

    fn shape() -> OwnedLogicalPlan {
        let table = table();
        let detail_predicate = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                column(10, 0, LogicalType::BigInt),
                Expression::Constant(
                    paro_planner::expression::ConstantExpression::new(
                        paro_common::runtime_value::Value::BigInt(0),
                        LogicalType::BigInt,
                    )
                    .into(),
                ),
            )
            .into(),
        );
        let scalar_predicate = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                column(20, 0, LogicalType::BigInt),
                Expression::Constant(
                    paro_planner::expression::ConstantExpression::new(
                        paro_common::runtime_value::Value::BigInt(0),
                        LogicalType::BigInt,
                    )
                    .into(),
                ),
            )
            .into(),
        );
        let detail = Filter::new(get(10, table.clone()), vec![detail_predicate]);
        let scalar_source = Filter::new(get(20, table), vec![scalar_predicate]);
        let scalar_reduction = Aggregate::new(
            40,
            41,
            42,
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(scalar_source)),
            vec![],
            vec![],
            vec![sum(column(20, 1, LogicalType::Integer))],
            vec![],
        );
        let scalar_projection = Projection::new(
            43,
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(scalar_reduction))),
            vec![column(41, 0, LogicalType::BigInt)],
        );
        let (first, _) = get_first_function().bind(&[LogicalType::BigInt]).unwrap();
        let scalar_wrapper = Aggregate::new(
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
        let scalar = Projection::new(
            53,
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(scalar_wrapper))),
            vec![checked],
        );
        let mut join = ComparisonJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(detail)),
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(scalar)),
            vec![JoinCondition::new(
                column(10, 0, LogicalType::BigInt),
                column(53, 0, LogicalType::BigInt),
                paro_planner::operator::JoinComparisonType::GreaterThan,
            )],
        );
        join.right_projection_map = ProjectionMap::none();
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)))
    }

    #[test]
    fn production_reduction_carrier_binding_stays_native() {
        for kind in [
            JoinType::Semi,
            JoinType::Anti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let mut input = MemoBuilder::build(
                carrier_shape(kind),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let binding = {
                let state = state.read().unwrap();
                matching::scoped_pattern_bindings(
                    PlannerTransformation::ScalarAggregateWindow,
                    input.root,
                    expression,
                    &input.memo,
                    &state,
                    None,
                    BudgetDimension::RuleWorkPerGroup,
                )
                .unwrap()
                .bindings
                .first()
                .cloned()
                .expect("carrier witness")
            };
            let mut ctx = TransformContext::new(&mut input.memo, input.root);
            let shell =
                try_native_scalar_aggregate_window(&binding.root, &mut ctx, &state.read().unwrap())
                    .unwrap()
                    .expect("reduction carrier must not force an owned bridge");
            assert!(
                matches!(shell.root_operator(), LogicalOperator::Join(Join::Comparison(join)) if join.join_type == kind)
            );
            let rule = super::super::PlannerTransformationRule {
                transformation: PlannerTransformation::ScalarAggregateWindow,
                planner_state: state,
            };
            assert_eq!(rule.apply_binding(&binding, &mut ctx).unwrap().len(), 1);
        }
    }

    fn carrier_shape(kind: JoinType) -> OwnedLogicalPlan {
        let mut plan = shape();
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            unreachable!()
        };
        let detail = std::mem::replace(
            &mut *join.left,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let gate = get(60, table());
        let right = matches!(kind, JoinType::RightSemi | JoinType::RightAnti);
        let condition = if right {
            JoinCondition::equality(
                column(60, 0, LogicalType::BigInt),
                column(10, 0, LogicalType::BigInt),
            )
        } else {
            JoinCondition::equality(
                column(10, 0, LogicalType::BigInt),
                column(60, 0, LogicalType::BigInt),
            )
        };
        let (left, right) = if right {
            (gate, detail)
        } else {
            (detail, gate)
        };
        *join.left = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(kind, left, right, vec![condition]),
        )));
        plan
    }

    #[test]
    fn production_binding_builds_native_scalar_window() {
        let mut input =
            MemoBuilder::build(shape(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = {
            let state_read = state.read().unwrap();
            matching::scoped_pattern_bindings(
                PlannerTransformation::ScalarAggregateWindow,
                input.root,
                root_expression,
                &input.memo,
                &state_read,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("scalar window witness should match")
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let state_read = state.read().unwrap();
        let shell = try_native_scalar_aggregate_window(&binding.root, &mut context, &state_read)
            .unwrap()
            .expect("production binding should rewrite");
        assert!(matches!(shell.root_operator(), LogicalOperator::Filter(_)));
    }

    #[test]
    fn production_rule_stages_native_scalar_window() {
        let mut input =
            MemoBuilder::build(shape(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = {
            let state_read = state.read().unwrap();
            matching::scoped_pattern_bindings(
                PlannerTransformation::ScalarAggregateWindow,
                input.root,
                root_expression,
                &input.memo,
                &state_read,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("scalar window witness should match")
        };
        let rule = super::super::PlannerTransformationRule {
            transformation: PlannerTransformation::ScalarAggregateWindow,
            planner_state: state,
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
    }

    #[test]
    fn production_wrapped_binding_preserves_projection_and_residual() {
        let residual = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                column(10, 0, LogicalType::BigInt),
                column(10, 0, LogicalType::BigInt),
            )
            .into(),
        );
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            80,
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                shape(),
                vec![residual],
            ))),
            vec![
                column(10, 1, LogicalType::Integer),
                column(10, 0, LogicalType::BigInt),
            ],
        )));
        let expected = plan.output_layout();
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = {
            let state_read = state.read().unwrap();
            matching::scoped_pattern_bindings(
                PlannerTransformation::ScalarAggregateWindow,
                input.root,
                root_expression,
                &input.memo,
                &state_read,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("wrapped witness should match")
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let shell =
            try_native_scalar_aggregate_window(&binding.root, &mut context, &state.read().unwrap())
                .unwrap()
                .expect("wrapped production binding must remain native");
        assert_eq!(shell.root_layout().unwrap(), expected);
        assert_eq!(
            shell
                .nodes
                .iter()
                .filter(|node| matches!(node.operator, LogicalOperator::Window(_)))
                .count(),
            1
        );
        assert_eq!(
            shell
                .nodes
                .iter()
                .filter(|node| matches!(node.operator, LogicalOperator::Filter(_)))
                .count(),
            3
        );
        let rule = super::super::PlannerTransformationRule {
            transformation: PlannerTransformation::ScalarAggregateWindow,
            planner_state: state,
        };
        assert_eq!(rule.apply_binding(&binding, &mut context).unwrap().len(), 1);
    }

    #[test]
    fn ancestor_cannot_observe_removed_scalar_even_with_same_output_type() {
        let mut plan = shape();
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            unreachable!()
        };
        join.right_projection_map = ProjectionMap::all();
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            80,
            plan,
            vec![column(53, 0, LogicalType::BigInt)],
        )));
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        assert!(try_native_scalar_aggregate_window(
            &PatternOperand::Group(input.root),
            &mut ctx,
            &state
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn scalar_output_contract_cannot_be_dropped() {
        let mut plan = shape();
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            unreachable!();
        };
        join.right_projection_map = ProjectionMap::all();
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        assert!(try_native_scalar_aggregate_window(
            &PatternOperand::Group(input.root),
            &mut ctx,
            &state,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn production_native_shell_matches_independent_nullable_bag_oracle() {
        type Row = Vec<Option<i64>>;
        fn value(
            expr: &Expression,
            row: &Row,
            layout: &paro_planner::operator::LogicalOutputLayout,
        ) -> Option<i64> {
            match expr {
                Expression::ColumnRef(column) => {
                    row[layout
                        .bindings()
                        .iter()
                        .position(|binding| *binding == column.binding)
                        .unwrap()]
                }
                Expression::Constant(constant) => match constant.value {
                    paro_common::runtime_value::Value::BigInt(value) => Some(value),
                    _ => panic!("unsupported oracle constant"),
                },
                Expression::Comparison(cmp) => {
                    assert_eq!(cmp.comparison_type, ComparisonType::GreaterThan);
                    Some(i64::from(
                        value(&cmp.left, row, layout)? > value(&cmp.right, row, layout)?,
                    ))
                }
                _ => panic!("unsupported oracle expression"),
            }
        }
        fn execute(shell: &NativeShell, rows: &[Row]) -> Vec<Row> {
            let layouts = shell.layouts().unwrap();
            let mut results: Vec<Vec<Row>> = Vec::new();
            let gate = vec![
                vec![Some(3), None],
                vec![Some(3), None],
                vec![Some(9), None],
                vec![None, None],
            ];
            for node in &shell.nodes {
                let output = match &node.operator {
                    LogicalOperator::Get(get) => {
                        if get.table_index == 60 {
                            gate.clone()
                        } else {
                            rows.to_vec()
                        }
                    }
                    LogicalOperator::Join(Join::Comparison(join)) => {
                        let child = |edge: &NativeChild| match edge {
                            NativeChild::Node(index) => {
                                (results[*index].clone(), layouts[*index].clone())
                            }
                            NativeChild::Group { layout, .. }
                            | NativeChild::MemoGroup { layout, .. } => {
                                assert_eq!(layout.bindings()[0].table_index, 60);
                                (gate.clone(), layout.clone())
                            }
                        };
                        let (left, left_layout) = child(&join.left);
                        let (right, right_layout) = child(&join.right);
                        let preserve_left =
                            matches!(join.join_type, JoinType::Semi | JoinType::Anti);
                        let anti = matches!(join.join_type, JoinType::Anti | JoinType::RightAnti);
                        let (preserved, other, map, width) = if preserve_left {
                            (&left, &right, &join.left_projection_map, left_layout.len())
                        } else {
                            (
                                &right,
                                &left,
                                &join.right_projection_map,
                                right_layout.len(),
                            )
                        };
                        let projection = map.to_indices(width);
                        preserved
                            .iter()
                            .filter(|row| {
                                let matched = other.iter().any(|peer| {
                                    let (l, r) = if preserve_left {
                                        (*row, peer)
                                    } else {
                                        (peer, *row)
                                    };
                                    join.conditions.iter().all(|condition| {
                                        assert_eq!(
                                            condition.comparison,
                                            paro_planner::operator::JoinComparisonType::Equal
                                        );
                                        value(&condition.left, l, &left_layout)
                                            .zip(value(&condition.right, r, &right_layout))
                                            .is_some_and(|(a, b)| a == b)
                                    })
                                });
                                matched != anti
                            })
                            .map(|row| projection.iter().map(|index| row[*index]).collect())
                            .collect()
                    }
                    LogicalOperator::Projection(projection) => {
                        let NativeChild::Node(child) = projection.child else {
                            panic!("unexpected hole")
                        };
                        results[child]
                            .iter()
                            .map(|row| {
                                projection
                                    .expressions
                                    .iter()
                                    .map(|expr| value(expr, row, &layouts[child]))
                                    .collect()
                            })
                            .collect()
                    }
                    LogicalOperator::Filter(filter) => {
                        let NativeChild::Node(child) = filter.child else {
                            panic!("unexpected hole")
                        };
                        let indices = filter.projection_map.to_indices(layouts[child].len());
                        results[child]
                            .iter()
                            .filter(|row| {
                                filter
                                    .expressions
                                    .iter()
                                    .all(|expr| value(expr, row, &layouts[child]) == Some(1))
                            })
                            .map(|row| indices.iter().map(|index| row[*index]).collect())
                            .collect()
                    }
                    LogicalOperator::Window(window) => {
                        let NativeChild::Node(child) = window.child else {
                            panic!("unexpected hole")
                        };
                        assert_eq!(window.expressions.len(), 1);
                        let invocation = window.expressions[0].aggregate_invocation().unwrap();
                        assert_eq!(invocation.function.name, "sum");
                        let values: Vec<i64> = results[child]
                            .iter()
                            .filter(|row| {
                                invocation
                                    .filter
                                    .as_ref()
                                    .is_none_or(|expr| value(expr, row, &layouts[child]) == Some(1))
                            })
                            .filter_map(|row| value(&invocation.children[0], row, &layouts[child]))
                            .collect();
                        let sum = (!values.is_empty()).then(|| values.iter().sum());
                        results[child]
                            .iter()
                            .map(|row| {
                                let mut row = row.clone();
                                row.push(sum);
                                row
                            })
                            .collect()
                    }
                    _ => panic!("unexpected operator in produced shell"),
                };
                results.push(output);
            }
            results[shell.root].clone()
        }
        for carrier in [
            None,
            Some(JoinType::Semi),
            Some(JoinType::Anti),
            Some(JoinType::RightSemi),
            Some(JoinType::RightAnti),
        ] {
            for wrapped in [false, true] {
                let base = carrier.map_or_else(shape, carrier_shape);
                let plan = if wrapped {
                    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                        80,
                        base,
                        vec![
                            column(10, 1, LogicalType::Integer),
                            column(10, 0, LogicalType::BigInt),
                        ],
                    )))
                } else {
                    base
                };
                let mut input =
                    MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
                let state = input.planner_state.read().unwrap();
                let mut ctx = TransformContext::new(&mut input.memo, input.root);
                let shell = try_native_scalar_aggregate_window(
                    &PatternOperand::Group(input.root),
                    &mut ctx,
                    &state,
                )
                .unwrap()
                .unwrap();
                for rows in [
                    vec![],
                    vec![vec![None, None], vec![Some(1), None]],
                    vec![
                        vec![Some(3), Some(-2)],
                        vec![Some(3), Some(-2)],
                        vec![Some(5), Some(8)],
                        vec![None, Some(100)],
                        vec![Some(-1), Some(200)],
                        vec![Some(9), None],
                    ],
                ] {
                    let selected: Vec<Row> = rows
                        .iter()
                        .filter(|row| row[0].is_some_and(|key| key > 0))
                        .cloned()
                        .collect();
                    let values: Vec<i64> = selected.iter().filter_map(|row| row[1]).collect();
                    let sum: Option<i64> = (!values.is_empty()).then(|| values.iter().sum());
                    let mut expected: Vec<Row> = selected
                        .into_iter()
                        .filter(|row| row[0].zip(sum).is_some_and(|(key, sum)| key > sum))
                        .collect();
                    if let Some(kind) = carrier {
                        let anti = matches!(kind, JoinType::Anti | JoinType::RightAnti);
                        expected
                            .retain(|row| row[0].is_some_and(|key| key == 3 || key == 9) != anti);
                    }
                    if wrapped {
                        for row in &mut expected {
                            row.swap(0, 1);
                        }
                    }
                    assert_eq!(execute(&shell, &rows), expected);
                }
            }
        }
    }

    #[test]
    fn expanded_source_reads_invalidate_and_budget_retry_is_atomic() {
        for plan in [
            shape(),
            carrier_shape(JoinType::Semi),
            carrier_shape(JoinType::RightAnti),
        ] {
            let mut input =
                MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
            let state = input.planner_state.read().unwrap();
            let groups_before = input.memo.group_count();
            let arena_before = state.staging_arena.len();
            input
                .memo
                .group_ledger_mut(input.root)
                .unwrap()
                .set_limit(BudgetDimension::RuleWorkPerGroup, 2);
            {
                let mut ctx = TransformContext::new(&mut input.memo, input.root);
                assert!(try_native_scalar_aggregate_window(
                    &PatternOperand::Group(input.root),
                    &mut ctx,
                    &state,
                )
                .unwrap()
                .is_none());
            }
            assert_eq!(input.memo.group_count(), groups_before);
            assert_eq!(state.staging_arena.len(), arena_before);
            input
                .memo
                .group_ledger_mut(input.root)
                .unwrap()
                .set_limit(BudgetDimension::RuleWorkPerGroup, 65_536);
            let reads = {
                let mut ctx = TransformContext::new(&mut input.memo, input.root);
                assert!(try_native_scalar_aggregate_window(
                    &PatternOperand::Group(input.root),
                    &mut ctx,
                    &state,
                )
                .unwrap()
                .is_some());
                ctx.take_fact_reads()
            };
            assert_eq!(
                reads.len(),
                groups_before,
                "all expanded source groups must be subscribed"
            );
            assert!(reads
                .iter()
                .all(|read| read.is_current(&input.memo).unwrap()));
            let source = reads
                .iter()
                .find(|read| {
                    input
                        .memo
                        .group(read.group)
                        .unwrap()
                        .logical_exprs()
                        .iter()
                        .any(|id| {
                            let payload = input.memo.logical_expr(*id).unwrap().payload;
                            matches!(
                                state.payloads.logical[payload.index()]
                                    .semantic_template
                                    .operator,
                                LogicalOperator::Get(_)
                            )
                        })
                })
                .unwrap();
            input
                .memo
                .group_mut(source.group)
                .unwrap()
                .logical_properties
                .maximum_cardinality = Some(7);
            assert!(!source.is_current(&input.memo).unwrap());
            assert_eq!(input.memo.group_count(), groups_before);
            assert_eq!(state.staging_arena.len(), arena_before);
        }
    }
}
