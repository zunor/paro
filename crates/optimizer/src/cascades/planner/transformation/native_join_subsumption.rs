// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for aggregate join subsumption.
//!
//! The legacy implementation of this rule rewrites an owned logical tree and
//! then converts the closed result back into a native shell.  That conversion
//! is especially expensive for the Q11-shaped reduction join because the
//! matched shell already contains every operator the rule is allowed to
//! inspect.  This module performs the same conservative rewrite over the
//! immutable shell edges instead.
//!
//! The native path is deliberately structural.  It only accepts the exact
//! shapes recognized by the owned rule: plain distributive SUMs, clean inner
//! joins, a direct catalog Get for the detail side, and a one-row-per-key
//! partial aggregate.  Any shape which is not fully visible in the binding
//! remains on the authoritative owned path.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::error as paro_error;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::aggregate::AggregateAlgebra;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator,
    ProjectionMap,
};

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::{boundary, Memo, PatternOperand, PlannerTransformState};

#[derive(Clone)]
struct OuterSum {
    input_binding: ColumnBinding,
    return_type: LogicalType,
}

#[derive(Clone)]
struct DetailScan {
    table: Arc<TableCatalogEntry>,
    table_index: usize,
    key_column_id: usize,
    value_column_id: usize,
}

#[derive(Clone)]
struct ReductionExposure {
    output_binding: ColumnBinding,
    output_type: LogicalType,
    output_index: usize,
    append_projection: Option<(usize, ColumnBinding, LogicalType)>,
}

/// Try the complete native subset of aggregate join subsumption for one
/// pattern binding.  The shell is built directly from the binding; no owned
/// representative or settlement arena is created on the successful path.
pub(super) fn try_native_aggregate_join_subsumption(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> paro_common::error::Result<Option<NativeShell>> {
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let original_root_layout = layouts
        .get(shell.root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native subsumption has no root layout"))?;
    try_native_shell_with_layout(shell, original_root_layout)
}

#[cfg(test)]
fn try_native_shell(shell: NativeShell) -> paro_common::error::Result<Option<NativeShell>> {
    let original_root_layout = shell.root_layout()?;
    try_native_shell_with_layout(shell, original_root_layout)
}

fn try_native_shell_with_layout(
    shell: NativeShell,
    original_root_layout: paro_planner::operator::LogicalOutputLayout,
) -> paro_common::error::Result<Option<NativeShell>> {
    let shell = normalize_empty_filter_edges(shell)?;
    let root = shell.root;
    let LogicalOperator::Aggregate(aggregate) = shell.root_operator().clone() else {
        return Ok(None);
    };
    let Some(outer_sum) = outer_sum(aggregate.as_ref()) else {
        return Ok(None);
    };

    let nodes = shell.nodes.into_vec();
    let Some((mut nodes, child, replacement)) =
        substitute_detail_join(nodes, aggregate.child.clone(), &outer_sum)?
    else {
        return Ok(None);
    };

    let mut rewritten = *aggregate;
    rewritten.child = child;
    rewritten.aggregates[0] = replacement;
    rewritten.group_stats.resize(rewritten.groups.len(), None);
    rewritten.group_dependencies.clear();
    rewritten.group_input_multiplicity = paro_planner::operator::GroupInputMultiplicity::Arbitrary;
    rewritten.returned_types = rewritten
        .groups
        .iter()
        .map(|expression| expression.return_type())
        .chain(
            rewritten
                .aggregates
                .iter()
                .map(|expression| expression.return_type()),
        )
        .chain(
            rewritten
                .grouping_functions
                .iter()
                .map(|_| LogicalType::BigInt),
        )
        .collect();
    nodes[root].operator = LogicalOperator::Aggregate(Box::new(rewritten));
    // The root's scalar expression and child edge changed.  Its old proof
    // describes the source expression, not this native rewrite.
    nodes[root].source_proofs = Box::new([]);

    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_root_layout {
        return Ok(None);
    }
    Ok(Some(shell))
}

/// Resolve identity filters in post-order once, including scan and reduction
/// inputs. This is the tautology part of the owned pushdown prelude, not a
/// substitute for routing nonempty predicates. No Memo hole is expanded and
/// operator payloads stay in place. Unreachable identities are removed by
/// the existing final compactor only if subsumption actually succeeds.
fn normalize_empty_filter_edges(mut shell: NativeShell) -> paro_common::error::Result<NativeShell> {
    let mut aliases = Vec::<Option<NativeChild>>::with_capacity(shell.nodes.len());
    let mut changed = Vec::with_capacity(shell.nodes.len());
    for node in &mut shell.nodes {
        let mut rewritten = false;
        let mut invalid = false;
        node.operator.visit_child_links_mut(&mut |child| {
            if let NativeChild::Node(index) = child {
                let Some(alias) = aliases.get(*index) else {
                    invalid = true;
                    return;
                };
                rewritten |= changed[*index];
                if let Some(alias) = alias {
                    *child = alias.clone();
                    rewritten = true;
                }
            }
        });
        if invalid {
            return Err(paro_error::internal(
                "native filter normalization requires post-order edges",
            ));
        }
        let alias = match &node.operator {
            LogicalOperator::Filter(filter)
                if filter.projection_map.is_all()
                    && !filter.expressions.iter().any(|expression| {
                        expression.evaluation_properties().is_reorder_fence()
                    })
                    && super::FilterPushdown::normalize_predicates(filter.expressions.clone())
                        .is_some_and(|predicates| predicates.is_empty()) =>
            {
                Some(filter.child.clone())
            }
            _ => None,
        };
        rewritten |= alias.is_some();
        if rewritten {
            node.source_proofs = Box::new([]);
        }
        aliases.push(alias);
        changed.push(rewritten);
    }
    // This producer owns an Aggregate root, never an identity filter root.
    Ok(shell)
}

fn outer_sum<Child>(aggregate: &Aggregate<Child>) -> Option<OuterSum> {
    if aggregate.post_reduction.is_some()
        || aggregate.aggregates.len() != 1
        || !aggregate.grouping_functions.is_empty()
    {
        return None;
    }
    let Expression::Aggregate(sum) = &aggregate.aggregates[0] else {
        return None;
    };
    if !is_plain_sum(sum) {
        return None;
    }
    let [Expression::ColumnRef(input)] = sum.children.as_slice() else {
        return None;
    };
    if input.depth != 0
        || aggregate
            .groups
            .iter()
            .any(|group| references_table(group, input.binding.table_index))
    {
        return None;
    }
    Some(OuterSum {
        input_binding: input.binding,
        return_type: sum.return_type.clone(),
    })
}

/// Search the matched join spine in the same order as the owned rule.  The
/// vector is passed by value at branch points so an unsuccessful structural
/// alternative cannot leave a partially mutated shell behind.
fn substitute_detail_join(
    nodes: Vec<NativeNode>,
    current: NativeChild,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<(Vec<NativeNode>, NativeChild, Expression)>> {
    let NativeChild::Node(index) = current.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Join(Join::Comparison(join)) = nodes[index].operator.clone() else {
        return Ok(None);
    };

    if matches!(join.join_type, JoinType::Semi | JoinType::RightSemi) {
        if let Some(result) = try_substitute_reduction_join(nodes.clone(), index, &join, outer_sum)?
        {
            return Ok(Some(result));
        }
    }

    if is_clean_inner_join(&join) {
        if let Some(result) = try_substitute_direct_join(nodes.clone(), index, &join, outer_sum)? {
            return Ok(Some(result));
        }

        if let Some((mut nodes, child, replacement)) =
            substitute_detail_join(nodes.clone(), join.left.clone(), outer_sum)?
        {
            let LogicalOperator::Join(Join::Comparison(mut parent)) = nodes[index].operator.clone()
            else {
                return Err(paro_error::internal(
                    "native subsumption lost its inner join while updating the left edge",
                ));
            };
            parent.left = child;
            nodes[index].operator = LogicalOperator::Join(Join::Comparison(parent));
            nodes[index].source_proofs = Box::new([]);
            return Ok(Some((nodes, NativeChild::Node(index), replacement)));
        }
        if let Some((mut nodes, child, replacement)) =
            substitute_detail_join(nodes, join.right.clone(), outer_sum)?
        {
            let LogicalOperator::Join(Join::Comparison(mut parent)) = nodes[index].operator.clone()
            else {
                return Err(paro_error::internal(
                    "native subsumption lost its inner join while updating the right edge",
                ));
            };
            parent.right = child;
            nodes[index].operator = LogicalOperator::Join(Join::Comparison(parent));
            nodes[index].source_proofs = Box::new([]);
            return Ok(Some((nodes, NativeChild::Node(index), replacement)));
        }
    }
    Ok(None)
}

fn try_substitute_direct_join(
    mut nodes: Vec<NativeNode>,
    index: usize,
    join: &ComparisonJoin<NativeChild>,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<(Vec<NativeNode>, NativeChild, Expression)>> {
    if !is_clean_inner_join(join) || join.conditions.len() != 1 {
        return Ok(None);
    }
    let left_detail = direct_detail_scan(&nodes, &join.left, outer_sum);
    let right_detail = direct_detail_scan(&nodes, &join.right, outer_sum);
    if left_detail.is_some() == right_detail.is_some() {
        return Ok(None);
    }

    let (detail, detail_child, preserved_child) = if let Some(detail) = left_detail {
        (detail, join.left.clone(), join.right.clone())
    } else {
        let Some(detail) = right_detail else {
            return Ok(None);
        };
        (detail, join.right.clone(), join.left.clone())
    };
    let Some((detail_key, preserved_key)) =
        detail_join_keys(&join.conditions[0], detail.table_index)
    else {
        return Ok(None);
    };
    let Some(detail_key_column_id) = direct_detail_stored_column(&nodes, &detail_child, detail_key)
    else {
        return Ok(None);
    };
    let Some(value_column_id) =
        direct_detail_stored_column(&nodes, &detail_child, outer_sum.input_binding)
    else {
        return Ok(None);
    };
    let detail = DetailScan {
        key_column_id: detail_key_column_id,
        value_column_id,
        ..detail
    };

    let Some((replacement, append_projection)) = expose_reduction_sum(
        &mut nodes,
        preserved_child.clone(),
        preserved_key,
        &detail,
        outer_sum,
    )?
    else {
        return Ok(None);
    };
    apply_projection_append(&mut nodes, append_projection);
    // The detail join itself is replaced by the preserved input.  The outer
    // aggregate keeps its original schema; its SUM now consumes the partial
    // aggregate exposed by the reduction path.
    let _ = index;
    Ok(Some((nodes, preserved_child, replacement)))
}

fn try_substitute_reduction_join(
    mut nodes: Vec<NativeNode>,
    index: usize,
    join: &ComparisonJoin<NativeChild>,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<(Vec<NativeNode>, NativeChild, Expression)>> {
    let (preserved, reduction, preserved_is_left) = match join.join_type {
        JoinType::Semi => (join.left.clone(), join.right.clone(), true),
        JoinType::RightSemi => (join.right.clone(), join.left.clone(), false),
        _ => return Ok(None),
    };
    let (preserved_projection, reduction_projection) = if preserved_is_left {
        (&join.left_projection_map, &join.right_projection_map)
    } else {
        (&join.right_projection_map, &join.left_projection_map)
    };
    if join.conditions.len() != 1
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || !reduction_projection.is_none()
    {
        return Ok(None);
    }
    let condition = &join.conditions[0];
    if condition.comparison != JoinComparisonType::Equal {
        return Ok(None);
    }
    let Some(left) = column_binding(&condition.left) else {
        return Ok(None);
    };
    let Some(right) = column_binding(&condition.right) else {
        return Ok(None);
    };
    let preserved_bindings = child_layout(&nodes, &preserved)?.bindings().to_vec();
    let reduction_bindings = child_layout(&nodes, &reduction)?.bindings().to_vec();
    let (preserved_key, reduction_key) =
        if preserved_bindings.contains(&left) && reduction_bindings.contains(&right) {
            (left, right)
        } else if preserved_bindings.contains(&right) && reduction_bindings.contains(&left) {
            (right, left)
        } else {
            return Ok(None);
        };

    let Some(detail) = inspect_detail_edge(&nodes, &preserved, preserved_key, outer_sum)? else {
        return Ok(None);
    };
    let Some(retained_bindings) = projected_bindings(&preserved_bindings, preserved_projection)
    else {
        return Ok(None);
    };
    let retained_bindings = retained_bindings
        .into_iter()
        .filter(|binding| binding.table_index != detail.table_index)
        .collect::<Vec<_>>();
    let Some(exposure) = inspect_reduction(&nodes, &reduction, reduction_key, &detail)? else {
        return Ok(None);
    };
    let Some(replacement) = replacement_sum(&exposure, outer_sum) else {
        return Ok(None);
    };

    let Some(rewritten_preserved) = remove_detail_edge(
        &mut nodes,
        preserved.clone(),
        preserved_key,
        &detail,
        outer_sum,
    )?
    else {
        return Ok(None);
    };
    let rewritten_layout = child_layout(&nodes, &rewritten_preserved)?;
    let rewritten_bindings = rewritten_layout
        .bindings()
        .iter()
        .copied()
        .filter(|binding| binding.table_index != detail.table_index)
        .collect::<Vec<_>>();
    let Some(rewritten_projection) =
        projection_for_binding_layout(&rewritten_bindings, &retained_bindings)
    else {
        return Ok(None);
    };

    apply_projection_append(&mut nodes, exposure.append_projection);
    let LogicalOperator::Join(Join::Comparison(mut rewritten_join)) = nodes[index].operator.clone()
    else {
        return Err(paro_error::internal(
            "native subsumption lost its reduction join while committing",
        ));
    };
    if preserved_is_left {
        rewritten_join.left = rewritten_preserved;
        rewritten_join.left_projection_map = rewritten_projection;
        rewritten_join.right_projection_map = ProjectionMap::new(vec![exposure.output_index]);
    } else {
        rewritten_join.right = rewritten_preserved;
        rewritten_join.right_projection_map = rewritten_projection;
        rewritten_join.left_projection_map = ProjectionMap::new(vec![exposure.output_index]);
    }
    rewritten_join.join_type = JoinType::Inner;
    nodes[index].operator = LogicalOperator::Join(Join::Comparison(rewritten_join));
    nodes[index].source_proofs = Box::new([]);
    Ok(Some((nodes, NativeChild::Node(index), replacement)))
}

type ReductionSumExposure = (Expression, Option<(usize, ColumnBinding, LogicalType)>);

fn expose_reduction_sum(
    nodes: &mut Vec<NativeNode>,
    current: NativeChild,
    preserved_key: ColumnBinding,
    detail: &DetailScan,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<ReductionSumExposure>> {
    let NativeChild::Node(index) = current else {
        return Ok(None);
    };
    match nodes[index].operator.clone() {
        LogicalOperator::Filter(filter) => {
            if !filter.projection_map.is_all() {
                return Ok(None);
            }
            expose_reduction_sum(nodes, filter.child, preserved_key, detail, outer_sum)
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            if let Some((replacement, exposure)) = try_expose_from_reduction_join(
                nodes,
                index,
                &join,
                preserved_key,
                detail,
                outer_sum,
            )? {
                return Ok(Some((replacement, exposure.append_projection)));
            }
            if !is_clean_inner_join(&join) {
                return Ok(None);
            }
            let left_has_key = child_layout(nodes, &join.left)?
                .bindings()
                .contains(&preserved_key);
            let right_has_key = child_layout(nodes, &join.right)?
                .bindings()
                .contains(&preserved_key);
            if left_has_key == right_has_key {
                return Ok(None);
            }
            if left_has_key {
                expose_reduction_sum(nodes, join.left, preserved_key, detail, outer_sum)
            } else {
                expose_reduction_sum(nodes, join.right, preserved_key, detail, outer_sum)
            }
        }
        _ => Ok(None),
    }
}

fn try_expose_from_reduction_join(
    nodes: &mut Vec<NativeNode>,
    index: usize,
    join: &ComparisonJoin<NativeChild>,
    preserved_key: ColumnBinding,
    detail: &DetailScan,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<(Expression, ReductionExposure)>> {
    let (preserved, reduction, reduction_is_right) = match join.join_type {
        JoinType::Semi => (join.left.clone(), join.right.clone(), true),
        JoinType::RightSemi => (join.right.clone(), join.left.clone(), false),
        _ => return Ok(None),
    };
    let reduction_projection = if reduction_is_right {
        &join.right_projection_map
    } else {
        &join.left_projection_map
    };
    if join.conditions.len() != 1
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || !reduction_projection.is_none()
        || !child_layout(nodes, &preserved)?
            .bindings()
            .contains(&preserved_key)
    {
        return Ok(None);
    }
    let condition = &join.conditions[0];
    if condition.comparison != JoinComparisonType::Equal {
        return Ok(None);
    }
    let Some(left) = column_binding(&condition.left) else {
        return Ok(None);
    };
    let Some(right) = column_binding(&condition.right) else {
        return Ok(None);
    };
    let reduction_key = if left == preserved_key {
        right
    } else if right == preserved_key {
        left
    } else {
        return Ok(None);
    };
    let Some(exposure) = inspect_reduction(nodes, &reduction, reduction_key, detail)? else {
        return Ok(None);
    };
    let Some(replacement) = replacement_sum(&exposure, outer_sum) else {
        return Ok(None);
    };

    let Some(mut rewritten_join) = (match nodes[index].operator.clone() {
        LogicalOperator::Join(Join::Comparison(join)) => Some(join),
        _ => None,
    }) else {
        return Err(paro_error::internal(
            "native subsumption lost its semi join while exposing the partial",
        ));
    };
    rewritten_join.join_type = JoinType::Inner;
    if reduction_is_right {
        rewritten_join.right_projection_map = ProjectionMap::new(vec![exposure.output_index]);
    } else {
        rewritten_join.left_projection_map = ProjectionMap::new(vec![exposure.output_index]);
    }
    nodes[index].operator = LogicalOperator::Join(Join::Comparison(rewritten_join));
    nodes[index].source_proofs = Box::new([]);
    Ok(Some((replacement, exposure)))
}

fn inspect_reduction(
    nodes: &[NativeNode],
    current: &NativeChild,
    reduction_key: ColumnBinding,
    detail: &DetailScan,
) -> paro_common::error::Result<Option<ReductionExposure>> {
    let NativeChild::Node(index) = current else {
        return Ok(None);
    };
    if let LogicalOperator::Projection(projection) = nodes[*index].operator.clone() {
        if reduction_key.table_index != projection.table_index {
            return Ok(None);
        }
        let Some(projected_key) = projection
            .expressions
            .get(reduction_key.column_index)
            .and_then(column_binding)
        else {
            return Ok(None);
        };
        let Some((aggregate_binding, aggregate_type)) =
            inspect_reduction_core(nodes, &projection.child, projected_key, detail)?
        else {
            return Ok(None);
        };
        if let Some((index, expression)) = projection
            .expressions
            .iter()
            .enumerate()
            .find(|(_, expression)| column_binding(expression) == Some(aggregate_binding))
        {
            return Ok(Some(ReductionExposure {
                output_binding: ColumnBinding::new(projection.table_index, index),
                output_type: expression.return_type(),
                output_index: index,
                append_projection: None,
            }));
        }
        let output_index = projection.expressions.len();
        return Ok(Some(ReductionExposure {
            output_binding: ColumnBinding::new(projection.table_index, output_index),
            output_type: aggregate_type.clone(),
            output_index,
            append_projection: Some((*index, aggregate_binding, aggregate_type)),
        }));
    }

    let Some((aggregate_binding, aggregate_type)) =
        inspect_reduction_core(nodes, current, reduction_key, detail)?
    else {
        return Ok(None);
    };
    let layout = child_layout(nodes, current)?;
    let bindings = layout.bindings();
    let Some(output_index) = bindings
        .iter()
        .position(|binding| *binding == aggregate_binding)
    else {
        return Ok(None);
    };
    Ok(Some(ReductionExposure {
        output_binding: aggregate_binding,
        output_type: aggregate_type,
        output_index,
        append_projection: None,
    }))
}

fn inspect_reduction_core(
    nodes: &[NativeNode],
    current: &NativeChild,
    reduction_key: ColumnBinding,
    detail: &DetailScan,
) -> paro_common::error::Result<Option<(ColumnBinding, LogicalType)>> {
    let NativeChild::Node(index) = current else {
        return Ok(None);
    };
    match nodes[*index].operator.clone() {
        LogicalOperator::Filter(filter) => {
            if !filter.projection_map.is_all() {
                return Ok(None);
            }
            inspect_reduction_core(nodes, &filter.child, reduction_key, detail)
        }
        LogicalOperator::Aggregate(aggregate) => {
            if aggregate.groups.len() != 1
                || !aggregate.grouping_sets.is_empty()
                || !aggregate.grouping_functions.is_empty()
                || aggregate.post_reduction.is_some()
                || reduction_key != ColumnBinding::new(aggregate.group_index, 0)
            {
                return Ok(None);
            }
            let Expression::ColumnRef(group_key) = &aggregate.groups[0] else {
                return Ok(None);
            };
            if group_key.depth != 0 || group_key.binding.table_index == detail.table_index {
                return Ok(None);
            }
            let NativeChild::Node(get_index) = aggregate.child.clone() else {
                return Ok(None);
            };
            let LogicalOperator::Get(get) = nodes[get_index].operator.clone() else {
                return Ok(None);
            };
            if get.table_index != group_key.binding.table_index
                || !get.runtime_filter_expressions.is_empty()
                || !get
                    .table
                    .as_ref()
                    .is_some_and(|table| Arc::ptr_eq(table, &detail.table))
                || get.stored_column(group_key.binding.column_index) != Some(detail.key_column_id)
            {
                return Ok(None);
            }
            Ok(aggregate
                .aggregates
                .iter()
                .enumerate()
                .find_map(|(index, expression)| {
                    let Expression::Aggregate(sum) = expression else {
                        return None;
                    };
                    if !is_plain_sum(sum) {
                        return None;
                    }
                    let [Expression::ColumnRef(value)] = sum.children.as_slice() else {
                        return None;
                    };
                    (value.depth == 0
                        && value.binding.table_index == get.table_index
                        && get.stored_column(value.binding.column_index)
                            == Some(detail.value_column_id))
                    .then(|| {
                        (
                            ColumnBinding::new(aggregate.aggregate_index, index),
                            sum.return_type.clone(),
                        )
                    })
                }))
        }
        _ => Ok(None),
    }
}

fn replacement_sum(exposure: &ReductionExposure, outer_sum: &OuterSum) -> Option<Expression> {
    let (function, target_types) = get_sum_function()
        .bind(std::slice::from_ref(&exposure.output_type))
        .ok()?;
    if target_types != [exposure.output_type.clone()]
        || function.algebra != Some(AggregateAlgebra::Sum)
        || function.return_type != outer_sum.return_type
    {
        return None;
    }
    Some(Expression::Aggregate(
        AggregateExpression::new(
            function,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(exposure.output_binding, exposure.output_type.clone())
                    .into(),
            )],
            outer_sum.return_type.clone(),
        )
        .into(),
    ))
}

fn inspect_detail_edge(
    nodes: &[NativeNode],
    current: &NativeChild,
    preserved_key: ColumnBinding,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<DetailScan>> {
    let NativeChild::Node(index) = current else {
        return Ok(None);
    };
    let LogicalOperator::Join(Join::Comparison(join)) = nodes[*index].operator.clone() else {
        return Ok(None);
    };
    if !is_clean_inner_join(&join) {
        return Ok(None);
    }
    if join.conditions.len() == 1 {
        let left_detail = direct_detail_scan(nodes, &join.left, outer_sum);
        let right_detail = direct_detail_scan(nodes, &join.right, outer_sum);
        if left_detail.is_some() != right_detail.is_some() {
            let left_has_detail = left_detail.is_some();
            let detail = left_detail
                .or(right_detail)
                .expect("one detail edge exists");
            let Some((detail_key, edge_preserved_key)) =
                detail_join_keys(&join.conditions[0], detail.table_index)
            else {
                return Ok(None);
            };
            if edge_preserved_key == preserved_key {
                let detail_child = if left_has_detail {
                    &join.left
                } else {
                    &join.right
                };
                let Some(key_column_id) =
                    direct_detail_stored_column(nodes, detail_child, detail_key)
                else {
                    return Ok(None);
                };
                let Some(value_column_id) =
                    direct_detail_stored_column(nodes, detail_child, outer_sum.input_binding)
                else {
                    return Ok(None);
                };
                return Ok(Some(DetailScan {
                    key_column_id,
                    value_column_id,
                    ..detail
                }));
            }
        }
    }
    if let Some(detail) = inspect_detail_edge(nodes, &join.left, preserved_key, outer_sum)? {
        return Ok(Some(detail));
    }
    inspect_detail_edge(nodes, &join.right, preserved_key, outer_sum)
}

fn remove_detail_edge(
    nodes: &mut Vec<NativeNode>,
    current: NativeChild,
    preserved_key: ColumnBinding,
    detail: &DetailScan,
    outer_sum: &OuterSum,
) -> paro_common::error::Result<Option<NativeChild>> {
    let NativeChild::Node(index) = current else {
        return Ok(None);
    };
    let LogicalOperator::Join(Join::Comparison(join)) = nodes[index].operator.clone() else {
        return Ok(None);
    };
    if !is_clean_inner_join(&join) {
        return Ok(None);
    }
    if join.conditions.len() == 1 {
        let left_detail = direct_detail_scan(nodes, &join.left, outer_sum)
            .filter(|candidate| same_detail_identity(candidate, detail));
        let right_detail = direct_detail_scan(nodes, &join.right, outer_sum)
            .filter(|candidate| same_detail_identity(candidate, detail));
        if left_detail.is_some() != right_detail.is_some() {
            let Some((_, edge_preserved_key)) =
                detail_join_keys(&join.conditions[0], detail.table_index)
            else {
                return Ok(None);
            };
            let detail_child = if left_detail.is_some() {
                &join.left
            } else {
                &join.right
            };
            let detail_key = detail_join_keys(&join.conditions[0], detail.table_index)
                .map(|(detail_key, _)| detail_key);
            let key_column_id = detail_key
                .and_then(|binding| direct_detail_stored_column(nodes, detail_child, binding));
            let value_column_id =
                direct_detail_stored_column(nodes, detail_child, outer_sum.input_binding);
            if edge_preserved_key == preserved_key
                && key_column_id == Some(detail.key_column_id)
                && value_column_id == Some(detail.value_column_id)
            {
                return Ok(Some(if left_detail.is_some() {
                    join.right
                } else {
                    join.left
                }));
            }
        }
    }
    if let Some(child) =
        remove_detail_edge(nodes, join.left.clone(), preserved_key, detail, outer_sum)?
    {
        let mut rewritten = join;
        rewritten.left = child;
        nodes[index].operator = LogicalOperator::Join(Join::Comparison(rewritten));
        nodes[index].source_proofs = Box::new([]);
        return Ok(Some(NativeChild::Node(index)));
    }
    if let Some(child) =
        remove_detail_edge(nodes, join.right.clone(), preserved_key, detail, outer_sum)?
    {
        let mut rewritten = match nodes[index].operator.clone() {
            LogicalOperator::Join(Join::Comparison(join)) => join,
            _ => unreachable!("native subsumption parent remained a join"),
        };
        rewritten.right = child;
        nodes[index].operator = LogicalOperator::Join(Join::Comparison(rewritten));
        nodes[index].source_proofs = Box::new([]);
        return Ok(Some(NativeChild::Node(index)));
    }
    Ok(None)
}

fn direct_detail_scan(
    nodes: &[NativeNode],
    child: &NativeChild,
    outer_sum: &OuterSum,
) -> Option<DetailScan> {
    let NativeChild::Node(index) = child else {
        return None;
    };
    let LogicalOperator::Get(get) = &nodes.get(*index)?.operator else {
        return None;
    };
    if get.table_index != outer_sum.input_binding.table_index
        || !get.runtime_filter_expressions.is_empty()
    {
        return None;
    }
    Some(DetailScan {
        table: get.table.as_ref()?.clone(),
        table_index: get.table_index,
        key_column_id: 0,
        value_column_id: 0,
    })
}

fn direct_detail_stored_column(
    nodes: &[NativeNode],
    child: &NativeChild,
    binding: ColumnBinding,
) -> Option<usize> {
    let NativeChild::Node(index) = child else {
        return None;
    };
    let LogicalOperator::Get(get) = &nodes.get(*index)?.operator else {
        return None;
    };
    (get.table_index == binding.table_index).then(|| get.stored_column(binding.column_index))?
}

fn same_detail_identity(left: &DetailScan, right: &DetailScan) -> bool {
    left.table_index == right.table_index && Arc::ptr_eq(&left.table, &right.table)
}

fn child_layout(
    nodes: &[NativeNode],
    child: &NativeChild,
) -> paro_common::error::Result<paro_planner::operator::LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => super::native_shell_layouts_for_nodes(nodes)?
            .get(*index)
            .cloned()
            .ok_or_else(|| paro_error::internal("native subsumption child has no layout")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
            Ok(layout.clone())
        }
    }
}

fn projected_bindings(
    child_bindings: &[ColumnBinding],
    projection: &ProjectionMap,
) -> Option<Vec<ColumnBinding>> {
    match projection.as_columns() {
        None => Some(child_bindings.to_vec()),
        Some(indices) => indices
            .iter()
            .map(|index| child_bindings.get(*index).copied())
            .collect(),
    }
}

fn projection_for_binding_layout(
    child_bindings: &[ColumnBinding],
    bindings: &[ColumnBinding],
) -> Option<ProjectionMap> {
    bindings
        .iter()
        .map(|binding| {
            child_bindings
                .iter()
                .position(|candidate| candidate == binding)
        })
        .collect::<Option<Vec<_>>>()
        .map(ProjectionMap::new)
}

fn apply_projection_append(
    nodes: &mut [NativeNode],
    append: Option<(usize, ColumnBinding, LogicalType)>,
) {
    let Some((index, aggregate_binding, aggregate_type)) = append else {
        return;
    };
    let LogicalOperator::Projection(mut projection) = nodes[index].operator.clone() else {
        return;
    };
    projection.expressions.push(Expression::ColumnRef(
        ColumnRefExpression::new(aggregate_binding, aggregate_type.clone()).into(),
    ));
    projection
        .visible_names
        .push("partial_aggregate".to_string());
    projection.visible_count += 1;
    projection.returned_types.push(aggregate_type);
    nodes[index].operator = LogicalOperator::Projection(projection);
    nodes[index].source_proofs = Box::new([]);
}

fn detail_join_keys(
    condition: &paro_planner::operator::JoinCondition,
    detail_table_index: usize,
) -> Option<(ColumnBinding, ColumnBinding)> {
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let left = column_binding(&condition.left)?;
    let right = column_binding(&condition.right)?;
    match (
        left.table_index == detail_table_index,
        right.table_index == detail_table_index,
    ) {
        (true, false) => Some((left, right)),
        (false, true) => Some((right, left)),
        _ => None,
    }
}

fn is_plain_sum(aggregate: &AggregateExpression) -> bool {
    aggregate.function.algebra == Some(AggregateAlgebra::Sum)
        && aggregate.aggr_type == AggregateType::NonDistinct
        && aggregate.filter.is_none()
        && aggregate.order_bys.is_empty()
        && aggregate.children.len() == 1
}

fn is_clean_inner_join(join: &ComparisonJoin<NativeChild>) -> bool {
    join.join_type == JoinType::Inner
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join.left_projection_map.is_all()
        && join.right_projection_map.is_all()
}

fn column_binding(expression: &Expression) -> Option<ColumnBinding> {
    let Expression::ColumnRef(column) = expression else {
        return None;
    };
    (column.depth == 0).then_some(column.binding)
}

fn references_table(expression: &Expression, table_index: usize) -> bool {
    if matches!(expression, Expression::ColumnRef(column) if column.depth == 0 && column.binding.table_index == table_index)
    {
        return true;
    }
    let mut found = false;
    paro_planner::expression::ExpressionIterator::enumerate_children(expression, |child| {
        if !found {
            found = references_table(child, table_index);
        }
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::{
        matching, PlannerTransformation, PlannerTransformationRule, TransformContext,
    };
    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::MemoBuilder;
    use crate::cascades::rules::TransformationRule;
    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::operator::{ExpressionGet, Get, JoinCondition, Projection};
    use paro_planner::plan::OwnedLogicalPlan;
    use paro_storage::table::table_factory::TableFactory;
    use std::collections::HashMap;

    const OUTER_DETAIL: usize = 10;
    const INNER_DETAIL: usize = 20;
    const PRESERVED: usize = 30;
    const INNER_GROUP: usize = 40;
    const INNER_AGGREGATE: usize = 41;
    const REDUCTION_PROJECTION: usize = 50;
    const OUTER_GROUP: usize = 60;
    const OUTER_AGGREGATE: usize = 61;

    fn decimal(precision: u8) -> LogicalType {
        LogicalType::Decimal {
            precision,
            scale: 2,
        }
    }

    fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, index), ty).into())
    }

    fn sum(input: Expression) -> Expression {
        let input_type = input.return_type();
        let (function, targets) = get_sum_function()
            .bind(std::slice::from_ref(&input_type))
            .unwrap();
        assert_eq!(targets, [input_type]);
        Expression::Aggregate(
            AggregateExpression::new(function.clone(), vec![input], function.return_type.clone())
                .into(),
        )
    }

    fn detail_table(object_id: u64) -> Arc<TableCatalogEntry> {
        let types = vec![LogicalType::BigInt, decimal(15)];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            format!("native_subsumption_{object_id}"),
            vec![
                ColumnDefinition::new("key".to_string(), types[0].clone()),
                ColumnDefinition::new("value".to_string(), types[1].clone()),
            ],
        );
        Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(object_id), 0)
                .unwrap(),
        )
    }

    fn get(table_index: usize, table: Arc<TableCatalogEntry>) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            table_index,
            vec!["key".to_string(), "value".to_string()],
            vec![LogicalType::BigInt, decimal(15)],
            table,
        ))))
    }

    fn q18_shape(table: Arc<TableCatalogEntry>) -> OwnedLogicalPlan {
        let inner_aggregate =
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
                INNER_GROUP,
                INNER_AGGREGATE,
                42,
                get(INNER_DETAIL, table.clone()),
                vec![column(INNER_DETAIL, 0, LogicalType::BigInt)],
                vec![],
                vec![sum(column(INNER_DETAIL, 1, decimal(15)))],
                vec![],
            ))));
        let reduction = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            REDUCTION_PROJECTION,
            inner_aggregate,
            vec![column(INNER_GROUP, 0, LogicalType::BigInt)],
        )));
        let semi = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Semi,
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                PRESERVED,
                vec![],
                vec!["key".to_string()],
                vec![LogicalType::BigInt],
            ))),
            reduction,
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(REDUCTION_PROJECTION, 0, LogicalType::BigInt),
            )],
        )));
        let detail_join = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            semi,
            get(OUTER_DETAIL, table),
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(OUTER_DETAIL, 0, LogicalType::BigInt),
            )],
        )));
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
            OUTER_GROUP,
            OUTER_AGGREGATE,
            62,
            detail_join,
            vec![column(PRESERVED, 0, LogicalType::BigInt)],
            vec![],
            vec![sum(column(OUTER_DETAIL, 1, decimal(15)))],
            vec![],
        ))))
    }

    fn reduction_wraps_detail_join(table: Arc<TableCatalogEntry>) -> OwnedLogicalPlan {
        let inner_aggregate =
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
                INNER_GROUP,
                INNER_AGGREGATE,
                42,
                get(INNER_DETAIL, table.clone()),
                vec![column(INNER_DETAIL, 0, LogicalType::BigInt)],
                vec![],
                vec![sum(column(INNER_DETAIL, 1, decimal(15)))],
                vec![],
            ))));
        let reduction = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            REDUCTION_PROJECTION,
            inner_aggregate,
            vec![column(INNER_GROUP, 0, LogicalType::BigInt)],
        )));
        let detail_join = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                PRESERVED,
                vec![],
                vec!["key".to_string()],
                vec![LogicalType::BigInt],
            ))),
            get(OUTER_DETAIL, table),
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(OUTER_DETAIL, 0, LogicalType::BigInt),
            )],
        )));
        let mut outer = match Join::comparison(
            JoinType::Semi,
            detail_join,
            reduction,
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(REDUCTION_PROJECTION, 0, LogicalType::BigInt),
            )],
        ) {
            Join::Comparison(join) => join,
            _ => unreachable!(),
        };
        outer.left_projection_map = ProjectionMap::new(vec![0, 2]);
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
            OUTER_GROUP,
            OUTER_AGGREGATE,
            62,
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(outer))),
            vec![column(PRESERVED, 0, LogicalType::BigInt)],
            vec![],
            vec![sum(column(OUTER_DETAIL, 1, decimal(15)))],
            vec![],
        ))))
    }

    #[test]
    fn native_shell_rewrites_direct_detail_and_reduction_edges() {
        let source = q18_shape(detail_table(79_001));
        let source_layout = source.output_layout();
        let shell = NativeShell::from_owned(source, &HashMap::new()).unwrap();
        let rewritten = try_native_shell(shell).unwrap().expect("native rewrite");
        assert_eq!(rewritten.root_layout().unwrap(), source_layout);

        let LogicalOperator::Aggregate(outer) = rewritten.root_operator() else {
            panic!("expected outer aggregate")
        };
        let Expression::Aggregate(sum) = &outer.aggregates[0] else {
            panic!("expected outer sum")
        };
        let [Expression::ColumnRef(partial)] = sum.children.as_slice() else {
            panic!("expected partial reference")
        };
        assert_eq!(partial.binding, ColumnBinding::new(REDUCTION_PROJECTION, 1));

        let NativeChild::Node(semi_index) = outer.child.clone() else {
            panic!("detail join should be removed")
        };
        let LogicalOperator::Join(Join::Comparison(semi)) = &rewritten.nodes[semi_index].operator
        else {
            panic!("expected reduction join")
        };
        assert_eq!(semi.join_type, JoinType::Inner);
        assert_eq!(semi.right_projection_map, ProjectionMap::new(vec![1]));

        let NativeChild::Node(projection_index) = semi.right.clone() else {
            panic!("expected reduction projection")
        };
        let LogicalOperator::Projection(projection) = &rewritten.nodes[projection_index].operator
        else {
            panic!("expected projected reduction")
        };
        assert_eq!(projection.expressions.len(), 2);
    }

    #[test]
    fn native_shell_rewrites_outer_reduction_join_with_projection_contract() {
        let source = reduction_wraps_detail_join(detail_table(79_004));
        let source_layout = source.output_layout();
        let shell = NativeShell::from_owned(source, &HashMap::new()).unwrap();
        let rewritten = try_native_shell(shell).unwrap().expect("native rewrite");
        assert_eq!(rewritten.root_layout().unwrap(), source_layout);

        let LogicalOperator::Aggregate(outer) = rewritten.root_operator() else {
            panic!("expected outer aggregate")
        };
        let Expression::Aggregate(sum) = &outer.aggregates[0] else {
            panic!("expected outer sum")
        };
        let [Expression::ColumnRef(partial)] = sum.children.as_slice() else {
            panic!("expected partial reference")
        };
        assert_eq!(partial.binding, ColumnBinding::new(REDUCTION_PROJECTION, 1));
        let NativeChild::Node(join_index) = outer.child.clone() else {
            panic!("expected rewritten reduction join")
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &rewritten.nodes[join_index].operator
        else {
            panic!("expected comparison join")
        };
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.left_projection_map, ProjectionMap::new(vec![0]));
        assert_eq!(join.right_projection_map, ProjectionMap::new(vec![1]));
    }

    #[test]
    fn production_binding_builds_the_same_native_subsumption_shell() {
        let plan = q18_shape(detail_table(79_002));
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateJoinSubsumption,
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
        .expect("the production pattern should expose the complete reduction shell");
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .expect("production binding should have boundary facts");
        let shell =
            try_native_aggregate_join_subsumption(&binding.root, context.memo(), &state, &facts)
                .unwrap()
                .expect("production binding should take the native path");
        assert_eq!(shell.root_layout().unwrap().len(), 2);
    }

    #[test]
    fn native_subsumption_does_not_erase_false_null_or_projecting_filters() {
        use paro_common::runtime_value::Value;
        for (value, projected, expected) in [
            (Value::Boolean(true), false, true),
            (Value::Boolean(false), false, false),
            (Value::Null(LogicalType::Boolean), false, false),
            (Value::Boolean(true), true, false),
        ] {
            let mut plan = q18_shape(detail_table(79_004));
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            let child = std::mem::replace(
                &mut *aggregate.child,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            );
            let mut filter = paro_planner::operator::Filter::new(
                child,
                vec![Expression::Constant(
                    paro_planner::expression::ConstantExpression::new(value, LogicalType::Boolean)
                        .into(),
                )],
            );
            if projected {
                filter.projection_map = ProjectionMap::none();
            }
            *aggregate.child = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter));
            let reference = expected.then(|| {
                let normalized = super::super::FilterPushdown::new().rewrite_plan(
                    paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
                        &plan,
                        BindContext::new().shared(),
                    ),
                );
                let (reference, changed) =
                    crate::rewrite::aggregate::join_subsumption::optimize_root_with_change(
                        normalized,
                    );
                assert!(changed);
                reference
            });
            let shell = NativeShell::from_owned(plan, &HashMap::new()).unwrap();
            let rewritten = try_native_shell(shell).unwrap();
            assert_eq!(rewritten.is_some(), expected);
            if let (Some(rewritten), Some(reference)) = (rewritten, reference) {
                assert_eq!(rewritten.root_layout().unwrap(), reference.output_layout());
                let LogicalOperator::Aggregate(actual) = rewritten.root_operator() else {
                    panic!("native result must retain the outer aggregate")
                };
                let LogicalOperator::Aggregate(expected) = &reference.operator else {
                    panic!("reference result must retain the outer aggregate")
                };
                assert_eq!(actual.groups.len(), expected.groups.len());
                assert!(actual
                    .groups
                    .iter()
                    .zip(&expected.groups)
                    .all(|(a, b)| a.equals(b)));
                assert_eq!(actual.aggregates.len(), expected.aggregates.len());
                assert!(actual
                    .aggregates
                    .iter()
                    .zip(&expected.aggregates)
                    .all(|(a, b)| a.equals(b)));
            }
        }
    }

    #[test]
    fn production_apply_stages_subsumption_without_owned_settlement() {
        for location in ["none", "spine", "detail", "detail-stack"] {
            let mut plan = q18_shape(detail_table(79_003));
            if matches!(location, "detail" | "detail-stack") {
                plan = plan.try_fold_post_order(|plan, _: Vec<()>| {
                    let plan = if matches!(&plan.operator, LogicalOperator::Get(get) if get.table_index == OUTER_DETAIL) {
                        let mut plan = plan;
                        for _ in 0..if location == "detail-stack" { 3 } else { 1 } {
                            plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                                paro_planner::operator::Filter::new(plan, vec![Expression::Constant(
                                    paro_planner::expression::ConstantExpression::new(
                                        paro_common::runtime_value::Value::Boolean(true),
                                        LogicalType::Boolean,
                                    ).into(),
                                )]),
                            ));
                        }
                        plan
                    } else { plan };
                    Ok((plan, ()))
                }).unwrap().0;
            }
            if location == "spine" {
                let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                    unreachable!()
                };
                let child = std::mem::replace(
                    &mut *aggregate.child,
                    OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                );
                *aggregate.child = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                    paro_planner::operator::Filter::new(
                        child,
                        vec![Expression::Constant(
                            paro_planner::expression::ConstantExpression::new(
                                paro_common::runtime_value::Value::Boolean(true),
                                LogicalType::Boolean,
                            )
                            .into(),
                        )],
                    ),
                ));
            }
            let mut input =
                MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let binding = {
                let state = state.read().unwrap();
                let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
                let binding = matching::scoped_pattern_bindings(
                    PlannerTransformation::AggregateJoinSubsumption,
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
                .expect("the production pattern should match");
                binding
            };
            let arena_before = state.read().unwrap().staging_arena.len();
            let rule = PlannerTransformationRule {
                transformation: PlannerTransformation::AggregateJoinSubsumption,
                planner_state: state.clone(),
            };
            let mut context = TransformContext::new(&mut input.memo, input.root);
            let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
            let outputs = rule.apply_binding(&binding, &mut context).unwrap();
            assert_eq!(outputs.len(), 1);
            assert_eq!(
                super::super::semantic_plan::owned_binding_instantiation_count(),
                bridges,
                "location={location}"
            );
            assert_eq!(state.read().unwrap().staging_arena.len(), arena_before);
        }
    }
}
