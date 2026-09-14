// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the conservative join-elimination rule.
//!
//! `JoinElimination` only removes a comparison join when the discarded side
//! is not observable and a complete unique-key witness is available.  The
//! legacy implementation walks an owned tree to calculate the same required
//! bindings and then settles the surviving tree back into the Memo.  This
//! adapter performs that calculation over the immutable shell edges instead.
//!
//! The adapter is intentionally fail-closed.  It supports the transparent
//! relational shells used by the scoped outer-join matcher and the exact
//! comparison-join proof used by the owned rule.  An unfamiliar operator,
//! control boundary, or incomplete key fact declines the native path and
//! leaves the authoritative owned implementation available.

use std::collections::HashSet;

use paro_catalog::entry::ConstraintType;
use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::{Expression, ExpressionIterator};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinComparisonType, LogicalOperator, Projection,
};

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::{boundary, Memo, PatternOperand, PlannerTransformState};

/// Completion of this selected rewrite, independent of global search state.
pub(super) enum EliminationResult {
    /// Coverage is missing, or the shell/output contract could not be built.
    Unsupported,
    /// The entire selected shell was checked. This is not search completion.
    NoRewrite,
    Rewritten(NativeShell),
}

pub(super) fn apply_native_join_elimination(
    root_binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<EliminationResult> {
    let Some((shell, _layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, root_binding, facts)?
    else {
        return Ok(EliminationResult::Unsupported);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(EliminationResult::Unsupported);
    }
    rewrite_shell_result(shell)
}

#[cfg(test)]
fn rewrite_shell(shell: NativeShell) -> Result<Option<NativeShell>> {
    Ok(match rewrite_shell_result(shell)? {
        EliminationResult::Rewritten(shell) => Some(shell),
        _ => None,
    })
}

fn rewrite_shell_result(shell: NativeShell) -> Result<EliminationResult> {
    let root = shell.root;
    let layouts = shell.layouts()?;
    let original_layout = layouts
        .get(root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native join elimination has no root layout"))?;
    let required = output_bindings_from_layout(&original_layout);
    let mut nodes = shell.nodes.into_vec();
    let Some((replacement, changed)) = rewrite_node(&mut nodes, &layouts, root, &required)? else {
        return Ok(EliminationResult::Unsupported);
    };
    if !changed {
        return Ok(EliminationResult::NoRewrite);
    }
    if !matches!(replacement, NativeChild::Node(index) if index == root) {
        return Ok(EliminationResult::Unsupported);
    }

    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_layout {
        return Ok(EliminationResult::Unsupported);
    }
    Ok(EliminationResult::Rewritten(shell))
}

/// Rewrite children before trying to remove the current comparison join.
/// Returning the replacement child lets a parent consume a surviving Memo
/// boundary without creating an owned subtree for the discarded join.
fn rewrite_node(
    nodes: &mut [NativeNode],
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    index: usize,
    required: &HashSet<ColumnBinding>,
) -> Result<Option<(NativeChild, bool)>> {
    let source_operator = nodes
        .get(index)
        .ok_or_else(|| paro_error::internal("native join elimination lost a node"))?
        .operator
        .clone();
    let mut source_children = Vec::new();
    source_operator.visit_child_links(&mut |child| source_children.push(child.clone()));
    let Some(required_children) = required_children(&source_operator, layouts, required) else {
        // A leaf which cannot contain the witness is harmless.  An unknown
        // operator with descendants could contain a join, so decline the
        // entire native binding rather than silently skipping that path.
        return Ok(source_children
            .is_empty()
            .then_some((NativeChild::Node(index), false)));
    };
    if required_children.len() != source_children.len() {
        return Err(paro_error::internal(
            "native join elimination child requirement arity mismatch",
        ));
    }

    let mut replacements = Vec::with_capacity(source_children.len());
    let mut changed = false;
    for (child, child_required) in source_children.iter().zip(required_children) {
        let replacement = match child {
            NativeChild::Node(child_index) => {
                let Some((replacement, child_changed)) =
                    rewrite_node(nodes, layouts, *child_index, &child_required)?
                else {
                    return Ok(None);
                };
                changed |= child_changed;
                replacement
            }
            NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => child.clone(),
        };
        replacements.push(replacement);
    }

    let mut replacements = replacements.into_iter();
    let operator = source_operator.try_map_child_links(&mut |_| {
        replacements
            .next()
            .ok_or_else(|| paro_error::internal("native join elimination lost a child replacement"))
    })?;
    if replacements.next().is_some() {
        return Err(paro_error::internal(
            "native join elimination retained excess child replacements",
        ));
    }
    nodes[index].operator = operator;
    if changed {
        // A copied proof belongs to the old child choices.  The transformed
        // parent is rechecked by the Memo verifier and receives the current
        // rule proof during staging.
        nodes[index].source_proofs = Box::new([]);
    }

    let replacement = match &nodes[index].operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            eliminate_join(nodes, layouts, join, required)
                .or_else(|| Some(NativeChild::Node(index)))
        }
        _ => Some(NativeChild::Node(index)),
    };
    let Some(replacement) = replacement else {
        return Ok(None);
    };
    let eliminated = !matches!(&replacement, NativeChild::Node(replacement_index) if *replacement_index == index);
    Ok(Some((replacement, changed || eliminated)))
}

/// Calculate the same conservative child requirements as the owned rule.  We
/// deliberately cover only operators whose child contracts are explicit in
/// this adapter; an unsupported descendant is a native miss, not an implicit
/// proof that it is irrelevant.
fn required_children(
    operator: &LogicalOperator<NativeChild>,
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    required: &HashSet<ColumnBinding>,
) -> Option<Vec<HashSet<ColumnBinding>>> {
    let child_bindings = |child: &NativeChild| output_bindings_for_child(layouts, child);
    let only_child = |child: &NativeChild, mut child_required: HashSet<ColumnBinding>| {
        child_required.retain(|binding| child_bindings(child).contains(binding));
        vec![child_required]
    };
    match operator {
        LogicalOperator::Filter(filter) => {
            let mut child_required =
                filter_required_bindings(required, &child_bindings(&filter.child));
            collect_bindings_from_exprs(&filter.expressions, &mut child_required);
            Some(only_child(&filter.child, child_required))
        }
        LogicalOperator::Projection(projection) => Some(vec![projection_child_required_bindings(
            projection,
            required,
            &child_bindings(&projection.child),
        )]),
        LogicalOperator::Limit(limit) => {
            let mut child_required =
                filter_required_bindings(required, &child_bindings(&limit.child));
            if let Some(expression) = &limit.limit {
                collect_bindings_from_expr(expression, &mut child_required);
            }
            if let Some(expression) = &limit.offset {
                collect_bindings_from_expr(expression, &mut child_required);
            }
            Some(only_child(&limit.child, child_required))
        }
        LogicalOperator::Order(order) => {
            let mut child_required =
                filter_required_bindings(required, &child_bindings(&order.child));
            for order_by in &order.orders {
                collect_bindings_from_expr(&order_by.expression, &mut child_required);
            }
            Some(only_child(&order.child, child_required))
        }
        LogicalOperator::TopN(topn) => {
            let mut child_required =
                filter_required_bindings(required, &child_bindings(&topn.child));
            for order_by in &topn.orders {
                collect_bindings_from_expr(&order_by.expression, &mut child_required);
            }
            Some(only_child(&topn.child, child_required))
        }
        LogicalOperator::Aggregate(aggregate) => {
            let mut child_required = HashSet::new();
            collect_bindings_from_exprs(&aggregate.groups, &mut child_required);
            collect_bindings_from_exprs(&aggregate.aggregates, &mut child_required);
            Some(only_child(&aggregate.child, child_required))
        }
        LogicalOperator::Distinct(distinct) => {
            let mut child_required = if distinct.distinct_targets.is_empty() {
                // Ordinary DISTINCT compares every input column, including
                // columns hidden by a later projection.
                child_bindings(&distinct.child)
            } else {
                filter_required_bindings(required, &child_bindings(&distinct.child))
            };
            collect_bindings_from_exprs(&distinct.distinct_targets, &mut child_required);
            if let Some(orders) = &distinct.order_by {
                for order in orders {
                    collect_bindings_from_expr(&order.expression, &mut child_required);
                }
            }
            Some(only_child(&distinct.child, child_required))
        }
        LogicalOperator::Window(window) => {
            let mut child_required =
                filter_required_bindings(required, &child_bindings(&window.child));
            // We retain every invocation in this shell. Ignoring dependencies
            // of an unselected output would leave a dangling scalar reference.
            for expression in &window.expressions {
                ExpressionIterator::enumerate_window_children(expression, |child| {
                    collect_bindings_from_expr(child, &mut child_required);
                });
            }
            Some(only_child(&window.child, child_required))
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let left_bindings = child_bindings(&join.left);
            let right_bindings = child_bindings(&join.right);
            let mut left_required = filter_required_bindings(required, &left_bindings);
            let mut right_required = filter_required_bindings(required, &right_bindings);
            add_join_local_bindings(
                join,
                &left_bindings,
                &mut left_required,
                &right_bindings,
                &mut right_required,
            );
            Some(vec![left_required, right_required])
        }
        LogicalOperator::Join(Join::Any(join)) => {
            let left_bindings = child_bindings(&join.left);
            let right_bindings = child_bindings(&join.right);
            let mut left_required = filter_required_bindings(required, &left_bindings);
            let mut right_required = filter_required_bindings(required, &right_bindings);
            add_bindings_for_child(&join.condition, &left_bindings, &mut left_required);
            add_bindings_for_child(&join.condition, &right_bindings, &mut right_required);
            Some(vec![left_required, right_required])
        }
        LogicalOperator::Join(Join::Cross(join)) => {
            let left_bindings = child_bindings(&join.left);
            let right_bindings = child_bindings(&join.right);
            Some(vec![
                filter_required_bindings(required, &left_bindings),
                filter_required_bindings(required, &right_bindings),
            ])
        }
        LogicalOperator::SetOperation(setop) => {
            // A set operator owns a new output namespace. Its branches must
            // retain their complete positional contracts (also for DISTINCT,
            // INTERSECT and EXCEPT); parent bindings do not name branch columns.
            Some(vec![
                child_bindings(&setop.left),
                child_bindings(&setop.right),
            ])
        }
        LogicalOperator::EmptyResult(empty) => Some(only_child(
            &empty.child,
            filter_required_bindings(required, &child_bindings(&empty.child)),
        )),
        LogicalOperator::DependentJoin(join) => {
            let mut left = child_bindings(&join.left);
            let mut right = child_bindings(&join.right);
            if let Some(payload) = join.any_all_payload() {
                collect_bindings_from_exprs(&payload.expression_children, &mut left);
                collect_bindings_from_exprs(&payload.expression_children, &mut right);
            }
            if let Some(condition) = join.join_condition() {
                collect_bindings_from_expr(condition, &mut left);
                collect_bindings_from_expr(condition, &mut right);
            }
            Some(vec![left, right])
        }
        LogicalOperator::Update(update) => {
            let mut child_required = child_bindings(&update.child);
            collect_bindings_from_exprs(&update.expressions, &mut child_required);
            Some(vec![child_required])
        }
        LogicalOperator::Explain(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Insert(_)
        | LogicalOperator::GraphExpand(_) => {
            // These wrappers consume their children's complete contracts.
            // They are traversed, never removed or moved across a boundary.
            let mut children = Vec::new();
            operator.visit_child_links(&mut |child| children.push(child_bindings(child)));
            Some(children)
        }
        _ => {
            let mut children = Vec::new();
            operator.visit_child_links(&mut |child| children.push(child));
            children.is_empty().then(Vec::new)
        }
    }
}

fn eliminate_join(
    nodes: &[NativeNode],
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    join: &ComparisonJoin<NativeChild>,
    required: &HashSet<ColumnBinding>,
) -> Option<NativeChild> {
    if join.join_type == paro_planner::operator::JoinType::Left
        && join_shape_supported(join)
        && !has_required_bindings_from_child(required, &join.right, layouts)
        && conditions_cover_unique_key(nodes, join, &join.right, true)
    {
        return Some(join.left.clone());
    }
    if join.join_type == paro_planner::operator::JoinType::Right
        && join_shape_supported(join)
        && !has_required_bindings_from_child(required, &join.left, layouts)
        && conditions_cover_unique_key(nodes, join, &join.left, false)
    {
        return Some(join.right.clone());
    }
    None
}

fn join_shape_supported(join: &ComparisonJoin<NativeChild>) -> bool {
    join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
}

fn conditions_cover_unique_key(
    nodes: &[NativeNode],
    join: &ComparisonJoin<NativeChild>,
    eliminated: &NativeChild,
    eliminate_right: bool,
) -> bool {
    let mut key_bindings = HashSet::new();
    for condition in &join.conditions {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        let preserved = if eliminate_right {
            &condition.left
        } else {
            &condition.right
        };
        let discarded = if eliminate_right {
            &condition.right
        } else {
            &condition.left
        };
        if !matches!(preserved, Expression::ColumnRef(_)) {
            return false;
        }
        let Expression::ColumnRef(column) = discarded else {
            return false;
        };
        key_bindings.insert(column.binding);
    }
    if key_bindings.is_empty() {
        return false;
    }
    match eliminated {
        NativeChild::MemoGroup { reference, .. } | NativeChild::Group { reference, .. } => {
            reference.facts.unique_keys.iter().any(|key| {
                !key.columns.is_empty()
                    && key
                        .columns
                        .iter()
                        .all(|column| key_bindings.contains(&column.binding))
            })
        }
        NativeChild::Node(index) => match &nodes[*index].operator {
            LogicalOperator::Get(get) => {
                let Some(table) = get.table.as_ref() else {
                    return false;
                };
                let key_columns = key_bindings
                    .iter()
                    .filter(|binding| binding.table_index == get.table_index)
                    .filter_map(|binding| get.stored_column(binding.column_index))
                    .collect::<HashSet<_>>();
                table.constraints().iter().any(|constraint| {
                    matches!(
                        constraint.constraint_type,
                        ConstraintType::Unique | ConstraintType::PrimaryKey
                    ) && !constraint.columns.is_empty()
                        && constraint
                            .columns
                            .iter()
                            .all(|column| key_columns.contains(column))
                })
            }
            LogicalOperator::BoundReference(reference) => {
                reference.facts.unique_keys.iter().any(|key| {
                    !key.columns.is_empty()
                        && key
                            .columns
                            .iter()
                            .all(|column| key_bindings.contains(&column.binding))
                })
            }
            _ => false,
        },
    }
}

fn output_bindings_from_layout(
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> HashSet<ColumnBinding> {
    layout.bindings().iter().copied().collect()
}

fn output_bindings_for_child(
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    child: &NativeChild,
) -> HashSet<ColumnBinding> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .map(output_bindings_from_layout)
            .unwrap_or_default(),
        NativeChild::MemoGroup { reference, .. } | NativeChild::Group { reference, .. } => {
            reference.bindings.iter().copied().collect()
        }
    }
}

fn filter_required_bindings(
    required: &HashSet<ColumnBinding>,
    child_bindings: &HashSet<ColumnBinding>,
) -> HashSet<ColumnBinding> {
    required
        .iter()
        .copied()
        .filter(|binding| child_bindings.contains(binding))
        .collect()
}

fn projection_child_required_bindings(
    projection: &Projection<NativeChild>,
    required: &HashSet<ColumnBinding>,
    child_bindings: &HashSet<ColumnBinding>,
) -> HashSet<ColumnBinding> {
    let mut child_required = HashSet::new();
    for (index, expression) in projection.expressions.iter().enumerate() {
        if required.contains(&ColumnBinding::new(projection.table_index, index)) {
            collect_bindings_from_expr(expression, &mut child_required);
        }
    }
    child_required.retain(|binding| child_bindings.contains(binding));
    child_required
}

fn has_required_bindings_from_child(
    required: &HashSet<ColumnBinding>,
    child: &NativeChild,
    layouts: &[paro_planner::operator::LogicalOutputLayout],
) -> bool {
    let child_bindings = output_bindings_for_child(layouts, child);
    required
        .iter()
        .any(|binding| child_bindings.contains(binding))
}

fn add_join_local_bindings(
    join: &ComparisonJoin<NativeChild>,
    left_bindings: &HashSet<ColumnBinding>,
    left_required: &mut HashSet<ColumnBinding>,
    right_bindings: &HashSet<ColumnBinding>,
    right_required: &mut HashSet<ColumnBinding>,
) {
    for condition in &join.conditions {
        add_bindings_for_child(&condition.left, left_bindings, left_required);
        add_bindings_for_child(&condition.right, left_bindings, left_required);
        add_bindings_for_child(&condition.left, right_bindings, right_required);
        add_bindings_for_child(&condition.right, right_bindings, right_required);
    }
    for expression in &join.duplicate_eliminated_columns {
        add_bindings_for_child(expression, left_bindings, left_required);
        add_bindings_for_child(expression, right_bindings, right_required);
    }
}

fn add_bindings_for_child(
    expression: &Expression,
    child_bindings: &HashSet<ColumnBinding>,
    required: &mut HashSet<ColumnBinding>,
) {
    let mut bindings = HashSet::new();
    collect_bindings_from_expr(expression, &mut bindings);
    required.extend(
        bindings
            .into_iter()
            .filter(|binding| child_bindings.contains(binding)),
    );
}

fn collect_bindings_from_exprs(expressions: &[Expression], bindings: &mut HashSet<ColumnBinding>) {
    for expression in expressions {
        collect_bindings_from_expr(expression, bindings);
    }
}

fn collect_bindings_from_expr(expression: &Expression, bindings: &mut HashSet<ColumnBinding>) {
    if let Expression::ColumnRef(column) = expression {
        bindings.insert(column.binding);
        return;
    }
    ExpressionIterator::enumerate_children(expression, |child| {
        collect_bindings_from_expr(child, bindings);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use paro_common::types::LogicalType;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::bound_reference::{
        BoundReference, BoundReferenceId, BoundRelationFactValues,
    };
    use paro_planner::operator::{Get, JoinCondition, JoinType, LogicalOperator, Projection};
    use paro_planner::plan::{
        OwnedLogicalPlan, UniqueKey, UniqueKeyColumn, UniqueKeyNullSemantics, UniqueKeyProvenance,
    };

    fn column(table: usize, ordinal: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, ordinal), LogicalType::Integer)
                .into(),
        )
    }

    fn boundary(table: usize, unique: bool) -> OwnedLogicalPlan {
        let binding = ColumnBinding::new(table, 0);
        let facts = BoundRelationFactValues {
            can_replay: true,
            contains_control_region: false,
            unique_keys: if unique {
                vec![UniqueKey::new(
                    [UniqueKeyColumn {
                        output_index: 0,
                        binding,
                    }],
                    UniqueKeyProvenance::Structural,
                    UniqueKeyNullSemantics::NullsDistinct,
                )]
            } else {
                Vec::new()
            },
            ..BoundRelationFactValues::default()
        };
        let reference = BoundReference::new(
            BoundReferenceId::group_hole(table as u32),
            vec![binding],
            vec![LogicalType::Integer],
        )
        .with_facts(Arc::new(
            paro_planner::operator::bound_reference::BoundRelationFacts::new(
                facts,
                vec![LogicalType::Integer],
            ),
        ))
        .unwrap();
        OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(reference))
    }

    fn candidate(right_unique: bool, project_right: bool) -> OwnedLogicalPlan {
        let join = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Left,
            boundary(0, false),
            boundary(1, right_unique),
            vec![JoinCondition::equality(column(0, 0), column(1, 0))],
        )));
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            10,
            join,
            vec![column(if project_right { 1 } else { 0 }, 0)],
        )))
    }

    fn memo_candidate() -> OwnedLogicalPlan {
        let left = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(0, vec!["key".to_string()], vec![LogicalType::Integer]),
        )));
        let right = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(1, vec!["key".to_string()], vec![LogicalType::Integer]),
        )));
        let join = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Left,
            left,
            right,
            vec![JoinCondition::equality(column(0, 0), column(1, 0))],
        )));
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            10,
            join,
            vec![column(0, 0)],
        )))
    }

    #[test]
    fn native_elimination_matches_owned_reference_for_both_outer_sides() {
        for unique in [false, true] {
            for observed in [false, true] {
                for right_outer in [false, true] {
                    let make_source = || {
                        let mut source = candidate(unique, observed);
                        if right_outer {
                            let LogicalOperator::Projection(projection) = &mut source.operator
                            else {
                                unreachable!()
                            };
                            let LogicalOperator::Join(Join::Comparison(join)) =
                                &mut projection.child.operator
                            else {
                                unreachable!()
                            };
                            std::mem::swap(&mut join.left, &mut join.right);
                            join.join_type = JoinType::Right;
                            join.conditions =
                                vec![JoinCondition::equality(column(1, 0), column(0, 0))];
                        }
                        source
                    };
                    let source = make_source();
                    let (expected, changed) = crate::join::elimination::JoinElimination::new()
                        .optimize_plan_with_change(make_source());
                    let actual =
                        rewrite_shell(NativeShell::from_owned(source, &HashMap::new()).unwrap())
                            .unwrap();
                    assert_eq!(actual.is_some(), changed);
                    if let Some(actual) = actual {
                        let expected = NativeShell::from_owned(expected, &HashMap::new()).unwrap();
                        assert_eq!(
                            actual.root_layout().unwrap(),
                            expected.root_layout().unwrap()
                        );
                        assert_eq!(
                            format!("{:?}", actual.root_operator()),
                            format!("{:?}", expected.root_operator()),
                            "native success must include the entire reference rewrite"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn distinct_comparison_columns_remain_required_below_outer_projection() {
        for target in [None, Some(0), Some(1)] {
            let make = || {
                let mut plan = candidate(true, false);
                let LogicalOperator::Projection(projection) = &mut plan.operator else {
                    unreachable!()
                };
                let join = std::mem::replace(
                    &mut *projection.child,
                    OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                );
                let distinct = match target {
                    None => paro_planner::operator::Distinct::new(join),
                    Some(side) => {
                        paro_planner::operator::Distinct::distinct_on(vec![column(side, 0)], join)
                    }
                };
                *projection.child =
                    OwnedLogicalPlan::synthetic(LogicalOperator::Distinct(distinct));
                plan
            };
            let (_, changed) =
                crate::join::elimination::JoinElimination::new().optimize_plan_with_change(make());
            let native =
                rewrite_shell(NativeShell::from_owned(make(), &HashMap::new()).unwrap()).unwrap();
            assert_eq!(changed, target == Some(0));
            assert_eq!(native.is_some(), changed);
        }
    }

    #[test]
    fn set_operations_rewrite_both_selected_branches_with_independent_layouts() {
        use paro_planner::operator::{SetOpType, SetOperation};
        for kind in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
            for all in [false, true] {
                let make = || {
                    let right = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(
                        Projection::new(20, candidate(true, false), vec![column(10, 0)]),
                    ));
                    OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
                        40,
                        candidate(true, false),
                        right,
                        kind,
                        all,
                        vec![LogicalType::Integer],
                    )))
                };
                let (reference, changed) = crate::join::elimination::JoinElimination::new()
                    .optimize_plan_with_change(make());
                assert!(changed);
                let native =
                    rewrite_shell(NativeShell::from_owned(make(), &HashMap::new()).unwrap())
                        .unwrap()
                        .expect("both selected branches should be covered natively");
                assert_eq!(native.root_layout().unwrap(), reference.output_layout());
                assert!(!native
                    .nodes
                    .iter()
                    .any(|node| matches!(node.operator, LogicalOperator::Join(_))));
                let LogicalOperator::SetOperation(setop) = native.root_operator() else {
                    panic!("set operation disappeared")
                };
                assert_eq!((setop.setop_type, setop.setop_all), (kind, all));
                let layouts = native.layouts().unwrap();
                assert!(output_bindings_for_child(&layouts, &setop.left)
                    .contains(&ColumnBinding::new(10, 0)));
                assert!(output_bindings_for_child(&layouts, &setop.right)
                    .contains(&ColumnBinding::new(20, 0)));
            }
        }
    }

    #[test]
    fn wrapper_traversal_matches_reference_without_changing_output_contracts() {
        for kind in 0..3 {
            let make = || match kind {
                0 => OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(
                    paro_planner::operator::EmptyResult::new(candidate(true, false)),
                )),
                1 => OwnedLogicalPlan::synthetic(LogicalOperator::Explain(
                    paro_planner::operator::Explain::new(
                        candidate(true, false),
                        paro_planner::operator::ExplainSpec::default(),
                    ),
                )),
                _ => OwnedLogicalPlan::synthetic(LogicalOperator::DependentJoin(Box::new(
                    paro_planner::operator::DependentJoin::scalar(
                        candidate(true, false),
                        boundary(2, false),
                        vec![],
                        None,
                    ),
                ))),
            };
            let (reference, changed) =
                crate::join::elimination::JoinElimination::new().optimize_plan_with_change(make());
            assert!(changed);
            let native = rewrite_shell(NativeShell::from_owned(make(), &HashMap::new()).unwrap())
                .unwrap()
                .expect("wrapper should be traversed");
            assert_eq!(native.root_layout().unwrap(), reference.output_layout());
            assert_eq!(
                native.root_operator().op_type(),
                reference.operator.op_type()
            );
            assert!(!native
                .nodes
                .iter()
                .any(|node| matches!(node.operator, LogicalOperator::Join(_))));
        }
    }

    #[test]
    fn unsupported_root_replacement_is_not_a_complete_negative() {
        let plan = candidate(true, false);
        let LogicalOperator::Projection(projection) = plan.into_operator() else {
            unreachable!()
        };
        let mut join_plan = *projection.child;
        let LogicalOperator::Join(Join::Comparison(join)) = &mut join_plan.operator else {
            unreachable!()
        };
        join.right_projection_map = paro_planner::operator::ProjectionMap::none();
        assert!(matches!(
            rewrite_shell_result(NativeShell::from_owned(join_plan, &HashMap::new()).unwrap())
                .unwrap(),
            EliminationResult::Unsupported
        ));
        assert!(matches!(
            rewrite_shell_result(
                NativeShell::from_owned(candidate(false, false), &HashMap::new()).unwrap()
            )
            .unwrap(),
            EliminationResult::NoRewrite
        ));
    }

    #[test]
    fn native_shell_eliminates_unobserved_unique_outer_side() {
        let source = candidate(true, false);
        let expected_layout = source.output_layout();
        let shell = NativeShell::from_owned(source, &HashMap::new()).unwrap();
        let rewritten = rewrite_shell(shell)
            .unwrap()
            .expect("unique, unobserved outer side should be removable");
        assert_eq!(rewritten.root_layout().unwrap(), expected_layout);
        let LogicalOperator::Projection(projection) = rewritten.root_operator() else {
            panic!("expected projection root")
        };
        assert!(matches!(
            projection.child,
            NativeChild::Group { .. } | NativeChild::MemoGroup { .. }
        ));
    }

    #[test]
    fn native_shell_keeps_unique_outer_side_when_projected() {
        let source = candidate(true, true);
        let shell = NativeShell::from_owned(source, &HashMap::new()).unwrap();
        assert!(rewrite_shell(shell).unwrap().is_none());
    }

    #[test]
    fn native_shell_requires_unique_witness_before_elimination() {
        let source = candidate(false, false);
        let shell = NativeShell::from_owned(source, &HashMap::new()).unwrap();
        assert!(rewrite_shell(shell).unwrap().is_none());
    }

    #[test]
    fn production_binding_uses_memo_unique_key_boundary() {
        use crate::cascades::budget::{BudgetDimension, SearchBudget};
        use crate::cascades::planner::transformation::{
            matching, PlannerTransformation, TransformContext,
        };
        use crate::cascades::planner::MemoBuilder;
        use paro_planner::binder::context::BindContext;

        let mut input = MemoBuilder::build(
            memo_candidate(),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let join_group = input
            .memo
            .logical_expr(root_expression)
            .unwrap()
            .key
            .children[0];
        let join_expression = input.memo.group(join_group).unwrap().logical_exprs()[0];
        let right_group = input
            .memo
            .logical_expr(join_expression)
            .unwrap()
            .key
            .children[1];
        let column = {
            let state = input.planner_state.read().unwrap();
            state
                .binding_ids
                .get(1, 0, &LogicalType::Integer)
                .copied()
                .expect("right Memo column should be interned")
        };
        input
            .memo
            .group_mut(right_group)
            .unwrap()
            .logical_properties
            .unique_keys
            .insert(vec![column].into_boxed_slice());
        let state = input.planner_state.read().unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::JoinElimination,
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
        .expect("outer join path should produce a binding");
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = super::boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .expect("production binding should have boundary facts");
        assert!(
            matches!(
                apply_native_join_elimination(&binding.root, context.memo(), &state, &facts)
                    .unwrap(),
                EliminationResult::Rewritten(_)
            ),
            "the native adapter should consume the same Memo binding"
        );
    }

    #[test]
    fn production_apply_stages_native_elimination_candidate() {
        use crate::cascades::budget::{BudgetDimension, SearchBudget};
        use crate::cascades::planner::transformation::{
            matching, PlannerTransformation, PlannerTransformationRule, TransformContext,
        };
        use crate::cascades::planner::MemoBuilder;
        use crate::cascades::rules::TransformationRule;
        use paro_context::TestStatementContextBuilder;
        use paro_planner::binder::context::BindContext;

        for unique in [false, true] {
            for wrapper in 0..5 {
                let wrapped = wrapper != 0;
                let plan = if wrapped {
                    let child = if wrapper == 1 {
                        OwnedLogicalPlan::synthetic(LogicalOperator::Distinct(
                            paro_planner::operator::Distinct::new(memo_candidate()),
                        ))
                    } else if wrapper == 2 {
                        window(memo_candidate(), column(10, 0))
                    } else if wrapper == 4 {
                        OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(
                            paro_planner::operator::EmptyResult::new(memo_candidate()),
                        ))
                    } else {
                        OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(
                            paro_planner::operator::SetOperation::union(
                                40,
                                memo_candidate(),
                                OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                                    Get::new_without_table(
                                        41,
                                        vec!["key".into()],
                                        vec![LogicalType::Integer],
                                    ),
                                ))),
                                true,
                                vec![LogicalType::Integer],
                            ),
                        ))
                    };
                    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                        20,
                        child,
                        vec![column(if wrapper == 3 { 40 } else { 10 }, 0)],
                    )))
                } else {
                    memo_candidate()
                };
                let mut input =
                    MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
                let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
                let mut join_group = input
                    .memo
                    .logical_expr(root_expression)
                    .unwrap()
                    .key
                    .children[0];
                if wrapped {
                    for _ in 0..2 {
                        let expression = input.memo.group(join_group).unwrap().logical_exprs()[0];
                        join_group = input.memo.logical_expr(expression).unwrap().key.children[0];
                    }
                }
                let join_expression = input.memo.group(join_group).unwrap().logical_exprs()[0];
                let right_group = input
                    .memo
                    .logical_expr(join_expression)
                    .unwrap()
                    .key
                    .children[1];
                let column = {
                    let state = input.planner_state.read().unwrap();
                    state
                        .binding_ids
                        .get(1, 0, &LogicalType::Integer)
                        .copied()
                        .expect("right Memo column should be interned")
                };
                input
                    .memo
                    .group_mut(right_group)
                    .unwrap()
                    .logical_properties
                    .unique_keys
                    .insert(vec![column].into_boxed_slice());

                if !unique {
                    input
                        .memo
                        .group_mut(right_group)
                        .unwrap()
                        .logical_properties
                        .unique_keys
                        .clear();
                }
                let state = input.planner_state.clone();
                state.write().unwrap().session =
                    Some(TestStatementContextBuilder::minimal().build());
                let binding = {
                    let state_read = state.read().unwrap();
                    matching::scoped_pattern_bindings(
                        PlannerTransformation::JoinElimination,
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
                    .expect("outer join path should produce a binding")
                };
                let rule = PlannerTransformationRule {
                    transformation: PlannerTransformation::JoinElimination,
                    planner_state: state,
                };
                let mut context = TransformContext::new(&mut input.memo, input.root);
                let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
                let outputs = rule.apply_binding(&binding, &mut context).unwrap();
                assert_eq!(
                    outputs.len(),
                    usize::from(unique),
                    "a complete native elimination must not stage the same owned peer"
                );
                assert_eq!(
                    super::super::semantic_plan::owned_binding_instantiation_count(),
                    bridges,
                    "selected ancestor must not force owned construction"
                );
                if !unique {
                    let reads = context.take_fact_reads();
                    drop(context);
                    input
                        .memo
                        .group_mut(right_group)
                        .unwrap()
                        .logical_properties
                        .unique_keys
                        .insert(vec![column].into_boxed_slice());
                    assert!(reads
                        .iter()
                        .any(|read| !read.is_current(&input.memo).unwrap()));
                    let mut context = TransformContext::new(&mut input.memo, input.root);
                    assert_eq!(rule.apply_binding(&binding, &mut context).unwrap().len(), 1);
                    assert_eq!(
                        super::super::semantic_plan::owned_binding_instantiation_count(),
                        bridges
                    );
                }
            }
        }
    }

    fn window(child: OwnedLogicalPlan, input: Expression) -> OwnedLogicalPlan {
        use paro_planner::expression::{AggregateExpression, WindowExpression, WindowFrame};
        let (function, _) = paro_function::aggregate::distributive::count::get_count_function()
            .bind(&[LogicalType::Integer])
            .unwrap();
        let invocation = AggregateExpression::new(function, vec![input], LogicalType::BigInt);
        OwnedLogicalPlan::synthetic(LogicalOperator::Window(
            paro_planner::operator::Window::new(
                30,
                vec![WindowExpression::aggregate(
                    invocation,
                    vec![],
                    vec![],
                    WindowFrame::default(),
                )],
                child,
            ),
        ))
    }

    #[test]
    fn retained_window_invocation_cannot_reference_an_eliminated_side() {
        let make = || {
            let mut plan = candidate(true, false);
            let LogicalOperator::Projection(projection) = &mut plan.operator else {
                unreachable!()
            };
            let join = std::mem::replace(
                &mut *projection.child,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            );
            *projection.child = window(join, column(1, 0));
            plan
        };
        let (_, changed) =
            crate::join::elimination::JoinElimination::new().optimize_plan_with_change(make());
        assert!(
            !changed,
            "the Window invocation is still present even if its output is not selected"
        );
        assert!(
            rewrite_shell(NativeShell::from_owned(make(), &HashMap::new()).unwrap())
                .unwrap()
                .is_none()
        );
    }
}
