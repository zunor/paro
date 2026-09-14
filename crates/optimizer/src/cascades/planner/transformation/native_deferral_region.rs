// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::aggregate::dimension_deferral::join_region as contract;
use paro_planner::operator::{JoinCondition, LogicalOutputLayout, ProjectionMap};

/// Recover the observed group of an unchanged selected edge, not an arbitrary
/// equivalent child. Newly synthesized region joins have no original group.
pub(super) fn selected_group(
    shell: &NativeShell,
    binding: &PatternOperand,
    target: usize,
) -> Option<GroupId> {
    let mut pending = vec![(shell.root, binding)];
    while let Some((index, operand)) = pending.pop() {
        let PatternOperand::Expression {
            group, children, ..
        } = operand
        else {
            continue;
        };
        if index == target {
            return Some(*group);
        }
        let mut edges = Vec::new();
        shell.nodes[index]
            .operator
            .visit_child_links(&mut |child| edges.push(child.clone()));
        if edges.len() != children.len() {
            return None;
        }
        for (edge, child) in edges.into_iter().zip(children.iter()) {
            if let NativeChild::Node(index) = edge {
                pending.push((index, child));
            }
        }
    }
    None
}

/// Isolate the same widest dimension as the reference rule, using only selected
/// native edges. Opaque children retain their original group and facts.
pub(super) fn isolate(
    shell: &mut NativeShell,
    layouts: &mut Vec<LogicalOutputLayout>,
    root: usize,
    aggregate: &Aggregate<NativeChild>,
    state: &PlannerTransformState,
) -> Result<Option<usize>> {
    fn collect(
        edge: NativeChild,
        shell: &NativeShell,
        layouts: &[LogicalOutputLayout],
        relations: &mut Vec<NativeChild>,
        conditions: &mut Vec<JoinCondition>,
    ) -> Result<bool> {
        if let NativeChild::Node(index) = &edge {
            if let LogicalOperator::Join(Join::Comparison(join)) = &shell.nodes[*index].operator {
                if contract::is_plain_inner_equi_join(join) {
                    // Never drop a physical enforcement or a restricted output
                    // layout while flattening a semantic region.
                    if join.build_side_constraint
                        != paro_planner::operator::JoinBuildSideConstraint::Either
                        || !join
                            .left_projection_map
                            .is_identity(native_dimension_child_layout(&join.left, layouts)?.len())
                        || !join
                            .right_projection_map
                            .is_identity(native_dimension_child_layout(&join.right, layouts)?.len())
                    {
                        return Ok(false);
                    }
                    if !collect(join.left.clone(), shell, layouts, relations, conditions)?
                        || !collect(join.right.clone(), shell, layouts, relations, conditions)?
                    {
                        return Ok(false);
                    }
                    conditions.extend(join.conditions.iter().cloned());
                    return Ok(true);
                }
            }
        }
        relations.push(edge);
        Ok(true)
    }
    fn bindings(
        edge: &NativeChild,
        layouts: &[LogicalOutputLayout],
    ) -> Result<HashSet<ColumnBinding>> {
        Ok(native_dimension_child_layout(edge, layouts)?
            .bindings()
            .iter()
            .copied()
            .collect())
    }
    fn table(edge: &NativeChild, shell: &NativeShell) -> Option<usize> {
        let NativeChild::Node(index) = edge else {
            return None;
        };
        match &shell.nodes[*index].operator {
            LogicalOperator::Get(get) => Some(get.table_index),
            LogicalOperator::CTERef(reference) => Some(reference.table_index),
            _ => None,
        }
    }
    let mut relations = Vec::new();
    let mut conditions = Vec::new();
    if !collect(
        NativeChild::Node(root),
        shell,
        layouts,
        &mut relations,
        &mut conditions,
    )? {
        return Ok(None);
    }
    if relations.len() < 2 {
        return Ok(Some(root));
    }
    let mut dimensions = Vec::new();
    for relation in &relations {
        if let Some(table) = table(relation, shell) {
            dimensions.push((table, bindings(relation, layouts)?));
        }
    }
    let all = bindings(&NativeChild::Node(root), layouts)?;
    let Some(selected) = contract::select_dimension(
        &aggregate.groups,
        &aggregate.aggregates,
        &all,
        dimensions.into_iter(),
        &conditions.iter().collect::<Vec<_>>(),
    ) else {
        return Ok(Some(root));
    };
    let position = relations
        .iter()
        .position(|edge| table(edge, shell) == Some(selected))
        .ok_or_else(|| paro_error::internal("native dimension disappeared from region"))?;
    if relations.len() == 2 && position == 1 {
        return Ok(Some(root));
    }
    let dimension = relations.remove(position);
    let dimension_bindings = bindings(&dimension, layouts)?;
    let (boundary, mut conditions): (Vec<_>, Vec<_>) =
        conditions.into_iter().partition(|condition| {
            contract::expression_references_any(&condition.left, &dimension_bindings)
                || contract::expression_references_any(&condition.right, &dimension_bindings)
        });
    let LogicalOperator::Join(Join::Comparison(template)) = &shell.nodes[root].operator else {
        return Ok(None);
    };
    let template = template.clone();
    let mut additions = Vec::new();
    let base = shell.nodes.len();
    let mut current = relations.remove(0);
    let mut current_bindings = bindings(&current, layouts)?;
    let relation_bindings = relations
        .iter()
        .map(|edge| bindings(edge, layouts))
        .collect::<Result<Vec<_>>>()?;
    let mut remaining = relations
        .into_iter()
        .zip(relation_bindings)
        .collect::<Vec<_>>();
    let mut append = |left: NativeChild,
                      right: NativeChild,
                      conditions: Vec<JoinCondition>|
     -> Result<NativeChild> {
        let mut join = template.clone();
        join.left = left;
        join.right = right;
        join.conditions = conditions;
        join.left_projection_map = ProjectionMap::all();
        join.right_projection_map = ProjectionMap::all();
        let left = native_dimension_child_layout(&join.left, layouts)?;
        let right = native_dimension_child_layout(&join.right, layouts)?;
        let operator = LogicalOperator::Join(Join::Comparison(join));
        layouts.push(operator.output_layout_from_child_refs(&[&left, &right]));
        let index = base + additions.len();
        additions.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator,
            source_proofs: Box::new([]),
        });
        Ok(NativeChild::Node(index))
    };
    // Track bindings independently while appending: this is the same connected
    // left-deep construction and condition orientation as the reference path.
    while !remaining.is_empty() {
        let position = remaining
            .iter()
            .position(|(_, other)| {
                conditions.iter().any(|condition| {
                    contract::condition_crosses_boundary(condition, &current_bindings, other)
                })
            })
            .ok_or_else(|| paro_error::internal("fact join region became disconnected"))?;
        let (relation, other) = remaining.remove(position);
        let mut joined = Vec::new();
        conditions.retain(|condition| {
            if contract::condition_crosses_boundary(condition, &current_bindings, &other) {
                joined.push(condition.clone());
                false
            } else {
                true
            }
        });
        let joined = joined
            .into_iter()
            .map(|condition| contract::orient_condition(condition, &current_bindings, &other))
            .collect::<Result<Vec<_>>>()?;
        current = append(current, relation, joined)?;
        current_bindings.extend(other);
    }
    if !conditions.is_empty() {
        return Err(paro_error::internal(
            "fact join region retained unassigned predicates",
        ));
    }
    let boundary = boundary
        .into_iter()
        .map(|condition| {
            contract::orient_condition(condition, &current_bindings, &dimension_bindings)
        })
        .collect::<Result<Vec<_>>>()?;
    let NativeChild::Node(index) = append(current, dimension, boundary)? else {
        unreachable!()
    };
    if let Some(final_join) = additions.last_mut() {
        final_join.id = shell.nodes[root].id;
        final_join.stats = shell.nodes[root].stats.clone();
    }
    let mut nodes = std::mem::take(&mut shell.nodes).into_vec();
    nodes.extend(additions);
    shell.nodes = nodes.into_boxed_slice();
    Ok(Some(index))
}
