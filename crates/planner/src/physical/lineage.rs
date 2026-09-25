// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact positional lineage through row-preserving physical operators.

use std::collections::BTreeSet;

use crate::expression::Expression;
use crate::operator::join::JoinComparisonType;

use super::{
    HashJoinSpec, PhysicalNodeKind, PhysicalPlan, PhysicalPlanNodeArena, PhysicalPlanNodeId,
    PlanChildrenArena,
};

/// Resolve one output position to every base rowset position that can produce
/// it. Multiple entries are possible for positional `UNION ALL` branches.
pub fn trace_rowset_lineage(
    plan: &PhysicalPlan,
    node: PhysicalPlanNodeId,
    output_index: usize,
) -> Vec<(PhysicalPlanNodeId, usize)> {
    trace_rowset_lineage_in(&plan.nodes, &plan.children, node, output_index)
}

pub fn runtime_filter_consumers_in(
    arena: &PhysicalPlanNodeArena,
    children: &PlanChildrenArena,
    probe: PhysicalPlanNodeId,
    spec: &HashJoinSpec,
) -> Vec<PhysicalPlanNodeId> {
    let mut consumers = BTreeSet::new();
    for condition in spec
        .key_conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal)
    {
        let Expression::Reference(reference) = &condition.left else {
            continue;
        };
        consumers.extend(
            trace_rowset_lineage_in(arena, children, probe, reference.index)
                .into_iter()
                .map(|(scan, _)| scan),
        );
    }
    consumers.into_iter().collect()
}

pub fn trace_rowset_lineage_in(
    arena: &PhysicalPlanNodeArena,
    children: &PlanChildrenArena,
    node: PhysicalPlanNodeId,
    output_index: usize,
) -> Vec<(PhysicalPlanNodeId, usize)> {
    let Some(current) = arena.get(node) else {
        return Vec::new();
    };
    match &current.kind {
        PhysicalNodeKind::RowsetScan(_) => (output_index < current.output.types.len())
            .then_some(vec![(node, output_index)])
            .unwrap_or_default(),
        PhysicalNodeKind::Project(spec) => {
            let Some(Expression::Reference(reference)) = spec.expressions.get(output_index) else {
                return Vec::new();
            };
            let [child] = current.children.as_slice(children) else {
                return Vec::new();
            };
            trace_rowset_lineage_in(arena, children, *child, reference.index)
        }
        PhysicalNodeKind::Filter(spec) => {
            let Some(&child_index) = spec.projection_map.get(output_index) else {
                return Vec::new();
            };
            let [child] = current.children.as_slice(children) else {
                return Vec::new();
            };
            trace_rowset_lineage_in(arena, children, *child, child_index)
        }
        PhysicalNodeKind::SetOperation(spec)
            if spec.op == crate::operator::SetOpType::Union && spec.all =>
        {
            current
                .children
                .as_slice(children)
                .iter()
                .flat_map(|child| trace_rowset_lineage_in(arena, children, *child, output_index))
                .collect()
        }
        PhysicalNodeKind::HashJoin(spec) => {
            let [left, right] = current.children.as_slice(children) else {
                return Vec::new();
            };
            let Some(natural_output_index) = spec.output_permutation.natural_of(output_index)
            else {
                return Vec::new();
            };
            if let Some(&child_index) = spec.left_projection.get(natural_output_index) {
                if !spec.join_type.preserves_left_values() {
                    return Vec::new();
                }
                return trace_rowset_lineage_in(arena, children, *left, child_index);
            }
            if !spec.join_type.preserves_right_values() {
                return Vec::new();
            }
            let Some(build_output) = natural_output_index.checked_sub(spec.left_projection.len())
            else {
                return Vec::new();
            };
            let Some(&child_index) = spec.build_input_projection.get(build_output) else {
                return Vec::new();
            };
            trace_rowset_lineage_in(arena, children, *right, child_index)
        }
        _ => Vec::new(),
    }
}
