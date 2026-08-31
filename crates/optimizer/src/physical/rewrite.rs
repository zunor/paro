// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Semantics-preserving rewrites over the fully bound physical plan.

use paro_planner::expression::{Expression, ExpressionIterator};

use super::ids::PhysicalPlanNodeId;
use super::plan::PhysicalPlan;
use super::specs::{PhysicalNodeKind, ProjectSpec};

pub(super) fn rewrite_projection_chains(plan: &mut PhysicalPlan) {
    let mut visited = vec![false; plan.nodes.len()];
    rewrite_node(plan.root, plan, &mut visited);
}

fn rewrite_node(id: PhysicalPlanNodeId, plan: &mut PhysicalPlan, visited: &mut [bool]) {
    if std::mem::replace(&mut visited[id.index()], true) {
        return;
    }
    let children = plan.node(id).children.as_slice(&plan.children).to_vec();
    for child in children {
        rewrite_node(child, plan, visited);
    }

    loop {
        let (outer, child) = {
            let node = plan.node(id);
            let PhysicalNodeKind::Project(outer) = &node.kind else {
                return;
            };
            let [child] = node.children.as_slice(&plan.children) else {
                return;
            };
            (outer.clone(), *child)
        };
        let (inner, grandchild) = {
            let node = plan.node(child);
            let PhysicalNodeKind::Project(inner) = &node.kind else {
                return;
            };
            let [grandchild] = node.children.as_slice(&plan.children) else {
                return;
            };
            (inner.clone(), *grandchild)
        };
        let Some(expressions) = compose_project_expressions(&outer, &inner) else {
            return;
        };
        let node = plan
            .nodes
            .get_mut(id)
            .expect("physical rewrite node id must remain valid");
        let PhysicalNodeKind::Project(project) = &mut node.kind else {
            return;
        };
        project.expressions = expressions;
        node.children.replace_only(grandchild);
    }
}

fn compose_project_expressions(
    outer: &ProjectSpec,
    inner: &ProjectSpec,
) -> Option<Box<[Expression]>> {
    if outer
        .expressions
        .iter()
        .chain(inner.expressions.iter())
        .any(|expression| expression.evaluation_properties().is_reorder_fence())
    {
        return None;
    }

    let mut references = vec![0usize; inner.expressions.len()];
    for expression in &outer.expressions {
        if !count_physical_references(expression, &inner.expressions, &mut references) {
            return None;
        }
    }
    if inner
        .expressions
        .iter()
        .zip(&references)
        .any(|(expression, &count)| !expression.is_passive_value() && count != 1)
    {
        return None;
    }

    let mut expressions = outer.expressions.to_vec();
    for expression in &mut expressions {
        substitute_physical_references(expression, &inner.expressions);
    }
    Some(expressions.into_boxed_slice())
}

fn count_physical_references(
    expression: &Expression,
    inner: &[Expression],
    references: &mut [usize],
) -> bool {
    if let Expression::Reference(reference) = expression {
        let Some(inner_expression) = inner.get(reference.index) else {
            return false;
        };
        if reference.return_type != inner_expression.return_type() {
            return false;
        }
        references[reference.index] += 1;
        return true;
    }

    let mut valid = true;
    ExpressionIterator::enumerate_children(expression, |child| {
        valid &= count_physical_references(child, inner, references);
    });
    valid
}

fn substitute_physical_references(expression: &mut Expression, inner: &[Expression]) {
    if let Expression::Reference(reference) = expression {
        *expression = inner[reference.index].clone();
        return;
    }
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        substitute_physical_references(child, inner);
    });
}
