// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Semantics-preserving rewrites over the fully bound physical plan.

use super::ids::PhysicalPlanNodeId;
use super::plan::PhysicalPlan;
use super::specs::PhysicalNodeKind;

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
        let Some(composed) = outer.compose_over(&inner) else {
            return;
        };
        let node = plan
            .nodes
            .get_mut(id)
            .expect("physical rewrite node id must remain valid");
        let PhysicalNodeKind::Project(project) = &mut node.kind else {
            return;
        };
        *project = composed;
        node.children.replace_only(grandchild);
    }
}
