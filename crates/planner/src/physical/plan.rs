// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Arena-backed immutable physical plan and reachability operations.

use super::artifact::ExecutionResourceContract;
use super::children::{PlanChildren, PlanChildrenArena};
use super::dependencies::PlanDependencies;
use super::edges::PhysicalEdgeArena;
use super::ids::PhysicalPlanNodeId;
use super::node::PhysicalPlanNode;
use super::properties::PlanPropertyMap;
use super::specs::PhysicalNodeKind;

mod encoding;
mod identity;
mod render;
pub use identity::PhysicalIdentityError;

#[derive(Debug, Clone, Default)]
pub struct PhysicalPlanNodeArena {
    nodes: Vec<PhysicalPlanNode>,
}

impl PhysicalPlanNodeArena {
    pub fn push(&mut self, mut node: PhysicalPlanNode) -> PhysicalPlanNodeId {
        let id = PhysicalPlanNodeId::new(self.nodes.len());
        node.id = id;
        self.nodes.push(node);
        id
    }

    pub fn get(&self, id: PhysicalPlanNodeId) -> Option<&PhysicalPlanNode> {
        self.nodes.get(id.index())
    }

    pub fn get_mut(&mut self, id: PhysicalPlanNodeId) -> Option<&mut PhysicalPlanNode> {
        self.nodes.get_mut(id.index())
    }

    pub fn iter(&self) -> impl Iterator<Item = &PhysicalPlanNode> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub root: PhysicalPlanNodeId,
    pub nodes: PhysicalPlanNodeArena,
    pub children: PlanChildrenArena,
    pub edges: PhysicalEdgeArena,
    pub properties: PlanPropertyMap,
    pub dependencies: PlanDependencies,
    /// Bound only after artifact admission. It is not an optimizer input and
    /// therefore does not participate in the artifact fingerprint.
    pub execution_resources: Option<ExecutionResourceContract>,
}

impl PhysicalPlan {
    pub fn new(
        root: PhysicalPlanNodeId,
        nodes: PhysicalPlanNodeArena,
        children: PlanChildrenArena,
        properties: PlanPropertyMap,
    ) -> Self {
        Self {
            root,
            nodes,
            children,
            edges: PhysicalEdgeArena::default(),
            properties,
            dependencies: PlanDependencies::default(),
            execution_resources: None,
        }
    }

    pub fn node(&self, id: PhysicalPlanNodeId) -> &PhysicalPlanNode {
        self.nodes
            .get(id)
            .expect("physical plan node id must refer to arena entry")
    }

    pub fn child_ids<'a>(&'a self, children: &'a PlanChildren) -> &'a [PhysicalPlanNodeId] {
        children.as_slice(&self.children)
    }

    /// Remove nodes made unreachable by physical rewrites and reassign dense
    /// ids. A physical plan arena is part of the observable plan contract; it
    /// must not retain folded operators that can be mistaken for consumers.
    pub fn compact_reachable(&mut self) {
        let mut reachable = vec![false; self.nodes.len()];
        let mut stack = vec![self.root];
        loop {
            while let Some(id) = stack.pop() {
                if std::mem::replace(&mut reachable[id.index()], true) {
                    continue;
                }
                stack.extend_from_slice(self.child_ids(&self.node(id).children));
            }
            let mut discovered_auxiliary_producer = false;
            for edge in self.edges.iter() {
                if reachable[edge.consumer.index()] && !reachable[edge.producer.index()] {
                    stack.push(edge.producer);
                    discovered_auxiliary_producer = true;
                }
            }
            if !discovered_auxiliary_producer {
                break;
            }
        }

        if reachable.iter().all(|reachable| *reachable) {
            return;
        }

        let mut remap = vec![PhysicalPlanNodeId::INVALID; reachable.len()];
        let mut next_index = 0;
        for (old_index, is_reachable) in reachable.iter().copied().enumerate() {
            if is_reachable {
                remap[old_index] = PhysicalPlanNodeId::new(next_index);
                next_index += 1;
            }
        }

        let old_children = std::mem::take(&mut self.children);
        let old_nodes = std::mem::take(&mut self.nodes.nodes);
        let mut nodes = PhysicalPlanNodeArena::default();
        let mut children = PlanChildrenArena::default();
        for mut node in old_nodes
            .into_iter()
            .filter(|node| reachable[node.id.index()])
        {
            let remapped_children = node
                .children
                .as_slice(&old_children)
                .iter()
                .map(|child| remap[child.index()])
                .collect();
            node.children = children.pack(remapped_children);
            node.id = PhysicalPlanNodeId::INVALID;
            nodes.push(node);
        }

        self.root = remap[self.root.index()];
        self.properties.retain_remapped(&reachable, &remap);
        let edge_remap = self.edges.retain_remapped(&reachable, &remap);
        self.properties.remap_auxiliary_edges(&edge_remap);
        self.nodes = nodes;
        self.children = children;
    }

    /// Return a structural one-row guarantee, independent of optimizer
    /// cardinality estimates. Consumers may use this as a semantic proof.
    pub fn guarantees_exactly_one_row(&self, id: PhysicalPlanNodeId) -> bool {
        let node = self.node(id);
        match &node.kind {
            PhysicalNodeKind::Aggregate(spec) => {
                spec.grouping_key_count == 0
                    && spec.grouping_sets.len() <= 1
                    && spec.having_filter.is_empty()
            }
            PhysicalNodeKind::Project(_) | PhysicalNodeKind::Sort(_) => {
                let [child] = self.child_ids(&node.children) else {
                    return false;
                };
                self.guarantees_exactly_one_row(*child)
            }
            _ => false,
        }
    }
}
