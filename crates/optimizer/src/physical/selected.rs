// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable selected occurrences, ready for physical ABI lowering.
//!
//! Unlike binder trees, these nodes have frozen output layouts, assigned scalar
//! slots and replayed positional keys. Children are shared immutable handles;
//! lowering can inspect fusion patterns without rebuilding an OwnedLogicalPlan.
//! A local binder-shaped shell is used only to adapt scalar/layout contracts.
//! It contains boundary references, never descendant transport trees.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::operator::{
    BoundReference, BoundReferenceId, ColumnBinding, LogicalOperator, LogicalOutputLayout,
};
use paro_planner::plan::arena::LogicalPlanNode;
use paro_planner::plan::{NodeStats, OwnedLogicalPlan, PlanNodeId};

pub(crate) type SelectedChild = Arc<SelectedNode>;

#[derive(Debug)]
pub(crate) struct SelectedNode {
    pub id: PlanNodeId,
    pub stats: NodeStats,
    pub operator: LogicalOperator<SelectedChild>,
    layout: LogicalOutputLayout,
    names: Vec<String>,
}

impl SelectedNode {
    pub fn layout(&self) -> &LogicalOutputLayout {
        &self.layout
    }
    pub fn types(&self) -> Vec<LogicalType> {
        self.layout.types().to_vec()
    }
    pub fn output_names(&self) -> Vec<String> {
        self.names.clone()
    }
    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.layout.bindings().to_vec()
    }
    #[cfg(test)]
    pub fn children(&self) -> Vec<&SelectedNode> {
        let mut children = Vec::new();
        self.operator
            .visit_child_links(&mut |child| children.push(child.as_ref()));
        children
    }
    #[cfg(test)]
    pub fn try_visit_pre_order(&self, mut visit: impl FnMut(&Self) -> Result<()>) -> Result<()> {
        let mut pending = vec![self];
        while let Some(node) = pending.pop() {
            visit(node)?;
            pending.extend(node.children().into_iter().rev());
        }
        Ok(())
    }
    pub fn is_graph_chain(&self) -> bool {
        matches!(
            self.operator,
            LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_)
        )
    }

    /// Transport only one input's completed contract into a local operator
    /// adapter. The occurrence id remains exact; no descendant is exported.
    pub fn boundary(&self) -> OwnedLogicalPlan {
        OwnedLogicalPlan {
            id: self.id,
            stats: self.stats.clone(),
            operator: LogicalOperator::BoundReference(BoundReference::new(
                BoundReferenceId::frozen_output(),
                self.get_column_bindings(),
                self.types(),
            )),
        }
    }

    /// Consume a single local operator and its already prepared exact inputs.
    pub fn from_local(
        mut local: OwnedLogicalPlan,
        children: Vec<SelectedChild>,
        replay_keys: bool,
    ) -> Result<SelectedChild> {
        let child_layouts = children
            .iter()
            .map(|child| child.layout())
            .collect::<Vec<_>>();
        let layout = local.operator.output_layout_from_child_refs(&child_layouts);
        let names = local.operator.output_names_from_child_refs(
            &children
                .iter()
                .map(|child| child.names.as_slice())
                .collect::<Vec<_>>(),
        );
        // All children have already been verified. The local verifier sees
        // only their frozen schemas, never recurses over the selected DAG.
        paro_planner::verify::verify_physical_planner_invariants(&local.operator)?;
        if replay_keys {
            local.stats.unique_keys = crate::statistics::unique_keys::derive_unique_keys_from_facts(
                &local.operator,
                &layout,
                &child_layouts,
                &children
                    .iter()
                    .map(|child| child.stats.unique_keys.as_slice())
                    .collect::<Vec<_>>(),
            );
        }
        // Graph projection expressions belong to a separate late-fetch
        // namespace, not to the graph carrier's immediate child slots.
        if !(matches!(local.operator, LogicalOperator::Projection(_))
            && children.first().is_some_and(|child| child.is_graph_chain()))
        {
            super::slot_assignment::assign_expression_slots(&mut local.operator)?;
        }
        let mut children = children.into_iter();
        let (id, stats, operator) = local.into_parts();
        let operator = operator.try_map_child_links(&mut |boundary| {
            let child = children
                .next()
                .ok_or_else(|| paro_error::internal("selected shell is missing a child"))?;
            if boundary.id != child.id || boundary.operator.output_layout() != child.layout {
                return Err(paro_error::internal(
                    "selected shell child contract changed before lowering",
                ));
            }
            Ok(child)
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal("selected shell has excess children"));
        }
        Ok(Arc::new(Self {
            id,
            stats,
            operator,
            layout,
            names,
        }))
    }

    /// Utility statements do not enter Memo. Consume their binder tree once
    /// at this boundary; query winners instead call from_local during frozen
    /// candidate post-order extraction.
    pub fn from_owned(plan: OwnedLogicalPlan) -> Result<SelectedChild> {
        enum Task {
            Visit(OwnedLogicalPlan),
            Build(LogicalPlanNode<()>, usize),
        }
        let mut tasks = vec![Task::Visit(plan)];
        let mut completed: Vec<SelectedChild> = Vec::new();
        while let Some(task) = tasks.pop() {
            match task {
                Task::Visit(plan) => {
                    let (shell, children) = LogicalPlanNode::detach(plan);
                    tasks.push(Task::Build(shell, children.len()));
                    tasks.extend(children.into_iter().rev().map(|child| Task::Visit(*child)));
                }
                Task::Build(shell, arity) => {
                    let start = completed.len().checked_sub(arity).ok_or_else(|| {
                        paro_error::internal("selected utility input stack underflow")
                    })?;
                    let children = completed.split_off(start);
                    let local =
                        shell.assemble(children.iter().map(|child| Box::new(child.boundary())))?;
                    completed.push(Self::from_local(local, children, false)?);
                }
            }
        }
        completed
            .pop()
            .ok_or_else(|| paro_error::internal("selected utility root is absent"))
    }
}

impl paro_planner::plan::LogicalInput for SelectedNode {
    fn output_layout(&self) -> std::borrow::Cow<'_, LogicalOutputLayout> {
        std::borrow::Cow::Borrowed(&self.layout)
    }
    fn types(&self) -> Vec<LogicalType> {
        self.types()
    }
    fn output_names(&self) -> Vec<String> {
        self.output_names()
    }
    fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.get_column_bindings()
    }
    fn node_stats(&self) -> &NodeStats {
        &self.stats
    }
}

impl paro_planner::plan::LogicalPlanRead for SelectedNode {
    type Child = SelectedChild;
    fn operator(&self) -> &LogicalOperator<Self::Child> {
        &self.operator
    }
}

impl Drop for SelectedNode {
    fn drop(&mut self) {
        fn detach(node: &mut SelectedNode, pending: &mut Vec<SelectedChild>) {
            let operator = std::mem::replace(&mut node.operator, LogicalOperator::DummyScan);
            let _ = operator.try_map_child_links(&mut |child| {
                pending.push(child);
                Ok::<(), std::convert::Infallible>(())
            });
        }
        let mut pending = Vec::new();
        detach(self, &mut pending);
        while let Some(child) = pending.pop() {
            if let Ok(mut child) = Arc::try_unwrap(child) {
                detach(&mut child, &mut pending);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression, Expression};
    use paro_planner::operator::{Filter, Projection};

    #[test]
    fn selected_inputs_keep_bindings_while_scalars_become_slots() {
        let child = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            7,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(4), LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(9), LogicalType::Integer).into(),
                ),
            ],
        )));
        let parent = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            8,
            child,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(7, 1), LogicalType::Integer).into(),
            )],
        )));
        let expected = parent.output_layout();
        let selected = SelectedNode::from_owned(parent).unwrap();
        assert_eq!(selected.layout(), &expected);
        let LogicalOperator::Projection(projection) = &selected.operator else {
            panic!("projection")
        };
        let Expression::Reference(reference) = &projection.expressions[0] else {
            panic!("assigned slot")
        };
        assert_eq!(reference.index, 1);
        assert_eq!(
            projection.child.layout().bindings()[1],
            ColumnBinding::new(7, 1)
        );
        let boundary = selected.boundary();
        assert!(boundary.children().is_empty());
        assert_eq!(boundary.output_layout(), expected);
        assert_eq!(boundary.id, selected.id);
    }

    #[test]
    fn selected_shell_rejects_a_mismatched_child_contract() {
        let child =
            SelectedNode::from_owned(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan))
                .unwrap();
        let other =
            OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(BoundReference::new(
                BoundReferenceId::frozen_output(),
                vec![ColumnBinding::new(2, 0)],
                vec![LogicalType::Integer],
            )));
        let local =
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(other, vec![])));
        assert!(SelectedNode::from_local(local, vec![child], true).is_err());
    }

    #[test]
    fn deep_selected_inputs_construct_and_drop_without_recursive_transport() {
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
                for _ in 0..10_000 {
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                        plan,
                        vec![],
                    )));
                }
                let selected = SelectedNode::from_owned(plan).unwrap();
                let mut count = 0;
                selected
                    .try_visit_pre_order(|_| {
                        count += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(count, 10_001);
                // Retain one child: iterative destruction must also respect
                // external references instead of assuming unique tree ownership.
                let LogicalOperator::Filter(filter) = &selected.operator else {
                    panic!("filter")
                };
                let shared = filter.child.clone();
                drop(selected);
                assert!(matches!(shared.operator, LogicalOperator::Filter(_)));
                drop(shared);
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
