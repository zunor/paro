// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical plan wrapper and plan-node metadata.

use std::ops::ControlFlow;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::binder::context::BindContext;
use crate::operator::{ColumnBinding, LogicalOperator};

/// Stable node identifier within a planning session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanNodeId(pub u32);

impl PlanNodeId {
    /// Shared id for synthetic plan nodes that do not participate in identity tracking.
    ///
    /// Callers may create multiple nested synthetic nodes with the same id
    /// (for example `execution::physical_plan::search_lowering`), so synthetic
    /// ids are intentionally not unique.
    pub const SYNTHETIC: Self = Self(0);
}

/// Cardinality interval persisted on a logical plan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardinalityEstimate {
    pub min: u64,
    pub expected: u64,
    pub max: u64,
}

impl CardinalityEstimate {
    pub fn exact(n: u64) -> Self {
        Self {
            min: n,
            expected: n,
            max: n,
        }
    }
}

/// Provenance controls which optimizer phase owns a cardinality annotation.
///
/// Tree-local statistics can always be recomputed after a rewrite. A join
/// graph estimate, however, accounts for equality classes and joint domains
/// across the whole associative region; reconstructing it from one physical
/// tree cut loses that information.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CardinalityProvenance {
    #[default]
    Statistics,
    JoinGraph,
}

/// Statistics attached to a logical plan node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeStats {
    pub estimated_cardinality: Option<CardinalityEstimate>,
    pub cardinality_provenance: CardinalityProvenance,
}

/// Logical plan wrapper that owns plan-node identity and node-local metadata.
#[derive(Debug)]
pub struct LogicalPlan {
    pub id: PlanNodeId,
    pub stats: NodeStats,
    pub operator: LogicalOperator,
}

#[derive(Debug)]
pub struct PlannedStatement {
    pub types: Vec<LogicalType>,
    pub names: Vec<String>,
    pub plan: LogicalPlan,
}

impl PlannedStatement {
    pub fn types(&self) -> Vec<LogicalType> {
        self.types.clone()
    }

    pub fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}

impl LogicalPlan {
    pub fn new(bind_ctx: &BindContext, operator: LogicalOperator) -> Self {
        Self {
            id: bind_ctx.next_plan_id(),
            stats: NodeStats::default(),
            operator,
        }
    }

    pub fn synthetic(operator: LogicalOperator) -> Self {
        Self {
            id: PlanNodeId::SYNTHETIC,
            stats: NodeStats::default(),
            operator,
        }
    }

    pub fn dummy_scan(bind_ctx: &BindContext) -> Self {
        Self::new(bind_ctx, LogicalOperator::DummyScan)
    }

    /// Column names produced by this plan node (delegates to the wrapped operator).
    pub fn output_names(&self) -> Vec<String> {
        self.operator.output_names()
    }

    /// Logical types of output columns.
    pub fn types(&self) -> Vec<LogicalType> {
        self.operator.types()
    }

    /// Column bindings produced by this plan node.
    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.operator.get_column_bindings()
    }

    /// Child plan nodes (one level).
    pub fn children(&self) -> Vec<&LogicalPlan> {
        self.operator.children()
    }

    pub fn is_empty_result(&self) -> bool {
        matches!(self.operator, LogicalOperator::EmptyResult(_))
    }

    pub fn map_operator(self, f: impl FnOnce(LogicalOperator) -> LogicalOperator) -> Self {
        self.try_map_operator(|operator| Ok(f(operator)))
            .expect("infallible operator mapping cannot fail")
    }

    pub fn try_map_operator(
        self,
        f: impl FnOnce(LogicalOperator) -> Result<LogicalOperator>,
    ) -> Result<Self> {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = self;
        Ok(Self {
            id,
            stats,
            operator: f(operator)?,
        })
    }

    pub fn map_children(self, mut f: impl FnMut(LogicalPlan) -> LogicalPlan) -> Self {
        self.try_map_children(|child| Ok(f(child)))
            .expect("infallible child mapping cannot fail")
    }

    pub fn try_map_children(
        self,
        mut f: impl FnMut(LogicalPlan) -> Result<LogicalPlan>,
    ) -> Result<Self> {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = self;
        Ok(Self {
            id,
            stats,
            operator: operator.try_map_owned_children(&mut f)?,
        })
    }

    /// Iteratively transform a plan after all of its children have been
    /// transformed, while folding one caller-defined state per subtree.
    ///
    /// Logical plans can become substantially deeper than the SQL surface
    /// shape after decorrelation and CTE rewrites. Keeping the traversal here
    /// gives every pass a bounded native stack and a single child
    /// detach/rebuild contract instead of duplicating recursive walkers.
    pub fn try_fold_post_order<State>(
        self,
        mut transform: impl FnMut(LogicalPlan, Vec<State>) -> Result<(LogicalPlan, State)>,
    ) -> Result<(LogicalPlan, State)> {
        struct Frame<State> {
            skeleton: LogicalPlan,
            remaining: std::vec::IntoIter<LogicalPlan>,
            children: Vec<LogicalPlan>,
            child_states: Vec<State>,
        }

        impl<State> Frame<State> {
            fn detach(plan: LogicalPlan) -> Result<Self> {
                let mut detached = Vec::new();
                let skeleton = plan.try_map_children(|child| {
                    detached.push(child);
                    Ok(LogicalPlan::synthetic(LogicalOperator::DummyScan))
                })?;
                let child_count = detached.len();
                Ok(Self {
                    skeleton,
                    remaining: detached.into_iter(),
                    children: Vec::with_capacity(child_count),
                    child_states: Vec::with_capacity(child_count),
                })
            }

            fn rebuild(self) -> Result<(LogicalPlan, Vec<State>)> {
                let mut children = self.children.into_iter();
                let plan = self.skeleton.try_map_children(|_| {
                    children.next().ok_or_else(|| {
                        paro_error::internal("post-order traversal lost a transformed child")
                    })
                })?;
                if children.next().is_some() {
                    return Err(paro_error::internal(
                        "post-order traversal produced excess transformed children",
                    ));
                }
                Ok((plan, self.child_states))
            }
        }

        let mut frames = vec![Frame::detach(self)?];
        loop {
            if let Some(child) = frames.last_mut().and_then(|frame| frame.remaining.next()) {
                frames.push(Frame::detach(child)?);
                continue;
            }
            let frame = frames
                .pop()
                .ok_or_else(|| paro_error::internal("post-order traversal stack is empty"))?;
            let (plan, child_states) = frame.rebuild()?;
            let (plan, state) = transform(plan, child_states)?;
            let Some(parent) = frames.last_mut() else {
                return Ok((plan, state));
            };
            parent.children.push(plan);
            parent.child_states.push(state);
        }
    }

    /// Iterative post-order map without a caller-visible fold state.
    pub fn try_map_post_order(
        self,
        mut transform: impl FnMut(LogicalPlan) -> Result<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        self.try_fold_post_order(|plan, _children: Vec<()>| Ok((transform(plan)?, ())))
            .map(|(plan, ())| plan)
    }

    /// Visit every node with a bounded native stack.
    pub fn try_visit_pre_order(
        &self,
        mut visitor: impl FnMut(&LogicalPlan) -> Result<()>,
    ) -> Result<()> {
        let mut pending = vec![self];
        while let Some(plan) = pending.pop() {
            visitor(plan)?;
            pending.extend(plan.children().into_iter().rev());
        }
        Ok(())
    }

    pub fn visit_children_mut<F>(&mut self, f: F) -> ControlFlow<()>
    where
        F: for<'a> FnMut(&'a mut LogicalPlan) -> ControlFlow<()>,
    {
        self.operator.visit_children_mut(f)
    }

    /// `true` if this subtree is a graph scan/expand chain (see `LogicalOperator::is_graph_chain`).
    pub fn is_graph_chain(&self) -> bool {
        self.operator.is_graph_chain()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::EmptyResult;

    #[test]
    fn logical_plan_ids_share_bind_context_counter() {
        let root_ctx = BindContext::new();
        let child_ctx = root_ctx.create_child();

        let plan_a = LogicalPlan::dummy_scan(&root_ctx);
        let plan_b = LogicalPlan::dummy_scan(&child_ctx);
        let plan_c = LogicalPlan::dummy_scan(&root_ctx);

        assert_eq!(plan_a.id, PlanNodeId(1));
        assert_eq!(plan_b.id, PlanNodeId(2));
        assert_eq!(plan_c.id, PlanNodeId(3));
    }

    #[test]
    fn synthetic_plan_uses_shared_synthetic_id_and_default_stats() {
        let synthetic = LogicalPlan::synthetic(LogicalOperator::DummyScan);

        assert_eq!(synthetic.id, PlanNodeId::SYNTHETIC);
        assert_eq!(synthetic.stats, NodeStats::default());
        assert!(!synthetic.is_empty_result());
    }

    #[test]
    fn post_order_fold_handles_deep_plans_without_native_recursion() {
        const DEPTH: usize = 10_000;
        let mut plan = LogicalPlan::synthetic(LogicalOperator::DummyScan);
        for _ in 0..DEPTH {
            plan = LogicalPlan::synthetic(LogicalOperator::EmptyResult(EmptyResult::new(plan)));
        }

        let (mut plan, node_count) = plan
            .try_fold_post_order(|plan, children: Vec<usize>| {
                Ok((plan, 1 + children.into_iter().sum::<usize>()))
            })
            .expect("bounded post-order traversal");
        assert_eq!(node_count, DEPTH + 1);

        // Dismantle iteratively as well so the test itself never relies on a
        // recursive Box drop for its deep synthetic tree.
        let mut wrappers = 0;
        loop {
            match plan.operator {
                LogicalOperator::EmptyResult(empty) => {
                    wrappers += 1;
                    plan = *empty.child;
                }
                LogicalOperator::DummyScan => break,
                _ => panic!("unexpected operator in deep synthetic plan"),
            }
        }
        assert_eq!(wrappers, DEPTH);
    }
}
