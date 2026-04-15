// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical plan wrapper and plan-node metadata.

use std::ops::ControlFlow;

use paro_common::error::Result;
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

/// Statistics attached to a logical plan node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeStats {
    pub estimated_cardinality: Option<CardinalityEstimate>,
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
}
