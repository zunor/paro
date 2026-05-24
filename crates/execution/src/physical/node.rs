// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical plan node and debug labels.

use paro_planner::plan::{CardinalityEstimate, PlanNodeId};

use super::children::PlanChildren;
use super::ids::PhysicalPlanNodeId;
use super::row_type::RowType;
use super::specs::PhysicalNodeKind;

#[derive(Debug, Clone)]
pub struct OperatorLabel {
    pub logical_plan_node: PlanNodeId,
    pub display_name: String,
}

impl OperatorLabel {
    pub fn new(logical_plan_node: PlanNodeId, display_name: impl Into<String>) -> Self {
        Self {
            logical_plan_node,
            display_name: display_name.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalPlanNode {
    pub id: PhysicalPlanNodeId,
    pub output: RowType,
    pub cardinality: Option<CardinalityEstimate>,
    pub kind: PhysicalNodeKind,
    pub children: PlanChildren,
    pub label: OperatorLabel,
}
