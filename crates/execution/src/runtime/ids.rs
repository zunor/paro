// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::pipeline::graph::PipelineId;
use paro_planner::logical::plan::PlanNodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeOperatorId(u32);

impl RuntimeOperatorId {
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "runtime operator id exhausted");
        Self(index as u32)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeOperatorOrigin {
    pub pipeline: PipelineId,
    pub role: OperatorRole,
    pub ordinal: RuntimeRoleOrdinal,
    /// Optional logical coordinate retained for diagnostic profile joins.
    /// Runtime operator ids remain allocation-local and are intentionally not
    /// replaced by this field.
    pub logical_plan_node: Option<PlanNodeId>,
}

impl RuntimeOperatorOrigin {
    pub fn new(pipeline: PipelineId, role: OperatorRole, ordinal: RuntimeRoleOrdinal) -> Self {
        Self {
            pipeline,
            role,
            ordinal,
            logical_plan_node: None,
        }
    }

    #[inline]
    pub fn with_logical_plan_node(mut self, logical_plan_node: Option<PlanNodeId>) -> Self {
        self.logical_plan_node = logical_plan_node.filter(|node| !node.is_synthetic());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorRole {
    Source,
    Transform,
    Sink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeRoleOrdinal(u16);

impl RuntimeRoleOrdinal {
    pub fn new(index: usize) -> Self {
        assert!(index <= u16::MAX as usize, "runtime role ordinal exhausted");
        Self(index as u16)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

pub type SubRoleIndex = RuntimeRoleOrdinal;
