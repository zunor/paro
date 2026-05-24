// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed ids used by the physical plan arena.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalPlanNodeId(u32);

impl PhysicalPlanNodeId {
    pub const INVALID: Self = Self(u32::MAX);

    #[inline]
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "physical plan arena exhausted");
        Self(index as u32)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlanChildrenId(u32);

impl PlanChildrenId {
    #[inline]
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "plan children arena exhausted");
        Self(index as u32)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}
