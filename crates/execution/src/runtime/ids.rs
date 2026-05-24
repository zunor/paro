// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::pipeline::graph::PipelineId;

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
}

impl RuntimeOperatorOrigin {
    pub fn new(pipeline: PipelineId, role: OperatorRole, ordinal: RuntimeRoleOrdinal) -> Self {
        Self {
            pipeline,
            role,
            ordinal,
        }
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
