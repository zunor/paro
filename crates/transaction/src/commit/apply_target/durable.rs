// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable/apply-time apply-target projection.

use super::ApplyTargetDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitApplyTarget {
    pub descriptor: ApplyTargetDescriptor,
}

impl From<ApplyTargetDescriptor> for CommitApplyTarget {
    fn from(descriptor: ApplyTargetDescriptor) -> Self {
        Self { descriptor }
    }
}
