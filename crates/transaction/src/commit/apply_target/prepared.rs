// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepare-time apply-target projection.

use super::ApplyTargetDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PreparedApplyTarget {
    pub descriptor: ApplyTargetDescriptor,
}

impl From<ApplyTargetDescriptor> for PreparedApplyTarget {
    fn from(descriptor: ApplyTargetDescriptor) -> Self {
        Self { descriptor }
    }
}
