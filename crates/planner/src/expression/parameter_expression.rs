// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime parameter expression used by reusable physical program images.

use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;

#[derive(Debug, Clone)]
pub struct ParameterExpression {
    pub slot: ParameterSlot,
}

impl ParameterExpression {
    pub fn new(slot: ParameterSlot) -> Self {
        Self { slot }
    }

    pub fn return_type(&self) -> LogicalType {
        self.slot.ty.clone()
    }
}
