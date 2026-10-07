// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Create Sequence Operator
//!

use crate::binder::ir::statement::BoundCreateSequenceInfo;

#[derive(Debug, Clone)]
pub struct CreateSequence {
    pub info: BoundCreateSequenceInfo,
}

impl CreateSequence {
    pub fn new(info: BoundCreateSequenceInfo) -> Self {
        Self { info }
    }
}
