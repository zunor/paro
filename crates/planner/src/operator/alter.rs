// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Alter Operator
//!

use crate::binder::ir::statement::BoundAlterEntryInfo;

#[derive(Debug, Clone)]
pub struct Alter {
    pub info: BoundAlterEntryInfo,
}

impl Alter {
    pub fn new(info: BoundAlterEntryInfo) -> Self {
        Self { info }
    }
}
