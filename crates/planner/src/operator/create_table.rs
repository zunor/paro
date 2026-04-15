// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Create Table Operator
//!
//!

use crate::binder::ir::statement::BoundCreateTableInfo;

/// CreateTable represents a CREATE TABLE operation.
#[derive(Debug, Clone)]
pub struct CreateTable {
    pub info: BoundCreateTableInfo,
}

impl CreateTable {
    pub fn new(info: BoundCreateTableInfo) -> Self {
        Self { info }
    }
}
