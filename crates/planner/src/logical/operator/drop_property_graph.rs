// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Drop Property Graph Operator

use crate::binder::ir::statement::BoundDropPropertyGraphInfo;

/// DropPropertyGraph represents a DROP PROPERTY GRAPH operation.
#[derive(Debug, Clone)]
pub struct DropPropertyGraph {
    pub info: BoundDropPropertyGraphInfo,
}

impl DropPropertyGraph {
    pub fn new(info: BoundDropPropertyGraphInfo) -> Self {
        Self { info }
    }
}
