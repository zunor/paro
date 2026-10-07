// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Create Property Graph Operator

use crate::binder::ir::statement::BoundCreatePropertyGraphInfo;

/// CreatePropertyGraph represents a CREATE PROPERTY GRAPH operation.
#[derive(Debug, Clone)]
pub struct CreatePropertyGraph {
    pub info: BoundCreatePropertyGraphInfo,
}

impl CreatePropertyGraph {
    pub fn new(info: BoundCreatePropertyGraphInfo) -> Self {
        Self { info }
    }
}
