// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Refresh Property Graph Operator

use crate::binder::ir::statement::BoundRefreshPropertyGraphInfo;

/// RefreshPropertyGraph represents a REFRESH PROPERTY GRAPH operation.
#[derive(Debug, Clone)]
pub struct RefreshPropertyGraph {
    pub info: BoundRefreshPropertyGraphInfo,
}

impl RefreshPropertyGraph {
    pub fn new(info: BoundRefreshPropertyGraphInfo) -> Self {
        Self { info }
    }
}
