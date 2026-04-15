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
