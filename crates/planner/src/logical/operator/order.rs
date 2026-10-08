// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Order Operator
//!
//!

use crate::binder::ir::OrderByNode;
use crate::logical::operator::ProjectionMap;
use crate::logical::plan::OwnedLogicalPlan;

/// Order represents an ORDER BY operation.
#[derive(Debug, Clone)]
pub struct Order<Child = Box<OwnedLogicalPlan>> {
    pub orders: Vec<OrderByNode>,
    pub child: Child,
    /// Exact output projection derived by column lifetime analysis.
    pub projection_map: ProjectionMap,
}

impl Order {
    pub fn new(child: OwnedLogicalPlan, orders: Vec<OrderByNode>) -> Self {
        let projection_map = ProjectionMap::all();
        Self {
            orders,
            child: Box::new(child),
            projection_map,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::context::BindContext;

    use super::*;

    #[test]
    fn test_order_has_projection_map() {
        let ctx = BindContext::new();
        let order = Order::new(OwnedLogicalPlan::dummy_scan(&ctx), vec![]);
        assert!(order.projection_map.is_identity(order.child.types().len()));
    }

    #[test]
    fn test_order_projection_map_can_be_set() {
        let ctx = BindContext::new();
        let mut order = Order::new(OwnedLogicalPlan::dummy_scan(&ctx), vec![]);
        order.projection_map = vec![0, 1, 3].into();
        assert_eq!(order.projection_map.as_columns(), Some(&[0, 1, 3][..]));
    }
}
