// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Order Operator
//!
//!

use crate::binder::ir::OrderByNode;
use crate::operator::ProjectionMap;
use crate::plan::LogicalPlan;

/// Order represents an ORDER BY operation.
#[derive(Debug)]
pub struct Order {
    pub orders: Vec<OrderByNode>,
    pub child: Box<LogicalPlan>,
    /// Exact output projection derived by column lifetime analysis.
    pub projection_map: ProjectionMap,
}

impl Order {
    pub fn new(child: LogicalPlan, orders: Vec<OrderByNode>) -> Self {
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
        let order = Order::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        assert!(order.projection_map.is_identity(order.child.types().len()));
    }

    #[test]
    fn test_order_projection_map_can_be_set() {
        let ctx = BindContext::new();
        let mut order = Order::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        order.projection_map = vec![0, 1, 3].into();
        assert_eq!(order.projection_map.as_columns(), Some(&[0, 1, 3][..]));
    }
}
