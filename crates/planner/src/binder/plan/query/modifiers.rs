//! Plans `DISTINCT`, `ORDER BY`, `LIMIT`/`OFFSET` (limit/offset must be constants for now).

use crate::binder::ir::BoundSelect;
use crate::binder::ir::{LimitModifier, OrderByNode};
use crate::binder::Binder;
use crate::operator::{Distinct, Limit, LogicalOperator, Order};
use paro_common::error::Result;

impl Binder {
    pub fn visit_query_node(
        &mut self,
        root: LogicalOperator,
        distinct: bool,
        order_by: Option<Vec<OrderByNode>>,
        limit: Option<LimitModifier>,
    ) -> Result<LogicalOperator> {
        let mut result = root;

        if distinct {
            result = self.plan_distinct(result)?;
        }

        if let Some(orders) = order_by {
            result = self.plan_order_by(result, orders)?;
        }

        if let Some(limit_mod) = limit {
            result = self.plan_limit(result, limit_mod)?;
        }

        Ok(result)
    }

    fn plan_distinct(&mut self, child: LogicalOperator) -> Result<LogicalOperator> {
        let distinct = Distinct::new(self.wrap_plan(child));
        Ok(LogicalOperator::Distinct(distinct))
    }

    fn plan_order_by(
        &mut self,
        child: LogicalOperator,
        orders: Vec<OrderByNode>,
    ) -> Result<LogicalOperator> {
        let order = Order::new(self.wrap_plan(child), orders);
        Ok(LogicalOperator::Order(order))
    }

    fn plan_limit(
        &mut self,
        child: LogicalOperator,
        limit_mod: LimitModifier,
    ) -> Result<LogicalOperator> {
        let limit = Limit::new(self.wrap_plan(child), limit_mod.limit, limit_mod.offset);
        Ok(LogicalOperator::Limit(limit))
    }
    pub fn has_distinct(&self, _node: &BoundSelect) -> bool {
        false
    }
}
