// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plans query-level `ORDER BY`, `LIMIT`/`OFFSET`, and hidden-column pruning.

use crate::binder::ir::BoundQueryModifiers;
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::{Limit, LogicalOperator, Order, Projection};
use paro_common::error::{self as paro_error, Result};

impl Binder {
    pub(crate) fn plan_query_modifiers(
        &mut self,
        node: BoundQueryModifiers,
    ) -> Result<LogicalOperator> {
        let visible_names = node.child.names();
        let visible_types = node.child.types();
        let mut root = self.plan_query(*node.child)?;

        if let Some(orders) = node.order_by {
            match &mut root {
                LogicalOperator::Distinct(distinct) if distinct.is_distinct_on() => {
                    distinct.order_by = Some(orders);
                }
                _ => {
                    root = LogicalOperator::Order(Order::new(self.wrap_plan(root), orders));
                }
            }
        }

        if let Some(limit) = node.limit {
            root = LogicalOperator::Limit(
                Limit::new(self.wrap_plan(root), limit.limit, limit.offset)
                    .with_hnsw_ef_hint(node.hnsw_ef_hint),
            );
        }

        if let Some(projection_index) = node.prune_index {
            let bindings = root.get_column_bindings();
            if bindings.len() < visible_types.len() {
                return Err(paro_error::internal(format!(
                    "Query modifier child exposes {} bindings for {} visible columns",
                    bindings.len(),
                    visible_types.len()
                )));
            }
            let expressions = visible_types
                .iter()
                .enumerate()
                .map(|(index, return_type)| {
                    Expression::ColumnRef(ColumnRefExpression::new(
                        bindings[index],
                        return_type.clone(),
                    ))
                })
                .collect();
            root = LogicalOperator::Projection(
                Projection::new(projection_index, self.wrap_plan(root), expressions)
                    .with_output_names(visible_names),
            );
        }

        Ok(root)
    }
}
