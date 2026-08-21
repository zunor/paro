// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Pruner Optimizer
//!

use paro_planner::expression::Expression;
use paro_planner::operator::{Get, LogicalOperator, Projection, TopN};
use paro_planner::plan::LogicalPlan;
use paro_storage::table::segment_reorderer::{
    OrderByColumnType, OrderByStatistics, SegmentOrderOptions, SegmentOrderType,
};

/// Resolve `Get` under optional leading `Projection` nodes and map a TopN
/// order expression back to the base scan output.
fn get_under_projections_mut<'a>(
    plan: &'a mut LogicalPlan,
    order_expression: &Expression,
) -> Option<(&'a mut Get, Expression)> {
    match &mut plan.operator {
        LogicalOperator::Projection(proj) => {
            let mapped = map_projection_expression(order_expression, proj)?;
            get_under_projections_mut(proj.child.as_mut(), &mapped)
        }
        LogicalOperator::Get(get) => Some((get, order_expression.clone())),
        _ => None,
    }
}

fn map_projection_expression(expr: &Expression, projection: &Projection) -> Option<Expression> {
    match expr {
        Expression::Reference(reference) => projection.expressions.get(reference.index).cloned(),
        Expression::ColumnRef(column_ref)
            if column_ref.binding.table_index == projection.table_index =>
        {
            projection
                .expressions
                .get(column_ref.binding.column_index)
                .cloned()
        }
        Expression::Cast(cast) => {
            let mapped_child = map_projection_expression(cast.child.as_ref(), projection)?;
            let mut mapped = cast.clone();
            mapped.child = Box::new(mapped_child);
            Some(Expression::Cast(mapped))
        }
        _ => Some(expr.clone()),
    }
}

/// SegmentPruner optimizer that pushes down ORDER BY and LIMIT information to the table scan.
///
/// This optimizer looks for patterns of ORDER BY and LIMIT (often combined as TopN)
/// on top of a table scan (Get). It instructs the table scan to reorder
/// top N rows much faster by scanning segments that are likely to contain
/// the smallest (or largest) values first.
pub struct SegmentPruner;

impl SegmentPruner {
    /// Create a new SegmentPruner optimizer.
    pub fn new() -> Self {
        Self
    }

    /// Optimize the logical plan by pushing down scan order information.
    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_plan(plan)
    }

    fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_plan(child));
        self.try_optimize_plan(plan)
    }

    /// Try to apply segment pruning optimization to the given plan.
    fn try_optimize_plan(&mut self, mut plan: LogicalPlan) -> LogicalPlan {
        // We can also optimize ORDER BY followed by LIMIT, but TopN optimizer
        // usually combines them. If they aren't combined, we'd need to handle
        // them here as well.
        if let LogicalOperator::TopN(topn) = &mut plan.operator {
            self.try_optimize_topn(topn);
        }
        plan
    }

    /// Try to optimize a TopN operator.
    fn try_optimize_topn(&mut self, topn: &mut TopN) {
        let Some(order) = topn.orders.first() else {
            return;
        };
        let Some((get, order_expression)) =
            get_under_projections_mut(topn.child.as_mut(), &order.expression)
        else {
            return;
        };

        // We can only optimize if we scan a single table and have an order
        // clause that maps to a real base column.
        if get.table.is_some() {
            if let Some((base_col_idx, projected_idx)) =
                self.extract_column_indices(&order_expression, get)
            {
                let column_type = self.get_column_type(get, projected_idx);

                // Only support numeric and string columns for statistic-based reordering
                if let Some(col_type) = column_type {
                    let options = SegmentOrderOptions {
                        column_idx: base_col_idx,
                        order_by: if order.ascending {
                            OrderByStatistics::Min
                        } else {
                            OrderByStatistics::Max
                        },
                        order_type: if order.ascending {
                            SegmentOrderType::Asc
                        } else {
                            SegmentOrderType::Desc
                        },
                        column_type: col_type,
                        row_limit: Some(topn.limit),
                        row_offset: topn.offset,
                    };

                    get.scan_order = Some(options);
                }
            }
        }
    }

    /// Extract the base column index and projected index from a column reference expression.
    /// Returns (base_column_index, projected_index)
    fn extract_column_indices(&self, expr: &Expression, get: &Get) -> Option<(usize, usize)> {
        match expr {
            Expression::ColumnRef(col_ref) => {
                // Check if this column reference matches the Get's table index
                if col_ref.binding.table_index == get.table_index {
                    // Extract the column index in the base table
                    return get
                        .stored_column(col_ref.binding.column_index)
                        .map(|column_id| (column_id, col_ref.binding.column_index));
                }
                None
            }
            Expression::Cast(cast) => {
                // Look through casts (e.g., to BigInt)
                self.extract_column_indices(&cast.child, get)
            }
            _ => None,
        }
    }

    /// Determine the OrderByColumnType for a given column in Get.
    fn get_column_type(&self, get: &Get, projected_idx: usize) -> Option<OrderByColumnType> {
        // Find the logical type of the column
        let logical_type = if projected_idx < get.column_types.len() {
            &get.column_types[projected_idx]
        } else {
            // This case shouldn't normally happen if col_idx is valid
            return None;
        };

        match logical_type {
            paro_common::types::LogicalType::TinyInt
            | paro_common::types::LogicalType::SmallInt
            | paro_common::types::LogicalType::Integer
            | paro_common::types::LogicalType::BigInt
            | paro_common::types::LogicalType::Float
            | paro_common::types::LogicalType::Double => Some(OrderByColumnType::Numeric),
            paro_common::types::LogicalType::Varchar => Some(OrderByColumnType::String),
            _ => None,
        }
    }
}

impl Default for SegmentPruner {
    fn default() -> Self {
        Self::new()
    }
}
