// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query Planning (SELECT, VALUES)
//!
//!
//!
//! ## Supported
//! - PlanFilter (for WHERE, HAVING, QUALIFY)
//! - PlanSubqueries (for correlated subqueries)
//! - Aggregate operator creation
//! - Window operator creation
//! - Projection operator creation
//! - DISTINCT (regular)
//! - DISTINCT ON
//! - ORDER BY
//! - LIMIT/OFFSET
//! - need_prune (final projection to remove extra columns)

use crate::binder::ir::{BoundSelect, BoundValues, DistinctType};
use crate::binder::ir::{DistinctModifier, OrderByNode};
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::{
    Aggregate, ColumnBinding, Distinct, ExpressionGet, Filter, Limit, LogicalOperator, Order,
    Projection,
};
use paro_common::error::Result;
use paro_common::types::LogicalType;

impl Binder {
    /// Create a filter operator with a condition.
    ///
    ///
    /// This method creates a Filter operator and handles subqueries
    /// within the filter condition.
    pub fn plan_filter(
        &mut self,
        mut condition: Expression,
        mut root: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Handle subqueries in the condition
        self.plan_subqueries(&mut condition, &mut root)?;

        // Create filter operator
        let filter = Filter::new(self.wrap_plan(root), vec![condition]);
        Ok(LogicalOperator::Filter(filter))
    }

    /// Plan a SELECT node (CreatePlan for BoundSelect).
    ///
    ///
    /// Operator creation order:
    /// 1. FROM table (or DummyScan)
    /// 2. Sample (if any)
    /// 3. WHERE filter
    /// 4. Aggregate (groups + aggregates)
    /// 5. HAVING filter
    /// 6. Window functions
    /// 7. QUALIFY filter
    /// 8. Unnest operators
    /// 9. Projection (SELECT list)
    /// 10. VisitQueryNode (ORDER BY, LIMIT)
    /// 11. Prune projection (if need_prune)
    pub(crate) fn plan_select(&mut self, mut node: BoundSelect) -> Result<LogicalOperator> {
        // =================================================================
        // 1. FROM clause (or DummyScan if no FROM)
        // =================================================================
        let mut root = if let Some(from_table) = node.from_table {
            self.plan_table_ref(from_table)?
        } else {
            LogicalOperator::DummyScan
        };

        // =================================================================
        // 2. Sample (not yet implemented in paro)
        // =================================================================
        // TODO: Implement SAMPLE clause

        // =================================================================
        // 3. WHERE filter
        // =================================================================
        if let Some(where_clause) = node.where_clause.take() {
            root = self.plan_filter(where_clause, root)?;
        }

        // =================================================================
        // 4. Aggregate (groups + aggregates + HAVING)
        // =================================================================
        if !node.groups.group_expressions.is_empty()
            || !node.aggregates.is_empty()
            || node.having_clause.is_some()
        {
            if !node.groups.group_expressions.is_empty() {
                // Visit the groups - handle subqueries
                for group in &mut node.groups.group_expressions {
                    self.plan_subqueries(group, &mut root)?;
                }
            }

            // Visit aggregate expressions - handle subqueries
            for expr in &mut node.aggregates {
                self.plan_subqueries(expr, &mut root)?;
            }

            // Create the aggregate operator
            let aggregate = Aggregate::new(
                node.group_index,
                node.aggregate_index,
                node.groupings_index,
                self.wrap_plan(root),
                node.groups.group_expressions.clone(),
                node.groups.grouping_sets.clone(),
                node.aggregates.clone(),
                node.grouping_functions.clone(),
            );
            root = LogicalOperator::Aggregate(aggregate);
        } else if !node.groups.grouping_sets.is_empty() {
            // =================================================================
            // Edge case: grouping sets but no groups or aggregates
            // Just output a dummy scan
            // =================================================================
            root = LogicalOperator::DummyScan;
        }

        // =================================================================
        // 5. HAVING filter
        // =================================================================
        if let Some(having) = node.having_clause.take() {
            root = self.plan_filter(having, root)?;
        }

        // =================================================================
        // 6. Window functions
        // =================================================================
        for expression in &mut node.select_list {
            expression.extract_windows_in_place(&mut node.windows, node.window_index);
        }
        if let Some(qualify) = &mut node.qualify_clause {
            qualify.extract_windows_in_place(&mut node.windows, node.window_index);
        }

        if !node.windows.is_empty() {
            // Handle subqueries in window expressions
            for expr in &mut node.windows {
                self.plan_subqueries(expr, &mut root)?;
            }

            // Extract WindowExpression from Expression::Window variants
            let window_exprs: Vec<crate::expression::WindowExpression> = node
                .windows
                .drain(..)
                .filter_map(|expr| {
                    if let Expression::Window(win_expr) = expr {
                        Some(win_expr)
                    } else {
                        None
                    }
                })
                .collect();

            if !window_exprs.is_empty() {
                // Create the Window operator
                let window = crate::operator::Window::new(
                    node.window_index,
                    window_exprs,
                    self.wrap_plan(root),
                );
                root = LogicalOperator::Window(window);
            }
        }

        // =================================================================
        // 7. QUALIFY filter
        // QUALIFY filters window function results (like HAVING for aggregates)
        // =================================================================
        if let Some(qualify) = node.qualify_clause.take() {
            root = self.plan_filter(qualify, root)?;
        }

        // =================================================================
        // 8. Unnest operators (not yet implemented)
        // =================================================================
        // TODO: Implement Unnest operator

        // =================================================================
        // 9. Projection (SELECT list)
        //
        // Note: ORDER BY expressions that reference columns not in SELECT
        // have already been added to select_list during binding (in prepare_order_by).
        // The ORDER BY expressions are already BoundColumnRefExpressions pointing
        // to the projection's output indices.
        // =================================================================
        let original_select_count = node.column_count;

        // Handle subqueries in SELECT list
        for expr in &mut node.select_list {
            self.plan_subqueries(expr, &mut root)?;
        }

        let projection = Projection::new(
            node.projection_index,
            self.wrap_plan(root),
            node.select_list.clone(),
        )
        .with_output_names(node.names.clone());
        root = LogicalOperator::Projection(projection);

        // =================================================================
        // 11. DISTINCT
        // =================================================================
        if let Some(distinct_mod) = node.distinct {
            root = self.plan_distinct_modifier(root, distinct_mod, node.order_by.take())?;
        }

        // =================================================================
        // 12. ORDER BY
        // =================================================================
        if let Some(orders) = node.order_by {
            let order_op = Order::new(self.wrap_plan(root), orders);
            root = LogicalOperator::Order(order_op);
        }

        // =================================================================
        // 13. LIMIT/OFFSET
        // =================================================================
        if let Some(limit_modifier) = node.limit {
            let limit_op = Limit::new(
                self.wrap_plan(root),
                limit_modifier.limit,
                limit_modifier.offset,
            )
            .with_hnsw_ef_hint(node.hnsw_ef_hint);
            root = LogicalOperator::Limit(limit_op);
        }

        // =================================================================
        // 14. Prune projection (if need_prune)
        // This removes any extra columns added for ORDER BY
        // =================================================================
        if node.need_prune {
            let final_proj_index = node.prune_index;
            let final_select_list: Vec<Expression> = (0..original_select_count)
                .map(|i| {
                    let return_type = if i < node.types.len() {
                        node.types[i].clone()
                    } else {
                        LogicalType::Unknown
                    };
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(node.projection_index, i),
                        return_type,
                    ))
                })
                .collect();
            let final_projection =
                Projection::new(final_proj_index, self.wrap_plan(root), final_select_list)
                    .with_output_names(node.names.clone());
            root = LogicalOperator::Projection(final_projection);
        }

        Ok(root)
    }

    /// Plan a DISTINCT modifier.
    ///
    fn plan_distinct_modifier(
        &mut self,
        child: LogicalOperator,
        distinct_mod: DistinctModifier,
        order_by: Option<Vec<OrderByNode>>,
    ) -> Result<LogicalOperator> {
        match distinct_mod.distinct_type {
            DistinctType::Distinct => {
                let distinct = Distinct::new(self.wrap_plan(child));
                Ok(LogicalOperator::Distinct(distinct))
            }
            DistinctType::DistinctOn => {
                let targets = distinct_mod.target_distincts;

                if let Some(orders) = order_by {
                    let distinct =
                        Distinct::distinct_on_with_order(targets, orders, self.wrap_plan(child));
                    Ok(LogicalOperator::Distinct(distinct))
                } else {
                    let distinct = Distinct::distinct_on(targets, self.wrap_plan(child));
                    Ok(LogicalOperator::Distinct(distinct))
                }
            }
        }
    }

    /// Plan a VALUES node.
    pub(crate) fn plan_values(&mut self, node: BoundValues) -> Result<LogicalOperator> {
        let op = ExpressionGet::new(node.projection_index, node.values, node.names, node.types);
        Ok(LogicalOperator::ExpressionGet(op))
    }
}
