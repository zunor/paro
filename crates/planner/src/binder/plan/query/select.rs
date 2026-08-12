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
//!
//! Query-level ORDER BY, LIMIT/OFFSET, and hidden-column pruning are applied
//! by `query::modifiers` after this SELECT body is planned.

use crate::binder::ir::DistinctModifier;
use crate::binder::ir::{BoundSelect, BoundValues, DistinctType};
use crate::binder::Binder;
use crate::expression::{Expression, ExpressionIterator, WindowExpression};
use crate::operator::{
    Aggregate, ColumnBinding, Distinct, ExpressionGet, Filter, LogicalOperator, Projection,
};
use paro_common::error::{self as paro_error, Result};

fn group_window_expressions(
    expressions: Vec<(usize, WindowExpression)>,
) -> Vec<Vec<(usize, WindowExpression)>> {
    let mut groups: Vec<Vec<(usize, WindowExpression)>> = Vec::new();
    for (original_index, expression) in expressions {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group[0].1.has_same_layout(&expression))
        {
            group.push((original_index, expression));
        } else {
            groups.push(vec![(original_index, expression)]);
        }
    }
    groups
}

fn remap_window_bindings(
    expression: &mut Expression,
    placeholder_index: usize,
    output_bindings: &[ColumnBinding],
) -> Result<()> {
    if let Expression::ColumnRef(column) = expression {
        if column.binding.table_index == placeholder_index {
            column.binding = output_bindings
                .get(column.binding.column_index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Window output {} has no layout binding",
                        column.binding.column_index
                    ))
                })?;
        }
        return Ok(());
    }

    let mut result = Ok(());
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        if result.is_ok() {
            result = remap_window_bindings(child, placeholder_index, output_bindings);
        }
    });
    result
}

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
    ///
    /// Query modifiers are planned by the enclosing `BoundQuery::Modifiers`.
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

            let indexed_windows = node
                .windows
                .drain(..)
                .enumerate()
                .map(|(original_index, expression)| match expression {
                    Expression::Window(window) => Ok((original_index, window)),
                    other => Err(paro_error::internal(format!(
                        "Window extraction produced a non-window expression: {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            let output_count = indexed_windows.len();
            let groups = group_window_expressions(indexed_windows);
            let mut output_bindings = vec![None; output_count];
            let mut planned_groups = Vec::with_capacity(groups.len());

            for (group_index, group) in groups.into_iter().enumerate() {
                let window_index = if group_index == 0 {
                    node.window_index
                } else {
                    self.bind_context.generate_table_index()
                };
                let mut expressions = Vec::with_capacity(group.len());
                for (local_index, (original_index, expression)) in group.into_iter().enumerate() {
                    output_bindings[original_index] =
                        Some(ColumnBinding::new(window_index, local_index));
                    expressions.push(expression);
                }
                planned_groups.push((window_index, expressions));
            }

            let output_bindings = output_bindings
                .into_iter()
                .enumerate()
                .map(|(index, binding)| {
                    binding.ok_or_else(|| {
                        paro_error::internal(format!(
                            "Window output {index} was not assigned to a layout"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            for expression in &mut node.select_list {
                remap_window_bindings(expression, node.window_index, &output_bindings)?;
            }
            if let Some(qualify) = &mut node.qualify_clause {
                remap_window_bindings(qualify, node.window_index, &output_bindings)?;
            }

            // Each physical window runtime owns one partition/order layout. Stack groups so prior
            // outputs remain attached to their rows while the next group applies its own ordering.
            for (window_index, expressions) in planned_groups {
                let window =
                    crate::operator::Window::new(window_index, expressions, self.wrap_plan(root));
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
        // 10. DISTINCT
        // =================================================================
        if let Some(distinct_mod) = node.distinct {
            root = self.plan_distinct_modifier(root, distinct_mod)?;
        }

        Ok(root)
    }

    /// Plan a DISTINCT modifier.
    ///
    fn plan_distinct_modifier(
        &mut self,
        child: LogicalOperator,
        distinct_mod: DistinctModifier,
    ) -> Result<LogicalOperator> {
        match distinct_mod.distinct_type {
            DistinctType::Distinct => {
                let distinct = Distinct::new(self.wrap_plan(child));
                Ok(LogicalOperator::Distinct(distinct))
            }
            DistinctType::DistinctOn => {
                let targets = distinct_mod.target_distincts;
                let distinct = Distinct::distinct_on(targets, self.wrap_plan(child));
                Ok(LogicalOperator::Distinct(distinct))
            }
        }
    }

    /// Plan a VALUES node.
    pub(crate) fn plan_values(&mut self, node: BoundValues) -> Result<LogicalOperator> {
        let op = ExpressionGet::new(node.projection_index, node.values, node.names, node.types);
        Ok(LogicalOperator::ExpressionGet(op))
    }
}

#[cfg(test)]
mod tests {
    use super::group_window_expressions;
    use crate::expression::{ColumnRefExpression, Expression, WindowExpression, WindowFrame};
    use crate::operator::ColumnBinding;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;

    fn row_number(partition_column: usize) -> WindowExpression {
        WindowExpression::native(
            WindowFunction::row_number(),
            Vec::new(),
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(10, partition_column),
                LogicalType::Integer,
            ))],
            Vec::new(),
            WindowFrame::default(),
            false,
        )
    }

    #[test]
    fn window_groups_are_stable_and_combine_equal_layouts() {
        let groups = group_window_expressions(vec![
            (0, row_number(0)),
            (1, row_number(1)),
            (2, row_number(0)),
        ]);

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .iter()
                .map(|(original_index, _)| *original_index)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(groups[1][0].0, 1);
    }
}
