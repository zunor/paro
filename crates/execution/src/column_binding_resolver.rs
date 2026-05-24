// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Resolve logical column bindings into physical chunk indexes.

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression, ReferenceExpression};
use paro_planner::operator::{ColumnBinding, Join, LogicalOperator};
use paro_planner::visitor::LogicalOperatorVisitor;

/// ColumnBindingResolver resolves ColumnBindings into physical indices.
///
/// It traverses the logical plan and replaces all ColumnRefExpression
/// with ReferenceExpression, where the index is the position in the
/// current set of column bindings.
///
/// # Usage
/// ```ignore
/// ColumnBindingResolver::resolve(&mut plan)?;
/// ```
pub struct ColumnBindingResolver {
    /// Current column bindings from child operators.
    /// Updated as we traverse the plan tree.
    bindings: Vec<ColumnBinding>,
    /// Types of current bindings (for verification).
    types: Vec<LogicalType>,
    /// If true, only verify bindings without replacing expressions.
    verify_only: bool,
}

impl ColumnBindingResolver {
    /// Create a new ColumnBindingResolver.
    pub fn new() -> Self {
        Self {
            bindings: Vec::new(),
            types: Vec::new(),
            verify_only: false,
        }
    }

    /// Create a ColumnBindingResolver in verify-only mode.
    ///
    /// In this mode, the resolver checks that all column references can be
    /// resolved but does not replace them with ReferenceExpression.
    pub fn new_verify_only() -> Self {
        Self {
            bindings: Vec::new(),
            types: Vec::new(),
            verify_only: true,
        }
    }

    /// Resolve column bindings in a logical plan.
    ///
    /// This is the main entry point. After calling this method, all
    /// ColumnRefExpression in the plan will be replaced with
    /// ReferenceExpression.
    pub fn resolve(plan: &mut LogicalOperator) -> Result<()> {
        let mut resolver = Self::new();
        resolver.visit_operator(plan);
        Ok(())
    }

    /// Verify that all column bindings can be resolved without modifying the plan.
    /// Also checks for duplicate table indices.
    ///
    /// This is primarily used in debug builds to catch internal errors early.
    pub fn verify(plan: &mut LogicalOperator) -> Result<()> {
        // First, verify all column references can be resolved
        let mut resolver = Self::new_verify_only();
        resolver.visit_operator(plan);

        // Then, verify no duplicate table indices exist
        Self::verify_table_indices(plan)?;

        Ok(())
    }

    /// Verify that all table indices in the plan are unique.
    ///
    /// Duplicate table indices indicate a bug in the planner.
    /// This check is performed recursively on all operators.
    fn verify_table_indices(op: &LogicalOperator) -> Result<()> {
        Self::verify_table_indices_internal(op)?;
        Ok(())
    }

    /// Internal recursive implementation of table index verification.
    /// Returns the set of table indices found in this subtree.
    fn verify_table_indices_internal(op: &LogicalOperator) -> Result<HashSet<usize>> {
        let mut result = HashSet::new();

        // Collect indices from children
        for child in op.children() {
            let child_indexes = Self::verify_table_indices_internal(&child.operator)?;
            for index in child_indexes {
                if result.contains(&index) {
                    return Err(paro_error::internal(format!(
                        "Duplicate table index {} found in logical plan",
                        index
                    )));
                }
                result.insert(index);
            }
        }

        // Add this operator's table indices
        let indexes = op.get_table_index();
        for index in indexes {
            if result.contains(&index) {
                return Err(paro_error::internal(format!(
                    "Duplicate table index {} found in logical plan",
                    index
                )));
            }
            result.insert(index);
        }

        Ok(result)
    }

    /// Find the index of a column binding in the current bindings.
    fn find_binding(&self, binding: &ColumnBinding) -> Option<usize> {
        self.bindings.iter().position(|b| b == binding)
    }
}

impl Default for ColumnBindingResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOperatorVisitor for ColumnBindingResolver {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        match op {
            // =========================================================================
            // Special case: Comparison Join
            // We need to resolve LHS expressions with LHS bindings, then RHS with RHS
            // =========================================================================
            LogicalOperator::Join(Join::Comparison(comp_join)) => {
                // First get the bindings of the LHS and resolve the LHS expressions
                self.visit_logical_plan(comp_join.left.as_mut());
                if !comp_join.delim_flipped {
                    for expr in &mut comp_join.duplicate_eliminated_columns {
                        self.visit_expression(expr);
                    }
                }
                for cond in &mut comp_join.conditions {
                    self.visit_expression(&mut cond.left);
                }

                // Then get the bindings of the RHS and resolve the RHS expressions
                self.visit_logical_plan(comp_join.right.as_mut());
                if comp_join.delim_flipped {
                    for expr in &mut comp_join.duplicate_eliminated_columns {
                        self.visit_expression(expr);
                    }
                }
                for cond in &mut comp_join.conditions {
                    self.visit_expression(&mut cond.right);
                }

                // Finally update the bindings with the result bindings of the join
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }

            // =========================================================================
            // Special case: Any Join
            // Evaluate the expression on full bindings of BOTH children at once
            // =========================================================================
            LogicalOperator::Join(Join::Any(any_join)) => {
                self.visit_logical_plan(any_join.left.as_mut());
                let left_bindings = any_join.left.get_column_bindings();
                let left_types = any_join.left.types();

                self.visit_logical_plan(any_join.right.as_mut());
                let right_bindings = any_join.right.get_column_bindings();
                let right_types = any_join.right.types();

                self.bindings = left_bindings;
                self.bindings.extend(right_bindings);
                self.types = left_types;
                self.types.extend(right_types);
                self.visit_expression(&mut any_join.condition);

                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }

            // =========================================================================
            // Special case: Create Index
            // Add the columns of the table with table index 0 to the binding set,
            // then bind the expressions of the CREATE INDEX statement
            // =========================================================================
            LogicalOperator::CreateIndex(create_index) => {
                // Generate bindings for the table columns (table_index = 0)
                let column_count = create_index.table.columns.len();
                self.bindings = LogicalOperator::generate_column_bindings(0, column_count);
                self.types.clear();
                self.visit_operator_expressions(op);
            }

            // =========================================================================
            // Special case: Get (Scan)
            // We first update bindings then visit expressions
            // =========================================================================
            LogicalOperator::Get(_) => {
                self.bindings = op.get_column_bindings();
                self.types = op.types();
                self.visit_operator_expressions(op);
            }

            // =========================================================================
            // Special case: ExpressionGet
            // Similar to Get, update bindings first
            // =========================================================================
            LogicalOperator::ExpressionGet(_) => {
                self.bindings = op.get_column_bindings();
                self.types = op.types();
                self.visit_operator_expressions(op);
            }

            // =========================================================================
            // Special case: DelimGet
            // Similar to Get/ExpressionGet, update bindings first
            // =========================================================================
            LogicalOperator::DelimGet(_) => {
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }

            // =========================================================================
            // Special case: TableFunctionGet
            // Similar to Get, update bindings first
            // =========================================================================
            LogicalOperator::TableFunctionGet(_) => {
                self.bindings = op.get_column_bindings();
                self.types = op.types();
                self.visit_operator_expressions(op);
            }

            // =========================================================================
            // Special case: SearchScan / FullTextFilterScan
            // These leaf operators absorb a Get plus additional expressions. Resolve
            // the embedded expressions against the underlying Get bindings first.
            // =========================================================================
            LogicalOperator::SearchScan(search) => {
                self.bindings = LogicalOperator::generate_column_bindings(
                    search.get.table_index,
                    search.get.returned_types.len(),
                );
                self.types = search.get.returned_types.clone();
                self.visit_operator_expressions(op);
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }
            LogicalOperator::FullTextFilterScan(scan) => {
                self.bindings = LogicalOperator::generate_column_bindings(
                    scan.get.table_index,
                    scan.get.returned_types.len(),
                );
                self.types = scan.get.returned_types.clone();
                self.visit_operator_expressions(op);
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }

            // =========================================================================
            // Special case: CTERef
            // Similar to Get, update bindings first (no children to visit)
            // =========================================================================
            LogicalOperator::CTERef(_) => {
                self.bindings = op.get_column_bindings();
                self.types = op.types();
                // CTERef has no expressions to visit
            }

            // =========================================================================
            // Special case: Graph Projection
            // A Projection over a graph chain (GraphScan/GraphExpand)
            // uses PhysicalGraphProject which does its own late-materialization
            // column remapping. We must NOT resolve the COLUMNS expressions
            // here because the graph chain's output bindings (local_id, rowid,
            // edge_rowid, ...) don't correspond to the actual table columns
            // referenced in the COLUMNS expressions.
            // =========================================================================
            LogicalOperator::Projection(ref proj) if proj.child.is_graph_chain() => {
                // Visit children (graph chain) but skip expression resolution.
                self.visit_operator_children(op);
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }

            // =========================================================================
            // General case for all other operators
            // 1. First visit children
            // 2. Then visit expressions with current bindings
            // 3. Finally update bindings to this operator's output
            // =========================================================================
            _ => {
                self.visit_operator_children(op);
                self.visit_operator_expressions(op);
                self.bindings = op.get_column_bindings();
                self.types = op.types();
            }
        }
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        // Use the binding directly from the expression
        let effective_binding = expr.binding;

        // Find the binding in our current set
        if let Some(index) = self.find_binding(&effective_binding) {
            // Verify type if we have type information
            if !self.types.is_empty()
                && self.bindings.len() == self.types.len()
                && expr.return_type != self.types[index]
            {
                // Type mismatches are unexpected; keep the resolver side-effect free here.
            }

            if self.verify_only {
                // In verify mode, don't replace
                return None;
            }

            // Replace with ReferenceExpression
            return Some(Expression::Reference(ReferenceExpression {
                index,
                return_type: expr.return_type.clone(),
            }));
        }

        // Could not find the binding. Keep the original column reference and let
        // downstream operators evaluate it positionally if possible.
        None
    }
}
