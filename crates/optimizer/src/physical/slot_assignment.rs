// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Assign execution slots while extracting a selected physical skeleton.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression, ReferenceExpression};
use paro_planner::operator::{ColumnBinding, Join, LogicalOperator};
use paro_planner::visitor::LogicalOperatorVisitor;

/// Assigns physical chunk ordinals to stable planner bindings in an already
/// selected winner skeleton.
///
/// It traverses the logical plan and replaces all ColumnRefExpression
/// with ReferenceExpression, where the index is the position in the
/// current set of column bindings.
///
/// # Usage
/// ```ignore
/// assign_expression_slots(&mut plan)?;
/// ```
pub(crate) fn assign_expression_slots(plan: &mut LogicalOperator) -> Result<()> {
    let mut assigner = SlotAssigner::new();
    assigner.visit_operator(plan);
    assigner.error.map_or(Ok(()), Err)
}

struct SlotAssigner {
    /// Current column bindings from child operators.
    /// Updated as we traverse the plan tree.
    bindings: Vec<ColumnBinding>,
    /// Types of current bindings (for verification).
    types: Vec<LogicalType>,
    error: Option<paro_common::error::ParoError>,
    scope: String,
}

impl SlotAssigner {
    fn new() -> Self {
        Self {
            bindings: Vec::new(),
            types: Vec::new(),
            error: None,
            scope: "operator input".to_string(),
        }
    }

    /// Slot assignment is an extraction operation. Calling it before winner
    /// selection would erase logical identity and is intentionally impossible
    /// outside this crate.
    /// Find the index of a column binding in the current bindings.
    fn find_binding(&self, binding: &ColumnBinding) -> Option<usize> {
        self.bindings.iter().position(|b| b == binding)
    }
}

impl Default for SlotAssigner {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOperatorVisitor for SlotAssigner {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        match op {
            // =========================================================================
            // Special case: Comparison Join
            // We need to resolve LHS expressions with LHS bindings, then RHS with RHS
            // =========================================================================
            LogicalOperator::Join(Join::Comparison(comp_join)) => {
                let join_type = comp_join.join_type;
                let left_bindings = comp_join.left.get_column_bindings();
                let right_bindings = comp_join.right.get_column_bindings();
                // First get the bindings of the LHS and resolve the LHS expressions
                self.visit_logical_plan(comp_join.left.as_mut());
                if !comp_join.delim_flipped {
                    self.scope = format!(
                        "{join_type} comparison join left duplicate key; left={left_bindings:?}, right={right_bindings:?}"
                    );
                    for expr in &mut comp_join.duplicate_eliminated_columns {
                        self.visit_expression(expr);
                    }
                }
                self.scope = format!(
                    "{join_type} comparison join probe key; left={left_bindings:?}, right={right_bindings:?}"
                );
                for cond in &mut comp_join.conditions {
                    self.visit_expression(&mut cond.left);
                }

                // Then get the bindings of the RHS and resolve the RHS expressions
                self.visit_logical_plan(comp_join.right.as_mut());
                if comp_join.delim_flipped {
                    self.scope = format!(
                        "{join_type} comparison join right duplicate key; left={left_bindings:?}, right={right_bindings:?}"
                    );
                    for expr in &mut comp_join.duplicate_eliminated_columns {
                        self.visit_expression(expr);
                    }
                }
                self.scope = format!(
                    "{join_type} comparison join build key; left={left_bindings:?}, right={right_bindings:?}"
                );
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
            // Special case: Aggregate with an optional post-reduction domain
            //
            // Ordinary GROUP BY and aggregate inputs are evaluated against the
            // child and therefore resolve normally. Post-reduction reducers and
            // their predicate intentionally retain logical bindings to finalized
            // aggregate outputs; aggregate extraction validates and rebases
            // those independent local domains after slot assignment.
            // =========================================================================
            LogicalOperator::Aggregate(aggregate) => {
                self.visit_logical_plan(aggregate.child.as_mut());
                self.scope = "Aggregate input expression".to_string();
                for expression in &mut aggregate.groups {
                    self.visit_expression(expression);
                }
                for expression in &mut aggregate.aggregates {
                    self.visit_expression(expression);
                }
                if self.error.is_none() {
                    if let Err(error) = aggregate.verify_post_reduction() {
                        self.error = Some(error);
                    }
                }

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
            // uses GraphProject which does its own late-materialization
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
                let operator_type = op.op_type();
                self.visit_operator_children(op);
                self.scope = format!("{operator_type:?} expression");
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
                if self.error.is_none() {
                    self.error = Some(paro_error::internal(format!(
                        "Column binding type mismatch for {:?} in {} at input index {}: expression={:?}, input={:?}",
                        effective_binding, self.scope, index, expr.return_type, self.types[index]
                    )));
                }
                return None;
            }

            // Replace with ReferenceExpression
            return Some(Expression::Reference(
                ReferenceExpression {
                    index,
                    return_type: expr.return_type.clone(),
                }
                .into(),
            ));
        }

        if self.error.is_none() {
            self.error = Some(paro_error::internal(format!(
                "Column binding {:?} ({:?}) is not produced by {} {:?}",
                effective_binding, expr.return_type, self.scope, self.bindings
            )));
        }
        None
    }
}
