// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Visitor trait over [`crate::operator::LogicalOperator`] and expressions.

use std::ops::ControlFlow;

use crate::expression::*;
use crate::operator::LogicalOperator;
use crate::plan::OwnedLogicalPlan;

/// LogicalOperatorVisitor trait for traversing logical plans.
///
/// This trait provides the foundation for all optimizer passes. Implementations
/// can override specific methods to transform operators or expressions.
///
/// # Usage Pattern
///
/// ```ignore
/// struct MyOptimizer;
///
/// impl LogicalOperatorVisitor for MyOptimizer {
///     fn visit_replace_column_ref(
///         &mut self,
///         expr: &ColumnRefExpression,
///     ) -> Option<Expression> {
///         // Transform column references
///         Some(Expression::Reference(...))
///     }
/// }
/// ```
pub trait LogicalOperatorVisitor {
    /// Visit a logical operator. Default implementation visits children first,
    /// then expressions.
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        self.visit_operator_children(op);
        self.visit_operator_expressions(op);
    }

    /// Visit a child [`OwnedLogicalPlan`] (delegates to the wrapped operator).
    fn visit_logical_plan(&mut self, plan: &mut OwnedLogicalPlan) {
        self.visit_operator(&mut plan.operator);
    }

    /// Visit all child operators. Can be overridden to change traversal order.
    fn visit_operator_children(&mut self, op: &mut LogicalOperator) {
        let _ = op.visit_children_mut(|child| {
            self.visit_logical_plan(child);
            ControlFlow::Continue(())
        });
    }

    /// Visit all expressions in an operator. Calls `visit_expression` for each.
    fn visit_operator_expressions(&mut self, op: &mut LogicalOperator) {
        enumerate_expressions(op, |expr| {
            self.visit_expression(expr);
        });
    }

    /// Visit an expression. May return a replacement expression.
    /// Default implementation dispatches to type-specific visit_replace_* methods.
    fn visit_expression(&mut self, expr: &mut Expression) {
        let replacement = match expr {
            Expression::Aggregate(e) => self.visit_replace_aggregate(e),
            Expression::Case(e) => self.visit_replace_case(e),
            Expression::Cast(e) => self.visit_replace_cast(e),
            Expression::ColumnRef(e) => self.visit_replace_column_ref(e),
            Expression::Comparison(e) => self.visit_replace_comparison(e),
            Expression::Conjunction(e) => self.visit_replace_conjunction(e),
            Expression::Constant(e) => self.visit_replace_constant(e),
            Expression::Function(e) => self.visit_replace_function(e),
            Expression::Operator(e) => self.visit_replace_operator(e),
            Expression::Parameter(_) => None,
            Expression::Reference(e) => self.visit_replace_reference(e),
            Expression::Subquery(e) => self.visit_replace_subquery(e),
            Expression::Window(e) => self.visit_replace_window(e),
        };

        if let Some(new_expr) = replacement {
            *expr = new_expr;
        } else {
            // No replacement, visit children
            self.visit_expression_children(expr);
        }
    }

    /// Visit children of an expression. Called when visit_replace_* returns None.
    fn visit_expression_children(&mut self, expr: &mut Expression) {
        ExpressionIterator::enumerate_children_mut(expr, |child| {
            self.visit_expression(child);
        });
    }

    // =========================================================================
    // Expression-specific visit methods. Override to transform specific types.
    // Return Some(expr) to replace, None to continue visiting children.
    // =========================================================================

    fn visit_replace_aggregate(&mut self, _expr: &mut AggregateExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_case(&mut self, _expr: &mut CaseExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_cast(&mut self, _expr: &mut CastExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_column_ref(&mut self, _expr: &mut ColumnRefExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_comparison(&mut self, _expr: &mut ComparisonExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_conjunction(
        &mut self,
        _expr: &mut ConjunctionExpression,
    ) -> Option<Expression> {
        None
    }

    fn visit_replace_constant(&mut self, _expr: &mut ConstantExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_function(&mut self, _expr: &mut FunctionExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_operator(&mut self, _expr: &mut OperatorExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_reference(&mut self, _expr: &mut ReferenceExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_subquery(&mut self, _expr: &mut SubqueryExpression) -> Option<Expression> {
        None
    }

    fn visit_replace_window(&mut self, _expr: &mut WindowExpression) -> Option<Expression> {
        None
    }
}

/// One local-scalar field contract generates both borrowed and mutable walks.
/// Immutable consumers must never clone an operator just to visit its scalars.
macro_rules! define_expression_enumerator {
    ($name:ident, [$($borrow:tt)*], $window:ident, $payload:ident, $condition:ident) => {
pub fn $name<Child, F>(op: &$($borrow)* LogicalOperator<Child>, mut callback: F)
where
    F: FnMut(&$($borrow)* Expression),
{
    match op {
        LogicalOperator::Get(get) => {
            // Get may have filter expressions in table_filters
            // Currently not exposed, but can be added if needed
            let _ = get;
        }
        LogicalOperator::Filter(filter) => {
            for expr in &$($borrow)* filter.expressions {
                callback(expr);
            }
        }
        LogicalOperator::Projection(proj) => {
            for expr in &$($borrow)* proj.expressions {
                callback(expr);
            }
        }
        LogicalOperator::RowFetch(fetch) => {
            for source in &$($borrow)* fetch.sources {
                callback(&$($borrow)* source.rowid);
            }
        }
        LogicalOperator::ExternalProject(project) => {
            for expr in &$($borrow)* project.expressions {
                callback(&$($borrow)* expr.expression);
            }
        }
        LogicalOperator::ExternalTable(table) => {
            callback(&$($borrow)* table.call_expression);
        }
        LogicalOperator::Limit(limit) => {
            if let Some(expr) = &$($borrow)* limit.limit {
                callback(expr);
            }
            if let Some(expr) = &$($borrow)* limit.offset {
                callback(expr);
            }
        }
        LogicalOperator::Order(order) => {
            for bound_order in &$($borrow)* order.orders {
                callback(&$($borrow)* bound_order.expression);
            }
        }
        LogicalOperator::TopN(topn) => {
            for bound_order in &$($borrow)* topn.orders {
                callback(&$($borrow)* bound_order.expression);
            }
        }
        LogicalOperator::Aggregate(agg) => {
            for expr in &$($borrow)* agg.groups {
                callback(expr);
            }
            for expr in &$($borrow)* agg.aggregates {
                callback(expr);
            }
            if let Some(reduction) = &$($borrow)* agg.post_reduction {
                for reducer in &$($borrow)* reduction.reducers {
                    callback(reducer);
                }
                for scalar in &$($borrow)* reduction.scalar_expressions {
                    callback(scalar);
                }
                callback(&$($borrow)* reduction.predicate);
            }
        }
        LogicalOperator::Insert(_insert) => {
            // Insert doesn't have expressions to enumerate
            // The child provides the data
        }
        LogicalOperator::Delete(_delete) => {
            // Delete doesn't have expressions
            // The filter is in the child operator
        }
        LogicalOperator::Update(update) => {
            for expr in &$($borrow)* update.expressions {
                callback(expr);
            }
        }
        LogicalOperator::CopyTo(_copy) => {
            // COPY TO doesn't have expressions to enumerate
        }
        LogicalOperator::ExpressionGet(expr_get) => {
            for expr_list in &$($borrow)* expr_get.expressions {
                for expr in expr_list {
                    callback(expr);
                }
            }
        }
        LogicalOperator::DelimGet(_) => {
            // DelimGet has no expressions of its own.
        }
        LogicalOperator::Join(join) => {
            match join {
                crate::operator::Join::Comparison(cj) => {
                    for cond in &$($borrow)* cj.conditions {
                        callback(&$($borrow)* cond.left);
                        callback(&$($borrow)* cond.right);
                    }
                }
                crate::operator::Join::Any(aj) => {
                    callback(&$($borrow)* aj.condition);
                }
                crate::operator::Join::Cross(_) => {
                    // Cross product has no join conditions
                }
            }
        }
        LogicalOperator::DependentJoin(dj) => {
            if let Some(payload) = dj.$payload() {
                for expr in &$($borrow)* payload.expression_children {
                    callback(expr);
                }
            }
            if let Some(cond) = dj.$condition() {
                callback(cond);
            }
        }
        LogicalOperator::SetOperation(_) => {
            // Set operations don't have expressions to enumerate
        }
        LogicalOperator::Distinct(distinct) => {
            for expr in &$($borrow)* distinct.distinct_targets {
                callback(expr);
            }
            if let Some(orders) = &$($borrow)* distinct.order_by {
                for order in orders {
                    callback(&$($borrow)* order.expression);
                }
            }
        }
        LogicalOperator::Window(window) => {
            for window_expr in &$($borrow)* window.expressions {
                ExpressionIterator::$window(window_expr, &mut callback);
            }
        }
        LogicalOperator::Explain(_) => {
            // Explain itself has no expressions; child expressions are visited in traversal.
        }
        LogicalOperator::EmptyResult(_) => {
            // EmptyResult preserves schema only and has no local expressions.
        }
        LogicalOperator::MaterializedCTE(_) | LogicalOperator::RecursiveCTE(_) => {
            // CTE itself doesn't have expressions, the child does
        }
        LogicalOperator::CTERef(_) => {
            // CTERef doesn't have expressions
        }
        LogicalOperator::TableFunctionGet(tf) => {
            for expr in &$($borrow)* tf.arguments {
                callback(expr);
            }
        }
        LogicalOperator::SearchScan(search) => {
            for expr in &$($borrow)* search.projections {
                callback(expr);
            }
            for expr in &$($borrow)* search.absorbed_predicates {
                callback(expr);
            }
            for expr in &$($borrow)* search.residual_predicates {
                callback(expr);
            }
            callback(&$($borrow)* search.score_expression);
        }
        LogicalOperator::FullTextFilterScan(scan) => {
            callback(&$($borrow)* scan.match_expression);
            for expr in &$($borrow)* scan.other_predicates {
                callback(expr);
            }
            for expr in &$($borrow)* scan.residual_predicates {
                callback(expr);
            }
        }
        // DDL and other operators without expressions
        LogicalOperator::Alter(_)
        | LogicalOperator::BoundReference(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateRoutine(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::GraphMatch(_)
        | LogicalOperator::GraphScan(_)
        | LogicalOperator::GraphExpand(_)
        | LogicalOperator::DummyScan => {}
    }
}
    };
}

define_expression_enumerator!(enumerate_expressions, [mut], enumerate_window_children_mut, any_all_payload_mut, join_condition_mut);
define_expression_enumerator!(
    enumerate_expression_refs,
    [],
    enumerate_window_children,
    any_all_payload,
    join_condition
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::context::BindContext;
    use crate::operator::{EmptyResult, Filter, Join};

    struct RecordingVisitor {
        visited: Vec<crate::plan::PlanNodeId>,
    }

    impl LogicalOperatorVisitor for RecordingVisitor {
        fn visit_logical_plan(&mut self, plan: &mut OwnedLogicalPlan) {
            self.visited.push(plan.id);
            self.visit_operator(&mut plan.operator);
        }
    }

    #[test]
    fn visitor_recurses_via_operator_child_primitive() {
        let ctx = BindContext::new();

        let left_leaf = OwnedLogicalPlan::new(&ctx, LogicalOperator::DummyScan);
        let left_leaf_id = left_leaf.id;
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Filter(Filter::new(left_leaf, Vec::new())),
        );
        let left_id = left.id;

        let right_leaf = OwnedLogicalPlan::new(&ctx, LogicalOperator::DummyScan);
        let right_leaf_id = right_leaf.id;
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::EmptyResult(EmptyResult::new(right_leaf)),
        );
        let right_id = right.id;

        let mut root = LogicalOperator::Join(Join::cross(left, right));
        let mut visitor = RecordingVisitor {
            visited: Vec::new(),
        };
        visitor.visit_operator(&mut root);

        assert_eq!(
            visitor.visited,
            vec![left_id, left_leaf_id, right_id, right_leaf_id]
        );
    }
}
