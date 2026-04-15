// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Detects correlated column references in expressions and operator trees at a given lateral depth.

use crate::binder::CorrelatedColumnInfo;
use crate::expression::{Expression, ExpressionIterator};
use crate::operator::LogicalOperator;
use crate::plan::LogicalPlan;

pub struct HasCorrelatedExpressions {
    has_correlated: bool,
    correlated_columns: Vec<CorrelatedColumnInfo>,
    lateral_depth: usize,
}

impl HasCorrelatedExpressions {
    pub fn new_lateral(
        correlated_columns: Vec<CorrelatedColumnInfo>,
        lateral_depth: usize,
    ) -> Self {
        Self {
            has_correlated: false,
            correlated_columns,
            lateral_depth,
        }
    }

    pub fn has_correlated_expressions(&self) -> bool {
        self.has_correlated
    }

    fn matches_correlated_column(
        &self,
        table_index: usize,
        column_index: usize,
        depth: usize,
        lateral_depth: usize,
    ) -> bool {
        if depth <= lateral_depth || depth != lateral_depth + 1 {
            return false;
        }

        self.correlated_columns.iter().any(|corr| {
            corr.table_index == table_index
                && corr.column_index == column_index
                && corr.depth == depth
        })
    }

    fn visit_expression_internal(&mut self, expr: &Expression, lateral_depth: usize) {
        match expr {
            Expression::ColumnRef(col_ref) => {
                if self.matches_correlated_column(
                    col_ref.binding.table_index,
                    col_ref.binding.column_index,
                    col_ref.depth,
                    lateral_depth,
                ) {
                    self.has_correlated = true;
                }
            }
            Expression::Subquery(subquery) => {
                if !subquery.correlated_columns.is_empty() {
                    for corr in &self.correlated_columns {
                        for sub_corr in &subquery.correlated_columns {
                            if corr.table_index == sub_corr.table_index
                                && corr.column_index == sub_corr.column_index
                                && corr.depth == sub_corr.depth
                                && sub_corr.depth == lateral_depth + 1
                            {
                                self.has_correlated = true;
                                return;
                            }
                        }
                    }
                }
                return;
            }
            Expression::Constant(_) | Expression::Reference(_) => {}
            _ => {}
        }

        ExpressionIterator::enumerate_children(expr, |child| {
            self.visit_expression_internal(child, lateral_depth);
        });
    }

    pub fn visit_operator(&mut self, op: &LogicalOperator) {
        self.visit_operator_internal(op, self.lateral_depth);
    }

    fn visit_operator_internal(&mut self, op: &LogicalOperator, lateral_depth: usize) {
        self.visit_operator_expressions(op, lateral_depth);
        match op {
            LogicalOperator::DependentJoin(dep) => {
                self.visit_logical_plan(&dep.left, lateral_depth);
                self.visit_logical_plan(&dep.right, lateral_depth + 1);
            }
            _ => {
                for child in op.children() {
                    self.visit_logical_plan(child, lateral_depth);
                }
            }
        }
    }

    fn visit_logical_plan(&mut self, plan: &LogicalPlan, lateral_depth: usize) {
        self.visit_operator_internal(&plan.operator, lateral_depth);
    }

    fn visit_operator_expressions(&mut self, op: &LogicalOperator, lateral_depth: usize) {
        match op {
            LogicalOperator::Filter(filter) => {
                for expr in &filter.expressions {
                    self.visit_expression_internal(expr, lateral_depth);
                }
            }
            LogicalOperator::Projection(proj) => {
                for expr in &proj.expressions {
                    self.visit_expression_internal(expr, lateral_depth);
                }
            }
            LogicalOperator::Aggregate(agg) => {
                for expr in &agg.groups {
                    self.visit_expression_internal(expr, lateral_depth);
                }
                for expr in &agg.aggregates {
                    self.visit_expression_internal(expr, lateral_depth);
                }
            }
            LogicalOperator::Order(order) => {
                for order_expr in &order.orders {
                    self.visit_expression_internal(&order_expr.expression, lateral_depth);
                }
            }
            LogicalOperator::Join(join) => {
                use crate::operator::Join;
                match join {
                    Join::Comparison(comp) => {
                        for cond in &comp.conditions {
                            self.visit_expression_internal(&cond.left, lateral_depth);
                            self.visit_expression_internal(&cond.right, lateral_depth);
                        }
                    }
                    Join::Any(any) => {
                        self.visit_expression_internal(&any.condition, lateral_depth);
                    }
                    Join::Cross(_) => {}
                }
            }
            LogicalOperator::DependentJoin(dep) => {
                if let Some(cond) = dep.join_condition() {
                    self.visit_expression_internal(cond, lateral_depth);
                }
                if let Some(payload) = dep.any_all_payload() {
                    for expr in &payload.expression_children {
                        self.visit_expression_internal(expr, lateral_depth);
                    }
                }
            }
            _ => {}
        }
    }
}

pub fn expression_has_correlated_columns_at_depth(
    expr: &Expression,
    correlated_columns: &[CorrelatedColumnInfo],
    lateral_depth: usize,
) -> bool {
    let mut visitor =
        HasCorrelatedExpressions::new_lateral(correlated_columns.to_vec(), lateral_depth);
    visitor.visit_expression_internal(expr, lateral_depth);
    visitor.has_correlated_expressions()
}

pub fn operator_has_correlated_columns_at_depth(
    op: &LogicalOperator,
    correlated_columns: &[CorrelatedColumnInfo],
    lateral_depth: usize,
) -> bool {
    let mut visitor =
        HasCorrelatedExpressions::new_lateral(correlated_columns.to_vec(), lateral_depth);
    visitor.visit_operator(op);
    visitor.has_correlated_expressions()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{ColumnRefExpression, Expression};
    use crate::operator::{ColumnBinding, ExpressionGet};
    use paro_common::types::LogicalType;

    fn correlated_column(depth: usize) -> CorrelatedColumnInfo {
        CorrelatedColumnInfo {
            table_index: 10,
            column_index: 0,
            return_type: LogicalType::Integer,
            name: "corr".to_string(),
            depth,
        }
    }

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::<Vec<Expression>>::new(),
            vec!["c0".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    #[test]
    fn lateral_detection_ignores_local_correlations_from_same_scope() {
        let expr = Expression::ColumnRef(ColumnRefExpression::with_depth(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
            1,
        ));

        assert!(!expression_has_correlated_columns_at_depth(
            &expr,
            &[correlated_column(1)],
            1,
        ));
    }

    #[test]
    fn lateral_detection_only_matches_next_outer_scope() {
        let expr = Expression::ColumnRef(ColumnRefExpression::with_depth(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
            2,
        ));

        assert!(expression_has_correlated_columns_at_depth(
            &expr,
            &[correlated_column(2)],
            1,
        ));
        assert!(!expression_has_correlated_columns_at_depth(
            &expr,
            &[correlated_column(2)],
            0,
        ));
    }

    #[test]
    fn nested_dependent_join_right_child_is_treated_as_local_to_nested_scope() {
        use crate::binder::context::BindContext;
        let ctx = BindContext::new();
        let nested_right = LogicalOperator::Projection(crate::operator::Projection::new(
            20,
            crate::plan::LogicalPlan::new(&ctx, expression_get(30)),
            vec![Expression::ColumnRef(ColumnRefExpression::with_depth(
                ColumnBinding::new(10, 0),
                LogicalType::Integer,
                2,
            ))],
        ));
        let dependent = LogicalOperator::DependentJoin(crate::operator::DependentJoin::scalar(
            crate::plan::LogicalPlan::new(&ctx, expression_get(11)),
            crate::plan::LogicalPlan::new(&ctx, nested_right),
            vec![correlated_column(1)],
        ));

        assert!(!operator_has_correlated_columns_at_depth(
            &dependent,
            &[correlated_column(2)],
            1,
        ));
    }
}
