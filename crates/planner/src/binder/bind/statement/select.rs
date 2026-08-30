// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bind Select Statement
//!
//!

use crate::binder::bind::clause::{AliasLookup, OrderBinder, SelectBindState};
use crate::binder::ir::{
    BoundQuery, BoundSetOperation, BoundStatementKind, OrderByNode, SetOperationType, WithCTE,
};
use crate::binder::plan::subquery::{split_child_correlated_columns, CorrelationBoundaryMode};
use crate::binder::Binder;
use crate::stack::maybe_grow_planner_stack;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{OrderByExpr, SetExpr, SetOperator};

impl Binder {
    fn absorb_child_correlated_columns(
        &mut self,
        correlated_columns: Vec<crate::binder::CorrelatedColumnInfo>,
    ) {
        let split = split_child_correlated_columns(
            correlated_columns,
            CorrelationBoundaryMode::TransparentBoundary,
        );
        for corr in split.propagate_to_parent {
            if !self.correlated_columns.contains(&corr) {
                self.correlated_columns.push(corr);
            }
        }
    }

    /// Bind a Query (BoundSelect, BoundValues, or BoundSetOperation).
    pub fn bind_query(&mut self, statement: paro_parser::ast::Query) -> Result<BoundQuery> {
        maybe_grow_planner_stack(|| self.bind_query_inner(statement))
    }

    fn bind_query_inner(&mut self, statement: paro_parser::ast::Query) -> Result<BoundQuery> {
        let mut registered_ctes = Vec::new();

        // First, register any CTEs from the WITH clause
        if let Some(with) = statement.with {
            for cte in with.ctes {
                registered_ctes.push(self.register_cte(cte, with.recursive)?);
            }
        }

        // Then bind the main query body
        let query = self.bind_set_expr(
            statement.body,
            &statement.order_by,
            &statement.limit,
            &statement.offset,
        )?;

        if registered_ctes.is_empty() {
            Ok(query)
        } else {
            Ok(BoundQuery::With(Box::new(WithCTE {
                ctes: registered_ctes,
                child: Box::new(query),
            })))
        }
    }

    pub fn bind_set_expr(
        &mut self,
        body: SetExpr,
        order_by: &[OrderByExpr],
        limit: &[paro_parser::ast::Expr],
        offset: &Option<paro_parser::ast::Expr>,
    ) -> Result<BoundQuery> {
        match body {
            SetExpr::Select(select) => self.bind_select_query(*select, order_by, limit, offset),
            SetExpr::Values { values, .. } => {
                let node = self.bind_values_rows(values)?;
                let query = BoundQuery::Values(node);
                self.bind_output_modifiers(query, order_by, limit, offset)
            }
            SetExpr::SetOperation(set_op) => {
                let (left_node, left_correlated_columns) = {
                    let mut left_binder = self.create_child();
                    let node = left_binder.bind_set_expr(*set_op.left, &[], &[], &None)?;
                    (node, left_binder.correlated_columns)
                };
                let (right_node, right_correlated_columns) = {
                    let mut right_binder = self.create_child();
                    let node = right_binder.bind_set_expr(*set_op.right, &[], &[], &None)?;
                    (node, right_binder.correlated_columns)
                };
                self.absorb_child_correlated_columns(left_correlated_columns);
                self.absorb_child_correlated_columns(right_correlated_columns);
                let query =
                    self.bind_set_operation(set_op.op, set_op.all, left_node, right_node)?;
                self.bind_output_modifiers(query, order_by, limit, offset)
            }
            SetExpr::Query(query) => {
                let query = self.bind_query(*query)?;
                self.bind_output_modifiers(query, order_by, limit, offset)
            }
        }
    }

    /// Bind modifiers for query bodies whose ORDER BY can only reference
    /// visible result columns (VALUES and set-operation results).
    fn bind_output_modifiers(
        &mut self,
        query: BoundQuery,
        order_by: &[OrderByExpr],
        limit: &[paro_parser::ast::Expr],
        offset: &Option<paro_parser::ast::Expr>,
    ) -> Result<BoundQuery> {
        let bound_order_by = self.bind_output_order_by(&query, order_by)?;
        let bound_limit = self.bind_limit(limit, offset)?;
        Ok(query.with_modifiers(bound_order_by, bound_limit, Default::default(), None))
    }

    fn bind_output_order_by(
        &mut self,
        query: &BoundQuery,
        order_by: &[OrderByExpr],
    ) -> Result<Option<Vec<OrderByNode>>> {
        if order_by.is_empty() {
            return Ok(None);
        }

        let names = query.names();
        let types = query.types();
        let output_table_index = query.output_table_index();
        let mut bind_state = SelectBindState::new();
        for (index, name) in names.iter().enumerate() {
            bind_state.add_alias(name, false, index);
        }
        let alias_lookup = AliasLookup::snapshot(&bind_state);
        let mut order_binder = OrderBinder::new(self, &mut bind_state, alias_lookup);
        let mut orders = Vec::with_capacity(order_by.len());

        for order in order_by {
            let binding = order_binder.bind(order.expr.clone())?;
            let return_type = types.get(binding.index).cloned().ok_or_else(|| {
                paro_error::syntax("ORDER BY position is not in the query result")
            })?;
            orders.push(OrderByNode {
                expression: binding.to_bound_expression_with_type(output_table_index, return_type),
                ascending: order.asc.unwrap_or(true),
                nulls_first: order.nulls_first.unwrap_or(!order.asc.unwrap_or(true)),
            });
        }
        Self::simplify_order_by(&mut orders);
        Ok(Some(orders))
    }

    pub fn bind_set_operation(
        &mut self,
        op: SetOperator,
        all: bool,
        left: BoundQuery,
        right: BoundQuery,
    ) -> Result<BoundQuery> {
        // 1. Verify column counts match
        let left_types = left.types();
        let right_types = right.types();
        if left_types.len() != right_types.len() {
            return Err(paro_error::syntax(format!(
                "UNION/INTERSECT/EXCEPT column count mismatch: {} vs {}",
                left_types.len(),
                right_types.len()
            )));
        }

        // 2. Generate a table_index for the set operation result
        let table_index = self.bind_context.generate_table_index();

        // 3. Determine result types using max_logical_type for compatibility
        let mut result_types = Vec::new();
        for (lt, rt) in left_types.iter().zip(right_types.iter()) {
            let result_type = LogicalType::max_logical_type(lt, rt);
            result_types.push(result_type);
        }

        let names = left.names();

        let setop_type = match (op, all) {
            (SetOperator::Union, false) => SetOperationType::Union,
            (SetOperator::Union, true) => SetOperationType::UnionAll,
            (SetOperator::Intersect, false) => SetOperationType::Intersect,
            (SetOperator::Intersect, true) => SetOperationType::IntersectAll,
            (SetOperator::Except, false) => SetOperationType::Except,
            (SetOperator::Except, true) => SetOperationType::ExceptAll,
        };

        Ok(BoundQuery::SetOperation(Box::new(BoundSetOperation {
            table_index,
            setop_type,
            left: Box::new(left),
            right: Box::new(right),
            names,
            types: result_types,
        })))
    }
}

/// Entry point for binding a Select statement.
pub fn bind_select(
    binder: &mut Binder,
    statement: paro_parser::ast::Query,
) -> Result<BoundStatementKind> {
    let node = binder.bind_query(statement)?;
    Ok(BoundStatementKind::Query(Box::new(node)))
}

#[cfg(test)]
mod tests {
    use crate::binder::test_utils::test_binder;
    use crate::operator::LogicalOperator;

    fn plan(sql: &str) -> crate::plan::LogicalPlan {
        let statement = paro_parser::parse_one(sql).expect("parse query").stmt;
        test_binder().bind(statement).expect("plan query").plan
    }

    #[test]
    fn values_query_modifiers_wrap_the_values_body() {
        let plan = plan("VALUES (3), (1), (2) ORDER BY 1 LIMIT 1 OFFSET 1");
        let LogicalOperator::Limit(limit) = plan.operator else {
            panic!("expected LIMIT at query boundary");
        };
        let LogicalOperator::Order(order) = limit.child.operator else {
            panic!("expected ORDER BY below LIMIT");
        };
        assert!(matches!(
            order.child.operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn set_operation_query_modifiers_wrap_the_set_result() {
        let plan = plan("SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n LIMIT 1");
        let LogicalOperator::Limit(limit) = plan.operator else {
            panic!("expected LIMIT at query boundary");
        };
        let LogicalOperator::Order(order) = limit.child.operator else {
            panic!("expected ORDER BY below LIMIT");
        };
        assert!(matches!(
            order.child.operator,
            LogicalOperator::SetOperation(_)
        ));
    }

    #[test]
    fn parenthesized_query_modifiers_remain_on_the_inner_set_operand() {
        let plan = plan("(SELECT 2 AS n UNION ALL SELECT 1 ORDER BY n LIMIT 1) UNION ALL SELECT 3");
        let LogicalOperator::SetOperation(set) = plan.operator else {
            panic!("expected outer set operation");
        };
        assert!(matches!(set.left.operator, LogicalOperator::Limit(_)));
    }

    #[test]
    fn parenthesized_select_prunes_hidden_order_columns_before_set_operation() {
        let plan = plan(
            "(SELECT n FROM (VALUES (2, 20), (1, 10)) AS t(n, hidden) \
             ORDER BY hidden LIMIT 1) UNION ALL SELECT 3",
        );
        let LogicalOperator::SetOperation(set) = plan.operator else {
            panic!("expected outer set operation");
        };
        let LogicalOperator::Projection(prune) = set.left.operator else {
            panic!("expected hidden-column pruning at the inner query boundary");
        };
        assert!(matches!(prune.child.operator, LogicalOperator::Limit(_)));
    }

    #[test]
    fn distinct_ordering_is_planned_above_distinct() {
        let plan = plan("SELECT DISTINCT n FROM (VALUES (2), (1), (2)) AS t(n) ORDER BY n ASC");
        let LogicalOperator::Order(order) = plan.operator else {
            panic!("expected ORDER BY at query boundary");
        };
        assert!(matches!(order.child.operator, LogicalOperator::Distinct(_)));
    }

    #[test]
    fn distinct_accepts_qualified_selected_expression() {
        let plan = plan("SELECT DISTINCT n FROM (VALUES (2), (1), (2)) AS t(n) ORDER BY t.n");
        let LogicalOperator::Order(order) = plan.operator else {
            panic!("expected ORDER BY at query boundary");
        };
        assert!(matches!(order.child.operator, LogicalOperator::Distinct(_)));
    }

    #[test]
    fn distinct_rejects_hidden_order_expressions() {
        let statement = paro_parser::parse_one(
            "SELECT DISTINCT n FROM (VALUES (2, 20), (1, 10)) AS t(n, hidden) ORDER BY hidden",
        )
        .expect("parse query")
        .stmt;
        let error = test_binder()
            .bind(statement)
            .expect_err("DISTINCT ORDER BY must use a result column");
        assert!(error
            .to_string()
            .contains("only query result columns can be used"));
    }
}
