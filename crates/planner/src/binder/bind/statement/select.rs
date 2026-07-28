// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bind Select Statement
//!
//!

use crate::binder::ir::{
    BoundQuery, BoundSetOperation, BoundStatementKind, SetOperationType, WithCTE,
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
            SetExpr::Select(select) => {
                let node = self.bind_select_stmt(*select, order_by, limit, offset)?;
                Ok(BoundQuery::Select(Box::new(node)))
            }
            SetExpr::Values { values, .. } => {
                let node = self.bind_values_rows(values)?;
                // TODO: handle limit/order_by for VALUES
                Ok(BoundQuery::Values(node))
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
                self.bind_set_operation(set_op.op, set_op.all, left_node, right_node)
                // TODO: handle limit/order_by for the result of set operation
            }
            _ => Err(paro_error::not_implemented("Query body type not supported")),
        }
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
