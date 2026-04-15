// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Base Select Binder
//!
//!
//!
//! The BaseSelectBinder is the base binder for SELECT, HAVING, and QUALIFY binders.
//! It can bind aggregates and window functions.

use std::collections::HashMap;

use crate::binder::bind::expr::ExpressionBinder;
use crate::binder::ir::BoundSelect;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnRef, Expr, FunctionCall};

use super::select_bind_state::SelectBindState;

/// Information about bound GROUP BY expressions.
#[derive(Debug, Clone, Default)]
pub struct BoundGroupInformation {
    pub map: HashMap<String, usize>,
    pub alias_map: HashMap<String, usize>,
    pub collated_groups: HashMap<usize, usize>,
}

impl BoundGroupInformation {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_group(&mut self, expr_str: String, index: usize) {
        self.map.insert(expr_str, index);
    }
    pub fn add_alias(&mut self, alias: &str, index: usize) {
        self.alias_map.insert(alias.to_lowercase(), index);
    }
    pub fn add_collated_group(&mut self, group_index: usize, aggregate_index: usize) {
        self.collated_groups.insert(group_index, aggregate_index);
    }
    pub fn find_group(&self, expr_str: &str) -> Option<usize> {
        self.map.get(expr_str).copied()
    }
    pub fn find_alias(&self, alias: &str) -> Option<usize> {
        self.alias_map.get(&alias.to_lowercase()).copied()
    }
}

/// The BaseSelectBinder is the base binder for SELECT, HAVING, and QUALIFY binders.
pub struct BaseSelectBinder<'a> {
    pub base: ExpressionBinder<'a>,
    pub bind_state: &'a mut SelectBindState,
    pub node: Option<&'a mut BoundSelect>,
    pub info: Option<&'a BoundGroupInformation>,
    pub inside_window: bool,
    pub bound_aggregate: bool,
}

impl<'a> BaseSelectBinder<'a> {
    /// Create a new BaseSelectBinder.
    pub fn new(binder: &'a mut crate::binder::Binder, bind_state: &'a mut SelectBindState) -> Self {
        Self {
            base: ExpressionBinder::new(binder),
            bind_state,
            node: None,
            info: None,
            inside_window: false,
            bound_aggregate: false,
        }
    }

    /// Create a new BaseSelectBinder with node and group info.
    pub fn with_node_and_info(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        node: &'a mut BoundSelect,
        info: &'a BoundGroupInformation,
    ) -> Self {
        Self {
            base: ExpressionBinder::new(binder),
            bind_state,
            node: Some(node),
            info: Some(info),
            inside_window: false,
            bound_aggregate: false,
        }
    }

    /// Main binding entry point.
    ///
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.bind_expression(expr)
    }

    /// Bind an expression, handling grouping and specialized SELECT list types.
    ///
    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        let span = expr.span();
        // check if the expression binds to one of the groups
        if let Some(group_index) = self.try_bind_group(&expr) {
            return self.bind_group(expr, group_index);
        }

        match expr {
            Expr::ColumnRef { column, .. } => self.bind_column_ref(column),
            Expr::FunctionCall { func, .. } => {
                if func.window.is_some() {
                    self.bind_window(func)
                } else {
                    self.base.bind_expression(Expr::FunctionCall { span, func })
                }
            }
            _ => self.base.bind_expression(expr),
        }
    }

    /// Try to bind an expression to a GROUP BY clause.
    ///
    pub fn try_bind_group(&self, expr: &Expr) -> Option<usize> {
        let info = self.info.as_ref()?;

        // first check the group alias map, if expr is a ColumnRef
        if let Expr::ColumnRef { column, .. } = expr {
            if column.schema.is_none() && column.table.is_none() {
                if let Some(index) = info.find_alias(column.column.name()) {
                    return Some(index);
                }
            }
        }

        // no alias reference found
        // check the list of group columns for a match
        let expr_str = expr.to_string();
        info.find_group(&expr_str)
    }

    /// Bind a GROUP BY reference.
    ///
    pub fn bind_group(&mut self, _expr: Expr, group_index: usize) -> Result<Expression> {
        let node = self.node.as_mut().ok_or_else(|| {
            paro_common::error::internal("BaseSelectBinder: BindGroup called without node")
        })?;
        let info = self.info.as_ref().unwrap();

        if let Some(&aggr_index) = info.collated_groups.get(&group_index) {
            // This is an implicitly collated group, so we need to refer to the first() aggregate
            let aggr = &node.aggregates[aggr_index];
            let return_type = aggr.return_type();
            // For now, we simplify and return the column reference
            Ok(Expression::ColumnRef(ColumnRefExpression {
                return_type,
                binding: ColumnBinding::new(node.aggregate_index, aggr_index),
                depth: 0,
            }))
        } else {
            let group = &node.groups.group_expressions[group_index];
            Ok(Expression::ColumnRef(ColumnRefExpression {
                return_type: group.return_type(),
                binding: ColumnBinding::new(node.group_index, group_index),
                depth: 0,
            }))
        }
    }

    /// Bind a column reference.
    ///
    pub fn bind_column_ref(&mut self, column: ColumnRef) -> Result<Expression> {
        // Track the bound column in the base binder
        self.base
            .add_bound_column(column.column.name().to_string(), None);

        let span = match &column.column {
            paro_parser::ast::ColumnID::Name(id) => id.span,
            paro_parser::ast::ColumnID::Position(pos) => pos.span,
        };
        // Use default column binding logic from base ExpressionBinder
        self.base.bind_expression(Expr::ColumnRef { span, column })
    }

    /// Bind a window function.
    ///
    pub fn bind_window(&mut self, func: FunctionCall) -> Result<Expression> {
        if self.inside_window {
            return Err(paro_error::syntax("Window functions cannot be nested"));
        }
        self.inside_window = true;
        let span = func.name.span;
        let result = self.base.bind_expression(Expr::FunctionCall { span, func });
        self.inside_window = false;
        result
    }

    /// Bind a GROUPING() function.
    ///
    pub fn bind_grouping_function(&mut self, func: FunctionCall) -> Result<Expression> {
        if func.args.is_empty() {
            return Err(paro_error::syntax(
                "GROUPING requires at least one argument",
            ));
        }
        if func.args.len() >= 64 {
            return Err(paro_error::syntax(
                "GROUPING statement cannot have more than 64 groups",
            ));
        }

        let mut group_indexes = Vec::with_capacity(func.args.len());
        for mut arg in func.args {
            ExpressionBinder::qualify_column_names(self.base.binder, &mut arg);
            if let Some(idx) = self.try_bind_group(&arg) {
                group_indexes.push(idx);
            } else {
                return Err(paro_error::syntax(format!(
                    "GROUPING child \"{}\" must be a grouping column",
                    arg
                )));
            }
        }

        let node = self.node.as_mut().ok_or_else(|| {
            paro_common::error::internal(
                "BaseSelectBinder: BindGroupingFunction called without node",
            )
        })?;

        if node.groups.group_expressions.is_empty() {
            return Err(paro_error::syntax(
                "GROUPING statement cannot be used without groups",
            ));
        }

        let col_idx = node.grouping_functions.len();
        node.grouping_functions.push(group_indexes);

        Ok(Expression::ColumnRef(ColumnRefExpression {
            return_type: LogicalType::BigInt,
            binding: ColumnBinding::new(node.groupings_index, col_idx),
            depth: 0,
        }))
    }
}
