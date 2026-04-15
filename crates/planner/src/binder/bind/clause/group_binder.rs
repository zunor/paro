// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Group Binder
//!
//!
//!
//! The GroupBinder is responsible for binding expressions in the GROUP BY clause.

use std::collections::{HashMap, HashSet};

use crate::binder::bind::expr::ExpressionBinder;
use crate::expression::Expression;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_parser::ast::{ColumnRef, Expr, Literal, SelectTarget};

use super::SelectBindState;

/// Binder for GROUP BY clause expressions.
///
///
/// The GroupBinder handles:
/// - Integer references (GROUP BY 1, 2, etc.)
/// - Column references (first from tables, then from aliases)
/// - Prevents aggregate functions in GROUP BY
/// - Prevents window functions in GROUP BY
/// - Prevents DEFAULT clause in GROUP BY
/// - Prevents parameters (?) in GROUP BY
/// - Tracks used aliases to prevent duplicate groupings
pub struct GroupBinder<'a> {
    pub base: ExpressionBinder<'a>,
    /// The SELECT list from the query node.
    pub select_list: &'a [SelectTarget],
    /// The SELECT bind state containing alias information.
    pub bind_state: &'a SelectBindState,
    /// Map from alias/index string to GROUP BY index.
    /// This is populated as we bind GROUP BY expressions.
    ///
    pub group_alias_map: HashMap<String, usize>,
    /// Set of SELECT list indices that have been used in GROUP BY.
    /// Used to detect duplicate groupings (e.g., GROUP BY 1, 1).
    ///
    used_aliases: HashSet<usize>,
    /// The current bind index (position in the GROUP BY list).
    ///
    pub bind_index: usize,
    /// The unbound root expression (before binding).
    /// Set when binding a SELECT reference.
    ///
    pub unbound_expression: Option<Expr>,
}

impl<'a> GroupBinder<'a> {
    /// Create a new GroupBinder.
    ///
    pub fn new(
        binder: &'a mut crate::binder::Binder,
        select_list: &'a [SelectTarget],
        bind_state: &'a SelectBindState,
    ) -> Self {
        Self::with_group_index(binder, select_list, bind_state, 0)
    }

    /// Create a new GroupBinder with a specific group index.
    ///
    pub fn with_group_index(
        binder: &'a mut crate::binder::Binder,
        select_list: &'a [SelectTarget],
        bind_state: &'a SelectBindState,
        _group_index: usize,
    ) -> Self {
        let mut base = ExpressionBinder::new(binder);
        // GROUP BY clause does not allow aggregates
        base.allow_aggregates = false;
        // GROUP BY clause does not allow window functions
        base.allow_window = false;
        // GROUP BY clause does not allow DEFAULT clause
        base.allow_default = false;

        Self {
            base,
            select_list,
            bind_state,
            group_alias_map: HashMap::new(),
            used_aliases: HashSet::new(),
            bind_index: 0,
            unbound_expression: None,
        }
    }

    /// Set the current bind index.
    pub fn set_bind_index(&mut self, index: usize) {
        self.bind_index = index;
    }

    /// Bind a GROUP BY expression.
    ///
    ///
    /// This method handles the special cases for root expressions at depth 0:
    /// - COLUMN_REF: Try to bind as column first, then as alias
    /// - CONSTANT: Handle GROUP BY 1, 2, etc.
    /// - PARAMETER: Throw error (not allowed)
    /// - WINDOW: Throw error (not allowed)
    /// - DEFAULT: Throw error (not allowed)
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.bind_expression(expr, 0, true)
    }

    /// Bind an expression at a specific depth.
    ///
    fn bind_expression(
        &mut self,
        expr: Expr,
        depth: usize,
        root_expression: bool,
    ) -> Result<Expression> {
        // Special handling for root expressions at depth 0
        if root_expression && depth == 0 {
            match &expr {
                Expr::ColumnRef { column, .. } => {
                    return self.bind_column_ref(column.clone(), expr);
                }
                Expr::Literal { value, .. } => {
                    if let Some(result) = self.try_bind_constant(value)? {
                        return Ok(result);
                    }
                    // Non-integral constants fall through to regular binding
                }
                // Note: paro doesn't have Parameter or Default expr variants at the AST level
                // These would be caught by the parser or handled differently
                _ => {}
            }
        }

        // Check for unsupported expressions (window functions)
        match &expr {
            Expr::FunctionCall { func, .. } if func.window.is_some() => {
                return Err(paro_error::syntax(
                    "GROUP BY clause cannot contain window functions",
                ));
            }
            Expr::CountAll {
                window: Some(_), ..
            } => {
                return Err(paro_error::syntax(
                    "GROUP BY clause cannot contain window functions",
                ));
            }
            _ => {}
        }

        // Default: use base expression binder
        self.base.bind(expr)
    }

    /// Returns the error message for unsupported aggregates.
    ///
    pub fn unsupported_aggregate_message() -> &'static str {
        "GROUP BY clause cannot contain aggregates!"
    }

    /// Bind a SELECT list reference by index.
    ///
    ///
    /// This method:
    /// 1. Checks if the alias has already been used (duplicate grouping)
    /// 2. Validates the index is within range
    /// 3. Stores the unbound expression
    /// 4. Binds the SELECT list expression
    /// 5. Records the alias mapping
    /// 6. Replaces the original SELECT list entry with a column reference
    fn bind_select_ref(&mut self, entry: usize) -> Result<Expression> {
        // Check if the alias has already been bound
        // the grouping with a constant since the second grouping has no effect"
        if self.used_aliases.contains(&entry) {
            // Return a constant (42) - the value doesn't matter as it will be optimized out
            return Ok(Expression::Constant(
                crate::expression::ConstantExpression::new(
                    Value::Integer(42),
                    paro_common::types::LogicalType::Integer,
                ),
            ));
        }

        // Validate index range
        if entry >= self.select_list.len() {
            return Err(paro_error::syntax(format!(
                "GROUP BY term out of range - should be between 1 and {}",
                self.select_list.len()
            )));
        }

        // Get the SELECT list target
        let target = &self.select_list[entry];
        let select_expr: Expr = match target {
            SelectTarget::AliasedExpr { expr, .. } => (*expr.clone()).into(),
            SelectTarget::StarColumns { .. } => {
                return Err(paro_error::syntax(
                    "GROUP BY cannot reference a star (*) expression",
                ));
            }
        };

        // Store the unbound expression
        self.unbound_expression = Some(select_expr.clone());

        // Bind the expression
        let bound = self.base.bind(select_expr)?;

        // Record the alias mapping (using index as string key)
        self.group_alias_map
            .insert(entry.to_string(), self.bind_index);

        // Insert into used aliases set
        self.used_aliases.insert(entry);

        Ok(bound)
    }

    /// Bind a constant expression (GROUP BY 1, 2, etc.).
    ///
    ///
    /// Returns:
    /// - Some(bound_expr) if the constant is an integral SELECT list reference
    /// - None if the constant should be bound normally (non-integral)
    fn try_bind_constant(&mut self, value: &Literal) -> Result<Option<Expression>> {
        // Check if it's an integral type
        // paro's Literal only has UInt64 for integers
        let index = match value {
            Literal::UInt64(n) => *n as i64,
            // Non-integral constants (Float64, String, Boolean, Null, Decimal256)
            // should be bound normally
            _ => {
                return Ok(None);
            }
        };

        // Validate index
        if index <= 0 {
            return Err(paro_error::syntax(
                "GROUP BY position must be a positive integer",
            ));
        }

        // Convert to 0-based index
        let entry = (index - 1) as usize;
        Ok(Some(self.bind_select_ref(entry)?))
    }

    /// Try to resolve an alias reference in the GROUP BY clause.
    ///
    ///
    /// This method:
    /// 1. Looks up the alias in the bind_state alias_map
    /// 2. If found and not root expression, returns an error
    /// 3. If found and root expression, binds the SELECT reference
    /// 4. Records the alias mapping
    fn try_resolve_alias_reference(
        &mut self,
        colref: &ColumnRef,
        root_expression: bool,
    ) -> Option<Result<Expression>> {
        // Get the alias name (case-insensitive)
        let alias_name = colref.column.name().to_lowercase();

        // Try to find in alias_map
        let entry = self.bind_state.alias_map.get(&alias_name)?;
        let entry = *entry;
        if !root_expression {
            return Some(Err(paro_error::syntax(format!(
                "Alias with name \"{}\" exists, but aliases cannot be used as part of an expression in the GROUP BY",
                alias_name
            ))));
        }

        // Bind the SELECT reference
        let result = self.bind_select_ref(entry);
        if result.is_ok() {
            // Record the alias mapping
            self.group_alias_map.insert(alias_name, self.bind_index);
        }
        Some(result)
    }

    /// Bind a column reference expression.
    ///
    ///
    /// Columns in GROUP BY clauses:
    /// 1. FIRST refer to the original tables
    /// 2. THEN if no match is found, refer to aliases in the SELECT list
    /// 3. THEN if no match is found, refer to outer queries
    fn bind_column_ref(&mut self, colref: ColumnRef, original_expr: Expr) -> Result<Expression> {
        // First try to bind to base columns (original tables)
        match self.base.bind_expression(original_expr.clone()) {
            Ok(bound) => Ok(bound),
            Err(_) => {
                // If binding failed, check if it's a potential alias
                if ExpressionBinder::is_potential_alias(&colref) {
                    if let Some(result) = self.try_resolve_alias_reference(&colref, true) {
                        return result;
                    }
                }
                // If alias resolution also failed, propagate the original binding error
                self.base.bind_expression(original_expr)
            }
        }
    }

    /// Get the group alias map.
    pub fn get_group_alias_map(&self) -> &HashMap<String, usize> {
        &self.group_alias_map
    }

    /// Check if an entry has been used as a grouping.
    pub fn is_entry_used(&self, entry: usize) -> bool {
        self.used_aliases.contains(&entry)
    }
}
