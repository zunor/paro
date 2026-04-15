//! Qualify Binder
//!
//!
//!
//! The QUALIFY binder is responsible for binding an expression within
//! the QUALIFY clause of a SQL statement.

use std::collections::HashSet;

use crate::binder::ir::BoundSelect;
use crate::expression::{CastExpression, Expression};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnRef, Expr};

use super::{AliasLookup, BaseSelectBinder, BoundGroupInformation, SelectBindState};

/// Binder for QUALIFY clause expressions.
///
/// QUALIFY is used to filter window function results, similar to HAVING for aggregates.
/// Example: `SELECT *, ROW_NUMBER() OVER (PARTITION BY dept) AS rn FROM t QUALIFY rn = 1`
///
pub struct QualifyBinder<'a> {
    pub base: BaseSelectBinder<'a>,
    alias_lookup: Option<AliasLookup>,
    visited_select_indexes: HashSet<usize>,
}

impl<'a> QualifyBinder<'a> {
    /// Create a new QualifyBinder.
    pub fn new(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        alias_lookup: AliasLookup,
    ) -> Self {
        Self {
            base: BaseSelectBinder::new(binder, bind_state),
            alias_lookup: Some(alias_lookup),
            visited_select_indexes: HashSet::new(),
        }
    }

    /// Create a new QualifyBinder with node and group info.
    ///
    pub fn with_node_and_info(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        node: &'a mut BoundSelect,
        info: &'a BoundGroupInformation,
        alias_lookup: AliasLookup,
    ) -> Self {
        Self {
            base: BaseSelectBinder::with_node_and_info(binder, bind_state, node, info),
            alias_lookup: Some(alias_lookup),
            visited_select_indexes: HashSet::new(),
        }
    }

    /// Bind the QUALIFY clause.
    ///
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        let bound_expr = self.bind_internal(expr)?;

        // QUALIFY clause must be BOOLEAN
        if bound_expr.return_type() != LogicalType::Boolean {
            return CastExpression::add_cast_if_needed(
                bound_expr,
                LogicalType::Boolean,
                &self.base.base.binder.cast_functions,
            );
        }

        Ok(bound_expr)
    }

    /// Internal binding that handles column references with alias support.
    fn bind_internal(&mut self, expr: Expr) -> Result<Expression> {
        // Check if this is a column reference that might be an alias
        if let Expr::ColumnRef { ref column, .. } = expr {
            // First try to bind as a regular column
            let result = self.base.bind(expr.clone());

            if result.is_ok() {
                return result;
            }

            // Keep the original column reference's string for error message
            let expr_string = column.to_string();

            // Try to bind as an alias using the immutable lookup
            if let Some(alias_lookup) = self.alias_lookup.as_ref() {
                if let Some((index, original_expr)) =
                    alias_lookup.resolve_alias(&column.column.name().to_lowercase())?
                {
                    if !self.visited_select_indexes.contains(&index) {
                        self.base.bind_state.mark_alias_referenced(index);
                        self.visited_select_indexes.insert(index);
                        let result = self.base.bind(original_expr);
                        self.visited_select_indexes.remove(&index);
                        return result;
                    }
                }
            }

            // Neither column nor alias found
            return Err(paro_error::column_not_found(&expr_string));
        }

        // For non-column-ref expressions, use the base binder
        self.base.bind(expr)
    }

    /// Bind a column reference, trying alias resolution if regular binding fails.
    ///
    pub fn bind_column_ref(&mut self, colref: &ColumnRef) -> Result<Expression> {
        // First try to bind as a regular column through BaseSelectBinder
        let expr = Expr::ColumnRef {
            span: None,
            column: colref.clone(),
        };

        let result = self.base.bind(expr.clone());

        if result.is_ok() {
            return result;
        }

        // Keep the original column reference's string for error message
        let expr_string = colref.to_string();

        // Try to bind as an alias using the immutable lookup
        if let Some(alias_lookup) = self.alias_lookup.as_ref() {
            if let Some((index, original_expr)) =
                alias_lookup.resolve_alias(&colref.column.name().to_lowercase())?
            {
                if !self.visited_select_indexes.contains(&index) {
                    self.base.bind_state.mark_alias_referenced(index);
                    self.visited_select_indexes.insert(index);
                    let result = self.base.bind(original_expr);
                    self.visited_select_indexes.remove(&index);
                    return result;
                }
            }
        }

        Err(paro_error::column_not_found(&expr_string))
    }
}
