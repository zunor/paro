//! Order Binder
//!
//!
//!
//! The OrderBinder is responsible for binding expressions in the ORDER BY clause.
//! Unlike other binders, it does NOT extend ExpressionBinder. Instead, it returns
//! projection references (as ConstantExpression with index values) that point
//! to entries in the SELECT list.

use crate::binder::bind::expr::ExpressionBinder;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{ColumnRef, Expr, Literal, SelectTarget};

use super::{AliasLookup, SelectBindState};

/// Binder for ORDER BY clause expressions.
///
///
/// The OrderBinder handles:
/// - Integer references (ORDER BY 1, 2, etc.)
/// - Alias references (ORDER BY alias_name)
/// - Positional references
/// - COLLATE expressions
/// - Adding extra expressions to SELECT list when needed
///
/// Unlike other binders, OrderBinder does NOT bind expressions directly.
/// Instead, it returns projection references that point to SELECT list entries.
pub struct OrderBinder<'a> {
    /// The binder used for qualifying column names.
    pub binder: &'a mut crate::binder::Binder,
    /// The SELECT bind state containing alias and projection information.
    pub bind_state: &'a mut SelectBindState,
    alias_lookup: AliasLookup,
    /// The SELECT list that can be extended with extra expressions.
    /// If None, extra expressions cannot be added (e.g., for UNION queries).
    ///
    extra_list: Option<&'a mut Vec<SelectTarget>>,
    /// The query component name for error messages (default: "ORDER BY").
    ///
    query_component: String,
}

/// Result of trying to get a projection reference.
/// Contains the index into the SELECT list.
pub struct ProjectionReference {
    /// The index into the SELECT list (0-based).
    pub index: usize,
    /// Optional collation to apply.
    pub collation: Option<String>,
}

impl<'a> OrderBinder<'a> {
    /// Create a new OrderBinder without the ability to add extra expressions.
    ///
    pub fn new(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        alias_lookup: AliasLookup,
    ) -> Self {
        Self {
            binder,
            bind_state,
            alias_lookup,
            extra_list: None,
            query_component: "ORDER BY".to_string(),
        }
    }

    /// Create a new OrderBinder with the ability to add extra expressions to the SELECT list.
    ///
    pub fn with_extra_list(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        alias_lookup: AliasLookup,
        extra_list: &'a mut Vec<SelectTarget>,
    ) -> Self {
        Self {
            binder,
            bind_state,
            alias_lookup,
            extra_list: Some(extra_list),
            query_component: "ORDER BY".to_string(),
        }
    }

    /// Check if an extra list is available.
    ///
    pub fn has_extra_list(&self) -> bool {
        self.extra_list.is_some()
    }

    /// Set the query component for error messages.
    ///
    pub fn set_query_component(&mut self, component: String) {
        if component.is_empty() {
            self.query_component = "ORDER BY".to_string();
        } else {
            self.query_component = component;
        }
    }

    /// Bind an ORDER BY expression.
    ///
    ///
    /// In the ORDER BY clause we do not bind children - we bind ONLY to the SELECT list.
    /// If there is no matching entry in the SELECT list, we add the expression to the
    /// SELECT list and refer to the new entry.
    ///
    /// Returns a projection reference (index into SELECT list) wrapped as a Expression.
    pub fn bind(&mut self, expr: Expr) -> Result<OrderByBinding> {
        match &expr {
            // ORDER BY constant (e.g., ORDER BY 1)
            Expr::Literal { value, .. } => {
                if let Some(result) = self.try_bind_constant(value)? {
                    return Ok(result);
                }
            }

            // COLUMN REF expression - check if it can be bound to an alias
            Expr::ColumnRef { column, .. } => {
                if let Some(index) = self.try_get_projection_reference_from_colref(column) {
                    return Ok(self.create_projection_reference(&expr, index));
                }
            }

            // Note: paro doesn't have Parameter or Collate expr variants at the AST level
            // COLLATE would be handled through function calls or operators
            _ => {}
        }

        // General case: first qualify column names
        let mut qualified_expr = expr.clone();
        ExpressionBinder::qualify_column_names(self.binder, &mut qualified_expr);

        // Check if the expression already exists in the projection
        let expr_str = format!("{:?}", qualified_expr);
        if let Some(index) = self.bind_state.get_projection_index(&expr_str) {
            return Ok(self.create_projection_reference(&expr, index));
        }

        // Need to add the expression to the SELECT list
        if self.extra_list.is_none() {
            return Err(paro_error::syntax(format!(
                "Could not {} by column \"{}\": add the expression/function to every SELECT, \
                 or move the UNION into a FROM clause.",
                self.query_component,
                expr.to_string_simple()
            )));
        }

        // Add the expression to the SELECT list
        Ok(self.create_extra_reference(qualified_expr))
    }

    /// Try to get a projection reference from a column reference.
    fn try_get_projection_reference_from_colref(&self, colref: &ColumnRef) -> Option<usize> {
        // Check if it's a potential alias (unqualified column)
        if !ExpressionBinder::is_potential_alias(colref) {
            return None;
        }

        // Get the alias name
        self.alias_lookup.get_alias_index(colref.column.name())
    }

    /// Try to get an index from a constant expression.
    fn try_get_index_from_constant(&self, value: &Literal) -> Result<Option<usize>> {
        // Check if it's an integral type
        // paro's Literal only has UInt64 for integers
        let order_value = match value {
            Literal::UInt64(n) => *n as i64,
            // Non-integral constants (Float64, String, Boolean, Null, Decimal256)
            _ => {
                // Non-integral constant - ORDER BY <constant> has no effect
                return Err(paro_error::syntax(format!(
                    "{} non-integer literal has no effect.",
                    self.query_component
                )));
            }
        };

        // Validate the order value
        if order_value <= 0 {
            // Return max index (will be caught by later validation)
            return Ok(Some(usize::MAX));
        }

        Ok(Some((order_value - 1) as usize))
    }

    /// Try to bind a constant expression.
    ///
    fn try_bind_constant(&self, value: &Literal) -> Result<Option<OrderByBinding>> {
        let index = match self.try_get_index_from_constant(value)? {
            Some(idx) => idx,
            None => return Ok(None),
        };

        Ok(Some(OrderByBinding {
            index,
            collation: None,
            alias: None,
        }))
    }

    /// Create a projection reference.
    ///
    fn create_projection_reference(&self, expr: &Expr, index: usize) -> OrderByBinding {
        let alias = if let Some(ref extra) = self.extra_list {
            if index < extra.len() {
                extra[index].get_alias()
            } else {
                expr.get_alias()
            }
        } else {
            expr.get_alias()
        };

        OrderByBinding {
            index,
            collation: None,
            alias,
        }
    }

    /// Create an extra reference by adding the expression to the SELECT list.
    ///
    fn create_extra_reference(&mut self, expr: Expr) -> OrderByBinding {
        let extra_list = self.extra_list.as_mut().unwrap();

        // Record in projection map
        let expr_str = format!("{:?}", expr);
        let index = extra_list.len();
        self.bind_state.add_projection(expr_str, index);

        // Get alias before moving expr
        let alias = expr.get_alias();

        // Add to SELECT list
        extra_list.push(SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias: None,
        });

        OrderByBinding {
            index,
            collation: None,
            alias,
        }
    }
}

/// Represents an ORDER BY binding result.
///
/// Instead of returning a Expression, the OrderBinder returns
/// an OrderByBinding which contains the index into the SELECT list
/// and optional collation information.
#[derive(Debug, Clone)]
pub struct OrderByBinding {
    /// The index into the SELECT list (0-based).
    pub index: usize,
    /// Optional collation to apply.
    pub collation: Option<String>,
    /// Optional alias.
    pub alias: Option<String>,
}

impl OrderByBinding {
    /// Create a new OrderByBinding.
    pub fn new(index: usize) -> Self {
        Self {
            index,
            collation: None,
            alias: None,
        }
    }

    /// Create a new OrderByBinding with collation.
    pub fn with_collation(index: usize, collation: String) -> Self {
        Self {
            index,
            collation: Some(collation),
            alias: None,
        }
    }

    /// Convert this OrderByBinding to a Expression (BoundColumnRef).
    ///
    /// This creates a column reference expression that points to the projection
    /// at the given index. The projection_table_index is the table index for
    /// the projection operator.
    pub fn to_bound_expression(
        &self,
        projection_table_index: usize,
        select_list_types: &[paro_common::types::LogicalType],
    ) -> crate::expression::Expression {
        let return_type = if self.index < select_list_types.len() {
            select_list_types[self.index].clone()
        } else {
            paro_common::types::LogicalType::Unknown
        };
        self.to_bound_expression_with_type(projection_table_index, return_type)
    }

    /// Convert this OrderByBinding to a Expression with a specific return type.
    ///
    /// This is useful when the type is already known or when the expression
    /// was added to the SELECT list and its type needs to be determined separately.
    pub fn to_bound_expression_with_type(
        &self,
        projection_table_index: usize,
        return_type: paro_common::types::LogicalType,
    ) -> crate::expression::Expression {
        use crate::expression::ColumnRefExpression;
        use crate::operator::ColumnBinding;

        crate::expression::Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(projection_table_index, self.index),
            return_type,
        ))
    }
}

/// Helper trait to get alias from expressions.
trait ExprAlias {
    fn get_alias(&self) -> Option<String>;
    fn to_string_simple(&self) -> String;
}

impl ExprAlias for Expr {
    fn get_alias(&self) -> Option<String> {
        // Expressions typically don't carry aliases directly in paro's AST
        // The alias is on the SelectTarget
        None
    }

    fn to_string_simple(&self) -> String {
        match self {
            Expr::ColumnRef { column, .. } => {
                if let Some(ref table) = column.table {
                    format!("{}.{}", table.name, column.column.name())
                } else {
                    column.column.name().to_string()
                }
            }
            Expr::Literal { value, .. } => format!("{:?}", value),
            _ => format!("{:?}", self),
        }
    }
}

/// Helper trait to get alias from SelectTarget.
trait SelectTargetAlias {
    fn get_alias(&self) -> Option<String>;
}

impl SelectTargetAlias for SelectTarget {
    fn get_alias(&self) -> Option<String> {
        match self {
            SelectTarget::AliasedExpr { alias, .. } => alias.as_ref().map(|a| a.name.clone()),
            SelectTarget::StarColumns { .. } => None,
        }
    }
}
