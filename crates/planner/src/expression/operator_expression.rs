//! Bound Operator Expression
//!
//!

use super::Expression;
use paro_common::types::LogicalType;

/// Type of operator operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    /// `LHS IN (RHS1, RHS2,...)`
    In,
    /// `LHS NOT IN (RHS1, RHS2,...)`
    NotIn,
    /// `NOT expr`
    Not,
    /// `expr IS NULL`
    IsNull,
    /// `expr IS NOT NULL`
    IsNotNull,
    /// `COALESCE(child1, child2,...)`
    Coalesce,
    /// `expr LIKE pattern`
    Like,
    /// `expr ILIKE pattern` (case-insensitive LIKE)
    ILike,
    /// `[expr1, expr2,...]`
    ArrayConstructor,
    /// `(expr1, expr2,...)`
    StructConstructor,
    /// `expr[index]`
    ArrayExtract,
    /// Internal scalar-subquery contract: return child[0], but error if child[1] > 1.
    ErrorIfMultipleRows,
}

/// A bound operator expression.
#[derive(Debug, Clone)]
pub struct OperatorExpression {
    /// Type of operator.
    pub operator_type: OperatorType,
    /// Children expressions.
    pub children: Vec<Expression>,
    /// Resulting logical type.
    pub return_type: LogicalType,
}

impl OperatorExpression {
    /// Create a new operator expression with 1 child.
    pub fn new_unary(
        operator_type: OperatorType,
        child: Expression,
        return_type: LogicalType,
    ) -> Self {
        Self {
            operator_type,
            children: vec![child],
            return_type,
        }
    }

    /// Create a new operator expression with multiple children.
    pub fn new(
        operator_type: OperatorType,
        children: Vec<Expression>,
        return_type: LogicalType,
    ) -> Self {
        Self {
            operator_type,
            children,
            return_type,
        }
    }
}
