//! Bound Comparison Expression
//!
//!

use super::Expression;
use paro_common::types::LogicalType;

/// Type of comparison operation.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonType {
    /// `=` or `==`
    Equal,
    /// `<>` or `!=`
    NotEqual,
    /// `<`
    LessThan,
    /// `<=`
    LessThanOrEqual,
    /// `>`
    GreaterThan,
    /// `>=`
    GreaterThanOrEqual,
    /// `IS DISTINCT FROM` - treats NULL as a regular value
    DistinctFrom,
    /// `IS NOT DISTINCT FROM` - NULL equals NULL
    NotDistinctFrom,
}

impl ComparisonType {
    /// Convert to display string for debugging/error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            ComparisonType::Equal => "=",
            ComparisonType::NotEqual => "<>",
            ComparisonType::LessThan => "<",
            ComparisonType::LessThanOrEqual => "<=",
            ComparisonType::GreaterThan => ">",
            ComparisonType::GreaterThanOrEqual => ">=",
            ComparisonType::DistinctFrom => "IS DISTINCT FROM",
            ComparisonType::NotDistinctFrom => "IS NOT DISTINCT FROM",
        }
    }

    /// Returns true if this is an equality comparison.
    pub fn is_equality(&self) -> bool {
        matches!(
            self,
            ComparisonType::Equal
                | ComparisonType::NotEqual
                | ComparisonType::DistinctFrom
                | ComparisonType::NotDistinctFrom
        )
    }
}

impl std::fmt::Display for ComparisonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A bound comparison expression.
///
/// Represents binary comparison operations like `a = b`, `a < b`, etc.
/// The result type is always Boolean.
#[derive(Debug, Clone)]
pub struct ComparisonExpression {
    /// Left operand.
    pub left: Box<Expression>,
    /// Right operand.
    pub right: Box<Expression>,
    /// Type of comparison.
    pub comparison_type: ComparisonType,
}

impl ComparisonExpression {
    /// Create a new comparison expression.
    pub fn new(comparison_type: ComparisonType, left: Expression, right: Expression) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            comparison_type,
        }
    }

    /// Comparison always returns Boolean.
    pub fn return_type(&self) -> LogicalType {
        LogicalType::Boolean
    }
}
