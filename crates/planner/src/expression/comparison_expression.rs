// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

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
    /// Exchange operands while preserving the comparison's truth value,
    /// including UNKNOWN and both NULL-safe comparison operators.
    pub const fn flipped(self) -> Self {
        match self {
            Self::LessThan => Self::GreaterThan,
            Self::LessThanOrEqual => Self::GreaterThanOrEqual,
            Self::GreaterThan => Self::LessThan,
            Self::GreaterThanOrEqual => Self::LessThanOrEqual,
            other => other,
        }
    }

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
        let comparison = Self {
            left: Box::new(left),
            right: Box::new(right),
            comparison_type,
        };
        debug_assert!(
            comparison.has_bound_input_contract(),
            "bound comparison operands require one explicit normalized type: left={}, right={}",
            comparison.left.return_type(),
            comparison.right.return_type(),
        );
        comparison
    }

    /// Whether both operands satisfy the executor's bound-input contract.
    ///
    /// Implicit coercion ends at the binder. A physical comparison therefore
    /// receives two operands of one concrete normalized type; any required
    /// cast is represented explicitly in either child expression.
    pub fn has_bound_input_contract(&self) -> bool {
        Self::operands_have_bound_input_contract(self.left.as_ref(), self.right.as_ref())
    }

    pub fn operands_have_bound_input_contract(left: &Expression, right: &Expression) -> bool {
        let left_type = left.return_type();
        let right_type = right.return_type();
        left_type == right_type
            && left_type == left_type.normalize_type()
            && left_type != LogicalType::Unknown
    }

    /// Comparison always returns Boolean.
    pub fn return_type(&self) -> LogicalType {
        LogicalType::Boolean
    }
}
