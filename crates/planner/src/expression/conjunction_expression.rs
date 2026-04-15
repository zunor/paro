//! Bound Conjunction Expression
//!
//!

use super::Expression;
use paro_common::types::LogicalType;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConjunctionType {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct ConjunctionExpression {
    pub conjunction_type: ConjunctionType,
    pub children: Vec<Expression>,
}

impl ConjunctionExpression {
    pub fn new(conjunction_type: ConjunctionType, children: Vec<Expression>) -> Self {
        Self {
            conjunction_type,
            children,
        }
    }

    pub fn return_type(&self) -> LogicalType {
        LogicalType::Boolean
    }
}
