//! Bound Case Expression
//!
//!

use super::Expression;
use paro_common::types::LogicalType;

#[derive(Debug, Clone)]
pub struct CaseExpression {
    pub check: Box<Expression>,
    pub result_if_true: Box<Expression>,
    pub result_if_false: Box<Expression>,
    pub return_type: LogicalType,
}

impl CaseExpression {
    pub fn new(
        check: Expression,
        result_if_true: Expression,
        result_if_false: Expression,
        return_type: LogicalType,
    ) -> Self {
        Self {
            check: Box::new(check),
            result_if_true: Box::new(result_if_true),
            result_if_false: Box::new(result_if_false),
            return_type,
        }
    }

    pub fn return_type(&self) -> LogicalType {
        self.return_type.clone()
    }
}
