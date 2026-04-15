// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Empty Result Operator
//!
//! Represents an operator that preserves the schema of its child but produces
//! zero rows.

use paro_common::types::LogicalType;

use super::ColumnBinding;
use crate::plan::LogicalPlan;

/// EmptyResult wraps an existing operator shape while forcing the
/// cardinality to zero.
#[derive(Debug)]
pub struct EmptyResult {
    /// Child operator whose schema/bindings are preserved.
    pub child: Box<LogicalPlan>,
}

impl EmptyResult {
    pub fn new(child: LogicalPlan) -> Self {
        Self {
            child: Box::new(child),
        }
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.child.types()
    }

    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.child.get_column_bindings()
    }

    pub fn name(&self) -> &'static str {
        "EMPTY_RESULT"
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::context::BindContext;
    use crate::operator::{ExpressionGet, LogicalOperator};
    use crate::plan::LogicalPlan;

    use super::EmptyResult;
    use paro_common::types::LogicalType;

    #[test]
    fn empty_result_preserves_child_schema() {
        let ctx = BindContext::new();
        let child_op = LogicalOperator::ExpressionGet(ExpressionGet::new(
            42,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ));
        let expected_bindings = child_op.get_column_bindings();
        let child = LogicalPlan::new(&ctx, child_op);
        let empty = EmptyResult::new(child);

        assert_eq!(empty.get_types(), vec![LogicalType::Integer]);
        assert_eq!(empty.get_column_bindings(), expected_bindings);
        assert_eq!(empty.name(), "EMPTY_RESULT");
    }
}
