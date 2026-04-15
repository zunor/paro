//! Replace column bindings inside expressions and operators.

use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{ColumnBinding, LogicalOperator};
use paro_planner::visitor::LogicalOperatorVisitor;

#[derive(Debug, Clone)]
pub struct ReplacementBinding {
    pub old_binding: ColumnBinding,
    pub new_binding: ColumnBinding,
    pub replace_type: bool,
    pub new_type: Option<LogicalType>,
}

impl ReplacementBinding {
    pub fn new(old_binding: ColumnBinding, new_binding: ColumnBinding) -> Self {
        Self {
            old_binding,
            new_binding,
            replace_type: false,
            new_type: None,
        }
    }

    pub fn with_type(
        old_binding: ColumnBinding,
        new_binding: ColumnBinding,
        new_type: LogicalType,
    ) -> Self {
        Self {
            old_binding,
            new_binding,
            replace_type: true,
            new_type: Some(new_type),
        }
    }
}

/// Rewrite column bindings inside expressions and operators.
pub struct ColumnBindingReplacer {
    pub replacement_bindings: Vec<ReplacementBinding>,
    pub stop_operator: Option<*const LogicalOperator>,
}

impl ColumnBindingReplacer {
    pub fn new() -> Self {
        Self {
            replacement_bindings: Vec::new(),
            stop_operator: None,
        }
    }
}

impl Default for ColumnBindingReplacer {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOperatorVisitor for ColumnBindingReplacer {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        // Check if we should stop at this operator
        if let Some(stop_ptr) = self.stop_operator {
            if std::ptr::eq(op as *const _, stop_ptr) {
                return;
            }
        }
        self.visit_operator_children(op);
        self.visit_operator_expressions(op);
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        for replacement in &self.replacement_bindings {
            if expr.binding == replacement.old_binding {
                expr.binding = replacement.new_binding;
                if replacement.replace_type {
                    if let Some(ref new_type) = replacement.new_type {
                        expr.return_type = new_type.clone();
                    }
                }
                break;
            }
        }
        None // Don't replace the entire expression
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::Expression;

    #[test]
    fn test_replace_binding() {
        let old_binding = ColumnBinding::new(0, 2);
        let new_binding = ColumnBinding::new(0, 1);

        let mut expr =
            Expression::ColumnRef(ColumnRefExpression::new(old_binding, LogicalType::Integer));

        let mut replacer = ColumnBindingReplacer::new();
        replacer
            .replacement_bindings
            .push(ReplacementBinding::new(old_binding, new_binding));

        replacer.visit_expression(&mut expr);

        if let Expression::ColumnRef(col_ref) = expr {
            assert_eq!(col_ref.binding, new_binding);
        } else {
            panic!("Expected ColumnRef");
        }
    }

    #[test]
    fn test_replace_binding_with_type() {
        let old_binding = ColumnBinding::new(0, 0);
        let new_binding = ColumnBinding::new(0, 1);
        let new_type = LogicalType::Varchar;

        let mut expr =
            Expression::ColumnRef(ColumnRefExpression::new(old_binding, LogicalType::Integer));

        let mut replacer = ColumnBindingReplacer::new();
        replacer
            .replacement_bindings
            .push(ReplacementBinding::with_type(
                old_binding,
                new_binding,
                new_type.clone(),
            ));

        replacer.visit_expression(&mut expr);

        if let Expression::ColumnRef(col_ref) = expr {
            assert_eq!(col_ref.binding, new_binding);
            assert_eq!(col_ref.return_type, new_type);
        } else {
            panic!("Expected ColumnRef");
        }
    }
}
