// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared read-only traversal and associative-expression helpers.

use paro_planner::expression::{ConjunctionType, Expression, ExpressionIterator};
use paro_planner::operator::JoinSide;

/// Visit an expression and all of its descendants in pre-order.
pub(crate) fn visit_expression(expression: &Expression, visitor: &mut impl FnMut(&Expression)) {
    visitor(expression);
    ExpressionIterator::enumerate_children(expression, |child| {
        visit_expression(child, visitor);
    });
}

/// Return the leaves of an associative AND/OR tree without cloning them.
pub(crate) fn associative_terms(
    expression: &Expression,
    conjunction_type: ConjunctionType,
) -> Vec<&Expression> {
    fn collect<'a>(
        expression: &'a Expression,
        conjunction_type: ConjunctionType,
        output: &mut Vec<&'a Expression>,
    ) {
        if let Expression::Conjunction(conjunction) = expression {
            if conjunction.conjunction_type == conjunction_type {
                for child in &conjunction.children {
                    collect(child, conjunction_type, output);
                }
                return;
            }
        }
        output.push(expression);
    }

    let mut output = Vec::new();
    collect(expression, conjunction_type, &mut output);
    output
}

/// Consume an associative AND/OR tree and return its leaves.
pub(crate) fn into_associative_terms(
    expression: Expression,
    conjunction_type: ConjunctionType,
) -> Vec<Expression> {
    fn collect(
        expression: Expression,
        conjunction_type: ConjunctionType,
        output: &mut Vec<Expression>,
    ) {
        match expression {
            Expression::Conjunction(conjunction)
                if conjunction.conjunction_type == conjunction_type =>
            {
                for child in conjunction.children {
                    collect(child, conjunction_type, output);
                }
            }
            expression => output.push(expression),
        }
    }

    let mut output = Vec::new();
    collect(expression, conjunction_type, &mut output);
    output
}

/// Classify every relevant node in an expression and combine the classifications.
///
/// The callback returns `None` for nodes that do not identify an input. Keeping the leaf policy
/// at the call site lets positional references and bound columns share traversal without assuming
/// that they use the same namespace.
pub(crate) fn expression_join_side(
    expression: &Expression,
    classify: &mut impl FnMut(&Expression) -> Option<JoinSide>,
) -> JoinSide {
    let mut side = JoinSide::None;
    visit_expression(expression, &mut |expression| {
        if let Some(expression_side) = classify(expression) {
            side = JoinSide::combine(side, expression_side);
        }
    });
    side
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ConjunctionExpression, ConstantExpression, ReferenceExpression,
    };

    use super::*;

    fn constant(value: bool) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Boolean(value),
            LogicalType::Boolean,
        ))
    }

    #[test]
    fn associative_terms_flatten_only_the_requested_operator() {
        let nested_or = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![constant(false), constant(true)],
        ));
        let expression = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                constant(true),
                Expression::Conjunction(ConjunctionExpression::new(
                    ConjunctionType::And,
                    vec![nested_or, constant(false)],
                )),
            ],
        ));

        let borrowed = associative_terms(&expression, ConjunctionType::And);
        assert_eq!(borrowed.len(), 3);
        assert!(matches!(borrowed[1], Expression::Conjunction(_)));

        let owned = into_associative_terms(expression, ConjunctionType::And);
        assert_eq!(owned.len(), 3);
        assert!(matches!(owned[1], Expression::Conjunction(_)));
    }

    #[test]
    fn expression_join_side_combines_classified_leaves() {
        let expression = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(2, LogicalType::Integer)),
            ],
        ));

        let side = expression_join_side(&expression, &mut |expression| match expression {
            Expression::Reference(reference) if reference.index == 0 => Some(JoinSide::Left),
            Expression::Reference(_) => Some(JoinSide::Right),
            _ => None,
        });
        assert_eq!(side, JoinSide::Both);
    }
}
