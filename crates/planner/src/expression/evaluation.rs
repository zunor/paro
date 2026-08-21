// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Evaluation properties used to guard semantics-changing rewrites.

use paro_function::scalar::{FunctionErrorMode, FunctionSideEffects, FunctionStability};

use super::{Expression, ExpressionIterator, WindowExpression};

/// Properties that determine whether an expression may be moved or evaluated once for several
/// structurally equal uses.
///
/// Structural equality is deliberately separate from evaluation equivalence. Two calls to a
/// volatile routine can have identical trees while still requiring independent evaluations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationProperties {
    stability: FunctionStability,
    side_effects: FunctionSideEffects,
    crosses_execution_boundary: bool,
    contains_subquery: bool,
    can_error: bool,
}

impl Default for EvaluationProperties {
    fn default() -> Self {
        Self {
            stability: FunctionStability::Consistent,
            side_effects: FunctionSideEffects::NoSideEffects,
            crosses_execution_boundary: false,
            contains_subquery: false,
            can_error: false,
        }
    }
}

impl EvaluationProperties {
    /// Whether structurally equal uses may share one evaluation.
    ///
    /// A stable routine may be shared within a query. An external execution boundary is
    /// orthogonal to volatility, so immutable external calls remain shareable after they are
    /// lowered to the external runtime.
    pub fn can_share_evaluation(self) -> bool {
        self.stability != FunctionStability::Volatile
            && self.side_effects == FunctionSideEffects::NoSideEffects
            && !self.contains_subquery
    }

    /// Whether moving other expressions across this one can change observable evaluation order.
    pub fn is_reorder_fence(self) -> bool {
        !self.can_share_evaluation() || self.crosses_execution_boundary
    }

    /// Whether the expression is total over every row admitted by its input
    /// types. Moving a total expression across a row-removing operator cannot
    /// expose a new SQL error on a row that the original plan never evaluated.
    pub fn is_infallible(self) -> bool {
        !self.can_error
    }

    fn merge(&mut self, other: Self) {
        self.stability = merge_stability(self.stability, other.stability);
        if other.side_effects == FunctionSideEffects::HasSideEffects {
            self.side_effects = FunctionSideEffects::HasSideEffects;
        }
        self.crosses_execution_boundary |= other.crosses_execution_boundary;
        self.contains_subquery |= other.contains_subquery;
        self.can_error |= other.can_error;
    }
}

impl Expression {
    /// Whether reading this expression has no computation of its own to elide.
    ///
    /// Composite expressions deliberately return false even when immutable: functions and casts
    /// may fail, and future expression kinds should not become removable by default.
    pub fn is_passive_value(&self) -> bool {
        matches!(
            self,
            Expression::Constant(_)
                | Expression::ColumnRef(_)
                | Expression::Parameter(_)
                | Expression::Reference(_)
        )
    }

    /// Compute the evaluation contract for this expression tree.
    pub fn evaluation_properties(&self) -> EvaluationProperties {
        let mut properties = match self {
            Expression::Function(function) => EvaluationProperties {
                stability: function.function.stability,
                side_effects: function.function.side_effects,
                crosses_execution_boundary: function.crosses_execution_boundary(),
                contains_subquery: false,
                can_error: function.function.error_mode == FunctionErrorMode::CanError,
            },
            // A subquery owns a plan rather than an ordinary expression child. Until plan-level
            // properties are available, treating it as non-shareable prevents accidental cloning
            // or elimination of work hidden behind that boundary.
            Expression::Subquery(_) => EvaluationProperties {
                contains_subquery: true,
                can_error: true,
                ..EvaluationProperties::default()
            },
            // Casts and the remaining composite expression kinds do not yet
            // carry a bound totality contract. Keep them conservative rather
            // than inferring safety from their children.
            Expression::Cast(_)
            | Expression::Conjunction(_)
            | Expression::Case(_)
            | Expression::Comparison(_)
            | Expression::Operator(_)
            | Expression::Aggregate(_)
            | Expression::Window(_) => EvaluationProperties {
                can_error: true,
                ..EvaluationProperties::default()
            },
            _ => EvaluationProperties::default(),
        };

        ExpressionIterator::enumerate_children(self, |child| {
            properties.merge(child.evaluation_properties());
        });
        properties
    }
}

impl WindowExpression {
    /// Compute evaluation properties for the arguments and clauses owned by a window expression.
    pub fn evaluation_properties(&self) -> EvaluationProperties {
        let mut properties = EvaluationProperties::default();
        ExpressionIterator::enumerate_window_children(self, |child| {
            properties.merge(child.evaluation_properties());
        });
        properties
    }
}

fn merge_stability(left: FunctionStability, right: FunctionStability) -> FunctionStability {
    use FunctionStability::{Consistent, ConsistentWithinQuery, Volatile};
    match (left, right) {
        (Volatile, _) | (_, Volatile) => Volatile,
        (ConsistentWithinQuery, _) | (_, ConsistentWithinQuery) => ConsistentWithinQuery,
        (Consistent, Consistent) => Consistent,
    }
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_external::routine::boundary::PlacementClass;
    use paro_function::scalar::{ExpressionState, ScalarFunction};

    use super::*;
    use crate::expression::FunctionExpression;

    fn noop(_input: &Chunk, _state: &dyn ExpressionState, _result: &mut Vector) -> Result<()> {
        Ok(())
    }

    fn call(
        stability: FunctionStability,
        side_effects: FunctionSideEffects,
        external: bool,
        children: Vec<Expression>,
    ) -> Expression {
        let function = ScalarFunction::new("test".to_string(), vec![], LogicalType::Integer, noop)
            .with_stability(stability)
            .with_side_effects(side_effects);
        let mut expression = FunctionExpression::new(function, children, LogicalType::Integer);
        if external {
            expression
                .routine_meta
                .as_mut()
                .expect("builtin routine metadata")
                .boundary
                .placement = PlacementClass::External;
        }
        Expression::Function(expression)
    }

    fn infallible_call(children: Vec<Expression>) -> Expression {
        let mut expression = call(
            FunctionStability::Consistent,
            FunctionSideEffects::NoSideEffects,
            false,
            children,
        );
        let Expression::Function(function) = &mut expression else {
            unreachable!("call constructs a function expression");
        };
        function.function.error_mode = FunctionErrorMode::Infallible;
        expression
    }

    #[test]
    fn volatile_descendant_prevents_shared_evaluation() {
        let volatile = call(
            FunctionStability::Volatile,
            FunctionSideEffects::NoSideEffects,
            false,
            vec![],
        );
        let parent = call(
            FunctionStability::Consistent,
            FunctionSideEffects::NoSideEffects,
            false,
            vec![volatile],
        );

        let properties = parent.evaluation_properties();
        assert!(!properties.can_share_evaluation());
        assert!(properties.is_reorder_fence());
    }

    #[test]
    fn side_effects_prevent_shared_evaluation() {
        let expression = call(
            FunctionStability::Consistent,
            FunctionSideEffects::HasSideEffects,
            false,
            vec![],
        );

        assert!(!expression.evaluation_properties().can_share_evaluation());
    }

    #[test]
    fn external_immutable_call_is_shareable_but_fences_reordering() {
        let expression = call(
            FunctionStability::Consistent,
            FunctionSideEffects::NoSideEffects,
            true,
            vec![],
        );

        let properties = expression.evaluation_properties();
        assert!(properties.can_share_evaluation());
        assert!(properties.is_reorder_fence());
    }

    #[test]
    fn totality_requires_every_function_in_the_tree_to_be_infallible() {
        let leaf = infallible_call(vec![]);
        assert!(leaf.evaluation_properties().is_infallible());

        let total = infallible_call(vec![leaf]);
        assert!(total.evaluation_properties().is_infallible());

        let fallible_leaf = call(
            FunctionStability::Consistent,
            FunctionSideEffects::NoSideEffects,
            false,
            vec![],
        );
        let mixed = infallible_call(vec![fallible_leaf]);
        assert!(!mixed.evaluation_properties().is_infallible());
    }
}
