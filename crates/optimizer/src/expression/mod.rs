// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Expression rewriting engine and column binding replacement.

pub mod binding_replacer;
pub mod in_clause;
pub mod rewriter;
pub(crate) mod traversal;

use paro_planner::operator::{ComparisonJoin, Join, LogicalOperator};
use paro_planner::plan::OwnedLogicalPlan;

use crate::rules::arithmetic::ArithmeticSimplificationRule;
use crate::rules::comparison::ComparisonSimplificationRule;
use crate::rules::conjunction::{CommonConjunctionFactorRule, ConjunctionSimplificationRule};
use crate::rules::constant_folding::ConstantFoldingRule;
use crate::rules::move_constants::MoveConstantsRule;

/// The scalar construction boundary of one normalization pipeline. Relational
/// substitutions may replace roots, but cannot mutate a witnessed allocation:
/// copy-on-write detaches it. Thus unchanged roots need no second rule walk.
/// Only completed roots are remembered; child/root rule contexts are never
/// conflated, and this cache neither shares evaluations nor removes fences.
pub(crate) struct CanonicalScalars {
    rewriter: rewriter::ExpressionRewriter,
    completed: std::collections::HashMap<
        paro_planner::expression::ExpressionIdentity,
        paro_planner::expression::ExpressionWitness,
    >,
    next_sweep: usize,
    #[cfg(test)]
    rewrites: usize,
}

impl Default for CanonicalScalars {
    fn default() -> Self {
        Self {
            rewriter: scalar_normalizer(),
            completed: Default::default(),
            next_sweep: 256,
            #[cfg(test)]
            rewrites: 0,
        }
    }
}

impl CanonicalScalars {
    pub(crate) fn normalize_plan(&mut self, plan: &mut OwnedLogicalPlan) -> bool {
        let mut changed = false;
        plan.visit_post_order_mut(|node| {
            changed |= self.normalize_operator(&mut node.operator);
        });
        changed
    }

    pub(crate) fn normalize_operator<Child>(
        &mut self,
        operator: &mut LogicalOperator<Child>,
    ) -> bool {
        if self.completed.len() >= self.next_sweep {
            self.completed.retain(|_, witness| witness.is_alive());
            self.next_sweep = self.completed.len().saturating_mul(2).max(256);
        }
        let mut changed = false;
        paro_planner::visitor::enumerate_expressions(operator, |expression| {
            if self
                .completed
                .get(&expression.allocation_identity())
                .is_some_and(|witness| witness.matches(expression))
            {
                return;
            }
            changed |= self
                .rewriter
                .rewrite_expression(expression, &LogicalOperator::DummyScan);
            #[cfg(test)]
            {
                self.rewrites += 1;
            }
            self.completed
                .insert(expression.allocation_identity(), expression.witness());
        });
        if let LogicalOperator::Aggregate(aggregate) = operator {
            aggregate.recompute_returned_types();
        }
        changed
    }
}

#[cfg(test)]
mod canonical_tests;

pub(crate) fn scalar_normalizer() -> rewriter::ExpressionRewriter {
    let mut rewriter = rewriter::ExpressionRewriter::new();
    rewriter.add_rule(Box::new(ConstantFoldingRule::new()));
    rewriter.add_rule(Box::new(ArithmeticSimplificationRule::new()));
    rewriter.add_rule(Box::new(ComparisonSimplificationRule::new()));
    rewriter.add_rule(Box::new(ConjunctionSimplificationRule::new()));
    rewriter.add_rule(Box::new(CommonConjunctionFactorRule::new()));
    rewriter.add_rule(Box::new(MoveConstantsRule::new()));
    rewriter
}

pub(crate) fn join_has_evaluation_fence<Child>(join: &Join<Child>) -> bool {
    match join {
        Join::Comparison(join) => comparison_join_has_evaluation_fence(join),
        Join::Any(join) => join.condition.evaluation_properties().is_reorder_fence(),
        Join::Cross(_) => false,
    }
}

/// Whether the join/filter region that join-order optimization would extract owns an evaluation
/// fence. Operators that become atomic relations deliberately stop the traversal.
pub(crate) fn join_tree_has_evaluation_fence(join: &Join) -> bool {
    match join {
        Join::Comparison(join) => comparison_join_tree_has_evaluation_fence(join),
        Join::Any(_) | Join::Cross(_) => {
            join_has_evaluation_fence(join)
                || [join.left(), join.right()]
                    .into_iter()
                    .any(|child| join_region_has_evaluation_fence(&child.operator))
        }
    }
}

/// Whether the comparison-join region extracted by join ordering owns an
/// evaluation fence.
///
/// Keeping this entry point on the borrowed concrete join lets specialized
/// eligibility checks share the complete region proof without cloning a
/// logical tree merely to wrap it in [`Join::Comparison`].
pub(crate) fn comparison_join_tree_has_evaluation_fence(join: &ComparisonJoin) -> bool {
    comparison_join_has_evaluation_fence(join)
        || [&join.left, &join.right]
            .into_iter()
            .any(|child| join_region_has_evaluation_fence(&child.operator))
}

pub(crate) fn comparison_join_has_evaluation_fence<Child>(join: &ComparisonJoin<Child>) -> bool {
    join.conditions.iter().any(|condition| {
        condition.left.evaluation_properties().is_reorder_fence()
            || condition.right.evaluation_properties().is_reorder_fence()
    })
}

pub(crate) fn join_region_has_evaluation_fence(operator: &LogicalOperator) -> bool {
    match operator {
        LogicalOperator::Join(join) => join_tree_has_evaluation_fence(join),
        LogicalOperator::Filter(filter) => {
            filter
                .expressions
                .iter()
                .any(|expr| expr.evaluation_properties().is_reorder_fence())
                || join_region_has_evaluation_fence(&filter.child.operator)
        }
        _ => false,
    }
}
