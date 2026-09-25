// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction-time normalization of total, schema-preserving restrictions.
//!
//! This is not a rule alternative or an estimator. Both initial construction
//! and transactional publication use this contract before interning a shell.
//! Estimates remain owned by the output relation, not by the number of filters.

use paro_planner::{
    expression::Expression, logical::operator::LogicalOperator, logical::plan::OwnedLogicalPlan,
};

fn transparent<Child>(filter: &paro_planner::logical::operator::Filter<Child>) -> bool {
    filter.projection_map.is_all()
        && filter.expressions.iter().all(|predicate| {
            let properties = predicate.evaluation_properties();
            !properties.is_reorder_fence() && properties.is_infallible()
        })
}

fn compose(outer: &[Expression], inner: &[Expression]) -> Option<Vec<Expression>> {
    crate::rewrite::predicate::pushdown::FilterPushdown::normalize_predicates(
        inner.iter().chain(outer).cloned(),
    )
}

/// Normalize the SQL pipeline before initial post-order Memo construction.
/// Transactional native publication uses the same transparency/composition
/// contract below; generic Memo fixtures need not be SQL-normalized trees.
#[cfg(test)]
pub(crate) fn normalize_tree(
    plan: OwnedLogicalPlan,
) -> paro_common::error::Result<OwnedLogicalPlan> {
    plan.try_map_post_order(|plan| Ok(normalize_node(plan)))
}

/// Local construction law shared by the SQL construction owner and the
/// standalone oracle. Children are canonical; only this boundary is repaired.
pub(crate) fn normalize_node(mut plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
    while let LogicalOperator::Filter(outer) = &mut plan.operator {
        let LogicalOperator::Filter(inner) = &outer.child.operator else {
            break;
        };
        if !transparent(outer) || !transparent(inner) {
            break;
        }
        let Some(predicates) = compose(&outer.expressions, &inner.expressions) else {
            // Contradiction/empty-output construction belongs to its own
            // normalizer; do not silently fabricate new cardinality here.
            break;
        };
        let mut child = std::mem::replace(
            &mut outer.child,
            Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        let LogicalOperator::Filter(inner) = &mut child.operator else {
            unreachable!()
        };
        outer.child = std::mem::replace(
            &mut inner.child,
            Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        outer.expressions = predicates;
    }
    plan
}
