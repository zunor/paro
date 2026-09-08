// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use paro_planner::expression::ComparisonType;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType, Expression};
use paro_planner::operator::ColumnBinding;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::expression::binding_replacer::{ColumnBindingReplacer, ReplacementBinding};

#[derive(Debug, Clone)]
pub(crate) struct FilteredCTERef {
    pub(crate) old_bindings: Vec<ColumnBinding>,
    pub(crate) filters: Vec<Expression>,
}

/// Derive the producer domain from proved consumer occurrences. This helper
/// has no policy or plan-tree side effects; Memo owns occurrence coverage.
pub(crate) fn derive_producer_predicates(
    references: Vec<FilteredCTERef>,
    producer_bindings: &[ColumnBinding],
) -> Option<Vec<Expression>> {
    if references.is_empty()
        || references.iter().any(|reference| {
            reference.filters.is_empty() || reference.old_bindings.len() != producer_bindings.len()
        })
    {
        return None;
    }
    let info = MaterializedCTEInfo {
        filtered_refs: references,
    };
    let mut predicates = vec![build_or_filter(&info, producer_bindings)?];
    if let Some(domain) = build_common_equality_domain(&info, producer_bindings) {
        predicates.push(domain);
    }
    Some(predicates)
}

#[derive(Debug, Clone)]
struct MaterializedCTEInfo {
    filtered_refs: Vec<FilteredCTERef>,
}

/// Derive a producer-side necessary condition from equality domains present
/// on every consumer. For `OR(ref_1, ..., ref_n)`, weakening each disjunct to
/// its common-key equalities yields a safe superset filter while allowing the
/// condition to cross aggregates and set-operation projections.
fn build_common_equality_domain(
    info: &MaterializedCTEInfo,
    new_bindings: &[ColumnBinding],
) -> Option<Expression> {
    let mut per_ref = Vec::with_capacity(info.filtered_refs.len());
    for reference in &info.filtered_refs {
        let ordinal_by_binding = reference
            .old_bindings
            .iter()
            .copied()
            .enumerate()
            .map(|(ordinal, binding)| (binding, ordinal))
            .collect::<HashMap<_, _>>();
        let mut equalities = HashMap::new();
        for filter in &reference.filters {
            let Expression::Comparison(comparison) = filter else {
                continue;
            };
            if comparison.comparison_type != ComparisonType::Equal {
                continue;
            }
            let binding = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(_))
                | (Expression::Constant(_), Expression::ColumnRef(column))
                    if column.depth == 0 =>
                {
                    column.binding
                }
                _ => continue,
            };
            let Some(ordinal) = ordinal_by_binding.get(&binding).copied() else {
                continue;
            };
            equalities.entry(ordinal).or_insert_with(|| filter.clone());
        }
        per_ref.push((reference, equalities));
    }
    let first = per_ref.first()?;
    let mut common_ordinals = first
        .1
        .keys()
        .copied()
        .filter(|ordinal| per_ref.iter().all(|(_, map)| map.contains_key(ordinal)))
        .collect::<Vec<_>>();
    common_ordinals.sort_unstable();
    if common_ordinals.is_empty() {
        return None;
    }

    let mut disjuncts = Vec::with_capacity(per_ref.len());
    for (reference, equalities) in per_ref {
        let mut replacer = ColumnBindingReplacer::new();
        for (old_binding, new_binding) in reference.old_bindings.iter().zip(new_bindings.iter()) {
            replacer
                .replacement_bindings
                .push(ReplacementBinding::new(*old_binding, *new_binding));
        }
        let mut conjuncts = Vec::with_capacity(common_ordinals.len());
        for ordinal in &common_ordinals {
            let mut equality = equalities
                .get(ordinal)
                .expect("common consumer domain vanished")
                .clone();
            replacer.visit_expression(&mut equality);
            conjuncts.push(equality);
        }
        disjuncts.push(conjunction(ConjunctionType::And, conjuncts));
    }
    Some(conjunction(ConjunctionType::Or, disjuncts))
}

fn conjunction(kind: ConjunctionType, mut expressions: Vec<Expression>) -> Expression {
    if expressions.len() == 1 {
        expressions.pop().expect("one expression")
    } else {
        Expression::Conjunction(ConjunctionExpression::new(kind, expressions).into())
    }
}

fn build_or_filter(
    info: &MaterializedCTEInfo,
    new_bindings: &[ColumnBinding],
) -> Option<Expression> {
    let mut refs = Vec::new();

    for filtered_ref in &info.filtered_refs {
        if filtered_ref
            .filters
            .iter()
            .any(|filter| filter.evaluation_properties().is_reorder_fence())
        {
            return None;
        }
        if filtered_ref.old_bindings.len() != new_bindings.len() {
            continue;
        }
        let old_bindings = filtered_ref
            .old_bindings
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut referenced = Vec::new();
        for filter in &filtered_ref.filters {
            crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                filter,
                &mut referenced,
            );
        }
        if referenced
            .iter()
            .any(|binding| !old_bindings.contains(binding))
        {
            return None;
        }

        let mut replacer = ColumnBindingReplacer::new();
        for (old_binding, new_binding) in filtered_ref.old_bindings.iter().zip(new_bindings.iter())
        {
            replacer
                .replacement_bindings
                .push(ReplacementBinding::new(*old_binding, *new_binding));
        }

        let mut rewritten_filters = filtered_ref.filters.clone();
        for filter in &mut rewritten_filters {
            replacer.visit_expression(filter);
        }
        let new_bindings = new_bindings.iter().copied().collect::<HashSet<_>>();
        let mut rewritten_references = Vec::new();
        for filter in &rewritten_filters {
            crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                filter,
                &mut rewritten_references,
            );
        }
        if rewritten_references
            .iter()
            .any(|binding| !new_bindings.contains(binding))
        {
            return None;
        }

        let and_expr = if rewritten_filters.len() == 1 {
            rewritten_filters.pop().unwrap()
        } else {
            Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, rewritten_filters).into(),
            )
        };
        refs.push(and_expr);
    }

    match refs.len() {
        0 => None,
        1 => refs.pop(),
        _ => Some(Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::Or, refs).into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_common_equality_domain, build_or_filter, FilteredCTERef, MaterializedCTEInfo,
    };
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionType,
        ConstantExpression, Expression, FunctionExpression,
    };

    fn integer_equality(table_index: usize, column_index: usize, value: i32) -> Expression {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(
                    ColumnRefExpression::new(
                        paro_planner::operator::ColumnBinding::new(table_index, column_index),
                        LogicalType::Integer,
                    )
                    .into(),
                ),
                Expression::Constant(
                    ConstantExpression {
                        value: paro_common::runtime_value::Value::Integer(value),
                        return_type: LogicalType::Integer,
                    }
                    .into(),
                ),
            )
            .into(),
        )
    }

    #[test]
    fn does_not_copy_volatile_filters_into_cte_producer() {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        let random = || {
            Expression::Function(
                FunctionExpression::new(function.clone(), vec![], LogicalType::Double).into(),
            )
        };
        let info = MaterializedCTEInfo {
            filtered_refs: vec![FilteredCTERef {
                old_bindings: vec![],
                filters: vec![Expression::Comparison(
                    ComparisonExpression::new(ComparisonType::GreaterThan, random(), random())
                        .into(),
                )],
            }],
        };

        assert!(build_or_filter(&info, &[]).is_none());
    }

    #[test]
    fn common_equality_domain_keeps_only_ordinals_constrained_by_every_consumer() {
        let info = MaterializedCTEInfo {
            filtered_refs: vec![
                FilteredCTERef {
                    old_bindings: vec![
                        paro_planner::operator::ColumnBinding::new(2, 0),
                        paro_planner::operator::ColumnBinding::new(2, 1),
                    ],
                    filters: vec![integer_equality(2, 0, 2001), integer_equality(2, 1, 1)],
                },
                FilteredCTERef {
                    old_bindings: vec![
                        paro_planner::operator::ColumnBinding::new(3, 0),
                        paro_planner::operator::ColumnBinding::new(3, 1),
                    ],
                    filters: vec![integer_equality(3, 0, 2002)],
                },
            ],
        };
        let producer_bindings = [
            paro_planner::operator::ColumnBinding::new(10, 0),
            paro_planner::operator::ColumnBinding::new(10, 1),
        ];

        let domain = build_common_equality_domain(&info, &producer_bindings)
            .expect("the first ordinal is constrained by every consumer");
        let Expression::Conjunction(disjunction) = domain else {
            panic!("two consumers must produce a disjunction");
        };
        assert_eq!(disjunction.conjunction_type, ConjunctionType::Or);
        assert_eq!(disjunction.children.len(), 2);
        for (predicate, expected_value) in disjunction.children.iter().zip([2001, 2002]) {
            let Expression::Comparison(comparison) = predicate else {
                panic!("one shared ordinal must produce one equality per consumer");
            };
            assert_eq!(comparison.comparison_type, ComparisonType::Equal);
            let Expression::ColumnRef(column) = comparison.left.as_ref() else {
                panic!("normalized equality must retain its column on the left");
            };
            assert_eq!(column.binding, producer_bindings[0]);
            let Expression::Constant(constant) = comparison.right.as_ref() else {
                panic!("normalized equality must retain its literal on the right");
            };
            assert_eq!(
                constant.value,
                paro_common::runtime_value::Value::Integer(expected_value)
            );
        }
    }
}
