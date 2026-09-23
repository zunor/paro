// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction-time normalization of total, schema-preserving restrictions.
//!
//! This is not a rule alternative or an estimator. Both initial construction
//! and transactional publication use this contract before interning a shell.
//! Estimates remain owned by the output relation, not by the number of filters.

use super::*;

fn transparent<Child>(filter: &paro_planner::operator::Filter<Child>) -> bool {
    filter.projection_map.is_all()
        && filter.expressions.iter().all(|predicate| {
            let properties = predicate.evaluation_properties();
            !properties.is_reorder_fence() && properties.is_infallible()
        })
}

fn compose(outer: &[Expression], inner: &[Expression]) -> Option<Vec<Expression>> {
    crate::filter::pushdown::FilterPushdown::normalize_predicates(
        inner.iter().chain(outer).cloned(),
    )
}

/// Normalize the SQL pipeline before initial post-order Memo construction.
/// Transactional native publication uses the same transparency/composition
/// contract below; generic Memo fixtures need not be SQL-normalized trees.
pub(crate) fn normalize_tree(plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
    plan.try_fold_post_order(|mut plan, _: Vec<()>| {
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
        Ok((plan, ()))
    })
    .map(|(plan, ())| plan)
}

/// Follow only already-observed inputs in the same context. No representative
/// subtree is instantiated and no new untracked fact read is introduced. A
/// group with multiple alternatives stays opaque: normalization must not elect
/// a member according to arrival order. Each consumed expression remains an
/// immutable equivalence witness even if more alternatives are published later.
pub(super) fn normalize_memo_input(
    operator: &LogicalOperator<()>,
    input: GroupId,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
    context: OptimizationContextId,
) -> Result<Option<(LogicalOperator<()>, GroupId)>> {
    let LogicalOperator::Filter(outer) = operator else {
        return Ok(None);
    };
    if !transparent(outer) {
        return Ok(None);
    }
    let mut predicates = outer.expressions.clone();
    let mut current = memo.canonical_group(input);
    let mut seen = BTreeSet::new();
    while seen.insert(current) {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        let Some(group) = memo.group(current) else {
            return Err(paro_error::internal("restriction input group is missing"));
        };
        let [expression] = group.logical_exprs() else {
            break;
        };
        let logical = memo
            .logical_expr(*expression)
            .ok_or_else(|| paro_error::internal("restriction input expression is missing"))?;
        let metadata = state
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("restriction input metadata is missing"))?;
        let LogicalOperator::Filter(inner) = &state.payloads.logical[logical.payload.index()]
            .semantic_template
            .operator
        else {
            break;
        };
        if !transparent(inner)
            || metadata.input_context != context
            || metadata.child_context != context
            || metadata.required_region_facet.is_some()
            || metadata.runtime_filter_region_facet.is_some()
        {
            break;
        }
        let [child] = logical.key.children.as_ref() else {
            return Err(paro_error::internal("restriction has invalid child arity"));
        };
        let child = memo.canonical_group(*child);
        // Canonical payloads erase projection maps. Check the positional
        // contract, not just the unordered group schema: a reordered output
        // must not silently acquire the input's physical column order.
        let [layout] = metadata.child_layouts.as_ref() else {
            return Err(paro_error::internal("restriction has invalid layout arity"));
        };
        let preserves_layout = metadata.output_columns.len() == layout.len()
            && metadata
                .output_columns
                .iter()
                .zip(layout.bindings().iter().zip(layout.types()))
                .all(|(column, (binding, ty))| {
                    state
                        .binding_ids
                        .get(binding.table_index, binding.column_index, ty)
                        == Some(column)
                });
        if seen.contains(&child)
            || !facts.contains_group(memo, child)
            || !preserves_layout
            || memo
                .group(child)
                .is_none_or(|child| child.schema != group.schema)
        {
            break;
        }
        let Some(combined) = compose(&predicates, &inner.expressions) else {
            break;
        };
        predicates = combined;
        current = child;
    }
    if current == memo.canonical_group(input) {
        return Ok(None);
    }
    let mut normalized = outer.clone();
    normalized.expressions = predicates;
    Ok(Some((LogicalOperator::Filter(normalized), current)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression,
    };
    use paro_planner::operator::{Filter, Get, ProjectionMap};
    use paro_planner::plan::CardinalityEstimate;

    fn scan() -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new_without_table(
            0,
            vec!["a".into(), "b".into()],
            vec![LogicalType::Integer; 2],
        ))))
    }

    fn predicate(value: i32) -> Expression {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    }

    #[test]
    fn initial_construction_flattens_without_reestimating_the_relation() {
        let inner = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            scan(),
            vec![predicate(1)],
        )));
        let mut outer = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            inner,
            vec![predicate(2)],
        )));
        outer.stats.estimated_cardinality = Some(CardinalityEstimate::exact(73));
        let normalized = normalize_tree(outer).unwrap();
        assert_eq!(normalized.stats.estimated_cardinality.unwrap().expected, 73);
        let LogicalOperator::Filter(filter) = &normalized.operator else {
            panic!()
        };
        assert!(matches!(filter.child.operator, LogicalOperator::Get(_)));
        let input =
            MemoBuilder::build(normalized, BindContext::new(), SearchBudget::default()).unwrap();
        assert_eq!(input.memo.group_count(), 2);
    }

    #[test]
    fn projection_and_evaluation_barriers_remain_opaque() {
        let mut inner = Filter::new(scan(), vec![predicate(1)]);
        inner.projection_map = vec![1, 0].into();
        let outer = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(inner)),
            vec![predicate(2)],
        )));
        let normalized = normalize_tree(outer).unwrap();
        assert!(matches!(
            normalized.children()[0].operator,
            LogicalOperator::Filter(_)
        ));

        // An ordinary bound function is fallible unless its implementation
        // supplies the stronger totality contract. Such a predicate may not
        // be evaluated on rows eliminated by the inner restriction.
        use paro_function::scalar::ScalarFunction;
        use paro_planner::expression::FunctionExpression;
        let function = ScalarFunction::new(
            "f".into(),
            vec![],
            LogicalType::Boolean,
            |_, _, _| unreachable!(),
        );
        let mut expression = FunctionExpression::new(function, vec![], LogicalType::Boolean);
        expression.function.error_mode = paro_function::FunctionErrorMode::CanError;
        let fenced = Expression::Function(expression.into());
        assert!(!fenced.evaluation_properties().is_infallible());
        let inner = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            scan(),
            vec![predicate(1)],
        )));
        let outer =
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(inner, vec![fenced])));
        let normalized = normalize_tree(outer).unwrap();
        assert!(matches!(
            normalized.children()[0].operator,
            LogicalOperator::Filter(_)
        ));
    }

    #[test]
    fn memo_normalization_uses_observed_positional_contracts_only() {
        for reorder in [false, true] {
            let mut inner = Filter::new(scan(), vec![predicate(1)]);
            if reorder {
                inner.projection_map = vec![1, 0].into();
            }
            let mut input = MemoBuilder::build(
                OwnedLogicalPlan::synthetic(LogicalOperator::Filter(inner)),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            let state = input.planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let context =
                state.metadata[&input.memo.logical_expr(expression).unwrap().payload].child_context;
            let mut ctx = TransformContext::new(&mut input.memo, input.root);
            let facts = boundary::BoundarySnapshot::read(
                &mut ctx,
                &state,
                &PatternOperand::Group(input.root),
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .unwrap();
            let outer = LogicalOperator::Filter(Filter {
                expressions: vec![predicate(2)],
                child: (),
                projection_map: ProjectionMap::all(),
            });
            let result =
                normalize_memo_input(&outer, input.root, ctx.memo(), &state, &facts, context)
                    .unwrap();
            assert_eq!(result.is_some(), !reorder);
            assert!(normalize_memo_input(
                &outer,
                input.root,
                ctx.memo(),
                &state,
                &boundary::BoundarySnapshot::default(),
                context
            )
            .unwrap()
            .is_none());
            if let Some((operator, child)) = result {
                assert_ne!(child, input.root);
                assert!(normalize_memo_input(
                    &operator,
                    child,
                    ctx.memo(),
                    &state,
                    &facts,
                    context
                )
                .unwrap()
                .is_none());
            }
        }
    }
}
