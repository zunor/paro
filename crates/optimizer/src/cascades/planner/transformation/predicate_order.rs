// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native predicate ordering. Evaluation fences partition the operand stream;
//! selectivity is read once per movable operand, not inside sort comparisons.

use super::*;

pub(super) fn permutation<'a>(
    roots: &[ScalarExprId],
    state: &'a PlannerTransformState,
    statistics: impl Fn(ColumnId) -> Option<crate::cost_model::ColumnPredicateEvidence<'a>>,
    control: &crate::cascades::control::SearchControl,
) -> Result<Option<Box<[usize]>>> {
    if !control.checkpoint()? {
        return Ok(None);
    }
    if roots.len() < 2 {
        return Ok(None);
    }
    let mut ordering = Vec::with_capacity(roots.len());
    let mut segment = Vec::<(usize, f64)>::new();
    let flush = |segment: &mut Vec<(usize, f64)>, ordering: &mut Vec<usize>| {
        segment.sort_by(|left, right| left.1.total_cmp(&right.1));
        ordering.extend(segment.drain(..).map(|(ordinal, _)| ordinal));
    };
    for (ordinal, &root) in roots.iter().enumerate() {
        if !control.checkpoint()? {
            return Ok(None);
        }
        let node = state
            .scalars
            .get(root)
            .ok_or_else(|| paro_error::internal("predicate ordering lost a native operand"))?;
        if node.properties.is_evaluation_fence() {
            flush(&mut segment, &mut ordering);
            ordering.push(ordinal);
        } else {
            let Some(selectivity) = state.cost_model.estimate_native_selectivity(
                root,
                &state.scalars,
                &state.binding_ids,
                &statistics,
                || control.checkpoint(),
            )?
            else {
                return Ok(None);
            };
            segment.push((ordinal, selectivity));
        }
    }
    flush(&mut segment, &mut ordering);
    Ok(ordering
        .iter()
        .enumerate()
        .any(|(before, after)| before != *after)
        .then(|| ordering.into_boxed_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression,
        FunctionExpression,
    };
    use paro_planner::operator::{Filter, Get};

    #[test]
    fn native_order_retains_fences_stable_ties_and_original_scalar_identities() {
        let column = Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
        );
        let constant = Expression::Constant(
            ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
        );
        let range = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::LessThan, column.clone(), constant.clone())
                .into(),
        );
        let equality = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, column, constant).into(),
        );
        let random = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .unwrap();
        let fence = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::LessThan,
                Expression::Function(
                    FunctionExpression::new(random, vec![], LogicalType::Double).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Double(0.5), LogicalType::Double).into(),
                ),
            )
            .into(),
        );
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                    Get::new_without_table(0, vec!["k".into()], vec![LogicalType::Integer]),
                ))),
                vec![
                    range.clone(),
                    equality.clone(),
                    fence,
                    range,
                    equality.clone(),
                    equality,
                ],
            )));
        let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let root = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let state = input.planner_state.read().unwrap();
        let before = (
            state.staging_arena.len(),
            state.scalars.len(),
            state.columns.len(),
        );
        let order = permutation(&root.key.scalars, &state, |_| None, input.memo.control())
            .unwrap()
            .unwrap();
        assert_eq!(&*order, &[1, 0, 2, 4, 5, 3]);
        let ordered: Vec<_> = order
            .iter()
            .map(|&ordinal| root.key.scalars[ordinal])
            .collect();
        assert!(
            permutation(&ordered, &state, |_| None, input.memo.control())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            before,
            (
                state.staging_arena.len(),
                state.scalars.len(),
                state.columns.len()
            )
        );
    }
}
