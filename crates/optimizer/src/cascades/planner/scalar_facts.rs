// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable operator-local evidence derived from native scalar operands.
//!
//! Publish once with the logical shell. Root dispatch never exports or walks
//! executable expressions, and child-frontier changes cannot invalidate these
//! scalar-only facts. Relational placement still requires its own binding.

use super::*;
use crate::cascades::scalar::{ScalarKind, ScalarNode};
use paro_function::aggregate::AggregateAlgebra;

#[derive(Debug, Default)]
pub(super) struct NativeScalarFacts {
    pub(super) aggregate: Option<AggregateScalarFacts>,
}

#[derive(Debug)]
pub(super) struct AggregateScalarFacts {
    /// SUM of one local column, with no DISTINCT/FILTER/ORDER modifiers, and
    /// no grouping expression consuming that column's relation occurrence.
    pub(super) subsumable_sum_input: Option<ColumnId>,
    /// Total narrowing expressions whose raw inputs are dead outside equal
    /// native candidate occurrences. A join-domain proof is still required.
    pub(super) materializable_inputs: Box<[ScalarExprId]>,
}

impl NativeScalarFacts {
    pub(super) fn derive<Child>(
        operator: &LogicalOperator<Child>,
        roots: &[ScalarExprId],
        arena: &ScalarArena,
        bindings: &BindingCatalog,
        columns: &ColumnCatalog,
        checkpoint: impl FnMut() -> Result<bool>,
    ) -> Result<Option<Self>> {
        let mut reads = FactReads {
            arena,
            checkpoint,
            stopped: false,
        };
        if !reads.admit()? {
            return Ok(None);
        }
        let LogicalOperator::Aggregate(aggregate) = operator else {
            return Ok(Some(Self::default()));
        };
        let group_count = aggregate.groups.len();
        let aggregate_end = group_count + aggregate.aggregates.len();
        let operands = roots.get(..aggregate_end).ok_or_else(|| {
            paro_error::internal("native aggregate facts lost their operand fields")
        })?;
        let (groups, aggregates) = operands.split_at(group_count);
        let sum_input = if aggregate.post_reduction.is_none()
            && aggregate.grouping_functions.is_empty()
            && aggregates.len() == 1
        {
            plain_sum_input(aggregates[0], groups, &mut reads, bindings)?
        } else {
            None
        };
        let mut materializable = BTreeSet::new();
        let mut examined = BTreeSet::new();
        for &root in aggregates {
            let Some(node) = reads.scalar(root)? else {
                return Ok(None);
            };
            let ScalarKind::Aggregate { function } = &node.kind else {
                continue;
            };
            for &candidate in &node.children[..function.argument_count()] {
                if !reads.admit()? {
                    return Ok(None);
                }
                if examined.insert(candidate)
                    && narrowing_total(candidate, &mut reads, bindings, columns)?
                    && inputs_dead_outside(candidate, operands, &mut reads)?
                {
                    materializable.insert(candidate);
                }
            }
        }
        if reads.stopped {
            return Ok(None);
        }
        Ok(Some(Self {
            aggregate: Some(AggregateScalarFacts {
                subsumable_sum_input: sum_input,
                materializable_inputs: materializable.into_iter().collect(),
            }),
        }))
    }
}

struct FactReads<'a, F> {
    arena: &'a ScalarArena,
    checkpoint: F,
    stopped: bool,
}

impl<'a, F: FnMut() -> Result<bool>> FactReads<'a, F> {
    fn admit(&mut self) -> Result<bool> {
        self.stopped = self.stopped || !(self.checkpoint)()?;
        Ok(!self.stopped)
    }

    fn scalar(&mut self, id: ScalarExprId) -> Result<Option<&'a ScalarNode>> {
        if !self.admit()? {
            return Ok(None);
        }
        self.arena
            .get(id)
            .map(Some)
            .ok_or_else(|| paro_error::internal("native scalar fact references an unknown operand"))
    }
}

fn plain_sum_input(
    root: ScalarExprId,
    groups: &[ScalarExprId],
    reads: &mut FactReads<'_, impl FnMut() -> Result<bool>>,
    bindings: &BindingCatalog,
) -> Result<Option<ColumnId>> {
    let Some(node) = reads.scalar(root)? else {
        return Ok(None);
    };
    let ScalarKind::Aggregate { function } = &node.kind else {
        return Ok(None);
    };
    if function.function().algebra != Some(AggregateAlgebra::Sum)
        || function.is_distinct()
        || function.filter_ordinal().is_some()
        || !function.orders().is_empty()
        || function.argument_count() != 1
    {
        return Ok(None);
    }
    let Some(input) = reads.scalar(node.children[0])? else {
        return Ok(None);
    };
    let ScalarKind::Column(input) = input.kind else {
        return Ok(None);
    };
    let Some(owner) = bindings.relation_binding(input) else {
        return Ok(None);
    };
    for &group in groups {
        let Some(group) = reads.scalar(group)? else {
            return Ok(None);
        };
        for column in group.properties.local_columns() {
            if !reads.admit()? {
                return Ok(None);
            }
            let Some(binding) = bindings.relation_binding(column) else {
                return Ok(None);
            };
            if binding.table_index == owner.table_index {
                return Ok(None);
            }
        }
    }
    Ok(Some(input))
}

fn narrowing_total(
    candidate: ScalarExprId,
    reads: &mut FactReads<'_, impl FnMut() -> Result<bool>>,
    bindings: &BindingCatalog,
    columns: &ColumnCatalog,
) -> Result<bool> {
    let Some(node) = reads.scalar(candidate)? else {
        return Ok(false);
    };
    if matches!(
        node.kind,
        ScalarKind::Column(_)
            | ScalarKind::CorrelatedColumn { .. }
            | ScalarKind::Constant { .. }
            | ScalarKind::Parameter(_)
    ) || !node.properties.can_reorder_and_share()
        || node.properties.has_outer_references()
    {
        return Ok(false);
    }
    let mut count = 0;
    let mut width = 0usize;
    for column in node.properties.local_columns() {
        if !reads.admit()? {
            return Ok(false);
        }
        if bindings.relation_binding(column).is_none() {
            return Ok(false);
        }
        let column = columns
            .get(column)
            .ok_or_else(|| paro_error::internal("native scalar fact lost a column type"))?;
        count += 1;
        width = width.saturating_add(column.logical_type.type_size());
    }
    Ok(count >= 2 && node.logical_type.type_size() < width)
}

fn inputs_dead_outside(
    candidate: ScalarExprId,
    roots: &[ScalarExprId],
    reads: &mut FactReads<'_, impl FnMut() -> Result<bool>>,
) -> Result<bool> {
    let Some(candidate_node) = reads.scalar(candidate)? else {
        return Ok(false);
    };
    let inputs = &candidate_node.properties.column_references;
    let mut seen = BTreeSet::new();
    let mut pending = roots.to_vec();
    while let Some(root) = pending.pop() {
        if !reads.admit()? {
            return Ok(false);
        }
        if root == candidate || !seen.insert(root) {
            continue;
        }
        let Some(node) = reads.scalar(root)? else {
            return Ok(false);
        };
        if node.properties.column_references.is_disjoint(inputs) {
            continue;
        }
        if matches!(node.kind, ScalarKind::Column(_)) {
            return Ok(false);
        }
        pending.extend(node.children.iter().copied());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_planner::expression::{
        AggregateExpression, AggregateType, CaseExpression, ColumnRefExpression,
        ComparisonExpression, ComparisonType, ConstantExpression,
    };
    use paro_planner::operator::{Aggregate, PostAggregateReduction};

    fn col(owner: usize, index: usize, depth: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::with_depth(
                ColumnBinding::new(owner, index),
                LogicalType::BigInt,
                depth,
            )
            .into(),
        )
    }

    fn constant(value: i64) -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::BigInt(value), LogicalType::BigInt).into(),
        )
    }

    fn sum(input: Expression) -> Expression {
        let (function, _) = get_sum_function().bind(&[input.return_type()]).unwrap();
        let ty = function.return_type.clone();
        Expression::Aggregate(AggregateExpression::new(function, vec![input], ty).into())
    }

    fn case(depth: usize) -> Expression {
        Expression::Case(
            CaseExpression::new(
                Expression::Comparison(
                    ComparisonExpression::new(
                        ComparisonType::Equal,
                        col(0, 0, 0),
                        col(0, 1, depth),
                    )
                    .into(),
                ),
                constant(1),
                constant(0),
                LogicalType::BigInt,
            )
            .into(),
        )
    }

    fn operator(groups: Vec<Expression>, aggregates: Vec<Expression>) -> LogicalOperator {
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            10,
            11,
            12,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            groups,
            vec![],
            aggregates,
            vec![],
        )))
    }

    fn derive(operator: &LogicalOperator) -> AggregateScalarFacts {
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let mut scalars = ScalarArena::default();
        let roots = intern_operator_scalars(
            operator,
            &[],
            &[Box::<[ColumnId]>::default()],
            &mut bindings,
            &mut columns,
            &mut scalars,
        )
        .unwrap();
        NativeScalarFacts::derive(operator, &roots, &scalars, &bindings, &columns, || Ok(true))
            .unwrap()
            .unwrap()
            .aggregate
            .unwrap()
    }

    #[test]
    fn incomplete_fact_derivation_is_not_published_as_negative_evidence() {
        let operator = operator(vec![], vec![sum(case(0))]);
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let mut scalars = ScalarArena::default();
        let roots = intern_operator_scalars(
            &operator,
            &[],
            &[Box::<[ColumnId]>::default()],
            &mut bindings,
            &mut columns,
            &mut scalars,
        )
        .unwrap();
        let mut complete_reads = 0;
        assert!(NativeScalarFacts::derive(
            &operator,
            &roots,
            &scalars,
            &bindings,
            &columns,
            || {
                complete_reads += 1;
                Ok(true)
            }
        )
        .unwrap()
        .is_some());
        for limit in 0..complete_reads {
            let mut reads = 0;
            let outcome =
                NativeScalarFacts::derive(&operator, &roots, &scalars, &bindings, &columns, || {
                    reads += 1;
                    Ok(reads <= limit)
                })
                .unwrap();
            assert!(outcome.is_none(), "read limit {limit}");
            assert_eq!(reads, limit + 1);
        }
        let error =
            NativeScalarFacts::derive(&operator, &roots, &scalars, &bindings, &columns, || {
                Err(paro_error::internal("injected cancellation"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("injected cancellation"));
    }

    #[test]
    fn native_aggregate_guards_match_executable_ir_oracles() {
        let groups = [
            vec![],
            vec![col(0, 0, 0)],
            vec![col(1, 0, 0)],
            vec![col(0, 0, 1)],
        ];
        let inputs = [col(0, 1, 0), col(0, 1, 1), constant(1), case(0), case(1)];
        let mut count = 0;
        for groups in groups {
            for input in &inputs {
                for modifier in 0..5 {
                    let mut aggregate = sum(input.clone());
                    let Expression::Aggregate(sum) = &mut aggregate else {
                        unreachable!()
                    };
                    match modifier {
                        1 => sum.aggr_type = AggregateType::Distinct,
                        2 => {
                            sum.filter = Some(Box::new(Expression::Comparison(
                                ComparisonExpression::new(
                                    ComparisonType::GreaterThan,
                                    col(0, 0, 0),
                                    constant(0),
                                )
                                .into(),
                            )))
                        }
                        3 => sum
                            .order_bys
                            .push(paro_planner::expression::OrderByExpression {
                                expression: col(0, 0, 0),
                                ascending: true,
                                nulls_first: false,
                            }),
                        _ => {}
                    }
                    let aggregates = if modifier == 4 {
                        vec![aggregate.clone(), aggregate]
                    } else {
                        vec![aggregate]
                    };
                    let operator = operator(groups.clone(), aggregates);
                    let native = derive(&operator);
                    assert_eq!(
                        native.subsumable_sum_input.is_some(),
                        crate::aggregate::join_subsumption::recognizes_outer_aggregate(&operator)
                    );
                    assert_eq!(
                        !native.materializable_inputs.is_empty(),
                        crate::aggregate::input_materialization::recognizes_aggregate(&operator)
                    );
                    count += 1;
                }
            }
        }
        assert_eq!(count, 100);
    }

    #[test]
    fn narrowing_evidence_tracks_raw_input_liveness_and_lexical_scopes() {
        assert_eq!(
            derive(&operator(vec![], vec![sum(case(0))]))
                .materializable_inputs
                .len(),
            1
        );
        assert_eq!(
            derive(&operator(vec![], vec![sum(case(0)), sum(case(0))]))
                .materializable_inputs
                .len(),
            1
        );
        assert!(derive(&operator(vec![col(0, 0, 0)], vec![sum(case(0))]))
            .materializable_inputs
            .is_empty());
        assert!(
            derive(&operator(vec![], vec![sum(case(0)), sum(col(0, 0, 0))]))
                .materializable_inputs
                .is_empty()
        );
        assert_eq!(
            derive(&operator(vec![col(0, 0, 1)], vec![sum(case(0))]))
                .materializable_inputs
                .len(),
            1
        );
        assert!(derive(&operator(vec![], vec![sum(case(1))]))
            .materializable_inputs
            .is_empty());
    }

    #[test]
    fn subsumption_requires_plain_sum_and_excludes_post_reduction_and_grouping_functions() {
        let mut op = operator(vec![], vec![sum(col(0, 0, 0))]);
        assert!(derive(&op).subsumable_sum_input.is_some());
        let LogicalOperator::Aggregate(aggregate) = &mut op else {
            unreachable!()
        };
        aggregate.grouping_functions.push(vec![]);
        assert!(derive(&op).subsumable_sum_input.is_none());
        let LogicalOperator::Aggregate(aggregate) = &mut op else {
            unreachable!()
        };
        aggregate.grouping_functions.clear();
        aggregate.post_reduction = Some(PostAggregateReduction {
            reduction_index: 13,
            reducers: vec![],
            scalar_expressions: vec![],
            predicate: Expression::Constant(
                ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
            ),
        });
        assert!(derive(&op).subsumable_sum_input.is_none());
    }
}
