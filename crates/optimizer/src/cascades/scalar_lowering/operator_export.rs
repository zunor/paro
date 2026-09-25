// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Rehydrate native operands only at a selected implementation boundary.

use paro_common::error::{self as paro_error, Result};
use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
use paro_planner::expression::{Expression, ParameterExpression};
use paro_planner::operator::{JoinComparisonType, LogicalOperator};

use super::super::ids::{ColumnId, ScalarExprId};
use super::super::scalar::{ComparisonOp, ScalarArena, ScalarKind};
use super::fields::{self, OperandMut, ReferenceScope};
use super::BindingCatalog;

/// Replace every local scalar field using the same field/role enumeration as
/// import. The shell supplies operator shape only, never an executable operand.
/// It is unpublished until the complete export and plan verification succeed.
pub(crate) fn export_operator_scalars<Child>(
    operator: &mut LogicalOperator<Child>,
    roots: &[ScalarExprId],
    arena: &ScalarArena,
    bindings: &BindingCatalog,
    mut child_contains: impl FnMut(usize, ColumnId) -> bool,
    mut checkpoint: impl FnMut() -> Result<()>,
) -> Result<()> {
    let reducer_owner = match operator {
        LogicalOperator::Aggregate(aggregate) => {
            aggregate.post_reduction.as_ref().map(|r| r.reduction_index)
        }
        _ => None,
    };
    let mut roots = roots.iter().copied();
    fields::visit_fields_mut(operator, |field| {
        checkpoint()?;
        let root = roots
            .next()
            .ok_or_else(|| paro_error::internal("native operator lost a scalar operand"))?;
        let export = |root, owner, checkpoint: &mut dyn FnMut() -> Result<()>| {
            arena.export_expression(
                root,
                |column, ty, depth| bindings.export_column(column, ty, depth, owner),
                |slot, ty| {
                    Ok(Expression::Parameter(
                        ParameterExpression::new(ParameterSlot::new(
                            RuntimeParamId::new(slot as usize),
                            ty.clone(),
                        ))
                        .into(),
                    ))
                },
                checkpoint,
            )
        };
        match field {
            OperandMut::Expression(expression, scope) => {
                let owner = if matches!(scope, ReferenceScope::Reducers) {
                    Some(reducer_owner.ok_or_else(|| {
                        paro_error::internal("native reducer scalar has no scope owner")
                    })?)
                } else {
                    None
                };
                *expression = export(root, owner, &mut checkpoint)?;
            }
            OperandMut::Window(expression) => {
                let Expression::Window(window) = export(root, None, &mut checkpoint)? else {
                    return Err(paro_error::internal(
                        "native window field has a non-window operand",
                    ));
                };
                *expression = window.into_inner();
            }
            OperandMut::Comparison(condition) => {
                let node = arena
                    .get(root)
                    .ok_or_else(|| paro_error::internal("native join predicate is unknown"))?;
                let ScalarKind::Comparison(op) = node.kind else {
                    return Err(paro_error::internal(
                        "native join condition is not a comparison",
                    ));
                };
                let [mut left, mut right]: [ScalarExprId; 2] =
                    node.children.as_ref().try_into().map_err(|_| {
                        paro_error::internal("native join comparison has invalid arity")
                    })?;
                let mut comparison = match op {
                    ComparisonOp::Equal => JoinComparisonType::Equal,
                    ComparisonOp::NotEqual => JoinComparisonType::NotEqual,
                    ComparisonOp::Less => JoinComparisonType::LessThan,
                    ComparisonOp::LessOrEqual => JoinComparisonType::LessThanOrEqual,
                    ComparisonOp::Greater => JoinComparisonType::GreaterThan,
                    ComparisonOp::GreaterOrEqual => JoinComparisonType::GreaterThanOrEqual,
                    ComparisonOp::DistinctFrom => JoinComparisonType::DistinctFrom,
                    ComparisonOp::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
                };
                let mut belongs = |id, child| -> Result<bool> {
                    Ok(arena
                        .get(id)
                        .ok_or_else(|| paro_error::internal("native join operand is unknown"))?
                        .properties
                        .local_columns()
                        .all(|column| child_contains(child, column)))
                };
                // Scalar canonicalization may exchange comparison operands.
                // Execution's join conditions have side-specific coordinates;
                // restore those from local ColumnId membership, never old
                // payloads. Outer invocation values are not child columns.
                if !(belongs(left, 0)? && belongs(right, 1)?) {
                    if !(belongs(right, 0)? && belongs(left, 1)?) {
                        return Err(paro_error::internal(
                            "native join operands do not belong to their child domains",
                        ));
                    }
                    std::mem::swap(&mut left, &mut right);
                    comparison = comparison.flip();
                }
                condition.left = export(left, None, &mut checkpoint)?;
                condition.right = export(right, None, &mut checkpoint)?;
                condition.comparison = comparison;
            }
        }
        Ok(())
    })?;
    if roots.next().is_some() {
        return Err(paro_error::internal(
            "native operator has excess scalar operands",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::column::ColumnCatalog;
    use crate::cascades::scalar::{ScalarLocalProperties, ScalarSpec};
    use crate::cascades::scalar_lowering::intern_expression;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, Filter, Join, JoinCondition, JoinType,
    };
    use paro_planner::plan::OwnedLogicalPlan;

    #[test]
    fn canonical_comparisons_restore_execution_child_sides_without_a_template() {
        let pairs = [
            (ComparisonOp::Equal, JoinComparisonType::Equal),
            (ComparisonOp::NotEqual, JoinComparisonType::NotEqual),
            (ComparisonOp::Less, JoinComparisonType::GreaterThan),
            (
                ComparisonOp::LessOrEqual,
                JoinComparisonType::GreaterThanOrEqual,
            ),
            (ComparisonOp::Greater, JoinComparisonType::LessThan),
            (
                ComparisonOp::GreaterOrEqual,
                JoinComparisonType::LessThanOrEqual,
            ),
            (ComparisonOp::DistinctFrom, JoinComparisonType::DistinctFrom),
            (
                ComparisonOp::NotDistinctFrom,
                JoinComparisonType::NotDistinctFrom,
            ),
        ];
        for (op, expected) in pairs {
            let mut arena = ScalarArena::default();
            let mut bindings = BindingCatalog::default();
            let mut columns = ColumnCatalog::default();
            let col = |table| {
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer)
                        .into(),
                )
            };
            let left =
                intern_expression(&col(0), &[], &mut bindings, &mut columns, &mut arena).unwrap();
            let right =
                intern_expression(&col(1), &[], &mut bindings, &mut columns, &mut arena).unwrap();
            let root = arena
                .intern(ScalarSpec {
                    kind: ScalarKind::Comparison(op),
                    logical_type: LogicalType::Boolean,
                    // Explicitly model the canonical direction opposite to the
                    // selected physical join's two child namespaces.
                    children: Box::new([right, left]),
                    local_properties: ScalarLocalProperties::default(),
                })
                .unwrap();
            let ScalarKind::Column(left_column) = arena.get(left).unwrap().kind else {
                unreachable!()
            };
            let ScalarKind::Column(right_column) = arena.get(right).unwrap().kind else {
                unreachable!()
            };
            let mut operator = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                vec![JoinCondition::equality(
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(123), LogicalType::Integer).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(456), LogicalType::Integer).into(),
                    ),
                )],
            )));
            export_operator_scalars(
                &mut operator,
                &[root],
                &arena,
                &bindings,
                |side, column| column == if side == 0 { left_column } else { right_column },
                || Ok(()),
            )
            .unwrap();
            let LogicalOperator::Join(Join::Comparison(join)) = &operator else {
                unreachable!()
            };
            assert!(join.conditions[0].left.equals(&col(0)));
            assert!(join.conditions[0].right.equals(&col(1)));
            assert_eq!(join.conditions[0].comparison, expected);
            assert!(export_operator_scalars(
                &mut operator,
                &[root],
                &arena,
                &bindings,
                |_, _| false,
                || Ok(()),
            )
            .unwrap_err()
            .to_string()
            .contains("child domains"));
        }
    }

    #[test]
    fn correlated_operands_are_owned_by_the_invocation_not_a_join_child() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let outer = Expression::ColumnRef(
            ColumnRefExpression::with_depth(ColumnBinding::new(9, 0), LogicalType::Integer, 2)
                .into(),
        );
        let right = Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer).into(),
        );
        let mut operator = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![JoinCondition::equality(outer.clone(), right.clone())],
        )));
        let roots = super::super::intern_operator_scalars(
            &operator,
            &[],
            &[Box::<[ColumnId]>::default(), Box::<[ColumnId]>::default()],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        let right_column = *bindings.get(1, 0, &LogicalType::Integer).unwrap();
        export_operator_scalars(
            &mut operator,
            &roots,
            &arena,
            &bindings,
            |side, column| side == 1 && column == right_column,
            || Ok(()),
        )
        .unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = operator else {
            unreachable!()
        };
        assert!(join.conditions[0].left.equals(&outer));
        assert!(join.conditions[0].right.equals(&right));
    }

    #[test]
    fn operand_export_checks_arity_and_cancellation_before_mutation() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let value = Expression::Constant(
            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
        );
        let root = intern_expression(
            &value,
            &[],
            &mut bindings,
            &mut ColumnCatalog::default(),
            &mut arena,
        )
        .unwrap();
        let mut operator = LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![value.clone()],
        ));
        assert!(export_operator_scalars(
            &mut operator,
            &[],
            &arena,
            &bindings,
            |_, _| false,
            || Ok(()),
        )
        .unwrap_err()
        .to_string()
        .contains("lost a scalar operand"));
        assert!(export_operator_scalars(
            &mut operator,
            &[root, root],
            &arena,
            &bindings,
            |_, _| false,
            || Ok(()),
        )
        .unwrap_err()
        .to_string()
        .contains("excess scalar operands"));
        let error = export_operator_scalars(
            &mut operator,
            &[root],
            &arena,
            &bindings,
            |_, _| false,
            || Err(paro_error::internal("test cancellation")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("test cancellation"));
        let LogicalOperator::Filter(filter) = operator else {
            unreachable!()
        };
        assert!(filter.expressions[0].equals(&value));
    }
}
