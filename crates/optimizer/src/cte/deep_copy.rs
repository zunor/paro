// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deep-copy helpers for CTE inlining (delegates to the planner).

#[cfg(test)]
mod tests {
    use crate::verify::verify_logical_plan;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::deep_copy::deep_copy_plan;
    use paro_planner::binder::ir::CTEMaterialize;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        CTERef, ComparisonJoin, Join, JoinComparisonType, JoinCondition, LogicalOperator,
        MaterializedCTE, Projection, SetOpType, SetOperation,
    };
    use paro_planner::plan::LogicalPlan;

    fn expr_get(table_index: usize, values: &[i32]) -> LogicalOperator {
        let expressions = values
            .iter()
            .map(|v| {
                vec![Expression::Constant(
                    paro_planner::expression::ConstantExpression {
                        value: paro_common::runtime_value::Value::Integer(*v),
                        return_type: LogicalType::Integer,
                    },
                )]
            })
            .collect();
        LogicalOperator::ExpressionGet(paro_planner::operator::ExpressionGet::new(
            table_index,
            expressions,
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    fn plan_expr_get(ctx: &BindContext, table_index: usize, values: &[i32]) -> LogicalPlan {
        LogicalPlan::new(ctx, expr_get(table_index, values))
    }

    #[test]
    fn deep_copy_rebinds_indices_and_internal_cte_refs() {
        let bind_context = BindContext::new();
        let cte_query = LogicalOperator::Projection(Projection::new(
            3,
            plan_expr_get(&bind_context, 2, &[1, 2]),
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                paro_planner::operator::ColumnBinding::new(2, 0),
                LogicalType::Integer,
            ))],
        ));
        let cte_query_with_nested = LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            4,
            "nested".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            LogicalPlan::new(&bind_context, cte_query),
            LogicalPlan::new(
                &bind_context,
                LogicalOperator::CTERef(CTERef::new(
                    4,
                    5,
                    "cte".to_string(),
                    vec!["v".to_string()],
                    vec![LogicalType::Integer],
                )),
            ),
        ));

        let left_ref = LogicalOperator::CTERef(CTERef::new(
            9,
            6,
            "cte".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ));
        let right_ref = LogicalOperator::CTERef(CTERef::new(
            9,
            7,
            "cte".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ));
        let join = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            paro_planner::operator::JoinType::Inner,
            LogicalPlan::new(&bind_context, left_ref),
            LogicalPlan::new(&bind_context, right_ref),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    paro_planner::operator::ColumnBinding::new(6, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    paro_planner::operator::ColumnBinding::new(7, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        )));

        let plan = LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "outer".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            LogicalPlan::new(&bind_context, cte_query_with_nested),
            LogicalPlan::new(&bind_context, join),
        ));

        let copied = deep_copy_plan(
            &LogicalPlan::new(&bind_context, plan),
            bind_context.shared().as_ref(),
        );

        verify_logical_plan(&bind_context, &copied).expect("copied plan should remain valid");

        match copied.operator {
            LogicalOperator::MaterializedCTE(cte) => {
                assert_ne!(cte.cte_index, 9);
                match cte.cte_query.operator {
                    LogicalOperator::MaterializedCTE(nested) => {
                        assert_ne!(nested.cte_index, 4);
                        match nested.child.operator {
                            LogicalOperator::CTERef(cte_ref) => {
                                assert_eq!(cte_ref.cte_index, nested.cte_index);
                            }
                            other => panic!("expected nested CTERef, got {other:?}"),
                        }
                    }
                    other => panic!("expected nested materialized cte, got {other:?}"),
                }

                match cte.child.operator {
                    LogicalOperator::Join(Join::Comparison(join)) => {
                        match (join.left.operator, join.right.operator) {
                            (LogicalOperator::CTERef(left), LogicalOperator::CTERef(right)) => {
                                assert_eq!(left.cte_index, cte.cte_index);
                                assert_eq!(right.cte_index, cte.cte_index);
                                assert_ne!(left.table_index, 6);
                                assert_ne!(right.table_index, 7);
                            }
                            other => panic!("expected cte refs, got {other:?}"),
                        }
                    }
                    other => panic!("expected join child, got {other:?}"),
                }
            }
            other => panic!("expected outer materialized cte, got {other:?}"),
        }
    }

    #[test]
    fn deep_copy_keeps_external_cte_references() {
        let bind_context = BindContext::new();
        let inner_ref = LogicalOperator::CTERef(CTERef::new(
            123,
            1,
            "cte".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ));
        let plan = LogicalOperator::SetOperation(SetOperation::new(
            2,
            LogicalPlan::new(&bind_context, inner_ref),
            plan_expr_get(&bind_context, 3, &[1]),
            SetOpType::Union,
            true,
            vec![LogicalType::Integer],
        ));

        let copied = deep_copy_plan(
            &LogicalPlan::new(&bind_context, plan),
            bind_context.shared().as_ref(),
        );

        verify_logical_plan(&bind_context, &copied).expect("copied plan should remain valid");

        match copied.operator {
            LogicalOperator::SetOperation(setop) => match setop.left.operator {
                LogicalOperator::CTERef(cte_ref) => {
                    assert_eq!(cte_ref.cte_index, 123);
                }
                other => panic!("expected cte ref, got {other:?}"),
            },
            other => panic!("expected set op, got {other:?}"),
        }
    }
}
