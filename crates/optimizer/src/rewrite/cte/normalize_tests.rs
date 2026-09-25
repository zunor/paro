// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::{runtime_value::Value, types::LogicalType};
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
};
use paro_planner::operator::{
    CTERef, CrossProduct, ExpressionGet, Filter, LogicalOperator, MaterializedCTE,
};
use paro_planner::plan::OwnedLogicalPlan;

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

fn filtered_consumer(table_index: usize, value: i32) -> OwnedLogicalPlan {
    let reference = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
        9,
        table_index,
        "shared".into(),
        vec!["key".into()],
        vec![LogicalType::Integer],
    )));
    OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        reference,
        vec![integer_equality(table_index, 0, value)],
    )))
}

fn materialized_cte_plan(unfiltered: bool) -> OwnedLogicalPlan {
    let bind_context = BindContext::new();
    let producer = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        vec![vec![Expression::Constant(
            ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
        )]],
        vec!["key".into()],
        vec![LogicalType::Integer],
    )));
    let second = if unfiltered {
        OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            9,
            11,
            "shared".into(),
            vec!["key".into()],
            vec![LogicalType::Integer],
        )))
    } else {
        filtered_consumer(11, 2)
    };
    let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(
        paro_planner::operator::Join::Cross(CrossProduct::new(filtered_consumer(10, 1), second)),
    ));
    OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "shared".into(),
            vec!["key".into()],
            vec![LogicalType::Integer],
            CTEMaterialize::Materialized,
            producer,
            consumers,
        )),
    )
}

#[test]
fn all_consumers_required_and_residuals_preserved() {
    for unfiltered in [false, true] {
        let result = normalize(materialized_cte_plan(unfiltered)).unwrap();
        let LogicalOperator::MaterializedCTE(cte) = &result.operator else {
            panic!("expected CTE")
        };
        assert_eq!(
            matches!(cte.cte_query.operator, LogicalOperator::Filter(_)),
            !unfiltered
        );
        let LogicalOperator::Join(paro_planner::operator::Join::Cross(join)) = &cte.child.operator
        else {
            panic!("expected cross")
        };
        assert!(matches!(join.left.operator, LogicalOperator::Filter(_)));
        // Repeated normalization must not stack duplicate producer filters.
        let again = normalize(result).unwrap();
        let LogicalOperator::MaterializedCTE(cte) = &again.operator else {
            panic!("expected CTE")
        };
        if let LogicalOperator::Filter(filter) = &cte.cte_query.operator {
            assert!(!matches!(filter.child.operator, LogicalOperator::Filter(_)));
        }
    }
}

#[test]
fn unfiltered_reference_in_nested_producer_blocks_restriction() {
    let mut plan = materialized_cte_plan(false);
    let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator else {
        panic!("expected CTE")
    };
    let nested_producer = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
        9,
        12,
        "shared".into(),
        vec!["key".into()],
        vec![LogicalType::Integer],
    )));
    let body = std::mem::replace(
        &mut cte.child,
        Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    cte.child = Box::new(OwnedLogicalPlan::synthetic(
        LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            20,
            "nested".into(),
            vec!["key".into()],
            vec![LogicalType::Integer],
            CTEMaterialize::Materialized,
            nested_producer,
            *body,
        )),
    ));
    let result = normalize(plan).unwrap();
    let LogicalOperator::MaterializedCTE(cte) = &result.operator else {
        panic!("expected CTE")
    };
    assert!(matches!(
        cte.cte_query.operator,
        LogicalOperator::ExpressionGet(_)
    ));
}

#[test]
fn consumer_mapping_uses_definition_identity_not_column_position() {
    use paro_planner::operator::cte::{CteColumnId, CteOutputColumn};
    let mut reference = CTERef::new(
        9,
        10,
        "shared".into(),
        vec!["b".into(), "a".into()],
        vec![LogicalType::Integer; 2],
    );
    reference.definition_columns = vec![CteColumnId(1), CteColumnId(0)];
    let output = [
        CteOutputColumn {
            definition: CteColumnId(0),
            binding: ColumnBinding::new(0, 0),
        },
        CteOutputColumn {
            definition: CteColumnId(1),
            binding: ColumnBinding::new(0, 1),
        },
    ];
    let filter = Filter {
        child: (),
        expressions: vec![integer_equality(10, 0, 2)],
        projection_map: paro_planner::operator::ProjectionMap::all(),
    };
    let mapped = filtered_cte_ref(&reference, &filter, &output).unwrap();
    assert_eq!(
        mapped.old_bindings,
        vec![ColumnBinding::new(10, 1), ColumnBinding::new(10, 0)]
    );
    reference.definition_columns = vec![CteColumnId(0); 2];
    assert!(filtered_cte_ref(&reference, &filter, &output).is_none());
}

#[test]
fn a_foreign_consumer_column_cannot_restrict_the_producer() {
    let mut plan = materialized_cte_plan(false);
    let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator else {
        unreachable!()
    };
    let LogicalOperator::Join(paro_planner::operator::Join::Cross(join)) = &mut cte.child.operator
    else {
        unreachable!()
    };
    let LogicalOperator::Filter(filter) = &mut join.left.operator else {
        unreachable!()
    };
    filter.expressions = vec![integer_equality(1234, 0, 1)];
    let result = normalize(plan).unwrap();
    let LogicalOperator::MaterializedCTE(cte) = &result.operator else {
        unreachable!()
    };
    assert!(matches!(
        cte.cte_query.operator,
        LogicalOperator::ExpressionGet(_)
    ));
}
