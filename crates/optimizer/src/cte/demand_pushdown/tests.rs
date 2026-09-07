// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression, Expression};
use paro_planner::operator::{
    Aggregate, CTERef, ComparisonJoin, ExpressionGet, Join, JoinComparisonType, JoinCondition,
    JoinType, LogicalOperator, MaterializedCTE, Projection, SetOperation,
};
use paro_planner::plan::OwnedLogicalPlan;

use super::CTEDemandPusher;

fn column(table_index: usize) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(
        paro_planner::operator::ColumnBinding::new(table_index, 0),
        LogicalType::Integer,
    ))
}

fn values(bind_context: &BindContext, table_index: usize, value: i32) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![vec![Expression::Constant(ConstantExpression::new(
                Value::Integer(value),
                LogicalType::Integer,
            ))]],
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    )
}

fn equality_join(
    bind_context: &BindContext,
    left: OwnedLogicalPlan,
    right: OwnedLogicalPlan,
    left_table: usize,
    right_table: usize,
) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::new(
                column(left_table),
                column(right_table),
                JoinComparisonType::Equal,
            )],
        ))),
    )
}

fn cte_ref(bind_context: &BindContext, cte_index: usize, table_index: usize) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::CTERef(CTERef::new(
            cte_index,
            table_index,
            "shared".to_string(),
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    )
}

fn demand_plan(bind_context: &BindContext) -> OwnedLogicalPlan {
    let producer_join = equality_join(
        bind_context,
        values(bind_context, 1, 1),
        values(bind_context, 2, 1),
        1,
        2,
    );
    let producer_aggregate = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Aggregate::new(
            3,
            4,
            5,
            producer_join,
            vec![column(2)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )),
    );
    let producer = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(6, producer_aggregate, vec![column(3)])),
    );

    let first = equality_join(
        bind_context,
        cte_ref(bind_context, 9, 10),
        values(bind_context, 11, 1),
        10,
        11,
    );
    let second = equality_join(
        bind_context,
        cte_ref(bind_context, 9, 12),
        values(bind_context, 13, 2),
        12,
        13,
    );
    let consumers = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::SetOperation(SetOperation::union(
            14,
            first,
            second,
            true,
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );

    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "shared".to_string(),
            vec!["key".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            producer,
            consumers,
        )),
    )
}

#[test]
fn pushes_join_key_union_to_the_narrowest_group_key_owner() {
    let bind_context = BindContext::new();
    let plan = demand_plan(&bind_context);

    let (rewritten, changed) =
        CTEDemandPusher::new(&bind_context).optimize_default_root_with_change(plan);
    assert!(changed, "{rewritten:#?}");
    let LogicalOperator::MaterializedCTE(cte) = &rewritten.operator else {
        panic!("expected materialized CTE")
    };
    assert_eq!(cte.materialized, CTEMaterialize::Materialized);

    let mut producer_semi_joins = 0;
    cte.cte_query
        .try_visit_pre_order(|plan| {
            if matches!(
                &plan.operator,
                LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Semi
            ) {
                producer_semi_joins += 1;
            }
            Ok(())
        })
        .expect("inspect demand-restricted producer");
    assert_eq!(producer_semi_joins, 1, "{:#?}", cte.cte_query);

    let (rewritten, changed_again) =
        CTEDemandPusher::new(&bind_context).optimize_default_root_with_change(rewritten);
    assert!(!changed_again, "{rewritten:#?}");
}
