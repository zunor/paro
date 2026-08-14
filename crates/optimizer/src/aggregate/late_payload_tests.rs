// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry};
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{AggregateExpression, ColumnRefExpression, Expression};
use paro_planner::operator::aggregate::GroupDependency;
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, CrossProduct, Get, Join, JoinComparisonType,
    JoinCondition, JoinType, LogicalOperator, Projection, TopN,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::table::table_factory::TableFactory;

use super::late_payload::optimize_plan;
use crate::cost_model::CostModel;

const SOURCE: usize = 10;
const GROUP: usize = 20;
const AGGREGATE: usize = 21;
const GROUPINGS: usize = 22;
const OUTPUT: usize = 30;

fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(table, index),
        ty,
    ))
}

fn source_table() -> Arc<TableCatalogEntry> {
    let columns = vec![
        ColumnDefinition::new("key".to_string(), LogicalType::BigInt),
        ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ColumnDefinition::new("address".to_string(), LogicalType::Varchar),
        ColumnDefinition::new("value".to_string(), LogicalType::Integer),
    ];
    let types = columns
        .iter()
        .map(|column| column.logical_type.clone())
        .collect::<Vec<_>>();
    let storage = Arc::new(TableFactory::default().create_table(&types).expect("table"));
    Arc::new(
        TableCatalogEntry::from_info(
            CreateTableInfo::new(
                "paro".to_string(),
                "public".to_string(),
                "late_source".to_string(),
                columns,
            ),
            storage,
            CatalogObjectId::from_raw(91_010),
            0,
        )
        .expect("catalog table"),
    )
}

fn candidate(order_by_payload: bool) -> LogicalPlan {
    let table = source_table();
    let get = Get::new(
        SOURCE,
        table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        table
            .columns
            .iter()
            .map(|column| column.logical_type.clone())
            .collect(),
        table,
    );
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind sum");
    let mut aggregate = Aggregate::new(
        GROUP,
        AGGREGATE,
        GROUPINGS,
        LogicalPlan::synthetic(LogicalOperator::Get(get)),
        vec![
            column(SOURCE, 0, LogicalType::BigInt),
            column(SOURCE, 1, LogicalType::Varchar),
            column(SOURCE, 2, LogicalType::Varchar),
        ],
        vec![],
        vec![Expression::Aggregate(AggregateExpression::new(
            sum,
            vec![column(SOURCE, 3, LogicalType::Integer)],
            LogicalType::BigInt,
        ))],
        vec![],
    );
    aggregate.group_dependencies.push(GroupDependency {
        determinants: vec![0].into_boxed_slice(),
        dependents: vec![1, 2].into_boxed_slice(),
    });
    aggregate.child.stats.estimated_cardinality =
        Some(paro_planner::plan::CardinalityEstimate::exact(100_000));
    let mut aggregate_plan = LogicalPlan::synthetic(LogicalOperator::Aggregate(aggregate));
    aggregate_plan.stats.estimated_cardinality =
        Some(paro_planner::plan::CardinalityEstimate::exact(10_000));
    let projection = Projection::new(
        OUTPUT,
        aggregate_plan,
        vec![
            column(GROUP, 0, LogicalType::BigInt),
            column(GROUP, 1, LogicalType::Varchar),
            column(AGGREGATE, 0, LogicalType::BigInt),
            column(GROUP, 2, LogicalType::Varchar),
        ],
    );
    let order_index = if order_by_payload { 1 } else { 2 };
    let order = OrderByNode {
        expression: column(
            OUTPUT,
            order_index,
            if order_by_payload {
                LogicalType::Varchar
            } else {
                LogicalType::BigInt
            },
        ),
        ascending: false,
        nulls_first: true,
    };
    let mut projection_plan = LogicalPlan::synthetic(LogicalOperator::Projection(projection));
    projection_plan.stats.estimated_cardinality =
        Some(paro_planner::plan::CardinalityEstimate::exact(10_000));
    LogicalPlan::synthetic(LogicalOperator::TopN(TopN::new(
        projection_plan,
        vec![order],
        20,
        0,
    )))
}

fn candidate_with_null_extended_source() -> LogicalPlan {
    let mut plan = candidate(false);
    let LogicalOperator::TopN(topn) = &mut plan.operator else {
        unreachable!()
    };
    let LogicalOperator::Projection(projection) = &mut topn.child.operator else {
        unreachable!()
    };
    let LogicalOperator::Aggregate(aggregate) = &mut projection.child.operator else {
        unreachable!()
    };
    let source = std::mem::replace(
        aggregate.child.as_mut(),
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let dimension = LogicalPlan::synthetic(LogicalOperator::Get(Get::new_without_table(
        11,
        vec!["key".to_string()],
        vec![LogicalType::BigInt],
    )));
    aggregate.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Join(
        Join::Comparison(ComparisonJoin::new(
            JoinType::Right,
            source,
            dimension,
            vec![JoinCondition::new(
                column(SOURCE, 0, LogicalType::BigInt),
                column(11, 0, LogicalType::BigInt),
                JoinComparisonType::Equal,
            )],
        )),
    )));
    plan
}

#[test]
fn bounded_topn_replaces_wide_dependent_groups_with_rowid() {
    let context = BindContext::new();
    let (optimized, changed) =
        optimize_plan(candidate(false), &context, &CostModel::default()).unwrap();
    assert!(changed);
    let LogicalOperator::Projection(output) = &optimized.operator else {
        panic!("expected late row-fetch projection")
    };
    let LogicalOperator::RowFetch(fetch) = &output.child.operator else {
        panic!("expected RowFetch below output projection")
    };
    assert_eq!(fetch.sources.len(), 1);
    let LogicalOperator::TopN(topn) = &fetch.child.operator else {
        panic!("expected TopN below row fetch")
    };
    let LogicalOperator::Projection(carrier) = &topn.child.operator else {
        panic!("expected narrow carrier")
    };
    assert_eq!(carrier.expressions.len(), 3, "key, sum, and rowid");
    let LogicalOperator::Aggregate(aggregate) = &carrier.child.operator else {
        panic!("expected aggregate")
    };
    assert_eq!(aggregate.groups.len(), 2, "key and rowid only");
    let get =
        super::late_payload::unique_get(aggregate.child.as_ref(), SOURCE).expect("source Get");
    assert_eq!(
        get.column_ids.last().copied(),
        Some(get.table.as_ref().unwrap().columns.len())
    );
}

#[test]
fn ordering_by_delayed_payload_keeps_preserving_plan() {
    let context = BindContext::new();
    let (optimized, changed) =
        optimize_plan(candidate(true), &context, &CostModel::default()).unwrap();
    assert!(!changed);
    assert!(matches!(optimized.operator, LogicalOperator::TopN(_)));
}

#[test]
fn null_extended_source_rowid_keeps_preserving_plan() {
    let context = BindContext::new();
    let (optimized, changed) = optimize_plan(
        candidate_with_null_extended_source(),
        &context,
        &CostModel::default(),
    )
    .unwrap();
    assert!(!changed);
    assert!(matches!(optimized.operator, LogicalOperator::TopN(_)));
}

#[test]
fn three_matching_gets_never_restore_false_uniqueness() {
    let source = || {
        let table = source_table();
        LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            SOURCE,
            table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            table
                .columns
                .iter()
                .map(|column| column.logical_type.clone())
                .collect(),
            table,
        )))
    };
    let two = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        source(),
        source(),
    ))));
    let three = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        two,
        source(),
    ))));

    assert!(super::late_payload::unique_get(&three, SOURCE).is_none());
}
