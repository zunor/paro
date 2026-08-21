// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{
    CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, ConstantExpression, Expression, ReferenceExpression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ExpressionGet, Get, GroupInputMultiplicity, LogicalOperator,
    SingletonGroupProof,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::table::table_factory::TableFactory;

use super::{PhysicalNodeKind, PhysicalPlanGenerator, PlanBuildContext};

#[test]
fn stale_singleton_hint_falls_back_to_a_physical_aggregate() {
    let storage = Arc::new(
        TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("table storage"),
    );
    let table = Arc::new(
        TableCatalogEntry::from_info(
            CreateTableInfo::new(
                "paro".to_string(),
                "public".to_string(),
                "proof_source".to_string(),
                vec![ColumnDefinition::new(
                    "key".to_string(),
                    LogicalType::Integer,
                )],
            )
            .with_constraints(vec![Constraint::unique(vec![0])]),
            storage,
            CatalogObjectId::from_raw(10_002),
            0,
        )
        .expect("table catalog"),
    );
    let proof_get = Get::new(
        10,
        vec!["key".to_string()],
        vec![LogicalType::Integer],
        table,
    );
    let proof =
        SingletonGroupProof::from_null_free_declared_key(&proof_get, &[ColumnBinding::new(10, 0)])
            .expect("declared key witness");

    let ctx = BindContext::new();
    let child = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![vec![Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            ))]],
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        child,
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::Integer,
        ))],
        Vec::new(),
        vec![Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            Vec::new(),
            LogicalType::BigInt,
        ))],
        Vec::new(),
    );
    aggregate.group_input_multiplicity = GroupInputMultiplicity::AtMostOne(proof);
    let logical = LogicalPlan::new(&ctx, LogicalOperator::Aggregate(aggregate));

    let physical = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&logical)
        .expect("stale hint must lower conservatively");

    assert!(matches!(
        physical.node(physical.root).kind,
        PhysicalNodeKind::Aggregate(_)
    ));
}
