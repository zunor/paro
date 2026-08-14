// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{
    CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConstantExpression, Expression, ReferenceExpression,
};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};
use paro_planner::operator::{Filter, Get, LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;

use super::{
    hash_join_build_keys_are_declared_unique, plan_build_time_integer_join_index,
    plan_reduction_runtime_filter_fusion, remap_reduction_expression, resolve_base_get_column,
    ReductionPredicateBits,
};

fn declared_unique_get(ctx: &BindContext) -> LogicalPlan {
    let storage = Arc::new(
        paro_storage::table::table_factory::TableFactory::default()
            .create_table(&[LogicalType::Varchar, LogicalType::BigInt])
            .expect("table storage"),
    );
    storage
        .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_string_vector(&["row"]),
            paro_common::test_utils::test_i64_vector(&[42]),
        ]))
        .expect("seed storage statistics");
    let table = Arc::new(
        TableCatalogEntry::from_info(
            CreateTableInfo::new(
                "paro".to_string(),
                "public".to_string(),
                "unique_build".to_string(),
                vec![
                    ColumnDefinition::new("payload".to_string(), LogicalType::Varchar),
                    ColumnDefinition::new("id".to_string(), LogicalType::BigInt),
                ],
            )
            .with_constraints(vec![Constraint::unique(vec![1])]),
            storage,
            CatalogObjectId::from_raw(91_001),
            0,
        )
        .expect("unique table catalog entry"),
    );
    LogicalPlan::new(
        ctx,
        LogicalOperator::Get(Get {
            table_index: 7,
            returned_types: vec![LogicalType::Varchar, LogicalType::BigInt],
            names: vec!["payload".to_string(), "id".to_string()],
            relation_name: Some("unique_build".to_string()),
            relation_alias: None,
            column_ids: vec![0, 1],
            column_types: vec![LogicalType::Varchar, LogicalType::BigInt],
            table: Some(table),
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        }),
    )
}

#[test]
fn unique_build_proof_resolves_physical_references_through_carriers() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let mut filter = Filter::new(get, Vec::new());
    filter.projection_map = vec![1, 0].into();
    let filter = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));
    let projection = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            8,
            filter,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            ))],
        )),
    );
    let conditions = [JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        JoinComparisonType::Equal,
    )];

    let (get, column_id) = resolve_base_get_column(&projection, &conditions[0].right)
        .expect("physical key must trace to its base column");
    assert_eq!(get.table_index, 7);
    assert_eq!(column_id, 1);
    assert!(hash_join_build_keys_are_declared_unique(
        &projection,
        &conditions
    ));
}

#[test]
fn unique_build_proof_declines_computed_keys_and_null_safe_equality() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let computed = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            8,
            get,
            vec![Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            ))],
        )),
    );
    let mut condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        JoinComparisonType::Equal,
    );
    assert!(!hash_join_build_keys_are_declared_unique(
        &computed,
        std::slice::from_ref(&condition)
    ));

    let get = declared_unique_get(&ctx);
    condition.right = Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt));
    condition.comparison = JoinComparisonType::NotDistinctFrom;
    assert!(!hash_join_build_keys_are_declared_unique(
        &get,
        std::slice::from_ref(&condition)
    ));
}

#[test]
fn build_time_integer_index_uses_storage_bounds_without_a_uniqueness_proof() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt)),
        JoinComparisonType::Equal,
    );
    let planned = plan_build_time_integer_join_index(&get, std::slice::from_ref(&condition))
        .expect("base integer column has guaranteed storage bounds");
    assert!(planned.estimated_rows > 0);

    let computed = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            8,
            get,
            vec![Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            ))],
        )),
    );
    assert!(
        plan_build_time_integer_join_index(&computed, std::slice::from_ref(&condition)).is_none()
    );
}

#[test]
fn build_and_source_predicates_share_one_collision_free_namespace() {
    let mut bits = ReductionPredicateBits::default();
    let build_residual = bits.allocate().unwrap();
    // A duplicate build residual reuses its existing bit and therefore
    // does not consume the allocator. The next source predicate must still
    // receive a distinct bit.
    let duplicate_build_residual = build_residual;
    let source_predicate = bits.allocate().unwrap();

    assert_eq!(duplicate_build_residual, build_residual);
    assert_ne!(source_predicate, build_residual);
    assert_eq!(source_predicate, 0b10);
    for _ in 2..u8::BITS {
        assert!(bits.allocate().is_some());
    }
    assert_eq!(bits.allocate(), None);
}

#[test]
fn branch_runtime_filters_require_one_shared_pruning_contract() {
    fn bound(index: usize, value: i64) -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThanOrEqual,
            Expression::Reference(ReferenceExpression::new(index, LogicalType::BigInt)),
            Expression::Constant(ConstantExpression::new(
                Value::BigInt(value),
                LogicalType::BigInt,
            )),
        ))
    }
    let shared = vec![bound(0, 10), bound(1, 11)];
    let merged = plan_reduction_runtime_filter_fusion(
        vec![
            Some(shared.clone()),
            Some(vec![shared[1].clone(), shared[0].clone()]),
        ],
        16,
        32,
    )
    .unwrap();
    assert_eq!(merged.len(), 2);
    assert!(merged
        .iter()
        .zip(&shared)
        .all(|(left, right)| left.equals(right)));
    assert!(plan_reduction_runtime_filter_fusion(
        vec![Some(vec![bound(0, 10)]), Some(vec![bound(0, 20)])],
        16,
        32,
    )
    .is_some_and(|filters| filters.len() == 1));
    assert!(
        plan_reduction_runtime_filter_fusion(vec![Some(Vec::new()), Some(Vec::new())], 16, 32,)
            .is_some_and(|filters| filters.is_empty())
    );
    assert!(plan_reduction_runtime_filter_fusion(
        vec![Some(vec![bound(0, 10)]), Some(vec![bound(0, 20)])],
        16,
        16,
    )
    .is_none());
}

#[test]
fn reduction_remap_rejects_correlated_source_bindings() {
    let expression =
        Expression::ColumnRef(paro_planner::expression::ColumnRefExpression::with_depth(
            paro_planner::operator::ColumnBinding::new(7, 0),
            LogicalType::BigInt,
            1,
        ));
    assert!(remap_reduction_expression(&expression, &[3], 7, &[3], 9).is_none());
}
