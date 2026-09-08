// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{
    CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, EdgeTableInfo,
    TableCatalogEntry,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConstantExpression, Expression, ReferenceExpression,
};
use paro_planner::operator::graph_expand::{ExpandDirection, GraphExpand};
use paro_planner::operator::join::{ComparisonJoin, JoinComparisonType, JoinCondition, JoinType};
use paro_planner::operator::{Filter, Get, Join, LogicalOperator, Projection, Window};
use paro_planner::plan::OwnedLogicalPlan;

use super::{
    hash_join_build_keys_are_declared_unique, plan_reduction_runtime_filter_fusion,
    remap_reduction_expression, resolve_base_get_column, ReductionPredicateBits,
};

fn declared_unique_get(ctx: &BindContext) -> OwnedLogicalPlan {
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
    OwnedLogicalPlan::new(
        ctx,
        LogicalOperator::Get(Box::new(Get {
            table_index: 7,
            returned_types: vec![LogicalType::Varchar, LogicalType::BigInt],
            names: vec!["payload".to_string(), "id".to_string()],
            relation_name: Some("unique_build".to_string()),
            relation_alias: None,
            column_sources: vec![
                paro_planner::operator::GetColumnSource::Stored { column_id: 0 },
                paro_planner::operator::GetColumnSource::Stored { column_id: 1 },
            ],
            column_types: vec![LogicalType::Varchar, LogicalType::BigInt],
            table: Some(table),
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        })),
    )
}

#[test]
fn unique_build_proof_resolves_physical_references_through_carriers() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let mut filter = Filter::new(get, Vec::new());
    filter.projection_map = vec![1, 0].into();
    let filter = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));
    let projection = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            8,
            filter,
            vec![Expression::Reference(
                ReferenceExpression::new(0, LogicalType::BigInt).into(),
            )],
        )),
    );
    let projection =
        crate::statistics::unique_keys::refresh_unique_keys(projection).expect("cache unique keys");
    let conditions = [JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
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
    assert!(hash_join_build_keys_are_declared_unique(
        &projection,
        &[conditions[0].clone(), conditions[0].clone()],
    ));
}

#[test]
fn unique_build_proof_propagates_through_windows() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let window = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Window(Window::new(9, Vec::new(), get)),
    );
    let window =
        crate::statistics::unique_keys::refresh_unique_keys(window).expect("cache unique keys");
    let conditions = [JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar).into()),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt).into()),
        JoinComparisonType::Equal,
    )];

    assert!(hash_join_build_keys_are_declared_unique(
        &window,
        &conditions
    ));
}

#[test]
fn graph_expand_does_not_promote_its_input_key_to_an_output_key() {
    let ctx = BindContext::new();
    let expand = GraphExpand::new(
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 2,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "vertices".to_string(),
            source_ref_column_ids: vec![0],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "vertices".to_string(),
            destination_ref_column_ids: vec![0],
            label: "e".to_string(),
            property_column_ids: Vec::new(),
        },
        ExpandDirection::Forward,
        "v".to_string(),
        7,
        8,
        9,
        10,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        declared_unique_get(&ctx),
    );
    let expanded = OwnedLogicalPlan::new(&ctx, LogicalOperator::GraphExpand(Box::new(expand)));
    let expanded =
        crate::statistics::unique_keys::refresh_unique_keys(expanded).expect("cache unique keys");
    let conditions = [JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar).into()),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt).into()),
        JoinComparisonType::Equal,
    )];

    assert!(!hash_join_build_keys_are_declared_unique(
        &expanded,
        &conditions,
    ));
}

#[test]
fn unique_build_proof_requires_a_key_preserving_join() {
    let ctx = BindContext::new();
    let mut left = declared_unique_get(&ctx);
    let LogicalOperator::Get(left_get) = &mut left.operator else {
        unreachable!("test source is a get")
    };
    left_get.table_index = 6;
    let right = declared_unique_get(&ctx);
    let mut multiplicative = ComparisonJoin::new(JoinType::Inner, left, right, Vec::new());
    multiplicative.left_projection_map = vec![1].into();
    multiplicative.right_projection_map = vec![1].into();
    let multiplicative = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::Comparison(multiplicative)),
    );
    let multiplicative = crate::statistics::unique_keys::refresh_unique_keys(multiplicative)
        .expect("cache unique keys");
    let outer_conditions = [JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
        JoinComparisonType::Equal,
    )];
    assert!(!hash_join_build_keys_are_declared_unique(
        &multiplicative,
        &outer_conditions
    ));

    let mut left = declared_unique_get(&ctx);
    let LogicalOperator::Get(left_get) = &mut left.operator else {
        unreachable!("test source is a get")
    };
    left_get.table_index = 6;
    let right = declared_unique_get(&ctx);
    let join_conditions = vec![JoinCondition::new(
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::operator::ColumnBinding::new(6, 1),
                LogicalType::BigInt,
            )
            .into(),
        ),
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::operator::ColumnBinding::new(7, 1),
                LogicalType::BigInt,
            )
            .into(),
        ),
        JoinComparisonType::Equal,
    )];
    let mut preserving = ComparisonJoin::new(JoinType::Inner, left, right, join_conditions);
    preserving.left_projection_map = vec![1].into();
    preserving.right_projection_map = vec![1].into();
    let preserving =
        OwnedLogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(preserving)));
    let preserving =
        crate::statistics::unique_keys::refresh_unique_keys(preserving).expect("cache unique keys");
    assert!(
        crate::statistics::unique_keys::expressions_cover_unique_key(
            &preserving,
            &[&outer_conditions[0].right],
        )
    );
    assert!(!hash_join_build_keys_are_declared_unique(
        &preserving,
        &outer_conditions
    ));
}

#[test]
fn integer_build_hint_traces_projected_outputs_through_inner_join_carriers() {
    let ctx = BindContext::new();
    let mut left = declared_unique_get(&ctx);
    let LogicalOperator::Get(left_get) = &mut left.operator else {
        unreachable!("test source is a get")
    };
    left_get.table_index = 6;
    let right = declared_unique_get(&ctx);
    let mut carrier = ComparisonJoin::new(JoinType::Inner, left, right, Vec::new());
    carrier.left_projection_map = vec![0].into();
    carrier.right_projection_map = vec![1].into();
    let carrier = OwnedLogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(carrier)));

    let key = Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt).into());
    let (get, column_id) = resolve_base_get_column(&carrier, &key)
        .expect("inner join output must retain its base-column lineage");
    assert_eq!(get.table_index, 7);
    assert_eq!(column_id, 1);
}

#[test]
fn unique_build_proof_declines_computed_keys_and_null_safe_equality() {
    let ctx = BindContext::new();
    let get = declared_unique_get(&ctx);
    let computed = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            8,
            get,
            vec![Expression::Constant(
                ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt).into(),
            )],
        )),
    );
    let computed =
        crate::statistics::unique_keys::refresh_unique_keys(computed).expect("cache unique keys");
    let mut condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into()),
        JoinComparisonType::Equal,
    );
    assert!(!hash_join_build_keys_are_declared_unique(
        &computed,
        std::slice::from_ref(&condition)
    ));

    let get = crate::statistics::unique_keys::refresh_unique_keys(declared_unique_get(&ctx))
        .expect("cache unique keys");
    condition.right =
        Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt).into());
    condition.comparison = JoinComparisonType::NotDistinctFrom;
    assert!(!hash_join_build_keys_are_declared_unique(
        &get,
        std::slice::from_ref(&condition)
    ));
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
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThanOrEqual,
                Expression::Reference(ReferenceExpression::new(index, LogicalType::BigInt).into()),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(value), LogicalType::BigInt).into(),
                ),
            )
            .into(),
        )
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
    let expression = Expression::ColumnRef(
        paro_planner::expression::ColumnRefExpression::with_depth(
            paro_planner::operator::ColumnBinding::new(7, 0),
            LogicalType::BigInt,
            1,
        )
        .into(),
    );
    assert!(remap_reduction_expression(&expression, &[3], 7, &[3], 9).is_none());
}
