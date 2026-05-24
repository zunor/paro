// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tests for aggregate build sink helpers.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::memory::MemoryAccountingContext;
use paro_common::memory::MemoryOwner;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_function;
use paro_planner::expression::Expression;
use paro_planner::expression::{AggregateExpression, AggregateType, ReferenceExpression};

use crate::memory_runtime::QueryMemoryPool;
use crate::operators::aggregate::accounted_rows::{
    aggregate_modifier_memory_context, AccountedValueRow, AccountedValueRowSet,
};
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, create_hash_aggregate_tables, group_payload_refs, normalized_grouping_sets,
};
use crate::operators::aggregate::distinct_helpers::{
    collect_distinct_rows, finalize_distinct_into_tables, grouping_set_present_mask,
    populate_distinct_group_chunk,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHTScanPosition;
use crate::physical::specs::AggregateSpec;

fn modifier_memory(pool: Arc<QueryMemoryPool>) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = pool;
    aggregate_modifier_memory_context(owner)
}

fn accounted_row(values: Vec<Value>) -> AccountedValueRow {
    let pool = Arc::new(QueryMemoryPool::new(4096));
    AccountedValueRow::new(&modifier_memory(pool), values).expect("account row")
}

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn distinct_count_object() -> AggregateObject {
    let (function, _) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind count");
    AggregateObject {
        payload_size: function.state_size,
        child_count: 1,
        return_type: LogicalType::BigInt,
        function,
        bind_info: None,
        aggr_type: AggregateType::Distinct,
        filter: None,
        order_bys: Vec::new(),
    }
}

fn distinct_count_expression(input_idx: usize) -> Expression {
    let (function, _) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind count");
    Expression::Aggregate(
        AggregateExpression::new(
            function,
            vec![reference(input_idx, LogicalType::Integer)],
            LogicalType::BigInt,
        )
        .with_aggr_type(AggregateType::Distinct),
    )
}

fn distinct_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 0,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer]),
        groups: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([0])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        perfect_hash: None,
        output_names: Box::new([]),
        output_types: Box::new([]),
    }
}

fn grouped_distinct_grouping_set_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 2,
        projection_exprs: Box::new([]),
        payload_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ]),
        groups: Box::new([
            reference(0, LogicalType::Integer),
            reference(1, LogicalType::Integer),
        ]),
        grouping_sets: Box::new([Box::new([0])]),
        aggregates: Box::new([distinct_count_expression(2)]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([2])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        perfect_hash: None,
        output_names: Box::new(["g0".to_string(), "g1".to_string(), "count".to_string()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    }
}

#[test]
fn distinct_modifier_rows_are_query_memory_accounted() {
    let allocator = paro_common::test_utils::test_allocator();
    let mut payload =
        Chunk::try_initialize(&[LogicalType::Integer], 2, allocator).expect("payload");
    payload.set_cardinality(2);
    payload
        .column_mut(0)
        .expect("column")
        .set_value(0, &Value::Integer(11));
    payload
        .column_mut(0)
        .expect("column")
        .set_value(1, &Value::Integer(12));

    let spec = distinct_spec();
    let object = distinct_count_object();
    let pool = Arc::new(QueryMemoryPool::new(8));
    let mut distinct_sets = vec![None];

    let err = collect_distinct_rows(
        &spec,
        std::slice::from_ref(&object),
        &payload,
        &[],
        &modifier_memory(pool),
        &mut distinct_sets,
    )
    .expect_err("tiny query memory quota must reject DISTINCT rows");
    assert!(err.to_string().contains("distinct aggregate"));
}

#[test]
fn distinct_group_chunk_nulls_missing_grouping_set_keys() {
    let allocator = paro_common::test_utils::test_allocator();
    let rows = vec![
        accounted_row(vec![
            Value::Integer(1),
            Value::Integer(10),
            Value::Integer(100),
        ]),
        accounted_row(vec![
            Value::Integer(1),
            Value::Integer(20),
            Value::Integer(200),
        ]),
    ];
    let mut groups = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        rows.len(),
        allocator,
    )
    .expect("groups");

    let present_groups = grouping_set_present_mask(2, &[0], 0).expect("present groups");
    populate_distinct_group_chunk(&mut groups, &rows, &present_groups, 0).expect("populate groups");

    assert_eq!(groups.column(0).unwrap().get_value(0), Value::Integer(1));
    assert_eq!(groups.column(0).unwrap().get_value(1), Value::Integer(1));
    assert!(groups.column(1).unwrap().is_null(0));
    assert!(groups.column(1).unwrap().is_null(1));
}

#[test]
fn grouped_distinct_finalization_deduplicates_after_grouping_set_projection() {
    let allocator = paro_common::test_utils::test_allocator();
    let spec = grouped_distinct_grouping_set_spec();
    let objects = aggregate_objects(&spec).expect("aggregate objects");
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let mut tables = create_hash_aggregate_tables(
        &spec,
        allocator.clone(),
        modifier_memory(Arc::new(QueryMemoryPool::new(64 * 1024))),
    )
    .expect("aggregate tables");
    let memory = modifier_memory(Arc::new(QueryMemoryPool::new(64 * 1024)));
    let mut distinct = AccountedValueRowSet::new(memory.clone());
    distinct
        .insert(vec![
            Value::Integer(1),
            Value::Integer(10),
            Value::Integer(100),
        ])
        .expect("insert first row");
    distinct
        .insert(vec![
            Value::Integer(1),
            Value::Integer(20),
            Value::Integer(100),
        ])
        .expect("insert duplicate after grouping-set projection");
    distinct
        .insert(vec![
            Value::Integer(1),
            Value::Integer(20),
            Value::Integer(200),
        ])
        .expect("insert second distinct input");
    let mut distinct_sets = vec![Some(distinct)];

    finalize_distinct_into_tables(
        &spec,
        &objects,
        &group_refs,
        &grouping_sets,
        &memory,
        &mut distinct_sets,
        &mut tables,
    )
    .expect("finalize DISTINCT rows");

    let mut result = Chunk::try_initialize(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ],
        8,
        allocator,
    )
    .expect("result");
    let mut position = AggregateHTScanPosition::default();
    assert!(tables[0]
        .scan(&mut position, &mut result)
        .expect("scan result"));
    assert_eq!(result.size(), 1);
    assert_eq!(result.column(0).unwrap().get_value(0), Value::Integer(1));
    assert!(result.column(1).unwrap().is_null(0));
    assert_eq!(result.column(2).unwrap().get_value(0), Value::BigInt(2));
    assert!(!tables[0]
        .scan(&mut position, &mut result)
        .expect("scan complete"));
}

#[test]
fn hash_aggregate_table_init_respects_query_quota() {
    let spec = grouped_distinct_grouping_set_spec();
    let err = create_hash_aggregate_tables(
        &spec,
        paro_common::test_utils::test_allocator(),
        modifier_memory(Arc::new(QueryMemoryPool::new(1))),
    )
    .expect_err("tiny query quota must reject aggregate hash table metadata");

    assert!(err.to_string().contains("quota"));
}
