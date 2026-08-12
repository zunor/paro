// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tests for aggregate build sink helpers.

use std::collections::BTreeMap;
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
use crate::operators::aggregate::accounted_rows::aggregate_modifier_memory_context;
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, create_hash_aggregate_tables, group_payload_refs, normalized_grouping_sets,
};
use crate::operators::aggregate::distinct_helpers::{
    collect_distinct_rows, finalize_distinct_fragments_into_table, finalize_distinct_into_tables,
};
use crate::operators::aggregate::distinct_state::{DistinctAggregateState, DistinctKeyTable};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHTScanPosition;
use crate::physical::specs::{AggregateSpec, GroupKeyEncoding};

fn modifier_memory(pool: Arc<QueryMemoryPool>) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = pool;
    aggregate_modifier_memory_context(owner)
}

fn chunk_from_values(row_types: &[LogicalType], rows: Vec<Vec<Value>>) -> Chunk {
    let allocator = paro_common::test_utils::test_allocator();
    let mut chunk =
        Chunk::try_initialize(&row_types, rows.len().max(1), allocator.clone()).expect("chunk");
    chunk.set_cardinality(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            chunk
                .column_mut(col_idx)
                .expect("column")
                .set_value(row_idx, value);
        }
    }
    chunk
}

fn insert_distinct_values(
    state: &mut DistinctAggregateState,
    aggregate_idx: usize,
    group_key_count: usize,
    row_types: Vec<LogicalType>,
    rows: Vec<Vec<Value>>,
    memory: MemoryAccountingContext,
) {
    let allocator = paro_common::test_utils::test_allocator();
    let chunk = chunk_from_values(&row_types, rows);
    state
        .get_or_create(
            aggregate_idx,
            row_types,
            group_key_count,
            allocator,
            memory,
            1,
            crate::operators::aggregate::grouped_aggregate_hashtable::HashTableCapacityHint::default(),
        )
        .expect("distinct table")
        .insert(&chunk)
        .expect("insert distinct keys");
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
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer]),
        groups: Box::new([]),
        group_key_encodings: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([0])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: Box::new([]),
        output_types: Box::new([]),
    }
}

fn grouped_distinct_grouping_set_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 2,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
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
        group_key_encodings: Box::new([GroupKeyEncoding::Identity, GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([Box::new([0])]),
        aggregates: Box::new([distinct_count_expression(2)]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([2])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: Box::new(["g0".to_string(), "g1".to_string(), "count".to_string()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    }
}

fn grouped_distinct_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Varchar, LogicalType::Integer]),
        groups: Box::new([reference(0, LogicalType::Varchar)]),
        group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([distinct_count_expression(1)]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: Box::new(["group".to_string(), "count".to_string()]),
        output_types: Box::new([LogicalType::Varchar, LogicalType::BigInt]),
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
    let mut distinct = DistinctAggregateState::new(1);
    let mut groups = Chunk::try_new(payload.allocator().clone()).expect("groups");
    groups.set_cardinality(payload.size());

    let err = collect_distinct_rows(
        &spec,
        std::slice::from_ref(&object),
        &payload,
        &groups,
        1,
        8,
        &modifier_memory(pool),
        &mut distinct,
    )
    .expect_err("tiny query memory quota must reject DISTINCT rows");
    assert!(err.to_string().contains("quota"));
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
        modifier_memory(Arc::new(QueryMemoryPool::new(8 * 1024 * 1024))),
        1,
    )
    .expect("aggregate tables");
    let memory = modifier_memory(Arc::new(QueryMemoryPool::new(8 * 1024 * 1024)));
    let mut distinct_state = DistinctAggregateState::new(1);
    insert_distinct_values(
        &mut distinct_state,
        0,
        2,
        vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ],
        vec![
            vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)],
            vec![Value::Integer(1), Value::Integer(20), Value::Integer(100)],
            vec![Value::Integer(1), Value::Integer(20), Value::Integer(200)],
        ],
        memory.clone(),
    );

    finalize_distinct_into_tables(
        &spec,
        &objects,
        &group_refs,
        &grouping_sets,
        &memory,
        &mut distinct_state,
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
fn distinct_aggregate_state_unions_keys_across_local_states() {
    let pool = Arc::new(QueryMemoryPool::new(8 * 1024 * 1024));
    let memory = modifier_memory(pool);
    let mut global = DistinctAggregateState::new(1);
    let mut first = DistinctAggregateState::new(1);
    let mut second = DistinctAggregateState::new(1);
    insert_distinct_values(
        &mut first,
        0,
        0,
        vec![LogicalType::Integer],
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Null(LogicalType::Integer)],
        ],
        memory.clone(),
    );
    insert_distinct_values(
        &mut second,
        0,
        0,
        vec![LogicalType::Integer],
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Null(LogicalType::Integer)],
        ],
        memory,
    );

    global
        .merge_from(&mut first)
        .expect("merge first local state");
    global
        .merge_from(&mut second)
        .expect("merge second local state");

    let table = global
        .take_coalesced(0)
        .expect("global slot")
        .expect("global distinct table");
    assert_eq!(table.count(), 4);
    assert!(first.take_coalesced(0).expect("first slot").is_none());
    assert!(second.take_coalesced(0).expect("second slot").is_none());
}

#[test]
fn fragmented_distinct_finalization_probes_without_copying_duplicate_keys() {
    let allocator = paro_common::test_utils::test_allocator();
    let memory = modifier_memory(Arc::new(QueryMemoryPool::new(16 * 1024 * 1024)));
    let spec = grouped_distinct_spec();
    let objects = aggregate_objects(&spec).expect("aggregate objects");
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let mut tables = create_hash_aggregate_tables(&spec, allocator.clone(), memory.clone(), 1)
        .expect("aggregate tables");
    assert_eq!(tables.len(), 1);

    let key_types = vec![LogicalType::Varchar, LogicalType::Integer];
    let first_rows = vec![
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Integer(1),
        ],
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Integer(2),
        ],
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Null(LogicalType::Integer),
        ],
        vec![
            Value::Varchar("heap-backed-beta-group".to_string()),
            Value::Integer(7),
        ],
    ];
    let second_rows = vec![
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Integer(2),
        ],
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Integer(3),
        ],
        vec![
            Value::Varchar("heap-backed-alpha-group".to_string()),
            Value::Null(LogicalType::Integer),
        ],
        vec![
            Value::Varchar("heap-backed-beta-group".to_string()),
            Value::Integer(7),
        ],
        vec![
            Value::Varchar("heap-backed-beta-group".to_string()),
            Value::Integer(8),
        ],
    ];
    let mut fragments = Vec::new();
    for rows in [first_rows, second_rows] {
        let mut fragment = DistinctKeyTable::try_new(
            key_types.clone(),
            1,
            allocator.clone(),
            memory.clone(),
            1,
            crate::operators::aggregate::grouped_aggregate_hashtable::HashTableCapacityHint::default(),
        )
        .expect("distinct fragment");
        fragment
            .insert(&chunk_from_values(&key_types, rows))
            .expect("insert distinct fragment");
        fragments.push(fragment);
    }

    finalize_distinct_fragments_into_table(
        &spec,
        &objects,
        &group_refs,
        0,
        &fragments,
        &memory,
        &mut tables[0],
    )
    .expect("finalize fragmented DISTINCT");

    let mut output =
        Chunk::try_initialize(&[LogicalType::Varchar, LogicalType::BigInt], 8, allocator)
            .expect("output");
    let mut position = AggregateHTScanPosition::default();
    let mut counts = BTreeMap::new();
    while tables[0]
        .scan(&mut position, &mut output)
        .expect("scan aggregate")
    {
        for row_idx in 0..output.size() {
            let Value::Varchar(group) = output.column(0).unwrap().get_value(row_idx) else {
                panic!("expected VARCHAR group");
            };
            counts.insert(
                group,
                output
                    .column(1)
                    .unwrap()
                    .get_i64(row_idx)
                    .expect("count value"),
            );
        }
    }
    assert_eq!(
        counts,
        BTreeMap::from([
            ("heap-backed-alpha-group".to_string(), 3),
            ("heap-backed-beta-group".to_string(), 2),
        ])
    );
}

#[test]
fn hash_aggregate_table_init_respects_query_quota() {
    let spec = grouped_distinct_grouping_set_spec();
    let err = create_hash_aggregate_tables(
        &spec,
        paro_common::test_utils::test_allocator(),
        modifier_memory(Arc::new(QueryMemoryPool::new(1))),
        1,
    )
    .expect_err("tiny query quota must reject aggregate hash table metadata");

    assert!(err.to_string().contains("quota"));
}
