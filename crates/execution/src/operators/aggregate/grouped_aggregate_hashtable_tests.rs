// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::Cell;
use std::collections::HashMap;
use std::mem::size_of;
use std::thread_local;

use paro_common::runtime_value::Value;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::AggregateFunction;
use paro_planner::expression::{
    AggregateExpression, AggregateType, Expression, ReferenceExpression,
};

thread_local! {
    static DESTRUCTOR_CALLS: Cell<usize> = const { Cell::new(0) };
}

fn reset_destructor_calls() {
    DESTRUCTOR_CALLS.with(|calls| calls.set(0));
}

fn record_destructor_calls(count: usize) {
    DESTRUCTOR_CALLS.with(|calls| calls.set(calls.get() + count));
}

fn detached_table_memory() -> MemoryAccountingContext {
    MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable)
}

fn destructor_calls() -> usize {
    DESTRUCTOR_CALLS.with(Cell::get)
}

#[test]
fn lookup_entries_only_pay_for_inline_keys_when_supported() {
    assert_eq!(size_of::<AggregateHTEntry>(), size_of::<u64>());

    let varlen = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar],
        Vec::new(),
        Vec::new(),
        paro_common::test_utils::test_allocator(),
    )
    .expect("varlen table");
    assert!(varlen.inline_keys.is_none());
    assert_eq!(
        varlen.lookup_memory_usage(),
        varlen.capacity() * size_of::<AggregateHTEntry>()
    );

    let inline = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        Vec::new(),
        Vec::new(),
        paro_common::test_utils::test_allocator(),
    )
    .expect("inline table");
    assert!(inline.inline_keys.is_some());
    assert_eq!(
        inline.lookup_memory_usage(),
        inline.capacity() * (size_of::<AggregateHTEntry>() + size_of::<InlineKey>())
    );
}

#[test]
fn dictionary_varlen_groups_share_owned_heap_bytes() {
    let allocator = paro_common::test_utils::test_allocator();
    let value = "shared-dictionary-group-value";
    let child = Arc::new(paro_common::test_utils::test_string_vector_with_allocator(
        &[value],
        allocator.clone(),
    ));
    let selection = SelectionVector::try_from_indices(vec![0, 0, 0], allocator.clone())
        .expect("dictionary selection");
    let groups = Chunk::from_arc_vectors(
        vec![
            Arc::new(Vector::try_dictionary(child, selection).expect("dictionary group")),
            Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )),
        ],
        allocator,
    );
    let hashes = hash_group_columns(&groups).expect("group hashes");
    let mut table = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar, LogicalType::Integer],
        Vec::new(),
        Vec::new(),
        groups.allocator().clone(),
    )
    .expect("group table");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());

    table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("insert groups");

    assert_eq!(table.count(), 3);
    assert_eq!(table.varlen_heap.len(), value.len());
}

unsafe fn sum_initialize(state: *mut u8) {
    *(state as *mut i64) = 0;
}

unsafe fn sum_update(
    inputs: &[&Vector],
    _input_data: &AggregateInputData,
    states: &AggregateStateInput,
    count: usize,
) {
    let input = inputs[0].try_decode_ref(count).unwrap();
    let input_data = input.get_data::<i64>();
    for row in 0..count {
        let input_row = input.sel().get(row);
        if !input.validity().is_valid(input_row) {
            continue;
        }
        let state_ptr = states.state_ptr(row) as *mut i64;
        *state_ptr += *input_data.add(input_row);
    }
}

unsafe fn sum_combine(
    source: &Vector,
    target: &Vector,
    _input_data: &AggregateInputData,
    count: usize,
) {
    let source_format = source.try_decode_ref(count).unwrap();
    let target_format = target.try_decode_ref(count).unwrap();
    let source_data = source_format.get_data::<*mut u8>();
    let target_data = target_format.get_data::<*mut u8>();
    for row in 0..count {
        let source_idx = source_format.sel().get(row);
        let target_idx = target_format.sel().get(row);
        let source_ptr = *source_data.add(source_idx) as *const i64;
        let target_ptr = *target_data.add(target_idx) as *mut i64;
        *target_ptr += *source_ptr;
    }
}

unsafe fn sum_finalize(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    let state = states.try_decode_ref(count).unwrap();
    let state_data = state.get_data::<*mut u8>();
    let result_data = result.flat_data_mut::<i64>();
    for row in 0..count {
        let state_idx = state.sel().get(row);
        let state_ptr = *state_data.add(state_idx) as *const i64;
        *result_data.add(row) = *state_ptr;
    }
    Ok(())
}

unsafe fn sum_destructor(_states: &Vector, _input_data: &AggregateInputData, count: usize) {
    record_destructor_calls(count);
}

fn make_sum_object() -> AggregateObject {
    let function = AggregateFunction::new(
        "test_sum".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        size_of::<i64>(),
        sum_initialize,
        sum_update,
        sum_combine,
        sum_finalize,
        None,
        Some(sum_destructor),
    );
    let bound = AggregateExpression::new(
        function,
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::BigInt,
        ))],
        LogicalType::BigInt,
    )
    .with_aggr_type(AggregateType::NonDistinct);
    AggregateObject::from_bound(&bound).expect("aggregate object")
}

fn make_count_star_object() -> AggregateObject {
    let bound =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt)
            .with_aggr_type(AggregateType::NonDistinct);
    AggregateObject::from_bound(&bound).expect("count star object")
}

fn collect_scan_rows(table: &mut GroupedAggregateHashTable) -> Vec<Vec<Value>> {
    let mut types = table.layout.group_types.clone();
    types.extend(table.aggregate_return_types.clone());
    let mut position = HTScanPosition::default();
    let mut chunk = paro_common::test_utils::test_chunk_with_capacity(&types, VECTOR_SIZE);
    let mut rows = Vec::new();
    while table
        .scan(&mut position, &mut chunk)
        .expect("scan result chunk")
    {
        for row in 0..chunk.size() {
            let mut values = Vec::with_capacity(chunk.column_count());
            for col in 0..chunk.column_count() {
                values.push(chunk.column(col).expect("result column").get_value(row));
            }
            rows.push(values);
        }
    }
    rows
}

fn build_map_from_scan(rows: Vec<Vec<Value>>) -> HashMap<i32, i64> {
    let mut result = HashMap::new();
    for row in rows {
        let key = match row.first().expect("group key value") {
            Value::Integer(v) => *v,
            other => panic!("unexpected key value in scan output: {other:?}"),
        };
        let value = match row.get(1).expect("aggregate value") {
            Value::BigInt(v) => *v,
            other => panic!("unexpected aggregate value in scan output: {other:?}"),
        };
        result.insert(key, value);
    }
    result
}

#[test]
fn grouped_hash_table_find_create_and_update() {
    let mut table = GroupedAggregateHashTable::with_capacity(
        vec![LogicalType::Integer],
        vec![make_sum_object()],
        vec![vec![0]],
        8,
        paro_common::test_utils::test_allocator(),
        detached_table_memory(),
    )
    .expect("create grouped hash table");

    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 1, 3, 2],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    let new_group_count = table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("find/create groups");
    assert_eq!(new_group_count, 3);
    assert_eq!(new_groups.as_slice(), &[0, 1, 3]);

    let payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[10, 20, 5, 7, 8],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    table
        .update_aggregates(&payload, &addresses, None)
        .expect("update aggregates");
    assert_eq!(table.count(), 3);

    let actual = build_map_from_scan(collect_scan_rows(&mut table));
    let expected = HashMap::from([(1, 15), (2, 28), (3, 7)]);
    assert_eq!(actual, expected);
}

#[test]
fn grouped_hash_table_update_with_filter() {
    let mut table = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_sum_object()],
        vec![vec![0]],
        paro_common::test_utils::test_allocator(),
    )
    .expect("create grouped hash table");

    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("find/create");

    let payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let filter = paro_common::test_utils::test_selection(vec![0, 2]);
    table
        .update_aggregates(&payload, &addresses, Some(&filter))
        .expect("filtered update");

    let actual = build_map_from_scan(collect_scan_rows(&mut table));
    let expected = HashMap::from([(1, 10), (2, 0), (3, 30)]);
    assert_eq!(actual, expected);
}

#[test]
fn grouped_hash_table_varlen_and_null_group_keys() {
    let mut table = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer, LogicalType::Varchar],
        vec![make_sum_object()],
        vec![vec![0]],
        paro_common::test_utils::test_allocator(),
    )
    .expect("create grouped hash table");

    let mut strings = paro_common::test_utils::test_string_vector_with_allocator(
        &["a", "n", "a", "b", "b"],
        paro_common::test_utils::test_allocator(),
    );
    strings.set_null(1, true);
    let groups = Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 1, 1, 2, 2],
                paro_common::test_utils::test_allocator(),
            ),
            strings,
        ],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("find/create");
    let payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[1, 2, 3, 4, 5],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    table
        .update_aggregates(&payload, &addresses, None)
        .expect("update");

    let rows = collect_scan_rows(&mut table);
    let mut actual: HashMap<(i32, Option<String>), i64> = HashMap::new();
    for row in rows {
        let key0 = match &row[0] {
            Value::Integer(v) => *v,
            other => panic!("unexpected integer group key: {other:?}"),
        };
        let key1 = match &row[1] {
            Value::Varchar(v) => Some(v.clone()),
            Value::Null(_) => None,
            other => panic!("unexpected varchar group key: {other:?}"),
        };
        let sum = match &row[2] {
            Value::BigInt(v) => *v,
            other => panic!("unexpected aggregate sum: {other:?}"),
        };
        actual.insert((key0, key1), sum);
    }
    let expected = HashMap::from([
        ((1, Some("a".to_string())), 4),
        ((1, None), 2),
        ((2, Some("b".to_string())), 9),
    ]);
    assert_eq!(actual, expected);
}

#[test]
fn serialized_prefix_projection_coalesces_only_adjacent_equal_runs() {
    let allocator = paro_common::test_utils::test_allocator();
    let alpha = "an alpha group key that lives outside the inline string";
    let beta = "a beta group key that also lives outside the inline string";
    let mut group_values = paro_common::test_utils::test_string_vector_with_allocator(
        &[alpha, alpha, beta, beta, alpha, "null one", "null two"],
        allocator.clone(),
    );
    group_values.set_null(5, true);
    group_values.set_null(6, true);
    let source_chunk = Chunk::from_vectors(
        vec![
            group_values,
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 11, 20, 21, 12, 30, 31],
                allocator.clone(),
            ),
        ],
        allocator.clone(),
    );
    let mut source = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar, LogicalType::Integer],
        Vec::new(),
        Vec::new(),
        allocator.clone(),
    )
    .expect("source table");
    let source_hashes = source.hash_groups(&source_chunk).expect("source hashes");
    let mut source_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 7);
    let mut source_new_groups = paro_common::test_utils::test_selection_with_capacity(7);
    source
        .find_or_create_groups(
            &source_chunk,
            &source_hashes,
            &mut source_addresses,
            &mut source_new_groups,
        )
        .expect("insert distinct source rows");
    assert_eq!(source.count(), 7);

    let mut run_starts = paro_common::test_utils::test_selection_with_capacity(7);
    let mut prefix_hashes =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::UBigInt, 7);
    let run_count = source
        .project_serialized_group_prefix_runs(
            0,
            source.count(),
            1,
            &mut run_starts,
            &mut prefix_hashes,
        )
        .expect("project prefix runs");
    assert_eq!(run_count, 4);
    assert_eq!(run_starts.as_slice(), &[0, 2, 4, 5]);
    assert_eq!(prefix_hashes.len(), run_count);
    assert_eq!(
        prefix_hashes.as_slice::<u64>()[0],
        prefix_hashes.as_slice::<u64>()[2],
        "separated runs of the same group must retain the same lookup hash"
    );

    let mut target = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar],
        Vec::new(),
        Vec::new(),
        allocator,
    )
    .expect("target table");
    let mut target_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, run_count);
    target
        .find_or_create_serialized_group_prefix(
            &source,
            SerializedSourceRows::new(0, run_starts.as_slice()),
            &prefix_hashes.as_slice::<u64>()[..run_count],
            &mut target_addresses,
        )
        .expect("lookup projected run heads");
    assert_eq!(target.count(), 3);
    assert_eq!(
        target_addresses.get_i64(0),
        target_addresses.get_i64(2),
        "non-adjacent runs of one group must resolve to the same state"
    );
}

#[test]
fn grouped_hash_table_resizes_and_reuses_entries() {
    let mut table = GroupedAggregateHashTable::with_capacity(
        vec![LogicalType::Integer],
        vec![],
        vec![],
        8,
        paro_common::test_utils::test_allocator(),
        detached_table_memory(),
    )
    .expect("create grouped hash table");

    let values = (0..50).map(|v| v as i32).collect::<Vec<_>>();
    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &values,
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    let new_group_count = table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("first insertion");
    assert_eq!(new_group_count, 50);
    assert_eq!(table.count(), 50);
    assert!(table.capacity() > 8);

    let base_memory = table.memory_usage();
    let mut probe_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut probe_new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    let second_new = table
        .find_or_create_groups(
            &groups,
            &hashes,
            &mut probe_addresses,
            &mut probe_new_groups,
        )
        .expect("second probe");
    assert_eq!(second_new, 0);
    assert_eq!(table.count(), 50);
    assert_eq!(probe_new_groups.len(), 0);
    assert!(table.memory_usage() >= base_memory);
}

#[test]
fn grouped_hash_table_reclaims_finalized_lookup_storage_without_breaking_scan() {
    let mut table = GroupedAggregateHashTable::with_capacity(
        vec![LogicalType::Integer],
        vec![],
        vec![],
        8,
        paro_common::test_utils::test_allocator(),
        detached_table_memory(),
    )
    .expect("create grouped hash table");

    let values = (0..50).map(|v| v as i32).collect::<Vec<_>>();
    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &values,
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("insert groups");
    assert_eq!(table.count(), 50);
    assert!(table.capacity() > 0);

    let before = table.external_accounted_memory_usage();
    let reclaimable = table.reclaimable_finalized_memory();
    assert!(
        reclaimable >= table.capacity() * size_of::<AggregateHTEntry>(),
        "finalized lookup entries should be reclaimable"
    );

    let reclaimed = table.reclaim_finalized_memory(1);
    assert!(reclaimed > 0);
    assert_eq!(table.capacity(), 0);
    assert!(table.external_accounted_memory_usage() < before);

    let mut scanned = collect_scan_rows(&mut table)
        .into_iter()
        .map(|row| match row.first().expect("group key") {
            Value::Integer(value) => *value,
            other => panic!("unexpected group key after lookup release: {other:?}"),
        })
        .collect::<Vec<_>>();
    scanned.sort_unstable();
    assert_eq!(scanned, values);
}

#[test]
fn grouped_hash_table_combines_other_table() {
    let mut left = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_sum_object()],
        vec![vec![0]],
        paro_common::test_utils::test_allocator(),
    )
    .expect("left table");
    let mut right = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_sum_object()],
        vec![vec![0]],
        paro_common::test_utils::test_allocator(),
    )
    .expect("right table");

    let left_groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let left_hashes = left.hash_groups(&left_groups).expect("left hashes");
    let mut left_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, left_groups.size());
    let mut left_new_groups =
        paro_common::test_utils::test_selection_with_capacity(left_groups.size());
    left.find_or_create_groups(
        &left_groups,
        &left_hashes,
        &mut left_addresses,
        &mut left_new_groups,
    )
    .expect("left find/create");
    let left_payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[10, 20],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    left.update_aggregates(&left_payload, &left_addresses, None)
        .expect("left update");

    let right_groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[2, 3, 2],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let right_hashes = right.hash_groups(&right_groups).expect("right hashes");
    let mut right_addresses = paro_common::test_utils::test_vector_with_capacity(
        LogicalType::BigInt,
        right_groups.size(),
    );
    let mut right_new_groups =
        paro_common::test_utils::test_selection_with_capacity(right_groups.size());
    right
        .find_or_create_groups(
            &right_groups,
            &right_hashes,
            &mut right_addresses,
            &mut right_new_groups,
        )
        .expect("right find/create");
    let right_payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[7, 8, 1],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    right
        .update_aggregates(&right_payload, &right_addresses, None)
        .expect("right update");

    left.combine(&mut right)
        .expect("combine grouped hash tables");
    let actual = build_map_from_scan(collect_scan_rows(&mut left));
    let expected = HashMap::from([(1, 10), (2, 28), (3, 8)]);
    assert_eq!(actual, expected);
}

#[test]
fn grouped_hash_table_combines_heap_backed_and_null_varlen_keys() {
    let mut left = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar],
        vec![],
        vec![],
        paro_common::test_utils::test_allocator(),
    )
    .expect("left table");
    let mut right = GroupedAggregateHashTable::new(
        vec![LogicalType::Varchar],
        vec![],
        vec![],
        paro_common::test_utils::test_allocator(),
    )
    .expect("right table");

    for (table, values) in [
        (
            &mut left,
            [
                "a shared group key that lives in the varlen heap",
                "a left-only group key that lives in the varlen heap",
                "left null placeholder",
            ],
        ),
        (
            &mut right,
            [
                "a shared group key that lives in the varlen heap",
                "a right-only group key that lives in the varlen heap",
                "right null placeholder",
            ],
        ),
    ] {
        let mut strings = paro_common::test_utils::test_string_vector_with_allocator(
            &values,
            paro_common::test_utils::test_allocator(),
        );
        strings.set_null(2, true);
        let groups = Chunk::from_vectors(vec![strings], paro_common::test_utils::test_allocator());
        let hashes = table.hash_groups(&groups).expect("group hashes");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("insert groups");
    }

    left.combine(&mut right)
        .expect("combine grouped varlen tables");
    let mut actual = collect_scan_rows(&mut left)
        .into_iter()
        .map(|row| match row.first().expect("group key") {
            Value::Varchar(value) => Some(value.clone()),
            Value::Null(LogicalType::Varchar) => None,
            other => panic!("unexpected group key: {other:?}"),
        })
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = vec![
        None,
        Some("a left-only group key that lives in the varlen heap".to_string()),
        Some("a right-only group key that lives in the varlen heap".to_string()),
        Some("a shared group key that lives in the varlen heap".to_string()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn grouped_hash_table_combines_count_star_low_cardinality_batch() {
    let mut left = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_count_star_object()],
        vec![Vec::new()],
        paro_common::test_utils::test_allocator(),
    )
    .expect("left table");
    let mut right = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_count_star_object()],
        vec![Vec::new()],
        paro_common::test_utils::test_allocator(),
    )
    .expect("right table");

    let values = (1..=1000).map(|i| i % 10).collect::<Vec<i32>>();
    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &values,
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = right.hash_groups(&groups).expect("right hashes");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    right
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("right find/create");
    let mut payload = Chunk::try_initialize(
        &[],
        groups.size(),
        paro_common::test_utils::test_allocator(),
    )
    .expect("payload chunk");
    payload.set_cardinality(groups.size());
    right
        .update_aggregates(&payload, &addresses, None)
        .expect("right update");

    left.combine(&mut right)
        .expect("combine grouped count star table");
    let actual = build_map_from_scan(collect_scan_rows(&mut left));
    let expected = (0..10)
        .map(|group| (group, 100))
        .collect::<HashMap<i32, i64>>();
    assert_eq!(actual, expected);
}

#[test]
fn serialized_prefix_projection_reuses_varlen_and_null_groups() {
    let allocator = paro_common::test_utils::test_allocator();
    let group_types = vec![LogicalType::Varchar, LogicalType::Integer];
    let source_types = vec![
        LogicalType::Varchar,
        LogicalType::Integer,
        LogicalType::BigInt,
    ];
    let long_key = "a heap-backed group key shared by source and target";

    let mut target = GroupedAggregateHashTable::new(
        group_types.clone(),
        Vec::new(),
        Vec::new(),
        allocator.clone(),
    )
    .expect("target table");
    let mut target_groups =
        Chunk::try_initialize(&group_types, 2, allocator.clone()).expect("target groups");
    target_groups.set_cardinality(2);
    target_groups
        .column_mut(0)
        .expect("target string")
        .set_value(0, &Value::Varchar(long_key.to_string()));
    target_groups
        .column_mut(0)
        .expect("target string")
        .set_value(1, &Value::Null(LogicalType::Varchar));
    target_groups
        .column_mut(1)
        .expect("target integer")
        .set_value(0, &Value::Integer(7));
    target_groups
        .column_mut(1)
        .expect("target integer")
        .set_value(1, &Value::Integer(8));
    let target_hashes = target.hash_groups(&target_groups).expect("target hashes");
    let mut target_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);
    let mut target_new_groups = paro_common::test_utils::test_selection_with_capacity(2);
    target
        .find_or_create_groups(
            &target_groups,
            &target_hashes,
            &mut target_addresses,
            &mut target_new_groups,
        )
        .expect("insert target groups");

    let mut source = GroupedAggregateHashTable::new(
        source_types.clone(),
        Vec::new(),
        Vec::new(),
        allocator.clone(),
    )
    .expect("source table");
    let mut source_groups =
        Chunk::try_initialize(&source_types, 4, allocator.clone()).expect("source groups");
    source_groups.set_cardinality(4);
    for (row_idx, value) in [long_key, long_key, "null placeholder", "new group"]
        .into_iter()
        .enumerate()
    {
        source_groups
            .column_mut(0)
            .expect("source string")
            .set_value(row_idx, &Value::Varchar(value.to_string()));
    }
    source_groups
        .column_mut(0)
        .expect("source string")
        .set_value(2, &Value::Null(LogicalType::Varchar));
    for (row_idx, value) in [7, 7, 8, 9].into_iter().enumerate() {
        source_groups
            .column_mut(1)
            .expect("source integer")
            .set_value(row_idx, &Value::Integer(value));
    }
    for (row_idx, value) in [100_i64, 200, 300, 400].into_iter().enumerate() {
        source_groups
            .column_mut(2)
            .expect("source input")
            .set_value(row_idx, &Value::BigInt(value));
    }
    let source_hashes = source.hash_groups(&source_groups).expect("source hashes");
    let mut source_addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);
    let mut source_new_groups = paro_common::test_utils::test_selection_with_capacity(4);
    source
        .find_or_create_groups(
            &source_groups,
            &source_hashes,
            &mut source_addresses,
            &mut source_new_groups,
        )
        .expect("insert source keys");

    let mut run_starts = paro_common::test_utils::test_selection_with_capacity(4);
    let mut projected_hashes =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::UBigInt, 4);
    let run_count = source
        .project_serialized_group_prefix_runs(0, 4, 2, &mut run_starts, &mut projected_hashes)
        .expect("project source prefix runs");
    assert_eq!(run_count, 3);
    assert_eq!(run_starts.as_slice(), &[0, 2, 3]);
    assert_eq!(projected_hashes.get_u64(0), target_hashes.get_u64(0));
    assert_eq!(projected_hashes.get_u64(1), target_hashes.get_u64(1));
    target
        .find_or_create_serialized_group_prefix(
            &source,
            SerializedSourceRows::new(0, run_starts.as_slice()),
            &projected_hashes.as_slice::<u64>()[..run_count],
            &mut target_addresses,
        )
        .expect("project source prefixes");

    assert_eq!(target.count(), 3);
    assert_ne!(target_addresses.get_i64(0), target_addresses.get_i64(2));
    assert_ne!(target_addresses.get_i64(1), target_addresses.get_i64(2));
}

#[test]
fn grouped_hash_table_destroy_calls_destructor() {
    reset_destructor_calls();

    let mut table = GroupedAggregateHashTable::new(
        vec![LogicalType::Integer],
        vec![make_sum_object()],
        vec![vec![0]],
        paro_common::test_utils::test_allocator(),
    )
    .expect("create grouped hash table");

    let groups = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = table.hash_groups(&groups).expect("hash groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
    table
        .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
        .expect("find/create groups");
    let payload = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i64_vector_with_allocator(
            &[4, 5, 6],
            paro_common::test_utils::test_allocator(),
        )],
        paro_common::test_utils::test_allocator(),
    );
    table
        .update_aggregates(&payload, &addresses, None)
        .expect("update aggregates");

    let before_destroy = table.memory_usage();
    table.destroy().expect("destroy hash table");
    assert_eq!(table.count(), 0);
    assert_eq!(destructor_calls(), 3);
    assert!(table.memory_usage() <= before_destroy);
}
