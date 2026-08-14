use super::*;
use paro_common::allocator::MemoryTag;
use paro_common::memory::{MemoryDomain, MemoryOwner};
use paro_common::vector::VECTOR_SIZE;
use paro_planner::expression::{ConstantExpression, Expression};
use paro_storage::buffer::BufferPool;

use crate::join_hashtable::hash_kernel::JoinKeyLayout;
use crate::join_hashtable::ht_entry::HtEntry;
use crate::memory_runtime::QueryMemoryPool;

fn create_test_buffer_pool() -> Arc<BufferPool> {
    BufferPool::new_arc(64 * 1024 * 1024) // 64MB
}

fn equality_condition() -> JoinCondition {
    JoinCondition::new(
        Expression::Constant(ConstantExpression::new(
            Value::Integer(1),
            LogicalType::Integer,
        )),
        Expression::Constant(ConstantExpression::new(
            Value::Integer(1),
            LogicalType::Integer,
        )),
        JoinComparisonType::Equal,
    )
}

fn bigint_equality_condition() -> JoinCondition {
    JoinCondition::new(
        Expression::Constant(ConstantExpression::new(
            Value::BigInt(1),
            LogicalType::BigInt,
        )),
        Expression::Constant(ConstantExpression::new(
            Value::BigInt(1),
            LogicalType::BigInt,
        )),
        JoinComparisonType::Equal,
    )
}

fn bigint_pair_equality_conditions() -> Vec<JoinCondition> {
    vec![bigint_equality_condition(), bigint_equality_condition()]
}

fn not_distinct_condition() -> JoinCondition {
    JoinCondition::new(
        Expression::Constant(ConstantExpression::new(
            Value::Integer(1),
            LogicalType::Integer,
        )),
        Expression::Constant(ConstantExpression::new(
            Value::Integer(1),
            LogicalType::Integer,
        )),
        JoinComparisonType::NotDistinctFrom,
    )
}

fn chunk_from_optional_i32(values: &[Option<i32>]) -> Chunk {
    let mut chunk =
        paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], values.len());
    for (row_idx, value) in values.iter().enumerate() {
        let column = chunk.column_mut(0).expect("column must exist");
        match value {
            Some(value) => column.set_value(row_idx, &Value::Integer(*value)),
            None => column.set_value(row_idx, &Value::Null(LogicalType::Integer)),
        }
    }
    chunk.set_cardinality(values.len());
    chunk
}

fn chunk_from_optional_i64_columns(columns: &[&[Option<i64>]]) -> Chunk {
    let row_count = columns.first().map_or(0, |values| values.len());
    assert!(columns.iter().all(|values| values.len() == row_count));
    let types = vec![LogicalType::BigInt; columns.len()];
    let mut chunk = paro_common::test_utils::test_chunk_with_capacity(&types, row_count.max(1));
    for (column_idx, values) in columns.iter().enumerate() {
        for (row_idx, value) in values.iter().enumerate() {
            let value = value
                .map(Value::BigInt)
                .unwrap_or(Value::Null(LogicalType::BigInt));
            chunk
                .column_mut(column_idx)
                .expect("column must exist")
                .set_value(row_idx, &value);
        }
    }
    chunk.set_cardinality(row_count);
    chunk
}

#[test]
fn grouped_reduction_mode_is_fixed_when_the_key_index_is_finalized() {
    let table = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![bigint_equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    table.configure_grouped_reduction_extrema(2).unwrap();
    let keys = chunk_from_optional_i64_columns(&[&[Some(0), Some(100)]]);
    let payload = chunk_from_optional_i32(&[Some(1), Some(2)]);
    table.build(&keys, &payload).unwrap();
    table.finalize().unwrap();

    assert!(table.grouped_reduction_extrema().is_some());
    assert!(table.configure_grouped_reduction_extrema(2).is_ok());
    assert!(table.configure_grouped_reduction_extrema(1).is_err());
}

#[test]
fn generic_hash_grouped_reduction_unavailability_is_a_stable_finalized_mode() {
    let mut condition = bigint_equality_condition();
    condition.comparison = JoinComparisonType::NotDistinctFrom;
    let table = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![condition],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    table.configure_grouped_reduction_extrema(2).unwrap();
    let keys = chunk_from_optional_i64_columns(&[&[Some(0), Some(1)]]);
    let payload = chunk_from_optional_i32(&[Some(1), Some(2)]);
    table.build(&keys, &payload).unwrap();
    table.finalize().unwrap();

    assert!(table.grouped_reduction_extrema().is_none());
    assert!(table.configure_grouped_reduction_extrema(2).is_ok());
    assert!(table.configure_grouped_reduction_extrema(1).is_err());
}

#[test]
fn nullable_i64_pair_fast_matcher_rejects_null_build_key() {
    let table = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![bigint_equality_condition(), bigint_equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Right,
        JoinHashTableConfig::default(),
    );
    let build_keys = chunk_from_optional_i64_columns(&[&[None], &[Some(0)]]);
    let payload = chunk_from_optional_i32(&[Some(42)]);
    table.build(&build_keys, &payload).unwrap();

    let probe_keys = chunk_from_optional_i64_columns(&[&[Some(0)], &[Some(0)]]);
    let prepared = table.prepare_probe_keys(&probe_keys).unwrap();
    let build_row = table.all_build_row_ptrs()[0];

    assert!(!table.key_values_match_build_row(&prepared, 0, build_row));
}

#[test]
fn join_hash_table_build_store_respects_query_quota() {
    let pool = Arc::new(QueryMemoryPool::new(1));
    let owner: Arc<dyn MemoryOwner> = pool;
    let memory = MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    );
    let table = JoinHashTable::new_with_memory(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
        memory,
    );
    let keys = chunk_from_optional_i32(&[Some(1), Some(2)]);
    let payload = chunk_from_optional_i32(&[Some(10), Some(20)]);

    let err = table
        .build(&keys, &payload)
        .expect_err("tiny query quota must reject hash join build storage");
    assert!(err.to_string().contains("quota"));
}

#[test]
fn data_collection_reset_clears_deferred_hash_state() {
    let table = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    let keys = chunk_from_optional_i32(&[Some(1), Some(2)]);
    let payload = chunk_from_optional_i32(&[Some(10), Some(20)]);

    table.build(&keys, &payload).unwrap();
    assert!(table.deferred_hashes.load(Ordering::Acquire));

    table.reset_data_collection();

    assert!(!table.deferred_hashes.load(Ordering::Acquire));
    assert_eq!(table.count(), 0);
    assert_eq!(table.build_rows_size_in_bytes(), 0);
}

fn find_linear_probe_collision_pair() -> (i32, i32) {
    let layout = JoinKeyLayout::new(&[LogicalType::Integer], &[JoinComparisonType::Equal], false);
    let values = (0..10_000).collect::<Vec<i32>>();
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &values,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let hashes = (0..values.len())
        .map(|row_idx| layout.hash_key_at(&keys, row_idx))
        .collect::<Vec<_>>();
    for left in 0..values.len() {
        let left_hash = hashes[left];
        for right in (left + 1)..values.len() {
            let right_hash = hashes[right];
            if (left_hash as usize & 15) == (right_hash as usize & 15)
                && (left_hash & HtEntry::SALT_MASK) != (right_hash & HtEntry::SALT_MASK)
            {
                return (values[left], values[right]);
            }
        }
    }
    panic!("failed to find collision pair with different salts");
}

#[test]
fn test_join_hash_table_new() {
    let buffer_pool = create_test_buffer_pool();
    let conditions = vec![];
    let build_types = vec![LogicalType::Integer, LogicalType::Varchar];

    let ht = JoinHashTable::new(
        buffer_pool,
        paro_common::test_utils::test_allocator(),
        conditions,
        build_types,
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    assert_eq!(ht.count(), 0);
    assert!(ht.is_empty());
    assert!(!ht.finalized.load(Ordering::Relaxed));
}

#[test]
fn test_calculate_capacity() {
    assert_eq!(JoinHashTable::calculate_capacity(0), 16);
    assert_eq!(JoinHashTable::calculate_capacity(10), 32);
    assert_eq!(JoinHashTable::calculate_capacity(100), 256);
    assert_eq!(JoinHashTable::calculate_capacity(1000), 2048);
}

#[test]
fn test_propagates_build_side() {
    assert!(!JoinHashTable::propagates_build_side(JoinType::Inner));
    assert!(!JoinHashTable::propagates_build_side(JoinType::Left));
    assert!(JoinHashTable::propagates_build_side(JoinType::Right));
    assert!(JoinHashTable::propagates_build_side(JoinType::Outer));
    assert!(!JoinHashTable::propagates_build_side(JoinType::Semi));
    assert!(!JoinHashTable::propagates_build_side(JoinType::Anti));
}

#[test]
fn test_finalize() {
    let buffer_pool = create_test_buffer_pool();
    let ht = JoinHashTable::new(
        buffer_pool,
        paro_common::test_utils::test_allocator(),
        vec![],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    assert!(!ht.finalized.load(Ordering::Relaxed));
    ht.finalize().unwrap();
    assert!(ht.finalized.load(Ordering::Relaxed));
}

#[test]
fn bounded_unique_integer_inner_join_uses_exact_index() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    let keys = chunk_from_optional_i32(&[Some(-2), Some(0), Some(3)]);
    let payload = chunk_from_optional_i32(&[Some(20), Some(30), Some(40)]);
    ht.build(&keys, &payload).expect("build keys");
    ht.finalize().expect("finalize join");
    assert!(ht.has_integer_index());

    let probe_keys =
        chunk_from_optional_i32(&[Some(-3), Some(-2), None, Some(0), Some(2), Some(3)]);
    let mut scan = ht.create_scan_structure().expect("scan state");
    ht.probe(&probe_keys, &mut scan, None, probe_keys.size())
        .expect("probe integer index");
    assert_eq!(scan.count, 3);
    assert_eq!(scan.sel_vector.as_slice(), &[1, 3, 5]);
}

#[test]
fn integer_index_is_filled_during_parallel_build_and_only_published_at_finish() {
    let memory = MemoryAccountingContext::detached(
        MemoryTag::HashTable,
        MemoryAccountingClass::NonRevocable,
    );
    let builder = Arc::new(
        ConcurrentBuildTimeIntegerIndexBuilder::try_new_from_values(
            &LogicalType::Integer,
            &Value::Integer(1),
            &Value::Integer(4),
            4,
            paro_common::test_utils::test_allocator(),
            &memory,
        )
        .expect("direct builder admission")
        .expect("compact integer domain"),
    );
    let config = JoinHashTableConfig {
        build_keys_unique: true,
        build_time_integer_builder: Some(Arc::clone(&builder)),
        ..Default::default()
    };
    let first = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        config.clone(),
    ));
    let second = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        config,
    ));
    first
        .build(
            &chunk_from_optional_i32(&[Some(1), Some(3)]),
            &chunk_from_optional_i32(&[Some(10), Some(30)]),
        )
        .expect("first local build");
    second
        .build(
            &chunk_from_optional_i32(&[Some(2), Some(4)]),
            &chunk_from_optional_i32(&[Some(20), Some(40)]),
        )
        .expect("second local build");

    let merged = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig {
            build_keys_unique: true,
            ..Default::default()
        },
    ));
    merged.merge(first).expect("merge first local");
    merged.merge(second).expect("merge second local");
    assert!(!merged.has_integer_index());

    let builder = Arc::try_unwrap(builder).expect("local tables released builder references");
    merged
        .publish_build_time_integer_builder(builder)
        .expect("publish build-time index");
    assert!(merged.has_integer_index());

    let probe = chunk_from_optional_i32(&[Some(4), Some(2), Some(9)]);
    let mut scan = merged.create_scan_structure().expect("scan state");
    merged
        .probe(&probe, &mut scan, None, probe.size())
        .expect("probe published build-time index");
    assert_eq!(scan.sel_vector.as_slice(), &[0, 1]);
}

#[test]
fn ranked_build_time_index_links_duplicates_across_parallel_local_tables() {
    let memory = MemoryAccountingContext::detached(
        MemoryTag::HashTable,
        MemoryAccountingClass::NonRevocable,
    );
    // A 1,001-value domain for four maximum rows declines the direct layout
    // (over 24 slots/row) but admits the compact ranked representation (under
    // 256 slots/row).
    let builder = Arc::new(
        ConcurrentBuildTimeIntegerIndexBuilder::try_new_from_values(
            &LogicalType::Integer,
            &Value::Integer(0),
            &Value::Integer(1_000),
            4,
            paro_common::test_utils::test_allocator(),
            &memory,
        )
        .expect("ranked builder admission")
        .expect("bounded sparse integer domain"),
    );
    let config = JoinHashTableConfig {
        build_time_integer_builder: Some(Arc::clone(&builder)),
        ..Default::default()
    };
    let first = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        config.clone(),
    ));
    let second = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        config,
    ));
    first
        .build(
            &chunk_from_optional_i32(&[Some(900), Some(10)]),
            &chunk_from_optional_i32(&[Some(90), Some(10)]),
        )
        .expect("first local build");
    second
        .build(
            &chunk_from_optional_i32(&[Some(500), Some(900)]),
            &chunk_from_optional_i32(&[Some(50), Some(91)]),
        )
        .expect("second local build");

    let merged = Arc::new(JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    ));
    // Reverse local creation order to make build-record ordinals differ from
    // merged-store ordinals. Probe chains must remain pointer-based and exact.
    merged.merge(second).expect("merge second local first");
    merged.merge(first).expect("merge first local second");
    let builder = Arc::try_unwrap(builder).expect("local tables released builder references");
    merged
        .publish_build_time_integer_builder(builder)
        .expect("publish ranked build-time index");
    assert!(merged.has_integer_index());
    assert!(merged.chains_longer_than_one.load(Ordering::Relaxed));

    let probe = chunk_from_optional_i32(&[Some(900), Some(500), Some(11)]);
    let mut scan = merged.create_scan_structure().expect("scan state");
    merged
        .probe(&probe, &mut scan, None, probe.size())
        .expect("probe ranked build-time index");
    assert_eq!(scan.sel_vector.as_slice(), &[0, 1]);
    let mut output = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
        paro_common::test_utils::test_allocator(),
    )
    .expect("output chunk");
    let count = scan
        .next_inner_join(&probe, &probe, &mut output, &merged, &[0])
        .expect("scan duplicate matches");
    assert_eq!(count, 3);
    let mut payloads = (0..count)
        .map(|row| output.column(1).unwrap().get_i32(row).unwrap())
        .collect::<Vec<_>>();
    payloads.sort_unstable();
    assert_eq!(payloads, [50, 90, 91]);
}

#[test]
fn duplicate_direct_integer_build_uses_exact_index_chains() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    let key_chunk = chunk_from_optional_i32(&[Some(10), Some(10)]);
    let payload = chunk_from_optional_i32(&[Some(20), Some(30)]);
    ht.build(&key_chunk, &payload).expect("build keys");
    ht.finalize().expect("finalize join");
    assert!(ht.has_integer_index());
    assert!(ht.chains_longer_than_one.load(Ordering::Relaxed));

    let probe = chunk_from_optional_i32(&[Some(10)]);
    let mut scan = ht.create_scan_structure().expect("scan state");
    ht.probe(&probe, &mut scan, None, probe.size())
        .expect("probe exact duplicate chain");
    let mut output = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
        paro_common::test_utils::test_allocator(),
    )
    .expect("output chunk");
    let count = scan
        .next_inner_join(&probe, &probe, &mut output, &ht, &[0])
        .expect("scan duplicate matches");
    assert_eq!(count, 2);
}

#[test]
fn bigint_pair_build_uses_exact_index_and_preserves_duplicate_chains() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        bigint_pair_equality_conditions(),
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    let keys = chunk_from_optional_i64_columns(&[
        &[Some(1), Some(1), Some(7)],
        &[Some(2), Some(2), Some(9)],
    ]);
    let payload = chunk_from_optional_i32(&[Some(10), Some(20), Some(30)]);
    ht.build(&keys, &payload).expect("build pair keys");
    ht.finalize().expect("finalize pair index");
    assert!(ht.has_pair_integer_index());
    assert!(ht.chains_longer_than_one.load(Ordering::Relaxed));

    let probe = chunk_from_optional_i64_columns(&[
        &[Some(1), Some(7), Some(7), None],
        &[Some(2), Some(8), Some(9), Some(2)],
    ]);
    let mut scan = ht.create_scan_structure().expect("scan state");
    ht.probe(&probe, &mut scan, None, probe.size())
        .expect("probe pair index");
    assert_eq!(scan.count, 2);
    assert_eq!(scan.sel_vector.as_slice(), &[0, 2]);

    let mut output = Chunk::try_initialize(
        &[LogicalType::BigInt, LogicalType::Integer],
        VECTOR_SIZE,
        paro_common::test_utils::test_allocator(),
    )
    .expect("output chunk");
    let count = scan
        .next_inner_join(&probe, &probe, &mut output, &ht, &[0])
        .expect("scan pair matches");
    assert_eq!(count, 3);
    let mut payloads = (0..count)
        .map(|row| output.column(1).unwrap().get_i32(row).unwrap())
        .collect::<Vec<_>>();
    payloads.sort_unstable();
    assert_eq!(payloads, [10, 20, 30]);
}

#[test]
fn sparse_integer_build_falls_back_to_hash_index() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    let key_chunk = chunk_from_optional_i32(&[Some(0), Some(10_000)]);
    let payload = chunk_from_optional_i32(&[Some(20), Some(30)]);
    ht.build(&key_chunk, &payload).expect("build keys");
    ht.finalize().expect("finalize join");
    assert!(!ht.has_integer_index());
    assert!(!ht.probe_entries.load(Ordering::Acquire).is_null());
}

#[test]
fn test_right_join_layout_tracks_found_flag_offset() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Right,
        JoinHashTableConfig::default(),
    );

    assert!(ht.has_found_flag());
    assert_eq!(ht.found_flag_column_index, Some(2));
    assert!(ht.found_flag_offset.is_some());
}

#[test]
fn test_scan_full_outer_uses_found_flag_filter() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Right,
        JoinHashTableConfig::default(),
    );

    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );

    ht.build(&keys, &payload).unwrap();
    ht.finalize().unwrap();

    let row_ptrs = ht.all_build_row_ptrs();
    assert_eq!(row_ptrs.len(), 3);

    for (row_ptr, found) in row_ptrs.iter().copied().zip([false, true, false]) {
        ht.set_build_side_found(row_ptr, found);
        let stored = ht.build_side_found(row_ptr).unwrap();
        assert_eq!(stored, found);
    }

    let mut unmatched_state = ht.create_full_outer_scan_state();
    let mut unmatched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let unmatched_count = ht
        .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
        .unwrap();
    assert_eq!(unmatched_count, 2);
    assert_eq!(unmatched.data[0].get_value(0).to_string(), "10");
    assert_eq!(unmatched.data[0].get_value(1).to_string(), "30");

    let mut matched_state = ht.create_full_outer_scan_state();
    let mut matched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let matched_count = ht
        .scan_full_outer(&mut matched_state, true, &mut matched)
        .unwrap();
    assert_eq!(matched_count, 1);
    assert_eq!(matched.data[0].get_value(0).to_string(), "20");
}

#[test]
fn test_build_filters_null_keys_for_equal_conditions() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    let keys = chunk_from_optional_i32(&[Some(1), None, Some(2)]);
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );

    ht.build(&keys, &payload).unwrap();

    assert_eq!(ht.count(), 2);
    assert!(ht.has_null.load(Ordering::Relaxed));
}

#[test]
fn test_not_distinct_from_keeps_null_keys_and_probe_matches_them() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![not_distinct_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    let build_keys = chunk_from_optional_i32(&[None]);
    let build_payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[99],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );

    ht.build(&build_keys, &build_payload).unwrap();
    ht.finalize().unwrap();

    assert_eq!(ht.count(), 1);
    assert!(!ht.has_null.load(Ordering::Relaxed));

    let probe_keys = chunk_from_optional_i32(&[None]);
    let left = probe_keys.clone();
    let mut scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");
    ht.probe(&probe_keys, &mut scan, None, probe_keys.size())
        .unwrap();

    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
    );
    let count = scan
        .next_inner_join(&probe_keys, &left, &mut result, &ht, &[0])
        .unwrap();

    assert_eq!(count, 1);
    assert!(result.data[0].is_null(0));
    assert_eq!(result.data[1].get_value(0).to_string(), "99");
}

#[test]
fn test_probe_respects_selected_count() {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[3],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[30],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    ht.build(&keys, &payload).unwrap();
    ht.finalize().unwrap();

    let probe_keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[3, 3],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let probe_sel = paro_common::test_utils::test_selection(vec![1, 0]);
    let mut scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");

    ht.probe(&probe_keys, &mut scan, Some(&probe_sel), 1)
        .unwrap();

    assert_eq!(scan.count, 1);
    assert_eq!(scan.sel_vector.get(0), 1);
}

#[test]
fn test_probe_linear_probing_finds_rows_behind_salt_mismatch() {
    let (first_key, second_key) = find_linear_probe_collision_pair();
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );

    let build_keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[first_key, second_key],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let build_payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[11, 22],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    ht.build(&build_keys, &build_payload).unwrap();
    ht.finalize().unwrap();

    let probe_keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &[second_key],
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let left = probe_keys.clone();
    let mut scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");
    ht.probe(&probe_keys, &mut scan, None, probe_keys.size())
        .unwrap();

    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
    );
    let count = scan
        .next_inner_join(&probe_keys, &left, &mut result, &ht, &[0])
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(
        result.data[0].get_value(0).to_string(),
        second_key.to_string()
    );
    assert_eq!(result.data[1].get_value(0).to_string(), "22");
}

#[test]
fn inner_join_drains_probe_matches_larger_than_one_output_vector() {
    let row_count = VECTOR_SIZE * 2;
    let values = (0..row_count as i32).collect::<Vec<_>>();
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                &values,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let table = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
    );
    table.build(&keys, &keys).unwrap();
    table.finalize().unwrap();

    let mut scan = table.create_scan_structure().unwrap();
    table.probe(&keys, &mut scan, None, keys.size()).unwrap();
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
    );
    let mut emitted = 0;
    while !scan.finished {
        emitted += scan
            .next_inner_join(&keys, &keys, &mut result, &table, &[0])
            .unwrap();
    }

    assert_eq!(emitted, row_count);
}
