// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
use paro_storage::buffer::BufferPool;
use std::sync::Arc;

fn create_test_buffer_pool() -> Arc<BufferPool> {
    BufferPool::new_arc(64 * 1024 * 1024)
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

fn build_hash_table(
    join_type: JoinType,
    build_keys: &[i32],
    build_payload: &[i32],
) -> JoinHashTable {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![equality_condition()],
        vec![LogicalType::Integer],
        join_type,
        JoinHashTableConfig::default(),
    );

    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                build_keys,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                build_payload,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    ht.build(&keys, &payload).unwrap();
    ht.finalize().unwrap();
    ht
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

fn build_hash_table_from_optional(
    join_type: JoinType,
    condition: JoinCondition,
    build_keys: &[Option<i32>],
    build_payload: &[i32],
) -> JoinHashTable {
    let ht = JoinHashTable::new(
        create_test_buffer_pool(),
        paro_common::test_utils::test_allocator(),
        vec![condition],
        vec![LogicalType::Integer],
        join_type,
        JoinHashTableConfig::default(),
    );

    let keys = chunk_from_optional_i32(build_keys);
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                build_payload,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    ht.build(&keys, &payload).unwrap();
    ht.finalize().unwrap();
    ht
}

fn prepare_probe(ht: &JoinHashTable, probe_keys: &[i32]) -> (ScanStructure, Chunk, Chunk) {
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(
            paro_common::test_utils::test_i32_vector_with_allocator(
                probe_keys,
                paro_common::test_utils::test_allocator(),
            ),
        )],
        paro_common::test_utils::test_allocator(),
    );
    let left = keys.clone();
    let mut scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");
    ht.probe(&keys, &mut scan, None, keys.size()).unwrap();
    (scan, keys, left)
}

#[test]
fn test_scan_structure_new() {
    let ss = ScanStructure::try_new(16, paro_common::test_utils::test_allocator())
        .expect("test scan structure allocation failed");
    assert_eq!(ss.count, 0);
    assert!(ss.is_null);
    assert!(!ss.finished);
    assert_eq!(ss.pointer_offset, 16);
}

#[test]
fn test_scan_structure_reset() {
    let mut ss = ScanStructure::try_new(16, paro_common::test_utils::test_allocator())
        .expect("test scan structure allocation failed");
    ss.count = 5;
    ss.finished = true;
    ss.is_null = false;
    ss.found_match[0] = true;

    ss.reset();

    assert_eq!(ss.count, 0);
    assert!(!ss.is_null);
    assert!(!ss.finished);
    assert!(!ss.found_match[0]);
}

#[test]
fn test_pointers_exhausted() {
    let mut ss = ScanStructure::try_new(16, paro_common::test_utils::test_allocator())
        .expect("test scan structure allocation failed");
    assert!(ss.pointers_exhausted());

    ss.count = 1;
    assert!(!ss.pointers_exhausted());
}

#[test]
fn test_next_left_join_emits_matches_then_unmatched_rows() {
    let ht = build_hash_table(JoinType::Left, &[1, 2], &[10, 20]);
    let (mut scan, keys, left) = prepare_probe(&ht, &[1, 3]);
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        2,
    );

    let first = scan
        .next_left_join(&keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(first, 1);
    assert_eq!(result.data[0].get_value(0).to_string(), "1");
    assert_eq!(result.data[1].get_value(0).to_string(), "10");

    let second = scan
        .next_left_join(&keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(second, 1);
    assert_eq!(result.data[0].get_value(0).to_string(), "3");
    assert!(result.data[1].is_null(0));
}

#[test]
fn test_next_semi_anti_and_mark_join() {
    let ht = build_hash_table(JoinType::Inner, &[1, 2], &[10, 20]);

    let (mut semi_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
    let mut semi_result =
        paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 2);
    let semi_count = semi_scan
        .next_semi_join(&keys, &left, &mut semi_result, &ht, &[])
        .unwrap();
    assert_eq!(semi_count, 1);
    assert_eq!(semi_result.data[0].get_value(0).to_string(), "1");

    let (mut anti_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
    let mut anti_result =
        paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 2);
    let anti_count = anti_scan
        .next_anti_join(&keys, &left, &mut anti_result, &ht, &[])
        .unwrap();
    assert_eq!(anti_count, 1);
    assert_eq!(anti_result.data[0].get_value(0).to_string(), "3");

    let (mut mark_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
    let mut mark_result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Boolean],
        2,
    );
    let mark_count = mark_scan
        .next_mark_join(&keys, &left, &mut mark_result, &ht, &[])
        .unwrap();
    assert_eq!(mark_count, 2);
    assert_eq!(mark_result.data[1].get_value(0).to_string(), "true");
    assert_eq!(mark_result.data[1].get_value(1).to_string(), "false");
}

#[test]
fn test_not_distinct_from_semi_and_anti_join_respect_null_matches() {
    let ht = build_hash_table_from_optional(
        JoinType::Semi,
        not_distinct_condition(),
        &[None, Some(2)],
        &[10, 20],
    );

    let keys = chunk_from_optional_i32(&[None, Some(1), Some(2)]);
    let left = keys.clone();

    let mut semi_scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");
    ht.probe(&keys, &mut semi_scan, None, keys.size()).unwrap();
    let mut semi_result =
        paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 3);
    let semi_count = semi_scan
        .next_semi_join(&keys, &left, &mut semi_result, &ht, &[])
        .unwrap();
    assert_eq!(semi_count, 2);
    assert!(semi_result.data[0].is_null(0));
    assert_eq!(semi_result.data[0].get_value(1).to_string(), "2");

    let mut anti_scan = ht
        .create_scan_structure()
        .expect("test scan structure allocation failed");
    ht.probe(&keys, &mut anti_scan, None, keys.size()).unwrap();
    let mut anti_result =
        paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 3);
    let anti_count = anti_scan
        .next_anti_join(&keys, &left, &mut anti_result, &ht, &[])
        .unwrap();
    assert_eq!(anti_count, 1);
    assert_eq!(anti_result.data[0].get_value(0).to_string(), "1");
}

#[test]
fn test_next_single_join_null_fills_unmatched_rows() {
    let ht = build_hash_table(JoinType::Single, &[1, 2], &[10, 20]);
    let (mut scan, keys, left) = prepare_probe(&ht, &[1, 3]);
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        2,
    );

    let count = scan
        .next_single_join(&keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(result.data[0].get_value(0).to_string(), "1");
    assert_eq!(result.data[1].get_value(0).to_string(), "10");
    assert_eq!(result.data[0].get_value(1).to_string(), "3");
    assert!(result.data[1].is_null(1));
}

#[test]
fn test_next_single_join_errors_on_duplicates() {
    let ht = build_hash_table(JoinType::Single, &[1, 1], &[10, 11]);
    let (mut scan, keys, left) = prepare_probe(&ht, &[1]);
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        1,
    );

    let err = scan
        .next_single_join(&keys, &left, &mut result, &ht, &[], &[])
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("More than one row returned by a SINGLE join"));
}

#[test]
fn test_next_single_join_drains_probe_larger_than_output_vector() {
    let row_count = VECTOR_SIZE * 2;
    let keys = (0..row_count as i32).collect::<Vec<_>>();
    let payload = keys.iter().map(|key| key * 10).collect::<Vec<_>>();
    let ht = build_hash_table(JoinType::Single, &keys, &payload);
    let (mut scan, probe_keys, left) = prepare_probe(&ht, &keys);
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        VECTOR_SIZE,
    );

    let first = scan
        .next_single_join(&probe_keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(first, VECTOR_SIZE);
    assert!(!scan.finished);
    assert_eq!(result.data[0].get_value(0), Value::Integer(0));
    assert_eq!(
        result.data[0].get_value(VECTOR_SIZE - 1),
        Value::Integer((VECTOR_SIZE - 1) as i32)
    );

    let second = scan
        .next_single_join(&probe_keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(second, VECTOR_SIZE);
    assert!(scan.finished);
    assert_eq!(
        result.data[0].get_value(0),
        Value::Integer(VECTOR_SIZE as i32)
    );
    assert_eq!(
        result.data[1].get_value(VECTOR_SIZE - 1),
        Value::Integer((row_count as i32 - 1) * 10)
    );
}

#[test]
fn test_next_right_semi_or_anti_join_marks_build_rows() {
    let ht = build_hash_table(JoinType::RightSemi, &[1, 2], &[10, 20]);
    let (mut scan, keys, _left) = prepare_probe(&ht, &[1]);

    let count = scan.next_right_semi_or_anti_join(&keys, &ht).unwrap();
    assert_eq!(count, 0);

    let mut matched_state = ht.create_full_outer_scan_state();
    let mut matched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let matched_count = ht
        .scan_full_outer(&mut matched_state, true, &mut matched)
        .unwrap();
    assert_eq!(matched_count, 1);
    assert_eq!(matched.data[0].get_value(0).to_string(), "10");

    let mut unmatched_state = ht.create_full_outer_scan_state();
    let mut unmatched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let unmatched_count = ht
        .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
        .unwrap();
    assert_eq!(unmatched_count, 1);
    assert_eq!(unmatched.data[0].get_value(0).to_string(), "20");
}

#[test]
fn test_next_right_semi_or_anti_join_marks_all_duplicate_build_rows() {
    let ht = build_hash_table(JoinType::RightSemi, &[3, 3], &[30, 31]);
    let (mut scan, keys, _left) = prepare_probe(&ht, &[3, 3]);

    let count = scan.next_right_semi_or_anti_join(&keys, &ht).unwrap();
    assert_eq!(count, 0);

    let mut matched_state = ht.create_full_outer_scan_state();
    let mut matched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let matched_count = ht
        .scan_full_outer(&mut matched_state, true, &mut matched)
        .unwrap();
    assert_eq!(matched_count, 2);
    assert_eq!(matched.data[0].get_value(0).to_string(), "30");
    assert_eq!(matched.data[0].get_value(1).to_string(), "31");

    let mut unmatched_state = ht.create_full_outer_scan_state();
    let mut unmatched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let unmatched_count = ht
        .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
        .unwrap();
    assert_eq!(unmatched_count, 0);
}

#[test]
fn test_next_inner_join_marks_build_rows_for_right_join_source_scan() {
    let ht = build_hash_table(JoinType::Right, &[1, 2], &[10, 20]);
    let (mut scan, keys, left) = prepare_probe(&ht, &[1]);
    let mut result = paro_common::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Integer],
        1,
    );

    let count = scan
        .next_inner_join(&keys, &left, &mut result, &ht, &[], &[])
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(result.data[0].get_value(0).to_string(), "1");
    assert_eq!(result.data[1].get_value(0).to_string(), "10");

    let mut unmatched_state = ht.create_full_outer_scan_state();
    let mut unmatched = Chunk::try_new(paro_common::test_utils::test_allocator())
        .expect("test chunk allocation failed");
    let unmatched_count = ht
        .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
        .unwrap();
    assert_eq!(unmatched_count, 1);
    assert_eq!(unmatched.data[0].get_value(0).to_string(), "20");
}
