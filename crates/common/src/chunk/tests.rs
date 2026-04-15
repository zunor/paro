// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::runtime_value::Value;
use crate::types::LogicalType;
use crate::vector::{AllocationSet, SelectionVector, Vector, VectorType, VECTOR_SIZE};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

fn make_int_string_chunk(ids: &[i32], labels: &[&str]) -> Chunk {
    Chunk::from_vectors(vec![Vector::from_i32(ids), Vector::from_strings(labels)])
}

#[test]
fn test_empty_chunk() {
    let chunk = Chunk::new();
    assert!(chunk.is_empty());
    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.capacity(), VECTOR_SIZE);
}

#[test]
fn test_initialize() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let chunk = Chunk::initialize(&types, 1024);
    assert_eq!(chunk.column_count(), 2);
    assert!(chunk.is_empty());
    assert_eq!(chunk.capacity(), 1024);
}

#[test]
fn test_init_empty() {
    let types = vec![LogicalType::BigInt, LogicalType::Boolean];
    let chunk = Chunk::init_empty(&types);
    assert_eq!(chunk.column_count(), 2);
    assert_eq!(chunk.types(), types);
}

#[test]
fn test_from_vectors() {
    let v1 = Vector::from_i32(&[1, 2, 3]);
    let v2 = Vector::from_strings(&["a", "b", "c"]);

    let chunk = Chunk::from_vectors(vec![v1, v2]);
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column_count(), 2);

    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(chunk.column(1).unwrap().get_string(2), Some("c"));
}

#[test]
fn test_set_cardinality() {
    let types = vec![LogicalType::Integer];
    let mut chunk = Chunk::initialize(&types, 100);

    chunk.set_cardinality(50);
    assert_eq!(chunk.size(), 50);
}

#[test]
fn test_set_cardinality_preserves_shared_column_already_at_target_count() {
    let types = vec![LogicalType::Integer, LogicalType::Integer];
    let mut chunk = Chunk::initialize(&types, 8);
    let shared = Arc::new(Vector::from_i32(&[11, 22]));

    chunk.data[0] = shared.clone();
    chunk.set_cardinality(2);

    assert!(Arc::ptr_eq(chunk.column(0).unwrap(), &shared));
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(11));
    assert_eq!(chunk.column(1).unwrap().len(), 2);
}

#[test]
fn test_set_cardinality_same_count_skips_shared_cow() {
    let shared = Arc::new(Vector::from_i32(&[7, 8]));
    let mut chunk = Chunk::from_arc_vectors(vec![shared.clone()]);

    chunk.set_cardinality(2);

    assert!(Arc::ptr_eq(chunk.column(0).unwrap(), &shared));
    assert_eq!(Arc::strong_count(&shared), 2);
}

#[test]
fn test_reset() {
    let v1 = Vector::from_i64(&[10, 20, 30]);
    let mut chunk = Chunk::from_vectors(vec![v1]);

    assert_eq!(chunk.size(), 3);
    chunk.reset();
    assert_eq!(chunk.size(), 0);
    assert_eq!(chunk.column_count(), 1);
}

#[test]
fn test_all_constant() {
    let v1 = Vector::constant::<i32>(LogicalType::Integer, 42, 100);
    let v2 = Vector::constant::<i64>(LogicalType::BigInt, 99, 100);

    let chunk = Chunk::from_vectors(vec![v1, v2]);
    assert!(chunk.all_constant());
}

#[test]
fn test_flatten() {
    let v1 = Vector::constant::<i32>(LogicalType::Integer, 5, 10);
    let mut chunk = Chunk::from_vectors(vec![v1]);

    assert_eq!(chunk.data[0].vector_type(), VectorType::Constant);
    chunk.flatten();
    assert_eq!(chunk.data[0].vector_type(), VectorType::Flat);
}

#[test]
fn test_split_fuse() {
    let v1 = Vector::from_i32(&[1, 2]);
    let v2 = Vector::from_strings(&["a", "b"]);
    let v3 = Vector::from_bool(&[true, false]);

    let mut chunk = Chunk::from_vectors(vec![v1, v2, v3]);
    let mut other = Chunk::new();

    chunk.split(&mut other, 1);

    assert_eq!(chunk.column_count(), 1);
    assert_eq!(other.column_count(), 2);

    chunk.fuse(&mut other);
    assert_eq!(chunk.column_count(), 3);
    assert!(other.is_empty());
}

#[test]
fn test_fuse_drops_reset_state_when_allocators_differ() {
    use crate::allocator::DefaultAllocator;

    let left_allocator = Arc::new(DefaultAllocator::new());
    let right_allocator = Arc::new(DefaultAllocator::new());
    let mut left =
        Chunk::initialize_with_allocator(&[LogicalType::Integer], 4, left_allocator.clone());
    let mut right =
        Chunk::initialize_with_allocator(&[LogicalType::Varchar], 4, right_allocator.clone());

    left.set_cardinality(1);
    left.set_value(0, 0, &Value::Integer(7)).unwrap();
    right.set_cardinality(1);
    right
        .set_value(0, 0, &Value::Varchar("seven".to_string()))
        .unwrap();

    left.fuse(&mut right);

    assert_eq!(left.column_count(), 2);
    assert_eq!(left.size(), 1);
    assert_eq!(left.get_value(0, 0), Some(Value::Integer(7)));
    assert_eq!(
        left.get_value(1, 0),
        Some(Value::Varchar("seven".to_string()))
    );
    assert_eq!(left.initial_capacity, 0);
    assert!(left.reset_state.is_none());
    assert!(right.is_empty());
}

#[test]
fn test_types() {
    let v1 = Vector::from_i32(&[1, 2]);
    let v2 = Vector::from_strings(&["x", "y"]);
    let chunk = Chunk::from_vectors(vec![v1, v2]);

    let types = chunk.types();
    assert_eq!(types, vec![LogicalType::Integer, LogicalType::Varchar]);
}

#[test]
fn test_allocator_propagation() {
    use crate::allocator::DefaultAllocator;

    let allocator = Arc::new(DefaultAllocator::new());
    let types = vec![LogicalType::Integer];
    let mut chunk = Chunk::initialize_with_allocator(&types, 100, allocator.clone());

    assert_eq!(chunk.allocator.name(), "DefaultAllocator");
    assert_eq!(
        chunk.column(0).unwrap().allocator().name(),
        "DefaultAllocator"
    );

    // Reset should preserve allocator
    chunk.reset();
    assert_eq!(
        chunk.column(0).unwrap().allocator().name(),
        "DefaultAllocator"
    );
}

#[test]
fn test_reset_restores_schema_capacity_and_allocator() {
    use crate::allocator::DefaultAllocator;

    let allocator = Arc::new(DefaultAllocator::new());
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut chunk = Chunk::initialize_with_allocator(&types, 4, allocator.clone());

    chunk.set_cardinality(2);
    chunk.column_mut(0).unwrap().set_i32(0, 10);
    chunk.column_mut(0).unwrap().set_i32(1, 20);
    chunk.column_mut(1).unwrap().set_string(0, "ten");
    chunk.column_mut(1).unwrap().set_string(1, "twenty");
    chunk.set_capacity(9);

    chunk.reset();

    assert_eq!(chunk.size(), 0);
    assert_eq!(chunk.capacity(), 4);
    assert_eq!(chunk.types(), types);
    assert_eq!(chunk.allocator().name(), "DefaultAllocator");
    assert_eq!(
        chunk.column(0).unwrap().allocator().name(),
        "DefaultAllocator"
    );

    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_i32(0, 99);
    chunk.column_mut(1).unwrap().set_string(0, "reset");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(99));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("reset"));
}

#[test]
fn test_reset_keeps_flat_buffer_writable() {
    let mut chunk = Chunk::initialize(&[LogicalType::Integer], 8);

    for value in 0..4 {
        chunk.reset();
        chunk.set_cardinality(1);

        let before = chunk.column(0).unwrap().as_slice::<i32>().as_ptr() as usize;
        chunk.column_mut(0).unwrap().set_i32(0, value);

        let after = chunk.column(0).unwrap().as_slice::<i32>().as_ptr() as usize;
        assert_eq!(before, after);
        assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(value));
    }
}

#[test]
fn test_reset_preserves_nested_vector_writability() {
    let struct_fields = vec![
        ("label".to_string(), LogicalType::Varchar),
        ("score".to_string(), LogicalType::Integer),
    ];
    let types = vec![
        LogicalType::Varchar,
        LogicalType::Array(Box::new(LogicalType::Float), 2),
        LogicalType::List(Box::new(LogicalType::Integer)),
        LogicalType::Struct(struct_fields.clone()),
    ];
    let mut chunk = Chunk::initialize(&types, 4);

    chunk.set_cardinality(2);
    chunk.column_mut(0).unwrap().set_string(0, "alpha");
    chunk.column_mut(0).unwrap().set_string(1, "beta");
    chunk.column_mut(1).unwrap().set_value(
        0,
        &Value::Array(
            vec![Value::Float(1.0), Value::Float(2.0)],
            LogicalType::Float,
            2,
        ),
    );
    chunk.column_mut(1).unwrap().set_value(
        1,
        &Value::Array(
            vec![Value::Float(3.0), Value::Float(4.0)],
            LogicalType::Float,
            2,
        ),
    );
    chunk.column_mut(2).unwrap().set_value(
        0,
        &Value::List(
            vec![Value::Integer(1), Value::Integer(2)],
            LogicalType::Integer,
        ),
    );
    chunk.column_mut(2).unwrap().set_value(
        1,
        &Value::List(vec![Value::Integer(3)], LogicalType::Integer),
    );
    chunk.column_mut(3).unwrap().set_value(
        0,
        &Value::Struct(
            vec![Value::Varchar("alice".to_string()), Value::Integer(10)],
            struct_fields.clone(),
        ),
    );
    chunk.column_mut(3).unwrap().set_value(
        1,
        &Value::Struct(
            vec![Value::Varchar("bob".to_string()), Value::Integer(20)],
            struct_fields.clone(),
        ),
    );

    chunk.reset();
    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_string(0, "gamma");
    chunk.column_mut(1).unwrap().set_value(
        0,
        &Value::Array(
            vec![Value::Float(5.0), Value::Float(6.0)],
            LogicalType::Float,
            2,
        ),
    );
    chunk.column_mut(2).unwrap().set_value(
        0,
        &Value::List(
            vec![Value::Integer(7), Value::Integer(8), Value::Integer(9)],
            LogicalType::Integer,
        ),
    );
    chunk.column_mut(3).unwrap().set_value(
        0,
        &Value::Struct(
            vec![Value::Varchar("carol".to_string()), Value::Integer(30)],
            struct_fields.clone(),
        ),
    );

    assert_eq!(chunk.column(0).unwrap().get_string(0), Some("gamma"));
    assert_eq!(
        chunk.column(1).unwrap().get_value(0),
        Value::Array(
            vec![Value::Float(5.0), Value::Float(6.0)],
            LogicalType::Float,
            2
        )
    );
    assert_eq!(
        chunk.column(2).unwrap().get_value(0),
        Value::List(
            vec![Value::Integer(7), Value::Integer(8), Value::Integer(9)],
            LogicalType::Integer,
        )
    );
    assert_eq!(
        chunk.column(3).unwrap().get_value(0),
        Value::Struct(
            vec![Value::Varchar("carol".to_string()), Value::Integer(30)],
            struct_fields,
        )
    );
}

#[test]
fn test_reference_reset_returns_to_own_initialized_state() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut reusable = Chunk::initialize(&types, 3);
    let source = make_int_string_chunk(&[10, 20], &["ten", "twenty"]);

    reusable.reference(&source);
    assert_eq!(reusable.size(), 2);
    assert_eq!(reusable.column(0).unwrap().get_i32(1), Some(20));

    reusable.reset();

    assert_eq!(reusable.size(), 0);
    assert_eq!(reusable.capacity(), 3);
    assert_eq!(reusable.types(), types);

    reusable.set_cardinality(1);
    reusable.column_mut(0).unwrap().set_i32(0, 77);
    reusable.column_mut(1).unwrap().set_string(0, "reset");
    assert_eq!(reusable.column(0).unwrap().get_i32(0), Some(77));
    assert_eq!(reusable.column(1).unwrap().get_string(0), Some("reset"));
}

#[test]
fn test_move_from_preserves_reset_state() {
    let mut source = Chunk::initialize(&[LogicalType::Integer], 5);
    source.set_cardinality(1);
    source.column_mut(0).unwrap().set_i32(0, 42);

    let mut target = Chunk::new();
    target.move_from(&mut source);

    assert_eq!(source.column_count(), 0);
    assert_eq!(target.size(), 1);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(42));

    target.reset();
    assert_eq!(target.capacity(), 5);
    target.set_cardinality(1);
    target.column_mut(0).unwrap().set_i32(0, 99);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(99));
}

#[test]
fn test_split_transfers_reset_state() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut chunk = Chunk::initialize(&types, 4);
    let mut tail = Chunk::new();

    chunk.split(&mut tail, 1);

    assert_eq!(chunk.column_count(), 1);
    assert_eq!(tail.column_count(), 1);

    chunk.reset();
    tail.reset();

    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_i32(0, 1);
    tail.set_cardinality(1);
    tail.column_mut(0).unwrap().set_string(0, "tail");

    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(tail.column(0).unwrap().get_string(0), Some("tail"));
}

#[test]
fn test_fuse_combines_reset_state_when_capacities_match() {
    let mut left = Chunk::initialize(&[LogicalType::Integer], 4);
    let mut right = Chunk::initialize(&[LogicalType::Varchar], 4);

    left.set_cardinality(1);
    left.column_mut(0).unwrap().set_i32(0, 10);
    right.set_cardinality(1);
    right.column_mut(0).unwrap().set_string(0, "ten");

    left.fuse(&mut right);

    assert_eq!(left.column_count(), 2);
    assert_eq!(right.column_count(), 0);

    left.reset();
    left.set_cardinality(1);
    left.column_mut(0).unwrap().set_i32(0, 20);
    left.column_mut(1).unwrap().set_string(0, "twenty");

    assert_eq!(left.column(0).unwrap().get_i32(0), Some(20));
    assert_eq!(left.column(1).unwrap().get_string(0), Some("twenty"));
}

#[test]
fn test_destroy_clears_reset_state() {
    let mut chunk = Chunk::initialize(&[LogicalType::Integer], 4);

    chunk.destroy();
    chunk.reset();

    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.capacity(), 0);
    assert!(chunk.is_empty());
}

#[test]
fn test_chunk_get_set_value_with_bounds_checks() {
    let mut chunk = Chunk::initialize(&[LogicalType::Integer, LogicalType::Varchar], 2);
    chunk.set_cardinality(2);

    assert_eq!(chunk.set_value(0, 0, &Value::Integer(42)), Some(()));
    assert_eq!(
        chunk.set_value(1, 1, &Value::Varchar("paro".to_string())),
        Some(())
    );
    assert_eq!(chunk.get_value(0, 0), Some(Value::Integer(42)));
    assert_eq!(
        chunk.get_value(1, 1),
        Some(Value::Varchar("paro".to_string()))
    );

    assert_eq!(chunk.get_value(2, 0), None);
    assert_eq!(chunk.get_value(0, 2), None);
    assert_eq!(chunk.set_value(2, 0, &Value::Integer(1)), None);
    assert_eq!(chunk.set_value(0, 2, &Value::Integer(1)), None);
}

#[test]
fn test_reference_columns_references_selected_columns_and_preserves_reset_state() {
    let source = Chunk::from_vectors(vec![
        Vector::from_i32(&[10, 20]),
        Vector::from_strings(&["ten", "twenty"]),
        Vector::from_bool(&[true, false]),
    ]);
    let mut target = Chunk::initialize(&[LogicalType::Varchar, LogicalType::Integer], 3);

    target.reference_columns(&source, &[1, 0]);

    assert_eq!(target.size(), 2);
    assert_eq!(
        target.get_value(0, 0),
        Some(Value::Varchar("ten".to_string()))
    );
    assert_eq!(target.get_value(1, 1), Some(Value::Integer(20)));

    target.reset();
    assert_eq!(target.size(), 0);
    assert_eq!(
        target.types(),
        vec![LogicalType::Varchar, LogicalType::Integer]
    );

    target.set_cardinality(1);
    assert_eq!(
        target.set_value(0, 0, &Value::Varchar("reset".to_string())),
        Some(())
    );
    assert_eq!(target.set_value(1, 0, &Value::Integer(99)), Some(()));
    assert_eq!(
        target.get_value(0, 0),
        Some(Value::Varchar("reset".to_string()))
    );
    assert_eq!(target.get_value(1, 0), Some(Value::Integer(99)));
}

#[test]
fn test_get_allocation_size_is_non_zero_and_grows_with_string_heap_usage() {
    let mut chunk = Chunk::initialize(&[LogicalType::Varchar], 2);
    let before = chunk.get_allocation_size();

    chunk.set_cardinality(2);
    chunk
        .set_value(
            0,
            0,
            &Value::Varchar("this is a sufficiently long string".to_string()),
        )
        .unwrap();
    chunk
        .set_value(
            0,
            1,
            &Value::Varchar("another long string that must live in the heap".to_string()),
        )
        .unwrap();

    let after = chunk.get_allocation_size();
    assert!(before > 0);
    assert!(after > before);
}

#[test]
fn test_allocation_size_deduplicates_shared_dictionary_child_within_chunk() {
    let shared = Arc::new(Vector::from_i32(&[1, 2, 3, 4]));
    let dict1 = Arc::new(Vector::dictionary(shared.clone(), vec![0, 1, 2]));
    let dict2 = Arc::new(Vector::dictionary(shared, vec![2, 3, 1]));
    let chunk = Chunk::from_arc_vectors(vec![dict1.clone(), dict2.clone()]);

    let combined = chunk.get_allocation_size();
    let separate = {
        let mut left = AllocationSet::new();
        let first = dict1.collect_allocation_size(&mut left);
        let mut right = AllocationSet::new();
        let second = dict2.collect_allocation_size(&mut right);
        first + second
    };

    assert!(combined < separate);
}

#[test]
fn test_allocation_set_deduplicates_shared_allocations_across_chunks() {
    let shared = Arc::new(Vector::from_i32(&[10, 20, 30, 40]));
    let chunk1 = Chunk::from_arc_vectors(vec![Arc::new(Vector::dictionary(
        shared.clone(),
        vec![0, 1, 2],
    ))]);
    let chunk2 = Chunk::from_arc_vectors(vec![Arc::new(Vector::dictionary(shared, vec![2, 3, 1]))]);

    let separate = chunk1.get_allocation_size() + chunk2.get_allocation_size();
    let mut allocations = AllocationSet::new();
    let deduplicated = chunk1.collect_allocation_size(&mut allocations)
        + chunk2.collect_allocation_size(&mut allocations);

    assert!(deduplicated < separate);
    assert!(!allocations.is_empty());
}

#[test]
fn test_verify_accepts_consistent_chunk() {
    let mut chunk = Chunk::initialize(
        &[
            LogicalType::Integer,
            LogicalType::List(Box::new(LogicalType::Integer)),
        ],
        4,
    );
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, &Value::Integer(42)).unwrap();
    chunk
        .set_value(
            1,
            0,
            &Value::List(
                vec![Value::Integer(1), Value::Integer(2)],
                LogicalType::Integer,
            ),
        )
        .unwrap();

    chunk.verify();
}

#[cfg(debug_assertions)]
#[test]
fn test_verify_panics_on_invalid_chunk_state() {
    let mut chunk = Chunk::initialize(&[LogicalType::Integer], 4);
    chunk.set_cardinality(1);
    chunk.capacity = 0;

    let result = catch_unwind(AssertUnwindSafe(|| chunk.verify()));
    assert!(result.is_err());
}

#[cfg(debug_assertions)]
#[test]
fn test_verify_panics_on_reset_allocator_mismatch() {
    use crate::allocator::DefaultAllocator;

    let mut chunk = Chunk::initialize_with_allocator(
        &[LogicalType::Integer],
        4,
        Arc::new(DefaultAllocator::new()),
    );
    chunk.reset_state.as_mut().unwrap().allocator = Arc::new(DefaultAllocator::new());

    let result = catch_unwind(AssertUnwindSafe(|| chunk.verify()));
    assert!(result.is_err());
}

#[test]
fn test_append_copies_rows() {
    let mut chunk = make_int_string_chunk(&[1, 2], &["a", "b"]);
    let other = make_int_string_chunk(&[3, 4], &["c", "d"]);

    chunk.append(&other);

    assert_eq!(chunk.size(), 4);
    assert_eq!(chunk.column(0).unwrap().get_i32(2), Some(3));
    assert_eq!(chunk.column(0).unwrap().get_i32(3), Some(4));
    assert_eq!(chunk.column(1).unwrap().get_string(2), Some("c"));
    assert_eq!(chunk.column(1).unwrap().get_string(3), Some("d"));
}

#[test]
fn test_append_grows_capacity_and_preserves_values() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut chunk = Chunk::initialize(&types, 1);
    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_i32(0, 10);
    chunk.column_mut(1).unwrap().set_string(0, "ten");

    let other = make_int_string_chunk(&[20, 30], &["twenty", "thirty"]);
    chunk.append(&other);

    assert!(chunk.capacity() >= 3);
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(10));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(20));
    assert_eq!(chunk.column(0).unwrap().get_i32(2), Some(30));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("ten"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("twenty"));
    assert_eq!(chunk.column(1).unwrap().get_string(2), Some("thirty"));
}

#[test]
fn test_slice_filters_rows_with_dictionary_vectors() {
    let mut chunk = make_int_string_chunk(&[10, 20, 30], &["ten", "twenty", "thirty"]);
    let sel = SelectionVector::from_indices(vec![2, 0]);

    chunk.slice(&sel, 2);

    assert_eq!(chunk.size(), 2);
    assert_eq!(
        chunk.column(0).unwrap().vector_type(),
        VectorType::Dictionary
    );
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(30));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(10));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("thirty"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("ten"));
}

#[test]
fn test_slice_reuses_selection_allocation_across_columns() {
    let mut chunk = make_int_string_chunk(&[10, 20, 30], &["ten", "twenty", "thirty"]);
    let sel = SelectionVector::from_indices(vec![2, 0]);
    let selection_allocation = sel.allocation_identity();

    chunk.slice(&sel, 2);

    let left_sel = chunk
        .column(0)
        .unwrap()
        .sel_vector()
        .expect("left dictionary selection");
    let right_sel = chunk
        .column(1)
        .unwrap()
        .sel_vector()
        .expect("right dictionary selection");

    assert_eq!(left_sel.allocation_identity(), selection_allocation);
    assert_eq!(right_sel.allocation_identity(), selection_allocation);
}

#[test]
fn test_slice_collapses_nested_dictionary() {
    let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
    let dict = Arc::new(Vector::dictionary(base, vec![3, 1, 2]));
    let mut chunk = Chunk::from_arc_vectors(vec![dict]);
    let sel = SelectionVector::from_indices(vec![1, 2]);

    chunk.slice(&sel, 2);

    let column = chunk.column(0).unwrap();
    assert_eq!(chunk.size(), 2);
    assert_eq!(column.vector_type(), VectorType::Dictionary);
    assert_eq!(column.get_i64(0), Some(20));
    assert_eq!(column.get_i64(1), Some(30));
    assert_eq!(
        column.child().expect("dictionary child").vector_type(),
        VectorType::Flat
    );
}

#[test]
fn test_slice_preserves_array_values() {
    let embeddings = vec![
        vec![1.0f32, 2.0, 3.0],
        vec![4.0f32, 5.0, 6.0],
        vec![7.0f32, 8.0, 9.0],
    ];
    let mut chunk = Chunk::from_vectors(vec![Vector::from_embeddings(&embeddings, 3)]);
    let sel = SelectionVector::from_indices(vec![2, 0]);

    chunk.slice(&sel, 2);

    match chunk.column(0).unwrap().get_value(0) {
        Value::Array(values, _, size) => {
            assert_eq!(size, 3);
            assert_eq!(
                values,
                vec![Value::Float(7.0), Value::Float(8.0), Value::Float(9.0)]
            );
        }
        other => panic!("expected array value, got {other:?}"),
    }

    match chunk.column(0).unwrap().get_value(1) {
        Value::Array(values, _, size) => {
            assert_eq!(size, 3);
            assert_eq!(
                values,
                vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)]
            );
        }
        other => panic!("expected array value, got {other:?}"),
    }
}

#[test]
fn test_slice_range_uses_contiguous_selection() {
    let mut chunk = make_int_string_chunk(&[10, 20, 30, 40], &["ten", "twenty", "thirty", "forty"]);

    chunk.slice_range(1, 2);

    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(20));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(30));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("twenty"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("thirty"));
}

#[test]
fn test_copy_to_deep_copies_rows() {
    let mut source = make_int_string_chunk(&[1, 2, 3], &["a", "b", "c"]);
    let mut target = Chunk::initialize(&source.types(), 3);

    source.copy_to(&mut target, 0);
    source.column_mut(0).unwrap().set_i32(0, 99);
    source.column_mut(1).unwrap().set_string(1, "changed");

    assert_eq!(target.size(), 3);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(target.column(1).unwrap().get_string(1), Some("b"));
}

#[test]
fn test_copy_to_respects_offset() {
    let source = make_int_string_chunk(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
    let mut target = Chunk::initialize(&source.types(), 1);

    source.copy_to(&mut target, 2);

    assert!(target.capacity() >= 2);
    assert_eq!(target.size(), 2);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(3));
    assert_eq!(target.column(0).unwrap().get_i32(1), Some(4));
    assert_eq!(target.column(1).unwrap().get_string(0), Some("c"));
    assert_eq!(target.column(1).unwrap().get_string(1), Some("d"));
}
