// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::allocator::{Allocator, DefaultAllocator};
use crate::error::{self as paro_error, Result};
use crate::runtime_value::Value;
use crate::types::LogicalType;
use crate::vector::{AllocationSet, VectorType, VECTOR_SIZE};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct ToggleAllocator {
    inner: DefaultAllocator,
    fail: AtomicBool,
}

impl ToggleAllocator {
    fn new() -> Self {
        Self {
            inner: DefaultAllocator::new(),
            fail: AtomicBool::new(false),
        }
    }

    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }
}

impl Allocator for ToggleAllocator {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(paro_error::out_of_memory(format!(
                "injected allocation failure: {size} bytes"
            )));
        }
        self.inner.allocate(size)
    }

    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(paro_error::out_of_memory(format!(
                "injected allocation failure: {size} bytes"
            )));
        }
        self.inner.allocate_zeroed(size)
    }

    fn free(&self, ptr: *mut u8, size: usize) {
        self.inner.free(ptr, size);
    }

    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(paro_error::out_of_memory(format!(
                "injected allocation failure: {new_size} bytes"
            )));
        }
        self.inner.reallocate(ptr, old_size, new_size)
    }

    fn name(&self) -> &'static str {
        "ToggleAllocator"
    }
}

fn make_int_string_chunk(ids: &[i32], labels: &[&str]) -> Chunk {
    crate::test_utils::test_chunk_from_vectors(vec![
        crate::test_utils::test_i32_vector(ids),
        crate::test_utils::test_string_vector(labels),
    ])
}

fn make_nested_chunk(rows: &[(i32, i32, Vec<i32>, &str)], capacity: usize) -> Chunk {
    let types = vec![
        LogicalType::Array(Box::new(LogicalType::Integer), 2),
        LogicalType::List(Box::new(LogicalType::Integer)),
        LogicalType::Varchar,
    ];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, capacity);
    chunk.set_cardinality(rows.len());

    for (row_idx, (left, right, list, label)) in rows.iter().enumerate() {
        chunk.column_mut(0).unwrap().set_value(
            row_idx,
            &Value::Array(
                vec![Value::Integer(*left), Value::Integer(*right)],
                LogicalType::Integer,
                2,
            ),
        );
        chunk.column_mut(1).unwrap().set_value(
            row_idx,
            &Value::List(
                list.iter().copied().map(Value::Integer).collect::<Vec<_>>(),
                LogicalType::Integer,
            ),
        );
        chunk.column_mut(2).unwrap().set_string(row_idx, label);
    }

    chunk
}

#[test]
fn test_empty_chunk() {
    let chunk = crate::test_utils::test_new_chunk();
    assert!(chunk.is_empty());
    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.capacity(), VECTOR_SIZE);
}

#[test]
fn test_initialize() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let chunk = crate::test_utils::test_chunk_with_capacity(&types, 1024);
    assert_eq!(chunk.column_count(), 2);
    assert!(chunk.is_empty());
    assert_eq!(chunk.capacity(), 1024);
}

#[test]
fn test_init_empty() {
    let types = vec![LogicalType::BigInt, LogicalType::Boolean];
    let chunk = crate::test_utils::test_empty_chunk(&types);
    assert_eq!(chunk.column_count(), 2);
    assert_eq!(chunk.types(), types);
}

#[test]
fn test_from_vectors() {
    let v1 = crate::test_utils::test_i32_vector(&[1, 2, 3]);
    let v2 = crate::test_utils::test_string_vector(&["a", "b", "c"]);

    let chunk = crate::test_utils::test_chunk_from_vectors(vec![v1, v2]);
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column_count(), 2);

    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(chunk.column(1).unwrap().get_string(2), Some("c"));
}

#[test]
fn test_set_cardinality() {
    let types = vec![LogicalType::Integer];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 100);

    chunk.set_cardinality(50);
    assert_eq!(chunk.size(), 50);
}

#[test]
fn test_try_set_cardinality_rejects_capacity_overflow() {
    let types = vec![LogicalType::Integer];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 4);

    let err = chunk.try_set_cardinality(5).unwrap_err();

    assert!(err.to_string().contains("cardinality exceeds capacity"));
    assert_eq!(chunk.size(), 0);
}

#[test]
fn test_try_set_cardinality_shrink_keeps_vector_validity_capacity() {
    let types = vec![LogicalType::Integer];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 8);

    chunk.try_set_cardinality(8).unwrap();
    let validity_len = chunk.column(0).unwrap().validity().len();

    chunk.try_set_cardinality(2).unwrap();

    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().validity().len(), validity_len);
}

#[test]
fn test_try_set_cardinality_growth_propagates_vector_allocation_error() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut chunk = Chunk::try_initialize(&[LogicalType::Null], 64, allocator.clone()).unwrap();
    chunk.column_mut(0).unwrap().try_set_null(1, true).unwrap();
    chunk.set_capacity(129);

    allocator.set_fail(true);
    let err = chunk.try_set_cardinality(129).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
    assert_eq!(chunk.size(), 0);
    assert_eq!(chunk.column(0).unwrap().validity().len(), 64);
}

#[test]
fn test_set_cardinality_preserves_shared_column_already_at_target_count() {
    let types = vec![LogicalType::Integer, LogicalType::Integer];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 8);
    let shared = Arc::new(crate::test_utils::test_i32_vector(&[11, 22]));

    chunk.data[0] = shared.clone();
    chunk.set_cardinality(2);

    assert!(Arc::ptr_eq(chunk.column(0).unwrap(), &shared));
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(11));
    assert_eq!(chunk.column(1).unwrap().len(), 2);
}

#[test]
fn test_set_cardinality_same_count_skips_shared_cow() {
    let shared = Arc::new(crate::test_utils::test_i32_vector(&[7, 8]));
    let mut chunk = crate::test_utils::test_chunk_from_arc_vectors(vec![shared.clone()]);

    chunk.set_cardinality(2);

    assert!(Arc::ptr_eq(chunk.column(0).unwrap(), &shared));
    assert_eq!(Arc::strong_count(&shared), 2);
}

#[test]
fn test_reset() {
    let v1 = crate::test_utils::test_i64_vector(&[10, 20, 30]);
    let mut chunk = crate::test_utils::test_chunk_from_vectors(vec![v1]);

    assert_eq!(chunk.size(), 3);
    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");
    assert_eq!(chunk.size(), 0);
    assert_eq!(chunk.column_count(), 1);
}

#[test]
fn test_all_constant() {
    let v1 = crate::test_utils::test_constant::<i32>(LogicalType::Integer, 42, 100);
    let v2 = crate::test_utils::test_constant::<i64>(LogicalType::BigInt, 99, 100);

    let chunk = crate::test_utils::test_chunk_from_vectors(vec![v1, v2]);
    assert!(chunk.all_constant());
}

#[test]
fn test_flatten() {
    let v1 = crate::test_utils::test_constant::<i32>(LogicalType::Integer, 5, 10);
    let mut chunk = crate::test_utils::test_chunk_from_vectors(vec![v1]);

    assert_eq!(chunk.data[0].vector_type(), VectorType::Constant);
    chunk.try_flatten().unwrap();
    assert_eq!(chunk.data[0].vector_type(), VectorType::Flat);
}

#[test]
fn test_split_fuse() {
    let v1 = crate::test_utils::test_i32_vector(&[1, 2]);
    let v2 = crate::test_utils::test_string_vector(&["a", "b"]);
    let v3 = crate::test_utils::test_bool_vector(&[true, false]);

    let mut chunk = crate::test_utils::test_chunk_from_vectors(vec![v1, v2, v3]);
    let mut other = crate::test_utils::test_new_chunk();

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
    let mut left = crate::test_utils::test_chunk_with_capacity_and_allocator(
        &[LogicalType::Integer],
        4,
        left_allocator.clone(),
    );
    let mut right = crate::test_utils::test_chunk_with_capacity_and_allocator(
        &[LogicalType::Varchar],
        4,
        right_allocator.clone(),
    );

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
    let v1 = crate::test_utils::test_i32_vector(&[1, 2]);
    let v2 = crate::test_utils::test_string_vector(&["x", "y"]);
    let chunk = crate::test_utils::test_chunk_from_vectors(vec![v1, v2]);

    let types = chunk.types();
    assert_eq!(types, vec![LogicalType::Integer, LogicalType::Varchar]);
}

#[test]
fn test_allocator_propagation() {
    use crate::allocator::DefaultAllocator;

    let allocator = Arc::new(DefaultAllocator::new());
    let types = vec![LogicalType::Integer];
    let mut chunk =
        crate::test_utils::test_chunk_with_capacity_and_allocator(&types, 100, allocator.clone());

    assert_eq!(chunk.allocator.name(), "DefaultAllocator");
    assert_eq!(
        chunk.column(0).unwrap().allocator().name(),
        "DefaultAllocator"
    );

    // Reset should preserve allocator
    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");
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
    let mut chunk =
        crate::test_utils::test_chunk_with_capacity_and_allocator(&types, 4, allocator.clone());

    chunk.set_cardinality(2);
    chunk.column_mut(0).unwrap().set_i32(0, 10);
    chunk.column_mut(0).unwrap().set_i32(1, 20);
    chunk.column_mut(1).unwrap().set_string(0, "ten");
    chunk.column_mut(1).unwrap().set_string(1, "twenty");
    chunk.set_capacity(9);

    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");

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
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 8);

    for value in 0..4 {
        chunk
            .try_reset(chunk.allocator().clone())
            .expect("test chunk reset allocation failed");
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
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 4);

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

    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");
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
    let mut reusable = crate::test_utils::test_chunk_with_capacity(&types, 3);
    let source = make_int_string_chunk(&[10, 20], &["ten", "twenty"]);

    reusable.reference(&source);
    assert_eq!(reusable.size(), 2);
    assert_eq!(reusable.column(0).unwrap().get_i32(1), Some(20));

    reusable
        .try_reset(reusable.allocator().clone())
        .expect("test chunk reset allocation failed");

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
    let mut source = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 5);
    source.set_cardinality(1);
    source.column_mut(0).unwrap().set_i32(0, 42);

    let mut target = crate::test_utils::test_new_chunk();
    target.move_from(&mut source);

    assert_eq!(source.column_count(), 0);
    assert_eq!(target.size(), 1);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(42));

    target
        .try_reset(target.allocator().clone())
        .expect("test chunk reset allocation failed");
    assert_eq!(target.capacity(), 5);
    target.set_cardinality(1);
    target.column_mut(0).unwrap().set_i32(0, 99);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(99));
}

#[test]
fn test_split_transfers_reset_state() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 4);
    let mut tail = crate::test_utils::test_new_chunk();

    chunk.split(&mut tail, 1);

    assert_eq!(chunk.column_count(), 1);
    assert_eq!(tail.column_count(), 1);

    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");
    tail.try_reset(tail.allocator().clone())
        .expect("test chunk reset allocation failed");

    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_i32(0, 1);
    tail.set_cardinality(1);
    tail.column_mut(0).unwrap().set_string(0, "tail");

    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(tail.column(0).unwrap().get_string(0), Some("tail"));
}

#[test]
fn test_fuse_combines_reset_state_when_capacities_match() {
    let mut left = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 4);
    let mut right = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Varchar], 4);

    left.set_cardinality(1);
    left.column_mut(0).unwrap().set_i32(0, 10);
    right.set_cardinality(1);
    right.column_mut(0).unwrap().set_string(0, "ten");

    left.fuse(&mut right);

    assert_eq!(left.column_count(), 2);
    assert_eq!(right.column_count(), 0);

    left.try_reset(left.allocator().clone())
        .expect("test chunk reset allocation failed");
    left.set_cardinality(1);
    left.column_mut(0).unwrap().set_i32(0, 20);
    left.column_mut(1).unwrap().set_string(0, "twenty");

    assert_eq!(left.column(0).unwrap().get_i32(0), Some(20));
    assert_eq!(left.column(1).unwrap().get_string(0), Some("twenty"));
}

#[test]
fn test_destroy_clears_reset_state() {
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 4);

    chunk.destroy();
    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");

    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.capacity(), 0);
    assert!(chunk.is_empty());
}

#[test]
fn test_chunk_get_set_value_with_bounds_checks() {
    let mut chunk = crate::test_utils::test_chunk_with_capacity(
        &[LogicalType::Integer, LogicalType::Varchar],
        2,
    );
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
    let source = crate::test_utils::test_chunk_from_vectors(vec![
        crate::test_utils::test_i32_vector(&[10, 20]),
        crate::test_utils::test_string_vector(&["ten", "twenty"]),
        crate::test_utils::test_bool_vector(&[true, false]),
    ]);
    let mut target = crate::test_utils::test_chunk_with_capacity(
        &[LogicalType::Varchar, LogicalType::Integer],
        3,
    );

    target.reference_columns(&source, &[1, 0]);

    assert_eq!(target.size(), 2);
    assert_eq!(
        target.get_value(0, 0),
        Some(Value::Varchar("ten".to_string()))
    );
    assert_eq!(target.get_value(1, 1), Some(Value::Integer(20)));

    target
        .try_reset(target.allocator().clone())
        .expect("test chunk reset allocation failed");
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
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Varchar], 2);
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
    let shared = Arc::new(crate::test_utils::test_i32_vector(&[1, 2, 3, 4]));
    let dict1 = Arc::new(crate::test_utils::test_dictionary(
        shared.clone(),
        vec![0, 1, 2],
    ));
    let dict2 = Arc::new(crate::test_utils::test_dictionary(shared, vec![2, 3, 1]));
    let chunk = crate::test_utils::test_chunk_from_arc_vectors(vec![dict1.clone(), dict2.clone()]);

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
    let shared = Arc::new(crate::test_utils::test_i32_vector(&[10, 20, 30, 40]));
    let chunk1 = crate::test_utils::test_chunk_from_arc_vectors(vec![Arc::new(
        crate::test_utils::test_dictionary(shared.clone(), vec![0, 1, 2]),
    )]);
    let chunk2 = crate::test_utils::test_chunk_from_arc_vectors(vec![Arc::new(
        crate::test_utils::test_dictionary(shared, vec![2, 3, 1]),
    )]);

    let separate = chunk1.get_allocation_size() + chunk2.get_allocation_size();
    let mut allocations = AllocationSet::new();
    let deduplicated = chunk1.collect_allocation_size(&mut allocations)
        + chunk2.collect_allocation_size(&mut allocations);

    assert!(deduplicated < separate);
    assert!(!allocations.is_empty());
}

#[test]
fn test_verify_accepts_consistent_chunk() {
    let mut chunk = crate::test_utils::test_chunk_with_capacity(
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
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 4);
    chunk.set_cardinality(1);
    chunk.capacity = 0;

    let result = catch_unwind(AssertUnwindSafe(|| chunk.verify()));
    assert!(result.is_err());
}

#[cfg(debug_assertions)]
#[test]
fn test_verify_panics_on_reset_allocator_mismatch() {
    use crate::allocator::DefaultAllocator;

    let mut chunk = crate::test_utils::test_chunk_with_capacity_and_allocator(
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

    chunk
        .try_append(&other)
        .expect("test chunk append allocation failed");

    assert_eq!(chunk.size(), 4);
    assert_eq!(chunk.column(0).unwrap().get_i32(2), Some(3));
    assert_eq!(chunk.column(0).unwrap().get_i32(3), Some(4));
    assert_eq!(chunk.column(1).unwrap().get_string(2), Some("c"));
    assert_eq!(chunk.column(1).unwrap().get_string(3), Some("d"));
}

#[test]
fn test_append_grows_capacity_and_preserves_values() {
    let types = vec![LogicalType::Integer, LogicalType::Varchar];
    let mut chunk = crate::test_utils::test_chunk_with_capacity(&types, 1);
    chunk.set_cardinality(1);
    chunk.column_mut(0).unwrap().set_i32(0, 10);
    chunk.column_mut(1).unwrap().set_string(0, "ten");

    let other = make_int_string_chunk(&[20, 30], &["twenty", "thirty"]);
    chunk
        .try_append(&other)
        .expect("test chunk append allocation failed");

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
fn test_append_grows_capacity_with_nested_columns() {
    let mut chunk = make_nested_chunk(&[(1, 2, vec![10, 20], "first long label")], 1);
    let other = make_nested_chunk(
        &[
            (3, 4, vec![30], "second long label"),
            (5, 6, vec![50, 60, 70], "third long label"),
        ],
        2,
    );

    chunk
        .try_append(&other)
        .expect("test chunk append allocation failed");

    assert!(chunk.capacity() >= 3);
    assert_eq!(chunk.size(), 3);
    assert_eq!(
        chunk.column(0).unwrap().get_value(2),
        Value::Array(
            vec![Value::Integer(5), Value::Integer(6)],
            LogicalType::Integer,
            2
        )
    );
    assert_eq!(
        chunk.column(1).unwrap().get_value(2),
        Value::List(
            vec![Value::Integer(50), Value::Integer(60), Value::Integer(70)],
            LogicalType::Integer
        )
    );
    assert_eq!(
        chunk.column(2).unwrap().get_string(2),
        Some("third long label")
    );

    let grown_capacity = chunk.capacity();
    chunk
        .try_reset(chunk.allocator().clone())
        .expect("test chunk reset allocation failed");
    assert_eq!(chunk.capacity(), grown_capacity);
    chunk.set_cardinality(1);
    chunk
        .column_mut(2)
        .unwrap()
        .set_string(0, "reset long label");
    assert_eq!(
        chunk.column(2).unwrap().get_string(0),
        Some("reset long label")
    );
}

#[test]
fn test_slice_filters_rows_with_dictionary_vectors() {
    let mut chunk = make_int_string_chunk(&[10, 20, 30], &["ten", "twenty", "thirty"]);
    let sel = crate::test_utils::test_selection(vec![2, 0]);

    chunk
        .try_slice(&sel, 2)
        .expect("test chunk slice allocation failed");

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
    let sel = crate::test_utils::test_selection(vec![2, 0]);
    let selection_allocation = sel.allocation_identity();

    chunk
        .try_slice(&sel, 2)
        .expect("test chunk slice allocation failed");

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
    let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
    let dict = Arc::new(crate::test_utils::test_dictionary(base, vec![3, 1, 2]));
    let mut chunk = crate::test_utils::test_chunk_from_arc_vectors(vec![dict]);
    let sel = crate::test_utils::test_selection(vec![1, 2]);

    chunk
        .try_slice(&sel, 2)
        .expect("test chunk slice allocation failed");

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
fn test_slice_range_uses_range_selection() {
    let mut chunk = make_int_string_chunk(&[10, 20, 30, 40], &["ten", "twenty", "thirty", "forty"]);

    chunk
        .try_slice_range(1, 2)
        .expect("test chunk range slice allocation failed");

    assert_eq!(chunk.size(), 2);
    assert!(matches!(
        chunk.column(0).unwrap().selection(),
        crate::vector::VectorSelection::Range {
            offset: 1,
            count: 2
        }
    ));
    assert!(chunk.column(0).unwrap().sel_vector().is_none());
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(20));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(30));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("twenty"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("thirty"));
}

#[test]
fn test_slice_preserves_array_values() {
    let embeddings = vec![
        vec![1.0f32, 2.0, 3.0],
        vec![4.0f32, 5.0, 6.0],
        vec![7.0f32, 8.0, 9.0],
    ];
    let mut chunk = crate::test_utils::test_chunk_from_vectors(vec![
        crate::test_utils::test_embeddings_vector(&embeddings, 3),
    ]);
    let sel = crate::test_utils::test_selection(vec![2, 0]);

    chunk
        .try_slice(&sel, 2)
        .expect("test chunk slice allocation failed");

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

    chunk
        .try_slice_range(1, 2)
        .expect("test chunk slice_range allocation failed");

    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(20));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(30));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("twenty"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("thirty"));
}

#[test]
fn test_copy_to_deep_copies_rows() {
    let mut source = make_int_string_chunk(&[1, 2, 3], &["a", "b", "c"]);
    let mut target = crate::test_utils::test_chunk_with_capacity(&source.types(), 3);

    source
        .try_copy_to(&mut target, 0)
        .expect("test chunk copy allocation failed");
    source.column_mut(0).unwrap().set_i32(0, 99);
    source.column_mut(1).unwrap().set_string(1, "changed");

    assert_eq!(target.size(), 3);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(target.column(1).unwrap().get_string(1), Some("b"));
}

#[test]
fn test_copy_to_respects_offset() {
    let source = make_int_string_chunk(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
    let mut target = crate::test_utils::test_chunk_with_capacity(&source.types(), 1);

    source
        .try_copy_to(&mut target, 2)
        .expect("test chunk copy allocation failed");

    assert!(target.capacity() >= 2);
    assert_eq!(target.size(), 2);
    assert_eq!(target.column(0).unwrap().get_i32(0), Some(3));
    assert_eq!(target.column(0).unwrap().get_i32(1), Some(4));
    assert_eq!(target.column(1).unwrap().get_string(0), Some("c"));
    assert_eq!(target.column(1).unwrap().get_string(1), Some("d"));
}

#[test]
fn test_deep_copy_materializes_dictionary_nested_columns() {
    let mut source = make_nested_chunk(
        &[
            (1, 2, vec![10, 20], "first long dictionary label"),
            (3, 4, vec![30], "second long dictionary label"),
            (5, 6, vec![50, 60], "third long dictionary label"),
        ],
        3,
    );
    source
        .try_slice_range(1, 2)
        .expect("test chunk slice_range allocation failed");
    assert_eq!(
        source.column(2).unwrap().vector_type(),
        VectorType::Dictionary
    );
    let source_heap_id = source
        .column(2)
        .unwrap()
        .child()
        .unwrap()
        .string_heap()
        .unwrap()
        .allocation_identity();

    let copied = source
        .try_deep_copy(crate::test_utils::test_allocator())
        .expect("test chunk deep copy allocation failed");

    assert_eq!(copied.size(), 2);
    assert_eq!(copied.column(0).unwrap().vector_type(), VectorType::Flat);
    assert_eq!(copied.column(1).unwrap().vector_type(), VectorType::Flat);
    assert_eq!(copied.column(2).unwrap().vector_type(), VectorType::Flat);
    assert_eq!(
        copied.column(0).unwrap().get_value(0),
        Value::Array(
            vec![Value::Integer(3), Value::Integer(4)],
            LogicalType::Integer,
            2
        )
    );
    assert_eq!(
        copied.column(1).unwrap().get_value(1),
        Value::List(
            vec![Value::Integer(50), Value::Integer(60)],
            LogicalType::Integer
        )
    );
    assert_eq!(
        copied.column(2).unwrap().get_string(0),
        Some("second long dictionary label")
    );
    assert_ne!(
        copied
            .column(2)
            .unwrap()
            .string_heap()
            .unwrap()
            .allocation_identity(),
        source_heap_id
    );
}
