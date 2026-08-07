// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::allocator::{Allocator, DefaultAllocator};
use crate::error::{self as paro_error, Result};
use crate::runtime_value::Value;
use crate::types::LogicalType;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

#[derive(Debug)]
struct FailAfterAllocator {
    inner: DefaultAllocator,
    remaining: AtomicUsize,
}

impl FailAfterAllocator {
    fn new(allowed_allocations: usize) -> Self {
        Self {
            inner: DefaultAllocator::new(),
            remaining: AtomicUsize::new(allowed_allocations),
        }
    }

    fn consume(&self, size: usize) -> Result<()> {
        self.remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .map(|_| ())
            .map_err(|_| {
                paro_error::out_of_memory(format!(
                    "injected allocation failure after limit: {size} bytes"
                ))
            })
    }
}

impl Allocator for FailAfterAllocator {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        self.consume(size)?;
        self.inner.allocate(size)
    }

    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
        self.consume(size)?;
        self.inner.allocate_zeroed(size)
    }

    fn free(&self, ptr: *mut u8, size: usize) {
        self.inner.free(ptr, size);
    }

    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
        self.consume(new_size)?;
        self.inner.reallocate(ptr, old_size, new_size)
    }

    fn name(&self) -> &'static str {
        "FailAfterAllocator"
    }
}

fn test_varlen_vector(logical_type: LogicalType, values: &[&[u8]]) -> Vector {
    let mut vector = Vector::try_new(
        logical_type.clone(),
        values.len(),
        crate::test_utils::test_allocator(),
    )
    .unwrap();
    vector.try_set_count(values.len()).unwrap();

    for (idx, value) in values.iter().enumerate() {
        if matches!(logical_type, LogicalType::Blob) {
            vector.try_set_blob(idx, value).unwrap();
        } else {
            let text = std::str::from_utf8(value).unwrap();
            vector.try_set_string(idx, text).unwrap();
        }
    }

    vector
}

fn assert_varlen_value(vector: &Vector, logical_type: &LogicalType, idx: usize, expected: &[u8]) {
    if matches!(logical_type, LogicalType::Blob) {
        assert_eq!(vector.get_blob(idx), Some(expected));
    } else {
        assert_eq!(
            vector.get_string(idx),
            Some(std::str::from_utf8(expected).unwrap())
        );
    }
}

#[test]
fn test_flat_vector_i64() {
    let vec = crate::test_utils::test_i64_vector(&[1, 2, 3, 4, 5]);
    assert_eq!(vec.len(), 5);
    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i64(0), Some(1));
    assert_eq!(vec.get_i64(4), Some(5));
    assert!(!vec.is_null(0));
}

#[test]
fn test_flat_vector_with_nulls() {
    let mut vec = crate::test_utils::test_i32_vector(&[10, 20, 30]);
    vec.validity_mut().set_null(1);

    assert_eq!(vec.get_i32(0), Some(10));
    assert_eq!(vec.get_i32(1), None); // null
    assert_eq!(vec.get_i32(2), Some(30));
    assert!(vec.is_null(1));
}

#[test]
fn test_constant_vector() {
    let vec = crate::test_utils::test_constant::<i64>(LogicalType::BigInt, 42, 1000);
    assert_eq!(vec.len(), 1000);
    assert_eq!(vec.vector_type(), VectorType::Constant);
    assert_eq!(vec.get_i64(0), Some(42));
    assert_eq!(vec.get_i64(999), Some(42));
}

#[test]
fn test_sequence_vector() {
    let vec = crate::test_utils::test_sequence(10, 5, 100);
    assert_eq!(vec.len(), 100);
    assert_eq!(vec.vector_type(), VectorType::Sequence);
    assert_eq!(vec.get_i64(0), Some(10));
    assert_eq!(vec.get_i64(1), Some(15));
    assert_eq!(vec.get_i64(2), Some(20));
    assert_eq!(vec.get_i64(99), Some(10 + 99 * 5));
}

#[test]
fn test_flatten_constant() {
    let mut vec = crate::test_utils::test_constant::<i32>(LogicalType::Integer, 7, 5);
    vec.try_flatten().unwrap();

    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i32(0), Some(7));
    assert_eq!(vec.get_i32(4), Some(7));
}

#[test]
fn test_flatten_sequence() {
    let mut vec = crate::test_utils::test_sequence(0, 1, 5);
    vec.try_flatten().unwrap();

    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i64(0), Some(0));
    assert_eq!(vec.get_i64(1), Some(1));
    assert_eq!(vec.get_i64(4), Some(4));
}

#[test]
fn test_string_vector() {
    let vec = crate::test_utils::test_string_vector(&["hello", "world", "test"]);
    assert_eq!(vec.len(), 3);
    assert_eq!(vec.get_string(0), Some("hello"));
    assert_eq!(vec.get_string(1), Some("world"));
    assert_eq!(vec.get_string(2), Some("test"));
}

#[test]
fn test_dictionary_vector_zero_copy() {
    // Create source data
    let data = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40, 50]));

    // Create dictionary selecting indices 1, 3 (values 20, 40)
    let dict = crate::test_utils::test_dictionary(data.clone(), vec![1, 3]);

    assert_eq!(dict.len(), 2);
    assert_eq!(dict.vector_type(), VectorType::Dictionary);
    assert_eq!(dict.get_i64(0), Some(20));
    assert_eq!(dict.get_i64(1), Some(40));

    // Verify zero-copy: Arc count should be 2 (original + dictionary)
    assert_eq!(Arc::strong_count(&data), 2);
}

#[test]
fn test_dictionary_flatten() {
    let data = Arc::new(crate::test_utils::test_i64_vector(&[100, 200, 300, 400]));
    let mut dict = crate::test_utils::test_dictionary(data, vec![0, 2, 3]);

    assert_eq!(dict.get_i64(0), Some(100));
    assert_eq!(dict.get_i64(1), Some(300));
    assert_eq!(dict.get_i64(2), Some(400));

    // Flatten materializes the data
    dict.try_flatten().unwrap();

    assert_eq!(dict.vector_type(), VectorType::Flat);
    assert_eq!(dict.get_i64(0), Some(100));
    assert_eq!(dict.get_i64(1), Some(300));
    assert_eq!(dict.get_i64(2), Some(400));
}

#[test]
fn test_try_flatten_dictionary_varlen_family_rebuilds_heap() {
    let cases = vec![
        LogicalType::Varchar,
        LogicalType::VarcharCollation("C".to_string()),
        LogicalType::TsVector,
        LogicalType::TsQuery,
        LogicalType::Json,
        LogicalType::Jsonb,
        LogicalType::Blob,
    ];
    let values: [&[u8]; 3] = [
        b"first long varlen value",
        b"second long varlen value",
        b"third long varlen value",
    ];

    for logical_type in cases {
        let source = test_varlen_vector(logical_type.clone(), &values);
        let source_heap_id = source.string_heap().unwrap().allocation_identity();
        let mut dict = crate::test_utils::test_dictionary(Arc::new(source), vec![2, 0]);

        dict.try_flatten().unwrap();

        assert_eq!(dict.vector_type(), VectorType::Flat);
        assert_varlen_value(&dict, &logical_type, 0, values[2]);
        assert_varlen_value(&dict, &logical_type, 1, values[0]);
        assert_ne!(
            dict.string_heap().unwrap().allocation_identity(),
            source_heap_id
        );
    }
}

#[test]
fn test_try_flatten_dictionary_nested_materializes_children() {
    let array_child = crate::test_utils::test_i32_vector(&[1, 2, 3, 4, 5, 6]);
    let array_source =
        crate::test_utils::test_array_vector(LogicalType::Integer, Arc::new(array_child), 3, 2);
    let mut array_dict = crate::test_utils::test_dictionary(Arc::new(array_source), vec![2, 0]);
    array_dict.try_flatten().unwrap();

    assert_eq!(array_dict.vector_type(), VectorType::Flat);
    assert_eq!(
        array_dict.get_value(0),
        Value::Array(
            vec![Value::Integer(5), Value::Integer(6)],
            LogicalType::Integer,
            2
        )
    );
    assert_eq!(
        array_dict.get_value(1),
        Value::Array(
            vec![Value::Integer(1), Value::Integer(2)],
            LogicalType::Integer,
            2
        )
    );
    assert_eq!(array_dict.child().unwrap().len(), 4);

    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut list_source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    list_source.try_set_count(2).unwrap();
    list_source.set_value(
        0,
        &Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer,
        ),
    );
    list_source.set_value(
        1,
        &Value::List(vec![Value::Integer(30)], LogicalType::Integer),
    );
    let mut list_dict = crate::test_utils::test_dictionary(Arc::new(list_source), vec![1, 0]);
    list_dict.try_flatten().unwrap();

    assert_eq!(list_dict.vector_type(), VectorType::Flat);
    assert_eq!(
        list_dict.get_value(0),
        Value::List(vec![Value::Integer(30)], LogicalType::Integer)
    );
    assert_eq!(
        list_dict.get_value(1),
        Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer
        )
    );

    let struct_type = LogicalType::Struct(vec![
        ("id".to_string(), LogicalType::Integer),
        ("name".to_string(), LogicalType::Varchar),
    ]);
    let mut struct_source = crate::test_utils::test_vector_with_capacity(struct_type.clone(), 2);
    struct_source.try_set_count(2).unwrap();
    struct_source.set_value(
        0,
        &Value::Struct(
            vec![
                Value::Integer(7),
                Value::Varchar("first long value".to_string()),
            ],
            vec![
                ("id".to_string(), LogicalType::Integer),
                ("name".to_string(), LogicalType::Varchar),
            ],
        ),
    );
    struct_source.set_value(
        1,
        &Value::Struct(
            vec![
                Value::Integer(9),
                Value::Varchar("second long value".to_string()),
            ],
            vec![
                ("id".to_string(), LogicalType::Integer),
                ("name".to_string(), LogicalType::Varchar),
            ],
        ),
    );
    let mut struct_dict = crate::test_utils::test_dictionary(Arc::new(struct_source), vec![1, 0]);
    struct_dict.try_flatten().unwrap();

    assert_eq!(struct_dict.vector_type(), VectorType::Flat);
    assert_eq!(
        struct_dict.get_value(0),
        Value::Struct(
            vec![
                Value::Integer(9),
                Value::Varchar("second long value".to_string())
            ],
            vec![
                ("id".to_string(), LogicalType::Integer),
                ("name".to_string(), LogicalType::Varchar),
            ]
        )
    );
    assert_eq!(
        struct_dict.get_value(1),
        Value::Struct(
            vec![
                Value::Integer(7),
                Value::Varchar("first long value".to_string())
            ],
            vec![
                ("id".to_string(), LogicalType::Integer),
                ("name".to_string(), LogicalType::Varchar),
            ]
        )
    );
}

#[test]
fn test_try_flatten_dictionary_array_varchar_rehomes_child_heap() {
    let child = Arc::new(crate::test_utils::test_string_vector(&[
        "first long child value",
        "second long child value",
        "third long child value",
        "fourth long child value",
    ]));
    let array_source =
        crate::test_utils::test_array_vector(LogicalType::Varchar, child.clone(), 2, 2);
    let source = Arc::new(array_source);
    let source_child = source.child().unwrap().clone();
    let source_heap = source_child
        .string_heap()
        .expect("source child heap")
        .allocation_identity();
    let mut dict = crate::test_utils::test_dictionary(source, vec![1_u32, 0]);

    dict.try_flatten().unwrap();

    let dest_child = dict.child().expect("flattened array child");
    assert!(!Arc::ptr_eq(dest_child, &source_child));
    assert_ne!(
        dest_child
            .string_heap()
            .expect("destination child heap")
            .allocation_identity(),
        source_heap
    );
    assert_eq!(
        dict.get_value(0),
        Value::Array(
            vec![
                Value::Varchar("third long child value".to_string()),
                Value::Varchar("fourth long child value".to_string()),
            ],
            LogicalType::Varchar,
            2
        )
    );
}

#[test]
fn test_try_flatten_dictionary_list_varchar_rehomes_child_heap() {
    let list_type = LogicalType::List(Box::new(LogicalType::Varchar));
    let mut list_source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    list_source.try_set_count(2).unwrap();
    list_source.set_value(
        0,
        &Value::List(
            vec![
                Value::Varchar("first long list value".to_string()),
                Value::Varchar("second long list value".to_string()),
            ],
            LogicalType::Varchar,
        ),
    );
    list_source.set_value(
        1,
        &Value::List(
            vec![Value::Varchar("third long list value".to_string())],
            LogicalType::Varchar,
        ),
    );

    let source = Arc::new(list_source);
    let source_child = source.child().unwrap().clone();
    let source_heap = source_child
        .string_heap()
        .expect("source child heap")
        .allocation_identity();
    let mut dict = crate::test_utils::test_dictionary(source, vec![1_u32, 0]);

    dict.try_flatten().unwrap();

    let dest_child = dict.child().expect("flattened list child");
    assert!(!Arc::ptr_eq(dest_child, &source_child));
    assert_ne!(
        dest_child
            .string_heap()
            .expect("destination child heap")
            .allocation_identity(),
        source_heap
    );
    assert_eq!(
        dict.get_value(0),
        Value::List(
            vec![Value::Varchar("third long list value".to_string())],
            LogicalType::Varchar
        )
    );
}

#[test]
fn test_try_flatten_dictionary_struct_varchar_rehomes_field_heap() {
    let fields = vec![
        ("id".to_string(), LogicalType::Integer),
        ("name".to_string(), LogicalType::Varchar),
    ];
    let struct_type = LogicalType::Struct(fields.clone());
    let mut struct_source = crate::test_utils::test_vector_with_capacity(struct_type, 2);
    struct_source.try_set_count(2).unwrap();
    struct_source.set_value(
        0,
        &Value::Struct(
            vec![
                Value::Integer(1),
                Value::Varchar("first long struct field".to_string()),
            ],
            fields.clone(),
        ),
    );
    struct_source.set_value(
        1,
        &Value::Struct(
            vec![
                Value::Integer(2),
                Value::Varchar("second long struct field".to_string()),
            ],
            fields.clone(),
        ),
    );

    let source = Arc::new(struct_source);
    let source_name = source.children().unwrap()[1].clone();
    let source_heap = source_name
        .string_heap()
        .expect("source field heap")
        .allocation_identity();
    let mut dict = crate::test_utils::test_dictionary(source, vec![1_u32, 0]);

    dict.try_flatten().unwrap();

    let dest_name = &dict.children().expect("flattened struct children")[1];
    assert!(!Arc::ptr_eq(dest_name, &source_name));
    assert_ne!(
        dest_name
            .string_heap()
            .expect("destination field heap")
            .allocation_identity(),
        source_heap
    );
    assert_eq!(
        dict.get_value(0),
        Value::Struct(
            vec![
                Value::Integer(2),
                Value::Varchar("second long struct field".to_string()),
            ],
            fields
        )
    );
}

#[test]
fn test_dictionary_collapses_nested_dictionary() {
    let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
    let dict = Arc::new(crate::test_utils::test_dictionary(base, vec![3, 1, 2]));
    let nested =
        crate::test_utils::test_dictionary(dict, crate::test_utils::test_selection(vec![1, 2]));

    assert_eq!(nested.vector_type(), VectorType::Dictionary);
    assert_eq!(nested.get_i64(0), Some(20));
    assert_eq!(nested.get_i64(1), Some(30));

    let child = nested.child().expect("dictionary child");
    assert_eq!(child.vector_type(), VectorType::Flat);
}

#[test]
fn test_dictionary_keeps_shared_selection_allocation() {
    let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
    let mut selection = crate::test_utils::test_selection(vec![3, 1, 0]);
    let allocation = selection.allocation_identity();

    let dict = crate::test_utils::test_dictionary(base, &selection);

    assert_eq!(
        dict.sel_vector()
            .expect("dictionary selection should exist")
            .allocation_identity(),
        allocation
    );

    selection.set(0, 2);
    assert_eq!(selection.as_slice(), &[2, 1, 0]);
    assert_eq!(dict.get_i64(0), Some(40));
    assert_eq!(dict.get_i64(1), Some(20));
    assert_eq!(dict.get_i64(2), Some(10));
}

#[test]
fn test_dictionary_marks_generic_selection_source() {
    let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
    let dict = crate::test_utils::test_dictionary(base, vec![3, 1, 0]);

    let info = dict
        .dictionary_info()
        .expect("dictionary info should exist");
    assert_eq!(info.unique_len, 4);
    assert_eq!(info.provenance_id, None);
    assert_eq!(info.source, DictionarySource::GenericSelection);
}

#[test]
fn test_generic_dictionary_overlay_strips_storage_provenance() {
    let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
    let storage_dict = Arc::new(crate::test_utils::test_with_dictionary(
        base,
        vec![3, 1, 2],
        DictionaryInfo {
            unique_len: 4,
            provenance_id: Some(7),
            source: DictionarySource::Storage,
        },
    ));
    let nested = crate::test_utils::test_dictionary(
        storage_dict,
        crate::test_utils::test_selection(vec![1, 2]),
    );

    let info = nested
        .dictionary_info()
        .expect("nested dictionary info should exist");
    assert_eq!(info.unique_len, 4);
    assert_eq!(info.provenance_id, None);
    assert_eq!(info.source, DictionarySource::GenericSelection);
}

#[test]
fn test_dictionary_string_zero_copy() {
    let data = Arc::new(crate::test_utils::test_string_vector(&[
        "apple", "banana", "cherry",
    ]));
    let dict = crate::test_utils::test_dictionary(data.clone(), vec![2, 0]);

    assert_eq!(dict.len(), 2);
    assert_eq!(dict.get_string(0), Some("cherry"));
    assert_eq!(dict.get_string(1), Some("apple"));
}

#[test]
fn test_slice_ref_uses_range_selection_without_materializing() {
    let vector = crate::test_utils::test_i64_vector(&[10, 20, 30, 40, 50]);

    let sliced = vector
        .slice_ref(1, 3)
        .expect("range slice should not allocate selection");

    assert_eq!(sliced.vector_type(), VectorType::Dictionary);
    assert!(sliced.sel_vector().is_none());
    assert!(matches!(
        sliced.selection(),
        VectorSelection::Range {
            offset: 1,
            count: 3
        }
    ));
    assert_eq!(sliced.get_i64(0), Some(20));
    assert_eq!(sliced.get_i64(2), Some(40));
}

#[test]
fn test_slice_ref_composes_nested_ranges_without_materializing() {
    let base = crate::test_utils::test_i64_vector(&[10, 20, 30, 40, 50]);
    let sliced = Arc::new(base.slice_ref(1, 4).expect("first range slice"));

    let nested = sliced.slice_ref(1, 2).expect("nested range slice");

    assert_eq!(
        nested.child().expect("dictionary child").vector_type(),
        VectorType::Flat
    );
    assert!(matches!(
        nested.selection(),
        VectorSelection::Range {
            offset: 2,
            count: 2
        }
    ));
    assert_eq!(nested.get_i64(0), Some(30));
    assert_eq!(nested.get_i64(1), Some(40));
}

#[test]
fn test_range_materialized_composition_materializes_once() {
    let base = crate::test_utils::test_i64_vector(&[10, 20, 30, 40, 50]);
    let range = Arc::new(base.slice_ref(1, 4).expect("range slice"));
    let selection = crate::test_utils::test_selection(vec![3, 1]);

    let gathered = Vector::try_gather_ref(range, selection).expect("range gather");

    let materialized = gathered
        .sel_vector()
        .expect("range plus materialized selection should materialize");
    assert_eq!(materialized.as_slice(), &[4, 2]);
    assert_eq!(gathered.get_i64(0), Some(50));
    assert_eq!(gathered.get_i64(1), Some(30));
}

#[test]
fn test_embedding_vector() {
    let embeddings = vec![vec![0.1f32, 0.2, 0.3, 0.4], vec![0.5, 0.6, 0.7, 0.8]];

    let vec = crate::test_utils::test_embeddings_vector(&embeddings, 4);

    assert_eq!(vec.len(), 2);
    assert_eq!(vec.vector_type(), VectorType::Flat);

    match vec.logical_type() {
        LogicalType::Array(elem, dim) => {
            assert_eq!(**elem, LogicalType::Float);
            assert_eq!(*dim, 4);
        }
        _ => panic!("Expected Array type"),
    }

    let child = vec.child().expect("Should have child");
    assert_eq!(child.len(), 8);
}

#[test]
fn test_f32_vector() {
    let vec = crate::test_utils::test_f32_vector(&[1.5f32, 2.5, 3.5]);
    assert_eq!(vec.len(), 3);
    assert_eq!(vec.logical_type(), &LogicalType::Float);
}

#[test]
fn test_array_from_child() {
    let child = Arc::new(crate::test_utils::test_i32_vector(&[
        1, 2, 3, 4, 5, 6, 7, 8, 9,
    ]));

    let arr = crate::test_utils::test_array_vector(LogicalType::Integer, child.clone(), 3, 3);

    assert_eq!(arr.len(), 3);
    match arr.logical_type() {
        LogicalType::Array(elem, dim) => {
            assert_eq!(**elem, LogicalType::Integer);
            assert_eq!(*dim, 3);
        }
        _ => panic!("Expected Array type"),
    }

    assert_eq!(Arc::strong_count(&child), 2);
}

#[test]
fn test_vector_shallow_clone() {
    let mut vec1 = crate::test_utils::test_i64_vector(&[10, 20, 30]);
    let vec2 = vec1.clone();

    unsafe {
        let data = vec1.flat_data_mut::<i64>();
        *data.add(0) = 42;
    }

    // `flat_data_mut` performs copy-on-write when buffers are shared.
    assert_eq!(vec2.get_i64(0), Some(10));
    assert_eq!(vec1.get_i64(0), Some(42));
}

#[test]
fn test_vector_reference_explicit() {
    let vec1 = crate::test_utils::test_i64_vector(&[1, 2, 3]);
    let vec2 = vec1.reference();

    assert_eq!(vec1.buffer.data(), vec2.buffer.data());
}

#[test]
fn test_vector_make_exclusive() {
    let mut vec = crate::test_utils::test_i32_vector(&[1, 2, 3]);
    let vec_ref = vec.reference();

    assert_eq!(vec.buffer.data(), vec_ref.buffer.data());

    vec.make_exclusive();

    assert_ne!(vec.buffer.data(), vec_ref.buffer.data());
    assert_eq!(vec.get_i32(0), Some(1));
}

#[test]
fn test_string_vector_short_strings() {
    let vec = crate::test_utils::test_string_vector(&["hi", "abc", "test", "hello world"]);
    assert_eq!(vec.len(), 4);
    assert_eq!(vec.get_string(0), Some("hi"));
    assert_eq!(vec.get_string(1), Some("abc"));
    assert_eq!(vec.get_string(2), Some("test"));
    assert_eq!(vec.get_string(3), Some("hello world"));
}

#[test]
fn test_string_vector_long_strings() {
    let vec = crate::test_utils::test_string_vector(&[
        "this is a very long string",
        "another long string here",
        "yet another long string for testing",
    ]);
    assert_eq!(vec.len(), 3);
    assert_eq!(vec.get_string(0), Some("this is a very long string"));
    assert_eq!(vec.get_string(1), Some("another long string here"));
    assert_eq!(
        vec.get_string(2),
        Some("yet another long string for testing")
    );
}

#[test]
fn test_string_vector_mixed_lengths() {
    let vec = crate::test_utils::test_string_vector(&[
        "short",
        "123456789012",
        "1234567890123",
        "this is definitely a long string",
    ]);

    assert_eq!(vec.len(), 4);
    assert_eq!(vec.get_string(0), Some("short"));
    assert_eq!(vec.get_string(1), Some("123456789012"));
    assert_eq!(vec.get_string(2), Some("1234567890123"));
    assert_eq!(vec.get_string(3), Some("this is definitely a long string"));
}

#[test]
fn test_string_vector_with_nulls() {
    let vec = crate::test_utils::test_nullable_string_vector(&[
        Some("hello"),
        None,
        Some("world"),
        None,
        Some("test"),
    ]);

    assert_eq!(vec.len(), 5);
    assert_eq!(vec.get_string(0), Some("hello"));
    assert_eq!(vec.get_string(1), None);
    assert!(vec.is_null(1));
    assert_eq!(vec.get_string(2), Some("world"));
    assert_eq!(vec.get_string(3), None);
    assert!(vec.is_null(3));
    assert_eq!(vec.get_string(4), Some("test"));
}

#[test]
fn test_string_vector_empty_strings() {
    let vec = crate::test_utils::test_string_vector(&["", "a", "", "bc", ""]);
    assert_eq!(vec.len(), 5);
    assert_eq!(vec.get_string(0), Some(""));
    assert_eq!(vec.get_string(1), Some("a"));
    assert_eq!(vec.get_string(2), Some(""));
    assert_eq!(vec.get_string(3), Some("bc"));
    assert_eq!(vec.get_string(4), Some(""));
}

#[test]
fn test_string_vector_set_values() {
    let mut vec = crate::test_utils::test_vector_with_capacity(LogicalType::Varchar, 3);
    vec.set_count(3);

    vec.set_string(0, "hello");
    vec.set_string(1, "this is a very long string");
    vec.set_string(2, "world");

    assert_eq!(vec.get_string(0), Some("hello"));
    assert_eq!(vec.get_string(1), Some("this is a very long string"));
    assert_eq!(vec.get_string(2), Some("world"));
}

#[test]
fn test_try_set_string_short_does_not_allocate_heap() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut vec = Vector::try_new(LogicalType::Varchar, 1, allocator.clone()).unwrap();
    vec.try_set_count(1).unwrap();

    allocator.set_fail(true);
    vec.try_set_string(0, "short").unwrap();

    assert_eq!(vec.get_string(0), Some("short"));
    assert!(vec.string_heap().is_none());
}

#[test]
fn test_try_set_string_long_propagates_heap_creation_failure() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut vec = Vector::try_new(LogicalType::Varchar, 1, allocator.clone()).unwrap();
    vec.try_set_count(1).unwrap();

    allocator.set_fail(true);
    let err = vec
        .try_set_string(0, "this long string must allocate in the vector heap")
        .unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
    assert!(vec.string_heap().is_none());
}

#[test]
fn test_try_set_string_rebuilds_shared_heap_and_preserves_values() {
    let mut vec = crate::test_utils::test_vector_with_capacity(LogicalType::Varchar, 2);
    vec.try_set_count(2).unwrap();
    vec.try_set_string(0, "first long string that lives in the heap")
        .unwrap();
    vec.try_set_string(1, "second long string that must survive heap cow")
        .unwrap();

    let shared = vec.clone();
    vec.try_set_string(0, "replacement long string that forces heap cow")
        .unwrap();
    drop(shared);

    assert_eq!(
        vec.get_string(0),
        Some("replacement long string that forces heap cow")
    );
    assert_eq!(
        vec.get_string(1),
        Some("second long string that must survive heap cow")
    );
}

#[test]
fn test_try_set_string_shared_heap_cow_failure_preserves_vector() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut vec = Vector::try_new(LogicalType::Varchar, 2, allocator.clone()).unwrap();
    vec.try_set_count(2).unwrap();
    vec.try_set_string(0, "first long string that lives in the heap")
        .unwrap();
    vec.try_set_string(1, "second long string that must survive failed cow")
        .unwrap();

    let shared = vec.clone();
    allocator.set_fail(true);
    let err = vec
        .try_set_string(0, "replacement long string that should fail")
        .unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
    assert_eq!(
        vec.get_string(0),
        Some("first long string that lives in the heap")
    );
    assert_eq!(
        vec.get_string(1),
        Some("second long string that must survive failed cow")
    );
    drop(shared);
}

#[test]
fn test_try_make_exclusive_propagates_buffer_cow_failure() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut vec = Vector::try_new(LogicalType::Integer, 1, allocator.clone()).unwrap();
    let _shared = vec.clone();

    allocator.set_fail(true);
    let err = vec.try_make_exclusive().unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_set_count_shrink_keeps_validity_capacity() {
    let mut vec = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 10);
    vec.try_set_count(10).unwrap();
    let validity_len = vec.validity().len();

    vec.try_set_count(3).unwrap();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec.validity().len(), validity_len);
}

#[test]
fn test_try_set_count_growth_propagates_validity_allocation_error() {
    let allocator = Arc::new(ToggleAllocator::new());
    let mut vec = Vector::try_new(LogicalType::Null, 64, allocator.clone()).unwrap();
    vec.try_set_null(1, true).unwrap();

    allocator.set_fail(true);
    let err = vec.try_set_count(129).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
    assert_eq!(vec.len(), 0);
    assert_eq!(vec.validity().len(), 64);
}

#[test]
fn test_string_vector_unicode() {
    let vec = crate::test_utils::test_string_vector(&[
        "你好",       // Chinese
        "こんにちは", // Japanese
        "🎉🎊🎁",     // Emojis
        "Привет",     // Russian
    ]);

    assert_eq!(vec.len(), 4);
    assert_eq!(vec.get_string(0), Some("你好"));
    assert_eq!(vec.get_string(1), Some("こんにちは"));
    assert_eq!(vec.get_string(2), Some("🎉🎊🎁"));
    assert_eq!(vec.get_string(3), Some("Привет"));
}

#[test]
fn test_string_vector_pointer_stability() {
    let mut vec = crate::test_utils::test_vector_with_capacity(LogicalType::Varchar, 100);
    vec.set_count(100);

    for i in 0..100 {
        vec.set_string(i, &format!("long_string_number_{:04}", i));
    }

    for i in 0..100 {
        let expected = format!("long_string_number_{:04}", i);
        assert_eq!(vec.get_string(i), Some(expected.as_str()));
    }
}

#[test]
fn test_string_copy_at() {
    let src = crate::test_utils::test_string_vector(&["alpha", "beta", "gamma"]);
    let mut dst = crate::test_utils::test_vector_with_capacity(LogicalType::Varchar, 3);
    dst.set_count(3);

    dst.try_copy_at(0, &src, 2).unwrap(); // gamma
    dst.try_copy_at(1, &src, 0).unwrap(); // alpha
    dst.try_copy_at(2, &src, 1).unwrap(); // beta

    assert_eq!(dst.get_string(0), Some("gamma"));
    assert_eq!(dst.get_string(1), Some("alpha"));
    assert_eq!(dst.get_string(2), Some("beta"));
}

#[test]
fn test_try_copy_accepts_null_source_for_typed_destination() {
    let src = crate::test_utils::test_constant_null(LogicalType::Null, 3);
    let mut dst = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);
    dst.try_set_count(3).unwrap();

    dst.try_copy_at(0, &src, 0).unwrap();
    dst.try_copy_range(1, &src, 1, 2).unwrap();

    assert!(dst.is_null(0));
    assert!(dst.is_null(1));
    assert!(dst.is_null(2));
    assert_eq!(dst.len(), 3);
}

#[test]
fn test_try_merge_compact_uses_explicit_allocator() {
    let true_vec = crate::test_utils::test_i32_vector(&[1, 2]);
    let false_vec = crate::test_utils::test_i32_vector(&[10]);
    let mask = [true, false, true];

    let result = Vector::try_merge(
        LogicalType::Integer,
        mask.len(),
        &mask,
        &true_vec,
        &false_vec,
        crate::test_utils::test_allocator(),
    )
    .unwrap();

    assert_eq!(result.get_i32(0), Some(1));
    assert_eq!(result.get_i32(1), Some(10));
    assert_eq!(result.get_i32(2), Some(2));
}

#[test]
fn test_try_merge_full_varlen_family_rebuilds_heap() {
    let cases = vec![
        LogicalType::Varchar,
        LogicalType::VarcharCollation("C".to_string()),
        LogicalType::TsVector,
        LogicalType::TsQuery,
        LogicalType::Json,
        LogicalType::Jsonb,
        LogicalType::Blob,
    ];
    let true_values: [&[u8]; 3] = [
        b"true zero long value",
        b"true one long value",
        b"true two long value",
    ];
    let false_values: [&[u8]; 3] = [
        b"false zero long value",
        b"false one long value",
        b"false two long value",
    ];
    let mask = [true, false, true];

    for logical_type in cases {
        let true_vec = test_varlen_vector(logical_type.clone(), &true_values);
        let false_vec = test_varlen_vector(logical_type.clone(), &false_values);
        let true_heap_id = true_vec.string_heap().unwrap().allocation_identity();
        let false_heap_id = false_vec.string_heap().unwrap().allocation_identity();

        let result = Vector::try_merge_full(
            logical_type.clone(),
            mask.len(),
            &mask,
            &true_vec,
            &false_vec,
            crate::test_utils::test_allocator(),
        )
        .unwrap();

        assert_varlen_value(&result, &logical_type, 0, true_values[0]);
        assert_varlen_value(&result, &logical_type, 1, false_values[1]);
        assert_varlen_value(&result, &logical_type, 2, true_values[2]);
        let result_heap_id = result.string_heap().unwrap().allocation_identity();
        assert_ne!(result_heap_id, true_heap_id);
        assert_ne!(result_heap_id, false_heap_id);
    }
}

#[test]
fn test_try_merge_full_long_varlen_propagates_heap_allocation_error() {
    let true_vec = test_varlen_vector(LogicalType::Blob, &[b"true long blob value"]);
    let false_vec = test_varlen_vector(LogicalType::Blob, &[b"false long blob value"]);
    let allocator = Arc::new(FailAfterAllocator::new(1));

    let err = Vector::try_merge_full(
        LogicalType::Blob,
        1,
        &[true],
        &true_vec,
        &false_vec,
        allocator,
    )
    .unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_slice_uses_range_copy_for_flat_values() {
    let source = crate::test_utils::test_i32_vector(&[10, 20, 30, 40]);
    let mut dest = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 2);

    dest.try_slice(&source, 1, 3).unwrap();

    assert_eq!(dest.len(), 2);
    assert_eq!(dest.get_i32(0), Some(20));
    assert_eq!(dest.get_i32(1), Some(30));
}

#[test]
fn test_try_slice_array_recurses_with_fallible_copy() {
    let child = crate::test_utils::test_i32_vector(&[1, 2, 3, 4, 5, 6]);
    let source = crate::test_utils::test_array_vector(LogicalType::Integer, Arc::new(child), 3, 2);
    let mut dest =
        Vector::try_new(source.logical_type().clone(), 2, source.allocator().clone()).unwrap();

    dest.try_slice(&source, 1, 3).unwrap();

    assert_eq!(dest.len(), 2);
    assert_eq!(dest.child().unwrap().len(), 4);
    assert_eq!(
        dest.get_value(0),
        Value::Array(
            vec![Value::Integer(3), Value::Integer(4)],
            LogicalType::Integer,
            2
        )
    );
    assert_eq!(
        dest.get_value(1),
        Value::Array(
            vec![Value::Integer(5), Value::Integer(6)],
            LogicalType::Integer,
            2
        )
    );
}

#[test]
fn test_try_copy_selection_range_avoids_selection_materialization() {
    let source = crate::test_utils::test_i32_vector(&[10, 20, 30, 40]);
    let allocator = Arc::new(ToggleAllocator::new());
    let mut dest = Vector::try_new(LogicalType::Integer, 2, allocator.clone()).unwrap();

    allocator.set_fail(true);
    dest.try_copy_selection(0, &source, &VectorSelection::range(1, 2), 2)
        .unwrap();

    assert_eq!(dest.len(), 2);
    assert_eq!(dest.get_i32(0), Some(20));
    assert_eq!(dest.get_i32(1), Some(30));
}

#[test]
fn test_try_copy_selection_materialized_rows() {
    let source = crate::test_utils::test_i32_vector(&[10, 20, 30, 40]);
    let selection = crate::test_utils::test_selection(vec![3, 1, 2]);
    let mut dest = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);

    dest.try_copy_selection(0, &source, &VectorSelection::materialized(selection), 3)
        .unwrap();

    assert_eq!(dest.len(), 3);
    assert_eq!(dest.get_i32(0), Some(40));
    assert_eq!(dest.get_i32(1), Some(20));
    assert_eq!(dest.get_i32(2), Some(30));
}

#[test]
fn test_try_copy_selection_ref_owned_and_constant_rows() {
    let source = crate::test_utils::test_i32_vector(&[10, 20, 30]);
    let mut selected = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 2);

    selected
        .try_copy_selection_ref(
            0,
            &source,
            SelectionRef::Owned(crate::test_utils::test_selection(vec![2, 0])),
            2,
        )
        .unwrap();

    assert_eq!(selected.get_i32(0), Some(30));
    assert_eq!(selected.get_i32(1), Some(10));

    let mut repeated = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);
    repeated
        .try_copy_selection_ref(0, &source, SelectionRef::Constant { count: 3 }, 3)
        .unwrap();

    assert_eq!(repeated.len(), 3);
    assert_eq!(repeated.get_i32(0), Some(10));
    assert_eq!(repeated.get_i32(1), Some(10));
    assert_eq!(repeated.get_i32(2), Some(10));
}

#[test]
fn test_try_copy_selection_deep_dictionary_composition_materializes_fallibly() {
    let allocator = crate::test_utils::test_allocator();
    let source = crate::test_utils::test_i32_vector(&[10, 20, 30, 40]);
    let inner_selection = crate::test_utils::test_selection(vec![2, 0, 3, 1]);
    let inner = Vector {
        vector_type: VectorType::Dictionary,
        logical_type: LogicalType::Integer,
        buffer: VectorBuffer::try_with_allocator(0, 0, allocator.clone()).unwrap(),
        validity: ValidityMask::with_allocator(inner_selection.len(), allocator.clone()),
        count: inner_selection.len(),
        selection: VectorSelection::materialized(inner_selection),
        child: Some(Arc::new(source)),
        children: Vec::new(),
        string_heap: None,
        dictionary_info: Some(DictionaryInfo {
            unique_len: 4,
            provenance_id: None,
            source: DictionarySource::GenericSelection,
        }),
    };
    let outer_selection = crate::test_utils::test_selection(vec![1, 3, 0]);
    let dictionary = Vector {
        vector_type: VectorType::Dictionary,
        logical_type: LogicalType::Integer,
        buffer: VectorBuffer::try_with_allocator(0, 0, allocator.clone()).unwrap(),
        validity: ValidityMask::with_allocator(outer_selection.len(), allocator.clone()),
        count: outer_selection.len(),
        selection: VectorSelection::materialized(outer_selection),
        child: Some(Arc::new(inner)),
        children: Vec::new(),
        string_heap: None,
        dictionary_info: Some(DictionaryInfo {
            unique_len: 4,
            provenance_id: None,
            source: DictionarySource::GenericSelection,
        }),
    };
    let overlay = crate::test_utils::test_selection(vec![2, 0]);
    let mut selected = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 2);

    selected
        .try_copy_selection_ref(0, &dictionary, SelectionRef::Borrowed(&overlay), 2)
        .unwrap();

    assert_eq!(selected.get_i32(0), Some(30));
    assert_eq!(selected.get_i32(1), Some(10));
}

#[test]
fn test_try_copy_scatter_destination_positions() {
    let mut source = crate::test_utils::test_i32_vector(&[10, 20, 30, 40]);
    source.try_set_null(2, true).unwrap();
    let mut dest = crate::test_utils::test_vector_with_capacity(LogicalType::Integer, 5);

    dest.try_copy_scatter(&source, 1, &[3, 0, 4]).unwrap();

    assert_eq!(dest.len(), 5);
    assert_eq!(dest.get_i32(3), Some(20));
    assert!(dest.is_null(0));
    assert_eq!(dest.get_i32(4), Some(40));
}

#[test]
fn test_try_copy_selection_resolves_nested_dictionary_sources() {
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    source.try_set_count(2).unwrap();
    source.set_value(
        0,
        &Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer,
        ),
    );
    source.set_value(
        1,
        &Value::List(vec![Value::Integer(30)], LogicalType::Integer),
    );
    let dictionary = crate::test_utils::test_dictionary(Arc::new(source), vec![1, 0]);
    let mut dest = Vector::try_new(list_type, 2, dictionary.allocator().clone()).unwrap();

    dest.try_copy_selection(0, &dictionary, &VectorSelection::range(0, 2), 2)
        .unwrap();

    assert_eq!(
        dest.get_value(0),
        Value::List(vec![Value::Integer(30)], LogicalType::Integer)
    );
    assert_eq!(
        dest.get_value(1),
        Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer
        )
    );
}

#[test]
fn test_try_copy_list_range_handles_null_empty_and_dictionary_rows() {
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 3);
    source.try_set_count(3).unwrap();
    source.set_value(
        0,
        &Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer,
        ),
    );
    source.set_value(1, &Value::List(vec![], LogicalType::Integer));
    source.set_value(
        2,
        &Value::List(vec![Value::Integer(30)], LogicalType::Integer),
    );
    source.try_set_null(2, true).unwrap();

    let dictionary = crate::test_utils::test_dictionary(Arc::new(source), vec![2, 1, 0]);
    let mut dest = Vector::try_new(list_type.clone(), 3, dictionary.allocator().clone()).unwrap();

    dest.try_copy_range(0, &dictionary, 0, 3).unwrap();

    assert_eq!(dest.get_value(0), Value::Null(list_type));
    assert_eq!(dest.get_value(1), Value::List(vec![], LogicalType::Integer));
    assert_eq!(
        dest.get_value(2),
        Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer
        )
    );
    assert_eq!(dest.child().unwrap().len(), 2);
}

#[test]
fn test_try_copy_nested_list_range_recurses_in_child_payload() {
    let inner_type = LogicalType::List(Box::new(LogicalType::Integer));
    let nested_type = LogicalType::List(Box::new(inner_type.clone()));
    let mut source = crate::test_utils::test_vector_with_capacity(nested_type.clone(), 2);
    source.try_set_count(2).unwrap();
    source.set_value(
        0,
        &Value::List(
            vec![
                Value::List(
                    vec![Value::Integer(1), Value::Integer(2)],
                    LogicalType::Integer,
                ),
                Value::List(vec![Value::Integer(3)], LogicalType::Integer),
            ],
            inner_type.clone(),
        ),
    );
    source.set_value(
        1,
        &Value::List(
            vec![
                Value::List(vec![], LogicalType::Integer),
                Value::List(
                    vec![Value::Integer(4), Value::Integer(5)],
                    LogicalType::Integer,
                ),
            ],
            inner_type.clone(),
        ),
    );

    let mut dest = Vector::try_new(nested_type, 2, source.allocator().clone()).unwrap();
    dest.try_copy_range(0, &source, 0, 2).unwrap();

    assert_eq!(dest.get_value(0), source.get_value(0));
    assert_eq!(dest.get_value(1), source.get_value(1));
    assert_eq!(dest.child().unwrap().len(), 4);
    assert_eq!(dest.child().unwrap().child().unwrap().len(), 5);
}

#[test]
fn test_try_copy_list_selection_uses_borrowed_selection_without_allocation() {
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    source.try_set_count(2).unwrap();
    source.set_value(
        0,
        &Value::List(vec![Value::Integer(10)], LogicalType::Integer),
    );
    source.set_value(
        1,
        &Value::List(vec![Value::Integer(20)], LogicalType::Integer),
    );
    let selection = crate::test_utils::test_selection(vec![1, 0]);

    let allocator = Arc::new(ToggleAllocator::new());
    let mut dest = Vector::try_new(list_type, 2, allocator.clone()).unwrap();
    allocator.set_fail(true);

    dest.try_copy_selection(0, &source, &VectorSelection::materialized(selection), 2)
        .unwrap();

    assert_eq!(
        dest.get_value(0),
        Value::List(vec![Value::Integer(20)], LogicalType::Integer)
    );
    assert_eq!(
        dest.get_value(1),
        Value::List(vec![Value::Integer(10)], LogicalType::Integer)
    );
}

#[test]
fn test_try_copy_list_scatter_rejects_initialized_overwrite() {
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut source = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    source.try_set_count(2).unwrap();
    source.set_value(
        0,
        &Value::List(vec![Value::Integer(10)], LogicalType::Integer),
    );
    source.set_value(
        1,
        &Value::List(vec![Value::Integer(20)], LogicalType::Integer),
    );

    let mut dest = crate::test_utils::test_vector_with_capacity(list_type.clone(), 2);
    dest.try_set_count(1).unwrap();
    dest.set_value(
        0,
        &Value::List(vec![Value::Integer(99)], LogicalType::Integer),
    );

    let err = dest.try_copy_scatter(&source, 0, &[0]).unwrap_err();
    assert!(err.to_string().contains("list scatter cannot overwrite"));

    let mut fresh = Vector::try_new(list_type, 3, source.allocator().clone()).unwrap();
    fresh.try_copy_scatter(&source, 0, &[2, 0]).unwrap();
    assert_eq!(fresh.len(), 3);
    assert_eq!(
        fresh.get_value(2),
        Value::List(vec![Value::Integer(10)], LogicalType::Integer)
    );
    assert_eq!(
        fresh.get_value(0),
        Value::List(vec![Value::Integer(20)], LogicalType::Integer)
    );
}

#[test]
fn test_try_copy_at_list_null_child_growth_uses_logical_capacity() {
    let list_type = LogicalType::List(Box::new(LogicalType::Null));
    let mut source =
        Vector::try_new(list_type.clone(), 1, crate::test_utils::test_allocator()).unwrap();
    source.try_set_count(1).unwrap();
    source.set_value(
        0,
        &Value::List(
            vec![
                Value::Null(LogicalType::Null),
                Value::Null(LogicalType::Null),
            ],
            LogicalType::Null,
        ),
    );

    let mut dest = Vector::try_new(list_type, 1, source.allocator().clone()).unwrap();
    dest.try_copy_at(0, &source, 0).unwrap();

    assert_eq!(
        dest.get_value(0),
        Value::List(
            vec![
                Value::Null(LogicalType::Null),
                Value::Null(LogicalType::Null),
            ],
            LogicalType::Null,
        )
    );
}

#[test]
fn test_try_copy_array_shared_child_cow_failure_is_fallible() {
    let child = crate::test_utils::test_i32_vector(&[1, 2]);
    let source = crate::test_utils::test_array_vector(LogicalType::Integer, Arc::new(child), 1, 2);
    let allocator = Arc::new(ToggleAllocator::new());
    let mut dest = Vector::try_new(source.logical_type().clone(), 1, allocator.clone()).unwrap();
    let _shared_child = Arc::clone(dest.child().expect("array child"));

    allocator.set_fail(true);
    let err = dest.try_copy_range(0, &source, 0, 1).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_copy_varlen_range_append_preserves_existing_rows() {
    let source = crate::test_utils::test_string_vector(&[
        "first appended long string",
        "second appended long string",
    ]);
    let mut dest = Vector::try_new(LogicalType::Varchar, 3, source.allocator().clone()).unwrap();
    dest.try_set_count(1).unwrap();
    dest.try_set_string(0, "existing long string").unwrap();

    dest.try_copy_range(1, &source, 0, 2).unwrap();

    assert_eq!(dest.get_string(0), Some("existing long string"));
    assert_eq!(dest.get_string(1), Some("first appended long string"));
    assert_eq!(dest.get_string(2), Some("second appended long string"));
}

#[test]
fn test_try_copy_varlen_range_append_shared_heap_failure_preserves_vector() {
    let source = crate::test_utils::test_string_vector(&["appended long string"]);
    let allocator = Arc::new(ToggleAllocator::new());
    let mut dest = Vector::try_new(LogicalType::Varchar, 2, allocator.clone()).unwrap();
    dest.try_set_count(1).unwrap();
    dest.try_set_string(0, "existing long string").unwrap();
    let shared = dest.clone();

    allocator.set_fail(true);
    let err = dest.try_copy_range(1, &source, 0, 1).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
    assert_eq!(dest.get_string(0), Some("existing long string"));
    drop(shared);
}

#[test]
fn test_try_copy_array_varchar_range_and_failure_propagation() {
    let child = crate::test_utils::test_string_vector(&[
        "first long string value",
        "second long string value",
        "third long string value",
        "fourth long string value",
    ]);
    let source = crate::test_utils::test_array_vector(LogicalType::Varchar, Arc::new(child), 2, 2);
    let mut dest =
        Vector::try_new(source.logical_type().clone(), 2, source.allocator().clone()).unwrap();

    dest.try_copy_range(0, &source, 0, 2).unwrap();

    assert_eq!(dest.get_value(0), source.get_value(0));
    assert_eq!(dest.get_value(1), source.get_value(1));

    let allocator = Arc::new(ToggleAllocator::new());
    let mut failing = Vector::try_new(source.logical_type().clone(), 2, allocator.clone()).unwrap();
    allocator.set_fail(true);
    let err = failing.try_copy_range(0, &source, 0, 2).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_copy_struct_nested_range_and_failure_propagation() {
    let struct_type = LogicalType::Struct(vec![
        ("name".to_string(), LogicalType::Varchar),
        (
            "items".to_string(),
            LogicalType::List(Box::new(LogicalType::Integer)),
        ),
    ]);
    let fields = vec![
        ("name".to_string(), LogicalType::Varchar),
        (
            "items".to_string(),
            LogicalType::List(Box::new(LogicalType::Integer)),
        ),
    ];
    let mut source = crate::test_utils::test_vector_with_capacity(struct_type.clone(), 2);
    source.try_set_count(2).unwrap();
    source.set_value(
        0,
        &Value::Struct(
            vec![
                Value::Varchar("first long struct string".to_string()),
                Value::List(
                    vec![Value::Integer(1), Value::Integer(2)],
                    LogicalType::Integer,
                ),
            ],
            fields.clone(),
        ),
    );
    source.set_value(
        1,
        &Value::Struct(
            vec![
                Value::Varchar("second long struct string".to_string()),
                Value::List(vec![Value::Integer(3)], LogicalType::Integer),
            ],
            fields.clone(),
        ),
    );
    let mut dest = Vector::try_new(struct_type.clone(), 2, source.allocator().clone()).unwrap();

    dest.try_copy_range(0, &source, 0, 2).unwrap();

    assert_eq!(dest.get_value(0), source.get_value(0));
    assert_eq!(dest.get_value(1), source.get_value(1));

    let allocator = Arc::new(ToggleAllocator::new());
    let mut failing = Vector::try_new(struct_type, 1, allocator.clone()).unwrap();
    allocator.set_fail(true);
    let err = failing.try_copy_range(0, &source, 0, 1).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
#[ignore = "manual targeted benchmark for Phase D nested copy paths"]
fn bench_phase_d_nested_copy_targets() {
    let row_count = VECTOR_SIZE;
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut list_source =
        crate::test_utils::test_vector_with_capacity(list_type.clone(), row_count);
    list_source.try_set_count(row_count).unwrap();
    for row in 0..row_count {
        list_source.set_value(
            row,
            &Value::List(
                (0..(row % 4))
                    .map(|value| Value::Integer((row + value) as i32))
                    .collect(),
                LogicalType::Integer,
            ),
        );
    }

    let start = std::time::Instant::now();
    let mut list_range = Vector::try_new(
        list_type.clone(),
        row_count,
        list_source.allocator().clone(),
    )
    .unwrap();
    list_range
        .try_copy_range(0, &list_source, 0, row_count)
        .unwrap();
    let list_range_elapsed = start.elapsed();

    let selection =
        crate::test_utils::test_selection((0..row_count as u32).rev().collect::<Vec<_>>());
    let start = std::time::Instant::now();
    let mut list_selection =
        Vector::try_new(list_type, row_count, list_source.allocator().clone()).unwrap();
    list_selection
        .try_copy_selection(
            0,
            &list_source,
            &VectorSelection::materialized(selection),
            row_count,
        )
        .unwrap();
    let list_selection_elapsed = start.elapsed();

    let array_strings = (0..row_count * 2)
        .map(|idx| format!("array string value {idx:04}"))
        .collect::<Vec<_>>();
    let array_refs = array_strings.iter().map(String::as_str).collect::<Vec<_>>();
    let array_child = crate::test_utils::test_string_vector(&array_refs);
    let array_source = crate::test_utils::test_array_vector(
        LogicalType::Varchar,
        Arc::new(array_child),
        row_count,
        2,
    );
    let start = std::time::Instant::now();
    let mut array_dest = Vector::try_new(
        array_source.logical_type().clone(),
        row_count,
        array_source.allocator().clone(),
    )
    .unwrap();
    array_dest
        .try_copy_range(0, &array_source, 0, row_count)
        .unwrap();
    let array_elapsed = start.elapsed();

    eprintln!(
        "phase_d_copy list_range={list_range_elapsed:?} list_selection={list_selection_elapsed:?} array_varchar={array_elapsed:?}"
    );
}

#[test]
fn test_try_copy_at_long_string_propagates_allocation_error() {
    let src = crate::test_utils::test_string_vector(&["this string is long enough to allocate"]);
    let allocator = Arc::new(ToggleAllocator::new());
    let mut dst = Vector::try_new(LogicalType::Varchar, 1, allocator.clone()).unwrap();
    dst.try_set_count(1).unwrap();

    allocator.set_fail(true);
    let err = dst.try_copy_at(0, &src, 0).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_copy_at_list_child_growth_propagates_allocation_error() {
    let list_type = LogicalType::List(Box::new(LogicalType::Integer));
    let mut src = crate::test_utils::test_vector_with_capacity(list_type.clone(), 1);
    src.try_set_count(1).unwrap();
    src.set_value(
        0,
        &Value::List(
            vec![Value::Integer(10), Value::Integer(20)],
            LogicalType::Integer,
        ),
    );

    let allocator = Arc::new(ToggleAllocator::new());
    let mut dst = Vector::try_new(list_type, 1, allocator.clone()).unwrap();
    dst.try_set_count(1).unwrap();

    allocator.set_fail(true);
    let err = dst.try_copy_at(0, &src, 0).unwrap_err();

    assert!(err.to_string().contains("injected allocation failure"));
}

#[test]
fn test_try_copy_at_array_out_of_order_preserves_written_extent() {
    let source = crate::test_utils::test_embeddings_vector(
        &[vec![1.0_f32, 2.0, 3.0], vec![4.0_f32, 5.0, 6.0]],
        3,
    );
    let mut destination = Vector::try_new(
        LogicalType::Array(Box::new(LogicalType::Float), 3),
        4,
        source.allocator().clone(),
    )
    .unwrap();
    destination.try_set_count(4).unwrap();

    destination.try_copy_at(3, &source, 0).unwrap();
    destination.try_copy_at(1, &source, 1).unwrap();

    let child = destination.child().expect("array child");
    assert_eq!(child.len(), 12);
    assert_eq!(child.validity().len(), 12);
    assert_eq!(
        destination.get_value(3),
        Value::Array(
            vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
            LogicalType::Float,
            3,
        )
    );
    assert_eq!(
        destination.get_value(1),
        Value::Array(
            vec![Value::Float(4.0), Value::Float(5.0), Value::Float(6.0)],
            LogicalType::Float,
            3,
        )
    );
}
