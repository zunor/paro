// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::LogicalType;
use std::sync::Arc;

#[test]
fn test_flat_vector_i64() {
    let vec = Vector::from_i64(&[1, 2, 3, 4, 5]);
    assert_eq!(vec.len(), 5);
    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i64(0), Some(1));
    assert_eq!(vec.get_i64(4), Some(5));
    assert!(!vec.is_null(0));
}

#[test]
fn test_flat_vector_with_nulls() {
    let mut vec = Vector::from_i32(&[10, 20, 30]);
    vec.validity_mut().set_null(1);

    assert_eq!(vec.get_i32(0), Some(10));
    assert_eq!(vec.get_i32(1), None); // null
    assert_eq!(vec.get_i32(2), Some(30));
    assert!(vec.is_null(1));
}

#[test]
fn test_constant_vector() {
    let vec = Vector::constant::<i64>(LogicalType::BigInt, 42, 1000);
    assert_eq!(vec.len(), 1000);
    assert_eq!(vec.vector_type(), VectorType::Constant);
    assert_eq!(vec.get_i64(0), Some(42));
    assert_eq!(vec.get_i64(999), Some(42));
}

#[test]
fn test_sequence_vector() {
    let vec = Vector::sequence(10, 5, 100);
    assert_eq!(vec.len(), 100);
    assert_eq!(vec.vector_type(), VectorType::Sequence);
    assert_eq!(vec.get_i64(0), Some(10));
    assert_eq!(vec.get_i64(1), Some(15));
    assert_eq!(vec.get_i64(2), Some(20));
    assert_eq!(vec.get_i64(99), Some(10 + 99 * 5));
}

#[test]
fn test_flatten_constant() {
    let mut vec = Vector::constant::<i32>(LogicalType::Integer, 7, 5);
    vec.flatten();

    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i32(0), Some(7));
    assert_eq!(vec.get_i32(4), Some(7));
}

#[test]
fn test_flatten_sequence() {
    let mut vec = Vector::sequence(0, 1, 5);
    vec.flatten();

    assert_eq!(vec.vector_type(), VectorType::Flat);
    assert_eq!(vec.get_i64(0), Some(0));
    assert_eq!(vec.get_i64(1), Some(1));
    assert_eq!(vec.get_i64(4), Some(4));
}

#[test]
fn test_string_vector() {
    let vec = Vector::from_strings(&["hello", "world", "test"]);
    assert_eq!(vec.len(), 3);
    assert_eq!(vec.get_string(0), Some("hello"));
    assert_eq!(vec.get_string(1), Some("world"));
    assert_eq!(vec.get_string(2), Some("test"));
}

#[test]
fn test_dictionary_vector_zero_copy() {
    // Create source data
    let data = Arc::new(Vector::from_i64(&[10, 20, 30, 40, 50]));

    // Create dictionary selecting indices 1, 3 (values 20, 40)
    let dict = Vector::dictionary(data.clone(), vec![1, 3]);

    assert_eq!(dict.len(), 2);
    assert_eq!(dict.vector_type(), VectorType::Dictionary);
    assert_eq!(dict.get_i64(0), Some(20));
    assert_eq!(dict.get_i64(1), Some(40));

    // Verify zero-copy: Arc count should be 2 (original + dictionary)
    assert_eq!(Arc::strong_count(&data), 2);
}

#[test]
fn test_dictionary_flatten() {
    let data = Arc::new(Vector::from_i64(&[100, 200, 300, 400]));
    let mut dict = Vector::dictionary(data, vec![0, 2, 3]);

    assert_eq!(dict.get_i64(0), Some(100));
    assert_eq!(dict.get_i64(1), Some(300));
    assert_eq!(dict.get_i64(2), Some(400));

    // Flatten materializes the data
    dict.flatten();

    assert_eq!(dict.vector_type(), VectorType::Flat);
    assert_eq!(dict.get_i64(0), Some(100));
    assert_eq!(dict.get_i64(1), Some(300));
    assert_eq!(dict.get_i64(2), Some(400));
}

#[test]
fn test_dictionary_collapses_nested_dictionary() {
    let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
    let dict = Arc::new(Vector::dictionary(base, vec![3, 1, 2]));
    let nested = Vector::dictionary(dict, SelectionVector::from_indices(vec![1, 2]));

    assert_eq!(nested.vector_type(), VectorType::Dictionary);
    assert_eq!(nested.get_i64(0), Some(20));
    assert_eq!(nested.get_i64(1), Some(30));

    let child = nested.child().expect("dictionary child");
    assert_eq!(child.vector_type(), VectorType::Flat);
}

#[test]
fn test_dictionary_keeps_shared_selection_allocation() {
    let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
    let mut selection = SelectionVector::from_indices(vec![3, 1, 0]);
    let allocation = selection.allocation_identity();

    let dict = Vector::dictionary(base, &selection);

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
    let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
    let dict = Vector::dictionary(base, vec![3, 1, 0]);

    let info = dict
        .dictionary_info()
        .expect("dictionary info should exist");
    assert_eq!(info.unique_len, 4);
    assert_eq!(info.provenance_id, None);
    assert_eq!(info.source, DictionarySource::GenericSelection);
}

#[test]
fn test_generic_dictionary_overlay_strips_storage_provenance() {
    let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
    let storage_dict = Arc::new(Vector::with_dictionary(
        base,
        vec![3, 1, 2],
        DictionaryInfo {
            unique_len: 4,
            provenance_id: Some(7),
            source: DictionarySource::Storage,
        },
    ));
    let nested = Vector::dictionary(storage_dict, SelectionVector::from_indices(vec![1, 2]));

    let info = nested
        .dictionary_info()
        .expect("nested dictionary info should exist");
    assert_eq!(info.unique_len, 4);
    assert_eq!(info.provenance_id, None);
    assert_eq!(info.source, DictionarySource::GenericSelection);
}

#[test]
fn test_dictionary_string_zero_copy() {
    let data = Arc::new(Vector::from_strings(&["apple", "banana", "cherry"]));
    let dict = Vector::dictionary(data.clone(), vec![2, 0]);

    assert_eq!(dict.len(), 2);
    assert_eq!(dict.get_string(0), Some("cherry"));
    assert_eq!(dict.get_string(1), Some("apple"));
}

#[test]
fn test_embedding_vector() {
    let embeddings = vec![vec![0.1f32, 0.2, 0.3, 0.4], vec![0.5, 0.6, 0.7, 0.8]];

    let vec = Vector::from_embeddings(&embeddings, 4);

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
    let vec = Vector::from_f32(&[1.5f32, 2.5, 3.5]);
    assert_eq!(vec.len(), 3);
    assert_eq!(vec.logical_type(), &LogicalType::Float);
}

#[test]
fn test_array_from_child() {
    let child = Arc::new(Vector::from_i32(&[1, 2, 3, 4, 5, 6, 7, 8, 9]));

    let arr = Vector::from_array(LogicalType::Integer, child.clone(), 3, 3);

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
    let mut vec1 = Vector::from_i64(&[10, 20, 30]);
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
    let vec1 = Vector::from_i64(&[1, 2, 3]);
    let vec2 = vec1.reference();

    assert_eq!(vec1.buffer.data(), vec2.buffer.data());
}

#[test]
fn test_vector_make_exclusive() {
    let mut vec = Vector::from_i32(&[1, 2, 3]);
    let vec_ref = vec.reference();

    assert_eq!(vec.buffer.data(), vec_ref.buffer.data());

    vec.make_exclusive();

    assert_ne!(vec.buffer.data(), vec_ref.buffer.data());
    assert_eq!(vec.get_i32(0), Some(1));
}

#[test]
fn test_string_vector_short_strings() {
    let vec = Vector::from_strings(&["hi", "abc", "test", "hello world"]);
    assert_eq!(vec.len(), 4);
    assert_eq!(vec.get_string(0), Some("hi"));
    assert_eq!(vec.get_string(1), Some("abc"));
    assert_eq!(vec.get_string(2), Some("test"));
    assert_eq!(vec.get_string(3), Some("hello world"));
}

#[test]
fn test_string_vector_long_strings() {
    let vec = Vector::from_strings(&[
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
    let vec = Vector::from_strings(&[
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
    let vec =
        Vector::from_nullable_strings(&[Some("hello"), None, Some("world"), None, Some("test")]);

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
    let vec = Vector::from_strings(&["", "a", "", "bc", ""]);
    assert_eq!(vec.len(), 5);
    assert_eq!(vec.get_string(0), Some(""));
    assert_eq!(vec.get_string(1), Some("a"));
    assert_eq!(vec.get_string(2), Some(""));
    assert_eq!(vec.get_string(3), Some("bc"));
    assert_eq!(vec.get_string(4), Some(""));
}

#[test]
fn test_string_vector_set_values() {
    let mut vec = Vector::with_capacity(LogicalType::Varchar, 3);
    vec.set_count(3);

    vec.set_string(0, "hello");
    vec.set_string(1, "this is a very long string");
    vec.set_string(2, "world");

    assert_eq!(vec.get_string(0), Some("hello"));
    assert_eq!(vec.get_string(1), Some("this is a very long string"));
    assert_eq!(vec.get_string(2), Some("world"));
}

#[test]
fn test_string_vector_unicode() {
    let vec = Vector::from_strings(&[
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
    let mut vec = Vector::with_capacity(LogicalType::Varchar, 100);
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
    let src = Vector::from_strings(&["alpha", "beta", "gamma"]);
    let mut dst = Vector::with_capacity(LogicalType::Varchar, 3);
    dst.set_count(3);

    dst.copy_at(0, &src, 2); // gamma
    dst.copy_at(1, &src, 0); // alpha
    dst.copy_at(2, &src, 1); // beta

    assert_eq!(dst.get_string(0), Some("gamma"));
    assert_eq!(dst.get_string(1), Some("alpha"));
    assert_eq!(dst.get_string(2), Some("beta"));
}
