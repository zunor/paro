// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Test-only constructors for chunk/vector data structures.

use std::sync::Arc;

use crate::allocator::{default_allocator, Allocator};
use crate::chunk::Chunk;
use crate::runtime_value::Value;
use crate::types::LogicalType;
use crate::vector::{DictionaryInfo, SelectionVector, Vector, VECTOR_SIZE};

pub fn test_allocator() -> Arc<dyn Allocator> {
    Arc::new(default_allocator())
}

pub fn test_vector(logical_type: LogicalType) -> Vector {
    test_vector_with_capacity(logical_type, VECTOR_SIZE)
}

pub fn test_vector_with_capacity(logical_type: LogicalType, capacity: usize) -> Vector {
    Vector::try_new(logical_type, capacity, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_i64_vector(values: &[i64]) -> Vector {
    Vector::try_from_i64(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_i64_vector_with_allocator(values: &[i64], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_i64(values, allocator).expect("test vector allocation failed")
}

pub fn test_i32_vector(values: &[i32]) -> Vector {
    Vector::try_from_i32(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_i32_vector_with_allocator(values: &[i32], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_i32(values, allocator).expect("test vector allocation failed")
}

pub fn test_f64_vector(values: &[f64]) -> Vector {
    Vector::try_from_f64(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_f64_vector_with_allocator(values: &[f64], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_f64(values, allocator).expect("test vector allocation failed")
}

pub fn test_f32_vector(values: &[f32]) -> Vector {
    Vector::try_from_f32(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_f32_vector_with_allocator(values: &[f32], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_f32(values, allocator).expect("test vector allocation failed")
}

pub fn test_bool_vector(values: &[bool]) -> Vector {
    Vector::try_from_bool(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_bool_vector_with_allocator(values: &[bool], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_bool(values, allocator).expect("test vector allocation failed")
}

pub fn test_nullable_bool_vector(values: &[Option<bool>]) -> Vector {
    Vector::try_from_nullable_bools(values, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_nullable_u64_vector(values: &[Option<u64>]) -> Vector {
    Vector::try_from_nullable_u64(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_string_vector(values: &[&str]) -> Vector {
    Vector::try_from_strings(values, test_allocator()).expect("test vector allocation failed")
}

pub fn test_string_vector_with_allocator(values: &[&str], allocator: Arc<dyn Allocator>) -> Vector {
    Vector::try_from_strings(values, allocator).expect("test vector allocation failed")
}

pub fn test_nullable_string_vector(values: &[Option<&str>]) -> Vector {
    Vector::try_from_nullable_strings(values, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_embeddings_vector(embeddings: &[Vec<f32>], dimensions: usize) -> Vector {
    Vector::try_from_embeddings(embeddings, dimensions, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_embeddings_vector_with_allocator(
    embeddings: &[Vec<f32>],
    dimensions: usize,
    allocator: Arc<dyn Allocator>,
) -> Vector {
    Vector::try_from_embeddings(embeddings, dimensions, allocator)
        .expect("test vector allocation failed")
}

pub fn test_constant<T: Copy>(logical_type: LogicalType, value: T, count: usize) -> Vector {
    Vector::try_constant(logical_type, value, count, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_constant_with_allocator<T: Copy>(
    logical_type: LogicalType,
    value: T,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Vector {
    Vector::try_constant(logical_type, value, count, allocator)
        .expect("test vector allocation failed")
}

pub fn test_constant_null(logical_type: LogicalType, count: usize) -> Vector {
    Vector::try_constant_null(logical_type, count, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_constant_null_with_allocator(
    logical_type: LogicalType,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Vector {
    Vector::try_constant_null(logical_type, count, allocator)
        .expect("test vector allocation failed")
}

pub fn test_sequence(start: i64, increment: i64, count: usize) -> Vector {
    Vector::try_sequence(start, increment, count, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_sequence_with_allocator(
    start: i64,
    increment: i64,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Vector {
    Vector::try_sequence(start, increment, count, allocator).expect("test vector allocation failed")
}

pub trait IntoTestSelection {
    fn into_test_selection(self) -> SelectionVector;
}

impl IntoTestSelection for SelectionVector {
    fn into_test_selection(self) -> SelectionVector {
        self
    }
}

impl IntoTestSelection for &SelectionVector {
    fn into_test_selection(self) -> SelectionVector {
        self.clone()
    }
}

impl IntoTestSelection for Vec<u32> {
    fn into_test_selection(self) -> SelectionVector {
        test_selection(self)
    }
}

pub fn test_dictionary<S>(child: Arc<Vector>, selection: S) -> Vector
where
    S: IntoTestSelection,
{
    Vector::try_dictionary(child, selection.into_test_selection())
        .expect("test dictionary vector allocation failed")
}

pub fn test_with_dictionary<S>(
    child: Arc<Vector>,
    selection: S,
    dictionary_info: DictionaryInfo,
) -> Vector
where
    S: IntoTestSelection,
{
    Vector::try_with_dictionary(child, selection.into_test_selection(), dictionary_info)
        .expect("test dictionary vector allocation failed")
}

pub fn test_array_vector(
    element_type: LogicalType,
    child: Arc<Vector>,
    count: usize,
    array_size: usize,
) -> Vector {
    Vector::try_from_array(element_type, child, count, array_size)
        .expect("test array vector allocation failed")
}

pub fn test_new_array(array_type: LogicalType, capacity: usize) -> Vector {
    Vector::try_new_array(array_type, capacity, test_allocator())
        .expect("test array vector allocation failed")
}

pub fn test_new_array_with_allocator(
    array_type: LogicalType,
    capacity: usize,
    allocator: Arc<dyn Allocator>,
) -> Vector {
    Vector::try_new_array(array_type, capacity, allocator)
        .expect("test array vector allocation failed")
}

pub fn test_constant_from_value(logical_type: LogicalType, value: Value, count: usize) -> Vector {
    Vector::try_constant_from_value(logical_type, value, count, test_allocator())
        .expect("test vector allocation failed")
}

pub fn test_new_chunk() -> Chunk {
    Chunk::try_new(test_allocator()).expect("test chunk allocation failed")
}

pub fn test_new_chunk_with_allocator(allocator: Arc<dyn Allocator>) -> Chunk {
    Chunk::try_new(allocator).expect("test chunk allocation failed")
}

pub fn test_chunk(types: &[LogicalType]) -> Chunk {
    test_chunk_with_capacity(types, VECTOR_SIZE)
}

pub fn test_chunk_with_capacity(types: &[LogicalType], capacity: usize) -> Chunk {
    Chunk::try_initialize(types, capacity, test_allocator()).expect("test chunk allocation failed")
}

pub fn test_chunk_with_capacity_and_allocator(
    types: &[LogicalType],
    capacity: usize,
    allocator: Arc<dyn Allocator>,
) -> Chunk {
    Chunk::try_initialize(types, capacity, allocator).expect("test chunk allocation failed")
}

pub fn test_empty_chunk(types: &[LogicalType]) -> Chunk {
    Chunk::try_init_empty(types, test_allocator()).expect("test chunk allocation failed")
}

pub fn test_empty_chunk_with_allocator(
    types: &[LogicalType],
    allocator: Arc<dyn Allocator>,
) -> Chunk {
    Chunk::try_init_empty(types, allocator).expect("test chunk allocation failed")
}

pub fn test_chunk_from_vectors(vectors: Vec<Vector>) -> Chunk {
    let allocator = vectors
        .first()
        .map(|v| v.allocator().clone())
        .unwrap_or_else(test_allocator);
    Chunk::from_vectors(vectors, allocator)
}

pub fn test_chunk_from_arc_vectors(vectors: Vec<Arc<Vector>>) -> Chunk {
    let allocator = vectors
        .first()
        .map(|v| v.allocator().clone())
        .unwrap_or_else(test_allocator);
    Chunk::from_arc_vectors(vectors, allocator)
}

pub fn test_selection(indices: Vec<u32>) -> SelectionVector {
    SelectionVector::try_from_indices(indices, test_allocator())
        .expect("test selection vector allocation failed")
}

pub fn test_incremental_selection(count: usize) -> SelectionVector {
    SelectionVector::try_incremental(count, test_allocator())
        .expect("test selection vector allocation failed")
}

pub fn test_constant_selection(count: usize) -> SelectionVector {
    SelectionVector::try_constant(count, test_allocator())
        .expect("test selection vector allocation failed")
}

pub fn test_selection_with_capacity(capacity: usize) -> SelectionVector {
    SelectionVector::try_with_capacity(capacity, test_allocator())
        .expect("test selection vector allocation failed")
}
