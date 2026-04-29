// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage crate test constructors built on allocator-explicit common APIs.

use std::sync::Arc;

pub(crate) use paro_common::test_utils::{
    test_allocator, test_chunk_with_capacity, test_selection, test_vector,
    test_vector_with_capacity,
};

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

pub(crate) fn test_empty_data_chunk() -> Chunk {
    Chunk::try_new(test_allocator()).expect("test chunk allocation failed")
}

pub(crate) fn test_chunk_from_vectors(vectors: Vec<Vector>) -> Chunk {
    let allocator = vectors
        .first()
        .map(|vector| vector.allocator().clone())
        .unwrap_or_else(test_allocator);
    Chunk::from_vectors(vectors, allocator)
}

pub(crate) fn test_chunk_from_arc_vectors(vectors: Vec<Arc<Vector>>) -> Chunk {
    let allocator = vectors
        .first()
        .map(|vector| vector.allocator().clone())
        .unwrap_or_else(test_allocator);
    Chunk::from_arc_vectors(vectors, allocator)
}

pub(crate) fn test_i32_vector(values: &[i32]) -> Vector {
    paro_common::test_utils::test_i32_vector(values)
}

pub(crate) fn test_i64_vector(values: &[i64]) -> Vector {
    paro_common::test_utils::test_i64_vector(values)
}

pub(crate) fn test_string_vector(values: &[&str]) -> Vector {
    paro_common::test_utils::test_string_vector(values)
}

pub(crate) fn test_nullable_string_vector(values: &[Option<&str>]) -> Vector {
    paro_common::test_utils::test_nullable_string_vector(values)
}

pub(crate) fn test_embedding_vector(embeddings: &[Vec<f32>], dimensions: usize) -> Vector {
    paro_common::test_utils::test_embeddings_vector(embeddings, dimensions)
}

pub(crate) fn test_constant_vector<T: Copy>(
    logical_type: LogicalType,
    value: T,
    count: usize,
) -> Vector {
    paro_common::test_utils::test_constant(logical_type, value, count)
}

pub(crate) fn test_constant_from_value(
    logical_type: LogicalType,
    value: &Value,
    count: usize,
) -> Vector {
    if let Value::Varchar(value) = value {
        let mut vector = paro_common::test_utils::test_string_vector(&[value.as_str()]);
        return vector.to_constant(count);
    }

    let mut vector =
        Vector::try_new(logical_type, 1, test_allocator()).expect("test vector allocation failed");
    vector.set_value(0, value);
    vector.set_count(1);
    vector.to_constant(count)
}

pub(crate) fn test_selection_with_capacity(capacity: usize) -> SelectionVector {
    SelectionVector::try_with_capacity(capacity, test_allocator())
        .expect("test selection vector allocation failed")
}
