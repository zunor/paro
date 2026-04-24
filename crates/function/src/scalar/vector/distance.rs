// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector Distance Functions (pgvector-compatible)
//!
//!
//!
//! ## Functions
//! - `l2_distance(v1, v2)` - Euclidean distance: sqrt(sum((v1[i] - v2[i])^2))
//! - `l1_distance(v1, v2)` - Manhattan distance: sum(|v1[i] - v2[i]|)
//! - `cosine_distance(v1, v2)` - Cosine distance: 1 - (v1·v2)/(||v1|| * ||v2||)
//! - `inner_product(v1, v2)` - Inner product: sum(v1[i] * v2[i])
//! - `vector_dims(v)` - Get vector dimensions
//! - `vector_norm(v)` - Get vector L2 norm: sqrt(sum(v[i]^2))
//!
//! ## SIMD Optimization
//! Distance computations use SIMD-optimized implementations from
//! `paro_common::distance` when available:
//! - AVX+FMA on x86_64 (for vectors >= 32 elements)
//! - SSE on x86/x86_64 (for vectors >= 16 elements)
//! - NEON on ARM64 (for vectors >= 16 elements)

use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::distance;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{ArrayView, DataRef, SelectionRef, Vector};
use paro_storage::rowset::SparseVector;

// ============================================================================
// Helper Functions
// ============================================================================

#[derive(Copy, Clone)]
enum ArrayElementReadKind {
    Float(*const f32),
    Double(*const f64),
    Integer(*const i32),
    BigInt(*const i64),
}

/// Read ARRAY rows as `&[f32]` via `ArrayView`.
///
/// This avoids per-row `Value::Array` object materialization and supports
/// flat/constant/dictionary ARRAY inputs through the shared view layer.
struct ArrayF32Reader<'a> {
    array_size: usize,
    array: ArrayView<'a>,
    child_validity_all_valid: bool,
    read_kind: ArrayElementReadKind,
}

impl<'a> ArrayF32Reader<'a> {
    fn new(vector: &'a Vector, count: usize) -> Result<Self> {
        let (child_type, array_size) = match vector.logical_type() {
            LogicalType::Array(child_type, array_size) => (child_type.as_ref(), *array_size),
            logical_type => {
                return Err(paro_error::invalid_value(
                    "VECTOR",
                    format!("Expected Array type, got {:?}", logical_type),
                ))
            }
        };

        let array = vector.try_to_array_view(count)?;

        let read_kind = match child_type {
            LogicalType::Float => ArrayElementReadKind::Float(
                array
                    .child()
                    .get_data::<f32>()
                    .ok_or_else(|| paro_error::internal("ARRAY child requires pointer data"))?,
            ),
            LogicalType::Double => ArrayElementReadKind::Double(
                array
                    .child()
                    .get_data::<f64>()
                    .ok_or_else(|| paro_error::internal("ARRAY child requires pointer data"))?,
            ),
            LogicalType::Integer => ArrayElementReadKind::Integer(
                array
                    .child()
                    .get_data::<i32>()
                    .ok_or_else(|| paro_error::internal("ARRAY child requires pointer data"))?,
            ),
            LogicalType::BigInt => ArrayElementReadKind::BigInt(
                array
                    .child()
                    .get_data::<i64>()
                    .ok_or_else(|| paro_error::internal("ARRAY child requires pointer data"))?,
            ),
            other => {
                return Err(paro_error::invalid_value(
                    "VECTOR",
                    format!("Invalid vector element type: {:?}", other),
                ))
            }
        };
        let child_validity_all_valid = array.child().validity().all_valid();

        Ok(Self {
            array_size,
            array,
            child_validity_all_valid,
            read_kind,
        })
    }

    #[inline]
    fn array_size(&self) -> usize {
        self.array_size
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        !self.array.is_valid(idx)
    }

    #[inline]
    unsafe fn read_element_unchecked(&self, physical_idx: usize) -> f32 {
        match self.read_kind {
            ArrayElementReadKind::Float(ptr) => *ptr.add(physical_idx),
            ArrayElementReadKind::Double(ptr) => *ptr.add(physical_idx) as f32,
            ArrayElementReadKind::Integer(ptr) => *ptr.add(physical_idx) as f32,
            ArrayElementReadKind::BigInt(ptr) => *ptr.add(physical_idx) as f32,
        }
    }

    fn row<'scratch>(
        &self,
        idx: usize,
        scratch: &'scratch mut Vec<f32>,
    ) -> Result<&'scratch [f32]> {
        if self.is_null(idx) {
            return Err(paro_error::invalid_value("VECTOR", "NULL vector"));
        }

        if self.array_size == 0 {
            scratch.clear();
            return Ok(scratch.as_slice());
        }

        let base_offset = self.array.logical_child_index(idx, 0);

        // Fast path: direct slice when the child view is flat and values are f32.
        if self.child_validity_all_valid
            && matches!(self.array.child().sel(), SelectionRef::Incremental { .. })
        {
            if let (ArrayElementReadKind::Float(ptr), DataRef::Ptr(_)) =
                (self.read_kind, self.array.child().data())
            {
                // SAFETY: pointer/type comes from the decoded child data and the index is valid.
                unsafe {
                    return Ok(std::slice::from_raw_parts(
                        ptr.add(base_offset),
                        self.array_size,
                    ));
                }
            }
        }

        scratch.clear();
        if scratch.capacity() < self.array_size {
            scratch.reserve(self.array_size - scratch.capacity());
        }

        for element_idx in 0..self.array_size {
            let logical_child_idx = base_offset + element_idx;

            if !self.child_validity_all_valid && !self.array.child().is_valid(logical_child_idx) {
                return Err(paro_error::invalid_value(
                    "VECTOR",
                    format!("NULL vector element at position {}", element_idx),
                ));
            }

            let physical_child_idx = self.array.child().physical_index(logical_child_idx);
            // SAFETY: physical_child_idx is produced by selection mapping for the child vector.
            let value = unsafe { self.read_element_unchecked(physical_child_idx) };
            scratch.push(value);
        }

        Ok(scratch.as_slice())
    }
}

/// Get array size from logical type.
fn get_array_size(logical_type: &LogicalType) -> Result<usize> {
    match logical_type {
        LogicalType::Array(_, size) => Ok(*size),
        _ => Err(paro_error::invalid_value(
            "VECTOR",
            format!("Expected Array type, got {:?}", logical_type),
        )),
    }
}

fn execute_binary_distance<F>(input: &Chunk, result: &mut Vector, op: F) -> Result<()>
where
    F: Fn(&[f32], &[f32]) -> f64,
{
    let count = input.size();
    result.set_count(count);

    let v1_reader = ArrayF32Reader::new(&input.data[0], count)?;
    let v2_reader = ArrayF32Reader::new(&input.data[1], count)?;
    let v1_dim = v1_reader.array_size();
    let v2_dim = v2_reader.array_size();
    if v1_dim != v2_dim {
        return Err(paro_error::invalid_value(
            "VECTOR",
            format!("Vector dimension mismatch: {} vs {}", v1_dim, v2_dim),
        ));
    }
    let zero = vec![0.0f32; v1_dim];
    let mut v1_scratch = Vec::new();
    let mut v2_scratch = Vec::new();

    for i in 0..count {
        let v1 = if v1_reader.is_null(i) {
            zero.as_slice()
        } else {
            v1_reader.row(i, &mut v1_scratch)?
        };
        let v2 = if v2_reader.is_null(i) {
            zero.as_slice()
        } else {
            v2_reader.row(i, &mut v2_scratch)?
        };

        result.set_f64(i, op(v1, v2));
    }

    Ok(())
}

fn execute_unary_distance<F>(input: &Chunk, result: &mut Vector, op: F) -> Result<()>
where
    F: Fn(&[f32]) -> f64,
{
    let count = input.size();
    result.set_count(count);

    let reader = ArrayF32Reader::new(&input.data[0], count)?;
    let mut scratch = Vec::new();

    for i in 0..count {
        if reader.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let v = reader.row(i, &mut scratch)?;
        result.set_f64(i, op(v));
    }

    Ok(())
}

// ============================================================================
// L2 Distance (Euclidean Distance)
// ============================================================================

fn l2_distance_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_binary_distance(input, result, |v1, v2| distance::l2_distance(v1, v2) as f64)
}

/// Get the `l2_distance` function set.
pub fn get_l2_distance_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("l2_distance".to_string());

    set.add_function(
        ScalarFunction::new(
            "l2_distance".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            l2_distance_fn,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// L1 Distance (Manhattan Distance)
// ============================================================================

fn l1_distance_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_binary_distance(input, result, |v1, v2| distance::l1_distance(v1, v2) as f64)
}

/// Get the `l1_distance` function set.
pub fn get_l1_distance_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("l1_distance".to_string());

    set.add_function(
        ScalarFunction::new(
            "l1_distance".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            l1_distance_fn,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// Cosine Distance
// ============================================================================

fn cosine_distance_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_binary_distance(input, result, |v1, v2| {
        let dist = distance::cosine_distance(v1, v2);
        // Handle zero vectors - cosine_distance returns 1.0 for zero vectors
        if dist.is_nan() {
            f64::NAN
        } else {
            dist as f64
        }
    })
}

/// Get the `cosine_distance` function set.
pub fn get_cosine_distance_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("cosine_distance".to_string());

    set.add_function(
        ScalarFunction::new(
            "cosine_distance".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            cosine_distance_fn,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// Inner Product (Dot Product)
// ============================================================================

fn inner_product_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_binary_distance(input, result, |v1, v2| distance::dot_product(v1, v2) as f64)
}

/// Get the `inner_product` function set.
pub fn get_inner_product_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("inner_product".to_string());

    set.add_function(
        ScalarFunction::new(
            "inner_product".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            inner_product_fn,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// Negative Inner Product
// ============================================================================

fn neg_inner_product_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_binary_distance(input, result, |v1, v2| {
        -(distance::dot_product(v1, v2) as f64)
    })
}

/// Get the `neg_inner_product` function set.
pub fn get_neg_inner_product_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("neg_inner_product".to_string());

    set.add_function(
        ScalarFunction::new(
            "neg_inner_product".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            neg_inner_product_fn,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// Vector Dims
// ============================================================================

fn vector_dims_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let v_vec = &input.data[0];
    let array_size = get_array_size(v_vec.logical_type())?;

    for i in 0..count {
        if v_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        result.set_i32(i, array_size as i32);
    }

    Ok(())
}

/// Get the `vector_dims` function set.
pub fn get_vector_dims_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("vector_dims".to_string());

    set.add_function(ScalarFunction::new(
        "vector_dims".to_string(),
        vec![LogicalType::Array(Box::new(LogicalType::Float), 0)],
        LogicalType::Integer,
        vector_dims_fn,
    ));

    set
}

// ============================================================================
// Vector Norm (L2 Norm)
// ============================================================================

fn vector_norm_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_unary_distance(input, result, |v| {
        // Use dot_product(v, v) for SIMD optimization, then sqrt
        let norm_sq = distance::dot_product(v, v);
        (norm_sq as f64).sqrt()
    })
}

/// Get the `vector_norm` function set.
pub fn get_vector_norm_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("vector_norm".to_string());

    set.add_function(ScalarFunction::new(
        "vector_norm".to_string(),
        vec![LogicalType::Array(Box::new(LogicalType::Float), 0)],
        LogicalType::Double,
        vector_norm_fn,
    ));

    set
}

// ============================================================================
// Sparse Vector Distance
// ============================================================================

fn sparse_distance_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let v1_vec = &input.data[0];
    let v2_vec = &input.data[1];

    for i in 0..count {
        if v1_vec.is_null(i) || v2_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let s1 = v1_vec.get_string(i).unwrap();
        let s2 = v2_vec.get_string(i).unwrap();

        let sv1 = SparseVector::parse(s1)?;
        let sv2 = SparseVector::parse(s2)?;

        let dist = match sv1.dot(&sv2) {
            Some(score) => score,
            None => 0.0,
        };
        result.set_f32(i, dist);
    }

    Ok(())
}

/// Get the `sparse_distance` function set.
pub fn get_sparse_distance_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("sparse_distance".to_string());

    set.add_function(ScalarFunction::new(
        "sparse_distance".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Float,
        sparse_distance_fn,
    ));

    set
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use std::any::Any;
    use std::sync::Arc;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn embedding_vector(rows: &[&[f32]]) -> Vector {
        let dims = rows.first().map(|row| row.len()).unwrap_or(0);
        let embeddings = rows
            .iter()
            .map(|row| row.to_vec())
            .collect::<Vec<Vec<f32>>>();
        paro_common::test_utils::test_embeddings_vector_with_allocator(
            &embeddings,
            dims,
            paro_common::test_utils::test_allocator(),
        )
    }

    #[test]
    fn test_l2_distance() {
        let v1 = vec![1.0f32, 2.0, 3.0];
        let v2 = vec![4.0f32, 5.0, 6.0];

        // sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(9 + 9 + 9) = sqrt(27) ≈ 5.196
        let dist = distance::l2_distance(&v1, &v2);
        assert!((dist - 5.196152).abs() < 1e-4);
    }

    #[test]
    fn test_l1_distance() {
        let v1 = vec![1.0f32, 2.0, 3.0];
        let v2 = vec![4.0f32, 5.0, 6.0];
        let dist = distance::l1_distance(&v1, &v2);
        assert!((dist - 9.0).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_distance() {
        let v1 = vec![1.0f32, 0.0, 0.0];
        let v2 = vec![0.0f32, 1.0, 0.0];

        // Orthogonal vectors: cosine similarity = 0, cosine distance = 1
        let dist = distance::cosine_distance(&v1, &v2);
        assert!((dist - 1.0).abs() < 1e-5);

        // Same direction: cosine similarity = 1, cosine distance = 0
        let v3 = vec![1.0f32, 2.0, 3.0];
        let v4 = vec![2.0f32, 4.0, 6.0];
        let dist2 = distance::cosine_distance(&v3, &v4);
        assert!(dist2.abs() < 1e-5);
    }

    #[test]
    fn test_inner_product() {
        let v1 = vec![1.0f32, 2.0, 3.0];
        let v2 = vec![4.0f32, 5.0, 6.0];

        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        let product = distance::dot_product(&v1, &v2);
        assert!((product - 32.0).abs() < 1e-5);
    }

    #[test]
    fn test_vector_norm() {
        let v = vec![3.0f32, 4.0];

        // sqrt(3^2 + 4^2) = sqrt(9 + 16) = sqrt(25) = 5
        let norm_sq = distance::dot_product(&v, &v);
        let norm = norm_sq.sqrt();
        assert!((norm - 5.0).abs() < 1e-5);
    }

    #[test]
    fn test_l2_distance_dictionary_array_input() {
        let dict_child = embedding_vector(&[&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0], &[7.0, 8.0, 9.0]]);
        let lhs = paro_common::test_utils::test_dictionary(Arc::new(dict_child), vec![2u32, 0, 1]);
        let rhs = embedding_vector(&[&[7.0, 8.0, 9.0], &[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]]);

        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![lhs, rhs]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
        l2_distance_fn(&chunk, &state, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-12);
        assert!((result.get_f64(1).unwrap() - 0.0).abs() < 1e-12);
        assert!((result.get_f64(2).unwrap() - 0.0).abs() < 1e-12);
    }

    #[test]
    fn test_l2_distance_constant_array_input() {
        let lhs = paro_common::test_utils::test_constant_from_value(
            LogicalType::Array(Box::new(LogicalType::Float), 3),
            Value::Array(
                vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
                LogicalType::Float,
                3,
            ),
            3,
        );
        let rhs = embedding_vector(&[&[1.0, 2.0, 3.0], &[2.0, 2.0, 3.0], &[1.0, 1.0, 1.0]]);

        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![lhs, rhs]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
        l2_distance_fn(&chunk, &state, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-12);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-12);
        assert!((result.get_f64(2).unwrap() - (5.0f64).sqrt()).abs() < 1e-6);
    }

    #[test]
    fn test_vector_norm_dictionary_array_input() {
        let dict_child = embedding_vector(&[&[3.0, 4.0], &[1.0, 0.0]]);
        let vec = paro_common::test_utils::test_dictionary(Arc::new(dict_child), vec![1u32, 0, 1]);

        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![vec]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
        vector_norm_fn(&chunk, &state, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-12);
        assert!((result.get_f64(1).unwrap() - 5.0).abs() < 1e-12);
        assert!((result.get_f64(2).unwrap() - 1.0).abs() < 1e-12);
    }
}
