// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector Hash Operations

use super::VectorOperations;
use crate::types::LogicalType;
use crate::vector::Vector;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

impl VectorOperations {
    /// Compute hashes for a vector.
    ///
    /// # Arguments
    /// * `input` - Input vector to hash
    /// * `result` - Output UBigInt vector to store hashes
    /// * `count` - Number of elements to hash
    pub fn hash(input: &Vector, result: &mut Vector, count: usize) {
        result.set_count(count);

        match input.logical_type() {
            LogicalType::Array(_, _) => {
                Self::hash_array(input, result, count);
            }
            _ => {
                Self::hash_general(input, result, count);
            }
        }
    }

    /// Hash Array vector.
    fn hash_array(input: &Vector, result: &mut Vector, count: usize) {
        let (_child_type, array_size) = match input.logical_type() {
            LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
            _ => unreachable!(),
        };

        let child = input.child().expect("Array vector missing child");
        let child_count = count * array_size;

        // 1. Hash child vector elements
        let mut child_hashes = Vector::with_capacity(LogicalType::UBigInt, child_count);
        Self::hash(child, &mut child_hashes, child_count);

        // 2. Combine hashes for each array
        for i in 0..count {
            if input.is_null(i) {
                // Use a default hash for NULL
                result.set_u64(i, 0xbf58476d1ce4e5b9);
                continue;
            }

            let mut h = 0u64;
            let offset = i * array_size;
            for j in 0..array_size {
                let element_hash = child_hashes.get_u64(offset + j).unwrap_or(0);
                h = Self::combine_hash(h, element_hash);
            }
            result.set_u64(i, h);
        }
    }

    /// General hashing for other types.
    fn hash_general(input: &Vector, result: &mut Vector, count: usize) {
        for i in 0..count {
            if input.is_null(i) {
                result.set_u64(i, 0xbf58476d1ce4e5b9);
                continue;
            }

            let mut hasher = DefaultHasher::new();
            match input.logical_type() {
                LogicalType::Integer => {
                    if let Some(v) = input.get_i32(i) {
                        v.hash(&mut hasher);
                    }
                }
                LogicalType::BigInt => {
                    if let Some(v) = input.get_i64(i) {
                        v.hash(&mut hasher);
                    }
                }
                LogicalType::Float => {
                    if let Some(v) = input.get_f32(i) {
                        // Hash float bits
                        v.to_bits().hash(&mut hasher);
                    }
                }
                LogicalType::Double => {
                    if let Some(v) = input.get_f64(i) {
                        v.to_bits().hash(&mut hasher);
                    }
                }
                LogicalType::Varchar => {
                    if let Some(v) = input.get_string(i) {
                        v.hash(&mut hasher);
                    }
                }
                LogicalType::Boolean => {
                    if let Some(v) = input.get_bool(i) {
                        v.hash(&mut hasher);
                    }
                }
                _ => {
                    // Fallback using get_value (slow)
                    input.get_value(i).hash(&mut hasher);
                }
            }
            result.set_u64(i, hasher.finish());
        }
    }

    /// Combine two hash values.
    #[inline]
    fn combine_hash(mut a: u64, b: u64) -> u64 {
        a ^= a >> 32;
        a = a.wrapping_mul(0xd6e8feb86659fd93);
        a ^ b
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_value::Value;

    #[test]
    fn test_hash_array() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 3);

        // Array 1: [1, 2, 3]
        // Array 2: [1, 2, 3] (same)
        // Array 3: [4, 5, 6] (different)

        let mut v = Vector::new_array(array_type.clone(), 3);
        v.set_count(3);
        v.set_value(
            0,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        v.set_value(
            1,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        v.set_value(
            2,
            &Value::Array(
                vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
                LogicalType::Integer,
                3,
            ),
        );

        let mut hashes = Vector::with_capacity(LogicalType::UBigInt, 3);
        VectorOperations::hash(&v, &mut hashes, 3);

        let h0 = hashes.get_u64(0).unwrap();
        let h1 = hashes.get_u64(1).unwrap();
        let h2 = hashes.get_u64(2).unwrap();

        assert_eq!(h0, h1);
        assert_ne!(h0, h2);
    }
}
