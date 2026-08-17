// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed vector hashing for execution-time hash tables.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::{DataRef, Vector, VectorOperations, VectorType};
use crate::error::Result;
use crate::hash::{combine_hash, hash_bytes, hash_i64, hash_u128, hash_u64, HASH_SEED, NULL_HASH};
use crate::types::LogicalType;

impl VectorOperations {
    /// Compute one deterministic hash per logical vector row.
    ///
    /// Primitive and varlen SQL types use typed kernels. Nested and uncommon
    /// types retain a single boxed-value fallback so their semantics remain
    /// complete without putting the standard-library hasher on analytical hot
    /// paths.
    pub fn hash(input: &Vector, result: &mut Vector, count: usize) -> Result<()> {
        result.try_set_count(count)?;
        if count == 0 {
            return Ok(());
        }
        if matches!(input.logical_type(), LogicalType::Array(_, _)) {
            return Self::hash_array(input, result, count);
        }

        let output = &mut result.as_mut_slice::<u64>()[..count];
        match input.logical_type() {
            LogicalType::Boolean => hash_fixed(
                input,
                count,
                output,
                |value: bool| hash_u64(u64::from(value)),
                Vector::get_bool,
            ),
            LogicalType::TinyInt => hash_fixed(
                input,
                count,
                output,
                |value: i8| hash_i64(value as i64),
                Vector::get_i8,
            ),
            LogicalType::SmallInt => hash_fixed(
                input,
                count,
                output,
                |value: i16| hash_i64(value as i64),
                Vector::get_i16,
            ),
            LogicalType::Integer | LogicalType::Date => hash_fixed(
                input,
                count,
                output,
                |value: i32| hash_i64(value as i64),
                Vector::get_i32,
            ),
            LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => hash_fixed(input, count, output, hash_i64, Vector::get_i64),
            LogicalType::HugeInt | LogicalType::Interval => hash_fixed(
                input,
                count,
                output,
                |value: i128| hash_u128(value as u128),
                Vector::get_i128,
            ),
            LogicalType::UTinyInt => hash_fixed(
                input,
                count,
                output,
                |value: u8| hash_u64(value as u64),
                Vector::get_u8,
            ),
            LogicalType::USmallInt => hash_fixed(
                input,
                count,
                output,
                |value: u16| hash_u64(value as u64),
                Vector::get_u16,
            ),
            LogicalType::UInteger => hash_fixed(
                input,
                count,
                output,
                |value: u32| hash_u64(value as u64),
                Vector::get_u32,
            ),
            LogicalType::UBigInt => hash_fixed(input, count, output, hash_u64, Vector::get_u64),
            LogicalType::UHugeInt | LogicalType::Uuid => {
                hash_fixed(input, count, output, hash_u128, Vector::get_u128)
            }
            LogicalType::Float => hash_fixed(
                input,
                count,
                output,
                |value: f32| hash_u64(value.to_bits() as u64),
                Vector::get_f32,
            ),
            LogicalType::Double => hash_fixed(
                input,
                count,
                output,
                |value: f64| hash_u64(value.to_bits()),
                Vector::get_f64,
            ),
            LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                hash_fixed(input, count, output, hash_i64, Vector::get_i64)
            }
            LogicalType::Decimal { .. } => hash_fixed(
                input,
                count,
                output,
                |value: i128| hash_u128(value as u128),
                Vector::get_i128,
            ),
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral
            | LogicalType::Blob => hash_varlen(input, count, output)?,
            _ => hash_fallback(input, count, output),
        }
        Ok(())
    }

    fn hash_array(input: &Vector, result: &mut Vector, count: usize) -> Result<()> {
        let array_size = match input.logical_type() {
            LogicalType::Array(_, size) => *size,
            _ => return Ok(()),
        };
        let Some(child) = input.child() else {
            return Ok(());
        };
        let child_count = count.saturating_mul(array_size);
        let mut child_hashes = Vector::try_new(
            LogicalType::UBigInt,
            child_count,
            result.allocator().clone(),
        )?;
        Self::hash(child, &mut child_hashes, child_count)?;

        let output = &mut result.as_mut_slice::<u64>()[..count];
        let child_output = child_hashes.as_slice::<u64>();
        for (row_idx, output_hash) in output.iter_mut().enumerate() {
            if input.is_null(row_idx) {
                *output_hash = NULL_HASH;
                continue;
            }
            let mut hash = HASH_SEED;
            let offset = row_idx * array_size;
            for child_hash in &child_output[offset..offset + array_size] {
                hash = combine_hash(hash, *child_hash);
            }
            *output_hash = hash;
        }
        Ok(())
    }
}

fn hash_fixed<T: Copy>(
    input: &Vector,
    count: usize,
    output: &mut [u64],
    hash_value: impl Fn(T) -> u64,
    read_value: impl Fn(&Vector, usize) -> Option<T>,
) {
    if let Ok(view) = input.try_to_view(count) {
        if input.vector_type() == VectorType::Constant {
            let value_hash = if !view.is_valid(0) {
                NULL_HASH
            } else if let Some(data) = view.get_data::<T>() {
                hash_value(unsafe { *data })
            } else {
                read_value(input, 0).map_or(NULL_HASH, &hash_value)
            };
            output.fill(value_hash);
            return;
        }

        if let DataRef::Ptr(data) = view.data() {
            let values = data as *const T;
            if input.vector_type() == VectorType::Dictionary {
                let mut previous_idx = usize::MAX;
                let mut previous_hash = NULL_HASH;
                for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
                    let physical_idx = view.physical_index(row_idx);
                    if physical_idx != previous_idx {
                        previous_hash = if view.validity().is_valid(physical_idx) {
                            hash_value(unsafe { *values.add(physical_idx) })
                        } else {
                            NULL_HASH
                        };
                        previous_idx = physical_idx;
                    }
                    *slot = previous_hash;
                }
                return;
            }
            if view.validity().all_valid() {
                for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
                    *slot = hash_value(unsafe { *values.add(view.physical_index(row_idx)) });
                }
            } else {
                for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
                    let physical_idx = view.physical_index(row_idx);
                    *slot = if view.validity().is_valid(physical_idx) {
                        hash_value(unsafe { *values.add(physical_idx) })
                    } else {
                        NULL_HASH
                    };
                }
            }
            return;
        }
    }

    for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
        *slot = if input.is_null(row_idx) {
            NULL_HASH
        } else {
            read_value(input, row_idx).map_or(NULL_HASH, &hash_value)
        };
    }
}

fn hash_varlen(input: &Vector, count: usize, output: &mut [u64]) -> Result<()> {
    let view = input.try_to_varlen_view(count)?;
    if input.vector_type() == VectorType::Constant {
        let value_hash = if !view.is_valid(0) {
            NULL_HASH
        } else {
            hash_bytes(view.bytes(0))
        };
        output.fill(value_hash);
        return Ok(());
    }
    if input.vector_type() == VectorType::Dictionary {
        let mut previous_idx = usize::MAX;
        let mut previous_hash = NULL_HASH;
        for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
            let physical_idx = view.sel().get(row_idx);
            if physical_idx != previous_idx {
                previous_hash = if view.validity().is_valid(physical_idx) {
                    hash_bytes(view.bytes(row_idx))
                } else {
                    NULL_HASH
                };
                previous_idx = physical_idx;
            }
            *slot = previous_hash;
        }
        return Ok(());
    }
    if view.validity().all_valid() {
        for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
            *slot = hash_bytes(view.bytes(row_idx));
        }
        return Ok(());
    }
    for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
        *slot = if !view.is_valid(row_idx) {
            NULL_HASH
        } else {
            hash_bytes(view.bytes(row_idx))
        };
    }
    Ok(())
}

fn hash_fallback(input: &Vector, count: usize, output: &mut [u64]) {
    for (row_idx, slot) in output.iter_mut().enumerate().take(count) {
        if input.is_null(row_idx) {
            *slot = NULL_HASH;
            continue;
        }
        let mut hasher = DefaultHasher::new();
        input.get_value(row_idx).hash(&mut hasher);
        *slot = hasher.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_value::Value;

    #[test]
    fn equal_arrays_hash_equally() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 3);
        let mut vector = crate::test_utils::test_new_array(array_type.clone(), 3);
        vector.set_count(3);
        vector.set_value(
            0,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        vector.set_value(
            1,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        vector.set_value(
            2,
            &Value::Array(
                vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
                LogicalType::Integer,
                3,
            ),
        );

        let mut hashes = crate::test_utils::test_vector_with_capacity(LogicalType::UBigInt, 3);
        VectorOperations::hash(&vector, &mut hashes, 3).unwrap();

        assert_eq!(hashes.get_u64(0), hashes.get_u64(1));
        assert_ne!(hashes.get_u64(0), hashes.get_u64(2));
    }

    #[test]
    fn dictionary_hashes_logical_rows() {
        let child = std::sync::Arc::new(crate::test_utils::test_i32_vector(&[10, 20, 30]));
        let dictionary = crate::test_utils::test_dictionary(child, vec![2, 0, 2, 1]);
        let mut hashes = crate::test_utils::test_vector_with_capacity(LogicalType::UBigInt, 4);

        VectorOperations::hash(&dictionary, &mut hashes, 4).unwrap();

        assert_eq!(hashes.get_u64(0), hashes.get_u64(2));
        assert_ne!(hashes.get_u64(0), hashes.get_u64(1));
        assert_ne!(hashes.get_u64(0), hashes.get_u64(3));
    }

    #[test]
    fn adjacent_dictionary_varlen_rows_hash_like_their_child_values() {
        let allocator = crate::test_utils::test_allocator();
        let child = std::sync::Arc::new(crate::test_utils::test_string_vector_with_allocator(
            &["alpha", "beta"],
            allocator.clone(),
        ));
        let dictionary = Vector::try_dictionary(
            child,
            crate::vector::SelectionVector::try_from_indices(vec![0, 0, 1, 1], allocator.clone())
                .unwrap(),
        )
        .unwrap();
        let mut hashes = Vector::try_new(LogicalType::UBigInt, 4, allocator).unwrap();

        VectorOperations::hash(&dictionary, &mut hashes, 4).unwrap();

        assert_eq!(hashes.get_u64(0), hashes.get_u64(1));
        assert_eq!(hashes.get_u64(2), hashes.get_u64(3));
        assert_ne!(hashes.get_u64(0), hashes.get_u64(2));
    }
}
