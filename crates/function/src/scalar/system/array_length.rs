// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! array_length() Function
//!
//! PostgreSQL-compatible function that returns the length of a requested array dimension.
//!
//!
//!
//! ## PostgreSQL Reference
//! `array_length(anyarray, int) -> int`
//! Returns the length of the requested array dimension.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, Vector, VectorType, VectorView};

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

fn read_list_entry(entries: &VectorView<'_>, child_len: usize, row: usize) -> Result<usize> {
    let DataRef::Ptr(entry_data) = entries.data() else {
        return Err(paro_error::internal(
            "array_length does not support sequence-backed LIST entries",
        ));
    };
    let entry_ptr = unsafe { entry_data.add(entries.physical_index(row) * 8) as *const u32 };
    let offset = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
    let length = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };

    if offset.saturating_add(length) > child_len {
        return Err(paro_error::internal(format!(
            "Invalid list entry ({offset}, {length}), child length is {child_len}",
        )));
    }

    Ok(length)
}

fn list_child_vector(vector: &Vector) -> Result<&Vector> {
    match vector.vector_type() {
        VectorType::Dictionary => {
            let child = vector
                .child()
                .ok_or_else(|| paro_error::internal("Dictionary LIST missing child"))?;
            list_child_vector(child)
        }
        VectorType::Flat | VectorType::Constant => vector
            .child()
            .map(|child| child.as_ref())
            .ok_or_else(|| paro_error::internal("List vector missing child")),
        VectorType::Sequence => Err(paro_error::type_mismatch(
            "array_length does not support sequence-backed LIST vectors",
        )),
    }
}

fn array_dimensions(logical_type: &LogicalType) -> Vec<usize> {
    let mut dims = Vec::new();
    let mut current = logical_type;
    while let LogicalType::Array(child, size) = current {
        dims.push(*size);
        current = child.as_ref();
    }
    dims
}

fn validate_dimension(dimension: i32, max_dimension: usize) -> Result<usize> {
    if dimension < 1 || dimension as usize > max_dimension {
        return Err(paro_error::array_subscript_error(format!(
            "array_length dimension '{}' out of range (min: '1', max: '{}')",
            dimension, max_dimension
        )));
    }
    Ok((dimension - 1) as usize)
}

fn array_length_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let array_vec = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing array/list input column"))?;
    let dim_vec = input
        .column(1)
        .ok_or_else(|| paro_error::internal("Missing dimension input column"))?;

    match array_vec.logical_type() {
        LogicalType::Array(_, _) => {
            let dims = array_dimensions(array_vec.logical_type());
            let max_dimension = dims.len();

            for i in 0..count {
                if array_vec.is_null(i) || dim_vec.is_null(i) {
                    result.set_null(i, true);
                    continue;
                }

                let dimension = dim_vec.get_i32(i).ok_or_else(|| {
                    paro_error::internal("array_length dimension column must be INTEGER")
                })?;
                let dim_idx = validate_dimension(dimension, max_dimension)?;
                result.set_i32(i, dims[dim_idx] as i32);
            }
        }
        LogicalType::List(_) => {
            let entries = array_vec.to_view(count);
            let child = list_child_vector(array_vec)?;

            for i in 0..count {
                if !entries.is_valid(i) || dim_vec.is_null(i) {
                    result.set_null(i, true);
                    continue;
                }

                let dimension = dim_vec.get_i32(i).ok_or_else(|| {
                    paro_error::internal("array_length dimension column must be INTEGER")
                })?;
                let _ = validate_dimension(dimension, 1)?;

                let length = read_list_entry(&entries, child.len(), i)?;
                let length_i32 = i32::try_from(length)
                    .map_err(|_| paro_error::out_of_range("array_length result exceeds INT"))?;
                result.set_i32(i, length_i32);
            }
        }
        other => {
            return Err(paro_error::type_mismatch(format!(
                "array_length can only be used on arrays or lists, got {}",
                other
            )));
        }
    }

    Ok(())
}

pub fn get_array_length_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("array_length".to_string());

    // array_length(ARRAY<ANY>, dimension)
    set.add_function(ScalarFunction::new(
        "array_length".to_string(),
        vec![
            LogicalType::Array(Box::new(LogicalType::Unknown), 0),
            LogicalType::Integer,
        ],
        LogicalType::Integer,
        array_length_impl,
    ));

    // array_length(LIST<ANY>, dimension)
    set.add_function(ScalarFunction::new(
        "array_length".to_string(),
        vec![
            LogicalType::List(Box::new(LogicalType::Unknown)),
            LogicalType::Integer,
        ],
        LogicalType::Integer,
        array_length_impl,
    ));

    set
}

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

    fn sample_array_vector() -> Vector {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 3);
        let mut array_vec = Vector::new_array(array_type, 2);
        array_vec.set_count(2);
        array_vec.set_value(
            0,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        array_vec.set_value(
            1,
            &Value::Array(
                vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
                LogicalType::Integer,
                3,
            ),
        );
        array_vec
    }

    #[test]
    fn test_array_length_array_dim1() {
        let array_vec = sample_array_vector();
        let dim_vec = Vector::from_i32(&[1, 1]);
        let chunk = Chunk::from_vectors(vec![array_vec, dim_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Integer);

        array_length_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i32(0), Some(3));
        assert_eq!(result.get_i32(1), Some(3));
    }

    #[test]
    fn test_array_length_nested_array_dimensions() {
        let nested_type = LogicalType::Array(
            Box::new(LogicalType::Array(Box::new(LogicalType::Integer), 3)),
            2,
        );
        let mut array_vec = Vector::new_array(nested_type, 2);
        array_vec.set_count(2);
        array_vec.set_value(
            0,
            &Value::Array(
                vec![
                    Value::Array(
                        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                        LogicalType::Integer,
                        3,
                    ),
                    Value::Array(
                        vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
                        LogicalType::Integer,
                        3,
                    ),
                ],
                LogicalType::Array(Box::new(LogicalType::Integer), 3),
                2,
            ),
        );
        array_vec.set_value(
            1,
            &Value::Array(
                vec![
                    Value::Array(
                        vec![Value::Integer(7), Value::Integer(8), Value::Integer(9)],
                        LogicalType::Integer,
                        3,
                    ),
                    Value::Array(
                        vec![Value::Integer(10), Value::Integer(11), Value::Integer(12)],
                        LogicalType::Integer,
                        3,
                    ),
                ],
                LogicalType::Array(Box::new(LogicalType::Integer), 3),
                2,
            ),
        );

        let dim_vec = Vector::from_i32(&[1, 2]);
        let chunk = Chunk::from_vectors(vec![array_vec, dim_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Integer);

        array_length_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i32(0), Some(2));
        assert_eq!(result.get_i32(1), Some(3));
    }

    #[test]
    fn test_array_length_list_dim1() {
        let mut list_vec =
            Vector::with_capacity(LogicalType::List(Box::new(LogicalType::Integer)), 2);
        list_vec.set_count(2);
        list_vec.set_child(Arc::new(Vector::from_i32(&[10, 20, 30, 40, 50])));

        unsafe {
            let entries = list_vec.flat_data_mut::<u32>();
            *entries.add(0) = 0;
            *entries.add(1) = 2;
            *entries.add(2) = 2;
            *entries.add(3) = 3;
        }

        let dim_vec = Vector::from_i32(&[1, 1]);
        let chunk = Chunk::from_vectors(vec![list_vec, dim_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Integer);

        array_length_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i32(0), Some(2));
        assert_eq!(result.get_i32(1), Some(3));
    }

    #[test]
    fn test_array_length_invalid_dimension_errors() {
        let array_vec = sample_array_vector();
        let dim_vec = Vector::from_i32(&[2, 2]);
        let chunk = Chunk::from_vectors(vec![array_vec, dim_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Integer);

        let err = array_length_impl(&chunk, &state, &mut result).unwrap_err();
        assert!(err.to_string().contains("array_length dimension"));
    }

    #[test]
    fn test_array_length_null_propagation() {
        let mut array_vec = sample_array_vector();
        array_vec.set_null(1, true);
        let mut dim_vec = Vector::from_i32(&[1, 1]);
        dim_vec.set_null(0, true);
        let chunk = Chunk::from_vectors(vec![array_vec, dim_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Integer);

        array_length_impl(&chunk, &state, &mut result).unwrap();
        assert!(result.is_null(0));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_array_length_function_set_signatures() {
        let set = get_array_length_functions();
        assert_eq!(set.name, "array_length");
        assert_eq!(set.functions.len(), 2);
        assert!(set.functions.iter().all(|func| func.arguments.len() == 2));
        assert!(set
            .functions
            .iter()
            .all(|func| !matches!(func.arguments[0], LogicalType::Varchar)));
    }
}
