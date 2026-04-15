//! Vector Null Operations

use super::VectorOperations;
use crate::types::LogicalType;
use crate::vector::{Vector, VectorType};

impl VectorOperations {
    /// Check if values in a vector are NULL.
    pub fn is_null(input: &Vector, result: &mut Vector, count: usize) {
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        match input.vector_type() {
            VectorType::Constant => {
                let is_null = input.is_null(0);
                result.set_vector_type(VectorType::Constant);
                result.as_mut_slice::<bool>()[0] = is_null;
            }
            _ => {
                result.set_vector_type(VectorType::Flat);
                let result_data = result.as_mut_slice::<bool>();
                for (i, item) in result_data.iter_mut().enumerate().take(count) {
                    *item = input.is_null(i);
                }
            }
        }
    }

    /// Check if values in a vector are NOT NULL.
    pub fn is_not_null(input: &Vector, result: &mut Vector, count: usize) {
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        match input.vector_type() {
            VectorType::Constant => {
                let is_not_null = !input.is_null(0);
                result.set_vector_type(VectorType::Constant);
                result.as_mut_slice::<bool>()[0] = is_not_null;
            }
            _ => {
                result.set_vector_type(VectorType::Flat);
                let result_data = result.as_mut_slice::<bool>();
                for (i, item) in result_data.iter_mut().enumerate().take(count) {
                    *item = !input.is_null(i);
                }
            }
        }
    }
}
