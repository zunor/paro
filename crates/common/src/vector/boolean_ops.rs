//! Vector Boolean Operations

use super::VectorOperations;
use crate::types::LogicalType;
use crate::vector::{Vector, VectorType};

impl VectorOperations {
    /// Vectorized logical NOT.
    pub fn not(input: &Vector, result: &mut Vector, count: usize) {
        debug_assert_eq!(input.logical_type(), &LogicalType::Boolean);
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        let input_data = input.as_slice::<bool>();

        // Logical NOT: NULL -> NULL, true -> false, false -> true
        match input.vector_type() {
            VectorType::Constant => {
                result.set_vector_type(VectorType::Constant);
                if input.is_null(0) {
                    result.set_null(0, true);
                } else {
                    let val = !input_data[0];
                    result.as_mut_slice::<bool>()[0] = val;
                    result.set_null(0, false);
                }
            }
            _ => {
                result.set_vector_type(VectorType::Flat);
                for (i, &val) in input_data.iter().enumerate().take(count) {
                    if input.is_null(i) {
                        result.set_null(i, true);
                    } else {
                        let val = !val;
                        result.as_mut_slice::<bool>()[i] = val;
                        result.set_null(i, false);
                    }
                }
            }
        }
    }

    /// Vectorized logical OR.
    pub fn or(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        debug_assert_eq!(left.logical_type(), &LogicalType::Boolean);
        debug_assert_eq!(right.logical_type(), &LogicalType::Boolean);
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        let left_data = left.as_slice::<bool>();
        let right_data = right.as_slice::<bool>();

        match (left.vector_type(), right.vector_type()) {
            (VectorType::Constant, VectorType::Constant) => {
                result.set_vector_type(VectorType::Constant);
                let (res, is_null) = Self::triple_or(
                    left_data[0],
                    left.is_null(0),
                    right_data[0],
                    right.is_null(0),
                );
                result.as_mut_slice::<bool>()[0] = res;
                result.set_null(0, is_null);
            }
            _ => {
                result.set_vector_type(VectorType::Flat);
                for (i, (&l, &r)) in left_data
                    .iter()
                    .zip(right_data.iter())
                    .enumerate()
                    .take(count)
                {
                    let (res, is_null) = Self::triple_or(l, left.is_null(i), r, right.is_null(i));
                    result.as_mut_slice::<bool>()[i] = res;
                    result.set_null(i, is_null);
                }
            }
        }
    }

    /// Vectorized logical AND.
    pub fn and(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        debug_assert_eq!(left.logical_type(), &LogicalType::Boolean);
        debug_assert_eq!(right.logical_type(), &LogicalType::Boolean);
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        let left_data = left.as_slice::<bool>();
        let right_data = right.as_slice::<bool>();

        match (left.vector_type(), right.vector_type()) {
            (VectorType::Constant, VectorType::Constant) => {
                result.set_vector_type(VectorType::Constant);
                let (res, is_null) = Self::triple_and(
                    left_data[0],
                    left.is_null(0),
                    right_data[0],
                    right.is_null(0),
                );
                result.as_mut_slice::<bool>()[0] = res;
                result.set_null(0, is_null);
            }
            _ => {
                result.set_vector_type(VectorType::Flat);
                for (i, (&l, &r)) in left_data
                    .iter()
                    .zip(right_data.iter())
                    .enumerate()
                    .take(count)
                {
                    let (res, is_null) = Self::triple_and(l, left.is_null(i), r, right.is_null(i));
                    result.as_mut_slice::<bool>()[i] = res;
                    result.set_null(i, is_null);
                }
            }
        }
    }

    fn triple_or(l: bool, l_null: bool, r: bool, r_null: bool) -> (bool, bool) {
        if (!l_null && l) || (!r_null && r) {
            (true, false)
        } else if l_null || r_null {
            (false, true)
        } else {
            (false, false)
        }
    }

    fn triple_and(l: bool, l_null: bool, r: bool, r_null: bool) -> (bool, bool) {
        if (!l_null && !l) || (!r_null && !r) {
            (false, false)
        } else if l_null || r_null {
            (false, true)
        } else {
            (true, false)
        }
    }
}
