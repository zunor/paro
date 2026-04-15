// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector Comparison Operations
//! ## Design Notes
//! - Returns Boolean Vector with result of comparison
//! - NULL semantics: NULL on either side → NULL result
//! - Optimizations for Flat×Flat, Flat×Constant, Constant×Flat, Constant×Constant

use super::VectorOperations;
use crate::types::LogicalType;
use crate::vector::{Vector, VectorType};

impl VectorOperations {
    /// Compare two vectors for equality: left == right
    ///
    /// # Arguments
    /// * `left` - Left operand vector
    /// * `right` - Right operand vector
    /// * `result` - Output Boolean vector (must be pre-allocated)
    /// * `count` - Number of elements to compare
    pub fn equals(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        if matches!(left.logical_type(), LogicalType::Array(_, _)) {
            Self::compare_array(left, right, result, count, Comparison::Equal);
        } else {
            Self::execute_comparison(left, right, result, count, Comparison::Equal)
        }
    }

    /// Compare two vectors for inequality: left != right
    pub fn not_equals(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        if matches!(left.logical_type(), LogicalType::Array(_, _)) {
            Self::compare_array(left, right, result, count, Comparison::NotEqual);
        } else {
            Self::execute_comparison(left, right, result, count, Comparison::NotEqual)
        }
    }

    /// Compare two vectors: left < right
    pub fn less_than(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        Self::execute_comparison(left, right, result, count, Comparison::LessThan)
    }

    /// Compare two vectors: left <= right
    pub fn less_than_equals(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        Self::execute_comparison(left, right, result, count, Comparison::LessThanEquals)
    }

    /// Compare two vectors: left > right
    pub fn greater_than(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        Self::execute_comparison(left, right, result, count, Comparison::GreaterThan)
    }

    /// Compare two vectors: left >= right
    pub fn greater_than_equals(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        Self::execute_comparison(left, right, result, count, Comparison::GreaterThanEquals)
    }

    /// Execute a comparison operation with type dispatch.
    fn execute_comparison(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        // Get vector types for optimization dispatch
        let left_type = left.vector_type();
        let right_type = right.vector_type();

        // Ensure result is Boolean Flat vector
        debug_assert_eq!(result.logical_type(), &LogicalType::Boolean);
        result.set_count(count);

        match (left_type, right_type) {
            // Constant × Constant
            (VectorType::Constant, VectorType::Constant) => {
                Self::compare_constant_constant(left, right, result, count, op);
            }
            // Constant × Flat
            (VectorType::Constant, VectorType::Flat) => {
                Self::compare_constant_flat(left, right, result, count, op);
            }
            // Flat × Constant
            (VectorType::Flat, VectorType::Constant) => {
                Self::compare_flat_constant(left, right, result, count, op);
            }
            // Flat × Flat
            (VectorType::Flat, VectorType::Flat) => {
                Self::compare_flat_flat(left, right, result, count, op);
            }
            // General case (Dictionary, Sequence, or mixed)
            _ => {
                Self::compare_general(left, right, result, count, op);
            }
        }
    }

    /// Compare Constant × Constant (result is also constant)
    fn compare_constant_constant(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        _count: usize,
        op: Comparison,
    ) {
        // If either is NULL, result is NULL
        if left.is_null(0) || right.is_null(0) {
            result.validity_mut().set_null(0);
            return;
        }

        match left.logical_type() {
            LogicalType::Float => {
                let l = left.get_f32(0).unwrap();
                let r = right.get_f32(0).unwrap();
                unsafe { result.set_flat::<bool>(0, op.compare_f32(l, r)) };
            }
            LogicalType::Double => {
                let l = left.get_f64(0).unwrap();
                let r = right.get_f64(0).unwrap();
                unsafe { result.set_flat::<bool>(0, op.compare_f64(l, r)) };
            }
            _ => {
                let l_val = Self::get_comparable_value(left, 0);
                let r_val = Self::get_comparable_value(right, 0);

                if let (Some(l), Some(r)) = (l_val, r_val) {
                    let res = op.compare_i128(l, r);
                    unsafe { result.set_flat::<bool>(0, res) };
                } else {
                    result.validity_mut().set_null(0);
                }
            }
        }
    }

    /// Compare Constant × Flat
    fn compare_constant_flat(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        // If constant (left) is NULL, all results are NULL
        if left.is_null(0) {
            result.validity_mut().set_all_invalid(count);
            return;
        }

        match left.logical_type() {
            LogicalType::Float => {
                let l = left.get_f32(0).unwrap();
                for i in 0..count {
                    if right.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(r) = right.get_f32(i) {
                        unsafe { result.set_flat::<bool>(i, op.compare_f32(l, r)) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
            LogicalType::Double => {
                let l = left.get_f64(0).unwrap();
                for i in 0..count {
                    if right.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(r) = right.get_f64(i) {
                        unsafe { result.set_flat::<bool>(i, op.compare_f64(l, r)) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
            _ => {
                let l_val = Self::get_comparable_value(left, 0);
                if l_val.is_none() {
                    result.validity_mut().set_all_invalid(count);
                    return;
                }
                let l = l_val.unwrap();

                for i in 0..count {
                    if right.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(r) = Self::get_comparable_value(right, i) {
                        let res = op.compare_i128(l, r);
                        unsafe { result.set_flat::<bool>(i, res) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
        }
    }

    /// Compare Flat × Constant
    fn compare_flat_constant(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        // If constant (right) is NULL, all results are NULL
        if right.is_null(0) {
            result.validity_mut().set_all_invalid(count);
            return;
        }

        match right.logical_type() {
            LogicalType::Float => {
                let r = right.get_f32(0).unwrap();
                for i in 0..count {
                    if left.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(l) = left.get_f32(i) {
                        unsafe { result.set_flat::<bool>(i, op.compare_f32(l, r)) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
            LogicalType::Double => {
                let r = right.get_f64(0).unwrap();
                for i in 0..count {
                    if left.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(l) = left.get_f64(i) {
                        unsafe { result.set_flat::<bool>(i, op.compare_f64(l, r)) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
            _ => {
                let r_val = Self::get_comparable_value(right, 0);
                if r_val.is_none() {
                    result.validity_mut().set_all_invalid(count);
                    return;
                }
                let r = r_val.unwrap();

                for i in 0..count {
                    if left.is_null(i) {
                        result.validity_mut().set_null(i);
                    } else if let Some(l) = Self::get_comparable_value(left, i) {
                        let res = op.compare_i128(l, r);
                        unsafe { result.set_flat::<bool>(i, res) };
                    } else {
                        result.validity_mut().set_null(i);
                    }
                }
            }
        }
    }

    /// Compare Flat × Flat (most common case)
    fn compare_flat_flat(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        // Type-specialized fast path for common primitive types
        match left.logical_type() {
            LogicalType::Integer => {
                Self::compare_flat_flat_typed::<i32>(left, right, result, count, op);
            }
            LogicalType::BigInt => {
                Self::compare_flat_flat_typed::<i64>(left, right, result, count, op);
            }
            LogicalType::Boolean => {
                Self::compare_flat_flat_typed::<bool>(left, right, result, count, op);
            }
            LogicalType::Float => {
                Self::compare_flat_flat_float::<f32>(left, right, result, count, op);
            }
            LogicalType::Double => {
                Self::compare_flat_flat_float::<f64>(left, right, result, count, op);
            }
            _ => {
                // General case
                Self::compare_general(left, right, result, count, op);
            }
        }
    }

    /// Type-specialized Flat × Flat comparison (zero-copy, fast path)
    fn compare_flat_flat_typed<T>(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) where
        T: Copy + Into<i128>,
    {
        let l_data = unsafe { left.flat_data::<T>() };
        let r_data = unsafe { right.flat_data::<T>() };
        let res_data = unsafe { result.flat_data_mut::<bool>() };

        let l_valid = left.validity();
        let r_valid = right.validity();

        for i in 0..count {
            if l_valid.is_valid(i) && r_valid.is_valid(i) {
                let l_val: i128 = unsafe { (*l_data.add(i)).into() };
                let r_val: i128 = unsafe { (*r_data.add(i)).into() };
                unsafe { *res_data.add(i) = op.compare_i128(l_val, r_val) };
            } else {
                result.validity_mut().set_null(i);
            }
        }
    }

    /// Type-specialized Flat × Flat comparison for Floats
    fn compare_flat_flat_float<T>(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) where
        T: Copy + Into<f64>,
    {
        let l_data = unsafe { left.flat_data::<T>() };
        let r_data = unsafe { right.flat_data::<T>() };
        let res_data = unsafe { result.flat_data_mut::<bool>() };

        let l_valid = left.validity();
        let r_valid = right.validity();

        for i in 0..count {
            if l_valid.is_valid(i) && r_valid.is_valid(i) {
                let l_val: f64 = unsafe { (*l_data.add(i)).into() };
                let r_val: f64 = unsafe { (*r_data.add(i)).into() };
                unsafe { *res_data.add(i) = op.compare_f64(l_val, r_val) };
            } else {
                result.validity_mut().set_null(i);
            }
        }
    }

    /// General comparison (works for any vector types)
    fn compare_general(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        for i in 0..count {
            if left.is_null(i) || right.is_null(i) {
                result.validity_mut().set_null(i);
            } else {
                let l_val = Self::get_comparable_value(left, i);
                let r_val = Self::get_comparable_value(right, i);

                match (l_val, r_val) {
                    (Some(l), Some(r)) => {
                        let res = op.compare_i128(l, r);
                        unsafe { result.set_flat::<bool>(i, res) };
                    }
                    _ => {
                        // try float as fallback
                        if let (Some(l), Some(r)) = (left.get_f64(i), right.get_f64(i)) {
                            unsafe { result.set_flat::<bool>(i, op.compare_f64(l, r)) };
                        } else {
                            result.validity_mut().set_null(i);
                        }
                    }
                }
            }
        }
    }

    /// Get a comparable value from a vector at a given index.
    /// Returns None for unsupported types or NULL values.
    fn get_comparable_value(vec: &Vector, idx: usize) -> Option<i128> {
        match vec.logical_type() {
            LogicalType::Integer => vec.get_i32(idx).map(|v| v as i128),
            LogicalType::BigInt => vec.get_i64(idx).map(|v| v as i128),
            LogicalType::Boolean => vec.get_bool(idx).map(|v| if v { 1 } else { 0 }),
            LogicalType::SmallInt => vec.get_i16(idx).map(|v| v as i128),
            LogicalType::TinyInt => vec.get_i8(idx).map(|v| v as i128),
            // Non-numeric types are handled by the generic comparison path.
            _ => None,
        }
    }

    /// Compare Array vectors.
    fn compare_array(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
        op: Comparison,
    ) {
        let array_size = match left.logical_type() {
            LogicalType::Array(_, size) => *size,
            _ => unreachable!(),
        };

        // For array comparison, we compare child vectors
        let left_child = left.child().expect("Array vector missing child");
        let right_child = right.child().expect("Array vector missing child");
        let child_count = count * array_size;

        let mut child_result = Vector::with_capacity(LogicalType::Boolean, child_count);
        match op {
            Comparison::Equal => {
                Self::equals(left_child, right_child, &mut child_result, child_count)
            }
            Comparison::NotEqual => {
                Self::not_equals(left_child, right_child, &mut child_result, child_count)
            }
            // Lexicographical comparison for others
            _ => {
                // In an MVP, only support Equal/NotEqual for Array
            }
        }

        result.set_count(count);
        // Now combine results
        for i in 0..count {
            if left.is_null(i) || right.is_null(i) {
                result.validity_mut().set_null(i);
                continue;
            }

            let offset = i * array_size;
            let mut res = match op {
                Comparison::Equal => true,
                Comparison::NotEqual => false,
                _ => false, // fallback
            };

            for j in 0..array_size {
                let elem_res = child_result.get_bool(offset + j).unwrap_or(false);
                match op {
                    Comparison::Equal => {
                        if !elem_res {
                            res = false;
                            break;
                        }
                    }
                    Comparison::NotEqual => {
                        if elem_res {
                            res = true;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            result.set_bool(i, res);
        }
    }

    // ========== Select Operations (Optimized Path) ==========
    //
    // These functions directly return the indices where the comparison is true,
    // avoiding the intermediate Boolean Vector creation.

    /// Select rows where left == right.
    ///
    /// Returns the number of matching rows and fills `true_sel` with their indices.
    /// NULL comparisons are excluded (NULL == x is never true).
    pub fn select_equals(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l == r)
    }

    /// Select rows where left != right.
    pub fn select_not_equals(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l != r)
    }

    /// Select rows where left < right.
    pub fn select_less_than(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l < r)
    }

    /// Select rows where left <= right.
    pub fn select_less_than_equals(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l <= r)
    }

    /// Select rows where left > right.
    pub fn select_greater_than(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l > r)
    }

    /// Select rows where left >= right.
    pub fn select_greater_than_equals(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
    ) -> usize {
        Self::select_comparison(left, right, count, true_sel, |l, r| l >= r)
    }

    /// Execute a select operation (returns matching indices directly).
    fn select_comparison<F>(
        left: &Vector,
        right: &Vector,
        count: usize,
        true_sel: &mut Vec<usize>,
        cmp: F,
    ) -> usize
    where
        F: Fn(i128, i128) -> bool + Copy,
    {
        true_sel.clear();

        for i in 0..count {
            // Skip NULLs - NULL comparison never returns true
            if left.is_null(i) || right.is_null(i) {
                continue;
            }

            let l_val = Self::get_comparable_value(left, i);
            let r_val = Self::get_comparable_value(right, i);

            if let (Some(l), Some(r)) = (l_val, r_val) {
                if cmp(l, r) {
                    true_sel.push(i);
                }
            }
        }

        true_sel.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_value::Value;

    fn create_bool_result(count: usize) -> Vector {
        let mut v = Vector::with_capacity(LogicalType::Boolean, count);
        v.set_count(count);
        v
    }

    #[test]
    fn test_equals_flat_flat() {
        let left = Vector::from_i32(&[1, 2, 3, 4]);
        let right = Vector::from_i32(&[1, 3, 3, 5]);
        let mut result = create_bool_result(4);

        VectorOperations::equals(&left, &right, &mut result, 4);

        assert_eq!(result.get_bool(0), Some(true)); // 1 == 1
        assert_eq!(result.get_bool(1), Some(false)); // 2 == 3
        assert_eq!(result.get_bool(2), Some(true)); // 3 == 3
        assert_eq!(result.get_bool(3), Some(false)); // 4 == 5
    }

    #[test]
    fn test_less_than_flat_flat() {
        let left = Vector::from_i32(&[1, 2, 3, 4]);
        let right = Vector::from_i32(&[2, 2, 2, 2]);
        let mut result = create_bool_result(4);

        VectorOperations::less_than(&left, &right, &mut result, 4);

        assert_eq!(result.get_bool(0), Some(true)); // 1 < 2
        assert_eq!(result.get_bool(1), Some(false)); // 2 < 2
        assert_eq!(result.get_bool(2), Some(false)); // 3 < 2
        assert_eq!(result.get_bool(3), Some(false)); // 4 < 2
    }

    #[test]
    fn test_greater_than_equals_flat_flat() {
        let left = Vector::from_i32(&[1, 2, 3, 4]);
        let right = Vector::from_i32(&[2, 2, 2, 2]);
        let mut result = create_bool_result(4);

        VectorOperations::greater_than_equals(&left, &right, &mut result, 4);

        assert_eq!(result.get_bool(0), Some(false)); // 1 >= 2
        assert_eq!(result.get_bool(1), Some(true)); // 2 >= 2
        assert_eq!(result.get_bool(2), Some(true)); // 3 >= 2
        assert_eq!(result.get_bool(3), Some(true)); // 4 >= 2
    }

    #[test]
    fn test_null_handling() {
        let mut left = Vector::from_i32(&[1, 2, 3]);
        left.validity_mut().set_null(1); // second element is NULL

        let right = Vector::from_i32(&[1, 2, 3]);
        let mut result = create_bool_result(3);

        VectorOperations::equals(&left, &right, &mut result, 3);

        assert_eq!(result.get_bool(0), Some(true)); // 1 == 1
        assert!(result.is_null(1)); // NULL == 2 -> NULL
        assert_eq!(result.get_bool(2), Some(true)); // 3 == 3
    }

    #[test]
    fn test_constant_flat() {
        let left = Vector::constant::<i32>(LogicalType::Integer, 10, 4);
        let right = Vector::from_i32(&[5, 10, 15, 20]);
        let mut result = create_bool_result(4);

        VectorOperations::greater_than(&left, &right, &mut result, 4);

        assert_eq!(result.get_bool(0), Some(true)); // 10 > 5
        assert_eq!(result.get_bool(1), Some(false)); // 10 > 10
        assert_eq!(result.get_bool(2), Some(false)); // 10 > 15
        assert_eq!(result.get_bool(3), Some(false)); // 10 > 20
    }

    #[test]
    fn test_bigint_comparison() {
        let left = Vector::from_i64(&[100_000_000_000i64, 200_000_000_000i64]);
        let right = Vector::from_i64(&[100_000_000_000i64, 100_000_000_000i64]);
        let mut result = create_bool_result(2);

        VectorOperations::equals(&left, &right, &mut result, 2);

        assert_eq!(result.get_bool(0), Some(true)); // equal
        assert_eq!(result.get_bool(1), Some(false)); // not equal
    }

    #[test]
    fn test_not_equals() {
        let left = Vector::from_i32(&[1, 2, 3]);
        let right = Vector::from_i32(&[1, 3, 3]);
        let mut result = create_bool_result(3);

        VectorOperations::not_equals(&left, &right, &mut result, 3);

        assert_eq!(result.get_bool(0), Some(false)); // 1 != 1
        assert_eq!(result.get_bool(1), Some(true)); // 2 != 3
        assert_eq!(result.get_bool(2), Some(false)); // 3 != 3
    }

    #[test]
    fn test_all_comparison_operators() {
        let left = Vector::from_i32(&[5]);
        let right = Vector::from_i32(&[3]);

        let mut result_eq = create_bool_result(1);
        let mut result_ne = create_bool_result(1);
        let mut result_lt = create_bool_result(1);
        let mut result_le = create_bool_result(1);
        let mut result_gt = create_bool_result(1);
        let mut result_ge = create_bool_result(1);

        VectorOperations::equals(&left, &right, &mut result_eq, 1);
        VectorOperations::not_equals(&left, &right, &mut result_ne, 1);
        VectorOperations::less_than(&left, &right, &mut result_lt, 1);
        VectorOperations::less_than_equals(&left, &right, &mut result_le, 1);
        VectorOperations::greater_than(&left, &right, &mut result_gt, 1);
        VectorOperations::greater_than_equals(&left, &right, &mut result_ge, 1);

        assert_eq!(result_eq.get_bool(0), Some(false)); // 5 == 3
        assert_eq!(result_ne.get_bool(0), Some(true)); // 5 != 3
        assert_eq!(result_lt.get_bool(0), Some(false)); // 5 < 3
        assert_eq!(result_le.get_bool(0), Some(false)); // 5 <= 3
        assert_eq!(result_gt.get_bool(0), Some(true)); // 5 > 3
        assert_eq!(result_ge.get_bool(0), Some(true)); // 5 >= 3
    }

    // ========== Select Operation Tests ==========

    #[test]
    fn test_select_equals() {
        let left = Vector::from_i32(&[1, 2, 3, 4, 5]);
        let right = Vector::from_i32(&[1, 3, 3, 5, 5]);
        let mut true_sel = Vec::new();

        let count = VectorOperations::select_equals(&left, &right, 5, &mut true_sel);

        assert_eq!(count, 3);
        assert_eq!(true_sel, vec![0, 2, 4]); // 1==1 at idx 0, 3==3 at idx 2, 5==5 at idx 4
    }

    #[test]
    fn test_select_less_than() {
        let left = Vector::from_i32(&[1, 2, 3, 4]);
        let right = Vector::from_i32(&[2, 2, 2, 2]);
        let mut true_sel = Vec::new();

        let count = VectorOperations::select_less_than(&left, &right, 4, &mut true_sel);

        assert_eq!(count, 1);
        assert_eq!(true_sel, vec![0]); // 1 < 2 at idx 0
    }

    #[test]
    fn test_select_greater_than_equals() {
        let left = Vector::from_i32(&[1, 2, 3, 4]);
        let right = Vector::from_i32(&[2, 2, 2, 2]);
        let mut true_sel = Vec::new();

        let count = VectorOperations::select_greater_than_equals(&left, &right, 4, &mut true_sel);

        assert_eq!(count, 3);
        assert_eq!(true_sel, vec![1, 2, 3]); // 2>=2, 3>=2, 4>=2
    }

    #[test]
    fn test_select_with_null() {
        let mut left = Vector::from_i32(&[1, 2, 3, 4]);
        left.validity_mut().set_null(1); // second element is NULL
        let right = Vector::from_i32(&[1, 2, 3, 4]);
        let mut true_sel = Vec::new();

        let count = VectorOperations::select_equals(&left, &right, 4, &mut true_sel);

        // NULL comparisons are excluded
        assert_eq!(count, 3);
        assert_eq!(true_sel, vec![0, 2, 3]); // 1==1, 3==3, 4==4 (skips NULL at idx 1)
    }

    #[test]
    fn test_select_all_comparison_types() {
        let left = Vector::from_i32(&[5, 5, 5]);
        let right = Vector::from_i32(&[3, 5, 7]);

        let mut sel_eq = Vec::new();
        let mut sel_ne = Vec::new();
        let mut sel_lt = Vec::new();
        let mut sel_le = Vec::new();
        let mut sel_gt = Vec::new();
        let mut sel_ge = Vec::new();

        VectorOperations::select_equals(&left, &right, 3, &mut sel_eq);
        VectorOperations::select_not_equals(&left, &right, 3, &mut sel_ne);
        VectorOperations::select_less_than(&left, &right, 3, &mut sel_lt);
        VectorOperations::select_less_than_equals(&left, &right, 3, &mut sel_le);
        VectorOperations::select_greater_than(&left, &right, 3, &mut sel_gt);
        VectorOperations::select_greater_than_equals(&left, &right, 3, &mut sel_ge);

        assert_eq!(sel_eq, vec![1]); // 5 == 5 at idx 1
        assert_eq!(sel_ne, vec![0, 2]); // 5 != 3, 5 != 7
        assert_eq!(sel_lt, vec![2]); // 5 < 7 at idx 2
        assert_eq!(sel_le, vec![1, 2]); // 5 <= 5, 5 <= 7
        assert_eq!(sel_gt, vec![0]); // 5 > 3 at idx 0
        assert_eq!(sel_ge, vec![0, 1]); // 5 >= 3, 5 >= 5
    }

    #[test]
    fn test_equals_array() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 3);

        let mut v1 = Vector::new_array(array_type.clone(), 2);
        v1.set_count(2);
        v1.set_value(
            0,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        v1.set_value(
            1,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );

        let mut v2 = Vector::new_array(array_type.clone(), 2);
        v2.set_count(2);
        v2.set_value(
            0,
            &Value::Array(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
                3,
            ),
        );
        v2.set_value(
            1,
            &Value::Array(
                vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
                LogicalType::Integer,
                3,
            ),
        );

        let mut result = create_bool_result(2);
        VectorOperations::equals(&v1, &v2, &mut result, 2);

        assert_eq!(result.get_bool(0), Some(true)); // [1,2,3] == [1,2,3]
        assert_eq!(result.get_bool(1), Some(false)); // [1,2,3] == [4,5,6]
    }
}

/// Specialized comparison operators.
#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanEquals,
    GreaterThan,
    GreaterThanEquals,
}

impl Comparison {
    pub fn compare_i128(&self, left: i128, right: i128) -> bool {
        match self {
            Comparison::Equal => left == right,
            Comparison::NotEqual => left != right,
            Comparison::LessThan => left < right,
            Comparison::LessThanEquals => left <= right,
            Comparison::GreaterThan => left > right,
            Comparison::GreaterThanEquals => left >= right,
        }
    }

    pub fn compare_f64(&self, left: f64, right: f64) -> bool {
        match self {
            Comparison::Equal => {
                if left.is_nan() && right.is_nan() {
                    return true;
                }
                left == right
            }
            Comparison::NotEqual => {
                if left.is_nan() && right.is_nan() {
                    return false;
                }
                left != right
            }
            Comparison::LessThan => {
                if right.is_nan() {
                    return !left.is_nan();
                }
                if left.is_nan() {
                    return false;
                }
                left < right
            }
            Comparison::LessThanEquals => {
                if right.is_nan() {
                    return true;
                }
                if left.is_nan() {
                    return right.is_nan();
                }
                left <= right
            }
            Comparison::GreaterThan => {
                if left.is_nan() {
                    return !right.is_nan();
                }
                if right.is_nan() {
                    return false;
                }
                left > right
            }
            Comparison::GreaterThanEquals => {
                if left.is_nan() {
                    return true;
                }
                if right.is_nan() {
                    return left.is_nan();
                }
                left >= right
            }
        }
    }

    pub fn compare_f32(&self, left: f32, right: f32) -> bool {
        self.compare_f64(left as f64, right as f64)
    }
}
