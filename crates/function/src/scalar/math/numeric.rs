// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Basic numeric math functions.
//!
//!
//!
//! ## Functions
//! - `abs` - Absolute value
//! - `ceil`, `ceiling` - Round up to nearest integer
//! - `floor` - Round down to nearest integer
//! - `round` - Round to nearest integer or decimal places
//! - `trunc`, `truncate` - Truncate toward zero
//! - `sign` - Sign of a number (-1, 0, 1)

use crate::scalar::executor::unary::UnaryExecutor;
use crate::scalar::executor::UnaryOperator;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// ============================================================================
// ABS - Absolute value
// ============================================================================

struct AbsOpI32;
impl UnaryOperator<i32, i32> for AbsOpI32 {
    fn operation(input: i32) -> i32 {
        input.abs()
    }
}

struct AbsOpI64;
impl UnaryOperator<i64, i64> for AbsOpI64 {
    fn operation(input: i64) -> i64 {
        input.abs()
    }
}

struct AbsOpF64;
impl UnaryOperator<f64, f64> for AbsOpF64 {
    fn operation(input: f64) -> f64 {
        input.abs()
    }
}

fn abs_i32(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<i32, i32, AbsOpI32>(&input.data[0], result, input.size())?;
    Ok(())
}

fn abs_i64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<i64, i64, AbsOpI64>(&input.data[0], result, input.size())?;
    Ok(())
}

fn abs_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, AbsOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `abs` function set.
pub fn get_abs_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("abs".to_string());

    set.add_function(ScalarFunction::new(
        "abs".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        abs_i32,
    ));

    set.add_function(ScalarFunction::new(
        "abs".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        abs_i64,
    ));

    set.add_function(ScalarFunction::new(
        "abs".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        abs_f64,
    ));

    set
}

// ============================================================================
// CEIL / CEILING - Round up to nearest integer
// ============================================================================

struct CeilOpF64;
impl UnaryOperator<f64, f64> for CeilOpF64 {
    fn operation(input: f64) -> f64 {
        input.ceil()
    }
}

fn ceil_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, CeilOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `ceil` function set.
pub fn get_ceil_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ceil".to_string());

    // ceil(DOUBLE) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "ceil".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        ceil_f64,
    ));

    // Also add alias "ceiling"
    set.add_function(ScalarFunction::new(
        "ceiling".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        ceil_f64,
    ));

    set
}

// ============================================================================
// FLOOR - Round down to nearest integer
// ============================================================================

struct FloorOpF64;
impl UnaryOperator<f64, f64> for FloorOpF64 {
    fn operation(input: f64) -> f64 {
        input.floor()
    }
}

fn floor_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, FloorOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `floor` function set.
pub fn get_floor_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("floor".to_string());

    // floor(DOUBLE) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "floor".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        floor_f64,
    ));

    set
}

// ============================================================================
// ROUND - Round to nearest integer or decimal places
// ============================================================================

struct RoundOpF64;
impl UnaryOperator<f64, f64> for RoundOpF64 {
    fn operation(input: f64) -> f64 {
        input.round()
    }
}

fn round_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, RoundOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Round to specified decimal places.
fn round_precision(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use crate::scalar::executor::binary::BinaryExecutor;
    use crate::scalar::executor::BinaryOperator;

    struct RoundPrecisionOp;
    impl BinaryOperator<f64, i32, f64> for RoundPrecisionOp {
        fn operation(value: f64, precision: i32) -> f64 {
            if precision >= 0 {
                let factor = 10_f64.powi(precision);
                (value * factor).round() / factor
            } else {
                let factor = 10_f64.powi(-precision);
                (value / factor).round() * factor
            }
        }
    }

    BinaryExecutor::execute::<f64, i32, f64, RoundPrecisionOp>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    )?;
    Ok(())
}

/// Get the `round` function set.
pub fn get_round_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("round".to_string());

    // round(DOUBLE) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "round".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        round_f64,
    ));

    // round(DOUBLE, INTEGER) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "round".to_string(),
        vec![LogicalType::Double, LogicalType::Integer],
        LogicalType::Double,
        round_precision,
    ));

    set
}

// ============================================================================
// TRUNC / TRUNCATE - Truncate toward zero
// ============================================================================

struct TruncOpF64;
impl UnaryOperator<f64, f64> for TruncOpF64 {
    fn operation(input: f64) -> f64 {
        input.trunc()
    }
}

fn trunc_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, TruncOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Truncate to specified decimal places.
fn trunc_precision(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use crate::scalar::executor::binary::BinaryExecutor;
    use crate::scalar::executor::BinaryOperator;

    struct TruncPrecisionOp;
    impl BinaryOperator<f64, i32, f64> for TruncPrecisionOp {
        fn operation(value: f64, precision: i32) -> f64 {
            if precision >= 0 {
                let factor = 10_f64.powi(precision);
                (value * factor).trunc() / factor
            } else {
                let factor = 10_f64.powi(-precision);
                (value / factor).trunc() * factor
            }
        }
    }

    BinaryExecutor::execute::<f64, i32, f64, TruncPrecisionOp>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    )?;
    Ok(())
}

/// Get the `trunc` function set.
pub fn get_trunc_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("trunc".to_string());

    // trunc(DOUBLE) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "trunc".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        trunc_f64,
    ));

    // trunc(DOUBLE, INTEGER) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "trunc".to_string(),
        vec![LogicalType::Double, LogicalType::Integer],
        LogicalType::Double,
        trunc_precision,
    ));

    // Alias: truncate
    set.add_function(ScalarFunction::new(
        "truncate".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        trunc_f64,
    ));

    set
}

// ============================================================================
// SIGN - Sign of a number (-1, 0, 1)
// ============================================================================

struct SignOpI32;
impl UnaryOperator<i32, i32> for SignOpI32 {
    fn operation(input: i32) -> i32 {
        match input.cmp(&0) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

struct SignOpI64;
impl UnaryOperator<i64, i64> for SignOpI64 {
    fn operation(input: i64) -> i64 {
        match input.cmp(&0) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

struct SignOpF64;
impl UnaryOperator<f64, i32> for SignOpF64 {
    fn operation(input: f64) -> i32 {
        if input.is_nan() || input == 0.0 {
            0
        } else if input > 0.0 {
            1
        } else {
            -1
        }
    }
}

fn sign_i32(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<i32, i32, SignOpI32>(&input.data[0], result, input.size())?;
    Ok(())
}

fn sign_i64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<i64, i64, SignOpI64>(&input.data[0], result, input.size())?;
    Ok(())
}

fn sign_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, i32, SignOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `sign` function set.
pub fn get_sign_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("sign".to_string());

    set.add_function(ScalarFunction::new(
        "sign".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        sign_i32,
    ));

    set.add_function(ScalarFunction::new(
        "sign".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        sign_i64,
    ));

    set.add_function(ScalarFunction::new(
        "sign".to_string(),
        vec![LogicalType::Double],
        LogicalType::Integer,
        sign_f64,
    ));

    set
}

// ============================================================================
// EXP - Exponential (e^x)
// ============================================================================

struct ExpOpF64;
impl UnaryOperator<f64, f64> for ExpOpF64 {
    fn operation(input: f64) -> f64 {
        input.exp()
    }
}

fn exp_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, ExpOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `exp` function set.
pub fn get_exp_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("exp".to_string());

    set.add_function(ScalarFunction::new(
        "exp".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        exp_f64,
    ));

    set
}

// ============================================================================
// POW / POWER - Exponentiation
// ============================================================================

fn pow_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use crate::scalar::executor::binary::BinaryExecutor;
    use crate::scalar::executor::BinaryOperator;

    struct PowOp;
    impl BinaryOperator<f64, f64, f64> for PowOp {
        fn operation(base: f64, exponent: f64) -> f64 {
            base.powf(exponent)
        }
    }

    BinaryExecutor::execute::<f64, f64, f64, PowOp>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    )?;
    Ok(())
}

/// Get the `pow` function set.
pub fn get_pow_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("pow".to_string());

    // pow(DOUBLE, DOUBLE) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "pow".to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        pow_f64,
    ));

    // Alias: power
    set.add_function(ScalarFunction::new(
        "power".to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        pow_f64,
    ));

    set
}

// ============================================================================
// SQRT - Square root
// ============================================================================

struct SqrtOpF64;
impl UnaryOperator<f64, f64> for SqrtOpF64 {
    fn operation(input: f64) -> f64 {
        input.sqrt()
    }
}

fn sqrt_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, SqrtOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `sqrt` function set.
pub fn get_sqrt_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("sqrt".to_string());

    set.add_function(ScalarFunction::new(
        "sqrt".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        sqrt_f64,
    ));

    set
}

// ============================================================================
// CBRT - Cube root
// ============================================================================

struct CbrtOpF64;
impl UnaryOperator<f64, f64> for CbrtOpF64 {
    fn operation(input: f64) -> f64 {
        input.cbrt()
    }
}

fn cbrt_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, CbrtOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `cbrt` function set.
pub fn get_cbrt_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("cbrt".to_string());

    set.add_function(ScalarFunction::new(
        "cbrt".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        cbrt_f64,
    ));

    set
}

#[cfg(test)]
mod tests {
    use super::*;

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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn create_f64_chunk(values: &[f64]) -> Chunk {
        let vec = paro_common::test_utils::test_f64_vector_with_allocator(
            values,
            paro_common::test_utils::test_allocator(),
        );
        paro_common::test_utils::test_chunk_from_vectors(vec![vec])
    }

    fn create_i32_chunk(values: &[i32]) -> Chunk {
        let vec = paro_common::test_utils::test_i32_vector_with_allocator(
            values,
            paro_common::test_utils::test_allocator(),
        );
        paro_common::test_utils::test_chunk_from_vectors(vec![vec])
    }

    fn create_i64_chunk(values: &[i64]) -> Chunk {
        let vec = paro_common::test_utils::test_i64_vector_with_allocator(
            values,
            paro_common::test_utils::test_allocator(),
        );
        paro_common::test_utils::test_chunk_from_vectors(vec![vec])
    }

    #[test]
    fn test_abs_i32() {
        let chunk = create_i32_chunk(&[-5, 0, 10, -100]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        abs_i32(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(5));
        assert_eq!(result.get_i32(1), Some(0));
        assert_eq!(result.get_i32(2), Some(10));
        assert_eq!(result.get_i32(3), Some(100));
    }

    #[test]
    fn test_abs_f64() {
        let chunk = create_f64_chunk(&[-3.14, 0.0, 2.71, -100.5]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        abs_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 3.14).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.71).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - 100.5).abs() < 1e-10);
    }

    #[test]
    fn test_ceil() {
        let chunk = create_f64_chunk(&[1.1, 1.9, -1.1, -1.9, 2.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        ceil_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - (-1.0)).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - (-1.0)).abs() < 1e-10);
        assert!((result.get_f64(4).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_floor() {
        let chunk = create_f64_chunk(&[1.1, 1.9, -1.1, -1.9, 2.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        floor_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - (-2.0)).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - (-2.0)).abs() < 1e-10);
        assert!((result.get_f64(4).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_round() {
        let chunk = create_f64_chunk(&[1.4, 1.5, 1.6, -1.4, -1.5, -1.6]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        round_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - (-1.0)).abs() < 1e-10);
        assert!((result.get_f64(4).unwrap() - (-2.0)).abs() < 1e-10);
        assert!((result.get_f64(5).unwrap() - (-2.0)).abs() < 1e-10);
    }

    #[test]
    fn test_round_precision() {
        let values = paro_common::test_utils::test_f64_vector_with_allocator(
            &[std::f64::consts::PI, std::f64::consts::E, 123.456],
            paro_common::test_utils::test_allocator(),
        );
        let precision = paro_common::test_utils::test_i32_vector_with_allocator(
            &[2, 3, -1],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![values, precision],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        round_precision(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 3.14).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 2.718).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 120.0).abs() < 1e-10);
    }

    #[test]
    fn test_trunc() {
        let chunk = create_f64_chunk(&[1.9, -1.9, 2.0, -2.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        trunc_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - (-1.0)).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - (-2.0)).abs() < 1e-10);
    }

    #[test]
    fn test_sign_i32() {
        let chunk = create_i32_chunk(&[-5, 0, 10]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        sign_i32(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(-1));
        assert_eq!(result.get_i32(1), Some(0));
        assert_eq!(result.get_i32(2), Some(1));
    }

    #[test]
    fn test_sign_f64() {
        let chunk = create_f64_chunk(&[-3.14, 0.0, 2.71, f64::NAN]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        sign_f64(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(-1));
        assert_eq!(result.get_i32(1), Some(0));
        assert_eq!(result.get_i32(2), Some(1));
        assert_eq!(result.get_i32(3), Some(0)); // NaN returns 0
    }

    #[test]
    fn test_exp() {
        let chunk = create_f64_chunk(&[0.0, 1.0, 2.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        exp_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - std::f64::consts::E).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - std::f64::consts::E.powi(2)).abs() < 1e-10);
    }

    #[test]
    fn test_pow() {
        let base = paro_common::test_utils::test_f64_vector_with_allocator(
            &[2.0, 3.0, 10.0],
            paro_common::test_utils::test_allocator(),
        );
        let exp = paro_common::test_utils::test_f64_vector_with_allocator(
            &[3.0, 2.0, 0.0],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![base, exp]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        pow_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 8.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 9.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_sqrt() {
        let chunk = create_f64_chunk(&[4.0, 9.0, 16.0, 2.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        sqrt_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 3.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 4.0).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - std::f64::consts::SQRT_2).abs() < 1e-10);
    }

    #[test]
    fn test_cbrt() {
        let chunk = create_f64_chunk(&[8.0, 27.0, -8.0, 1.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        cbrt_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 3.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - (-2.0)).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_abs_i64() {
        let chunk = create_i64_chunk(&[-5, 0, 10, -100]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        abs_i64(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(5));
        assert_eq!(result.get_i64(1), Some(0));
        assert_eq!(result.get_i64(2), Some(10));
        assert_eq!(result.get_i64(3), Some(100));
    }
}
