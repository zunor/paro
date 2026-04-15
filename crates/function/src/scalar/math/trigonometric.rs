//! Trigonometric functions.
//!
//!
//!
//! ## Functions
//! - `sin`, `cos`, `tan` - Basic trigonometric functions
//! - `asin`, `acos`, `atan` - Inverse trigonometric functions
//! - `atan2` - Two-argument arctangent

use crate::scalar::executor::unary::UnaryExecutor;
use crate::scalar::executor::UnaryOperator;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// ============================================================================
// SIN - Sine
// ============================================================================

struct SinOpF64;
impl UnaryOperator<f64, f64> for SinOpF64 {
    fn operation(input: f64) -> f64 {
        input.sin()
    }
}

fn sin_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, SinOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `sin` function set.
pub fn get_sin_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("sin".to_string());

    set.add_function(ScalarFunction::new(
        "sin".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        sin_f64,
    ));

    set
}

// ============================================================================
// COS - Cosine
// ============================================================================

struct CosOpF64;
impl UnaryOperator<f64, f64> for CosOpF64 {
    fn operation(input: f64) -> f64 {
        input.cos()
    }
}

fn cos_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, CosOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `cos` function set.
pub fn get_cos_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("cos".to_string());

    set.add_function(ScalarFunction::new(
        "cos".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        cos_f64,
    ));

    set
}

// ============================================================================
// TAN - Tangent
// ============================================================================

struct TanOpF64;
impl UnaryOperator<f64, f64> for TanOpF64 {
    fn operation(input: f64) -> f64 {
        input.tan()
    }
}

fn tan_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, TanOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `tan` function set.
pub fn get_tan_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("tan".to_string());

    set.add_function(ScalarFunction::new(
        "tan".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        tan_f64,
    ));

    set
}

// ============================================================================
// ASIN - Arc sine (inverse sine)
// ============================================================================

struct AsinOpF64;
impl UnaryOperator<f64, f64> for AsinOpF64 {
    fn operation(input: f64) -> f64 {
        input.asin()
    }
}

fn asin_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, AsinOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `asin` function set.
pub fn get_asin_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("asin".to_string());

    set.add_function(ScalarFunction::new(
        "asin".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        asin_f64,
    ));

    set
}

// ============================================================================
// ACOS - Arc cosine (inverse cosine)
// ============================================================================

struct AcosOpF64;
impl UnaryOperator<f64, f64> for AcosOpF64 {
    fn operation(input: f64) -> f64 {
        input.acos()
    }
}

fn acos_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, AcosOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `acos` function set.
pub fn get_acos_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("acos".to_string());

    set.add_function(ScalarFunction::new(
        "acos".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        acos_f64,
    ));

    set
}

// ============================================================================
// ATAN - Arc tangent (inverse tangent)
// ============================================================================

struct AtanOpF64;
impl UnaryOperator<f64, f64> for AtanOpF64 {
    fn operation(input: f64) -> f64 {
        input.atan()
    }
}

fn atan_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, AtanOpF64>(&input.data[0], result, input.size());
    Ok(())
}

/// Get the `atan` function set.
pub fn get_atan_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("atan".to_string());

    set.add_function(ScalarFunction::new(
        "atan".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        atan_f64,
    ));

    set
}

// ============================================================================
// ATAN2 - Two-argument arctangent
// ============================================================================

fn atan2_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use crate::scalar::executor::binary::BinaryExecutor;
    use crate::scalar::executor::BinaryOperator;

    struct Atan2Op;
    impl BinaryOperator<f64, f64, f64> for Atan2Op {
        fn operation(y: f64, x: f64) -> f64 {
            y.atan2(x)
        }
    }

    BinaryExecutor::execute::<f64, f64, f64, Atan2Op>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    );
    Ok(())
}

/// Get the `atan2` function set.
pub fn get_atan2_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("atan2".to_string());

    // atan2(y, x) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "atan2".to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        atan2_f64,
    ));

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, PI};

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
        let vec = Vector::from_f64(values);
        Chunk::from_vectors(vec![vec])
    }

    #[test]
    fn test_sin() {
        let chunk = create_f64_chunk(&[0.0, FRAC_PI_2, PI]);
        let mut result = Vector::new(LogicalType::Double);

        sin_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
        assert!(result.get_f64(2).unwrap().abs() < 1e-10); // sin(π) ≈ 0
    }

    #[test]
    fn test_cos() {
        let chunk = create_f64_chunk(&[0.0, FRAC_PI_2, PI]);
        let mut result = Vector::new(LogicalType::Double);

        cos_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.0).abs() < 1e-10);
        assert!(result.get_f64(1).unwrap().abs() < 1e-10); // cos(π/2) ≈ 0
        assert!((result.get_f64(2).unwrap() - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn test_tan() {
        let chunk = create_f64_chunk(&[0.0, FRAC_PI_4]);
        let mut result = Vector::new(LogicalType::Double);

        tan_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_asin() {
        let chunk = create_f64_chunk(&[0.0, 1.0, -1.0]);
        let mut result = Vector::new(LogicalType::Double);

        asin_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - FRAC_PI_2).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - (-FRAC_PI_2)).abs() < 1e-10);
    }

    #[test]
    fn test_acos() {
        let chunk = create_f64_chunk(&[1.0, 0.0, -1.0]);
        let mut result = Vector::new(LogicalType::Double);

        acos_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - FRAC_PI_2).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - PI).abs() < 1e-10);
    }

    #[test]
    fn test_atan() {
        let chunk = create_f64_chunk(&[0.0, 1.0, -1.0]);
        let mut result = Vector::new(LogicalType::Double);

        atan_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - FRAC_PI_4).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - (-FRAC_PI_4)).abs() < 1e-10);
    }

    #[test]
    fn test_atan2() {
        let y = Vector::from_f64(&[0.0, 1.0, 1.0, -1.0]);
        let x = Vector::from_f64(&[1.0, 0.0, 1.0, -1.0]);
        let chunk = Chunk::from_vectors(vec![y, x]);
        let mut result = Vector::new(LogicalType::Double);

        atan2_f64(&chunk, &MockState, &mut result).unwrap();

        // atan2(0, 1) = 0
        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        // atan2(1, 0) = π/2
        assert!((result.get_f64(1).unwrap() - FRAC_PI_2).abs() < 1e-10);
        // atan2(1, 1) = π/4
        assert!((result.get_f64(2).unwrap() - FRAC_PI_4).abs() < 1e-10);
        // atan2(-1, -1) = -3π/4
        assert!((result.get_f64(3).unwrap() - (-3.0 * FRAC_PI_4)).abs() < 1e-10);
    }

    #[test]
    fn test_asin_out_of_range() {
        let chunk = create_f64_chunk(&[2.0, -2.0]);
        let mut result = Vector::new(LogicalType::Double);

        asin_f64(&chunk, &MockState, &mut result).unwrap();

        // asin(2) and asin(-2) should return NaN
        assert!(result.get_f64(0).unwrap().is_nan());
        assert!(result.get_f64(1).unwrap().is_nan());
    }
}
