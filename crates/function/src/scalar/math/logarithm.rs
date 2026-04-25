// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logarithm functions.
//!
//!
//!
//! ## Functions
//! - `ln` - Natural logarithm (base e)
//! - `log`, `log10` - Base-10 logarithm
//! - `log2` - Base-2 logarithm

use crate::scalar::executor::unary::UnaryExecutor;
use crate::scalar::executor::UnaryOperator;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// ============================================================================
// LN - Natural logarithm (base e)
// ============================================================================

struct LnOpF64;
impl UnaryOperator<f64, f64> for LnOpF64 {
    fn operation(input: f64) -> f64 {
        input.ln()
    }
}

fn ln_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, LnOpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `ln` function set.
pub fn get_ln_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ln".to_string());

    set.add_function(ScalarFunction::new(
        "ln".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        ln_f64,
    ));

    set
}

// ============================================================================
// LOG / LOG10 - Base-10 logarithm
// ============================================================================

struct Log10OpF64;
impl UnaryOperator<f64, f64> for Log10OpF64 {
    fn operation(input: f64) -> f64 {
        input.log10()
    }
}

fn log10_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, Log10OpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Logarithm with custom base: log(base, value)
fn log_base(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use crate::scalar::executor::binary::BinaryExecutor;
    use crate::scalar::executor::BinaryOperator;

    struct LogBaseOp;
    impl BinaryOperator<f64, f64, f64> for LogBaseOp {
        fn operation(base: f64, value: f64) -> f64 {
            value.log(base)
        }
    }

    BinaryExecutor::execute::<f64, f64, f64, LogBaseOp>(
        &input.data[0],
        &input.data[1],
        result,
        input.size(),
    )?;
    Ok(())
}

/// Get the `log` function set.
pub fn get_log_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("log".to_string());

    // log(DOUBLE) -> DOUBLE (base 10)
    set.add_function(ScalarFunction::new(
        "log".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        log10_f64,
    ));

    // log(base, value) -> DOUBLE
    set.add_function(ScalarFunction::new(
        "log".to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        log_base,
    ));

    set
}

/// Get the `log10` function set.
pub fn get_log10_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("log10".to_string());

    set.add_function(ScalarFunction::new(
        "log10".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        log10_f64,
    ));

    set
}

// ============================================================================
// LOG2 - Base-2 logarithm
// ============================================================================

struct Log2OpF64;
impl UnaryOperator<f64, f64> for Log2OpF64 {
    fn operation(input: f64) -> f64 {
        input.log2()
    }
}

fn log2_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    UnaryExecutor::execute::<f64, f64, Log2OpF64>(&input.data[0], result, input.size())?;
    Ok(())
}

/// Get the `log2` function set.
pub fn get_log2_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("log2".to_string());

    set.add_function(ScalarFunction::new(
        "log2".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        log2_f64,
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

    #[test]
    fn test_ln() {
        let chunk = create_f64_chunk(&[1.0, std::f64::consts::E, std::f64::consts::E.powi(2)]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        ln_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_log10() {
        let chunk = create_f64_chunk(&[1.0, 10.0, 100.0, 1000.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        log10_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_log2() {
        let chunk = create_f64_chunk(&[1.0, 2.0, 4.0, 8.0, 16.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        log2_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 1.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 2.0).abs() < 1e-10);
        assert!((result.get_f64(3).unwrap() - 3.0).abs() < 1e-10);
        assert!((result.get_f64(4).unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_log_base() {
        let base = paro_common::test_utils::test_f64_vector_with_allocator(
            &[2.0, 10.0, std::f64::consts::E],
            paro_common::test_utils::test_allocator(),
        );
        let value = paro_common::test_utils::test_f64_vector_with_allocator(
            &[8.0, 1000.0, std::f64::consts::E.powi(3)],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![base, value]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        log_base(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 3.0).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 3.0).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_ln_special_values() {
        let chunk = create_f64_chunk(&[0.0, -1.0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        ln_f64(&chunk, &MockState, &mut result).unwrap();

        // ln(0) = -infinity
        assert!(result.get_f64(0).unwrap().is_infinite());
        assert!(result.get_f64(0).unwrap() < 0.0);

        // ln(-1) = NaN
        assert!(result.get_f64(1).unwrap().is_nan());
    }
}
