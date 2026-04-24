// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Special math functions.
//!
//!
//!
//! ## Functions
//! - `pi` - Mathematical constant π
//! - `random` - Random number (volatile)
//! - `greatest` - Maximum of multiple values
//! - `least` - Minimum of multiple values

use crate::{
    ExpressionState, FunctionNullHandling, FunctionStability, ScalarFunction, ScalarFunctionSet,
};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// ============================================================================
// PI - Mathematical constant π
// ============================================================================

fn pi_impl(_input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    result.set_count(1);
    result.set_f64(0, std::f64::consts::PI);
    Ok(())
}

/// Get the `pi` function.
pub fn get_pi_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("pi".to_string());

    set.add_function(ScalarFunction::new(
        "pi".to_string(),
        vec![],
        LogicalType::Double,
        pi_impl,
    ));

    set
}

// ============================================================================
// RANDOM - Random number between 0 and 1
// ============================================================================

fn random_impl(_input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Simple LCG random number generator
    // In production, this should use a proper RNG per-thread
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(12345);

    // Simple hash to get a pseudo-random value
    let random_val = ((seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407)) as f64)
        / (u64::MAX as f64);

    result.set_count(1);
    result.set_f64(0, random_val);
    Ok(())
}

/// Get the `random` function.
pub fn get_random_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("random".to_string());

    set.add_function(
        ScalarFunction::new(
            "random".to_string(),
            vec![],
            LogicalType::Double,
            random_impl,
        )
        .with_stability(FunctionStability::Volatile),
    );

    set
}

// ============================================================================
// GREATEST - Maximum of multiple values
// ============================================================================

fn execute_extremum<T, Read, Write, Combine>(
    input: &Chunk,
    result: &mut Vector,
    mut read: Read,
    mut write: Write,
    combine: Combine,
) -> Result<()>
where
    T: Copy,
    Read: FnMut(&Vector, usize) -> Option<T>,
    Write: FnMut(&mut Vector, usize, T),
    Combine: Fn(Option<T>, T) -> T,
{
    let count = input.size();
    result.set_count(count);

    for row in 0..count {
        let mut best = None;

        for column in &input.data {
            if let Some(value) = read(column, row) {
                best = Some(combine(best, value));
            }
        }

        if let Some(value) = best {
            write(result, row, value);
        } else {
            result.validity_mut().set_null(row);
        }
    }

    Ok(())
}

fn greatest_i32(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_i32(row),
        |result, row, value| result.set_i32(row, value),
        |current, value| current.map_or(value, |best| best.max(value)),
    )
}

fn greatest_i64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_i64(row),
        |result, row, value| result.set_i64(row, value),
        |current, value| current.map_or(value, |best| best.max(value)),
    )
}

fn greatest_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_f64(row),
        |result, row, value| result.set_f64(row, value),
        |current, value| match current {
            Some(best) if value.is_nan() => best,
            Some(best) if best.is_nan() => value,
            Some(best) => best.max(value),
            None => value,
        },
    )
}

/// Get the `greatest` function set.
pub fn get_greatest_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("greatest".to_string());

    // greatest(INTEGER...) -> INTEGER
    set.add_function(
        ScalarFunction::new(
            "greatest".to_string(),
            vec![],
            LogicalType::Integer,
            greatest_i32,
        )
        .with_varargs(LogicalType::Integer)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // greatest(BIGINT...) -> BIGINT
    set.add_function(
        ScalarFunction::new(
            "greatest".to_string(),
            vec![],
            LogicalType::BigInt,
            greatest_i64,
        )
        .with_varargs(LogicalType::BigInt)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // greatest(DOUBLE...) -> DOUBLE
    set.add_function(
        ScalarFunction::new(
            "greatest".to_string(),
            vec![],
            LogicalType::Double,
            greatest_f64,
        )
        .with_varargs(LogicalType::Double)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

// ============================================================================
// LEAST - Minimum of multiple values
// ============================================================================

fn least_i32(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_i32(row),
        |result, row, value| result.set_i32(row, value),
        |current, value| current.map_or(value, |best| best.min(value)),
    )
}

fn least_i64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_i64(row),
        |result, row, value| result.set_i64(row, value),
        |current, value| current.map_or(value, |best| best.min(value)),
    )
}

fn least_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_extremum(
        input,
        result,
        |vector, row| vector.get_f64(row),
        |result, row, value| result.set_f64(row, value),
        |current, value| match current {
            Some(best) if value.is_nan() => best,
            Some(best) if best.is_nan() => value,
            Some(best) => best.min(value),
            None => value,
        },
    )
}

/// Get the `least` function set.
pub fn get_least_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("least".to_string());

    // least(INTEGER...) -> INTEGER
    set.add_function(
        ScalarFunction::new("least".to_string(), vec![], LogicalType::Integer, least_i32)
            .with_varargs(LogicalType::Integer)
            .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // least(BIGINT...) -> BIGINT
    set.add_function(
        ScalarFunction::new("least".to_string(), vec![], LogicalType::BigInt, least_i64)
            .with_varargs(LogicalType::BigInt)
            .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // least(DOUBLE...) -> DOUBLE
    set.add_function(
        ScalarFunction::new("least".to_string(), vec![], LogicalType::Double, least_f64)
            .with_varargs(LogicalType::Double)
            .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

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

    #[test]
    fn test_pi() {
        let chunk = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        pi_impl(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - std::f64::consts::PI).abs() < 1e-15);
    }

    #[test]
    fn test_random() {
        let chunk = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        random_impl(&chunk, &MockState, &mut result).unwrap();

        let val = result.get_f64(0).unwrap();
        assert!(val >= 0.0 && val <= 1.0);
    }

    #[test]
    fn test_random_is_volatile() {
        let set = get_random_function();
        assert_eq!(set.functions[0].stability, FunctionStability::Volatile);
    }

    #[test]
    fn test_greatest_i32() {
        let v1 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 5, 3],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[4, 2, 6],
            paro_common::test_utils::test_allocator(),
        );
        let v3 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[2, 8, 1],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2, v3]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        greatest_i32(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(4));
        assert_eq!(result.get_i32(1), Some(8));
        assert_eq!(result.get_i32(2), Some(6));
    }

    #[test]
    fn test_greatest_f64() {
        let v1 = paro_common::test_utils::test_f64_vector_with_allocator(
            &[1.5, 5.5, 3.5],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_f64_vector_with_allocator(
            &[4.5, 2.5, 6.5],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        greatest_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 4.5).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 5.5).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 6.5).abs() < 1e-10);
    }

    #[test]
    fn test_greatest_with_null() {
        let mut v1 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 5, 3],
            paro_common::test_utils::test_allocator(),
        );
        v1.validity_mut().set_null(0);
        let v2 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[4, 2, 6],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        greatest_i32(&chunk, &MockState, &mut result).unwrap();

        // Row 0: NULL vs 4 -> 4
        assert_eq!(result.get_i32(0), Some(4));
        assert_eq!(result.get_i32(1), Some(5));
        assert_eq!(result.get_i32(2), Some(6));
    }

    #[test]
    fn test_least_i32() {
        let v1 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 5, 3],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[4, 2, 6],
            paro_common::test_utils::test_allocator(),
        );
        let v3 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[2, 8, 1],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2, v3]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        least_i32(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(1));
    }

    #[test]
    fn test_least_f64() {
        let v1 = paro_common::test_utils::test_f64_vector_with_allocator(
            &[1.5, 5.5, 3.5],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_f64_vector_with_allocator(
            &[4.5, 2.5, 6.5],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        least_f64(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 1.5).abs() < 1e-10);
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_least_with_null() {
        let mut v1 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 5, 3],
            paro_common::test_utils::test_allocator(),
        );
        v1.validity_mut().set_null(0);
        let v2 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[4, 2, 6],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        least_i32(&chunk, &MockState, &mut result).unwrap();

        // Row 0: NULL vs 4 -> 4
        assert_eq!(result.get_i32(0), Some(4));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_greatest_all_null() {
        let mut v1 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1],
            paro_common::test_utils::test_allocator(),
        );
        let mut v2 = paro_common::test_utils::test_i32_vector_with_allocator(
            &[2],
            paro_common::test_utils::test_allocator(),
        );
        v1.validity_mut().set_null(0);
        v2.validity_mut().set_null(0);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        greatest_i32(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0));
    }

    #[test]
    fn test_greatest_i64() {
        let v1 = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1_000_000_000_000i64, 5],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_i64_vector_with_allocator(
            &[4, 2_000_000_000_000i64],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        greatest_i64(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(1_000_000_000_000i64));
        assert_eq!(result.get_i64(1), Some(2_000_000_000_000i64));
    }

    #[test]
    fn test_least_i64() {
        let v1 = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1_000_000_000_000i64, 5],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_i64_vector_with_allocator(
            &[4, 2_000_000_000_000i64],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        least_i64(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(4));
        assert_eq!(result.get_i64(1), Some(5));
    }
}
