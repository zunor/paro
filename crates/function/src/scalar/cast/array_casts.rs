// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Array Cast Functions
//!
//!
//!
//! ## Overview
//! This module implements cast functions for Array types, including:
//! - VARCHAR -> Array (for pgvector-style '[1,2,3]' literals)
//! - Array -> VARCHAR
//! - Array -> Array (child type conversion)
//!
//! ## pgvector Compatibility
//! Supports pgvector-style vector literals: `'[1.0, 2.0, 3.0]'`
//!
//! - `VectorStringToArray::StringToNestedTypeCastLoop` → `varchar_to_array_cast`
//! - `ArrayToVarcharCast` → `array_to_varchar_cast`
//! - `ArrayBoundCastData` → `ArrayBoundCastData`

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::{ArrayType, LogicalType};
use paro_common::vector::{ArrayVector, ArrayView, Vector};

use super::{BindCastInput, BoundCastData, BoundCastInfo, CastExecCtx};
use crate::scalar::executor::varlen::VarcharResultWriter;

// ============================================================================
// ============================================================================

/// Bound cast data for Array casts.
///
/// Stores the child cast function for element-wise conversion.
///
#[derive(Debug)]
pub struct ArrayBoundCastData {
    /// Cast function for child elements
    pub child_cast_info: BoundCastInfo,
}

impl ArrayBoundCastData {
    /// Create new ArrayBoundCastData with the given child cast.
    pub fn new(child_cast_info: BoundCastInfo) -> Self {
        Self { child_cast_info }
    }

    /// Bind an Array to Array cast.
    ///
    pub fn bind_array_to_array_cast(
        input: &BindCastInput,
        source: &LogicalType,
        target: &LogicalType,
    ) -> Result<Box<dyn BoundCastData>> {
        let source_child = ArrayType::get_child_type(source);
        let target_child = ArrayType::get_child_type(target);
        let child_cast = input.get_cast_function(source_child, target_child)?;
        Ok(Box::new(Self::new(child_cast)))
    }
}

impl BoundCastData for ArrayBoundCastData {
    fn copy(&self) -> Box<dyn BoundCastData> {
        Box::new(Self {
            child_cast_info: self.child_cast_info.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn copy_array_parent_validity(array: &ArrayView<'_>, result: &mut Vector, count: usize) {
    result.set_count(count);
    for row in 0..count {
        result.set_null(row, !array.is_valid(row));
    }
}

fn materialize_array_elements(source: &Vector, count: usize) -> Result<(ArrayView<'_>, Vector)> {
    let array = source.try_to_array_view(count)?;
    let child = ArrayVector::get_entry(source);
    let child_count = count
        .checked_mul(array.array_size())
        .ok_or_else(|| paro_error::internal("ARRAY child count overflow"))?;
    let mut materialized = Vector::try_new(
        child.logical_type().clone(),
        child_count.max(1),
        source.allocator().clone(),
    )?;
    materialized.set_count(child_count);

    for row in 0..count {
        for offset in 0..array.array_size() {
            let output_idx = row * array.array_size() + offset;
            if !array.is_valid(row) || !array.child_is_valid(row, offset) {
                materialized.set_null(output_idx, true);
                continue;
            }
            let physical_idx = array.physical_child_index(row, offset);
            materialized.copy_at(output_idx, child, physical_idx);
        }
    }

    Ok((array, materialized))
}

fn append_array_literal_value(out: &mut String, value: &Value) {
    match value {
        Value::Float(v) => {
            if v.fract() == 0.0 {
                out.push_str(&format!("{}", *v as i64));
            } else {
                out.push_str(&format!("{v}"));
            }
        }
        Value::Double(v) => {
            if v.fract() == 0.0 {
                out.push_str(&format!("{}", *v as i64));
            } else {
                out.push_str(&format!("{v}"));
            }
        }
        Value::Integer(v) => out.push_str(&format!("{v}")),
        Value::BigInt(v) => out.push_str(&format!("{v}")),
        Value::Null(_) => out.push_str("NULL"),
        other => out.push_str(&format!("{other}")),
    }
}

// ============================================================================
// VARCHAR -> Array Cast (pgvector-style '[1,2,3]' literals)
// ============================================================================

/// Parse a pgvector-style vector literal string into a Vec of f32 values.
///
/// Supports formats:
/// - `[1,2,3]`
/// - `[1.0, 2.0, 3.0]`
/// - `[ 1 , 2 , 3 ]` (with whitespace)
///
/// # Arguments
/// * `s` - The string to parse (e.g., "[1.0, 2.0, 3.0]")
///
/// # Returns
/// * `Ok(Vec<f32>)` - The parsed vector elements
/// * `Err` - If parsing fails
fn parse_vector_literal(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();

    // Check for brackets
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(paro_error::invalid_value(
            "VECTOR",
            format!("Vector literal must be enclosed in brackets: {}", s),
        ));
    }

    // Remove brackets
    let inner = &trimmed[1..trimmed.len() - 1];

    // Handle empty vector
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    // Split by comma and parse each element
    let mut values = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(paro_error::invalid_value(
                "VECTOR",
                format!("Empty element in vector literal: {}", s),
            ));
        }

        match part.parse::<f32>() {
            Ok(val) => {
                // pgvector requires finite values
                if !val.is_finite() {
                    return Err(paro_error::invalid_value(
                        "VECTOR",
                        format!("Vector elements must be finite numbers, got: {}", part),
                    ));
                }
                values.push(val);
            }
            Err(_) => {
                return Err(paro_error::invalid_value(
                    "VECTOR",
                    format!("Invalid number in vector literal: {}", part),
                ));
            }
        }
    }

    Ok(values)
}

/// Cast VARCHAR to Array (Vector) type.
///
/// Parses pgvector-style literals like '[1.0, 2.0, 3.0]' into Array values.
///
///
/// # Arguments
/// * `source` - Source VARCHAR vector
/// * `result` - Target Array vector
/// * `count` - Number of rows to cast
/// * `params` - Cast parameters (try_cast, cast_data)
///
/// # Returns
/// * `Ok(true)` - All values cast successfully
/// * `Ok(false)` - Some values failed (nullified if try_cast)
/// * `Err` - Cast error (if not try_cast)
pub fn varchar_to_array_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (target_child_type, target_size) = match result.logical_type() {
        LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
        _ => {
            return Err(paro_error::internal(
                "varchar_to_array_cast: result is not Array type",
            ));
        }
    };

    let child_count = count
        .checked_mul(target_size)
        .ok_or_else(|| paro_error::internal("ARRAY child count overflow"))?;
    let source_view = source.try_to_varlen_view(count)?;
    let mut parsed_child = Vector::try_new(
        LogicalType::Float,
        child_count.max(1),
        result.allocator().clone(),
    )?;
    parsed_child.set_count(child_count);
    result.set_count(count);
    let mut all_success = true;

    for row in 0..count {
        let base = row * target_size;

        if !source_view.is_valid(row) {
            for offset in 0..target_size {
                parsed_child.set_null(base + offset, true);
            }
            result.set_null(row, true);
            continue;
        }

        let s = source_view.get_inline_string(row);
        let s = s.as_str();

        match parse_vector_literal(&s) {
            Ok(values) => {
                if values.len() != target_size {
                    let error_msg = format!(
                        "Vector dimension mismatch: expected {}, got {} in '{}'",
                        target_size,
                        values.len(),
                        s
                    );

                    if ctx.try_cast {
                        for offset in 0..target_size {
                            parsed_child.set_null(base + offset, true);
                        }
                        result.set_null(row, true);
                        all_success = false;
                    } else {
                        return Err(paro_error::invalid_value("VECTOR", error_msg));
                    }
                    continue;
                }

                result.set_null(row, false);
                for (offset, value) in values.into_iter().enumerate() {
                    parsed_child.set_f32(base + offset, value);
                }
            }
            Err(e) => {
                if ctx.try_cast {
                    for offset in 0..target_size {
                        parsed_child.set_null(base + offset, true);
                    }
                    result.set_null(row, true);
                    all_success = false;
                } else {
                    return Err(e);
                }
            }
        }
    }

    if child_count == 0 {
        return Ok(all_success);
    }

    let target_child = ArrayVector::get_entry_mut(result);
    if let Some(cast_data) = ctx.cast_data {
        let array_cast_data = cast_data
            .as_any()
            .downcast_ref::<ArrayBoundCastData>()
            .ok_or_else(|| paro_error::internal("varchar_to_array_cast: invalid cast_data"))?;
        let child_ctx = CastExecCtx {
            runtime: ctx.runtime,
            try_cast: ctx.try_cast,
            cast_data: array_cast_data
                .child_cast_info
                .cast_data
                .as_ref()
                .map(|data| data.as_ref()),
        };
        let child_success = array_cast_data.child_cast_info.execute(
            &parsed_child,
            target_child,
            child_count,
            &child_ctx,
        )?;
        Ok(all_success && child_success)
    } else if target_child_type == LogicalType::Float {
        *target_child = parsed_child;
        Ok(all_success)
    } else {
        Err(paro_error::internal(
            "varchar_to_array_cast: missing child cast metadata",
        ))
    }
}

// ============================================================================
// Array -> VARCHAR Cast
// ============================================================================

/// Cast Array to VARCHAR.
///
/// Converts Array values to pgvector-style string format: '[1.0, 2.0, 3.0]'
///
pub fn array_to_varchar_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let array = source.try_to_array_view(count)?;
    let child = ArrayVector::get_entry(source);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !array.is_valid(row) {
            writer.set_null(row);
            continue;
        }

        let mut output = String::with_capacity(array.array_size() * 8 + 2);
        output.push('[');
        for offset in 0..array.array_size() {
            if offset > 0 {
                output.push_str(", ");
            }
            let value = if array.child_is_valid(row, offset) {
                let physical_idx = array.physical_child_index(row, offset);
                child.get_value(physical_idx)
            } else {
                Value::Null(child.logical_type().clone())
            };
            append_array_literal_value(&mut output, &value);
        }
        output.push(']');
        writer.write_str(row, &output);
    }

    Ok(true)
}

// ============================================================================
// Array -> Array Cast (child type conversion)
// ============================================================================

/// Cast Array to Array with different child types.
///
pub fn array_to_array_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let source_size = match source.logical_type() {
        LogicalType::Array(_, size) => *size,
        _ => return Err(paro_error::internal("source is not Array type")),
    };

    let target_size = match result.logical_type() {
        LogicalType::Array(_, size) => *size,
        _ => return Err(paro_error::internal("result is not Array type")),
    };

    // Check size compatibility
    if source_size != target_size {
        let msg = format!(
            "Cannot cast array of size {} to array of size {}",
            source_size, target_size
        );

        if ctx.try_cast {
            // Set all results to NULL
            result.set_count(count);
            for i in 0..count {
                result.set_null(i, true);
            }
            return Ok(false);
        } else {
            return Err(paro_error::invalid_value("ARRAY", msg));
        }
    }

    // Get child cast function from cast_data
    let cast_data = ctx
        .cast_data
        .ok_or_else(|| paro_error::internal("array_to_array_cast: missing cast_data"))?;

    let array_cast_data = cast_data
        .as_any()
        .downcast_ref::<ArrayBoundCastData>()
        .ok_or_else(|| paro_error::internal("array_to_array_cast: invalid cast_data type"))?;

    let child_count = count
        .checked_mul(source_size)
        .ok_or_else(|| paro_error::internal("ARRAY child count overflow"))?;
    let (array, materialized_child) = materialize_array_elements(source, count)?;
    let result_child = ArrayVector::get_entry_mut(result);
    let child_ctx = CastExecCtx {
        runtime: ctx.runtime,
        try_cast: ctx.try_cast,
        cast_data: array_cast_data
            .child_cast_info
            .cast_data
            .as_ref()
            .map(|d| d.as_ref()),
    };

    let child_success = array_cast_data.child_cast_info.execute(
        &materialized_child,
        result_child,
        child_count,
        &child_ctx,
    )?;

    copy_array_parent_validity(&array, result, count);

    Ok(child_success)
}

// ============================================================================
// Array -> List Cast (fixed-size array to variable-length list)
// ============================================================================

/// Cast Array to List, preserving element order.
pub fn array_to_list_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let source_size = match source.logical_type() {
        LogicalType::Array(_, size) => *size,
        _ => {
            return Err(paro_error::internal(
                "array_to_list_cast: source is not Array type",
            ))
        }
    };

    let target_child = match result.logical_type() {
        LogicalType::List(child) => child.as_ref().clone(),
        _ => {
            return Err(paro_error::internal(
                "array_to_list_cast: result is not List type",
            ))
        }
    };

    let cast_data = ctx
        .cast_data
        .ok_or_else(|| paro_error::internal("array_to_list_cast: missing cast_data"))?;

    let array_cast_data = cast_data
        .as_any()
        .downcast_ref::<ArrayBoundCastData>()
        .ok_or_else(|| paro_error::internal("array_to_list_cast: invalid cast_data type"))?;

    let child_count = count
        .checked_mul(source_size)
        .ok_or_else(|| paro_error::internal("ARRAY child count overflow"))?;
    let (array, materialized_child) = materialize_array_elements(source, count)?;
    let allocator = result.allocator().clone();
    let mut result_child = Vector::try_new(target_child, child_count.max(1), allocator)?;
    result_child.set_count(child_count);

    let child_ctx = CastExecCtx {
        runtime: ctx.runtime,
        try_cast: ctx.try_cast,
        cast_data: array_cast_data
            .child_cast_info
            .cast_data
            .as_ref()
            .map(|d| d.as_ref()),
    };

    let child_success = array_cast_data.child_cast_info.execute(
        &materialized_child,
        &mut result_child,
        child_count,
        &child_ctx,
    )?;

    result.set_child(Arc::new(result_child));
    result.set_count(count);

    let entries = unsafe { result.flat_data_mut::<u8>() };
    for i in 0..count {
        let entry_ptr = unsafe { entries.add(i * 8) as *mut u32 };
        if !array.is_valid(i) {
            unsafe {
                std::ptr::write_unaligned(entry_ptr, 0);
                std::ptr::write_unaligned(entry_ptr.add(1), 0);
            }
            result.set_null(i, true);
        } else {
            unsafe {
                std::ptr::write_unaligned(entry_ptr, (i * source_size) as u32);
                std::ptr::write_unaligned(entry_ptr.add(1), source_size as u32);
            }
            result.set_null(i, false);
        }
    }

    Ok(child_success)
}

// ============================================================================
// Bind Functions
// ============================================================================

/// Bind function for Array casts.
///
/// This is registered as a dynamic bind function to handle Array types
/// with different child types and sizes.
///
pub fn bind_array_casts(
    input: &BindCastInput,
    source: &LogicalType,
    target: &LogicalType,
) -> Result<Option<BoundCastInfo>> {
    if matches!(source, LogicalType::Varchar | LogicalType::StringLiteral) {
        if let LogicalType::Array(_, _) = target {
            let target_child = ArrayType::get_child_type(target);
            let child_cast = input.get_cast_function(&LogicalType::Float, target_child)?;
            let cast_data = ArrayBoundCastData::new(child_cast);
            return Ok(Some(BoundCastInfo::array_with_data(
                varchar_to_array_cast,
                Arc::new(cast_data),
            )));
        }
    }

    if let LogicalType::Array(_, _) = source {
        if matches!(target, LogicalType::Varchar) {
            return Ok(Some(BoundCastInfo::array(array_to_varchar_cast)));
        }
    }

    if let LogicalType::Array(_, _) = source {
        if let LogicalType::Array(_, _) = target {
            let cast_data = ArrayBoundCastData::bind_array_to_array_cast(input, source, target)?;
            return Ok(Some(BoundCastInfo::array_with_data(
                array_to_array_cast,
                Arc::from(cast_data),
            )));
        }
    }

    if let LogicalType::Array(_, _) = source {
        if let LogicalType::List(_) = target {
            let source_child = ArrayType::get_child_type(source);
            let target_child = match target {
                LogicalType::List(child) => child.as_ref(),
                _ => unreachable!(),
            };
            let child_cast = input.get_cast_function(source_child, target_child)?;
            let cast_data = ArrayBoundCastData::new(child_cast);
            return Ok(Some(BoundCastInfo::array_with_data(
                array_to_list_cast,
                Arc::new(cast_data),
            )));
        }
    }

    Ok(None)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::FunctionExecContext;

    #[derive(Debug)]
    struct NoopRuntime;

    impl FunctionExecContext for NoopRuntime {
        fn current_database(&self) -> Option<&str> {
            None
        }

        fn current_schema(&self) -> Option<&str> {
            None
        }

        fn current_user(&self) -> Option<&str> {
            None
        }
    }

    static NOOP_RUNTIME: NoopRuntime = NoopRuntime;

    fn test_ctx(try_cast: bool) -> CastExecCtx<'static> {
        CastExecCtx {
            runtime: &NOOP_RUNTIME,
            try_cast,
            cast_data: None,
        }
    }

    #[test]
    fn test_parse_vector_literal_basic() {
        let result = parse_vector_literal("[1, 2, 3]").unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_vector_literal_floats() {
        let result = parse_vector_literal("[1.5, 2.5, 3.5]").unwrap();
        assert_eq!(result, vec![1.5, 2.5, 3.5]);
    }

    #[test]
    fn test_parse_vector_literal_whitespace() {
        let result = parse_vector_literal("[ 1 , 2 , 3 ]").unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_vector_literal_negative() {
        let result = parse_vector_literal("[-1, -2.5, 3]").unwrap();
        assert_eq!(result, vec![-1.0, -2.5, 3.0]);
    }

    #[test]
    fn test_parse_vector_literal_empty() {
        let result = parse_vector_literal("[]").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_vector_literal_invalid_no_brackets() {
        let result = parse_vector_literal("1, 2, 3");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vector_literal_invalid_not_number() {
        let result = parse_vector_literal("[1, abc, 3]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vector_literal_infinity() {
        let result = parse_vector_literal("[inf, 2, 3]");
        assert!(result.is_err());
    }

    #[test]
    fn test_varchar_to_array_cast() {
        let source = paro_common::test_utils::test_string_vector_with_allocator(
            &["[1, 2, 3]", "[4, 5, 6]"],
            paro_common::test_utils::test_allocator(),
        );
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3);
        let mut result = paro_common::test_utils::test_new_array_with_allocator(
            array_type,
            2,
            paro_common::test_utils::test_allocator(),
        );
        result.set_count(2);

        let ctx = test_ctx(false);
        let success = varchar_to_array_cast(&source, &mut result, 2, &ctx).unwrap();

        assert!(success);
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));

        // Verify first array
        let val0 = result.get_value(0);
        match val0 {
            Value::Array(elements, _, size) => {
                assert_eq!(size, 3);
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Float(1.0));
                assert_eq!(elements[1], Value::Float(2.0));
                assert_eq!(elements[2], Value::Float(3.0));
            }
            _ => panic!("Expected Array value"),
        }
    }

    #[test]
    fn test_varchar_to_array_cast_size_mismatch() {
        let source = paro_common::test_utils::test_string_vector_with_allocator(
            &["[1, 2]"],
            paro_common::test_utils::test_allocator(),
        ); // 2 elements
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3); // expects 3
        let mut result = paro_common::test_utils::test_new_array_with_allocator(
            array_type,
            1,
            paro_common::test_utils::test_allocator(),
        );
        result.set_count(1);

        // Non-try_cast should fail
        let ctx = test_ctx(false);
        let err = varchar_to_array_cast(&source, &mut result, 1, &ctx);
        assert!(err.is_err());

        // try_cast should succeed with NULL
        let ctx = test_ctx(true);
        let mut result = paro_common::test_utils::test_new_array_with_allocator(
            LogicalType::Array(Box::new(LogicalType::Float), 3),
            1,
            paro_common::test_utils::test_allocator(),
        );
        result.set_count(1);
        let success = varchar_to_array_cast(&source, &mut result, 1, &ctx).unwrap();
        assert!(!success);
        assert!(result.is_null(0));
    }

    #[test]
    fn test_array_to_varchar_cast() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3);
        let mut source = paro_common::test_utils::test_new_array_with_allocator(
            array_type,
            2,
            paro_common::test_utils::test_allocator(),
        );
        source.set_count(2);

        // Set array values
        let val1 = Value::Array(
            vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
            LogicalType::Float,
            3,
        );
        let val2 = Value::Array(
            vec![Value::Float(4.5), Value::Float(5.5), Value::Float(6.5)],
            LogicalType::Float,
            3,
        );
        source.set_value(0, &val1);
        source.set_value(1, &val2);

        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);
        result.set_count(2);
        let ctx = test_ctx(false);
        let success = array_to_varchar_cast(&source, &mut result, 2, &ctx).unwrap();

        assert!(success);
        assert_eq!(result.get_string(0), Some("[1, 2, 3]"));
        assert_eq!(result.get_string(1), Some("[4.5, 5.5, 6.5]"));
    }

    #[test]
    fn test_varchar_to_array_cast_with_capacity() {
        // Match the generic allocation path used by execute_cast.
        let source = paro_common::test_utils::test_string_vector_with_allocator(
            &["[1, 2, 3]", "[4, 5, 6]"],
            paro_common::test_utils::test_allocator(),
        );
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3);

        // Use the generic vector allocation path like execute_cast does.
        let mut result = paro_common::test_utils::test_vector_with_capacity(array_type.clone(), 2);
        result.set_len(2);

        // Verify child vector exists
        assert!(
            result.child().is_some(),
            "Result vector should have child for Array type"
        );

        let ctx = test_ctx(false);
        let success = varchar_to_array_cast(&source, &mut result, 2, &ctx).unwrap();

        assert!(success);
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));

        // Verify first array
        let val0 = result.get_value(0);
        match val0 {
            Value::Array(elements, _, size) => {
                assert_eq!(size, 3);
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Float(1.0));
                assert_eq!(elements[1], Value::Float(2.0));
                assert_eq!(elements[2], Value::Float(3.0));
            }
            _ => panic!("Expected Array value, got {:?}", val0),
        }

        // Verify second array
        let val1 = result.get_value(1);
        match val1 {
            Value::Array(elements, _, size) => {
                assert_eq!(size, 3);
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Float(4.0));
                assert_eq!(elements[1], Value::Float(5.0));
                assert_eq!(elements[2], Value::Float(6.0));
            }
            _ => panic!("Expected Array value, got {:?}", val1),
        }
    }
}
