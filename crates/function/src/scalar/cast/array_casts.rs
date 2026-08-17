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
use paro_common::vector::{ArrayVector, ArrayView, DataRef, Vector, VectorType, VectorView};

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

/// Bound child conversion for variable-length LIST casts.
#[derive(Debug)]
pub struct ListBoundCastData {
    pub child_cast_info: BoundCastInfo,
}

impl BoundCastData for ListBoundCastData {
    fn copy(&self) -> Box<dyn BoundCastData> {
        Box::new(Self {
            child_cast_info: self.child_cast_info.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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
    ) -> Result<Self> {
        let source_child = ArrayType::get_child_type(source);
        let target_child = ArrayType::get_child_type(target);
        let child_cast = input.get_cast_function(source_child, target_child)?;
        Ok(Self::new(child_cast))
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
    materialized.try_set_count(child_count)?;

    for row in 0..count {
        for offset in 0..array.array_size() {
            let output_idx = row * array.array_size() + offset;
            if !array.is_valid(row) || !array.child_is_valid(row, offset) {
                materialized.try_set_null(output_idx, true)?;
                continue;
            }
            let physical_idx = array.physical_child_index(row, offset);
            materialized.try_copy_at(output_idx, child, physical_idx)?;
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

        // SAFETY: this parser is reached only for a VARCHAR source.
        let s = unsafe { source_view.str_unchecked(row) };

        match parse_vector_literal(s) {
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
        writer.write_str(row, &output)?;
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
// List -> List Cast (variable-length element conversion)
// ============================================================================

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
            .map(AsRef::as_ref)
            .ok_or_else(|| paro_error::internal("LIST vector missing child")),
        VectorType::Sequence => Err(paro_error::internal(
            "LIST vector cannot use sequence encoding",
        )),
    }
}

fn read_list_entry(
    entries: &VectorView<'_>,
    child_len: usize,
    row: usize,
) -> Result<(usize, usize)> {
    let DataRef::Ptr(data) = entries.data() else {
        return Err(paro_error::internal(
            "LIST entries cannot use sequence encoding",
        ));
    };
    let entry = unsafe { data.add(entries.physical_index(row) * 8) as *const u32 };
    let offset = unsafe { std::ptr::read_unaligned(entry) as usize };
    let length = unsafe { std::ptr::read_unaligned(entry.add(1)) as usize };
    if offset.saturating_add(length) > child_len {
        return Err(paro_error::internal(format!(
            "Invalid LIST entry ({offset}, {length}), child length is {child_len}",
        )));
    }
    Ok((offset, length))
}

fn write_list_entry(result: &mut Vector, row: usize, offset: usize, length: usize) -> Result<()> {
    let offset = u32::try_from(offset)
        .map_err(|_| paro_error::out_of_range("LIST child offset exceeds u32"))?;
    let length = u32::try_from(length)
        .map_err(|_| paro_error::out_of_range("LIST child length exceeds u32"))?;
    let data = unsafe { result.flat_data_mut::<u8>() };
    let entry = unsafe { data.add(row * 8) as *mut u32 };
    unsafe {
        std::ptr::write_unaligned(entry, offset);
        std::ptr::write_unaligned(entry.add(1), length);
    }
    Ok(())
}

/// Cast a LIST by materializing its selected child rows once, executing the
/// bound child cast vector-wise, and rebuilding compact parent entries.
pub fn list_to_list_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (source_child_type, target_child_type) =
        match (source.logical_type(), result.logical_type()) {
            (LogicalType::List(source), LogicalType::List(target)) => {
                (source.as_ref().clone(), target.as_ref().clone())
            }
            _ => {
                return Err(paro_error::internal(
                    "list_to_list_cast requires LIST source and target",
                ))
            }
        };
    let cast_data = ctx
        .cast_data
        .and_then(|data| data.as_any().downcast_ref::<ListBoundCastData>())
        .ok_or_else(|| paro_error::internal("list_to_list_cast: missing cast data"))?;
    let entries = source.try_to_view(count)?;
    let source_child = list_child_vector(source)?;

    let mut rows = Vec::with_capacity(count);
    let mut child_count = 0usize;
    for row in 0..count {
        if !entries.is_valid(row) {
            rows.push(None);
            continue;
        }
        let (offset, length) = read_list_entry(&entries, source_child.len(), row)?;
        child_count = child_count
            .checked_add(length)
            .ok_or_else(|| paro_error::out_of_range("LIST child count overflow"))?;
        rows.push(Some((offset, length)));
    }

    let allocator = result.allocator().clone();
    let mut materialized =
        Vector::try_new(source_child_type, child_count.max(1), allocator.clone())?;
    materialized.try_set_count(child_count)?;
    let mut destination = 0usize;
    for (offset, length) in rows.iter().flatten().copied() {
        materialized.try_copy_range(destination, source_child, offset, length)?;
        destination += length;
    }

    let mut converted = Vector::try_new(target_child_type, child_count.max(1), allocator)?;
    converted.try_set_count(child_count)?;
    let child_success = if child_count == 0 {
        true
    } else {
        let child_ctx = CastExecCtx {
            runtime: ctx.runtime,
            try_cast: ctx.try_cast,
            cast_data: cast_data
                .child_cast_info
                .cast_data
                .as_ref()
                .map(AsRef::as_ref),
        };
        cast_data
            .child_cast_info
            .execute(&materialized, &mut converted, child_count, &child_ctx)?
    };

    result.set_child(Arc::new(converted));
    result.set_count(count);
    let mut output_offset = 0usize;
    for (row, entry) in rows.into_iter().enumerate() {
        match entry {
            Some((_, length)) => {
                write_list_entry(result, row, output_offset, length)?;
                result.set_null(row, false);
                output_offset += length;
            }
            None => {
                write_list_entry(result, row, output_offset, 0)?;
                result.set_null(row, true);
            }
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
            let dependency = child_cast.context_dependency();
            let cast_data = ArrayBoundCastData::new(child_cast);
            return Ok(Some(
                BoundCastInfo::array_with_data(varchar_to_array_cast, Arc::new(cast_data))
                    .with_context_dependency(dependency),
            ));
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
            let dependency = cast_data.child_cast_info.context_dependency();
            return Ok(Some(
                BoundCastInfo::array_with_data(array_to_array_cast, Arc::new(cast_data))
                    .with_context_dependency(dependency),
            ));
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
            let dependency = child_cast.context_dependency();
            let cast_data = ArrayBoundCastData::new(child_cast);
            return Ok(Some(
                BoundCastInfo::array_with_data(array_to_list_cast, Arc::new(cast_data))
                    .with_context_dependency(dependency),
            ));
        }
    }

    if let (LogicalType::List(source_child), LogicalType::List(target_child)) = (source, target) {
        let child_cast = input.get_cast_function(source_child, target_child)?;
        let dependency = child_cast.context_dependency();
        return Ok(Some(
            BoundCastInfo::array_with_data(
                list_to_list_cast,
                Arc::new(ListBoundCastData {
                    child_cast_info: child_cast,
                }),
            )
            .with_context_dependency(dependency),
        ));
    }

    Ok(None)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::cast::{decimal_casts, CastFunctionSet};
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

    #[test]
    fn list_to_list_cast_preserves_nested_boundaries_and_nulls() {
        let decimal_type = LogicalType::Decimal {
            precision: 3,
            scale: 1,
        };
        let source_type = LogicalType::List(Box::new(decimal_type.clone()));
        let target_type = LogicalType::List(Box::new(LogicalType::Double));
        let allocator = paro_common::test_utils::test_allocator();

        let mut source = Vector::try_new(source_type.clone(), 4, allocator.clone()).unwrap();
        source.try_set_count(4).unwrap();
        source.set_value(
            0,
            &Value::List(
                vec![
                    Value::Decimal(15, 3, 1),
                    Value::Null(decimal_type.clone()),
                    Value::Decimal(25, 3, 1),
                ],
                decimal_type.clone(),
            ),
        );
        source.set_null(1, true);
        source.set_value(2, &Value::List(vec![], decimal_type.clone()));
        source.set_value(
            3,
            &Value::List(vec![Value::Decimal(-20, 3, 1)], decimal_type),
        );

        let dictionary =
            paro_common::test_utils::test_dictionary(Arc::new(source), vec![3, 1, 2, 0]);
        let mut result = Vector::try_new(target_type.clone(), 4, allocator).unwrap();

        let mut casts = CastFunctionSet::new();
        casts.register_bind_function(decimal_casts::bind_decimal_casts);
        casts.register_bind_function(bind_array_casts);
        let bound = casts.get_cast_function(&source_type, &target_type).unwrap();
        let ctx = CastExecCtx {
            runtime: &NOOP_RUNTIME,
            try_cast: false,
            cast_data: bound.cast_data.as_deref(),
        };

        assert!(bound.execute(&dictionary, &mut result, 4, &ctx).unwrap());
        assert_eq!(
            result.get_value(0),
            Value::List(vec![Value::Double(-2.0)], LogicalType::Double)
        );
        assert_eq!(result.get_value(1), Value::Null(target_type));
        assert_eq!(
            result.get_value(2),
            Value::List(vec![], LogicalType::Double)
        );
        assert_eq!(
            result.get_value(3),
            Value::List(
                vec![
                    Value::Double(1.5),
                    Value::Null(LogicalType::Double),
                    Value::Double(2.5),
                ],
                LogicalType::Double,
            )
        );
    }
}
