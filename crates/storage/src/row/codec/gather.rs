// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed column gather from execution-time row buffers.

use std::ptr;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::{LogicalType, StringView};
use paro_common::vector::{Vector, VectorType};

use crate::row::codec::{unsafe_api, ColumnCodec};
use crate::row::RowLayout;

/// Gather one logical column from arbitrary execution-row pointers.
///
/// Fixed-width values are copied directly into the target vector and varlen
/// values are copied directly into its string heap. Nested values retain the
/// generic value path until their codecs gain a native gather implementation.
/// A null pointer represents an absent row and produces SQL NULL, which lets
/// outer and single joins share the same gather path.
///
/// # Safety
///
/// Every non-null pointer returned by `row_at` must reference a live row encoded
/// with `layout` for the duration of this call.
pub unsafe fn gather_column_from_rows<F>(
    layout: &RowLayout,
    column_idx: usize,
    output: &mut Vector,
    count: usize,
    mut row_at: F,
) -> Result<()>
where
    F: FnMut(usize) -> *const u8,
{
    let expected_type = layout.types().get(column_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "row gather column {column_idx} out of range {}",
            layout.column_count()
        ))
    })?;
    if output.logical_type() != expected_type {
        return Err(paro_error::internal(format!(
            "row gather type mismatch for column {column_idx}: expected={expected_type:?}, actual={:?}",
            output.logical_type()
        )));
    }

    prepare_output(output, expected_type, count)?;
    let codec = layout.codecs().get(column_idx).ok_or_else(|| {
        paro_error::internal(format!("missing row codec for column {column_idx}"))
    })?;
    match codec {
        ColumnCodec::Fixed { size } => {
            // SAFETY: upheld by this function's caller; output type equality
            // guarantees that its physical width matches the row codec.
            unsafe { gather_fixed(layout, column_idx, *size, output, count, &mut row_at) }
        }
        ColumnCodec::Varlen(_) => {
            // SAFETY: upheld by this function's caller; varlen row cells use
            // the RowLayout inline-or-heap representation.
            unsafe { gather_varlen(layout, column_idx, output, count, &mut row_at) }
        }
        ColumnCodec::List(_) | ColumnCodec::Array(_) | ColumnCodec::Struct(_) => {
            // SAFETY: upheld by this function's caller. The generic fallback
            // clones owned nested values before writing them to the vector.
            unsafe { gather_nested(layout, column_idx, output, count, &mut row_at) }
        }
    }
}

fn prepare_output(output: &mut Vector, expected_type: &LogicalType, count: usize) -> Result<()> {
    if output.vector_type() != VectorType::Flat || output.logical_capacity() < count {
        let allocator = output.allocator().clone();
        *output = Vector::try_new(expected_type.clone(), count.max(1), allocator)?;
    } else {
        output.try_make_exclusive()?;
    }
    output.try_set_count(count)?;
    output.validity_mut().try_set_all_valid(count)
}

#[inline(always)]
unsafe fn row_value_is_valid(layout: &RowLayout, row_ptr: *const u8, column_idx: usize) -> bool {
    !row_ptr.is_null()
        && (layout.all_valid()
            // SAFETY: the caller guarantees that a non-null pointer references
            // a row encoded with `layout`.
            || unsafe { unsafe_api::row_is_valid(row_ptr, column_idx) })
}

unsafe fn gather_fixed<F>(
    layout: &RowLayout,
    column_idx: usize,
    width: usize,
    output: &mut Vector,
    count: usize,
    row_at: &mut F,
) -> Result<()>
where
    F: FnMut(usize) -> *const u8,
{
    let offset = layout.offsets()[column_idx];
    // SAFETY: `prepare_output` guarantees a flat vector with matching type and
    // enough capacity for `count` values.
    let target = unsafe { output.flat_data_mut::<u8>() };

    for row_idx in 0..count {
        let row_ptr = row_at(row_idx);
        // SAFETY: upheld by this function's caller.
        if unsafe { row_value_is_valid(layout, row_ptr, column_idx) } {
            // SAFETY: both cells are live, non-overlapping, and exactly `width`
            // bytes wide according to the shared logical type.
            unsafe { copy_fixed_width(row_ptr.add(offset), target.add(row_idx * width), width) };
        } else {
            // Keep null payload bytes deterministic for consumers that perform
            // unconditional physical reads behind a validity check.
            unsafe { ptr::write_bytes(target.add(row_idx * width), 0, width) };
            output.validity_mut().try_set_invalid(row_idx)?;
        }
    }
    Ok(())
}

unsafe fn gather_varlen<F>(
    layout: &RowLayout,
    column_idx: usize,
    output: &mut Vector,
    count: usize,
    row_at: &mut F,
) -> Result<()>
where
    F: FnMut(usize) -> *const u8,
{
    let offset = layout.offsets()[column_idx];
    let (entries, validity, heap) = output.begin_varlen_write(count);
    validity.try_set_all_valid(count)?;

    for row_idx in 0..count {
        let row_ptr = row_at(row_idx);
        // SAFETY: upheld by this function's caller.
        if !unsafe { row_value_is_valid(layout, row_ptr, column_idx) } {
            // SAFETY: `entries` has capacity for `count` values.
            unsafe { ptr::write(entries.add(row_idx), StringView::empty()) };
            validity.try_set_invalid(row_idx)?;
            continue;
        }

        // SAFETY: the row pointer and column offset identify a live varlen cell.
        let cell = unsafe { row_ptr.add(offset) };
        // SAFETY: `cell` is a live row varlen cell and the row owner keeps any
        // referenced allocation alive for this gather operation.
        let value = unsafe { StringView::from_cell(cell) };
        if value.is_inlined() {
            unsafe { ptr::write(entries.add(row_idx), value) };
            continue;
        }

        // SAFETY: the output vector retains `heap` alongside the copied view.
        let copied = unsafe { heap.try_add_blob(value.as_bytes()) }?;
        unsafe { ptr::write(entries.add(row_idx), copied) };
    }
    Ok(())
}

/// Copy the scalar widths used by fixed SQL types without a variable-length
/// libc call for every gathered cell.
///
/// # Safety
///
/// `source` and `target` must be readable/writable for `width` non-overlapping
/// bytes.
#[inline]
unsafe fn copy_fixed_width(source: *const u8, target: *mut u8, width: usize) {
    macro_rules! copy_value {
        ($ty:ty) => {{
            let value = unsafe { ptr::read_unaligned(source.cast::<$ty>()) };
            unsafe { ptr::write_unaligned(target.cast::<$ty>(), value) };
        }};
    }
    match width {
        1 => copy_value!(u8),
        2 => copy_value!(u16),
        4 => copy_value!(u32),
        8 => copy_value!(u64),
        16 => copy_value!(u128),
        _ => unsafe { ptr::copy_nonoverlapping(source, target, width) },
    }
}

unsafe fn gather_nested<F>(
    layout: &RowLayout,
    column_idx: usize,
    output: &mut Vector,
    count: usize,
    row_at: &mut F,
) -> Result<()>
where
    F: FnMut(usize) -> *const u8,
{
    let logical_type = layout.types()[column_idx].clone();
    for row_idx in 0..count {
        let row_ptr = row_at(row_idx);
        let value = if row_ptr.is_null() {
            Value::Null(logical_type.clone())
        } else {
            // SAFETY: upheld by this function's caller.
            unsafe { unsafe_api::read_row_value(layout, row_ptr, column_idx) }
        };
        output.set_value(row_idx, &value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{ptr, sync::Arc};

    use paro_common::allocator::DefaultAllocator;
    use paro_common::error::Result;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::row::codec::{unsafe_api, RowHeapWriter};
    use crate::row::{RowLayout, RowValidityType};

    use super::gather_column_from_rows;

    #[derive(Default)]
    #[allow(clippy::vec_box)] // Row pointers must survive growth of the owning collection.
    struct TestHeap {
        bytes: Vec<Box<[u8]>>,
        values: Vec<Box<Value>>,
    }

    impl RowHeapWriter for TestHeap {
        fn store_bytes(&mut self, bytes: &[u8]) -> Result<*const u8> {
            let retained = bytes.to_vec().into_boxed_slice();
            let ptr = retained.as_ptr();
            self.bytes.push(retained);
            Ok(ptr)
        }

        fn store_value(&mut self, value: Value) -> Result<*const Value> {
            let retained = Box::new(value);
            let ptr = retained.as_ref() as *const Value;
            self.values.push(retained);
            Ok(ptr)
        }
    }

    #[test]
    fn gathers_fixed_and_varlen_columns_without_value_materialization() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let layout = RowLayout::from_types(types.clone(), RowValidityType::CanHaveNullValues);
        let row_width = layout.row_width();
        let mut rows = vec![0u8; row_width * 3];
        let mut heap = TestHeap::default();

        for row_idx in 0..3 {
            rows[row_idx * row_width] = u8::MAX;
        }
        let values = [
            [Value::Integer(7), Value::Varchar("short".to_string())],
            [
                Value::Null(LogicalType::Integer),
                Value::Varchar("a string stored outside the row".to_string()),
            ],
            [Value::Integer(-4), Value::Null(LogicalType::Varchar)],
        ];
        for (row_idx, row_values) in values.iter().enumerate() {
            let row_ptr = unsafe { rows.as_mut_ptr().add(row_idx * row_width) };
            for (column_idx, value) in row_values.iter().enumerate() {
                unsafe {
                    unsafe_api::write_row_value(&layout, row_ptr, column_idx, value, &mut heap)
                        .unwrap();
                }
            }
        }

        let allocator = Arc::new(DefaultAllocator::new());
        let mut integers = Vector::try_new(LogicalType::Integer, 4, allocator.clone()).unwrap();
        let mut strings = Vector::try_new(LogicalType::Varchar, 4, allocator).unwrap();
        let row_base = rows.as_ptr();
        let row_at = |row_idx: usize| {
            if row_idx == 3 {
                ptr::null()
            } else {
                unsafe { row_base.add(row_idx * row_width) }
            }
        };

        unsafe {
            gather_column_from_rows(&layout, 0, &mut integers, 4, row_at).unwrap();
            gather_column_from_rows(&layout, 1, &mut strings, 4, row_at).unwrap();
        }

        assert_eq!(integers.get_i32(0), Some(7));
        assert!(integers.is_null(1));
        assert_eq!(integers.get_i32(2), Some(-4));
        assert!(integers.is_null(3));
        assert_eq!(strings.get_string(0), Some("short"));
        assert_eq!(
            strings.get_string(1),
            Some("a string stored outside the row")
        );
        assert!(strings.is_null(2));
        assert!(strings.is_null(3));
    }
}
