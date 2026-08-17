// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepared scatter from columnar vectors into execution rows.

use std::mem::size_of;
use std::ptr;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::StringView;
use paro_common::vector::{DataRef, VarlenView, Vector, VectorView};

use super::{unsafe_api, ColumnCodec, RowHeapWriter};
use crate::row::RowLayout;

enum PreparedColumn<'a> {
    Fixed {
        view: VectorView<'a>,
        source: &'a Vector,
        width: usize,
    },
    Varlen {
        view: VarlenView<'a>,
    },
    Nested {
        source: &'a Vector,
    },
}

impl PreparedColumn<'_> {
    #[inline]
    fn is_valid(&self, row_idx: usize) -> bool {
        match self {
            Self::Fixed { view, .. } => view.is_valid(row_idx),
            Self::Varlen { view } => view.is_valid(row_idx),
            Self::Nested { source } => !source.is_null(row_idx),
        }
    }

    #[inline]
    fn all_valid(&self) -> bool {
        match self {
            Self::Fixed { view, .. } => view.validity().all_valid(),
            Self::Varlen { view } => view.validity().all_valid(),
            Self::Nested { source } => source.validity().all_valid(),
        }
    }
}

/// Bytes retained outside a row for one source value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowHeapUsage {
    varlen_bytes: usize,
    nested_value_bytes: usize,
}

impl RowHeapUsage {
    #[inline]
    pub fn varlen_bytes(self) -> usize {
        self.varlen_bytes
    }

    #[inline]
    pub fn nested_value_bytes(self) -> usize {
        self.nested_value_bytes
    }

    pub fn checked_add(self, other: Self) -> Result<Self> {
        Ok(Self {
            varlen_bytes: self
                .varlen_bytes
                .checked_add(other.varlen_bytes)
                .ok_or_else(|| paro_error::out_of_range("row byte heap size overflow"))?,
            nested_value_bytes: self
                .nested_value_bytes
                .checked_add(other.nested_value_bytes)
                .ok_or_else(|| paro_error::out_of_range("row value heap size overflow"))?,
        })
    }
}

/// Batch-scoped decoded vector views for scattering into [`RowLayout`] rows.
///
/// Dictionary and constant selections are composed once at construction. The
/// hot row loop then performs direct validity checks and physical copies rather
/// than recursively dispatching through `Vector` for every cell.
pub struct PreparedRowScatter<'a> {
    columns: Vec<PreparedColumn<'a>>,
    #[cfg(debug_assertions)]
    source_columns: Vec<&'a Vector>,
    heap_columns: Vec<usize>,
    all_valid: bool,
    count: usize,
}

struct PreparedFixedColumn<'a> {
    view: VectorView<'a>,
    data: *const u8,
    source_width: usize,
    target_offset: usize,
}

/// Direct scatter program for an all-valid, fixed-width vector batch.
///
/// The general prepared scatter keeps per-column variants because it also owns
/// varlen and nested semantics. Hash and aggregate builds commonly receive
/// only flat fixed-width values; compiling that shape once removes variant,
/// validity, and layout dispatch from every cell in the row loop.
pub struct PreparedFixedRowScatter<'a> {
    columns: Vec<PreparedFixedColumn<'a>>,
    validity_width: usize,
}

impl PreparedFixedRowScatter<'_> {
    /// Scatter a row whose logical index was validated by the enclosing batch.
    ///
    /// # Safety
    ///
    /// `row_ptr` must be writable for the layout used to compile this program,
    /// and `row_idx` must be below the prepared vector cardinality.
    #[inline]
    pub unsafe fn scatter_row_unchecked(&self, row_ptr: *mut u8, row_idx: usize) {
        if self.validity_width != 0 {
            unsafe { ptr::write_bytes(row_ptr, u8::MAX, self.validity_width) };
        }
        for column in &self.columns {
            let source = unsafe {
                column
                    .data
                    .add(column.view.physical_index(row_idx) * column.source_width)
            };
            let target = unsafe { row_ptr.add(column.target_offset) };
            unsafe { copy_fixed_width(source, target, column.source_width) };
        }
    }
}

impl<'a> PreparedRowScatter<'a> {
    pub fn try_new(layout: &RowLayout, columns: &[&'a Vector], count: usize) -> Result<Self> {
        if columns.len() != layout.column_count() {
            return Err(paro_error::internal(format!(
                "row scatter width mismatch: expected={}, actual={}",
                layout.column_count(),
                columns.len()
            )));
        }

        let mut prepared = Vec::with_capacity(columns.len());
        let mut heap_columns = Vec::new();
        for (column_idx, source) in columns.iter().copied().enumerate() {
            let expected = &layout.types()[column_idx];
            if source.logical_type() != expected {
                return Err(paro_error::internal(format!(
                    "row scatter type mismatch at column {column_idx}: expected={expected:?}, actual={:?}",
                    source.logical_type()
                )));
            }
            let codec = layout.codecs().get(column_idx).ok_or_else(|| {
                paro_error::internal(format!("missing row codec for column {column_idx}"))
            })?;
            prepared.push(match codec {
                ColumnCodec::Fixed { size } => PreparedColumn::Fixed {
                    view: source.try_to_view(count)?,
                    source,
                    width: *size,
                },
                ColumnCodec::Varlen(_) => {
                    heap_columns.push(column_idx);
                    PreparedColumn::Varlen {
                        view: source.try_to_varlen_view(count)?,
                    }
                }
                ColumnCodec::List(_) | ColumnCodec::Array(_) | ColumnCodec::Struct(_) => {
                    heap_columns.push(column_idx);
                    PreparedColumn::Nested { source }
                }
            });
        }
        let all_valid = prepared.iter().all(PreparedColumn::all_valid);
        Ok(Self {
            columns: prepared,
            #[cfg(debug_assertions)]
            source_columns: columns.to_vec(),
            heap_columns,
            all_valid,
            count,
        })
    }

    /// Verify the complete logical row after an unchecked scatter.
    ///
    /// This is deliberately debug-only: it turns the uninitialized destination
    /// contract into an executable assertion without adding release-build
    /// clearing or materialization to the hot path.
    #[cfg(debug_assertions)]
    pub unsafe fn debug_assert_row_initialized(
        &self,
        layout: &RowLayout,
        row_ptr: *const u8,
        row_idx: usize,
    ) {
        for (column_idx, source) in self.source_columns.iter().enumerate() {
            let expected = source.get_value(row_idx);
            let actual = unsafe { unsafe_api::read_row_value(layout, row_ptr, column_idx) };
            debug_assert_eq!(actual, expected, "row scatter missed column {column_idx}");
        }
    }

    /// Compile the common all-valid fixed-width shape into a direct row
    /// scatter program. Shapes containing sequences, NULLs, varlen, or nested
    /// values keep using [`Self::scatter_row_unchecked`].
    pub fn fixed_all_valid(&self, layout: &RowLayout) -> Option<PreparedFixedRowScatter<'a>> {
        if !self.all_valid || self.columns.len() != layout.column_count() {
            return None;
        }
        let mut columns = Vec::with_capacity(self.columns.len());
        for (column_idx, column) in self.columns.iter().enumerate() {
            let PreparedColumn::Fixed { view, width, .. } = column else {
                return None;
            };
            let DataRef::Ptr(data) = view.data() else {
                return None;
            };
            columns.push(PreparedFixedColumn {
                view: view.clone(),
                data,
                source_width: *width,
                target_offset: layout.offsets()[column_idx],
            });
        }
        Some(PreparedFixedRowScatter {
            columns,
            validity_width: usize::from(!layout.all_valid()) * layout.validity().flag_width(),
        })
    }

    /// Whether any source column can retain data outside its destination row.
    #[inline]
    pub fn has_heap_values(&self) -> bool {
        !self.heap_columns.is_empty()
    }

    /// Measure allocations owned outside the row for one logical source row.
    pub fn heap_usage(&self, row_idx: usize) -> Result<RowHeapUsage> {
        self.validate_row(row_idx)?;
        let mut usage = RowHeapUsage::default();
        for &column_idx in &self.heap_columns {
            let column = &self.columns[column_idx];
            if !column.is_valid(row_idx) {
                continue;
            }
            let column_usage = match column {
                PreparedColumn::Fixed { .. } => RowHeapUsage::default(),
                PreparedColumn::Varlen { view } => {
                    let len = view.bytes(row_idx).len();
                    RowHeapUsage {
                        varlen_bytes: usize::from(len > StringView::INLINE_CAPACITY) * len,
                        nested_value_bytes: 0,
                    }
                }
                PreparedColumn::Nested { source } => {
                    let value = source.get_value(row_idx);
                    RowHeapUsage {
                        varlen_bytes: 0,
                        nested_value_bytes: size_of::<Value>()
                            .saturating_add(value.allocation_size()),
                    }
                }
            };
            usage = usage.checked_add(column_usage)?;
        }
        Ok(usage)
    }

    /// Scatter one logical source row into an initialized row slot.
    ///
    /// # Safety
    ///
    /// `row_ptr` must be writable for `layout.row_width()` bytes. Allocations
    /// returned by `heap` must remain alive as long as the written row.
    pub unsafe fn scatter_row(
        &self,
        layout: &RowLayout,
        row_ptr: *mut u8,
        row_idx: usize,
        heap: &mut impl RowHeapWriter,
    ) -> Result<()> {
        self.validate_layout(layout)?;
        self.validate_row(row_idx)?;

        unsafe { self.scatter_row_unchecked(layout, row_ptr, row_idx, heap) }
    }

    /// Scatter a row after the caller has validated the prepared layout and
    /// source-row bounds for the enclosing batch.
    ///
    /// # Safety
    ///
    /// In addition to [`Self::scatter_row`]'s requirements, `layout` must be
    /// the layout used to prepare this scatter and `row_idx < self.count`.
    pub unsafe fn scatter_row_unchecked(
        &self,
        layout: &RowLayout,
        row_ptr: *mut u8,
        row_idx: usize,
        heap: &mut impl RowHeapWriter,
    ) -> Result<()> {
        if !layout.all_valid() {
            // All-valid batches dominate analytical joins. Initialize their
            // row validity in one contiguous write instead of one read-modify-
            // write per column. The general nullable path starts from zero and
            // sets only the observed valid cells.
            unsafe {
                ptr::write_bytes(
                    row_ptr,
                    u8::from(self.all_valid) * u8::MAX,
                    layout.validity().flag_width(),
                )
            };
        }

        for (column_idx, column) in self.columns.iter().enumerate() {
            let target = unsafe { row_ptr.add(layout.offsets()[column_idx]) };
            if !self.all_valid && !column.is_valid(row_idx) {
                let width = RowLayout::get_type_size(&layout.types()[column_idx]);
                unsafe { ptr::write_bytes(target, 0, width) };
                continue;
            }
            if !self.all_valid && !layout.all_valid() {
                unsafe { unsafe_api::set_row_validity(row_ptr, column_idx) };
            }

            match column {
                PreparedColumn::Fixed {
                    view,
                    source,
                    width,
                } => match view.data() {
                    DataRef::Ptr(data) => unsafe {
                        copy_fixed_width(
                            data.add(view.physical_index(row_idx) * width),
                            target,
                            *width,
                        );
                    },
                    DataRef::SequenceI64 { .. } => unsafe {
                        unsafe_api::write_vector_value(
                            layout, row_ptr, column_idx, source, row_idx, heap,
                        )?;
                    },
                },
                PreparedColumn::Varlen { view } => {
                    unsafe { write_varlen(target, view.bytes(row_idx), heap)? };
                }
                PreparedColumn::Nested { source } => unsafe {
                    unsafe_api::write_vector_value(
                        layout, row_ptr, column_idx, source, row_idx, heap,
                    )?;
                },
            }
        }
        Ok(())
    }

    fn validate_layout(&self, layout: &RowLayout) -> Result<()> {
        if self.columns.len() != layout.column_count() {
            return Err(paro_error::internal(format!(
                "prepared row scatter layout changed: columns={}, layout_columns={}",
                self.columns.len(),
                layout.column_count()
            )));
        }
        Ok(())
    }

    fn validate_row(&self, row_idx: usize) -> Result<()> {
        if row_idx >= self.count {
            return Err(paro_error::internal(format!(
                "row scatter source index {row_idx} out of bounds {}",
                self.count
            )));
        }
        Ok(())
    }
}

/// Copy the physical widths used by fixed SQL types without routing every
/// scalar cell through a variable-length libc memcpy call.
///
/// # Safety
///
/// `source` and `target` must be readable/writable for `width` bytes and must
/// not overlap.
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

unsafe fn write_varlen(target: *mut u8, bytes: &[u8], heap: &mut impl RowHeapWriter) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| paro_error::out_of_range("row varlen value exceeds u32 length"))?;
    let value = if let Some(value) = StringView::try_inline(bytes) {
        value
    } else {
        let retained = heap.store_bytes(bytes)?;
        // SAFETY: `store_bytes` returned immutable row-owned storage containing
        // all `len` bytes, and that owner outlives the target row cell.
        unsafe { StringView::from_raw_parts(retained, len) }
    };
    // SAFETY: `target` identifies a writable StringView-sized row cell.
    unsafe { value.write_cell(target) };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::allocator::DefaultAllocator;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::{SelectionVector, Vector};

    use super::*;
    use crate::row::codec::gather_column_from_rows;
    use crate::row::RowValidityType;

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
    fn scatters_dictionary_fixed_and_varlen_values() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut integers = Vector::try_new(LogicalType::Integer, 3, allocator.clone()).unwrap();
        for (idx, value) in [11, 22, 33].into_iter().enumerate() {
            integers.set_i32(idx, value);
        }
        integers.set_count(3);
        let mut strings = Vector::try_new(LogicalType::Varchar, 3, allocator.clone()).unwrap();
        strings.set_string(0, "short");
        strings.set_string(1, "a retained string longer than twelve bytes");
        strings.set_string(2, "unused");
        strings.set_count(3);
        let selection = SelectionVector::try_from_indices(vec![1, 0], allocator.clone()).unwrap();
        let integers = Vector::try_dictionary(Arc::new(integers), selection.clone()).unwrap();
        let strings = Vector::try_dictionary(Arc::new(strings), selection).unwrap();

        let layout = RowLayout::from_types(
            vec![LogicalType::Integer, LogicalType::Varchar],
            RowValidityType::CanHaveNullValues,
        );
        let scatter = PreparedRowScatter::try_new(&layout, &[&integers, &strings], 2).unwrap();
        assert!(scatter.has_heap_values());
        assert_eq!(scatter.heap_usage(0).unwrap().varlen_bytes(), 42);
        assert_eq!(scatter.heap_usage(1).unwrap().varlen_bytes(), 0);

        let mut rows = vec![0_u8; layout.row_width() * 2];
        let mut heap = TestHeap::default();
        for row_idx in 0..2 {
            unsafe {
                scatter
                    .scatter_row(
                        &layout,
                        rows.as_mut_ptr().add(row_idx * layout.row_width()),
                        row_idx,
                        &mut heap,
                    )
                    .unwrap();
            }
        }

        let mut gathered_int = Vector::try_new(LogicalType::Integer, 2, allocator.clone()).unwrap();
        let mut gathered_string = Vector::try_new(LogicalType::Varchar, 2, allocator).unwrap();
        let row_base = rows.as_ptr();
        unsafe {
            gather_column_from_rows(&layout, 0, &mut gathered_int, 2, |idx| {
                row_base.add(idx * layout.row_width())
            })
            .unwrap();
            gather_column_from_rows(&layout, 1, &mut gathered_string, 2, |idx| {
                row_base.add(idx * layout.row_width())
            })
            .unwrap();
        }
        assert_eq!(gathered_int.get_i32(0), Some(22));
        assert_eq!(gathered_int.get_i32(1), Some(11));
        assert_eq!(
            gathered_string.get_string(0),
            Some("a retained string longer than twelve bytes")
        );
        assert_eq!(gathered_string.get_string(1), Some("short"));
    }

    #[test]
    fn fixed_width_scatter_has_no_heap_planning_pass() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut integers = Vector::try_new(LogicalType::Integer, 2, allocator).unwrap();
        integers.set_i32(0, 10);
        integers.set_i32(1, 20);
        integers.set_count(2);

        let layout = RowLayout::from_types(
            vec![LogicalType::Integer],
            RowValidityType::CanHaveNullValues,
        );
        let scatter = PreparedRowScatter::try_new(&layout, &[&integers], 2).unwrap();

        assert!(!scatter.has_heap_values());
        assert_eq!(scatter.heap_usage(1).unwrap(), RowHeapUsage::default());
    }
}
