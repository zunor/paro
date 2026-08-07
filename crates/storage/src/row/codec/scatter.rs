// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepared scatter from columnar vectors into execution rows.

use std::mem::size_of;
use std::ptr;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::INLINE_LENGTH;
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
    count: usize,
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
                ColumnCodec::Varlen(_) => PreparedColumn::Varlen {
                    view: source.try_to_varlen_view(count)?,
                },
                ColumnCodec::List(_) | ColumnCodec::Array(_) | ColumnCodec::Struct(_) => {
                    PreparedColumn::Nested { source }
                }
            });
        }
        Ok(Self {
            columns: prepared,
            count,
        })
    }

    /// Measure allocations owned outside the row for one logical source row.
    pub fn heap_usage(&self, row_idx: usize) -> Result<RowHeapUsage> {
        self.validate_row(row_idx)?;
        let mut usage = RowHeapUsage::default();
        for column in &self.columns {
            if !column.is_valid(row_idx) {
                continue;
            }
            let column_usage = match column {
                PreparedColumn::Fixed { .. } => RowHeapUsage::default(),
                PreparedColumn::Varlen { view } => {
                    let value = view.get_inline_string(row_idx);
                    RowHeapUsage {
                        varlen_bytes: usize::from(value.len() > INLINE_LENGTH) * value.len(),
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

        if !layout.all_valid() {
            // The row slot may be recycled by a caller in the future, so make
            // validity independent of its previous contents.
            unsafe { ptr::write_bytes(row_ptr, 0, layout.validity().flag_width()) };
        }

        for (column_idx, column) in self.columns.iter().enumerate() {
            let target = unsafe { row_ptr.add(layout.offsets()[column_idx]) };
            if !column.is_valid(row_idx) {
                let width = RowLayout::get_type_size(&layout.types()[column_idx]);
                unsafe { ptr::write_bytes(target, 0, width) };
                continue;
            }
            if !layout.all_valid() {
                unsafe { unsafe_api::set_row_validity(row_ptr, column_idx) };
            }

            match column {
                PreparedColumn::Fixed {
                    view,
                    source,
                    width,
                } => match view.data() {
                    DataRef::Ptr(data) => unsafe {
                        ptr::copy_nonoverlapping(
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
                    let value = view.get_inline_string(row_idx);
                    unsafe { write_varlen(target, value.as_bytes(), heap)? };
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

unsafe fn write_varlen(target: *mut u8, bytes: &[u8], heap: &mut impl RowHeapWriter) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| paro_error::out_of_range("row varlen value exceeds u32 length"))?;
    unsafe { ptr::write_unaligned(target.cast::<u32>(), len) };
    if bytes.len() <= INLINE_LENGTH {
        if !bytes.is_empty() {
            unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), target.add(4), bytes.len()) };
        }
        if bytes.len() < INLINE_LENGTH {
            unsafe {
                ptr::write_bytes(target.add(4 + bytes.len()), 0, INLINE_LENGTH - bytes.len())
            };
        }
        return Ok(());
    }

    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), target.add(4), 4) };
    let retained = heap.store_bytes(bytes)?;
    unsafe { ptr::write_unaligned(target.add(8).cast::<*const u8>(), retained) };
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
}
