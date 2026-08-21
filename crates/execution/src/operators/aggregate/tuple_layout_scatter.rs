// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepared column views for scattering vector batches into aggregate rows.

use super::*;
use paro_common::vector::{DataRef, VarlenView, VectorView};
use std::cell::Cell;

enum ScatterColumn<'a> {
    Fixed {
        view: VectorView<'a>,
        source: &'a Vector,
        width: usize,
    },
    Varlen {
        view: VarlenView<'a>,
        last_heap_ref: Cell<Option<(usize, VarlenRef)>>,
    },
}

impl ScatterColumn<'_> {
    #[inline]
    fn is_valid(&self, row_idx: usize) -> bool {
        match self {
            Self::Fixed { view, .. } => view.is_valid(row_idx),
            Self::Varlen { view, .. } => view.is_valid(row_idx),
        }
    }
}

/// A batch-scoped, decoded view of group columns.
///
/// Dictionary and constant selections are composed once when this view is
/// built. Row insertion can then access validity and physical values directly,
/// without recursively dispatching through [`Vector`] for every column.
pub(crate) struct TupleScatterSource<'a> {
    columns: Vec<ScatterColumn<'a>>,
    count: usize,
    all_valid: bool,
}

impl<'a> TupleScatterSource<'a> {
    fn try_new(layout: &TupleLayout, groups: &'a Chunk) -> Result<Self> {
        if groups.column_count() != layout.group_count() {
            return Err(paro_error::internal(format!(
                "Tuple scatter group width mismatch: expected={}, actual={}",
                layout.group_count(),
                groups.column_count()
            )));
        }
        let mut columns = Vec::with_capacity(layout.group_count());
        for group_idx in 0..layout.group_count() {
            let source = groups
                .column(group_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Tuple scatter source column missing: index={group_idx}"
                    ))
                })?
                .as_ref();
            let expected = &layout.group_types[group_idx];
            if source.logical_type() != expected {
                return Err(paro_error::internal(format!(
                    "Tuple scatter source type mismatch at column {group_idx}: expected={expected:?}, actual={:?}",
                    source.logical_type()
                )));
            }
            if layout.varlen_groups[group_idx] {
                columns.push(ScatterColumn::Varlen {
                    view: source.try_to_varlen_view(groups.size())?,
                    last_heap_ref: Cell::new(None),
                });
            } else {
                columns.push(ScatterColumn::Fixed {
                    view: source.try_to_view(groups.size())?,
                    source,
                    width: group_storage_width(expected)?,
                });
            }
        }
        let all_valid = columns.iter().all(|column| match column {
            ScatterColumn::Fixed { view, .. } => view.validity().all_valid(),
            ScatterColumn::Varlen { view, .. } => view.validity().all_valid(),
        });
        Ok(Self {
            columns,
            count: groups.size(),
            all_valid,
        })
    }

    fn scatter_row(
        &self,
        layout: &TupleLayout,
        row_ptr: *mut u8,
        row_idx: usize,
        varlen_heap: &mut VarlenHeap,
    ) -> Result<()> {
        debug_assert_eq!(self.columns.len(), layout.group_count());
        debug_assert!(row_idx < self.count);
        if layout.validity_width > 0 {
            unsafe {
                std::ptr::write_bytes(
                    row_ptr,
                    if self.all_valid { u8::MAX } else { 0 },
                    layout.validity_width,
                );
            }
        }

        if self.all_valid {
            self.scatter_columns::<true>(layout, row_ptr, row_idx, varlen_heap)
        } else {
            self.scatter_columns::<false>(layout, row_ptr, row_idx, varlen_heap)
        }
    }

    fn scatter_columns<const ALL_VALID: bool>(
        &self,
        layout: &TupleLayout,
        row_ptr: *mut u8,
        row_idx: usize,
        varlen_heap: &mut VarlenHeap,
    ) -> Result<()> {
        for (group_idx, column) in self.columns.iter().enumerate() {
            let target = unsafe { row_ptr.add(layout.group_offsets[group_idx]) };
            if !ALL_VALID && !column.is_valid(row_idx) {
                unsafe {
                    std::ptr::write_bytes(
                        target,
                        0,
                        group_storage_width(&layout.group_types[group_idx])?,
                    );
                }
                continue;
            }
            if !ALL_VALID {
                set_validity(row_ptr, group_idx, true);
            }
            match column {
                ScatterColumn::Varlen {
                    view,
                    last_heap_ref,
                } => {
                    let bytes = view.bytes(row_idx);
                    let target_ref = if bytes.len() <= VarlenRef::inline_capacity() {
                        VarlenRef::from_inline(bytes)?
                    } else {
                        let physical_idx = view.sel().get(row_idx);
                        match last_heap_ref.get() {
                            Some((cached_idx, cached)) if cached_idx == physical_idx => cached,
                            _ => {
                                let stored = varlen_heap.intern(bytes)?;
                                last_heap_ref.set(Some((physical_idx, stored)));
                                stored
                            }
                        }
                    };
                    unsafe {
                        std::ptr::write_unaligned(target as *mut VarlenRef, target_ref);
                    }
                }
                ScatterColumn::Fixed {
                    view,
                    source,
                    width,
                } => match view.data() {
                    DataRef::Ptr(data) => unsafe {
                        std::ptr::copy_nonoverlapping(
                            data.add(view.physical_index(row_idx) * width),
                            target,
                            *width,
                        );
                    },
                    DataRef::SequenceI64 { .. } => write_fixed_group_value(
                        target,
                        source,
                        row_idx,
                        &layout.group_types[group_idx],
                    )?,
                },
            }
        }
        Ok(())
    }

    fn row_equals(
        &self,
        layout: &TupleLayout,
        stored_row: *const u8,
        row_idx: usize,
        varlen_heap: &VarlenHeap,
    ) -> Result<bool> {
        debug_assert_eq!(self.columns.len(), layout.group_count());
        debug_assert!(row_idx < self.count);
        if self.all_valid {
            self.row_equals_columns::<true>(layout, stored_row, row_idx, varlen_heap)
        } else {
            self.row_equals_columns::<false>(layout, stored_row, row_idx, varlen_heap)
        }
    }

    fn row_equals_columns<const ALL_VALID: bool>(
        &self,
        layout: &TupleLayout,
        stored_row: *const u8,
        row_idx: usize,
        varlen_heap: &VarlenHeap,
    ) -> Result<bool> {
        for (group_idx, column) in self.columns.iter().enumerate() {
            let stored_valid = row_is_valid(stored_row, group_idx);
            let source_valid = ALL_VALID || column.is_valid(row_idx);
            if stored_valid != source_valid {
                return Ok(false);
            }
            if !stored_valid {
                continue;
            }

            let stored = unsafe { stored_row.add(layout.group_offsets[group_idx]) };
            match column {
                ScatterColumn::Varlen { view, .. } => {
                    let stored_ref =
                        unsafe { std::ptr::read_unaligned(stored as *const VarlenRef) };
                    if read_varlen_ref_bytes(&stored_ref, varlen_heap)? != view.bytes(row_idx) {
                        return Ok(false);
                    }
                }
                ScatterColumn::Fixed {
                    view,
                    source,
                    width,
                } => match view.data() {
                    DataRef::Ptr(data) => {
                        let source = unsafe {
                            std::slice::from_raw_parts(
                                data.add(view.physical_index(row_idx) * width),
                                *width,
                            )
                        };
                        let stored = unsafe { std::slice::from_raw_parts(stored, *width) };
                        if stored != source {
                            return Ok(false);
                        }
                    }
                    DataRef::SequenceI64 { .. } => {
                        if !fixed_group_value_equals(
                            stored,
                            source,
                            row_idx,
                            &layout.group_types[group_idx],
                        )? {
                            return Ok(false);
                        }
                    }
                },
            }
        }
        Ok(true)
    }
}

impl TupleLayout {
    pub(crate) fn prepare_scatter<'a>(&self, groups: &'a Chunk) -> Result<TupleScatterSource<'a>> {
        TupleScatterSource::try_new(self, groups)
    }

    pub(crate) fn scatter_prepared_groups(
        &self,
        row_ptr: *mut u8,
        source: &TupleScatterSource<'_>,
        row_idx: usize,
        varlen_heap: &mut VarlenHeap,
    ) -> Result<()> {
        source.scatter_row(self, row_ptr, row_idx, varlen_heap)
    }

    pub(crate) fn compare_prepared_groups(
        &self,
        row_ptr: *const u8,
        source: &TupleScatterSource<'_>,
        row_idx: usize,
        varlen_heap: &VarlenHeap,
    ) -> Result<bool> {
        source.row_equals(self, row_ptr, row_idx, varlen_heap)
    }
}
