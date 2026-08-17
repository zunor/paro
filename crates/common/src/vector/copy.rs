// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};
use crate::types::{LogicalType, StringView};

use super::{SelectionRef, SelectionVector, StringHeap, Vector, VectorSelection, VectorType};

#[inline]
fn ranges_overlap(src: *const u8, dst: *mut u8, bytes: usize) -> bool {
    let src_start = src as usize;
    let src_end = src_start.saturating_add(bytes);
    let dst_start = dst as usize;
    let dst_end = dst_start.saturating_add(bytes);
    src_start < dst_end && dst_start < src_end
}

#[derive(Debug, Clone)]
pub(crate) enum CopySourceRows<'a> {
    Range {
        offset: usize,
    },
    BorrowedMaterialized(&'a SelectionVector),
    BorrowedMaterializedRange {
        selection: &'a SelectionVector,
        offset: usize,
    },
    OwnedMaterialized(SelectionVector),
    OffsetBorrowedMaterialized {
        offset: usize,
        selection: &'a SelectionVector,
    },
    OffsetOwnedMaterialized {
        offset: usize,
        selection: SelectionVector,
    },
    ComposedBorrowedMaterialized {
        base: &'a SelectionVector,
        selection: &'a SelectionVector,
    },
    ComposedOwnedMaterialized {
        base: &'a SelectionVector,
        selection: SelectionVector,
    },
    Constant {
        index: usize,
    },
    Incremental {
        start: usize,
    },
}

impl<'a> CopySourceRows<'a> {
    pub(crate) fn from_selection_ref(selection: SelectionRef<'a>) -> Self {
        match selection {
            SelectionRef::Borrowed(selection) => Self::BorrowedMaterialized(selection),
            SelectionRef::Owned(selection) => Self::OwnedMaterialized(selection),
            SelectionRef::Range { offset, .. } => Self::Range { offset },
            SelectionRef::Constant { .. } => Self::Constant { index: 0 },
            SelectionRef::Incremental { .. } => Self::Incremental { start: 0 },
        }
    }

    #[inline]
    fn source_index(&self, logical_idx: usize) -> usize {
        match self {
            Self::Range { offset } | Self::Incremental { start: offset } => offset + logical_idx,
            Self::BorrowedMaterialized(selection) => selection.get(logical_idx),
            Self::BorrowedMaterializedRange { selection, offset } => {
                selection.get(offset + logical_idx)
            }
            Self::OwnedMaterialized(selection) => selection.get(logical_idx),
            Self::OffsetBorrowedMaterialized { offset, selection } => {
                offset + selection.get(logical_idx)
            }
            Self::OffsetOwnedMaterialized { offset, selection } => {
                offset + selection.get(logical_idx)
            }
            Self::ComposedBorrowedMaterialized { base, selection } => {
                base.get(selection.get(logical_idx))
            }
            Self::ComposedOwnedMaterialized { base, selection } => {
                base.get(selection.get(logical_idx))
            }
            Self::Constant { index } => *index,
        }
    }

    #[inline]
    fn contiguous_offset(&self) -> Option<usize> {
        match self {
            Self::Range { offset } | Self::Incremental { start: offset } => Some(*offset),
            Self::BorrowedMaterialized(_)
            | Self::BorrowedMaterializedRange { .. }
            | Self::OwnedMaterialized(_)
            | Self::OffsetBorrowedMaterialized { .. }
            | Self::OffsetOwnedMaterialized { .. }
            | Self::ComposedBorrowedMaterialized { .. }
            | Self::ComposedOwnedMaterialized { .. }
            | Self::Constant { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CopyDestinationRows<'a> {
    Range { offset: usize },
    Scatter(&'a [usize]),
}

impl<'a> CopyDestinationRows<'a> {
    #[inline]
    fn destination_index(&self, logical_idx: usize) -> usize {
        match self {
            Self::Range { offset } => offset + logical_idx,
            Self::Scatter(positions) => positions[logical_idx],
        }
    }

    #[inline]
    fn contiguous_offset(&self) -> Option<usize> {
        match self {
            Self::Range { offset } => Some(*offset),
            Self::Scatter(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
struct CopyRun {
    dst_offset: usize,
    src_offset: usize,
    count: usize,
}

#[derive(Clone, Copy)]
struct ResolvedCopySource<'a> {
    vector: &'a Vector,
    row_idx: usize,
}

impl<'a> ResolvedCopySource<'a> {
    fn resolve(vector: &'a Vector, row_idx: usize) -> Self {
        match vector.vector_type {
            VectorType::Flat | VectorType::Sequence => Self { vector, row_idx },
            VectorType::Constant => Self { vector, row_idx: 0 },
            VectorType::Dictionary => {
                let child = vector
                    .child
                    .as_ref()
                    .expect("Dictionary vector missing child");
                Self::resolve(child, vector.selection.physical_index(row_idx))
            }
        }
    }
}

impl Vector {
    /// Copy a value from another vector at the given index.
    ///
    /// This is a single-value materializing copy operation. Allocation failures
    /// return `Err`; broken vector invariants remain programmer errors and may
    /// panic.
    pub fn try_copy_at(&mut self, idx: usize, source: &Vector, source_idx: usize) -> Result<()> {
        self.check_copy_compatible(source)?;
        self.check_source_row(source, source_idx)?;
        self.check_destination_row(idx)?;

        self.try_make_exclusive()?;
        let source = ResolvedCopySource::resolve(source, source_idx);

        if source.vector.is_null(source.row_idx) {
            self.try_set_null(idx, true)?;
            return Ok(());
        }

        if Self::is_varlen_type(&self.logical_type) && self.logical_type != LogicalType::Blob {
            if let Some(s) = source.vector.get_string(source.row_idx) {
                self.try_set_string(idx, s)?;
            } else {
                self.try_set_null(idx, true)?;
            }
            return Ok(());
        }

        if self.logical_type == LogicalType::Blob {
            if let Some(b) = source.vector.get_blob(source.row_idx) {
                self.try_set_blob(idx, b)?;
            } else {
                self.try_set_null(idx, true)?;
            }
            return Ok(());
        }

        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                self.try_copy_array_at(idx, source, *array_size)?;
                return Ok(());
            }
            LogicalType::List(_) => {
                self.try_copy_list_at(idx, source)?;
                return Ok(());
            }
            LogicalType::Struct(_) => {
                self.try_copy_struct_at(idx, source)?;
                return Ok(());
            }
            _ => {}
        }

        self.try_set_null(idx, false)?;
        self.copy_fixed_value_at(idx, source);
        Ok(())
    }

    /// Copy a contiguous logical range from `source` into this flat destination vector.
    pub fn try_copy_range(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        self.check_copy_compatible(source)?;
        let dst_end = self.checked_destination_end(dst_offset, count, "copy range")?;
        let src_end = Self::checked_source_end(src_offset, count, "copy range")?;

        if src_end > source.len() {
            return Err(paro_error::internal(format!(
                "copy range source out of bounds: end={src_end}, len={}",
                source.len()
            )));
        }
        if dst_end > self.logical_capacity() {
            return Err(paro_error::internal(format!(
                "copy range destination out of bounds for {:?}: end={dst_end}, capacity={}",
                self.logical_type,
                self.logical_capacity()
            )));
        }
        if count == 0 {
            return Ok(());
        }
        if source.logical_type == LogicalType::Null {
            return self.try_copy_null_rows(
                source,
                CopySourceRows::Range { offset: src_offset },
                CopyDestinationRows::Range { offset: dst_offset },
                count,
            );
        }
        if self.vector_type != VectorType::Flat {
            return Err(paro_error::internal(format!(
                "copy range destination must be flat, got {:?}",
                self.vector_type
            )));
        }

        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                self.try_copy_array_rows(
                    source,
                    CopySourceRows::Range { offset: src_offset },
                    CopyDestinationRows::Range { offset: dst_offset },
                    count,
                    *array_size,
                )?;
            }
            LogicalType::List(_) => {
                self.try_copy_list_rows(
                    source,
                    CopySourceRows::Range { offset: src_offset },
                    CopyDestinationRows::Range { offset: dst_offset },
                    count,
                )?;
            }
            LogicalType::Struct(_) => {
                self.try_copy_struct_rows(
                    source,
                    CopySourceRows::Range { offset: src_offset },
                    CopyDestinationRows::Range { offset: dst_offset },
                    count,
                )?;
            }
            _ if Self::is_fixed_payload_type(&self.logical_type)
                && source.logical_type == self.logical_type
                && source.vector_type == VectorType::Flat =>
            {
                self.try_copy_flat_fixed_range(dst_offset, source, src_offset, count)?;
            }
            _ if Self::is_fixed_payload_type(&self.logical_type)
                && source.logical_type == self.logical_type
                && source.vector_type == VectorType::Dictionary
                && source
                    .child
                    .as_ref()
                    .is_some_and(|child| child.vector_type == VectorType::Flat) =>
            {
                self.try_copy_dictionary_fixed_range(dst_offset, source, src_offset, count)?;
            }
            _ if Self::is_varlen_type(&self.logical_type)
                && source.logical_type == self.logical_type
                && dst_offset == 0
                && dst_end >= self.count =>
            {
                self.try_copy_varlen_range_rebuild_heap(dst_offset, source, src_offset, count)?;
            }
            _ if Self::is_varlen_type(&self.logical_type)
                && source.logical_type == self.logical_type
                && dst_offset >= self.count =>
            {
                self.try_copy_varlen_range_append_heap(dst_offset, source, src_offset, count)?;
            }
            _ => {
                for i in 0..count {
                    self.try_copy_at(dst_offset + i, source, src_offset + i)?;
                }
            }
        }

        if self.count < dst_end {
            self.try_set_count(dst_end)?;
        }
        Ok(())
    }

    /// Copy selected source rows into a contiguous destination range.
    pub fn try_copy_selection(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        selection: &VectorSelection,
        count: usize,
    ) -> Result<()> {
        let selection = match selection {
            VectorSelection::None => SelectionRef::Incremental { count },
            VectorSelection::Range {
                offset,
                count: selection_count,
            } => {
                if count > *selection_count {
                    return Err(paro_error::internal(format!(
                        "copy selection count exceeds range selection: count={count}, selection_count={selection_count}"
                    )));
                }
                SelectionRef::Range {
                    offset: *offset,
                    count,
                }
            }
            VectorSelection::Materialized(selection) => SelectionRef::Borrowed(selection),
        };
        self.try_copy_selection_ref(dst_offset, source, selection, count)
    }

    pub(crate) fn try_copy_selection_ref(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        selection: SelectionRef<'_>,
        count: usize,
    ) -> Result<()> {
        if count > selection.len() {
            return Err(paro_error::internal(format!(
                "copy selection count exceeds selection length: count={count}, selection_len={}",
                selection.len()
            )));
        }
        self.try_copy_rows(
            source,
            CopySourceRows::from_selection_ref(selection),
            CopyDestinationRows::Range { offset: dst_offset },
            count,
        )
    }

    /// Copy consecutive source rows into arbitrary destination positions.
    pub fn try_copy_scatter(
        &mut self,
        source: &Vector,
        src_start: usize,
        dst_positions: &[usize],
    ) -> Result<()> {
        self.try_copy_rows(
            source,
            CopySourceRows::Range { offset: src_start },
            CopyDestinationRows::Scatter(dst_positions),
            dst_positions.len(),
        )
    }

    fn try_copy_rows(
        &mut self,
        source: &Vector,
        src_rows: CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<()> {
        self.check_copy_compatible(source)?;
        if count == 0 {
            return Ok(());
        }

        let (source, src_rows) = Self::normalize_source_rows(source, src_rows)?;
        if source.logical_type == LogicalType::Null {
            return self.try_copy_null_rows(source, src_rows, dst_rows, count);
        }

        if let (Some(dst_offset), Some(src_offset)) =
            (dst_rows.contiguous_offset(), src_rows.contiguous_offset())
        {
            return self.try_copy_range(dst_offset, source, src_offset, count);
        }

        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                return self.try_copy_array_rows(source, src_rows, dst_rows, count, *array_size);
            }
            LogicalType::List(_) => {
                return self.try_copy_list_rows(source, src_rows, dst_rows, count);
            }
            LogicalType::Struct(_) => {
                return self.try_copy_struct_rows(source, src_rows, dst_rows, count);
            }
            _ => {}
        }

        self.try_make_exclusive()?;

        let mut max_dst = 0usize;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            let dst_idx = dst_rows.destination_index(logical_idx);
            self.check_source_row(source, src_idx)?;
            self.check_destination_row(dst_idx)?;
            max_dst = max_dst.max(dst_idx);
            self.try_copy_at(dst_idx, source, src_idx)?;
        }

        let required_count = max_dst.checked_add(1).ok_or_else(|| {
            paro_error::internal(format!(
                "copy rows destination count overflow: max_dst={max_dst}"
            ))
        })?;
        if self.count < required_count {
            self.try_set_count(required_count)?;
        }
        Ok(())
    }

    fn try_copy_null_rows(
        &mut self,
        source: &Vector,
        src_rows: CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<()> {
        if let CopyDestinationRows::Range { offset } = dst_rows {
            self.check_source_rows(source, &src_rows, count)?;
            let dst_end = self.checked_destination_end(offset, count, "copy null rows")?;
            if dst_end > self.logical_capacity() {
                return Err(paro_error::internal(format!(
                    "copy null rows destination out of bounds for {:?}: end={dst_end}, capacity={}",
                    self.logical_type,
                    self.logical_capacity()
                )));
            }

            self.try_make_exclusive()?;
            self.validity.try_set_range_invalid(offset, count)?;
            if self.count < dst_end {
                self.try_set_count(dst_end)?;
            }
            return Ok(());
        }

        self.try_make_exclusive()?;

        let mut max_dst = 0usize;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            let dst_idx = dst_rows.destination_index(logical_idx);
            self.check_source_row(source, src_idx)?;
            self.check_destination_row(dst_idx)?;
            max_dst = max_dst.max(dst_idx);
            self.try_set_null(dst_idx, true)?;
        }

        let required_count = max_dst.checked_add(1).ok_or_else(|| {
            paro_error::internal(format!(
                "copy null rows destination count overflow: max_dst={max_dst}"
            ))
        })?;
        if self.count < required_count {
            self.try_set_count(required_count)?;
        }
        Ok(())
    }

    fn check_source_rows(
        &self,
        source: &Vector,
        rows: &CopySourceRows<'_>,
        count: usize,
    ) -> Result<()> {
        if let Some(offset) = rows.contiguous_offset() {
            let end = Self::checked_source_end(offset, count, "copy source rows")?;
            if end <= source.len() {
                return Ok(());
            }
            return Err(paro_error::internal(format!(
                "copy source rows out of bounds: end={end}, len={}",
                source.len()
            )));
        }

        if let CopySourceRows::Constant { index } = rows {
            return self.check_source_row(source, *index);
        }

        for logical_idx in 0..count {
            self.check_source_row(source, rows.source_index(logical_idx))?;
        }
        Ok(())
    }

    fn normalize_source_rows<'a>(
        source: &'a Vector,
        rows: CopySourceRows<'a>,
    ) -> Result<(&'a Vector, CopySourceRows<'a>)> {
        match source.vector_type {
            VectorType::Dictionary => {
                let child = source
                    .child
                    .as_ref()
                    .expect("Dictionary vector missing child");
                let rows = Self::compose_source_rows(source.selection(), rows)?;
                Self::normalize_source_rows(child, rows)
            }
            VectorType::Constant => Ok((source, CopySourceRows::Constant { index: 0 })),
            VectorType::Flat | VectorType::Sequence => Ok((source, rows)),
        }
    }

    fn compose_source_rows<'a>(
        base_selection: &'a VectorSelection,
        rows: CopySourceRows<'a>,
    ) -> Result<CopySourceRows<'a>> {
        match base_selection {
            VectorSelection::None => Ok(rows),
            VectorSelection::Range { offset, .. } => match rows {
                CopySourceRows::Range { offset: row_offset }
                | CopySourceRows::Incremental { start: row_offset } => Ok(CopySourceRows::Range {
                    offset: offset + row_offset,
                }),
                CopySourceRows::BorrowedMaterialized(selection) => {
                    Ok(CopySourceRows::OffsetBorrowedMaterialized {
                        offset: *offset,
                        selection,
                    })
                }
                CopySourceRows::BorrowedMaterializedRange {
                    selection,
                    offset: row_offset,
                } => Ok(CopySourceRows::OffsetOwnedMaterialized {
                    offset: *offset,
                    selection: Self::try_slice_selection_from(selection, row_offset)?,
                }),
                CopySourceRows::OwnedMaterialized(selection) => {
                    Ok(CopySourceRows::OffsetOwnedMaterialized {
                        offset: *offset,
                        selection,
                    })
                }
                CopySourceRows::OffsetBorrowedMaterialized {
                    offset: row_offset,
                    selection,
                } => Ok(CopySourceRows::OffsetBorrowedMaterialized {
                    offset: *offset + row_offset,
                    selection,
                }),
                CopySourceRows::OffsetOwnedMaterialized {
                    offset: row_offset,
                    selection,
                } => Ok(CopySourceRows::OffsetOwnedMaterialized {
                    offset: *offset + row_offset,
                    selection,
                }),
                CopySourceRows::ComposedBorrowedMaterialized { base, selection } => {
                    Ok(CopySourceRows::ComposedBorrowedMaterialized { base, selection })
                }
                CopySourceRows::ComposedOwnedMaterialized { base, selection } => {
                    Ok(CopySourceRows::ComposedOwnedMaterialized { base, selection })
                }
                CopySourceRows::Constant { index } => Ok(CopySourceRows::Constant {
                    index: offset + index,
                }),
            },
            VectorSelection::Materialized(base) => match rows {
                CopySourceRows::Range { offset }
                | CopySourceRows::Incremental { start: offset } => {
                    Ok(CopySourceRows::BorrowedMaterializedRange {
                        selection: base,
                        offset,
                    })
                }
                CopySourceRows::BorrowedMaterialized(selection) => {
                    Ok(CopySourceRows::ComposedBorrowedMaterialized { base, selection })
                }
                CopySourceRows::OwnedMaterialized(selection) => {
                    Ok(CopySourceRows::ComposedOwnedMaterialized { base, selection })
                }
                CopySourceRows::Constant { index } => Ok(CopySourceRows::Constant {
                    index: base.get(index),
                }),
                CopySourceRows::BorrowedMaterializedRange { selection, offset } => {
                    Ok(CopySourceRows::ComposedOwnedMaterialized {
                        base,
                        selection: Self::try_slice_selection_from(selection, offset)?,
                    })
                }
                rows @ (CopySourceRows::OffsetBorrowedMaterialized { .. }
                | CopySourceRows::OffsetOwnedMaterialized { .. }
                | CopySourceRows::ComposedBorrowedMaterialized { .. }
                | CopySourceRows::ComposedOwnedMaterialized { .. }) => {
                    Ok(CopySourceRows::OwnedMaterialized(
                        Self::try_materialize_composed_rows(base, rows)?,
                    ))
                }
            },
        }
    }

    fn try_slice_selection_from(
        selection: &SelectionVector,
        offset: usize,
    ) -> Result<SelectionVector> {
        if offset > selection.len() {
            return Err(paro_error::internal(format!(
                "copy source selection offset out of bounds: offset={offset}, len={}",
                selection.len()
            )));
        }
        selection.try_slice_range(offset, selection.len() - offset)
    }

    fn try_materialize_composed_rows(
        base: &SelectionVector,
        rows: CopySourceRows<'_>,
    ) -> Result<SelectionVector> {
        let count = match &rows {
            CopySourceRows::BorrowedMaterialized(selection)
            | CopySourceRows::BorrowedMaterializedRange { selection, .. }
            | CopySourceRows::OffsetBorrowedMaterialized { selection, .. }
            | CopySourceRows::ComposedBorrowedMaterialized { selection, .. } => selection.len(),
            CopySourceRows::OwnedMaterialized(selection)
            | CopySourceRows::OffsetOwnedMaterialized { selection, .. }
            | CopySourceRows::ComposedOwnedMaterialized { selection, .. } => selection.len(),
            CopySourceRows::Range { .. }
            | CopySourceRows::Constant { .. }
            | CopySourceRows::Incremental { .. } => {
                return Err(paro_error::internal(
                    "copy source row composition requires bounded selection rows",
                ));
            }
        };

        let mut materialized_selection =
            SelectionVector::try_with_capacity(count, base.allocator().clone())?;
        materialized_selection.set_len(count);
        for logical_idx in 0..count {
            let row_idx = rows.source_index(logical_idx);
            if row_idx >= base.len() {
                return Err(paro_error::internal(format!(
                    "copy source row composition out of bounds: row={row_idx}, base_len={}",
                    base.len()
                )));
            }
            materialized_selection.try_set(logical_idx, base.get(row_idx))?;
        }
        Ok(materialized_selection)
    }

    #[inline]
    fn check_copy_compatible(&self, source: &Vector) -> Result<()> {
        if self.logical_type == source.logical_type || source.logical_type == LogicalType::Null {
            return Ok(());
        }

        Err(paro_error::internal(format!(
            "copy type mismatch: dst={:?}, src={:?}",
            self.logical_type, source.logical_type
        )))
    }

    #[inline]
    fn check_source_row(&self, source: &Vector, row_idx: usize) -> Result<()> {
        if row_idx < source.len() {
            return Ok(());
        }
        Err(paro_error::internal(format!(
            "copy source row out of bounds: row={row_idx}, len={}",
            source.len()
        )))
    }

    #[inline]
    fn check_destination_row(&self, row_idx: usize) -> Result<()> {
        if row_idx < self.logical_capacity() {
            return Ok(());
        }
        Err(paro_error::internal(format!(
            "copy destination row out of bounds: row={row_idx}, capacity={}",
            self.logical_capacity()
        )))
    }

    #[inline]
    fn checked_destination_end(
        &self,
        dst_offset: usize,
        count: usize,
        label: &str,
    ) -> Result<usize> {
        dst_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "{label} destination overflow: dst_offset={dst_offset}, count={count}"
            ))
        })
    }

    #[inline]
    fn checked_source_end(src_offset: usize, count: usize, label: &str) -> Result<usize> {
        src_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "{label} source overflow: src_offset={src_offset}, count={count}"
            ))
        })
    }

    #[inline]
    pub(super) fn is_varlen_type(logical_type: &LogicalType) -> bool {
        logical_type.physical_type() == crate::types::PhysicalType::Varchar
    }

    #[inline]
    fn is_fixed_payload_type(logical_type: &LogicalType) -> bool {
        !Self::is_varlen_type(logical_type)
            && !matches!(
                logical_type,
                LogicalType::Array(_, _) | LogicalType::List(_) | LogicalType::Struct(_)
            )
            && logical_type.type_size() > 0
    }

    #[inline]
    pub(super) fn varlen_bytes<'a>(
        logical_type: &LogicalType,
        vector: &'a Vector,
        row_idx: usize,
    ) -> Option<&'a [u8]> {
        if matches!(logical_type, LogicalType::Blob) {
            vector.get_blob(row_idx)
        } else {
            vector.get_string(row_idx).map(str::as_bytes)
        }
    }

    #[inline]
    pub(super) fn copy_varlen_entry(
        bytes: &[u8],
        heap: &mut Option<StringHeap>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<StringView> {
        if let Some(value) = StringView::try_inline(bytes) {
            return Ok(value);
        }

        if heap.is_none() {
            *heap = Some(StringHeap::with_allocator(1024, allocator));
        }

        // SAFETY: the heap is installed alongside the returned entry in the
        // destination vector and therefore owns its out-of-line bytes.
        unsafe {
            heap.as_mut()
                .expect("invariant: heap was initialized")
                .try_add_blob(bytes)
        }
    }

    fn try_copy_flat_fixed_range(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        let element_size = self.logical_type.type_size();
        let bytes = element_size.checked_mul(count).ok_or_else(|| {
            paro_error::internal(format!(
                "copy range byte count overflow: element_size={element_size}, count={count}"
            ))
        })?;

        self.try_make_exclusive()?;
        self.validity.try_ensure_capacity(dst_offset + count)?;
        self.validity
            .try_copy_range_from(dst_offset, &source.validity, src_offset, count)?;

        unsafe {
            let src = source.buffer.data().add(src_offset * element_size);
            let dst = self.buffer.data().add(dst_offset * element_size);
            if ranges_overlap(src, dst, bytes) {
                std::ptr::copy(src, dst, bytes);
            } else {
                std::ptr::copy_nonoverlapping(src, dst, bytes);
            }
        }

        Ok(())
    }

    fn try_copy_dictionary_fixed_range(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        let child = source
            .child
            .as_ref()
            .expect("Dictionary vector missing child");
        let element_size = self.logical_type.type_size();

        self.try_make_exclusive()?;
        self.validity.try_ensure_capacity(dst_offset + count)?;

        unsafe {
            let src_base = child.buffer.data();
            let dst_base = self.buffer.data();
            for i in 0..count {
                let dst_idx = dst_offset + i;
                let physical_idx = source.selection.physical_index(src_offset + i);
                if child.is_null(physical_idx) {
                    self.validity.try_set_null(dst_idx)?;
                    continue;
                }

                self.validity.try_set_valid(dst_idx)?;
                std::ptr::copy_nonoverlapping(
                    src_base.add(physical_idx * element_size),
                    dst_base.add(dst_idx * element_size),
                    element_size,
                );
            }
        }

        Ok(())
    }

    fn try_copy_varlen_range_rebuild_heap(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        self.try_make_exclusive()?;
        self.validity.try_ensure_capacity(dst_offset + count)?;

        let mut heap: Option<StringHeap> = None;
        let allocator = self.buffer.allocator().clone();

        unsafe {
            let entries = self.buffer.data() as *mut StringView;
            for i in 0..count {
                let dst_idx = dst_offset + i;
                let src_idx = src_offset + i;
                if source.is_null(src_idx) {
                    self.validity.try_set_null(dst_idx)?;
                    *entries.add(dst_idx) = StringView::empty();
                    continue;
                }

                self.validity.try_set_valid(dst_idx)?;
                let bytes =
                    Self::varlen_bytes(&self.logical_type, source, src_idx).ok_or_else(|| {
                        paro_error::internal(format!(
                            "copy range missing varlen value at row {src_idx}"
                        ))
                    })?;
                *entries.add(dst_idx) =
                    Self::copy_varlen_entry(bytes, &mut heap, allocator.clone())?;
            }
        }

        self.string_heap = heap.map(Arc::new);
        Ok(())
    }

    fn try_copy_varlen_range_append_heap(
        &mut self,
        dst_offset: usize,
        source: &Vector,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        self.try_make_exclusive()?;
        self.validity.try_ensure_capacity(dst_offset + count)?;

        let allocator = self.buffer.allocator().clone();
        if self
            .string_heap
            .as_ref()
            .is_some_and(|heap| Arc::strong_count(heap) > 1)
        {
            let old_heap = Arc::clone(
                self.string_heap
                    .as_ref()
                    .expect("checked shared heap must exist"),
            );
            let mut rebuilt_heap = StringHeap::with_allocator(
                old_heap.allocation_size().max(self.count).max(1),
                allocator.clone(),
            );
            let rebuilt_buffer = super::VectorBuffer::try_with_allocator(
                StringView::SIZE,
                self.buffer.capacity(),
                allocator,
            )?;

            unsafe {
                let old_entries = self.buffer.data() as *const StringView;
                let new_entries = rebuilt_buffer.data() as *mut StringView;
                for row_idx in 0..self.count {
                    let entry = *old_entries.add(row_idx);
                    // SAFETY: `rebuilt_heap` becomes the owner of `new_entries`.
                    *new_entries.add(row_idx) = rebuilt_heap.try_add_blob(entry.as_bytes())?;
                }

                for i in 0..count {
                    let dst_idx = dst_offset + i;
                    let src_idx = src_offset + i;
                    if source.is_null(src_idx) {
                        self.validity.try_set_null(dst_idx)?;
                        *new_entries.add(dst_idx) = StringView::empty();
                        continue;
                    }

                    self.validity.try_set_valid(dst_idx)?;
                    let bytes = Self::varlen_bytes(&self.logical_type, source, src_idx)
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "copy range missing varlen value at row {src_idx}"
                            ))
                        })?;
                    // SAFETY: `rebuilt_heap` becomes the owner of `new_entries`.
                    *new_entries.add(dst_idx) = rebuilt_heap.try_add_blob(bytes)?;
                }
            }

            self.buffer = rebuilt_buffer;
            self.string_heap = if rebuilt_heap.is_empty() {
                None
            } else {
                Some(Arc::new(rebuilt_heap))
            };
            return Ok(());
        }

        unsafe {
            let entries = self.buffer.data() as *mut StringView;
            for i in 0..count {
                let dst_idx = dst_offset + i;
                let src_idx = src_offset + i;
                if source.is_null(src_idx) {
                    self.validity.try_set_null(dst_idx)?;
                    *entries.add(dst_idx) = StringView::empty();
                    continue;
                }

                self.validity.try_set_valid(dst_idx)?;
                let bytes =
                    Self::varlen_bytes(&self.logical_type, source, src_idx).ok_or_else(|| {
                        paro_error::internal(format!(
                            "copy range missing varlen value at row {src_idx}"
                        ))
                    })?;
                *entries.add(dst_idx) = if let Some(value) = StringView::try_inline(bytes) {
                    value
                } else {
                    let allocator = self.buffer.allocator().clone();
                    let heap = self.string_heap.get_or_insert_with(|| {
                        Arc::new(StringHeap::with_allocator(count.max(1), allocator))
                    });
                    // SAFETY: this heap is retained by the destination vector.
                    Arc::get_mut(heap)
                        .expect("varlen append heap should be unique after shared path")
                        .try_add_blob(bytes)?
                };
            }
        }
        Ok(())
    }

    fn destination_required_count(
        &self,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<usize> {
        match dst_rows {
            CopyDestinationRows::Range { offset } => {
                let end = self.checked_destination_end(offset, count, "copy rows")?;
                if end > self.logical_capacity() {
                    return Err(paro_error::internal(format!(
                        "copy rows destination out of bounds: end={end}, capacity={}",
                        self.logical_capacity()
                    )));
                }
                Ok(end)
            }
            CopyDestinationRows::Scatter(positions) => {
                let mut required = 0usize;
                for &position in positions.iter().take(count) {
                    self.check_destination_row(position)?;
                    required = required.max(position.checked_add(1).ok_or_else(|| {
                        paro_error::internal(format!(
                            "copy rows destination count overflow: position={position}"
                        ))
                    })?);
                }
                Ok(required)
            }
        }
    }

    fn validate_source_rows(
        &self,
        source: &Vector,
        src_rows: &CopySourceRows<'_>,
        count: usize,
    ) -> Result<()> {
        for logical_idx in 0..count {
            self.check_source_row(source, src_rows.source_index(logical_idx))?;
        }
        Ok(())
    }

    fn try_copy_parent_validity_rows(
        &mut self,
        source: &Vector,
        src_rows: &CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
        required_count: usize,
    ) -> Result<()> {
        self.validity.try_ensure_capacity(required_count)?;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            let dst_idx = dst_rows.destination_index(logical_idx);
            if source.is_null(src_idx) {
                self.validity.try_set_null(dst_idx)?;
            } else {
                self.validity.try_set_valid(dst_idx)?;
            }
        }
        Ok(())
    }

    fn checked_array_child_offset(row_idx: usize, array_size: usize, label: &str) -> Result<usize> {
        row_idx.checked_mul(array_size).ok_or_else(|| {
            paro_error::internal(format!(
                "{label} array child offset overflow: row_idx={row_idx}, array_size={array_size}"
            ))
        })
    }

    fn extend_or_flush_run(
        run: &mut Option<CopyRun>,
        dst_offset: usize,
        src_offset: usize,
        count: usize,
        dest_child: &mut Vector,
        src_child: &Vector,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        if let Some(existing) = run.as_mut() {
            let existing_dst_end =
                existing
                    .dst_offset
                    .checked_add(existing.count)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "copy run destination overflow: dst_offset={}, count={}",
                            existing.dst_offset, existing.count
                        ))
                    })?;
            let existing_src_end =
                existing
                    .src_offset
                    .checked_add(existing.count)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "copy run source overflow: src_offset={}, count={}",
                            existing.src_offset, existing.count
                        ))
                    })?;
            if existing_dst_end == dst_offset && existing_src_end == src_offset {
                existing.count = existing.count.checked_add(count).ok_or_else(|| {
                    paro_error::internal(format!(
                        "copy run count overflow: current={}, add={count}",
                        existing.count
                    ))
                })?;
                return Ok(());
            }

            Self::flush_copy_run(run, dest_child, src_child)?;
        }

        *run = Some(CopyRun {
            dst_offset,
            src_offset,
            count,
        });
        Ok(())
    }

    fn flush_copy_run(
        run: &mut Option<CopyRun>,
        dest_child: &mut Vector,
        src_child: &Vector,
    ) -> Result<()> {
        if let Some(run) = run.take() {
            dest_child.try_copy_range(run.dst_offset, src_child, run.src_offset, run.count)?;
        }
        Ok(())
    }

    fn try_copy_array_rows(
        &mut self,
        source: &Vector,
        src_rows: CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
        array_size: usize,
    ) -> Result<()> {
        let (source, src_rows) = Self::normalize_source_rows(source, src_rows)?;
        let required_count = self.destination_required_count(dst_rows, count)?;
        self.validate_source_rows(source, &src_rows, count)?;

        let src_child = source.child.as_ref().expect("Array vector missing child");
        let dest_child = self.child.as_mut().expect("Array vector missing child");
        let dest_child = Self::try_make_arc_mut(dest_child)?;

        let mut max_child_count = 0usize;
        let mut run = None;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            if source.is_null(src_idx) {
                continue;
            }
            let dst_idx = dst_rows.destination_index(logical_idx);
            let src_child_offset = Self::checked_array_child_offset(src_idx, array_size, "source")?;
            let dst_child_offset =
                Self::checked_array_child_offset(dst_idx, array_size, "destination")?;
            max_child_count = max_child_count.max(
                dst_child_offset
                    .checked_add(array_size)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "array destination child count overflow: offset={dst_child_offset}, array_size={array_size}"
                        ))
                    })?,
            );
            Self::extend_or_flush_run(
                &mut run,
                dst_child_offset,
                src_child_offset,
                array_size,
                dest_child,
                src_child,
            )?;
        }
        Self::flush_copy_run(&mut run, dest_child, src_child)?;
        if dest_child.len() < max_child_count {
            dest_child.try_set_count(max_child_count)?;
        }

        self.try_make_exclusive()?;
        self.try_copy_parent_validity_rows(source, &src_rows, dst_rows, count, required_count)?;
        if self.count < required_count {
            self.try_set_count(required_count)?;
        }
        Ok(())
    }

    fn ensure_list_child_capacity(&mut self, needed: usize) -> Result<()> {
        let child = self.child.as_ref().expect("List vector missing child");
        if needed <= child.logical_capacity() {
            return Ok(());
        }

        let old_child = Arc::clone(child);
        let dest_offset = old_child.len();
        let new_capacity = needed
            .max(old_child.logical_capacity().saturating_mul(2))
            .max(1);
        let mut new_child = Vector::try_new(
            old_child.logical_type.clone(),
            new_capacity,
            old_child.allocator().clone(),
        )?;
        if dest_offset > 0 {
            new_child.try_copy_range(0, &old_child, 0, dest_offset)?;
            new_child.try_set_count(dest_offset)?;
        }
        self.child = Some(Arc::new(new_child));
        Ok(())
    }

    fn list_child_row_count(
        &self,
        source: &Vector,
        src_rows: &CopySourceRows<'_>,
        count: usize,
    ) -> Result<usize> {
        let mut total = 0usize;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            self.check_source_row(source, src_idx)?;
            if source.is_null(src_idx) {
                continue;
            }
            let (_, length) = Self::read_list_entry(source, src_idx);
            total = total.checked_add(length).ok_or_else(|| {
                paro_error::internal(format!(
                    "list child copy count overflow: total={total}, add={length}"
                ))
            })?;
        }
        Ok(total)
    }

    fn write_list_parent_rows(
        &mut self,
        source: &Vector,
        src_rows: &CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
        required_count: usize,
        dest_child_start: usize,
    ) -> Result<()> {
        self.validity.try_ensure_capacity(required_count)?;
        let mut dest_child_offset = dest_child_start;
        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            let dst_idx = dst_rows.destination_index(logical_idx);
            if dest_child_offset > u32::MAX as usize {
                return Err(paro_error::internal(format!(
                    "list entry offset exceeds u32 range: offset={dest_child_offset}"
                )));
            }
            if source.is_null(src_idx) {
                self.validity.try_set_null(dst_idx)?;
                Self::write_list_entry(self, dst_idx, dest_child_offset as u32, 0);
                continue;
            }

            let (_, length) = Self::read_list_entry(source, src_idx);
            if length > u32::MAX as usize {
                return Err(paro_error::internal(format!(
                    "list entry exceeds u32 range: offset={dest_child_offset}, length={length}"
                )));
            }
            self.validity.try_set_valid(dst_idx)?;
            Self::write_list_entry(self, dst_idx, dest_child_offset as u32, length as u32);
            dest_child_offset = dest_child_offset.checked_add(length).ok_or_else(|| {
                paro_error::internal(format!(
                    "list destination child offset overflow: offset={dest_child_offset}, length={length}"
                ))
            })?;
        }
        Ok(())
    }

    fn copy_list_child_runs(
        dest_child: &mut Vector,
        source: &Vector,
        src_child: &Vector,
        src_rows: &CopySourceRows<'_>,
        count: usize,
        dest_child_start: usize,
    ) -> Result<()> {
        let mut dest_child_offset = dest_child_start;
        let mut run = None;

        for logical_idx in 0..count {
            let src_idx = src_rows.source_index(logical_idx);
            if source.is_null(src_idx) {
                continue;
            }

            let (src_offset, length) = Self::read_list_entry(source, src_idx);
            Self::extend_or_flush_run(
                &mut run,
                dest_child_offset,
                src_offset,
                length,
                dest_child,
                src_child,
            )?;
            dest_child_offset = dest_child_offset.checked_add(length).ok_or_else(|| {
                paro_error::internal(format!(
                    "list destination child offset overflow: offset={dest_child_offset}, length={length}"
                ))
            })?;
        }

        Self::flush_copy_run(&mut run, dest_child, src_child)?;
        if dest_child.len() < dest_child_offset {
            dest_child.try_set_count(dest_child_offset)?;
        }
        Ok(())
    }

    fn check_list_scatter_overwrite(
        &self,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<()> {
        let CopyDestinationRows::Scatter(positions) = dst_rows else {
            return Ok(());
        };

        for &position in positions.iter().take(count) {
            if position < self.count {
                return Err(paro_error::internal(format!(
                    "list scatter cannot overwrite initialized row {position}; reset the destination batch first"
                )));
            }
        }
        Ok(())
    }

    fn try_copy_list_rows(
        &mut self,
        source: &Vector,
        src_rows: CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<()> {
        let (source, src_rows) = Self::normalize_source_rows(source, src_rows)?;
        let required_count = self.destination_required_count(dst_rows, count)?;
        self.check_list_scatter_overwrite(dst_rows, count)?;

        let total_child_rows = self.list_child_row_count(source, &src_rows, count)?;
        let dest_child_start = self
            .child
            .as_ref()
            .expect("List vector missing child")
            .len();
        let needed_child_count = dest_child_start
            .checked_add(total_child_rows)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "list destination child count overflow: start={dest_child_start}, add={total_child_rows}"
                ))
            })?;
        self.ensure_list_child_capacity(needed_child_count)?;

        self.try_make_exclusive()?;
        self.write_list_parent_rows(
            source,
            &src_rows,
            dst_rows,
            count,
            required_count,
            dest_child_start,
        )?;

        let src_child = source.child.as_ref().expect("List vector missing child");
        let dest_child =
            Self::try_make_arc_mut(self.child.as_mut().expect("List vector missing child"))?;
        Self::copy_list_child_runs(
            dest_child,
            source,
            src_child,
            &src_rows,
            count,
            dest_child_start,
        )?;

        if self.count < required_count {
            self.try_set_count(required_count)?;
        }
        Ok(())
    }

    fn try_copy_struct_rows(
        &mut self,
        source: &Vector,
        src_rows: CopySourceRows<'_>,
        dst_rows: CopyDestinationRows<'_>,
        count: usize,
    ) -> Result<()> {
        let (source, src_rows) = Self::normalize_source_rows(source, src_rows)?;
        let required_count = self.destination_required_count(dst_rows, count)?;
        self.validate_source_rows(source, &src_rows, count)?;

        let src_children = source.children().expect("Struct vector missing children");
        if self.children.len() != src_children.len() {
            panic!(
                "Struct child count mismatch: dest={}, src={}",
                self.children.len(),
                src_children.len()
            );
        }

        for (dest_child, src_child) in self.children.iter_mut().zip(src_children.iter()) {
            let dest_child = Self::try_make_arc_mut(dest_child)?;
            dest_child.try_copy_rows(src_child, src_rows.clone(), dst_rows, count)?;
        }

        self.try_make_exclusive()?;
        self.try_copy_parent_validity_rows(source, &src_rows, dst_rows, count, required_count)?;
        if self.count < required_count {
            self.try_set_count(required_count)?;
        }
        Ok(())
    }

    fn try_copy_array_at(
        &mut self,
        idx: usize,
        source: ResolvedCopySource<'_>,
        array_size: usize,
    ) -> Result<()> {
        if let (Some(dest_child), Some(src_child)) = (&mut self.child, source.vector.child.as_ref())
        {
            let dest_child = Self::try_make_arc_mut(dest_child)?;
            let dest_offset = idx.checked_mul(array_size).ok_or_else(|| {
                paro_error::internal(format!(
                    "array destination offset overflow: idx={idx}, array_size={array_size}"
                ))
            })?;
            let src_offset = source.row_idx.checked_mul(array_size).ok_or_else(|| {
                paro_error::internal(format!(
                    "array source offset overflow: idx={}, array_size={array_size}",
                    source.row_idx
                ))
            })?;
            let required_child_count = dest_offset.checked_add(array_size).ok_or_else(|| {
                paro_error::internal(format!(
                    "array destination child count overflow: offset={dest_offset}, array_size={array_size}"
                ))
            })?;
            dest_child.try_copy_range(dest_offset, src_child, src_offset, array_size)?;
            if dest_child.len() < required_child_count {
                dest_child.try_set_count(required_child_count)?;
            }
        }
        self.try_set_null(idx, false)
    }

    fn try_copy_list_at(&mut self, idx: usize, source: ResolvedCopySource<'_>) -> Result<()> {
        let (src_offset, src_length) = Self::read_list_entry(source.vector, source.row_idx);
        let src_child = source
            .vector
            .child
            .as_ref()
            .expect("List vector missing child");

        let (dest_offset, dest_capacity) = {
            let child = self.child.as_ref().expect("List vector missing child");
            (child.len(), child.logical_capacity())
        };

        let needed = dest_offset.checked_add(src_length).ok_or_else(|| {
            paro_error::internal(format!(
                "list child length overflow: dest_offset={dest_offset}, src_length={src_length}"
            ))
        })?;
        if needed > dest_capacity {
            self.ensure_list_child_capacity(needed)?;
        }

        let dest_child =
            Self::try_make_arc_mut(self.child.as_mut().expect("List vector missing child"))?;
        dest_child.try_copy_range(dest_offset, src_child, src_offset, src_length)?;
        dest_child.try_set_count(needed)?;

        if dest_offset > u32::MAX as usize || src_length > u32::MAX as usize {
            return Err(paro_error::internal(format!(
                "list entry exceeds u32 range: offset={dest_offset}, length={src_length}"
            )));
        }
        Self::write_list_entry(self, idx, dest_offset as u32, src_length as u32);
        self.try_set_null(idx, false)
    }

    fn try_copy_struct_at(&mut self, idx: usize, source: ResolvedCopySource<'_>) -> Result<()> {
        let src_children = source
            .vector
            .children()
            .expect("Struct vector missing children");

        if self.children.len() != src_children.len() {
            panic!(
                "Struct child count mismatch: dest={}, src={}",
                self.children.len(),
                src_children.len()
            );
        }

        for (dest_child, src_child) in self.children.iter_mut().zip(src_children.iter()) {
            let dest_child = Self::try_make_arc_mut(dest_child)?;
            dest_child.try_copy_at(idx, src_child, source.row_idx)?;
            dest_child.try_set_count(idx + 1)?;
        }

        self.try_set_null(idx, false)
    }

    fn copy_fixed_value_at(&mut self, idx: usize, source: ResolvedCopySource<'_>) {
        let size = self.logical_type.type_size();
        unsafe {
            let dest_ptr = self.buffer.data().add(idx * size);
            match source.vector.vector_type() {
                VectorType::Flat | VectorType::Constant => {
                    let src_ptr = source.vector.buffer.data().add(source.row_idx * size);
                    std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, size);
                }
                VectorType::Sequence => {
                    // Sequence vectors only exist for i64.
                    if let Some(val) = source.vector.get_i64(source.row_idx) {
                        *(dest_ptr as *mut i64) = val;
                    }
                }
                VectorType::Dictionary => unreachable!("dictionary sources are resolved first"),
            }
        }
    }

    fn read_list_entry(vector: &Vector, idx: usize) -> (usize, usize) {
        let entry_base = vector.buffer.data();
        let entry_ptr = unsafe { entry_base.add(idx * 8) as *const u32 };
        let offset = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
        let length = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };
        (offset, length)
    }

    fn write_list_entry(vector: &mut Vector, idx: usize, offset: u32, length: u32) {
        let entry_base = vector.buffer.data();
        let entry_ptr = unsafe { entry_base.add(idx * 8) as *mut u32 };
        unsafe {
            std::ptr::write_unaligned(entry_ptr, offset);
            std::ptr::write_unaligned(entry_ptr.add(1), length);
        }
    }
}
