// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{AllocationSet, VectorBuffer};
use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static SELECTION_MATERIALIZATION_COUNT: AtomicU64 = AtomicU64::new(0);

struct OwnedNativeIndices(Vec<u32>);

impl AsRef<[u8]> for OwnedNativeIndices {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: every byte of every u32 in the Vec is initialized, and the
        // byte slice is tied to the immutable owner retained by `Bytes`.
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr().cast::<u8>(),
                self.0.len() * std::mem::size_of::<u32>(),
            )
        }
    }
}

#[inline]
pub fn selection_materialization_count() -> u64 {
    SELECTION_MATERIALIZATION_COUNT.load(Ordering::Relaxed)
}

#[inline]
pub fn reset_selection_materialization_count() {
    SELECTION_MATERIALIZATION_COUNT.store(0, Ordering::Relaxed);
}

#[inline]
fn record_selection_materialization() {
    SELECTION_MATERIALIZATION_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Selection vector - maps logical indices to physical indices.
///
/// Uses `VectorBuffer` for memory management and supports shallow clones (Zero-copy).
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Buffer holding u32 indices
    buffer: VectorBuffer,
    /// Number of indices
    count: usize,
}

impl SelectionVector {
    /// Create a deep copy of this selection vector.
    pub fn try_deep_copy(&self) -> Result<Self> {
        Ok(Self {
            buffer: self.buffer.try_deep_copy()?,
            count: self.count,
        })
    }

    /// Ensure the selection vector is exclusively owned (Copy-on-Write).
    pub fn try_make_exclusive(&mut self) -> Result<()> {
        self.buffer.try_make_exclusive()
    }

    /// Create with custom allocator.
    pub fn try_with_capacity(capacity: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        Ok(Self {
            buffer: VectorBuffer::try_with_allocator(
                std::mem::size_of::<u32>(),
                capacity,
                allocator,
            )?,
            count: 0,
        })
    }

    /// Create incremental selection (0, 1, 2, ...).
    pub fn try_incremental(count: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut sv = Self::try_with_capacity(count, allocator)?;
        sv.count = count;

        // SAFETY: We allocated space for `count` u32s.
        unsafe {
            let ptr = sv.buffer.data() as *mut u32;
            for i in 0..count {
                *ptr.add(i) = i as u32;
            }
        }
        Ok(sv)
    }

    /// Create constant selection (all zeros) for constant vectors.
    /// All indices point to position 0.
    pub fn try_constant(count: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        Self::try_repeated(0, count, allocator)
    }

    /// Create a repeated selection where all logical rows point to `index`.
    pub fn try_repeated(index: usize, count: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut sv = Self::try_with_capacity(count, allocator)?;
        sv.count = count;

        // SAFETY: We allocated space for `count` u32s.
        unsafe {
            let ptr = sv.buffer.data() as *mut u32;
            for i in 0..count {
                *ptr.add(i) = index as u32;
            }
        }
        Ok(sv)
    }

    /// Create from indices.
    pub fn try_from_indices(indices: Vec<u32>, allocator: Arc<dyn Allocator>) -> Result<Self> {
        let count = indices.len();
        let mut sv = Self::try_with_capacity(count, allocator)?;
        sv.count = count;

        if count > 0 {
            // SAFETY: We allocated space for `count` u32s.
            unsafe {
                let dst = sv.buffer.data() as *mut u32;
                std::ptr::copy_nonoverlapping(indices.as_ptr(), dst, count);
            }
        }
        Ok(sv)
    }

    /// Adopt owned native-endian u32 indices without copying them.
    /// Mutation follows the normal copy-on-write path.
    pub fn try_from_owned_indices(
        indices: Vec<u32>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let count = indices.len();
        if count == 0 {
            return Self::try_with_capacity(0, allocator);
        }
        Self::try_from_native_bytes(
            Bytes::from_owner(OwnedNativeIndices(indices)),
            count,
            allocator,
        )
    }

    /// Adopt an immutable native-endian u32 selection buffer without copying.
    /// Mutation follows the normal copy-on-write path.
    pub fn try_from_native_bytes(
        bytes: Bytes,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self {
            buffer: VectorBuffer::try_from_bytes(
                std::mem::size_of::<u32>(),
                count,
                bytes,
                allocator,
            )?,
            count,
        })
    }

    /// Get index at position.
    #[inline]
    pub fn get(&self, idx: usize) -> usize {
        debug_assert!(idx < self.count, "Index out of bounds");
        // SAFETY: Bounds checked in debug mode.
        unsafe {
            let ptr = self.buffer.data() as *const u32;
            (*ptr.add(idx)) as usize
        }
    }

    /// Set index at position.
    #[inline]
    pub fn try_set(&mut self, idx: usize, value: usize) -> Result<()> {
        debug_assert!(idx < self.count, "Index out of bounds");
        self.try_make_exclusive()?;
        // SAFETY: Bounds checked in debug mode.
        unsafe {
            let ptr = self.buffer.data() as *mut u32;
            *ptr.add(idx) = value as u32;
        }
        Ok(())
    }

    #[inline]
    pub fn set(&mut self, idx: usize, value: usize) {
        self.try_set(idx, value)
            .expect("selection vector copy-on-write allocation failed");
    }

    pub(crate) fn try_fill_offset_from(
        &mut self,
        offset: usize,
        selection: &SelectionVector,
        count: usize,
    ) -> Result<()> {
        debug_assert!(count <= self.count, "Output selection length is too small");
        debug_assert!(
            count <= selection.len(),
            "Input selection length is too small"
        );
        self.try_make_exclusive()?;
        unsafe {
            let dst = self.buffer.data() as *mut u32;
            for i in 0..count {
                *dst.add(i) = (offset + selection.get(i)) as u32;
            }
        }
        Ok(())
    }

    /// Length of selection vector.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Allocated index capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn allocation_identity(&self) -> Option<usize> {
        self.buffer.allocation_identity()
    }

    /// Whether the backing index buffer can be mutated without copy-on-write.
    ///
    /// Runtime scratch pools use this to avoid taking an exclusive copy of a
    /// selection that is still referenced by a zero-copy dictionary result.
    #[inline]
    pub fn is_uniquely_owned(&self) -> bool {
        self.buffer.is_uniquely_owned()
    }

    pub fn allocation_size(&self) -> usize {
        self.buffer.size()
    }

    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        self.buffer.allocator()
    }

    pub(crate) fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        self.buffer.collect_allocation_size(allocations)
    }

    pub(crate) fn collect_allocation_entries(&self, entries: &mut Vec<(usize, usize)>) {
        self.buffer.collect_allocation_entries(entries);
    }

    /// Set the length of the selection vector.
    pub fn set_len(&mut self, count: usize) {
        self.count = count;
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get raw slice of indices.
    pub fn as_slice(&self) -> &[u32] {
        // SAFETY: buffer contains valid u32 data up to self.count
        unsafe { std::slice::from_raw_parts(self.buffer.data() as *const u32, self.count) }
    }

    /// Get mutable raw slice of indices.
    pub fn as_mut_slice(&mut self) -> &mut [u32] {
        self.try_make_exclusive()
            .expect("selection vector copy-on-write allocation failed");
        // SAFETY: buffer contains valid u32 data up to self.count
        unsafe { std::slice::from_raw_parts_mut(self.buffer.data() as *mut u32, self.count) }
    }

    /// Merge this selection vector with another one.
    /// Resulting selection maps 0..other.count -> physical indices.
    /// new_indices[i] = self.get(other.get(i))
    pub fn try_slice(&self, other: &SelectionVector, count: usize) -> Result<SelectionVector> {
        record_selection_materialization();
        let mut result =
            SelectionVector::try_with_capacity(count, self.buffer.allocator().clone())?;
        result.count = count;

        unsafe {
            let self_ptr = self.buffer.data() as *const u32;
            let other_ptr = other.buffer.data() as *const u32;
            let res_ptr = result.buffer.data() as *mut u32;

            for i in 0..count {
                let idx_in_self = *other_ptr.add(i) as usize;
                *res_ptr.add(i) = *self_ptr.add(idx_in_self);
            }
        }
        Ok(result)
    }

    /// Slice a contiguous logical range from this selection vector.
    pub fn try_slice_range(&self, offset: usize, count: usize) -> Result<SelectionVector> {
        record_selection_materialization();
        debug_assert!(
            offset + count <= self.count,
            "selection range out of bounds"
        );
        let mut result =
            SelectionVector::try_with_capacity(count, self.buffer.allocator().clone())?;
        result.count = count;

        if count > 0 {
            unsafe {
                let src = (self.buffer.data() as *const u32).add(offset);
                let dst = result.buffer.data() as *mut u32;
                std::ptr::copy_nonoverlapping(src, dst, count);
            }
        }
        Ok(result)
    }
}

impl From<&SelectionVector> for SelectionVector {
    fn from(sel: &SelectionVector) -> Self {
        sel.clone()
    }
}

/// Logical-to-physical row mapping carried by dictionary vectors and decoded
/// views.
///
/// `Range` is intentionally first-class so hot range slices can compose without
/// allocating a materialized `SelectionVector`.
#[derive(Debug, Clone, Default)]
pub enum VectorSelection {
    #[default]
    None,
    Materialized(SelectionVector),
    Range {
        offset: usize,
        count: usize,
    },
}

/// A logical-to-physical mapping proven to fit one exact child cardinality.
///
/// Fields are private so the proof can only be established by the validating
/// constructor. The value is cheaply cloneable and may be applied to several
/// same-sized column vectors in one batch without rescanning every index.
#[derive(Debug, Clone)]
pub struct ValidatedVectorSelection {
    pub(super) selection: VectorSelection,
    pub(super) child_count: usize,
}

impl ValidatedVectorSelection {
    pub fn try_new(selection: impl Into<VectorSelection>, child_count: usize) -> Result<Self> {
        let selection = selection.into();
        selection.validate_child_bounds(child_count)?;
        Ok(Self {
            selection,
            child_count,
        })
    }

    pub fn len(&self) -> usize {
        self.selection.len()
    }

    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }
}

impl VectorSelection {
    #[inline]
    pub fn none() -> Self {
        Self::None
    }

    #[inline]
    pub fn materialized(selection: SelectionVector) -> Self {
        Self::Materialized(selection)
    }

    #[inline]
    pub fn range(offset: usize, count: usize) -> Self {
        Self::Range { offset, count }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Materialized(sel) => sel.len(),
            Self::Range { count, .. } => *count,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Validate that every logical row resolves inside a child vector.
    /// Dictionary vectors establish this invariant at construction so decoded
    /// readers can use their physical mapping without repeated bounds checks.
    pub(crate) fn validate_child_bounds(&self, child_count: usize) -> Result<()> {
        let invalid = match self {
            Self::None => None,
            Self::Materialized(selection) => selection
                .as_slice()
                .iter()
                .copied()
                .find(|index| *index as usize >= child_count)
                .map(|index| index as usize),
            Self::Range { offset, count } => offset
                .checked_add(*count)
                .filter(|end| *end <= child_count)
                .is_none()
                .then_some(*offset),
        };
        if let Some(index) = invalid {
            return Err(paro_error::out_of_range(format!(
                "dictionary selection index {index} is outside child cardinality {child_count}"
            )));
        }
        Ok(())
    }

    #[inline]
    pub fn as_materialized(&self) -> Option<&SelectionVector> {
        match self {
            Self::Materialized(sel) => Some(sel),
            Self::None | Self::Range { .. } => None,
        }
    }

    #[inline]
    pub fn physical_index(&self, idx: usize) -> usize {
        match self {
            Self::None => idx,
            Self::Materialized(sel) => sel.get(idx),
            Self::Range { offset, .. } => offset + idx,
        }
    }

    #[inline]
    pub fn allocation_identity(&self) -> Option<usize> {
        match self {
            Self::Materialized(sel) => sel.allocation_identity(),
            Self::None | Self::Range { .. } => None,
        }
    }

    pub fn try_make_exclusive(&mut self) -> Result<()> {
        if let Self::Materialized(sel) = self {
            sel.try_make_exclusive()?;
        }
        Ok(())
    }

    pub fn collect_allocation_size(&self, allocations: &mut super::AllocationSet) -> usize {
        match self {
            Self::Materialized(sel) => sel.collect_allocation_size(allocations),
            Self::None | Self::Range { .. } => 0,
        }
    }

    pub fn collect_allocation_entries(&self, entries: &mut Vec<(usize, usize)>) {
        if let Self::Materialized(sel) = self {
            sel.collect_allocation_entries(entries);
        }
    }

    pub fn try_materialize(&self, allocator: Arc<dyn Allocator>) -> Result<SelectionVector> {
        match self {
            Self::None => SelectionVector::try_incremental(0, allocator),
            Self::Materialized(sel) => Ok(sel.clone()),
            Self::Range { offset, count } => {
                record_selection_materialization();
                let mut selection = SelectionVector::try_with_capacity(*count, allocator)?;
                selection.set_len(*count);
                unsafe {
                    let ptr = selection.buffer.data() as *mut u32;
                    for i in 0..*count {
                        *ptr.add(i) = (*offset + i) as u32;
                    }
                }
                Ok(selection)
            }
        }
    }

    pub fn try_compose(&self, selection: VectorSelection) -> Result<VectorSelection> {
        match (self, selection) {
            (Self::None, selection) => Ok(selection),
            (Self::Materialized(child), Self::Materialized(sel)) => {
                Ok(Self::Materialized(child.try_slice(&sel, sel.len())?))
            }
            (Self::Materialized(child), Self::Range { offset, count }) => {
                Ok(Self::Materialized(child.try_slice_range(offset, count)?))
            }
            (Self::Range { offset, .. }, Self::Materialized(sel)) => {
                record_selection_materialization();
                let mut result =
                    SelectionVector::try_with_capacity(sel.len(), sel.allocator().clone())?;
                result.set_len(sel.len());
                unsafe {
                    let dst = result.buffer.data() as *mut u32;
                    for i in 0..sel.len() {
                        *dst.add(i) = (offset + sel.get(i)) as u32;
                    }
                }
                Ok(Self::Materialized(result))
            }
            (
                Self::Range { offset, .. },
                Self::Range {
                    offset: child_offset,
                    count,
                },
            ) => Ok(Self::Range {
                offset: offset + child_offset,
                count,
            }),
            (Self::Materialized(child), Self::None) => Ok(Self::Materialized(child.clone())),
            (Self::Range { offset, count }, Self::None) => Ok(Self::Range {
                offset: *offset,
                count: *count,
            }),
        }
    }
}

impl From<SelectionVector> for VectorSelection {
    fn from(selection: SelectionVector) -> Self {
        Self::Materialized(selection)
    }
}

impl From<&SelectionVector> for VectorSelection {
    fn from(selection: &SelectionVector) -> Self {
        Self::Materialized(selection.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::default_allocator;

    #[test]
    fn owned_indices_are_adopted_and_mutate_with_copy_on_write() {
        let indices = vec![3_u32, 1, 4, 1];
        let original_address = indices.as_ptr() as usize;
        let mut selection =
            SelectionVector::try_from_owned_indices(indices, Arc::new(default_allocator()))
                .unwrap();

        assert_eq!(selection.buffer.data() as usize, original_address);
        assert_eq!(
            (0..selection.len())
                .map(|idx| selection.get(idx))
                .collect::<Vec<_>>(),
            vec![3, 1, 4, 1]
        );

        selection.try_set(0, 9).unwrap();
        assert_ne!(selection.buffer.data() as usize, original_address);
        assert_eq!(selection.get(0), 9);
    }
}
