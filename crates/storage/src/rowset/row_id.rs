// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Strong row-coordinate domains used by segment scans.
//!
//! Predicate kernels produce ordinals relative to one vector-sized batch,
//! while column gathers consume absolute row IDs within a segment. Both fit in
//! `u32`, but they are not interchangeable. Transparent wrappers preserve the
//! compact representation while keeping domain conversions explicit.

use std::mem::ManuallyDrop;

use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;

use super::rowset_meta::RowsetId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BatchRowOrdinal(u32);

impl BatchRowOrdinal {
    /// Construct an ordinal after the enclosing entry point has validated the
    /// vector-sized batch. Keeping this check in debug builds catches future
    /// batch-strategy drift without adding a branch to every hot loop lane.
    #[inline]
    pub(crate) fn from_validated_index(index: usize) -> Self {
        debug_assert!(index < VECTOR_SIZE);
        debug_assert!(u32::try_from(index).is_ok());
        Self(index as u32)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline]
    pub(crate) fn checked_sub(self, base: Self) -> Option<Self> {
        self.0.checked_sub(base.0).map(Self)
    }

    #[inline]
    pub(crate) fn checked_add(self, base: Self) -> Option<Self> {
        let ordinal = self.0.checked_add(base.0)?;
        (ordinal < VECTOR_SIZE as u32).then_some(Self(ordinal))
    }

    /// Convert an owned typed selection to the common vector selection ABI.
    ///
    /// SAFETY is centralized here: `repr(transparent)` guarantees identical
    /// element layout and every `u32` bit pattern is valid for both types.
    pub(crate) fn into_raw_vec(values: Vec<Self>) -> Vec<u32> {
        let mut values = ManuallyDrop::new(values);
        unsafe {
            Vec::from_raw_parts(
                values.as_mut_ptr().cast::<u32>(),
                values.len(),
                values.capacity(),
            )
        }
    }

    /// Borrow a typed batch-ordinal view over the vector selection ABI.
    ///
    /// The caller must already have validated the indices against this exact
    /// vector-sized child domain. `repr(transparent)` makes the view zero-cost.
    pub(crate) fn from_validated_raw_slice(values: &[u32]) -> &[Self] {
        debug_assert!(values.iter().all(|value| *value < VECTOR_SIZE as u32));
        unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<Self>(), values.len()) }
    }
}

/// Index into a compact candidate array. Predicate evaluation of a gathered
/// column produces batch ordinals in this temporary domain; the conversion is
/// explicit so those values cannot be mistaken for original batch ordinals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct CandidateIndex(u32);

impl CandidateIndex {
    #[inline]
    pub(crate) fn from_gathered_ordinal(ordinal: BatchRowOrdinal, candidate_count: usize) -> Self {
        debug_assert!(ordinal.index() < candidate_count);
        Self(ordinal.get())
    }

    #[inline]
    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

#[cfg(test)]
impl PartialEq<u32> for BatchRowOrdinal {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

/// Validate the invariant shared by every predicate selection kernel.
#[inline]
pub(crate) fn validate_predicate_batch_rows(rows: usize) -> Result<()> {
    if rows > VECTOR_SIZE {
        return Err(paro_error::internal(format!(
            "predicate batch contains {rows} rows, exceeding VECTOR_SIZE {VECTOR_SIZE}",
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SegmentRowId(u32);

impl SegmentRowId {
    #[inline]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn try_from_ordinal(value: u64) -> Result<Self> {
        u32::try_from(value)
            .map(Self)
            .map_err(|_| paro_error::data_corrupted("row id exceeds the segment domain"))
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Borrow the storage ABI view without losing the typed owner.
    pub(crate) fn as_raw_slice(values: &[Self]) -> &[u32] {
        unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u32>(), values.len()) }
    }

    /// Borrow the typed segment-row domain without copying an existing storage
    /// ABI slice. `repr(transparent)` guarantees the element layout and every
    /// `u32` bit pattern is a valid segment row ID.
    #[cfg(test)]
    pub(crate) fn from_raw_slice(values: &[u32]) -> &[Self] {
        unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<Self>(), values.len()) }
    }
}

impl std::fmt::Display for SegmentRowId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable physical row location shared by tablet, search, mutation, and
/// primary-key paths. There is exactly one representation of a segment-local
/// row coordinate in the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalRowRef {
    pub rowset_id: RowsetId,
    pub segment_id: u32,
    pub row_offset: SegmentRowId,
}

impl PhysicalRowRef {
    pub const fn new(rowset_id: RowsetId, segment_id: u32, row_offset: SegmentRowId) -> Self {
        Self {
            rowset_id,
            segment_id,
            row_offset,
        }
    }

    pub const fn segment_key(self) -> (RowsetId, u32) {
        (self.rowset_id, self.segment_id)
    }
}

impl From<(RowsetId, u32, SegmentRowId)> for PhysicalRowRef {
    fn from(value: (RowsetId, u32, SegmentRowId)) -> Self {
        Self::new(value.0, value.1, value.2)
    }
}

#[cfg(test)]
impl PartialEq<u32> for SegmentRowId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_row_domains_keep_the_u32_layout() {
        assert_eq!(std::mem::size_of::<BatchRowOrdinal>(), 4);
        assert_eq!(std::mem::align_of::<BatchRowOrdinal>(), 4);
        assert_eq!(std::mem::size_of::<SegmentRowId>(), 4);
        assert_eq!(std::mem::align_of::<SegmentRowId>(), 4);
    }

    #[test]
    fn owned_abi_conversion_reuses_the_allocation() {
        let values = vec![
            BatchRowOrdinal::from_validated_index(1),
            BatchRowOrdinal::from_validated_index(3),
        ];
        let pointer = values.as_ptr().cast::<u32>();
        let raw = BatchRowOrdinal::into_raw_vec(values);
        assert_eq!(raw, vec![1, 3]);
        assert_eq!(raw.as_ptr(), pointer);
    }
}
