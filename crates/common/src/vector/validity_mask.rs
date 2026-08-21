// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{AllocationSet, VectorBuffer};
use crate::allocator::{default_allocator, Allocator};
use crate::error::{self as paro_error, Result};
use crate::vector::{SelectionVector, VectorSelection, VECTOR_SIZE};
use std::fmt;
use std::sync::Arc;

/// Number of bits per validity entry (u64)
pub const BITS_PER_VALUE: usize = 64;

/// Maximum validity entry value (all bits set to 1)
pub const MAX_ENTRY: u64 = u64::MAX;

/// Validity bitmap for null tracking.
/// Each bit represents whether the corresponding value is valid (non-null).
///
pub struct ValidityMask {
    /// Bitmask where 1 = valid, 0 = null
    bits: Option<VectorBuffer>,
    /// Number of elements tracked (capacity)
    capacity: usize,
    /// Allocator for this mask
    allocator: Arc<dyn Allocator>,
}

impl fmt::Debug for ValidityMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidityMask")
            .field("bits", &self.bits)
            .field("capacity", &self.capacity)
            .field("allocator", &self.allocator.name())
            .finish()
    }
}

impl Clone for ValidityMask {
    fn clone(&self) -> Self {
        Self {
            bits: self.bits.clone(),
            capacity: self.capacity,
            allocator: self.allocator.clone(),
        }
    }
}

impl ValidityMask {
    #[inline]
    fn assert_in_bounds(&self, op: &str, row_idx: usize) {
        debug_assert!(
            row_idx < self.capacity,
            "ValidityMask::{op} - row_idx {} out of range for capacity {}",
            row_idx,
            self.capacity
        );
    }

    // --- Constructors ---

    /// Create a new validity mask with specified capacity and all values valid.
    ///
    /// NOTE: This convenience constructor uses `default_allocator()` and is mainly
    /// intended for tests or standalone utility code. Production paths should pass
    /// an explicit allocator via `with_allocator`.
    pub fn new(capacity: usize) -> Self {
        Self::with_allocator(capacity, Arc::new(default_allocator()))
    }

    /// Create a new validity mask with specific allocator.
    pub fn with_allocator(capacity: usize, allocator: Arc<dyn Allocator>) -> Self {
        Self {
            bits: None,
            capacity,
            allocator,
        }
    }

    // --- Static Helper Methods ---

    /// Calculate the number of entries needed for a given count.
    #[inline]
    pub fn entry_count(count: usize) -> usize {
        count.div_ceil(BITS_PER_VALUE)
    }

    /// Calculate the size in bytes for a validity mask with given count.
    #[inline]
    pub fn validity_mask_size(count: usize) -> usize {
        Self::entry_count(count) * std::mem::size_of::<u64>()
    }

    /// Check if count is aligned to BITS_PER_VALUE.
    #[inline]
    pub fn is_aligned(count: usize) -> bool {
        count.is_multiple_of(BITS_PER_VALUE)
    }

    /// Get entry index and bit index for a given row index.
    #[inline]
    pub fn get_entry_index(row_idx: usize) -> (usize, usize) {
        (row_idx / BITS_PER_VALUE, row_idx % BITS_PER_VALUE)
    }

    /// Check if a specific row is valid within an entry.
    #[inline]
    pub fn row_is_valid_in_entry(entry: u64, idx_in_entry: usize) -> bool {
        (entry & (1u64 << idx_in_entry)) != 0
    }

    /// Check if all bits in an entry are valid.
    #[inline]
    pub fn all_valid_entry(entry: u64) -> bool {
        entry == MAX_ENTRY
    }

    /// Check if no bits in an entry are valid.
    #[inline]
    pub fn none_valid_entry(entry: u64) -> bool {
        entry == 0
    }

    /// Get an entry with first n bits set as valid.
    #[inline]
    pub fn entry_with_valid_bits(n: usize) -> u64 {
        if n == 0 {
            0
        } else if n >= BITS_PER_VALUE {
            MAX_ENTRY
        } else {
            MAX_ENTRY >> (BITS_PER_VALUE - n)
        }
    }

    // --- Instance Methods - Basic Access ---

    /// Check if all values are valid (no nulls).
    #[inline]
    pub fn all_valid(&self) -> bool {
        self.bits.is_none()
    }

    /// Check if the validity mask data is set (has been initialized with specific values).
    #[inline]
    pub fn is_mask_set(&self) -> bool {
        self.bits.is_some()
    }

    /// Returns the capacity of the mask.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of tracked rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.capacity
    }

    /// Returns true if capacity is 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.capacity == 0
    }

    /// Get raw pointer to validity data (can be null if all valid).
    pub fn get_data(&self) -> Option<*const u64> {
        self.bits
            .as_ref()
            .map(|b| unsafe { b.as_slice::<u64>(Self::entry_count(self.capacity)).as_ptr() })
    }

    /// Get mutable raw pointer to validity data.
    pub fn get_data_mut(&mut self) -> Option<*mut u64> {
        self.ensure_writable();
        self.bits.as_mut().map(|b| unsafe {
            b.as_mut_slice::<u64>(Self::entry_count(self.capacity))
                .as_mut_ptr()
        })
    }

    /// Get a validity entry at the given index.
    #[inline]
    pub fn get_validity_entry(&self, entry_idx: usize) -> u64 {
        if let Some(bits) = &self.bits {
            unsafe {
                let slice = bits.as_slice::<u64>(Self::entry_count(self.capacity));
                slice[entry_idx]
            }
        } else {
            MAX_ENTRY
        }
    }

    /// Get a validity entry at the given index (unsafe version, assumes mask is set).
    #[inline]
    pub fn get_validity_entry_unsafe(&self, entry_idx: usize) -> u64 {
        debug_assert!(self.bits.is_some());
        unsafe {
            let bits = self
                .bits
                .as_ref()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_slice::<u64>(Self::entry_count(self.capacity));
            slice[entry_idx]
        }
    }

    // --- Instance Methods - Row Operations ---

    /// Check if the value at index is valid (non-null).
    #[inline]
    pub fn is_valid(&self, row_idx: usize) -> bool {
        self.assert_in_bounds("is_valid", row_idx);
        if self.bits.is_none() {
            return true;
        }
        self.is_valid_unsafe(row_idx)
    }

    /// Check if the value at index is valid (unsafe version, skips null check).
    #[inline]
    pub fn is_valid_unsafe(&self, row_idx: usize) -> bool {
        debug_assert!(self.bits.is_some());
        let (entry_idx, idx_in_entry) = Self::get_entry_index(row_idx);
        let entry = self.get_validity_entry_unsafe(entry_idx);
        Self::row_is_valid_in_entry(entry, idx_in_entry)
    }

    /// Set the value at index as null (invalid).
    #[inline]
    pub fn set_invalid(&mut self, row_idx: usize) {
        self.try_set_invalid(row_idx)
            .expect("validity mask allocation failed");
    }

    /// Set the value at index as null (invalid).
    #[inline]
    pub fn try_set_invalid(&mut self, row_idx: usize) -> Result<()> {
        self.assert_in_bounds("set_invalid", row_idx);
        self.try_ensure_writable()?;
        self.set_invalid_unsafe(row_idx);
        Ok(())
    }

    #[inline]
    pub fn set_null(&mut self, row_idx: usize) {
        self.set_invalid(row_idx);
    }

    #[inline]
    pub fn try_set_null(&mut self, row_idx: usize) -> Result<()> {
        self.try_set_invalid(row_idx)
    }

    /// Set the value at index as null (unsafe version, assumes mask is writable).
    #[inline]
    pub fn set_invalid_unsafe(&mut self, row_idx: usize) {
        debug_assert!(self.bits.is_some());
        let (entry_idx, idx_in_entry) = Self::get_entry_index(row_idx);
        unsafe {
            let bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));
            slice[entry_idx] &= !(1u64 << idx_in_entry);
        }
    }

    /// Set the value at index as valid.
    #[inline]
    pub fn set_valid(&mut self, row_idx: usize) {
        self.try_set_valid(row_idx)
            .expect("validity mask allocation failed");
    }

    /// Set the value at index as valid.
    #[inline]
    pub fn try_set_valid(&mut self, row_idx: usize) -> Result<()> {
        self.assert_in_bounds("set_valid", row_idx);
        if self.bits.is_none() {
            // Already valid
            return Ok(());
        }
        self.try_make_exclusive()?;
        self.set_valid_unsafe(row_idx);
        Ok(())
    }

    /// Set the value at index as valid (unsafe version, assumes mask is set).
    #[inline]
    pub fn set_valid_unsafe(&mut self, row_idx: usize) {
        debug_assert!(self.bits.is_some());
        let (entry_idx, idx_in_entry) = Self::get_entry_index(row_idx);
        unsafe {
            let bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));
            slice[entry_idx] |= 1u64 << idx_in_entry;
        }
    }

    /// Set the value at index to either valid or invalid.
    #[inline]
    pub fn set(&mut self, row_idx: usize, valid: bool) {
        self.try_set(row_idx, valid)
            .expect("validity mask allocation failed");
    }

    /// Set the value at index to either valid or invalid.
    #[inline]
    pub fn try_set(&mut self, row_idx: usize, valid: bool) -> Result<()> {
        if valid {
            self.try_set_valid(row_idx)
        } else {
            self.try_set_invalid(row_idx)
        }
    }

    // --- Instance Methods - Bulk Operations ---

    /// Set all values as invalid (null) for the given count.
    pub fn set_all_invalid(&mut self, count: usize) {
        self.try_set_range_invalid(0, count)
            .expect("validity mask allocation failed");
    }

    /// Set all values as invalid (null) for the given count.
    pub fn try_set_all_invalid(&mut self, count: usize) -> Result<()> {
        self.try_set_range_invalid(0, count)
    }

    /// Set all values as valid for the given count.
    pub fn set_all_valid(&mut self, count: usize) {
        self.try_set_range_valid(0, count)
            .expect("validity mask allocation failed");
    }

    /// Set all values as valid for the given count.
    pub fn try_set_all_valid(&mut self, count: usize) -> Result<()> {
        self.try_set_range_valid(0, count)
    }

    /// Set a row range as valid.
    pub fn try_set_range_valid(&mut self, start: usize, count: usize) -> Result<()> {
        self.try_set_range(start, count, true)
    }

    /// Set a row range as invalid.
    pub fn try_set_range_invalid(&mut self, start: usize, count: usize) -> Result<()> {
        self.try_set_range(start, count, false)
    }

    fn try_set_range(&mut self, start: usize, count: usize, valid: bool) -> Result<()> {
        let end = start.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask range overflow: start={start}, count={count}"
            ))
        })?;
        if end > self.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask range out of bounds: start={start}, count={count}, capacity={}",
                self.capacity
            )));
        }
        if count == 0 {
            return Ok(());
        }
        if valid && self.bits.is_none() {
            return Ok(());
        }

        if valid {
            self.try_make_exclusive()?;
        } else {
            self.try_ensure_writable()?;
        }

        let start_entry = start / BITS_PER_VALUE;
        let end_entry = (end - 1) / BITS_PER_VALUE;
        let start_bit = start % BITS_PER_VALUE;
        let end_bit = end % BITS_PER_VALUE;

        unsafe {
            let bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));

            if start_entry == end_entry {
                let mask_end = if end_bit == 0 {
                    BITS_PER_VALUE
                } else {
                    end_bit
                };
                let mask =
                    Self::entry_with_valid_bits(mask_end) & !Self::entry_with_valid_bits(start_bit);
                Self::apply_range_mask(&mut slice[start_entry], mask, valid);
                return Ok(());
            }

            let first_mask = MAX_ENTRY << start_bit;
            Self::apply_range_mask(&mut slice[start_entry], first_mask, valid);

            let middle_value = if valid { MAX_ENTRY } else { 0 };
            for item in &mut slice[(start_entry + 1)..end_entry] {
                *item = middle_value;
            }

            let last_mask = if end_bit == 0 {
                MAX_ENTRY
            } else {
                Self::entry_with_valid_bits(end_bit)
            };
            Self::apply_range_mask(&mut slice[end_entry], last_mask, valid);
        }

        Ok(())
    }

    #[inline]
    fn apply_range_mask(entry: &mut u64, mask: u64, valid: bool) {
        if valid {
            *entry |= mask;
        } else {
            *entry &= !mask;
        }
    }

    /// Copy a contiguous validity range from `source` into this mask.
    pub fn try_copy_range_from(
        &mut self,
        dst_offset: usize,
        source: &ValidityMask,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        let dst_end = dst_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask copy range destination overflow: offset={dst_offset}, count={count}"
            ))
        })?;
        let src_end = src_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask copy range source overflow: offset={src_offset}, count={count}"
            ))
        })?;
        if src_end > source.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask copy range source out of bounds: source={src_offset}..{src_end}/{}",
                source.capacity
            )));
        }
        if count == 0 {
            return Ok(());
        }

        self.try_ensure_capacity(dst_end)?;

        if source.all_valid() {
            return self.try_set_range_valid(dst_offset, count);
        }

        self.try_ensure_writable()?;

        if src_offset % BITS_PER_VALUE == dst_offset % BITS_PER_VALUE {
            self.copy_range_same_alignment(dst_offset, source, src_offset, count);
        } else {
            self.copy_range_shifted(dst_offset, source, src_offset, count);
        }
        Ok(())
    }

    /// Copy validity from a source selection into a contiguous destination range.
    pub fn try_copy_selection_from(
        &mut self,
        dst_offset: usize,
        source: &ValidityMask,
        selection: &VectorSelection,
        count: usize,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        match selection {
            VectorSelection::None => self.try_copy_range_from(dst_offset, source, 0, count),
            VectorSelection::Range { offset, .. } => {
                self.try_copy_range_from(dst_offset, source, *offset, count)
            }
            VectorSelection::Repeated {
                index,
                count: selection_count,
            } => {
                if count > *selection_count || (count != 0 && *index >= source.capacity) {
                    return Err(paro_error::internal(format!(
                        "ValidityMask repeated selection is out of bounds: index={index}, count={count}, selection_count={selection_count}, source_capacity={}",
                        source.capacity
                    )));
                }
                let dst_end = dst_offset.checked_add(count).ok_or_else(|| {
                    paro_error::internal(format!(
                        "ValidityMask repeated selection destination overflow: offset={dst_offset}, count={count}"
                    ))
                })?;
                self.try_ensure_capacity(dst_end)?;
                if source.all_valid() || source.is_valid(*index) {
                    self.try_set_range_valid(dst_offset, count)
                } else {
                    self.try_set_range_invalid(dst_offset, count)
                }
            }
            VectorSelection::Materialized(sel) => {
                if count > sel.len() {
                    return Err(paro_error::internal(format!(
                        "ValidityMask copy selection out of bounds: count={count}, selection_len={}",
                        sel.len()
                    )));
                }
                let dst_end = dst_offset.checked_add(count).ok_or_else(|| {
                    paro_error::internal(format!(
                        "ValidityMask copy selection destination overflow: offset={dst_offset}, count={count}"
                    ))
                })?;
                self.try_ensure_capacity(dst_end)?;
                if source.all_valid() {
                    return self.try_set_range_valid(dst_offset, count);
                }

                self.try_ensure_writable()?;
                for i in 0..count {
                    let source_idx = sel.get(i);
                    if source_idx >= source.capacity {
                        return Err(paro_error::internal(format!(
                            "ValidityMask copy selection source out of bounds: source_idx={source_idx}, capacity={}",
                            source.capacity
                        )));
                    }
                    let valid = source.is_valid(source_idx);
                    self.set_prepared_bit(dst_offset + i, valid);
                }
                Ok(())
            }
        }
    }

    /// Copy a contiguous source range into scattered destination positions.
    pub fn try_copy_scatter_from(
        &mut self,
        source: &ValidityMask,
        src_start: usize,
        dst_positions: &[usize],
    ) -> Result<()> {
        if dst_positions.is_empty() {
            return Ok(());
        }
        let src_end = src_start.checked_add(dst_positions.len()).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask copy scatter source overflow: start={src_start}, count={}",
                dst_positions.len()
            ))
        })?;
        if src_end > source.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask copy scatter source out of bounds: source={src_start}..{src_end}/{}",
                source.capacity
            )));
        }

        let required_capacity = dst_positions
            .iter()
            .copied()
            .max()
            .and_then(|idx| idx.checked_add(1))
            .ok_or_else(|| paro_error::internal("ValidityMask scatter destination overflow"))?;
        self.try_ensure_capacity(required_capacity)?;

        if source.all_valid() {
            for &dst_idx in dst_positions {
                self.try_set_valid(dst_idx)?;
            }
            return Ok(());
        }

        self.try_ensure_writable()?;

        let mut run_start = 0;
        while run_start < dst_positions.len() {
            let mut run_len = 1;
            while run_start + run_len < dst_positions.len()
                && dst_positions[run_start + run_len] == dst_positions[run_start] + run_len
            {
                run_len += 1;
            }

            if run_len >= 8 {
                self.try_copy_range_from(
                    dst_positions[run_start],
                    source,
                    src_start + run_start,
                    run_len,
                )?;
            } else {
                for i in 0..run_len {
                    let src_idx = src_start + run_start + i;
                    let dst_idx = dst_positions[run_start + i];
                    self.set_prepared_bit(dst_idx, source.is_valid(src_idx));
                }
            }
            run_start += run_len;
        }
        Ok(())
    }

    fn copy_range_same_alignment(
        &mut self,
        dst_offset: usize,
        source: &ValidityMask,
        src_offset: usize,
        count: usize,
    ) {
        let mut copied = 0;
        let bit_offset = dst_offset % BITS_PER_VALUE;

        if bit_offset != 0 {
            let take = count.min(BITS_PER_VALUE - bit_offset);
            let bits = source.read_bits(src_offset, take);
            self.write_bits(dst_offset, bits, take);
            copied += take;
        }

        let full_words = (count - copied) / BITS_PER_VALUE;
        if full_words > 0 {
            let dst_entry = (dst_offset + copied) / BITS_PER_VALUE;
            let src_entry = (src_offset + copied) / BITS_PER_VALUE;
            unsafe {
                let dst_bits = self
                    .bits
                    .as_mut()
                    .expect("invariant: mask bits must be set");
                let dst_slice = dst_bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));
                let src_bits = source
                    .bits
                    .as_ref()
                    .expect("invariant: source mask bits must be set");
                let src_slice = src_bits.as_slice::<u64>(Self::entry_count(source.capacity));
                dst_slice[dst_entry..dst_entry + full_words]
                    .copy_from_slice(&src_slice[src_entry..src_entry + full_words]);
            }
            copied += full_words * BITS_PER_VALUE;
        }

        if copied < count {
            let take = count - copied;
            let bits = source.read_bits(src_offset + copied, take);
            self.write_bits(dst_offset + copied, bits, take);
        }
    }

    fn copy_range_shifted(
        &mut self,
        dst_offset: usize,
        source: &ValidityMask,
        src_offset: usize,
        count: usize,
    ) {
        let mut copied = 0;
        while copied < count {
            let dst_bit = (dst_offset + copied) % BITS_PER_VALUE;
            let take = (count - copied).min(BITS_PER_VALUE - dst_bit);
            let bits = source.read_bits(src_offset + copied, take);
            self.write_bits(dst_offset + copied, bits, take);
            copied += take;
        }
    }

    fn read_bits(&self, offset: usize, count: usize) -> u64 {
        debug_assert!(count > 0 && count <= BITS_PER_VALUE);
        if self.bits.is_none() {
            return Self::entry_with_valid_bits(count);
        }

        let entry_idx = offset / BITS_PER_VALUE;
        let bit_idx = offset % BITS_PER_VALUE;
        unsafe {
            let bits = self
                .bits
                .as_ref()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_slice::<u64>(Self::entry_count(self.capacity));
            let mut value = slice[entry_idx] >> bit_idx;
            if bit_idx + count > BITS_PER_VALUE {
                value |= slice[entry_idx + 1] << (BITS_PER_VALUE - bit_idx);
            }
            value & Self::entry_with_valid_bits(count)
        }
    }

    fn write_bits(&mut self, offset: usize, bits: u64, count: usize) {
        debug_assert!(count > 0 && count <= BITS_PER_VALUE);
        let entry_idx = offset / BITS_PER_VALUE;
        let bit_idx = offset % BITS_PER_VALUE;
        debug_assert!(bit_idx + count <= BITS_PER_VALUE);

        let mask = if count == BITS_PER_VALUE {
            MAX_ENTRY
        } else {
            Self::entry_with_valid_bits(count) << bit_idx
        };
        unsafe {
            let dst_bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let dst_slice = dst_bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));
            dst_slice[entry_idx] = (dst_slice[entry_idx] & !mask) | ((bits << bit_idx) & mask);
        }
    }

    fn set_prepared_bit(&mut self, row_idx: usize, valid: bool) {
        debug_assert!(self.bits.is_some());
        if valid {
            self.set_valid_unsafe(row_idx);
        } else {
            self.set_invalid_unsafe(row_idx);
        }
    }

    // --- Instance Methods - Counting ---

    /// Count the number of valid (non-null) values in the first `count` elements.
    pub fn count_valid(&self, count: usize) -> usize {
        if self.all_valid() || count == 0 {
            return count;
        }

        let mut valid = 0;
        let entry_count = Self::entry_count(count);

        for entry_idx in 0..entry_count {
            let mut entry = self.get_validity_entry(entry_idx);

            // Handle ragged end (if not exactly multiple of BITS_PER_VALUE)
            if entry_idx == entry_count - 1 && !count.is_multiple_of(BITS_PER_VALUE) {
                let shift = BITS_PER_VALUE - (count % BITS_PER_VALUE);
                let mask = MAX_ENTRY >> shift;
                entry &= mask;
            } else if Self::all_valid_entry(entry) {
                // All bits set - fast path
                valid += BITS_PER_VALUE;
                continue;
            }

            // Count bits using popcount
            valid += entry.count_ones() as usize;
        }

        valid
    }

    /// Check if all values in [0, count) are valid.
    pub fn check_all_valid(&self, count: usize) -> bool {
        self.count_valid(count) == count
    }

    /// Check if all values in [from, to) are valid.
    pub fn check_all_valid_range(&self, from: usize, to: usize) -> bool {
        if self.all_valid() {
            return true;
        }
        for i in from..to {
            if !self.is_valid(i) {
                return false;
            }
        }
        true
    }

    /// Check if all values in [0, count) are invalid.
    pub fn check_all_invalid(&self, count: usize) -> bool {
        self.count_valid(count) == 0
    }

    // --- Instance Methods - Memory Management ---

    /// Ensure the validity mask is writable, allocating space if not initialized.
    pub fn ensure_writable(&mut self) {
        self.try_ensure_writable()
            .expect("validity mask allocation failed");
    }

    /// Ensure the validity mask is writable, allocating space if not initialized.
    pub fn try_ensure_writable(&mut self) -> Result<()> {
        if self.bits.is_none() && self.capacity > 0 {
            let num_words = Self::entry_count(self.capacity);
            let mut buf = VectorBuffer::try_with_allocator(
                std::mem::size_of::<u64>(),
                num_words,
                self.allocator.clone(),
            )?;
            // Initialize to all valid
            unsafe {
                let slice = buf.as_mut_slice::<u64>(num_words);
                slice.fill(MAX_ENTRY);
            }
            self.bits = Some(buf);
        } else if self.bits.is_some() {
            // Ensure exclusive ownership for mutation (CoW)
            self.bits
                .as_mut()
                .expect("invariant: mask bits must be set")
                .try_make_exclusive()?;
        }
        Ok(())
    }

    pub fn try_make_exclusive(&mut self) -> Result<()> {
        if self.bits.is_some() {
            self.bits
                .as_mut()
                .expect("invariant: mask bits must be set")
                .try_make_exclusive()?;
        }
        Ok(())
    }

    /// Reset the mask to all-valid state with new capacity.
    pub fn reset(&mut self, new_capacity: usize) {
        self.bits = None;
        self.capacity = new_capacity;
    }

    pub(crate) fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        self.bits
            .as_ref()
            .map(|bits| bits.collect_allocation_size(allocations))
            .unwrap_or(0)
    }

    pub(crate) fn collect_allocation_entries(&self, entries: &mut Vec<(usize, usize)>) {
        if let Some(bits) = &self.bits {
            bits.collect_allocation_entries(entries);
        }
    }

    /// Resize the validity mask to new size.
    pub fn resize(&mut self, new_size: usize) {
        self.try_resize(new_size)
            .expect("validity mask allocation failed");
    }

    /// Resize the validity mask to new size.
    pub fn try_resize(&mut self, new_size: usize) -> Result<()> {
        let old_size = self.capacity;
        if new_size <= old_size {
            self.capacity = new_size;
            return Ok(());
        }

        if self.bits.is_none() {
            self.capacity = new_size;
            return Ok(());
        }

        let new_word_count = Self::entry_count(new_size);
        let old_word_count = Self::entry_count(old_size);
        if new_word_count == old_word_count {
            self.try_make_exclusive()?;
            self.capacity = new_size;
            self.try_set_range_valid(old_size, new_size - old_size)?;
            return Ok(());
        }

        let mut new_buf = VectorBuffer::try_with_allocator(
            std::mem::size_of::<u64>(),
            new_word_count,
            self.allocator.clone(),
        )?;

        unsafe {
            let old_bits = self
                .bits
                .as_ref()
                .expect("invariant: mask bits must be set");
            let old_slice = old_bits.as_slice::<u64>(old_word_count);
            let new_slice = new_buf.as_mut_slice::<u64>(new_word_count);

            // Copy existing data
            new_slice[..old_word_count].copy_from_slice(old_slice);

            // Initialize new entries as valid
            for item in new_slice
                .iter_mut()
                .take(new_word_count)
                .skip(old_word_count)
            {
                *item = MAX_ENTRY;
            }
        }

        self.bits = Some(new_buf);
        self.capacity = new_size;
        Ok(())
    }

    /// Ensure that at least `required_capacity` rows remain addressable.
    ///
    /// Unlike [`Self::try_resize`], this operation never shrinks the logical
    /// range. Writers that update a subrange must preserve validity state for
    /// rows written by earlier, potentially out-of-order operations.
    #[inline]
    pub fn try_ensure_capacity(&mut self, required_capacity: usize) -> Result<()> {
        if required_capacity > self.capacity {
            self.try_resize(required_capacity)?;
        }
        Ok(())
    }

    /// Initialize with the contents of another mask.
    pub fn initialize(&mut self, other: &ValidityMask) {
        self.bits = other.bits.clone();
        self.capacity = other.capacity;
    }

    /// Copy validity data from another mask.
    pub fn copy(&mut self, other: &ValidityMask, count: usize) {
        self.try_copy(other, count)
            .expect("validity mask allocation failed");
    }

    /// Copy validity data from another mask.
    pub fn try_copy(&mut self, other: &ValidityMask, count: usize) -> Result<()> {
        if count > other.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask copy out of bounds: count={count}, source_capacity={}",
                other.capacity
            )));
        }
        self.capacity = count;
        if other.all_valid() {
            self.bits = None;
        } else {
            // Deep copy the data
            let num_words = Self::entry_count(count);
            let mut buf = VectorBuffer::try_with_allocator(
                std::mem::size_of::<u64>(),
                num_words,
                self.allocator.clone(),
            )?;
            unsafe {
                let other_bits = other
                    .bits
                    .as_ref()
                    .expect("invariant: other mask bits must be set");
                let src_slice = other_bits.as_slice::<u64>(num_words);
                let dst_slice = buf.as_mut_slice::<u64>(num_words);
                dst_slice.copy_from_slice(src_slice);
            }
            self.bits = Some(buf);
        }
        Ok(())
    }

    /// Shallow copy from another mask (shares underlying buffer).
    pub fn copy_from(&mut self, other: &Self) {
        self.bits = other.bits.clone();
        self.capacity = other.capacity;
    }

    // --- Instance Methods - Combining and Slicing ---

    /// Combine this mask with another using AND operation.
    /// Result has a null where either mask has a null.
    pub fn combine(&mut self, other: &ValidityMask, count: usize) {
        self.try_combine(other, count)
            .expect("validity mask allocation failed");
    }

    /// Combine this mask with another using AND operation.
    /// Result has a null where either mask has a null.
    pub fn try_combine(&mut self, other: &ValidityMask, count: usize) -> Result<()> {
        if other.all_valid() {
            // X & 1 = X
            return self.try_resize(count);
        }
        if count > other.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask combine source out of bounds: count={count}, source_capacity={}",
                other.capacity
            )));
        }
        if self.all_valid() {
            // 1 & Y = Y
            self.bits = other.bits.clone();
            self.capacity = count;
            return Ok(());
        }
        if count > self.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask combine target out of bounds: count={count}, target_capacity={}",
                self.capacity
            )));
        }

        // Check if they share the same buffer
        if let (Some(self_bits), Some(other_bits)) = (&self.bits, &other.bits) {
            unsafe {
                if std::ptr::eq(
                    self_bits.as_slice::<u64>(1).as_ptr(),
                    other_bits.as_slice::<u64>(1).as_ptr(),
                ) {
                    // X & X = X
                    return self.try_resize(count);
                }
            }
        }

        // Have to merge - create new mask with combined data
        let entry_count = Self::entry_count(count);
        let mut new_buf = VectorBuffer::try_with_allocator(
            std::mem::size_of::<u64>(),
            entry_count,
            self.allocator.clone(),
        )?;

        unsafe {
            let self_bits = self
                .bits
                .as_ref()
                .expect("invariant: mask bits must be set");
            let other_bits = other
                .bits
                .as_ref()
                .expect("invariant: other mask bits must be set");
            let self_slice = self_bits.as_slice::<u64>(entry_count);
            let other_slice = other_bits.as_slice::<u64>(entry_count);
            let new_slice = new_buf.as_mut_slice::<u64>(entry_count);

            for i in 0..entry_count {
                new_slice[i] = self_slice[i] & other_slice[i];
            }
        }

        self.bits = Some(new_buf);
        self.capacity = count;
        Ok(())
    }

    /// Slice the validity mask from source_offset for count elements.
    pub fn slice(&mut self, other: &ValidityMask, source_offset: usize, count: usize) {
        self.try_slice(other, source_offset, count)
            .expect("validity mask allocation failed");
    }

    /// Slice the validity mask from source_offset for count elements.
    pub fn try_slice(
        &mut self,
        other: &ValidityMask,
        source_offset: usize,
        count: usize,
    ) -> Result<()> {
        self.capacity = count;
        if other.all_valid() {
            self.bits = None;
            return Ok(());
        }
        if source_offset == 0 {
            return self.try_copy(other, count);
        }

        // Create a new mask and copy data
        let mut new_mask = ValidityMask::with_allocator(count, self.allocator.clone());
        new_mask.try_slice_in_place(other, 0, source_offset, count)?;
        self.initialize(&new_mask);
        Ok(())
    }

    /// Slice validity in place with bit-level precision.
    pub fn slice_in_place(
        &mut self,
        other: &ValidityMask,
        target_offset: usize,
        source_offset: usize,
        count: usize,
    ) {
        self.try_slice_in_place(other, target_offset, source_offset, count)
            .expect("validity mask allocation failed");
    }

    /// Slice validity in place with bit-level precision.
    pub fn try_slice_in_place(
        &mut self,
        other: &ValidityMask,
        target_offset: usize,
        source_offset: usize,
        count: usize,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let target_end = target_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask slice target overflow: offset={target_offset}, count={count}"
            ))
        })?;
        let source_end = source_offset.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "ValidityMask slice source overflow: offset={source_offset}, count={count}"
            ))
        })?;
        if target_end > self.capacity || source_end > other.capacity {
            return Err(paro_error::internal(format!(
                "ValidityMask slice out of bounds: target={target_offset}..{target_end}/{} source={source_offset}..{source_end}/{}",
                self.capacity, other.capacity
            )));
        }

        if self.all_valid() && other.all_valid() {
            return Ok(());
        }

        self.try_ensure_writable()?;

        let ragged = count % BITS_PER_VALUE;
        let entire_units = count / BITS_PER_VALUE;

        if Self::is_aligned(source_offset) && Self::is_aligned(target_offset) {
            // Fast path: both offsets are aligned
            let source_offset_entries = Self::entry_count(source_offset);
            let target_offset_entries = Self::entry_count(target_offset);

            unsafe {
                let target_bits = self
                    .bits
                    .as_mut()
                    .expect("invariant: mask bits must be set");
                let target_slice =
                    target_bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));

                if !other.is_mask_set() {
                    // Source is all valid - set all bytes to MAX
                    for i in 0..entire_units {
                        target_slice[target_offset_entries + i] = MAX_ENTRY;
                    }
                } else {
                    let source_bits = other
                        .bits
                        .as_ref()
                        .expect("invariant: other mask bits must be set");
                    let source_slice =
                        source_bits.as_slice::<u64>(Self::entry_count(other.capacity));

                    target_slice[target_offset_entries..(target_offset_entries + entire_units)]
                        .copy_from_slice(
                            &source_slice
                                [source_offset_entries..(source_offset_entries + entire_units)],
                        );
                }

                // Handle ragged end
                if ragged > 0 {
                    let src_entry = if other.is_mask_set() {
                        let source_bits = other
                            .bits
                            .as_ref()
                            .expect("invariant: other mask bits must be set");
                        let source_slice =
                            source_bits.as_slice::<u64>(Self::entry_count(other.capacity));
                        source_slice[source_offset_entries + entire_units]
                    } else {
                        MAX_ENTRY
                    };
                    let src_entry = src_entry & (MAX_ENTRY >> (BITS_PER_VALUE - ragged));

                    let tgt_idx = target_offset_entries + entire_units;
                    let tgt_entry = target_slice[tgt_idx] & (MAX_ENTRY << ragged);
                    target_slice[tgt_idx] = tgt_entry | src_entry;
                }
            }
        } else if Self::is_aligned(target_offset) {
            // Common case: target aligned, source not aligned
            let tail = source_offset % BITS_PER_VALUE;
            let head = BITS_PER_VALUE - tail;
            let source_start_entry = source_offset / BITS_PER_VALUE;
            let target_start_entry = target_offset / BITS_PER_VALUE;

            unsafe {
                let target_bits = self
                    .bits
                    .as_mut()
                    .expect("invariant: mask bits must be set");
                let target_slice =
                    target_bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));

                if let Some(source_bits) = other.bits.as_ref() {
                    let source_slice =
                        source_bits.as_slice::<u64>(Self::entry_count(other.capacity));

                    let mut src_entry = source_slice[source_start_entry];
                    for i in 0..entire_units {
                        // Start with head of previous src
                        let mut tgt_entry = src_entry >> tail;
                        src_entry = source_slice[source_start_entry + 1 + i];
                        // Add in tail of current src
                        tgt_entry |= src_entry << head;
                        target_slice[target_start_entry + i] = tgt_entry;
                    }

                    // Handle ragged end
                    if ragged > 0 {
                        let mut tgt_entry = src_entry >> tail;
                        if head < ragged {
                            src_entry = source_slice[source_start_entry + 1 + entire_units];
                            tgt_entry |= src_entry << head;
                        }
                        tgt_entry &= MAX_ENTRY >> (BITS_PER_VALUE - ragged);
                        tgt_entry |=
                            target_slice[target_start_entry + entire_units] & (MAX_ENTRY << ragged);
                        target_slice[target_start_entry + entire_units] = tgt_entry;
                    }
                } else {
                    for i in 0..entire_units {
                        target_slice[target_start_entry + i] = MAX_ENTRY;
                    }
                    if ragged > 0 {
                        target_slice[target_start_entry + entire_units] |=
                            Self::entry_with_valid_bits(ragged);
                    }
                }
            }
        } else {
            // Fallback: bit-by-bit copy
            for i in 0..count {
                self.try_set(target_offset + i, other.is_valid(source_offset + i))?;
            }
        }
        Ok(())
    }

    /// Copy validity using a selection vector.
    pub fn copy_sel(
        &mut self,
        other: &ValidityMask,
        sel: &SelectionVector,
        source_offset: usize,
        target_offset: usize,
        copy_count: usize,
    ) {
        self.try_copy_sel(other, sel, source_offset, target_offset, copy_count)
            .expect("validity mask allocation failed");
    }

    /// Copy validity using a selection vector.
    pub fn try_copy_sel(
        &mut self,
        other: &ValidityMask,
        sel: &SelectionVector,
        source_offset: usize,
        target_offset: usize,
        copy_count: usize,
    ) -> Result<()> {
        if !other.is_mask_set() && !self.is_mask_set() {
            // No need to copy if neither has null values
            return Ok(());
        }

        // Use selection vector
        for i in 0..copy_count {
            let source_idx = sel.get(source_offset + i);
            self.try_set(target_offset + i, other.is_valid(source_idx))?;
        }
        Ok(())
    }

    // --- Debug and Display ---

    /// Convert the validity mask to a string representation.
    pub fn to_string(&self, count: usize) -> String {
        let mut result = format!("Validity Mask ({}) [", count);
        for i in 0..count {
            result.push(if self.is_valid(i) { '.' } else { 'X' });
        }
        result.push(']');
        result
    }
}

impl Default for ValidityMask {
    fn default() -> Self {
        Self::new(VECTOR_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::{Allocator, DefaultAllocator};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct FailingAllocator;

    impl Allocator for FailingAllocator {
        fn allocate(&self, size: usize) -> Result<*mut u8> {
            Err(paro_error::out_of_memory(format!(
                "injected allocation failure: {size} bytes"
            )))
        }

        fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
            self.allocate(size)
        }

        fn free(&self, _ptr: *mut u8, _size: usize) {}

        fn reallocate(&self, _ptr: *mut u8, _old_size: usize, new_size: usize) -> Result<*mut u8> {
            self.allocate(new_size)
        }

        fn name(&self) -> &'static str {
            "FailingAllocator"
        }
    }

    #[derive(Debug)]
    struct ToggleAllocator {
        inner: DefaultAllocator,
        fail: AtomicBool,
    }

    impl ToggleAllocator {
        fn new() -> Self {
            Self {
                inner: DefaultAllocator::new(),
                fail: AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, Ordering::SeqCst);
        }
    }

    impl Allocator for ToggleAllocator {
        fn allocate(&self, size: usize) -> Result<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {size} bytes"
                )));
            }
            self.inner.allocate(size)
        }

        fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {size} bytes"
                )));
            }
            self.inner.allocate_zeroed(size)
        }

        fn free(&self, ptr: *mut u8, size: usize) {
            self.inner.free(ptr, size);
        }

        fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {new_size} bytes"
                )));
            }
            self.inner.reallocate(ptr, old_size, new_size)
        }

        fn name(&self) -> &'static str {
            "ToggleAllocator"
        }
    }

    #[test]
    fn test_validity_mask_basic() {
        let mut mask = ValidityMask::new(100);
        assert!(mask.all_valid());
        assert!(mask.is_valid(0));
        assert!(mask.is_valid(99));

        mask.set_null(50);
        assert!(!mask.all_valid());
        assert!(!mask.is_valid(50));
        assert!(mask.is_valid(49));
        assert!(mask.is_valid(51));

        mask.set_valid(50);
        assert!(mask.is_valid(50));
    }

    #[test]
    fn test_validity_mask_sharing() {
        let mut mask1 = ValidityMask::new(10);
        mask1.set_null(1);

        let mask2 = mask1.clone();
        assert!(!mask2.is_valid(1));

        // mask1 mutates, triggering CoW
        mask1.set_null(2);
        assert!(!mask1.is_valid(2));
        // mask2 should NOT be affected (CoW)
        assert!(mask2.is_valid(2));
        assert!(!mask2.is_valid(1)); // Still has old null
    }

    #[test]
    fn test_validity_mask_resize() {
        let mut mask = ValidityMask::new(10);
        mask.set_null(5);
        mask.resize(100);
        assert_eq!(mask.capacity(), 100);
        assert!(!mask.is_valid(5));
        assert!(mask.is_valid(99));
    }

    #[test]
    fn test_count_valid() {
        let mut mask = ValidityMask::new(100);
        assert_eq!(mask.count_valid(100), 100);

        mask.set_null(10);
        mask.set_null(20);
        mask.set_null(30);
        assert_eq!(mask.count_valid(100), 97);
        assert_eq!(mask.count_valid(15), 14); // Only one null in [0, 15)
    }

    #[test]
    fn test_check_all_valid() {
        let mask = ValidityMask::new(100);
        assert!(mask.check_all_valid(100));
        assert!(mask.check_all_valid_range(50, 100));

        let mut mask2 = ValidityMask::new(100);
        mask2.set_null(75);
        assert!(!mask2.check_all_valid(100));
        assert!(mask2.check_all_valid_range(0, 75));
        assert!(!mask2.check_all_valid_range(75, 76));
    }

    #[test]
    fn test_set_all_invalid() {
        let mut mask = ValidityMask::new(100);
        mask.set_all_invalid(50);

        for i in 0..50 {
            assert!(!mask.is_valid(i), "expected invalid at {}", i);
        }
        for i in 50..100 {
            assert!(
                mask.is_valid(i),
                "expected padding/range tail valid at {}",
                i
            );
        }
    }

    #[test]
    fn test_try_set_range_valid_invalid_preserves_edges() {
        let mut mask = ValidityMask::new(130);

        mask.try_set_range_invalid(3, 126).unwrap();
        assert!(mask.is_valid(0));
        assert!(mask.is_valid(2));
        for idx in 3..129 {
            assert!(!mask.is_valid(idx), "expected invalid at {idx}");
        }
        assert!(mask.is_valid(129));

        mask.try_set_range_valid(5, 120).unwrap();
        assert!(!mask.is_valid(3));
        assert!(!mask.is_valid(4));
        for idx in 5..125 {
            assert!(mask.is_valid(idx), "expected valid at {idx}");
        }
        assert!(!mask.is_valid(125));
    }

    #[test]
    fn test_try_set_invalid_propagates_allocation_error() {
        let mut mask = ValidityMask::with_allocator(64, Arc::new(FailingAllocator));

        let err = mask.try_set_invalid(0).unwrap_err();

        assert!(err.to_string().contains("injected allocation failure"));
        assert!(mask.all_valid());
    }

    #[test]
    fn test_try_set_valid_cow_propagates_allocation_error() {
        let allocator = Arc::new(ToggleAllocator::new());
        let mut mask = ValidityMask::with_allocator(64, allocator.clone());
        mask.try_set_invalid(0).unwrap();

        let mut shared = mask.clone();
        allocator.set_fail(true);

        let err = shared.try_set_valid(0).unwrap_err();

        assert!(err.to_string().contains("injected allocation failure"));
        assert!(!mask.is_valid(0));
        assert!(!shared.is_valid(0));
    }

    #[test]
    fn test_try_resize_failure_preserves_capacity() {
        let allocator = Arc::new(ToggleAllocator::new());
        let mut mask = ValidityMask::with_allocator(64, allocator.clone());
        mask.try_set_invalid(1).unwrap();

        allocator.set_fail(true);
        let err = mask.try_resize(129).unwrap_err();

        assert!(err.to_string().contains("injected allocation failure"));
        assert_eq!(mask.capacity(), 64);
        assert!(!mask.is_valid(1));
    }

    #[test]
    fn test_combine() {
        let mut mask1 = ValidityMask::new(10);
        mask1.set_null(2);
        mask1.set_null(4);

        let mut mask2 = ValidityMask::new(10);
        mask2.set_null(3);
        mask2.set_null(4);

        mask1.combine(&mask2, 10);

        // Combined mask should have nulls at 2, 3, 4
        assert!(mask1.is_valid(0));
        assert!(mask1.is_valid(1));
        assert!(!mask1.is_valid(2));
        assert!(!mask1.is_valid(3));
        assert!(!mask1.is_valid(4));
        assert!(mask1.is_valid(5));
    }

    #[test]
    fn test_slice() {
        let mut source = ValidityMask::new(20);
        source.set_null(5);
        source.set_null(10);

        let mut dest = ValidityMask::new(10);
        dest.slice(&source, 5, 10);

        // After slicing from offset 5:
        // - Position 0 in dest corresponds to position 5 in source (null)
        // - Position 5 in dest corresponds to position 10 in source (null)
        assert!(!dest.is_valid(0));
        assert!(dest.is_valid(1));
        assert!(!dest.is_valid(5));
    }

    #[test]
    fn test_entry_helpers() {
        assert_eq!(ValidityMask::entry_count(64), 1);
        assert_eq!(ValidityMask::entry_count(65), 2);
        assert_eq!(ValidityMask::entry_count(128), 2);

        assert!(ValidityMask::is_aligned(64));
        assert!(ValidityMask::is_aligned(128));
        assert!(!ValidityMask::is_aligned(65));

        assert_eq!(ValidityMask::entry_with_valid_bits(0), 0);
        assert_eq!(ValidityMask::entry_with_valid_bits(1), 1);
        assert_eq!(ValidityMask::entry_with_valid_bits(64), MAX_ENTRY);
    }

    #[test]
    fn test_to_string() {
        let mut mask = ValidityMask::new(10);
        mask.set_null(2);
        mask.set_null(5);

        let s = mask.to_string(10);
        assert_eq!(s, "Validity Mask (10) [..X..X....]");
    }

    #[test]
    fn test_reset() {
        let mut mask = ValidityMask::new(100);
        mask.set_null(50);
        assert!(!mask.all_valid());

        mask.reset(200);
        assert!(mask.all_valid());
        assert_eq!(mask.capacity(), 200);
    }

    #[test]
    fn test_copy() {
        let mut source = ValidityMask::new(100);
        source.set_null(10);
        source.set_null(20);

        let mut dest = ValidityMask::new(50);
        dest.copy(&source, 50);

        assert!(!dest.is_valid(10));
        assert!(!dest.is_valid(20));
        assert_eq!(dest.capacity(), 50);
    }

    #[test]
    fn test_slice_respects_requested_count() {
        let mut source = ValidityMask::new(100);
        source.set_null(10);
        source.set_null(60);

        let mut dest = ValidityMask::new(1);
        dest.slice(&source, 0, 32);

        assert_eq!(dest.capacity(), 32);
        assert!(!dest.is_valid(10));
    }

    #[test]
    fn test_slice_in_place_all_valid_unaligned_source() {
        let source = ValidityMask::new(128);
        let mut dest = ValidityMask::new(128);
        dest.set_null(0);
        dest.slice_in_place(&source, 64, 1, 17);

        for idx in 64..81 {
            assert!(dest.is_valid(idx), "expected valid at {}", idx);
        }
    }

    #[test]
    fn test_try_copy_range_from_same_alignment() {
        let mut source = ValidityMask::new(200);
        source.set_null(70);
        source.set_null(130);
        let mut dest = ValidityMask::new(220);
        dest.set_all_invalid(220);

        dest.try_copy_range_from(129, &source, 65, 80).unwrap();

        for i in 0..80 {
            assert_eq!(
                dest.is_valid(129 + i),
                source.is_valid(65 + i),
                "mismatch at copied row {i}"
            );
        }
        assert!(!dest.is_valid(128));
        assert!(!dest.is_valid(209));
    }

    #[test]
    fn test_try_copy_range_from_unaligned_shift() {
        let mut source = ValidityMask::new(160);
        for idx in [3, 4, 63, 64, 65, 101] {
            source.set_null(idx);
        }
        let mut dest = ValidityMask::new(180);
        dest.set_all_invalid(180);

        dest.try_copy_range_from(70, &source, 3, 110).unwrap();

        for i in 0..110 {
            assert_eq!(
                dest.is_valid(70 + i),
                source.is_valid(3 + i),
                "mismatch at copied row {i}"
            );
        }
    }

    #[test]
    fn test_try_copy_range_from_all_valid_source_sets_only_range() {
        let source = ValidityMask::new(100);
        let mut dest = ValidityMask::new(100);
        dest.set_all_invalid(100);

        dest.try_copy_range_from(10, &source, 0, 25).unwrap();

        for idx in 0..10 {
            assert!(!dest.is_valid(idx));
        }
        for idx in 10..35 {
            assert!(dest.is_valid(idx), "expected valid at {idx}");
        }
        for idx in 35..100 {
            assert!(!dest.is_valid(idx));
        }
    }

    #[test]
    fn test_try_copy_range_from_ragged_tail_preserves_edges() {
        let mut source = ValidityMask::new(130);
        source.set_null(64);
        source.set_null(126);
        source.set_null(127);
        let mut dest = ValidityMask::new(130);
        dest.set_null(70);

        dest.try_copy_range_from(1, &source, 64, 63).unwrap();

        assert!(!dest.is_valid(1));
        assert!(!dest.is_valid(63));
        assert!(dest.is_valid(64));
        assert!(!dest.is_valid(70));
        for i in 0..63 {
            assert_eq!(
                dest.is_valid(1 + i),
                source.is_valid(64 + i),
                "mismatch at copied row {i}"
            );
        }
    }

    #[test]
    fn test_try_copy_selection_from_materialized() {
        let mut source = ValidityMask::new(16);
        source.set_null(2);
        source.set_null(5);
        let selection =
            SelectionVector::try_from_indices(vec![5, 1, 2, 3], Arc::new(DefaultAllocator::new()))
                .unwrap();
        let mut dest = ValidityMask::new(8);
        dest.set_all_invalid(8);

        dest.try_copy_selection_from(2, &source, &VectorSelection::materialized(selection), 4)
            .unwrap();

        assert!(!dest.is_valid(2));
        assert!(dest.is_valid(3));
        assert!(!dest.is_valid(4));
        assert!(dest.is_valid(5));
    }

    #[test]
    fn test_try_copy_scatter_from_runs_and_random_positions() {
        let mut source = ValidityMask::new(16);
        source.set_null(1);
        source.set_null(4);
        source.set_null(9);
        let mut dest = ValidityMask::new(20);
        dest.set_all_invalid(20);

        let positions = [3, 4, 5, 10, 12, 13, 14, 15, 16];
        dest.try_copy_scatter_from(&source, 0, &positions).unwrap();

        for (src_idx, dst_idx) in positions.iter().copied().enumerate() {
            assert_eq!(dest.is_valid(dst_idx), source.is_valid(src_idx));
        }
        assert!(!dest.is_valid(2));
        assert!(!dest.is_valid(11));
    }
}
