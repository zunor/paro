// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{AllocationSet, VectorBuffer};
use crate::allocator::{default_allocator, Allocator};
use crate::vector::{SelectionVector, VECTOR_SIZE};
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
        self.assert_in_bounds("set_invalid", row_idx);
        self.ensure_writable();
        self.set_invalid_unsafe(row_idx);
    }

    #[inline]
    pub fn set_null(&mut self, row_idx: usize) {
        self.set_invalid(row_idx);
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
        self.assert_in_bounds("set_valid", row_idx);
        if self.bits.is_none() {
            // Already valid
            return;
        }
        self.set_valid_unsafe(row_idx);
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
        if valid {
            self.set_valid(row_idx);
        } else {
            self.set_invalid(row_idx);
        }
    }

    // --- Instance Methods - Bulk Operations ---

    /// Set all values as invalid (null) for the given count.
    pub fn set_all_invalid(&mut self, count: usize) {
        self.set_range_invalid(count, 0, Self::entry_count(count));
    }

    /// Marks a range of entries as invalid (null). Useful for parallel initialization.
    pub fn set_range_invalid(&mut self, count: usize, begin_entry: usize, end_entry: usize) {
        self.ensure_writable();
        if count == 0 {
            return;
        }

        let last_entry_index = Self::entry_count(count).saturating_sub(1);
        unsafe {
            let bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));

            // Set full entries to 0
            for item in slice
                .iter_mut()
                .take(std::cmp::min(last_entry_index, end_entry))
                .skip(begin_entry)
            {
                *item = 0;
            }

            // Handle the last entry if in range
            if end_entry > last_entry_index {
                let last_entry_bits = count % BITS_PER_VALUE;
                if last_entry_bits == 0 {
                    slice[last_entry_index] = 0;
                } else {
                    // Set bits beyond count as valid (1), bits within count as invalid (0)
                    slice[last_entry_index] = MAX_ENTRY << last_entry_bits;
                }
            }
        }
    }

    /// Set all values as valid for the given count.
    pub fn set_all_valid(&mut self, count: usize) {
        self.ensure_writable();
        if count == 0 {
            return;
        }

        let last_entry_index = Self::entry_count(count).saturating_sub(1);
        unsafe {
            let bits = self
                .bits
                .as_mut()
                .expect("invariant: mask bits must be set");
            let slice = bits.as_mut_slice::<u64>(Self::entry_count(self.capacity));

            // Set full entries to all valid
            for item in slice.iter_mut().take(last_entry_index) {
                *item = MAX_ENTRY;
            }

            // Handle last ragged entry
            let last_entry_bits = count % BITS_PER_VALUE;
            if last_entry_bits == 0 {
                slice[last_entry_index] = MAX_ENTRY;
            } else {
                slice[last_entry_index] |= !(MAX_ENTRY << last_entry_bits);
            }
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
        if self.bits.is_none() && self.capacity > 0 {
            let num_words = Self::entry_count(self.capacity);
            let mut buf = VectorBuffer::with_allocator(
                std::mem::size_of::<u64>(),
                num_words,
                self.allocator.clone(),
            );
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
                .make_exclusive();
        }
    }

    pub fn make_exclusive(&mut self) {
        if self.bits.is_some() {
            self.ensure_writable();
        }
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

    /// Resize the validity mask to new size.
    pub fn resize(&mut self, new_size: usize) {
        let old_size = self.capacity;
        if new_size <= old_size {
            self.capacity = new_size;
            return;
        }

        self.capacity = new_size;
        if self.bits.is_some() {
            let new_word_count = Self::entry_count(new_size);
            let old_word_count = Self::entry_count(old_size);

            let mut new_buf = VectorBuffer::with_allocator(
                std::mem::size_of::<u64>(),
                new_word_count,
                self.allocator.clone(),
            );

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
        }
    }

    /// Initialize with the contents of another mask.
    pub fn initialize(&mut self, other: &ValidityMask) {
        self.bits = other.bits.clone();
        self.capacity = other.capacity;
    }

    /// Copy validity data from another mask.
    pub fn copy(&mut self, other: &ValidityMask, count: usize) {
        self.capacity = count;
        if other.all_valid() {
            self.bits = None;
        } else {
            // Deep copy the data
            let num_words = Self::entry_count(count);
            let mut buf = VectorBuffer::with_allocator(
                std::mem::size_of::<u64>(),
                num_words,
                self.allocator.clone(),
            );
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
        if other.all_valid() {
            // X & 1 = X
            return;
        }
        if self.all_valid() {
            // 1 & Y = Y
            self.initialize(other);
            return;
        }

        // Check if they share the same buffer
        if let (Some(self_bits), Some(other_bits)) = (&self.bits, &other.bits) {
            unsafe {
                if std::ptr::eq(
                    self_bits.as_slice::<u64>(1).as_ptr(),
                    other_bits.as_slice::<u64>(1).as_ptr(),
                ) {
                    // X & X = X
                    return;
                }
            }
        }

        // Have to merge - create new mask with combined data
        let entry_count = Self::entry_count(count);
        let mut new_buf = VectorBuffer::with_allocator(
            std::mem::size_of::<u64>(),
            entry_count,
            self.allocator.clone(),
        );

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
    }

    /// Slice the validity mask from source_offset for count elements.
    pub fn slice(&mut self, other: &ValidityMask, source_offset: usize, count: usize) {
        self.capacity = count;
        if other.all_valid() {
            self.bits = None;
            return;
        }
        if source_offset == 0 {
            self.copy(other, count);
            return;
        }

        // Create a new mask and copy data
        let mut new_mask = ValidityMask::new(count);
        new_mask.slice_in_place(other, 0, source_offset, count);
        self.initialize(&new_mask);
    }

    /// Slice validity in place with bit-level precision.
    pub fn slice_in_place(
        &mut self,
        other: &ValidityMask,
        target_offset: usize,
        source_offset: usize,
        count: usize,
    ) {
        if self.all_valid() && other.all_valid() {
            return;
        }

        self.ensure_writable();

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
                    for i in 0..count {
                        self.set(target_offset + i, true);
                    }
                }
            }
        } else {
            // Fallback: bit-by-bit copy
            for i in 0..count {
                self.set(target_offset + i, other.is_valid(source_offset + i));
            }
        }
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
        if !other.is_mask_set() && !self.is_mask_set() {
            // No need to copy if neither has null values
            return;
        }

        // Use selection vector
        for i in 0..copy_count {
            let source_idx = sel.get(source_offset + i);
            self.set(target_offset + i, other.is_valid(source_idx));
        }
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
}
