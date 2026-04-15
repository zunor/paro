// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! HtEntry - Hash table entry structure.
//!
//! ## Design
//! The ht_entry_t stores a salt (upper 16 bits) and pointer (lower 48 bits)
//! in a single u64 value. This allows:
//! - Quick salt comparison to filter out non-matching entries
//! - Direct pointer access to the row data
//! - Pointer chaining for handling collisions

use std::ptr::NonNull;

/// Hash table entry that combines salt and pointer in a single u64.
///
/// Layout:
/// ```text
/// | 16 bits salt | 48 bits pointer |
/// ```
///
/// The salt is extracted from the hash value and used for quick filtering.
/// The pointer points to the row data materialized inside `HashBuildStore`.
#[derive(Debug, Clone, Copy, Default)]
pub struct HtEntry {
    value: u64,
}

impl HtEntry {
    /// Upper 16 bits are salt, lower 48 bits are pointer.
    pub const SALT_MASK: u64 = 0xFFFF_0000_0000_0000;
    pub const POINTER_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    /// Create an empty entry.
    #[inline]
    pub fn empty() -> Self {
        Self { value: 0 }
    }

    /// Create an entry with the given salt and pointer.
    #[inline]
    pub fn new(salt: u64, pointer: *const u8) -> Self {
        let ptr_value = pointer as u64;
        debug_assert!(
            (ptr_value & Self::SALT_MASK) == 0,
            "Pointer uses upper 16 bits"
        );
        Self {
            value: ptr_value | (salt & Self::SALT_MASK),
        }
    }

    /// Check if the entry is occupied (non-empty).
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.value != 0
    }

    /// Get the pointer from the entry.
    ///
    /// # Safety
    /// The caller must ensure the entry is occupied.
    #[inline]
    pub fn get_pointer(&self) -> *const u8 {
        debug_assert!(self.is_occupied());
        (self.value & Self::POINTER_MASK) as *const u8
    }

    /// Get the pointer from the entry, returning None if empty.
    #[inline]
    pub fn get_pointer_or_null(&self) -> Option<NonNull<u8>> {
        if self.is_occupied() {
            NonNull::new((self.value & Self::POINTER_MASK) as *mut u8)
        } else {
            None
        }
    }

    /// Extract salt from a hash value.
    ///
    /// Returns the salt with lower bits set to all 1s (for easy comparison).
    #[inline]
    pub fn extract_salt(hash: u64) -> u64 {
        hash | Self::POINTER_MASK
    }

    /// Get the salt from this entry.
    #[inline]
    pub fn get_salt(&self) -> u64 {
        Self::extract_salt(self.value)
    }

    /// Get the salt bits only (upper 16 bits, lower bits zeroed).
    #[inline]
    pub fn get_salt_bits(&self) -> u64 {
        self.value & Self::SALT_MASK
    }

    /// Set the pointer for this entry, preserving the salt.
    #[inline]
    pub fn set_pointer(&mut self, pointer: *const u8) {
        let ptr_value = pointer as u64;
        debug_assert!(
            (ptr_value & Self::SALT_MASK) == 0,
            "Pointer uses upper 16 bits"
        );
        // Preserve salt, set new pointer
        self.value = (self.value & Self::SALT_MASK) | ptr_value;
    }

    /// Get the raw value.
    #[inline]
    pub fn raw_value(&self) -> u64 {
        self.value
    }
}

/// Increment offset and wrap around using bitmask (power of 2 capacity).
///
/// This is more efficient than modulo for power-of-2 sizes.
#[inline]
pub fn increment_and_wrap(offset: &mut usize, capacity_mask: usize) {
    *offset = (*offset + 1) & capacity_mask;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_entry() {
        let entry = HtEntry::empty();
        assert!(!entry.is_occupied());
        assert!(entry.get_pointer_or_null().is_none());
    }

    #[test]
    fn test_entry_with_pointer() {
        let data: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let ptr = data.as_ptr();
        let salt = 0xABCD_0000_0000_0000u64; // Only upper 16 bits

        let entry = HtEntry::new(salt, ptr);

        assert!(entry.is_occupied());
        assert_eq!(entry.get_pointer(), ptr);
        assert_eq!(entry.get_salt_bits(), salt);
    }

    #[test]
    fn test_salt_extraction() {
        let hash: u64 = 0x1234_5678_9ABC_DEF0;
        let salt = HtEntry::extract_salt(hash);

        // Salt should have upper bits from hash, lower bits all 1s
        assert_eq!(salt & HtEntry::SALT_MASK, hash & HtEntry::SALT_MASK);
        assert_eq!(salt & HtEntry::POINTER_MASK, HtEntry::POINTER_MASK);
    }

    #[test]
    fn test_increment_and_wrap() {
        let capacity = 16; // Power of 2
        let mask = capacity - 1;

        let mut offset = 15;
        increment_and_wrap(&mut offset, mask);
        assert_eq!(offset, 0); // Wrapped around

        let mut offset2 = 5;
        increment_and_wrap(&mut offset2, mask);
        assert_eq!(offset2, 6); // Normal increment
    }
}
