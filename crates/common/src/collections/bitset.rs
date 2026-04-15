// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # FixedBitSet
//!
//! A simple fixed-size bitset backed by `Vec<u64>`, optimized for
//! O(1) set/test/clear and word-level operations.

/// Fixed-size bitset backed by `Vec<u64>`.
#[derive(Clone, Debug, Default)]
pub struct FixedBitSet {
    len_bits: usize,
    words: Vec<u64>,
}

impl FixedBitSet {
    /// Create a new FixedBitSet with `len_bits` bits, initialized to zero.
    pub fn new(len_bits: usize) -> Self {
        let num_words = len_bits.div_ceil(64);
        Self {
            len_bits,
            words: vec![0u64; num_words],
        }
    }

    /// Total number of bits in the bitset.
    pub fn len(&self) -> usize {
        self.len_bits
    }

    /// Returns true if the bitset has 0 capacity.
    pub fn is_empty(&self) -> bool {
        self.len_bits == 0
    }

    /// Number of u64 words backing the bitset.
    pub fn word_len(&self) -> usize {
        self.words.len()
    }

    /// Return true if any bit is set.
    pub fn any(&self) -> bool {
        self.words.iter().any(|&w| w != 0)
    }

    /// Count number of set bits.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Test whether bit at position `idx` is set.
    #[inline]
    pub fn test(&self, idx: u32) -> bool {
        let idx = idx as usize;
        if idx >= self.len_bits {
            return false;
        }
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (self.words[word_idx] >> bit_idx) & 1 != 0
    }

    /// Set bit at position `idx`.
    #[inline]
    pub fn set(&mut self, idx: u32) {
        let idx = idx as usize;
        if idx >= self.len_bits {
            return;
        }
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        self.words[word_idx] |= 1u64 << bit_idx;
    }

    /// Clear bit at position `idx`.
    #[inline]
    pub fn clear(&mut self, idx: u32) {
        let idx = idx as usize;
        if idx >= self.len_bits {
            return;
        }
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        self.words[word_idx] &= !(1u64 << bit_idx);
    }

    /// Reset all bits to zero.
    pub fn reset(&mut self) {
        self.words.fill(0);
    }

    /// In-place union with another bitset (bitwise OR).
    pub fn union_with(&mut self, other: &Self) {
        debug_assert_eq!(self.len_bits, other.len_bits);
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
    }

    /// In-place intersection with another bitset (bitwise AND).
    pub fn intersect_with(&mut self, other: &Self) {
        debug_assert_eq!(self.len_bits, other.len_bits);
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a &= *b;
        }
    }

    /// Iterate over u64 words backing the bitset.
    pub fn iter_words(&self) -> impl Iterator<Item = &u64> {
        self.words.iter()
    }

    /// Expose the underlying word slice.
    pub fn as_slice(&self) -> &[u64] {
        &self.words
    }

    /// Expose the underlying mutable word slice.
    pub fn as_mut_slice(&mut self) -> &mut [u64] {
        &mut self.words
    }
}

#[cfg(test)]
mod tests {
    use super::FixedBitSet;

    #[test]
    fn test_set_test_clear_reset() {
        let mut bitset = FixedBitSet::new(130);
        assert!(!bitset.test(0));
        assert!(!bitset.test(64));
        assert!(!bitset.test(129));

        bitset.set(0);
        bitset.set(64);
        bitset.set(129);
        assert!(bitset.test(0));
        assert!(bitset.test(64));
        assert!(bitset.test(129));

        bitset.clear(64);
        assert!(bitset.test(0));
        assert!(!bitset.test(64));
        assert!(bitset.test(129));

        bitset.reset();
        assert!(!bitset.any());
        assert_eq!(bitset.count_ones(), 0);
    }

    #[test]
    fn test_union_intersection_and_count() {
        let mut a = FixedBitSet::new(128);
        let mut b = FixedBitSet::new(128);
        a.set(1);
        a.set(65);
        b.set(65);
        b.set(90);

        let mut union = a.clone();
        union.union_with(&b);
        assert!(union.test(1));
        assert!(union.test(65));
        assert!(union.test(90));
        assert_eq!(union.count_ones(), 3);

        let mut inter = a.clone();
        inter.intersect_with(&b);
        assert!(!inter.test(1));
        assert!(inter.test(65));
        assert!(!inter.test(90));
        assert_eq!(inter.count_ones(), 1);
    }

    #[test]
    fn test_iter_words() {
        let mut bitset = FixedBitSet::new(70);
        bitset.set(0);
        bitset.set(64);
        let words: Vec<u64> = bitset.iter_words().copied().collect();
        assert_eq!(words.len(), 2);
        assert_eq!(words[0] & 1u64, 1u64);
        assert_eq!((words[1] & 1u64), 1u64);
    }
}
