// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bloom Filter
//!
//! Simple Bloom Filter implementation for join pre-filtering.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A simple Bloom Filter.
pub struct BloomFilter {
    /// Bitset
    bits: Vec<u64>,
    /// Number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new Bloom Filter.
    pub fn new(num_elements: usize, false_positive_rate: f64) -> Self {
        let num_bits = (-(num_elements as f64) * false_positive_rate.ln() / (2.0f64.ln().powi(2)))
            .ceil() as usize;
        let num_hashes = ((num_bits as f64 / num_elements as f64) * 2.0f64.ln()).round() as usize;

        let num_u64s = num_bits.div_ceil(64);
        Self {
            bits: vec![0; num_u64s],
            num_bits: num_u64s * 64,
            num_hashes: num_hashes.max(1),
        }
    }

    /// Add a value to the Bloom Filter.
    pub fn add<T: Hash>(&mut self, value: &T) {
        let (h1, h2) = self.hash_pair(value);
        for i in 0..self.num_hashes {
            let combined_hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_idx = (combined_hash as usize) % self.num_bits;
            self.bits[bit_idx / 64] |= 1 << (bit_idx % 64);
        }
    }

    /// Check if a value is in the Bloom Filter.
    pub fn contains<T: Hash>(&self, value: &T) -> bool {
        let (h1, h2) = self.hash_pair(value);
        for i in 0..self.num_hashes {
            let combined_hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_idx = (combined_hash as usize) % self.num_bits;
            if (self.bits[bit_idx / 64] & (1 << (bit_idx % 64))) == 0 {
                return false;
            }
        }
        true
    }

    /// Merge another Bloom Filter into this one.
    pub fn merge(&mut self, other: &Self) {
        debug_assert_eq!(self.num_bits, other.num_bits);
        debug_assert_eq!(self.num_hashes, other.num_hashes);
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
    }

    /// Hash a value into two 64-bit hashes for double hashing.
    fn hash_pair<T: Hash>(&self, value: &T) -> (u64, u64) {
        let mut s1 = DefaultHasher::new();
        value.hash(&mut s1);
        let h1 = s1.finish();

        let mut s2 = DefaultHasher::new();
        h1.hash(&mut s2);
        let h2 = s2.finish();

        (h1, h2)
    }
}
