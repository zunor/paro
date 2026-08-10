// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Implementation Notes
//! - Algorithms from "New cardinality estimation algorithms for HyperLogLog sketches"
//!   by Otmar Ertl, arXiv:1702.01284
//! - P = 10, M = 1024 registers
//! - Uses Sigma/Tau functions for improved accuracy (from Redis)
//!
//! ## Known Limitations
//! - Does not support Vector-based batch updates (will be added when needed)
//! - Only supports HLL_V3 format for serialization

use std::f64::consts::PI;
use std::io::{Read, Write};

use crate::error::{self as paro_error, ParoError, Result};

/// Storage type for HyperLogLog serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HllStorageType {
    /// Legacy Redis HLL format (not supported for writing)
    HllV1 = 1,
    /// Our own compact implementation
    HllV2 = 2,
    /// Precision-tagged register representation
    HllV3 = 3,
}

impl TryFrom<u8> for HllStorageType {
    type Error = ParoError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(HllStorageType::HllV1),
            2 => Ok(HllStorageType::HllV2),
            3 => Ok(HllStorageType::HllV3),
            _ => Err(paro_error::internal(format!(
                "Unknown HyperLogLog storage type: {}",
                value
            ))),
        }
    }
}

/// HyperLogLog for cardinality estimation.
///
/// Implements algorithms from "New cardinality estimation algorithms for HyperLogLog sketches"
/// by Otmar Ertl, arXiv:1702.01284.
///
/// # Example
/// ```
/// use paro_common::collections::HyperLogLog;
///
/// let mut hll = HyperLogLog::new();
///
/// // Insert some hash values
/// hll.insert_element(0x123456789ABCDEF0);
/// hll.insert_element(0xFEDCBA9876543210);
/// hll.insert_element(0x123456789ABCDEF0); // Duplicate
///
/// // Get estimated cardinality
/// let count = hll.count();
/// assert!(count >= 1 && count <= 3); // Approximate count
/// ```
#[derive(Clone)]
pub struct HyperLogLog {
    /// Register array (M = 1024 registers)
    k: [u8; Self::M],
}

impl HyperLogLog {
    /// Number of bits used for register index (P = 10)
    pub const P: usize = 10;

    /// Number of bits used for leading zeros count (Q = 64 - P = 58)
    pub const Q: usize = 64 - Self::P;

    /// Number of registers (M = 2^P = 1024)
    pub const M: usize = 1 << Self::P;

    /// Alpha constant: 1 / (2 * ln(2))
    pub const ALPHA: f64 = 0.721_347_520_444_481_7;

    /// Create a new empty HyperLogLog.
    pub fn new() -> Self {
        Self { k: [0u8; Self::M] }
    }

    /// Get the error rate for this HLL configuration.
    ///
    /// For P=10, M=1024, the error rate is approximately 3.9%.
    #[inline]
    pub fn error_rate() -> f64 {
        (PI / 2.0).sqrt() / (Self::M as f64).sqrt()
    }

    /// Insert an element by its hash value (Algorithm 1).
    ///
    /// # Arguments
    /// * `hash` - 64-bit hash value of the element
    #[inline]
    pub fn insert_element(&mut self, hash: u64) {
        // Extract register index from lowest P bits
        let i = (hash & ((1 << Self::P) - 1)) as usize;

        // Shift right by P bits and set the (Q+1)th bit to ensure we count at least 1
        let mut h = hash >> Self::P;
        h |= 1u64 << Self::Q;

        // Count trailing zeros + 1
        let z = (h.trailing_zeros() + 1) as u8;

        self.update(i, z);
    }

    /// Update a register with a new value (keeps maximum).
    #[inline]
    pub fn update(&mut self, i: usize, z: u8) {
        self.k[i] = self.k[i].max(z);
    }

    /// Get the value of a register.
    #[inline]
    pub fn get_register(&self, i: usize) -> u8 {
        self.k[i]
    }

    /// Estimate the cardinality (number of distinct elements).
    ///
    /// Uses Algorithm 6 from the Ertl paper with Sigma/Tau corrections.
    pub fn count(&self) -> usize {
        let mut c = [0u32; Self::Q + 2];
        self.extract_counts(&mut c);
        Self::estimate_cardinality(&c) as usize
    }

    /// Merge another HyperLogLog into this one (Algorithm 2).
    ///
    /// After merging, this HLL represents the union of both sets.
    pub fn merge(&mut self, other: &HyperLogLog) {
        for i in 0..Self::M {
            self.update(i, other.k[i]);
        }
    }

    /// Create a copy of this HyperLogLog.
    pub fn copy(&self) -> Self {
        Self { k: self.k }
    }

    /// Extract register value counts (Algorithm 4).
    ///
    /// c[j] = number of registers with value j
    fn extract_counts(&self, c: &mut [u32]) {
        for i in 0..Self::M {
            c[self.k[i] as usize] += 1;
        }
    }

    /// Estimate cardinality from register counts (Algorithm 6).
    fn estimate_cardinality(c: &[u32]) -> i64 {
        let m = Self::M as f64;

        // Start with tau term
        let mut z = m * hll_tau((m - c[Self::Q] as f64) / m);

        // Process counts from Q down to 1
        for k in (1..=Self::Q).rev() {
            z += c[k] as f64;
            z *= 0.5;
        }

        // Add sigma term
        z += m * hll_sigma(c[0] as f64 / m);

        // Final estimate
        (Self::ALPHA * m * m / z).round() as i64
    }

    /// Serialize the HyperLogLog to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        // Precision is part of the storage contract: a register array cannot
        // be interpreted correctly after P changes.
        w.write_all(&[HllStorageType::HllV3 as u8, Self::P as u8])?;
        // Write register data
        w.write_all(&self.k)?;
        Ok(())
    }

    /// Deserialize a HyperLogLog from a reader.
    pub fn deserialize<R: Read>(r: &mut R) -> Result<Self> {
        // Read storage type
        let mut type_buf = [0u8; 1];
        r.read_exact(&mut type_buf)?;
        let storage_type = HllStorageType::try_from(type_buf[0])?;

        match storage_type {
            HllStorageType::HllV1 => {
                // V1 format not supported for deserialization
                Err(paro_error::not_implemented(
                    "HyperLogLog V1 format deserialization not supported",
                ))
            }
            HllStorageType::HllV2 => Err(paro_error::not_implemented(
                "HyperLogLog V2 format deserialization not supported",
            )),
            HllStorageType::HllV3 => {
                let mut precision = [0u8; 1];
                r.read_exact(&mut precision)?;
                if precision[0] as usize != Self::P {
                    return Err(paro_error::internal(format!(
                        "HyperLogLog precision mismatch: stored P={}, expected P={}",
                        precision[0],
                        Self::P
                    )));
                }
                let mut hll = Self::new();
                r.read_exact(&mut hll.k)?;
                Ok(hll)
            }
        }
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + Self::M);
        // This cannot fail for Vec
        let _ = self.serialize(&mut buf);
        buf
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor)
    }

    /// Check if the HLL is empty (all registers are zero).
    pub fn is_empty(&self) -> bool {
        self.k.iter().all(|&v| v == 0)
    }

    /// Reset the HLL to empty state.
    pub fn clear(&mut self) {
        self.k.fill(0);
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog")
            .field("count", &self.count())
            .field(
                "registers",
                &format!("[{} non-zero]", self.k.iter().filter(|&&v| v > 0).count()),
            )
            .finish()
    }
}

/// Sigma function for HLL estimation (from Redis).
///
/// Used in Algorithm 6 for improved accuracy.
fn hll_sigma(x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }

    let mut x = x;
    let mut y = 1.0;
    let mut z = x;

    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;

        if z_prime == z {
            break;
        }
    }

    z
}

/// Tau function for HLL estimation (from Redis).
///
/// Used in Algorithm 6 for improved accuracy.
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }

    let mut x = x;
    let mut y = 1.0;
    let mut z = 1.0 - x;

    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;

        if z_prime == z {
            break;
        }
    }

    z / 3.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MurmurHash3 64-bit finalizer for better hash distribution in tests.
    fn murmur_hash_mix(mut h: u64) -> u64 {
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51AFD7ED558CCD);
        h ^= h >> 33;
        h = h.wrapping_mul(0xC4CEB9FE1A85EC53);
        h ^= h >> 33;
        h
    }

    #[test]
    fn test_new_hll_is_empty() {
        let hll = HyperLogLog::new();
        assert!(hll.is_empty());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_insert_single_element() {
        let mut hll = HyperLogLog::new();
        hll.insert_element(0x123456789ABCDEF0);
        assert!(!hll.is_empty());
        // Count should be approximately 1
        let count = hll.count();
        assert!((1..=2).contains(&count), "Expected ~1, got {}", count);
    }

    #[test]
    fn test_insert_duplicate_elements() {
        let mut hll = HyperLogLog::new();
        let hash = 0xDEADBEEFCAFEBABE;

        // Insert same element multiple times
        for _ in 0..100 {
            hll.insert_element(hash);
        }

        // Count should still be approximately 1
        let count = hll.count();
        assert!((1..=2).contains(&count), "Expected ~1, got {}", count);
    }

    #[test]
    fn test_insert_many_distinct_elements() {
        let mut hll = HyperLogLog::new();

        // Insert 1000 distinct elements using a better hash mixing
        for i in 0..1000u64 {
            // Use MurmurHash3 finalizer for better distribution
            let hash = murmur_hash_mix(i);
            hll.insert_element(hash);
        }

        let count = hll.count();
        // With P=6 (64 registers), error rate is ~15.6%
        // For 1000 elements, expect roughly 700-1500 (wider margin for small register count)
        assert!(
            (500..=2000).contains(&count),
            "Expected ~1000, got {}",
            count
        );
    }

    #[test]
    fn test_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();

        // Insert different elements into each HLL
        for i in 0..500u64 {
            hll1.insert_element(murmur_hash_mix(i));
        }
        for i in 500..1000u64 {
            hll2.insert_element(murmur_hash_mix(i));
        }

        let count1 = hll1.count();
        let count2 = hll2.count();

        // Merge hll2 into hll1
        hll1.merge(&hll2);

        let merged_count = hll1.count();

        // Merged count should be approximately sum (since no overlap)
        // With P=6, error can be significant
        assert!(
            (500..=2000).contains(&merged_count),
            "Expected ~1000, got {} (was {} + {})",
            merged_count,
            count1,
            count2
        );
    }

    #[test]
    fn test_merge_with_overlap() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();

        // Insert overlapping elements
        for i in 0..500u64 {
            let hash = murmur_hash_mix(i);
            hll1.insert_element(hash);
            hll2.insert_element(hash);
        }

        // Merge should not increase count significantly
        let count_before = hll1.count();
        hll1.merge(&hll2);
        let count_after = hll1.count();

        // Count should be similar (within error margin)
        let diff = (count_after as i64 - count_before as i64).unsigned_abs();
        assert!(
            diff <= 200,
            "Merge with overlap changed count too much: {} -> {}",
            count_before,
            count_after
        );
    }

    #[test]
    fn test_copy() {
        let mut hll = HyperLogLog::new();
        for i in 0..100u64 {
            hll.insert_element(murmur_hash_mix(i));
        }

        let copy = hll.copy();
        assert_eq!(hll.count(), copy.count());

        // Verify registers are equal
        for i in 0..HyperLogLog::M {
            assert_eq!(hll.get_register(i), copy.get_register(i));
        }
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut hll = HyperLogLog::new();
        for i in 0..100u64 {
            hll.insert_element(murmur_hash_mix(i));
        }

        let bytes = hll.to_bytes();
        assert_eq!(bytes[0], HllStorageType::HllV3 as u8);
        assert_eq!(bytes[1], HyperLogLog::P as u8);
        let restored = HyperLogLog::from_bytes(&bytes).expect("Deserialization failed");

        assert_eq!(hll.count(), restored.count());

        // Verify registers are equal
        for i in 0..HyperLogLog::M {
            assert_eq!(hll.get_register(i), restored.get_register(i));
        }
    }

    #[test]
    fn test_clear() {
        let mut hll = HyperLogLog::new();
        for i in 0..100u64 {
            hll.insert_element(murmur_hash_mix(i));
        }

        assert!(!hll.is_empty());
        hll.clear();
        assert!(hll.is_empty());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_error_rate() {
        let error_rate = HyperLogLog::error_rate();
        // For P=10, M=1024, error rate should be approximately 3.9%.
        assert!(
            error_rate > 0.03 && error_rate < 0.05,
            "Error rate {} not in expected range",
            error_rate
        );
    }

    #[test]
    fn test_deserialize_invalid_type() {
        let bytes = [0xFF, 0, 0, 0]; // Invalid storage type
        let result = HyperLogLog::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_legacy_precisionless_format() {
        let result = HyperLogLog::from_bytes(&[HllStorageType::HllV2 as u8]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_different_precision() {
        let result =
            HyperLogLog::from_bytes(&[HllStorageType::HllV3 as u8, (HyperLogLog::P - 1) as u8]);
        assert!(result.is_err());
    }
}
