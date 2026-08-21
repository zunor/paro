// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - Uses HyperLogLog for cardinality estimation
//! - Observes every written non-NULL value so estimates remain mergeable
//! - Uses a bounded HyperLogLog sketch per column segment

use paro_common::collections::HyperLogLog;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use std::io::{Read, Write};

/// Statistics for tracking distinct values using HyperLogLog.
///
/// This structure uses HyperLogLog for cardinality estimation. Every written
/// non-NULL value is observed: prefix sampling is not statistically valid for
/// clustered or sorted storage, and sampled sketches cannot be merged into a
/// trustworthy global NDV without retaining a sampling model.
///
/// # Example
/// ```ignore
/// use crate::statistics::DistinctStatistics;
///
/// let mut stats = DistinctStatistics::new();
///
/// // Update with hash values
/// let hashes = vec![0x123456789ABCDEF0, 0xFEDCBA9876543210];
/// stats.update(&hashes, hashes.len());
///
/// // Get estimated distinct count
/// let count = stats.get_count();
/// ```
#[derive(Debug)]
pub struct DistinctStatistics {
    /// HyperLogLog for cardinality estimation
    log: HyperLogLog,
    /// How many non-NULL values have been observed.
    total_count: usize,
}

impl DistinctStatistics {
    /// Create a new empty DistinctStatistics.
    pub fn new() -> Self {
        Self {
            log: HyperLogLog::new(),
            total_count: 0,
        }
    }

    /// Create DistinctStatistics with existing data.
    ///
    /// # Arguments
    /// * `log` - Existing HyperLogLog
    /// * `total_count` - Total number of observed non-NULL values
    pub fn with_data(log: HyperLogLog, total_count: usize) -> Self {
        Self { log, total_count }
    }

    /// Merge another DistinctStatistics into this one.
    ///
    /// After merging, this statistics represents the union of both sets.
    pub fn merge(&mut self, other: &DistinctStatistics) {
        self.log.merge(&other.log);
        self.total_count = self.total_count.saturating_add(other.total_count);
    }

    /// Create a copy of this DistinctStatistics.
    pub fn copy(&self) -> Self {
        Self {
            log: self.log.copy(),
            total_count: self.total_count,
        }
    }

    /// Update statistics without sampling.
    ///
    /// All provided hash values are inserted into the HyperLogLog.
    ///
    /// # Arguments
    /// * `hashes` - Hash values of the data
    /// * `count` - Number of values to process
    pub fn update(&mut self, hashes: &[u64], count: usize) {
        let actual_count = count.min(hashes.len());
        self.total_count = self.total_count.saturating_add(actual_count);
        for &hash in hashes.iter().take(actual_count) {
            self.log.insert_element(hash);
        }
    }

    /// Get the estimated distinct count.
    ///
    /// Returns 0 if no values have been inserted.
    pub fn get_count(&self) -> usize {
        if self.total_count == 0 {
            return 0;
        }
        self.log.count().min(self.total_count)
    }

    /// Get the raw HLL count without extrapolation.
    pub fn get_raw_count(&self) -> usize {
        self.log.count()
    }

    /// Get the total count.
    pub fn get_total_count(&self) -> usize {
        self.total_count
    }

    /// Check if a type is supported for distinct statistics.
    ///
    /// Nested types (LIST, STRUCT, ARRAY) and trivial types (BOOLEAN)
    /// are not supported.
    pub fn type_is_supported(ty: &LogicalType) -> bool {
        match ty {
            // Nested types are not supported
            LogicalType::List(_) | LogicalType::Struct(_) | LogicalType::Array(_, _) => false,
            // Boolean doesn't make much sense for distinct statistics
            LogicalType::Boolean => false,
            // All other types are supported
            _ => true,
        }
    }

    /// Convert to a string representation.
    pub fn to_display_string(&self) -> String {
        format!("[Approx Unique: {}]", self.get_count())
    }

    /// Check if the statistics is empty.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Reset the statistics to empty state.
    pub fn clear(&mut self) {
        self.log.clear();
        self.total_count = 0;
    }

    /// Serialize the DistinctStatistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        // Write total_count
        let total_count = self.total_count as u64;
        w.write_all(&total_count.to_le_bytes())?;

        // Write HLL
        self.log.serialize(w)?;

        Ok(())
    }

    /// Deserialize a DistinctStatistics from a reader.
    pub fn deserialize<R: Read>(r: &mut R) -> Result<Self> {
        // Read total_count
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        let total_count = u64::from_le_bytes(buf) as usize;

        // Read HLL
        let log = HyperLogLog::deserialize(r)?;

        Ok(Self::with_data(log, total_count))
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // This cannot fail for Vec
        let _ = self.serialize(&mut buf);
        buf
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor)
    }
}

impl Default for DistinctStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DistinctStatistics {
    fn clone(&self) -> Self {
        self.copy()
    }
}

impl std::fmt::Display for DistinctStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
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
    fn test_new_is_empty() {
        let stats = DistinctStatistics::new();
        assert!(stats.is_empty());
        assert_eq!(stats.get_count(), 0);
        assert_eq!(stats.get_total_count(), 0);
    }

    #[test]
    fn test_update_single_value() {
        let mut stats = DistinctStatistics::new();
        let hashes = vec![murmur_hash_mix(42)];
        stats.update(&hashes, 1);

        assert!(!stats.is_empty());
        assert_eq!(stats.get_total_count(), 1);
        // Count should be approximately 1
        let count = stats.get_count();
        assert!(count >= 1 && count <= 2, "Expected ~1, got {}", count);
    }

    #[test]
    fn test_update_many_distinct_values() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..1000u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        assert_eq!(stats.get_total_count(), 1000);

        let count = stats.get_count();
        // With HLL error rate, expect roughly 700-1500
        assert!(
            count >= 500 && count <= 2000,
            "Expected ~1000, got {}",
            count
        );
    }

    #[test]
    fn full_observation_handles_more_than_one_vector() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..10000u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        assert_eq!(stats.get_total_count(), 10000);
        let count = stats.get_count();
        assert!(
            (9_000..=11_000).contains(&count),
            "Expected ~10000, got {count}"
        );
    }

    #[test]
    fn test_merge() {
        let mut stats1 = DistinctStatistics::new();
        let mut stats2 = DistinctStatistics::new();

        // Insert different values into each
        let hashes1: Vec<u64> = (0..500u64).map(murmur_hash_mix).collect();
        let hashes2: Vec<u64> = (500..1000u64).map(murmur_hash_mix).collect();

        stats1.update(&hashes1, hashes1.len());
        stats2.update(&hashes2, hashes2.len());

        let count1 = stats1.get_count();
        let count2 = stats2.get_count();

        stats1.merge(&stats2);

        assert_eq!(stats1.get_total_count(), 1000);

        let merged_count = stats1.get_count();
        // Merged count should be approximately sum (no overlap)
        assert!(
            merged_count >= 500 && merged_count <= 2000,
            "Expected ~1000, got {} (was {} + {})",
            merged_count,
            count1,
            count2
        );
    }

    #[test]
    fn test_merge_with_overlap() {
        let mut stats1 = DistinctStatistics::new();
        let mut stats2 = DistinctStatistics::new();

        // Insert same values into both
        let hashes: Vec<u64> = (0..500u64).map(murmur_hash_mix).collect();

        stats1.update(&hashes, hashes.len());
        stats2.update(&hashes, hashes.len());

        let _count_before = stats1.get_count();
        stats1.merge(&stats2);

        // Total count doubles, but distinct count should be similar
        assert_eq!(stats1.get_total_count(), 1000);

        let count_after = stats1.get_count();
        assert!(
            count_after <= 1500,
            "Merged count {} too high for overlapping data",
            count_after
        );
    }

    #[test]
    fn test_copy() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        let copy = stats.copy();

        assert_eq!(stats.get_count(), copy.get_count());
        assert_eq!(stats.get_total_count(), copy.get_total_count());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        let bytes = stats.to_bytes();
        let restored = DistinctStatistics::from_bytes(&bytes).expect("Deserialization failed");

        assert_eq!(stats.get_count(), restored.get_count());
        assert_eq!(stats.get_total_count(), restored.get_total_count());
    }

    #[test]
    fn test_clear() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        assert!(!stats.is_empty());
        stats.clear();
        assert!(stats.is_empty());
        assert_eq!(stats.get_count(), 0);
    }

    #[test]
    fn test_type_is_supported() {
        // Supported types
        assert!(DistinctStatistics::type_is_supported(&LogicalType::Integer));
        assert!(DistinctStatistics::type_is_supported(&LogicalType::BigInt));
        assert!(DistinctStatistics::type_is_supported(&LogicalType::Varchar));
        assert!(DistinctStatistics::type_is_supported(&LogicalType::Double));
        assert!(DistinctStatistics::type_is_supported(&LogicalType::Date));
        assert!(DistinctStatistics::type_is_supported(
            &LogicalType::Timestamp
        ));
        assert!(DistinctStatistics::type_is_supported(
            &LogicalType::TimestampTz
        ));

        // Unsupported types
        assert!(!DistinctStatistics::type_is_supported(&LogicalType::List(
            Box::new(LogicalType::Integer)
        )));
        assert!(!DistinctStatistics::type_is_supported(
            &LogicalType::Struct(vec![("a".to_string(), LogicalType::Integer)])
        ));
        assert!(!DistinctStatistics::type_is_supported(
            &LogicalType::Boolean
        ));
    }

    #[test]
    fn test_to_string() {
        let mut stats = DistinctStatistics::new();
        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update(&hashes, hashes.len());

        let s = stats.to_string();
        assert!(s.contains("Approx Unique"));
    }

    #[test]
    fn test_default() {
        let stats = DistinctStatistics::default();
        assert!(stats.is_empty());
    }

    #[test]
    fn test_display() {
        let stats = DistinctStatistics::new();
        let display = format!("{}", stats);
        assert!(display.contains("Approx Unique"));
    }
}
