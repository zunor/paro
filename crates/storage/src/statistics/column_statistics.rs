// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - ColumnStatistics combines BaseStatistics with optional DistinctStatistics
//! - DistinctStatistics is only created for supported types (not nested/boolean)
//! - Provides unified interface for column-level statistics

use std::io::{Read, Write};
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::types::LogicalType;

use super::base_statistics::BaseStatistics;
use super::distinct_statistics::DistinctStatistics;

/// Column-level statistics combining base statistics with distinct statistics.
///
/// This structure provides a unified interface for managing all statistics
/// associated with a single column, including:
/// - Base statistics (min/max, null flags, type-specific stats)
/// - Distinct statistics (approximate unique count via HyperLogLog)
///
/// # Example
/// ```ignore
/// use crate::statistics::ColumnStatistics;
/// use paro_common::types::LogicalType;
///
/// // Create empty statistics for an integer column
/// let stats = ColumnStatistics::create_empty(LogicalType::Integer);
///
/// // Access base statistics
/// let base = stats.statistics();
///
/// // Check if distinct statistics are available
/// if stats.has_distinct_stats() {
///     let distinct = stats.distinct_stats().unwrap();
///     let _ = distinct.get_count();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Base statistics (min/max, null flags, etc.)
    stats: BaseStatistics,
    /// Optional distinct statistics (HyperLogLog-based)
    distinct_stats: Option<DistinctStatistics>,
}

impl ColumnStatistics {
    /// Create new ColumnStatistics from BaseStatistics.
    ///
    /// If the type supports distinct statistics, a new DistinctStatistics
    /// will be automatically created.
    pub fn new(stats: BaseStatistics) -> Self {
        let distinct_stats = if DistinctStatistics::type_is_supported(stats.get_type()) {
            Some(DistinctStatistics::new())
        } else {
            None
        };

        Self {
            stats,
            distinct_stats,
        }
    }

    /// Create new ColumnStatistics with explicit distinct statistics.
    pub fn with_distinct(
        stats: BaseStatistics,
        distinct_stats: Option<DistinctStatistics>,
    ) -> Self {
        Self {
            stats,
            distinct_stats,
        }
    }

    /// Create empty statistics for a given type.
    ///
    /// This is a convenience factory method that creates BaseStatistics::create_empty
    /// and wraps it in ColumnStatistics.
    pub fn create_empty(ty: LogicalType) -> Arc<Self> {
        Arc::new(Self::new(BaseStatistics::create_empty(ty)))
    }

    /// Create unknown statistics for a given type.
    ///
    /// This creates statistics where nothing is known about the data
    /// (has_null=true, has_no_null=true).
    pub fn create_unknown(ty: LogicalType) -> Arc<Self> {
        Arc::new(Self::new(BaseStatistics::create_unknown(ty)))
    }

    /// Merge another ColumnStatistics into this one.
    ///
    /// Both base statistics and distinct statistics are merged.
    pub fn merge(&mut self, other: &ColumnStatistics) {
        self.stats.merge(&other.stats);

        if let (Some(self_distinct), Some(other_distinct)) =
            (&mut self.distinct_stats, &other.distinct_stats)
        {
            self_distinct.merge(other_distinct);
        }
    }

    /// Update distinct statistics with hash values.
    ///
    /// Does nothing if distinct statistics are not available.
    ///
    /// # Arguments
    /// * `hashes` - Hash values of the data
    /// * `count` - Number of values
    pub fn update_distinct_statistics(&mut self, hashes: &[u64], count: usize) {
        if let Some(distinct) = &mut self.distinct_stats {
            distinct.update(hashes, count);
        }
    }

    /// Get a reference to the base statistics.
    pub fn statistics(&self) -> &BaseStatistics {
        &self.stats
    }

    /// Get a mutable reference to the base statistics.
    pub fn statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.stats
    }

    /// Check if distinct statistics are available.
    pub fn has_distinct_stats(&self) -> bool {
        self.distinct_stats.is_some()
    }

    /// Get a reference to the distinct statistics.
    ///
    /// Returns None if distinct statistics are not available.
    pub fn distinct_stats(&self) -> Option<&DistinctStatistics> {
        self.distinct_stats.as_ref()
    }

    /// Get a mutable reference to the distinct statistics.
    ///
    /// Returns None if distinct statistics are not available.
    pub fn distinct_stats_mut(&mut self) -> Option<&mut DistinctStatistics> {
        self.distinct_stats.as_mut()
    }

    /// Set the distinct statistics.
    ///
    /// This replaces any existing distinct statistics.
    pub fn set_distinct(&mut self, distinct_stats: Option<DistinctStatistics>) {
        self.distinct_stats = distinct_stats;
    }

    /// Create a copy of this ColumnStatistics.
    pub fn copy(&self) -> Self {
        Self {
            stats: self.stats.copy(),
            distinct_stats: self.distinct_stats.as_ref().map(|d| d.copy()),
        }
    }

    /// Get the logical type of the column.
    pub fn get_type(&self) -> &LogicalType {
        self.stats.get_type()
    }

    /// Get the estimated distinct count.
    ///
    /// Returns 0 if distinct statistics are not available.
    pub fn get_distinct_count(&self) -> usize {
        self.distinct_stats
            .as_ref()
            .map(|d| d.get_count())
            .unwrap_or(0)
    }

    /// Serialize the ColumnStatistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        // Serialize base statistics
        let stats_bytes = self.stats.to_bytes()?;
        w.write_all(&(stats_bytes.len() as u32).to_le_bytes())?;
        w.write_all(&stats_bytes)?;

        // Serialize distinct statistics presence flag
        let has_distinct = self.distinct_stats.is_some();
        w.write_all(&[has_distinct as u8])?;

        // Serialize distinct statistics if present
        if let Some(distinct) = &self.distinct_stats {
            distinct.serialize(w)?;
        }

        Ok(())
    }

    /// Deserialize a ColumnStatistics from a reader.
    pub fn deserialize<R: Read>(r: &mut R, data_type: LogicalType) -> Result<Self> {
        // Deserialize base statistics
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let stats_len = u32::from_le_bytes(len_buf) as usize;

        let mut stats_bytes = vec![0u8; stats_len];
        r.read_exact(&mut stats_bytes)?;
        let stats = BaseStatistics::from_bytes(&stats_bytes, data_type)?;

        // Deserialize distinct statistics presence flag
        let mut has_distinct_buf = [0u8; 1];
        r.read_exact(&mut has_distinct_buf)?;
        let has_distinct = has_distinct_buf[0] != 0;

        // Deserialize distinct statistics if present
        let distinct_stats = if has_distinct {
            Some(DistinctStatistics::deserialize(r)?)
        } else {
            None
        };

        Ok(Self {
            stats,
            distinct_stats,
        })
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8], data_type: LogicalType) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor, data_type)
    }

    /// Convert to a string representation.
    pub fn to_display_string(&self) -> String {
        let base_str = self.stats.to_display_string();
        let distinct_str = self
            .distinct_stats
            .as_ref()
            .map(|d| d.to_display_string())
            .unwrap_or_default();

        if distinct_str.is_empty() {
            base_str
        } else {
            format!("{}{}", base_str, distinct_str)
        }
    }
}

impl std::fmt::Display for ColumnStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;

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
    fn test_new_integer() {
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        assert!(stats.has_distinct_stats());
        assert_eq!(stats.get_type(), &LogicalType::Integer);
    }

    #[test]
    fn test_new_list_no_distinct() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type));
        // List types don't support distinct statistics
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_new_boolean_no_distinct() {
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Boolean));
        // Boolean types don't support distinct statistics
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_create_empty() {
        let stats = ColumnStatistics::create_empty(LogicalType::Varchar);
        assert!(stats.has_distinct_stats());
        assert!(!stats.statistics().can_have_null());
        assert!(!stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_create_unknown() {
        let stats = ColumnStatistics::create_unknown(LogicalType::BigInt);
        assert!(stats.has_distinct_stats());
        assert!(stats.statistics().can_have_null());
        assert!(stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        let mut stats2 = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));

        // Update base statistics
        stats1.statistics_mut().observe_value(&Value::Integer(10));
        stats2.statistics_mut().observe_value(&Value::Integer(20));

        // Update distinct statistics
        let hashes1: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        let hashes2: Vec<u64> = (100..200u64).map(murmur_hash_mix).collect();
        stats1.update_distinct_statistics(&hashes1, hashes1.len());
        stats2.update_distinct_statistics(&hashes2, hashes2.len());

        let count1 = stats1.get_distinct_count();
        let count2 = stats2.get_distinct_count();

        stats1.merge(&stats2);

        // Check base statistics merged
        assert_eq!(stats1.statistics().min_value(), Some(Value::Integer(10)));
        assert_eq!(stats1.statistics().max_value(), Some(Value::Integer(20)));

        // Check distinct statistics merged
        let merged_count = stats1.get_distinct_count();
        assert!(
            merged_count >= count1.max(count2),
            "Merged count {} should be >= max({}, {})",
            merged_count,
            count1,
            count2
        );
    }

    #[test]
    fn test_update_distinct_statistics() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));

        let hashes: Vec<u64> = (0..1000u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let count = stats.get_distinct_count();
        assert!(count > 0, "Distinct count should be positive");
    }

    #[test]
    fn test_copy() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let copy = stats.copy();

        assert_eq!(
            stats.statistics().min_value(),
            copy.statistics().min_value()
        );
        assert_eq!(
            stats.statistics().max_value(),
            copy.statistics().max_value()
        );
        assert_eq!(stats.get_distinct_count(), copy.get_distinct_count());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored = ColumnStatistics::from_bytes(&bytes, LogicalType::Integer)
            .expect("Deserialization failed");

        assert_eq!(
            stats.statistics().min_value(),
            restored.statistics().min_value()
        );
        assert_eq!(
            stats.statistics().max_value(),
            restored.statistics().max_value()
        );
        assert_eq!(stats.has_distinct_stats(), restored.has_distinct_stats());
        assert_eq!(stats.get_distinct_count(), restored.get_distinct_count());
    }

    #[test]
    fn test_serialize_deserialize_no_distinct() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type.clone()));

        assert!(!stats.has_distinct_stats());

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored =
            ColumnStatistics::from_bytes(&bytes, list_type).expect("Deserialization failed");

        assert!(!restored.has_distinct_stats());
    }

    #[test]
    fn test_with_distinct() {
        let stats = BaseStatistics::create_empty(LogicalType::Integer);
        let distinct = DistinctStatistics::new();

        let col_stats = ColumnStatistics::with_distinct(stats, Some(distinct));
        assert!(col_stats.has_distinct_stats());
    }

    #[test]
    fn test_set_distinct() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Boolean));
        assert!(!stats.has_distinct_stats());

        // Force set distinct stats even for boolean
        stats.set_distinct(Some(DistinctStatistics::new()));
        assert!(stats.has_distinct_stats());

        // Remove distinct stats
        stats.set_distinct(None);
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_get_distinct_count_no_stats() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type));

        // Should return 0 when no distinct stats
        assert_eq!(stats.get_distinct_count(), 0);
    }

    #[test]
    fn test_to_string() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let s = stats.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_display() {
        let stats = ColumnStatistics::create_empty(LogicalType::Varchar);
        let display = format!("{}", stats);
        assert!(!display.is_empty());
    }
}
