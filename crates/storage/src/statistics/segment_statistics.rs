// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Statistics
//!
//! Simple wrapper around BaseStatistics for column segments.
//!
//! ## Design Notes
//! SegmentStatistics wraps BaseStatistics with mutex-backed access per segment.
//! The current implementation favors straightforward per-segment locking.

use super::BaseStatistics;
use paro_common::types::LogicalType;
use std::sync::{Arc, Mutex};

/// Statistics for a column segment.
///
/// A wrapper around BaseStatistics with thread-safe access via Mutex.
#[derive(Debug, Clone)]
pub struct SegmentStatistics {
    /// Type-specific statistics of the segment (thread-safe)
    pub statistics: Arc<Mutex<BaseStatistics>>,
    /// The logical type (cached for quick access)
    data_type: LogicalType,
}

impl SegmentStatistics {
    /// Create new segment statistics with empty statistics for the given type.
    ///
    /// # Arguments
    /// * `data_type` - The logical type of the column
    pub fn new(data_type: LogicalType) -> Self {
        Self {
            statistics: Arc::new(Mutex::new(BaseStatistics::create_empty(data_type.clone()))),
            data_type,
        }
    }

    /// Create segment statistics from existing BaseStatistics.
    ///
    /// # Arguments
    /// * `stats` - The base statistics to wrap
    pub fn from_stats(stats: BaseStatistics) -> Self {
        let data_type = stats.get_type().clone();
        Self {
            statistics: Arc::new(Mutex::new(stats)),
            data_type,
        }
    }

    /// Get a clone of the underlying statistics.
    ///
    /// This acquires the lock and returns a cloned copy.
    pub fn get(&self) -> BaseStatistics {
        self.statistics.lock().unwrap().clone()
    }

    /// Get the logical type of this segment.
    pub fn logical_type(&self) -> &LogicalType {
        &self.data_type
    }

    /// Merge other statistics into this one.
    ///
    /// # Arguments
    /// * `other` - The statistics to merge from
    pub fn merge(&self, other: &SegmentStatistics) {
        let other_stats = other.statistics.lock().unwrap();
        self.statistics.lock().unwrap().merge(&other_stats);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_integer() {
        let stats = SegmentStatistics::new(LogicalType::Integer);
        assert_eq!(stats.logical_type(), &LogicalType::Integer);
        let base = stats.get();
        assert!(!base.can_have_null());
    }

    #[test]
    fn test_new_varchar() {
        let stats = SegmentStatistics::new(LogicalType::Varchar);
        assert_eq!(stats.logical_type(), &LogicalType::Varchar);
    }

    #[test]
    fn test_from_stats() {
        let base = BaseStatistics::create_unknown(LogicalType::BigInt);
        let stats = SegmentStatistics::from_stats(base);
        assert_eq!(stats.logical_type(), &LogicalType::BigInt);
        // Unknown stats have has_null = true
        let retrieved = stats.get();
        assert!(retrieved.can_have_null());
    }

    #[test]
    fn test_clone() {
        let stats1 = SegmentStatistics::new(LogicalType::Double);
        let stats2 = stats1.clone();
        assert_eq!(stats1.logical_type(), stats2.logical_type());
    }

    #[test]
    fn test_get_returns_clone() {
        let stats = SegmentStatistics::new(LogicalType::Integer);
        let base1 = stats.get();
        let base2 = stats.get();
        // Both should be independent clones
        assert_eq!(base1.get_type(), base2.get_type());
    }

    #[test]
    fn test_merge() {
        let stats1 = SegmentStatistics::new(LogicalType::Integer);
        let stats2 = SegmentStatistics::new(LogicalType::Integer);

        // Merge stats2 into stats1
        stats1.merge(&stats2);

        // Should still be valid
        assert_eq!(stats1.logical_type(), &LogicalType::Integer);
    }
}
