// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - StringStatsData: Contains min/max (truncated to 8 bytes), unicode flag, max_string_length
//! - Min/max are stored as fixed-size byte arrays for efficient comparison
//! - Unicode detection for optimization opportunities
//! - StringStats: Static methods for operating on BaseStatistics with string data
//! - CheckZonemap: Zone map filtering for string comparisons
//!
//! This file implements:
//! 1. StringStatsData - the data structure for string statistics
//! 2. StringStats - static methods for operating on BaseStatistics
//! 3. StringStats::check_zonemap - zone map filtering for string types

use std::cmp::Ordering;

use paro_common::expression_type::ExpressionType;
use paro_common::filter_propagate::FilterPropagateResult;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::base_statistics::{BaseStatistics, StatsData};
use super::types::StatsInfo;

/// Maximum string length for min/max storage.
/// Strings longer than this are truncated for statistics purposes.
pub const MAX_STRING_MINMAX_SIZE: usize = 8;

/// String statistics data.
///
/// Contains min/max values (truncated to 8 bytes), unicode flag, and max string length.
/// This mirrors the `StringStatsData` structure.
#[derive(Debug, Clone)]
pub struct StringStatsData {
    /// The minimum value of the segment, potentially truncated to 8 bytes.
    /// Initialized to 0xFF bytes for empty statistics.
    pub min: [u8; MAX_STRING_MINMAX_SIZE],
    /// The maximum value of the segment, potentially truncated to 8 bytes.
    /// Initialized to 0x00 bytes for empty statistics.
    pub max: [u8; MAX_STRING_MINMAX_SIZE],
    /// The actual length of the min value (may be less than MAX_STRING_MINMAX_SIZE).
    pub min_len: usize,
    /// The actual length of the max value (may be less than MAX_STRING_MINMAX_SIZE).
    pub max_len: usize,
    /// Whether or not the column can contain unicode characters.
    pub has_unicode: bool,
    /// Whether or not the maximum string length is known.
    pub has_max_string_length: bool,
    /// The maximum string length in bytes.
    pub max_string_length: u32,
}

impl Default for StringStatsData {
    fn default() -> Self {
        Self::new_unknown()
    }
}

impl StringStatsData {
    /// Create new string stats with unknown values.
    /// - has_unicode: true (assume unicode possible)
    /// - max_string_length: unknown
    /// - min: 0x00 bytes (lowest possible)
    /// - max: 0xFF bytes (highest possible)
    pub fn new_unknown() -> Self {
        Self {
            min: [0x00; MAX_STRING_MINMAX_SIZE],
            max: [0xFF; MAX_STRING_MINMAX_SIZE],
            min_len: 0,
            max_len: MAX_STRING_MINMAX_SIZE,
            has_unicode: true,
            has_max_string_length: false,
            max_string_length: 0,
        }
    }

    /// Create new string stats with empty values.
    /// - has_unicode: false (no data yet)
    /// - max_string_length: 0
    /// - min: 0xFF bytes (will be updated to lower values)
    /// - max: 0x00 bytes (will be updated to higher values)
    pub fn new_empty() -> Self {
        Self {
            min: [0xFF; MAX_STRING_MINMAX_SIZE],
            max: [0x00; MAX_STRING_MINMAX_SIZE],
            min_len: MAX_STRING_MINMAX_SIZE,
            max_len: 0,
            has_unicode: false,
            has_max_string_length: true,
            max_string_length: 0,
        }
    }

    /// Check if the statistics has a maximum string length defined.
    pub fn has_max_string_length(&self) -> bool {
        self.has_max_string_length
    }

    /// Get the maximum string length, if known.
    pub fn max_string_length(&self) -> Option<u32> {
        if self.has_max_string_length {
            Some(self.max_string_length)
        } else {
            None
        }
    }

    /// Check if the strings can contain unicode characters.
    pub fn can_contain_unicode(&self) -> bool {
        self.has_unicode
    }

    /// Get the minimum value as a string (up to MAX_STRING_MINMAX_SIZE bytes).
    pub fn min_string(&self) -> String {
        String::from_utf8_lossy(&self.min[..self.min_len]).to_string()
    }

    /// Get the maximum value as a string (up to MAX_STRING_MINMAX_SIZE bytes).
    pub fn max_string(&self) -> String {
        String::from_utf8_lossy(&self.max[..self.max_len]).to_string()
    }

    /// Get the minimum value as bytes.
    pub fn min_bytes(&self) -> &[u8] {
        &self.min[..self.min_len]
    }

    /// Get the maximum value as bytes.
    pub fn max_bytes(&self) -> &[u8] {
        &self.max[..self.max_len]
    }

    /// Reset the max string length so has_max_string_length() returns false.
    pub fn reset_max_string_length(&mut self) {
        self.has_max_string_length = false;
        self.max_string_length = 0;
    }

    /// Set the maximum string length.
    pub fn set_max_string_length(&mut self, length: u32) {
        self.has_max_string_length = true;
        self.max_string_length = length;
    }

    /// Mark that the column can contain unicode characters.
    pub fn set_contains_unicode(&mut self) {
        self.has_unicode = true;
    }

    /// Set the minimum value from a string.
    pub fn set_min(&mut self, value: &str) {
        let bytes = value.as_bytes();
        let len = bytes.len().min(MAX_STRING_MINMAX_SIZE);
        self.min[..len].copy_from_slice(&bytes[..len]);
        // Pad with 0x00 if shorter
        for i in len..MAX_STRING_MINMAX_SIZE {
            self.min[i] = 0x00;
        }
        self.min_len = len;
    }

    /// Set the maximum value from a string.
    pub fn set_max(&mut self, value: &str) {
        let bytes = value.as_bytes();
        let len = bytes.len().min(MAX_STRING_MINMAX_SIZE);
        self.max[..len].copy_from_slice(&bytes[..len]);
        // Pad with 0xFF if shorter (for proper comparison)
        for i in len..MAX_STRING_MINMAX_SIZE {
            self.max[i] = 0xFF;
        }
        self.max_len = len;
    }

    /// Update the statistics with a new string value.
    pub fn update(&mut self, value: &str) {
        let bytes = value.as_bytes();
        let len = bytes.len();

        // Update max string length
        if self.has_max_string_length && len as u32 > self.max_string_length {
            self.max_string_length = len as u32;
        }

        // Check for unicode
        if !self.has_unicode && !value.is_ascii() {
            self.has_unicode = true;
        }

        // Update min/max
        let truncated_len = len.min(MAX_STRING_MINMAX_SIZE);

        // Compare with current min
        let cmp_min = Self::compare_bytes(&bytes[..truncated_len], self.min_bytes());
        if cmp_min == Ordering::Less {
            self.set_min(value);
        }

        // Compare with current max
        let cmp_max = Self::compare_bytes(&bytes[..truncated_len], self.max_bytes());
        if cmp_max == Ordering::Greater {
            self.set_max(value);
        }
    }

    /// Merge another StringStatsData into this one.
    pub fn merge(&mut self, other: &StringStatsData) {
        // Merge unicode flag
        if other.has_unicode {
            self.has_unicode = true;
        }

        // Merge max string length
        if self.has_max_string_length && other.has_max_string_length {
            self.max_string_length = self.max_string_length.max(other.max_string_length);
        } else {
            // If either doesn't have max string length, we don't know the max
            self.has_max_string_length = false;
        }

        // Merge min
        let cmp_min = Self::compare_bytes(other.min_bytes(), self.min_bytes());
        if cmp_min == Ordering::Less {
            self.min = other.min;
            self.min_len = other.min_len;
        }

        // Merge max
        let cmp_max = Self::compare_bytes(other.max_bytes(), self.max_bytes());
        if cmp_max == Ordering::Greater {
            self.max = other.max;
            self.max_len = other.max_len;
        }
    }

    /// Compare two byte slices lexicographically.
    fn compare_bytes(a: &[u8], b: &[u8]) -> Ordering {
        let min_len = a.len().min(b.len());
        for i in 0..min_len {
            match a[i].cmp(&b[i]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        // If all compared bytes are equal, shorter string is "less"
        a.len().cmp(&b.len())
    }

    /// Check if the statistics represents a constant value (min == max).
    pub fn is_constant(&self) -> bool {
        self.min_len == self.max_len && self.min[..self.min_len] == self.max[..self.max_len]
    }

    /// Serialize the string stats to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        // Total size: 8 (min) + 1 (min_len) + 8 (max) + 1 (max_len) + 1 (flags) + 4 (max_string_length) = 23
        let mut result = Vec::with_capacity(23);

        // min (8 bytes) + min_len (1 byte)
        result.extend_from_slice(&self.min);
        result.push(self.min_len as u8);

        // max (8 bytes) + max_len (1 byte)
        result.extend_from_slice(&self.max);
        result.push(self.max_len as u8);

        // flags (1 byte): bit 0 = has_unicode, bit 1 = has_max_string_length
        let flags = (self.has_unicode as u8) | ((self.has_max_string_length as u8) << 1);
        result.push(flags);

        // max_string_length (4 bytes)
        result.extend_from_slice(&self.max_string_length.to_le_bytes());

        result
    }

    /// Deserialize string stats from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        // Total size: 8 (min) + 1 (min_len) + 8 (max) + 1 (max_len) + 1 (flags) + 4 (max_string_length) = 23
        const SERIALIZED_SIZE: usize =
            MAX_STRING_MINMAX_SIZE + 1 + MAX_STRING_MINMAX_SIZE + 1 + 1 + 4;
        if data.len() < SERIALIZED_SIZE {
            return None;
        }

        let mut offset = 0;

        // min (8 bytes)
        let mut min = [0u8; MAX_STRING_MINMAX_SIZE];
        min.copy_from_slice(&data[offset..offset + MAX_STRING_MINMAX_SIZE]);
        offset += MAX_STRING_MINMAX_SIZE;

        // min_len (1 byte)
        let min_len = data[offset] as usize;
        offset += 1;

        // max (8 bytes)
        let mut max = [0u8; MAX_STRING_MINMAX_SIZE];
        max.copy_from_slice(&data[offset..offset + MAX_STRING_MINMAX_SIZE]);
        offset += MAX_STRING_MINMAX_SIZE;

        // max_len (1 byte)
        let max_len = data[offset] as usize;
        offset += 1;

        // flags (1 byte)
        let flags = data[offset];
        offset += 1;
        let has_unicode = (flags & 1) != 0;
        let has_max_string_length = (flags & 2) != 0;

        // max_string_length (4 bytes)
        let max_string_length = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        Some(Self {
            min,
            max,
            min_len: min_len.min(MAX_STRING_MINMAX_SIZE),
            max_len: max_len.min(MAX_STRING_MINMAX_SIZE),
            has_unicode,
            has_max_string_length,
            max_string_length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_stats_data_new_unknown() {
        let stats = StringStatsData::new_unknown();
        assert!(stats.has_unicode);
        assert!(!stats.has_max_string_length);
        assert_eq!(stats.min, [0x00; MAX_STRING_MINMAX_SIZE]);
        assert_eq!(stats.max, [0xFF; MAX_STRING_MINMAX_SIZE]);
    }

    #[test]
    fn test_string_stats_data_new_empty() {
        let stats = StringStatsData::new_empty();
        assert!(!stats.has_unicode);
        assert!(stats.has_max_string_length);
        assert_eq!(stats.max_string_length, 0);
        assert_eq!(stats.min, [0xFF; MAX_STRING_MINMAX_SIZE]);
        assert_eq!(stats.max, [0x00; MAX_STRING_MINMAX_SIZE]);
    }

    #[test]
    fn test_string_stats_data_update_single() {
        let mut stats = StringStatsData::new_empty();
        stats.update("hello");

        assert_eq!(stats.min_string(), "hello");
        assert_eq!(stats.max_string(), "hello");
        assert_eq!(stats.max_string_length(), Some(5));
        assert!(!stats.has_unicode);
    }

    #[test]
    fn test_string_stats_data_update_multiple() {
        let mut stats = StringStatsData::new_empty();
        stats.update("banana");
        stats.update("apple");
        stats.update("cherry");

        assert_eq!(stats.min_string(), "apple");
        assert_eq!(stats.max_string(), "cherry");
        assert_eq!(stats.max_string_length(), Some(6)); // "banana" and "cherry" are 6 chars
    }

    #[test]
    fn test_string_stats_data_update_unicode() {
        let mut stats = StringStatsData::new_empty();
        stats.update("hello");
        assert!(!stats.has_unicode);

        stats.update("你好");
        assert!(stats.has_unicode);
    }

    #[test]
    fn test_string_stats_data_update_long_string() {
        let mut stats = StringStatsData::new_empty();
        let long_string = "this is a very long string that exceeds 8 bytes";
        stats.update(long_string);

        // Min/max should be truncated to 8 bytes
        assert_eq!(stats.min_len, MAX_STRING_MINMAX_SIZE);
        assert_eq!(stats.max_len, MAX_STRING_MINMAX_SIZE);
        assert_eq!(stats.min_bytes(), b"this is ");
        assert_eq!(stats.max_bytes(), b"this is ");

        // But max_string_length should reflect the full length
        assert_eq!(stats.max_string_length(), Some(long_string.len() as u32));
    }

    #[test]
    fn test_string_stats_data_merge() {
        let mut stats1 = StringStatsData::new_empty();
        stats1.update("banana");
        stats1.update("cherry");

        let mut stats2 = StringStatsData::new_empty();
        stats2.update("apple");
        stats2.update("date");

        stats1.merge(&stats2);

        assert_eq!(stats1.min_string(), "apple");
        assert_eq!(stats1.max_string(), "date");
    }

    #[test]
    fn test_string_stats_data_merge_unicode() {
        let mut stats1 = StringStatsData::new_empty();
        stats1.update("hello");
        assert!(!stats1.has_unicode);

        let mut stats2 = StringStatsData::new_empty();
        stats2.update("世界");

        stats1.merge(&stats2);
        assert!(stats1.has_unicode);
    }

    #[test]
    fn test_string_stats_data_merge_max_length() {
        let mut stats1 = StringStatsData::new_empty();
        stats1.update("short");

        let mut stats2 = StringStatsData::new_empty();
        stats2.update("much longer string");

        stats1.merge(&stats2);
        assert_eq!(stats1.max_string_length(), Some(18));
    }

    #[test]
    fn test_string_stats_data_is_constant() {
        let mut stats = StringStatsData::new_empty();
        stats.update("same");
        assert!(stats.is_constant());

        stats.update("different");
        assert!(!stats.is_constant());
    }

    #[test]
    fn test_string_stats_data_set_min_max() {
        let mut stats = StringStatsData::new_empty();
        stats.set_min("aaa");
        stats.set_max("zzz");

        assert_eq!(stats.min_string(), "aaa");
        assert_eq!(stats.max_string(), "zzz");
    }

    #[test]
    fn test_string_stats_data_serialize_deserialize() {
        let mut stats = StringStatsData::new_empty();
        stats.update("hello");
        stats.update("world");
        stats.set_contains_unicode();

        let serialized = stats.serialize();
        let deserialized = StringStatsData::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.min_string(), stats.min_string());
        assert_eq!(deserialized.max_string(), stats.max_string());
        assert_eq!(deserialized.has_unicode, stats.has_unicode);
        assert_eq!(
            deserialized.has_max_string_length,
            stats.has_max_string_length
        );
        assert_eq!(deserialized.max_string_length, stats.max_string_length);
    }

    #[test]
    fn test_string_stats_data_compare_bytes() {
        assert_eq!(
            StringStatsData::compare_bytes(b"apple", b"banana"),
            Ordering::Less
        );
        assert_eq!(
            StringStatsData::compare_bytes(b"cherry", b"banana"),
            Ordering::Greater
        );
        assert_eq!(
            StringStatsData::compare_bytes(b"same", b"same"),
            Ordering::Equal
        );
        // Shorter string is "less" when prefix matches
        assert_eq!(
            StringStatsData::compare_bytes(b"app", b"apple"),
            Ordering::Less
        );
    }

    #[test]
    fn test_string_stats_data_reset_max_length() {
        let mut stats = StringStatsData::new_empty();
        stats.update("hello");
        assert!(stats.has_max_string_length);

        stats.reset_max_string_length();
        assert!(!stats.has_max_string_length);
        assert_eq!(stats.max_string_length(), None);
    }

    #[test]
    fn test_string_stats_data_empty_string() {
        let mut stats = StringStatsData::new_empty();
        stats.update("");

        assert_eq!(stats.min_len, 0);
        assert_eq!(stats.max_len, 0);
        assert_eq!(stats.max_string_length(), Some(0));
    }
}

// ============================================================================
// StringStats - Static methods for operating on BaseStatistics
// ============================================================================

/// Static methods for operating on string statistics within BaseStatistics.
///
/// This mirrors the `StringStats` struct which provides static methods
/// for creating, accessing, and manipulating string statistics.
///
/// ## Usage
/// ```ignore
/// let stats = StringStats::create_unknown(LogicalType::Varchar);
/// StringStats::update(&mut stats, "hello");
/// let min = StringStats::min(&stats);
/// let max = StringStats::max(&stats);
/// ```
pub struct StringStats;

impl StringStats {
    /// Create unknown statistics for a string type.
    /// "has_unicode" is true, "max_string_length" is unknown, "min" is \0, max is \xFF.
    /// This can be used when nothing is known about the data.
    pub fn create_unknown(data_type: LogicalType) -> BaseStatistics {
        let mut stats = BaseStatistics::create_unknown(data_type);
        // Ensure string data is set to unknown state
        if let StatsData::String(data) = stats.stats_data_mut() {
            *data = StringStatsData::new_unknown();
        }
        stats
    }

    /// Create empty statistics for a string type.
    /// "has_unicode" is false, "max_string_length" is 0, "min" is \xFF, max is \x00.
    /// This is used when incrementally constructing statistics.
    pub fn create_empty(data_type: LogicalType) -> BaseStatistics {
        let mut stats = BaseStatistics::create_empty(data_type);
        // Ensure string data is set to empty state
        if let StatsData::String(data) = stats.stats_data_mut() {
            *data = StringStatsData::new_empty();
        }
        stats
    }

    /// Returns true if the stats has a maximum string length defined.
    pub fn has_max_string_length(stats: &BaseStatistics) -> bool {
        if let StatsData::String(data) = stats.stats_data() {
            data.has_max_string_length()
        } else {
            false
        }
    }

    /// Returns the maximum string length.
    /// Returns None if !has_max_string_length().
    pub fn max_string_length(stats: &BaseStatistics) -> Option<u32> {
        if let StatsData::String(data) = stats.stats_data() {
            data.max_string_length()
        } else {
            None
        }
    }

    /// Returns true if the strings can contain unicode characters.
    pub fn can_contain_unicode(stats: &BaseStatistics) -> bool {
        if let StatsData::String(data) = stats.stats_data() {
            data.can_contain_unicode()
        } else {
            true // Default to true if not string stats
        }
    }

    /// Returns the min value (up to a length of MAX_STRING_MINMAX_SIZE).
    pub fn min(stats: &BaseStatistics) -> String {
        if let StatsData::String(data) = stats.stats_data() {
            data.min_string()
        } else {
            String::new()
        }
    }

    /// Returns the max value (up to a length of MAX_STRING_MINMAX_SIZE).
    pub fn max(stats: &BaseStatistics) -> String {
        if let StatsData::String(data) = stats.stats_data() {
            data.max_string()
        } else {
            String::new()
        }
    }

    /// Returns the min value as bytes.
    pub fn min_bytes(stats: &BaseStatistics) -> &[u8] {
        if let StatsData::String(data) = stats.stats_data() {
            data.min_bytes()
        } else {
            &[]
        }
    }

    /// Returns the max value as bytes.
    pub fn max_bytes(stats: &BaseStatistics) -> &[u8] {
        if let StatsData::String(data) = stats.stats_data() {
            data.max_bytes()
        } else {
            &[]
        }
    }

    /// Resets the max string length so has_max_string_length() returns false.
    pub fn reset_max_string_length(stats: &mut BaseStatistics) {
        if let StatsData::String(data) = stats.stats_data_mut() {
            data.reset_max_string_length();
        }
    }

    /// Sets the max string length.
    pub fn set_max_string_length(stats: &mut BaseStatistics, length: u32) {
        if let StatsData::String(data) = stats.stats_data_mut() {
            data.set_max_string_length(length);
        }
    }

    /// Mark that the column can contain unicode characters.
    pub fn set_contains_unicode(stats: &mut BaseStatistics) {
        if let StatsData::String(data) = stats.stats_data_mut() {
            data.set_contains_unicode();
        }
    }

    /// Sets the min value of the statistics.
    pub fn set_min(stats: &mut BaseStatistics, value: &str) {
        if let StatsData::String(data) = stats.stats_data_mut() {
            data.set_min(value);
        }
    }

    /// Sets the max value of the statistics.
    pub fn set_max(stats: &mut BaseStatistics, value: &str) {
        if let StatsData::String(data) = stats.stats_data_mut() {
            data.set_max(value);
        }
    }

    /// Update statistics with a new string value.
    /// Updates min/max, max_string_length, and unicode flag.
    pub fn update(stats: &mut BaseStatistics, value: &str) {
        stats.set(StatsInfo::CanHaveValidValues);

        if let StatsData::String(data) = stats.stats_data_mut() {
            data.update(value);
        }
    }

    /// Update statistics with a Value.
    /// Handles NULL values appropriately.
    pub fn update_value(stats: &mut BaseStatistics, value: &Value) {
        if value.is_null() {
            stats.set(StatsInfo::CanHaveNullValues);
            return;
        }

        if let Value::Varchar(s) = value {
            Self::update(stats, s);
        }
    }

    /// Merge another statistics into this one.
    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        // Merge validity flags
        if other.can_have_null() {
            stats.set(StatsInfo::CanHaveNullValues);
        }
        if other.can_have_no_null() {
            stats.set(StatsInfo::CanHaveValidValues);
        }

        // Merge string data
        if let (StatsData::String(self_data), StatsData::String(other_data)) =
            (stats.stats_data_mut(), other.stats_data())
        {
            self_data.merge(other_data);
        }
    }

    /// Returns true if the stats represents a constant value (min == max).
    pub fn is_constant(stats: &BaseStatistics) -> bool {
        if let StatsData::String(data) = stats.stats_data() {
            data.is_constant()
        } else {
            false
        }
    }

    /// Get the string stats data from BaseStatistics.
    /// Returns None if the statistics is not for string type.
    pub fn get_data(stats: &BaseStatistics) -> Option<&StringStatsData> {
        if let StatsData::String(data) = stats.stats_data() {
            Some(data)
        } else {
            None
        }
    }

    /// Get mutable string stats data from BaseStatistics.
    /// Returns None if the statistics is not for string type.
    pub fn get_data_mut(stats: &mut BaseStatistics) -> Option<&mut StringStatsData> {
        if let StatsData::String(data) = stats.stats_data_mut() {
            Some(data)
        } else {
            None
        }
    }

    /// Convert statistics to a string representation.
    pub fn to_string(stats: &BaseStatistics) -> String {
        if let StatsData::String(data) = stats.stats_data() {
            let max_len_str = if data.has_max_string_length {
                data.max_string_length.to_string()
            } else {
                "?".to_string()
            };
            format!(
                "[Min: {}, Max: {}, Has Unicode: {}, Max String Length: {}]",
                data.min_string(),
                data.max_string(),
                data.has_unicode,
                max_len_str
            )
        } else {
            "[Not String Stats]".to_string()
        }
    }

    // ========== Zone Map Filtering ==========

    /// Check whether a comparison with constants could possibly be satisfied
    /// by rows given the statistics (zone map filtering).
    ///
    /// This is used to prune data segments based on min/max statistics.
    ///
    /// # Arguments
    /// * `stats` - The statistics to check against
    /// * `comparison_type` - The type of comparison (=, <, >, <=, >=, !=, etc.)
    /// * `constants` - The constant values to compare against
    ///
    /// # Returns
    /// * `FilterAlwaysTrue` - All values in the segment satisfy the filter
    /// * `FilterAlwaysFalse` - No values in the segment satisfy the filter (can prune)
    /// * `NoPruningPossible` - Cannot determine, need to scan
    ///
    /// # Example
    /// ```ignore
    /// // If segment has min="apple", max="cherry" and filter is "x = 'banana'"
    /// // Result is NoPruningPossible (some values may satisfy)
    ///
    /// // If segment has min="apple", max="cherry" and filter is "x = 'zebra'"
    /// // Result is FilterAlwaysFalse (can prune entire segment)
    ///
    /// // If segment has min="apple", max="cherry" and filter is "x > 'aaa'"
    /// // Result is FilterAlwaysTrue (all values satisfy)
    /// ```
    pub fn check_zonemap(
        stats: &BaseStatistics,
        comparison_type: ExpressionType,
        constants: &[Value],
    ) -> FilterPropagateResult {
        let Some(data) = Self::get_data(stats) else {
            return FilterPropagateResult::NoPruningPossible;
        };

        // For each constant, check if the filter could be satisfied
        for constant in constants {
            // Skip NULL constants - they don't help with pruning
            if constant.is_null() {
                continue;
            }

            // Extract string value from constant
            let constant_str = match constant {
                Value::Varchar(s) => s.as_str(),
                _ => return FilterPropagateResult::NoPruningPossible,
            };

            let result = Self::check_zonemap_single(
                data.min_bytes(),
                data.max_bytes(),
                comparison_type,
                constant_str,
            );

            // For equality checks with multiple constants (IN), if any constant
            // could match, we can't prune
            if result == FilterPropagateResult::NoPruningPossible {
                return FilterPropagateResult::NoPruningPossible;
            }
            if result == FilterPropagateResult::FilterAlwaysTrue {
                return FilterPropagateResult::FilterAlwaysTrue;
            }
        }

        // If we get here, all constants resulted in FilterAlwaysFalse
        FilterPropagateResult::FilterAlwaysFalse
    }

    /// Check zonemap for a single constant string value.
    ///
    /// Compares the constant against the min/max statistics to determine
    /// if the filter can be satisfied.
    fn check_zonemap_single(
        min_data: &[u8],
        max_data: &[u8],
        comparison_type: ExpressionType,
        constant: &str,
    ) -> FilterPropagateResult {
        let constant_bytes = constant.as_bytes();
        let compare_len = constant_bytes.len().min(MAX_STRING_MINMAX_SIZE);

        // Compare constant with min and max
        // min_comp: negative if constant < min, positive if constant > min, 0 if equal
        // max_comp: negative if constant < max, positive if constant > max, 0 if equal
        let min_comp = Self::string_value_comparison(&constant_bytes[..compare_len], min_data);
        let max_comp = Self::string_value_comparison(&constant_bytes[..compare_len], max_data);

        match comparison_type {
            ExpressionType::CompareEqual | ExpressionType::CompareNotDistinctFrom => {
                // X = C
                // If constant is within [min, max] range, we can't prune
                // If constant is outside range, filter is always false
                if min_comp >= Ordering::Equal && max_comp <= Ordering::Equal {
                    // constant >= min && constant <= max
                    FilterPropagateResult::NoPruningPossible
                } else {
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }

            ExpressionType::CompareNotEqual | ExpressionType::CompareDistinctFrom => {
                // X != C
                // If constant is outside [min, max] range, filter is always true
                // Otherwise, we can't prune
                if min_comp == Ordering::Less || max_comp == Ordering::Greater {
                    // constant < min || constant > max
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            ExpressionType::CompareGreaterThanOrEqualTo | ExpressionType::CompareGreaterThan => {
                // X >= C or X > C
                // If max <= constant (for >=) or max < constant (for >), we can't prune
                // If max < constant (for >=) or max <= constant (for >), filter is always false
                if max_comp <= Ordering::Equal {
                    // constant <= max, so some values might satisfy X >= C or X > C
                    FilterPropagateResult::NoPruningPossible
                } else {
                    // constant > max, so no values satisfy X >= C or X > C
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }

            ExpressionType::CompareLessThan | ExpressionType::CompareLessThanOrEqualTo => {
                // X < C or X <= C
                // If min >= constant (for <) or min > constant (for <=), filter is always false
                // Otherwise, we can't prune
                if min_comp >= Ordering::Equal {
                    // constant >= min, so some values might satisfy X < C or X <= C
                    FilterPropagateResult::NoPruningPossible
                } else {
                    // constant < min, so no values satisfy X < C or X <= C
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }

            _ => {
                // Unsupported comparison type
                FilterPropagateResult::NoPruningPossible
            }
        }
    }

    /// Compare two byte slices for string comparison.
    /// Returns Ordering::Less if data < comparison, Ordering::Greater if data > comparison,
    /// Ordering::Equal if they are equal up to the comparison length.
    #[inline]
    fn string_value_comparison(data: &[u8], comparison: &[u8]) -> Ordering {
        let len = data.len().min(comparison.len());
        for i in 0..len {
            match data[i].cmp(&comparison[i]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        // If all compared bytes are equal, compare lengths
        data.len().cmp(&comparison.len())
    }
}

#[cfg(test)]
mod string_stats_tests {
    use super::*;

    #[test]
    fn test_create_unknown() {
        let stats = StringStats::create_unknown(LogicalType::Varchar);
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert!(StringStats::can_contain_unicode(&stats));
        assert!(!StringStats::has_max_string_length(&stats));
    }

    #[test]
    fn test_create_empty() {
        let stats = StringStats::create_empty(LogicalType::Varchar);
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert!(!StringStats::can_contain_unicode(&stats));
        assert!(StringStats::has_max_string_length(&stats));
        assert_eq!(StringStats::max_string_length(&stats), Some(0));
    }

    #[test]
    fn test_set_min_max() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::set_min(&mut stats, "aaa");
        StringStats::set_max(&mut stats, "zzz");

        assert_eq!(StringStats::min(&stats), "aaa");
        assert_eq!(StringStats::max(&stats), "zzz");
    }

    #[test]
    fn test_update() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "hello");
        StringStats::update(&mut stats, "world");

        assert_eq!(StringStats::min(&stats), "hello");
        assert_eq!(StringStats::max(&stats), "world");
        assert_eq!(StringStats::max_string_length(&stats), Some(5));
        assert!(stats.can_have_no_null());
    }

    #[test]
    fn test_update_with_null() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update_value(&mut stats, &Value::Null(LogicalType::Varchar));

        assert!(stats.can_have_null());
        assert!(!stats.can_have_no_null());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats1, "banana");
        StringStats::update(&mut stats1, "cherry");

        let mut stats2 = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats2, "apple");
        StringStats::update(&mut stats2, "date");

        StringStats::merge(&mut stats1, &stats2);

        assert_eq!(StringStats::min(&stats1), "apple");
        assert_eq!(StringStats::max(&stats1), "date");
    }

    #[test]
    fn test_merge_unicode() {
        let mut stats1 = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats1, "hello");
        assert!(!StringStats::can_contain_unicode(&stats1));

        let mut stats2 = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats2, "世界");
        assert!(StringStats::can_contain_unicode(&stats2));

        StringStats::merge(&mut stats1, &stats2);
        assert!(StringStats::can_contain_unicode(&stats1));
    }

    #[test]
    fn test_is_constant() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "same");
        assert!(StringStats::is_constant(&stats));

        StringStats::update(&mut stats, "different");
        assert!(!StringStats::is_constant(&stats));
    }

    #[test]
    fn test_to_string() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "hello");
        StringStats::update(&mut stats, "world");

        let s = StringStats::to_string(&stats);
        assert!(s.contains("Min: hello"));
        assert!(s.contains("Max: world"));
        assert!(s.contains("Has Unicode: false"));
        assert!(s.contains("Max String Length: 5"));
    }

    #[test]
    fn test_get_data() {
        let stats = StringStats::create_empty(LogicalType::Varchar);
        let data = StringStats::get_data(&stats);
        assert!(data.is_some());

        let numeric_stats = BaseStatistics::create_empty(LogicalType::Integer);
        let data = StringStats::get_data(&numeric_stats);
        assert!(data.is_none());
    }

    #[test]
    fn test_reset_max_string_length() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "hello");
        assert!(StringStats::has_max_string_length(&stats));

        StringStats::reset_max_string_length(&mut stats);
        assert!(!StringStats::has_max_string_length(&stats));
    }

    #[test]
    fn test_set_contains_unicode() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        assert!(!StringStats::can_contain_unicode(&stats));

        StringStats::set_contains_unicode(&mut stats);
        assert!(StringStats::can_contain_unicode(&stats));
    }

    #[test]
    fn test_min_max_bytes() {
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "hello");

        assert_eq!(StringStats::min_bytes(&stats), b"hello");
        assert_eq!(StringStats::max_bytes(&stats), b"hello");
    }

    // ========== CheckZonemap tests ==========

    #[test]
    fn test_check_zonemap_equal_in_range() {
        // Segment: min="apple", max="cherry"
        // Filter: x = 'banana' (in range)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("banana".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_equal_out_of_range_high() {
        // Segment: min="apple", max="cherry"
        // Filter: x = 'zebra' (out of range, too high)
        // Result: FilterAlwaysFalse (can prune)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("zebra".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_equal_out_of_range_low() {
        // Segment: min="banana", max="cherry"
        // Filter: x = 'aaa' (out of range, too low)
        // Result: FilterAlwaysFalse (can prune)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "banana");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("aaa".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_not_equal_out_of_range() {
        // Segment: min="apple", max="cherry"
        // Filter: x != 'zebra' (out of range)
        // Result: FilterAlwaysTrue (all values satisfy)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotEqual,
            &[Value::Varchar("zebra".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_not_equal_in_range() {
        // Segment: min="apple", max="cherry"
        // Filter: x != 'banana' (in range)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotEqual,
            &[Value::Varchar("banana".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_greater_than_no_pruning() {
        // Segment: min="apple", max="cherry"
        // Filter: x > 'banana' (some values may satisfy)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Varchar("banana".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_greater_than_always_false() {
        // Segment: min="apple", max="cherry"
        // Filter: x > 'zebra' (max < constant)
        // Result: FilterAlwaysFalse
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Varchar("zebra".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_less_than_no_pruning() {
        // Segment: min="apple", max="cherry"
        // Filter: x < 'banana' (some values may satisfy)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::Varchar("banana".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_less_than_always_false() {
        // Segment: min="banana", max="cherry"
        // Filter: x < 'aaa' (min > constant)
        // Result: FilterAlwaysFalse
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "banana");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::Varchar("aaa".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_greater_than_or_equal() {
        // Segment: min="apple", max="cherry"
        // Filter: x >= 'apple' (min == constant)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThanOrEqualTo,
            &[Value::Varchar("apple".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_less_than_or_equal() {
        // Segment: min="apple", max="cherry"
        // Filter: x <= 'cherry' (max == constant)
        // Result: NoPruningPossible
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThanOrEqualTo,
            &[Value::Varchar("cherry".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_multiple_constants_in() {
        // Segment: min="apple", max="cherry"
        // Filter: x IN ('aaa', 'banana', 'zebra')
        // Result: NoPruningPossible ('banana' is in range)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[
                Value::Varchar("aaa".to_string()),
                Value::Varchar("banana".to_string()),
                Value::Varchar("zebra".to_string()),
            ],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_multiple_constants_all_out() {
        // Segment: min="banana", max="cherry"
        // Filter: x IN ('aaa', 'zebra', 'zzz')
        // Result: FilterAlwaysFalse (all out of range)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "banana");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[
                Value::Varchar("aaa".to_string()),
                Value::Varchar("zebra".to_string()),
                Value::Varchar("zzz".to_string()),
            ],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_long_string() {
        // Test with strings longer than MAX_STRING_MINMAX_SIZE (8 bytes)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "aaaaaaaaa"); // 9 chars, truncated to "aaaaaaaa"
        StringStats::update(&mut stats, "zzzzzzzzzz"); // 10 chars, truncated to "zzzzzzzz"

        // Filter: x = 'mmmmmmmm' (in range after truncation)
        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("mmmmmmmm".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_empty_string() {
        // Test with empty string
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "");
        StringStats::update(&mut stats, "zzz");

        // Filter: x = '' (empty string is min)
        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_not_distinct_from() {
        // IS NOT DISTINCT FROM behaves like = for non-null values
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotDistinctFrom,
            &[Value::Varchar("banana".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_distinct_from() {
        // IS DISTINCT FROM behaves like != for non-null values
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareDistinctFrom,
            &[Value::Varchar("zebra".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_non_string_stats() {
        // Test with non-string statistics
        let stats = BaseStatistics::create_empty(LogicalType::Integer);

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("test".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_null_constant() {
        // Test with NULL constant (should be skipped)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        // Only NULL constant - should result in FilterAlwaysFalse (no valid constants)
        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Null(LogicalType::Varchar)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_boundary_equal_min() {
        // Test equality at boundary (min)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("apple".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_boundary_equal_max() {
        // Test equality at boundary (max)
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "apple");
        StringStats::update(&mut stats, "cherry");

        let result = StringStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Varchar("cherry".to_string())],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }
}
