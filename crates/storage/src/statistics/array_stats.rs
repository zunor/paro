// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - ArrayStats operates on BaseStatistics for fixed-size array types
//! - Contains a single child_stats for the element type
//! - Similar to ListStats but for fixed-size arrays (ARRAY type)

use paro_common::types::LogicalType;

/// Array statistics data.
///
/// Contains statistics for the child elements of a fixed-size array.
/// This mirrors the `ArrayStats` structure.
///
/// Unlike NumericStatsData and StringStatsData, ArrayStats doesn't have
/// its own data fields - it operates on the child_stats of BaseStatistics.
/// This struct provides helper methods for working with array statistics.
#[derive(Debug, Clone)]
pub struct ArrayStatsData {
    /// Statistics for the child element type
    pub child_stats: Box<ChildStats>,
    /// The array size (fixed for ARRAY types)
    pub array_size: usize,
}

/// Placeholder for child statistics.
/// Will be replaced with BaseStatistics reference in a future refactor.
#[derive(Debug, Clone, Default)]
pub struct ChildStats {
    /// Whether the child has null values
    pub has_null: bool,
    /// Whether the child has non-null values
    pub has_no_null: bool,
    /// The logical type of the child
    pub child_type: Option<LogicalType>,
}

impl Default for ArrayStatsData {
    fn default() -> Self {
        Self::new_unknown(1)
    }
}

impl ArrayStatsData {
    /// Create new array stats with unknown values.
    pub fn new_unknown(array_size: usize) -> Self {
        Self {
            child_stats: Box::new(ChildStats {
                has_null: true,
                has_no_null: true,
                child_type: None,
            }),
            array_size,
        }
    }

    /// Create new array stats with empty values for the given child type.
    pub fn new_empty(child_type: LogicalType, array_size: usize) -> Self {
        Self {
            child_stats: Box::new(ChildStats {
                has_null: false,
                has_no_null: true,
                child_type: Some(child_type),
            }),
            array_size,
        }
    }

    /// Create array stats from a logical type.
    /// The type must be an Array type.
    pub fn from_type(ty: &LogicalType) -> Option<Self> {
        match ty {
            LogicalType::Array(child_type, size) => {
                Some(Self::new_empty(child_type.as_ref().clone(), *size))
            }
            _ => None,
        }
    }

    /// Get the array size.
    pub fn array_size(&self) -> usize {
        self.array_size
    }

    /// Get the child type, if known.
    pub fn child_type(&self) -> Option<&LogicalType> {
        self.child_stats.child_type.as_ref()
    }

    /// Check if the child can have null values.
    pub fn child_can_have_null(&self) -> bool {
        self.child_stats.has_null
    }

    /// Check if the child can have non-null values.
    pub fn child_can_have_valid(&self) -> bool {
        self.child_stats.has_no_null
    }

    /// Set whether the child can have null values.
    pub fn set_child_has_null(&mut self, has_null: bool) {
        self.child_stats.has_null = has_null;
    }

    /// Set whether the child can have non-null values.
    pub fn set_child_has_no_null(&mut self, has_no_null: bool) {
        self.child_stats.has_no_null = has_no_null;
    }

    /// Merge another ArrayStatsData into this one.
    pub fn merge(&mut self, other: &ArrayStatsData) {
        // Merge null flags
        self.child_stats.has_null = self.child_stats.has_null || other.child_stats.has_null;
        self.child_stats.has_no_null =
            self.child_stats.has_no_null && other.child_stats.has_no_null;

        // Array size should match
        debug_assert_eq!(self.array_size, other.array_size);
    }

    /// Serialize the array stats to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        // Format: array_size (8 bytes) + flags (1 byte)
        let mut result = Vec::with_capacity(9);

        // Array size (8 bytes)
        result.extend_from_slice(&(self.array_size as u64).to_le_bytes());

        // Flags (1 byte): bit 0 = has_null, bit 1 = has_no_null
        let flags = (self.child_stats.has_null as u8) | ((self.child_stats.has_no_null as u8) << 1);
        result.push(flags);

        result
    }

    /// Deserialize array stats from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }

        // Array size (8 bytes)
        let array_size = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]) as usize;

        // Flags (1 byte)
        let flags = data[8];
        let has_null = (flags & 1) != 0;
        let has_no_null = (flags & 2) != 0;

        Some(Self {
            child_stats: Box::new(ChildStats {
                has_null,
                has_no_null,
                child_type: None,
            }),
            array_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_stats_data_new_unknown() {
        let stats = ArrayStatsData::new_unknown(10);
        assert_eq!(stats.array_size(), 10);
        assert!(stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
        assert!(stats.child_type().is_none());
    }

    #[test]
    fn test_array_stats_data_new_empty() {
        let stats = ArrayStatsData::new_empty(LogicalType::Integer, 5);
        assert_eq!(stats.array_size(), 5);
        assert!(!stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
        assert_eq!(stats.child_type(), Some(&LogicalType::Integer));
    }

    #[test]
    fn test_array_stats_data_from_type() {
        let ty = LogicalType::Array(Box::new(LogicalType::Float), 1536);
        let stats = ArrayStatsData::from_type(&ty).unwrap();
        assert_eq!(stats.array_size(), 1536);
        assert_eq!(stats.child_type(), Some(&LogicalType::Float));

        // Non-array type should return None
        assert!(ArrayStatsData::from_type(&LogicalType::Integer).is_none());
    }

    #[test]
    fn test_array_stats_data_merge() {
        let mut stats1 = ArrayStatsData::new_empty(LogicalType::Integer, 10);
        stats1.set_child_has_null(false);

        let mut stats2 = ArrayStatsData::new_empty(LogicalType::Integer, 10);
        stats2.set_child_has_null(true);

        stats1.merge(&stats2);

        // After merge, has_null should be true (from stats2)
        assert!(stats1.child_can_have_null());
    }

    #[test]
    fn test_array_stats_data_serialize_deserialize() {
        let mut stats = ArrayStatsData::new_empty(LogicalType::Double, 100);
        stats.set_child_has_null(true);
        stats.set_child_has_no_null(true);

        let serialized = stats.serialize();
        let deserialized = ArrayStatsData::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.array_size(), 100);
        assert!(deserialized.child_can_have_null());
        assert!(deserialized.child_can_have_valid());
    }

    #[test]
    fn test_array_stats_data_set_flags() {
        let mut stats = ArrayStatsData::new_unknown(5);

        stats.set_child_has_null(false);
        assert!(!stats.child_can_have_null());

        stats.set_child_has_no_null(false);
        assert!(!stats.child_can_have_valid());
    }

    #[test]
    fn test_array_stats_data_default() {
        let stats = ArrayStatsData::default();
        assert_eq!(stats.array_size(), 1);
        assert!(stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
    }
}
