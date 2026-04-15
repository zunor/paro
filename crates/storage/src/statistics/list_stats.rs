// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - ListStats is a static method struct that operates on BaseStatistics
//! - Contains a single child_stats for the element type
//! - Similar to ArrayStats but for variable-length lists (LIST type)
//! - Provides factory methods: CreateUnknown, CreateEmpty
//! - Provides accessors: GetChildStats, SetChildStats
//! - Provides operations: Merge, Copy, ToString

use paro_common::types::LogicalType;

use super::base_statistics::BaseStatistics;
use super::types::StatisticsType;

/// List statistics data.
///
/// Contains statistics for the child elements of a variable-length list.
/// This mirrors the `ListStats` structure.
///
/// Unlike NumericStatsData and StringStatsData, ListStats doesn't have
/// its own min/max fields - it operates on the child_stats of BaseStatistics.
/// This struct provides helper methods for working with list statistics.
#[derive(Debug, Clone)]
pub struct ListStatsData {
    /// Statistics for the child element type
    pub child_stats: Box<ListChildStats>,
}

/// Placeholder for child statistics.
/// Will be replaced with BaseStatistics reference in a future refactor.
#[derive(Debug, Clone, Default)]
pub struct ListChildStats {
    /// Whether the child has null values
    pub has_null: bool,
    /// Whether the child has non-null values
    pub has_no_null: bool,
    /// The logical type of the child
    pub child_type: Option<LogicalType>,
}

impl Default for ListStatsData {
    fn default() -> Self {
        Self::new_unknown()
    }
}

impl ListStatsData {
    /// Create new list stats with unknown values.
    pub fn new_unknown() -> Self {
        Self {
            child_stats: Box::new(ListChildStats {
                has_null: true,
                has_no_null: true,
                child_type: None,
            }),
        }
    }

    /// Create new list stats with empty values for the given child type.
    pub fn new_empty(child_type: LogicalType) -> Self {
        Self {
            child_stats: Box::new(ListChildStats {
                has_null: false,
                has_no_null: true,
                child_type: Some(child_type),
            }),
        }
    }

    /// Create list stats from a logical type.
    /// The type must be a List type.
    pub fn from_type(ty: &LogicalType) -> Option<Self> {
        match ty {
            LogicalType::List(child_type) => Some(Self::new_empty(child_type.as_ref().clone())),
            _ => None,
        }
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

    /// Merge another ListStatsData into this one.
    pub fn merge(&mut self, other: &ListStatsData) {
        // Merge null flags
        self.child_stats.has_null = self.child_stats.has_null || other.child_stats.has_null;
        self.child_stats.has_no_null =
            self.child_stats.has_no_null && other.child_stats.has_no_null;
    }

    /// Serialize the list stats to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        // Format: flags (1 byte)
        // Flags: bit 0 = has_null, bit 1 = has_no_null
        let flags = (self.child_stats.has_null as u8) | ((self.child_stats.has_no_null as u8) << 1);
        vec![flags]
    }

    /// Deserialize list stats from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let flags = data[0];
        let has_null = (flags & 1) != 0;
        let has_no_null = (flags & 2) != 0;

        Some(Self {
            child_stats: Box::new(ListChildStats {
                has_null,
                has_no_null,
                child_type: None,
            }),
        })
    }

    /// Create a copy of this list stats.
    pub fn copy(&self) -> Self {
        Self {
            child_stats: Box::new(ListChildStats {
                has_null: self.child_stats.has_null,
                has_no_null: self.child_stats.has_no_null,
                child_type: self.child_stats.child_type.clone(),
            }),
        }
    }
}

// ============================================================================
// ListStats - Static methods for operating on BaseStatistics
// ============================================================================

/// Static methods for operating on list statistics within BaseStatistics.
///
/// This mirrors the `ListStats` struct which provides static methods
/// for creating, accessing, and manipulating list statistics.
///
/// # Example
/// ```ignore
/// use crate::statistics::{ListStats, BaseStatistics};
/// use paro_common::types::LogicalType;
///
/// // Create unknown list statistics
/// let stats = ListStats::create_unknown(LogicalType::List(Box::new(LogicalType::Integer)));
///
/// // Access child statistics
/// let child = ListStats::get_child_stats(&stats);
/// ```
pub struct ListStats;

impl ListStats {
    /// Create list statistics with unknown values.
    ///
    /// Both the list and child statistics are initialized as unknown
    /// (has_null=true, has_no_null=true).
    pub fn create_unknown(ty: LogicalType) -> BaseStatistics {
        if !matches!(ty, LogicalType::List(_)) {
            // Return base unknown stats for non-list types
            return BaseStatistics::create_unknown(ty);
        }
        BaseStatistics::create_unknown(ty)
    }

    /// Create list statistics with empty values.
    ///
    /// Both the list and child statistics are initialized as empty
    /// (has_null=false, has_no_null=false).
    pub fn create_empty(ty: LogicalType) -> BaseStatistics {
        if !matches!(ty, LogicalType::List(_)) {
            // Return base empty stats for non-list types
            return BaseStatistics::create_empty(ty);
        }
        BaseStatistics::create_empty(ty)
    }

    /// Get the child statistics from list statistics.
    ///
    /// # Panics
    /// Panics if the statistics is not for a list type.
    pub fn get_child_stats(stats: &BaseStatistics) -> &BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ListStats {
            panic!("ListStats::get_child_stats called on stats that is not a list");
        }
        stats
            .child_stats()
            .and_then(|children| children.first())
            .expect("List stats should have child stats")
    }

    /// Get mutable child statistics from list statistics.
    ///
    /// # Panics
    /// Panics if the statistics is not for a list type.
    pub fn get_child_stats_mut(stats: &mut BaseStatistics) -> &mut BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ListStats {
            panic!("ListStats::get_child_stats_mut called on stats that is not a list");
        }
        stats
            .child_stats_mut()
            .and_then(|children| children.first_mut())
            .expect("List stats should have child stats")
    }

    /// Set the child statistics for list statistics.
    ///
    /// If `new_stats` is None, creates unknown child statistics.
    ///
    /// # Panics
    /// Panics if the statistics is not for a list type.
    pub fn set_child_stats(stats: &mut BaseStatistics, new_stats: Option<BaseStatistics>) {
        if stats.get_stats_type() != StatisticsType::ListStats {
            panic!("ListStats::set_child_stats called on stats that is not a list");
        }

        let child_type = match stats.get_type() {
            LogicalType::List(child) => child.as_ref().clone(),
            _ => return,
        };

        let child_stats = match new_stats {
            Some(s) => s,
            None => BaseStatistics::create_unknown(child_type),
        };

        if let Some(children) = stats.child_stats_mut() {
            if children.is_empty() {
                children.push(child_stats);
            } else {
                children[0] = child_stats;
            }
        }
    }

    /// Merge another list statistics into this one.
    ///
    /// Merges both the base validity flags and the child statistics.
    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if stats.get_stats_type() != StatisticsType::ListStats {
            return;
        }
        if other.get_stats_type() != StatisticsType::ListStats {
            return;
        }

        // BaseStatistics::merge already handles child stats merging
        stats.merge(other);
    }

    /// Copy list statistics from another.
    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if stats.get_stats_type() != StatisticsType::ListStats {
            return;
        }
        if other.get_stats_type() != StatisticsType::ListStats {
            return;
        }

        stats.copy_from(other);
    }

    /// Convert list statistics to a string representation.
    pub fn to_string(stats: &BaseStatistics) -> String {
        if stats.get_stats_type() != StatisticsType::ListStats {
            return stats.to_string();
        }

        if let Some(children) = stats.child_stats() {
            if let Some(child) = children.first() {
                return format!("[{}]", child);
            }
        }

        "[<no child stats>]".to_string()
    }

    /// Check if the child can have null values.
    pub fn child_can_have_null(stats: &BaseStatistics) -> bool {
        if stats.get_stats_type() != StatisticsType::ListStats {
            return true;
        }
        Self::get_child_stats(stats).can_have_null()
    }

    /// Check if the child can have non-null values.
    pub fn child_can_have_valid(stats: &BaseStatistics) -> bool {
        if stats.get_stats_type() != StatisticsType::ListStats {
            return true;
        }
        Self::get_child_stats(stats).can_have_no_null()
    }

    /// Get the child type from list statistics.
    pub fn get_child_type(stats: &BaseStatistics) -> Option<LogicalType> {
        match stats.get_type() {
            LogicalType::List(child) => Some(child.as_ref().clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_stats_data_new_unknown() {
        let stats = ListStatsData::new_unknown();
        assert!(stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
        assert!(stats.child_type().is_none());
    }

    #[test]
    fn test_list_stats_data_new_empty() {
        let stats = ListStatsData::new_empty(LogicalType::Integer);
        assert!(!stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
        assert_eq!(stats.child_type(), Some(&LogicalType::Integer));
    }

    #[test]
    fn test_list_stats_data_from_type() {
        let ty = LogicalType::List(Box::new(LogicalType::Varchar));
        let stats = ListStatsData::from_type(&ty).unwrap();
        assert_eq!(stats.child_type(), Some(&LogicalType::Varchar));

        // Non-list type should return None
        assert!(ListStatsData::from_type(&LogicalType::Integer).is_none());
    }

    #[test]
    fn test_list_stats_data_merge() {
        let mut stats1 = ListStatsData::new_empty(LogicalType::Integer);
        stats1.set_child_has_null(false);

        let mut stats2 = ListStatsData::new_empty(LogicalType::Integer);
        stats2.set_child_has_null(true);

        stats1.merge(&stats2);

        // After merge, has_null should be true (from stats2)
        assert!(stats1.child_can_have_null());
    }

    #[test]
    fn test_list_stats_data_merge_has_no_null() {
        let mut stats1 = ListStatsData::new_empty(LogicalType::Integer);
        stats1.set_child_has_no_null(true);

        let mut stats2 = ListStatsData::new_empty(LogicalType::Integer);
        stats2.set_child_has_no_null(false);

        stats1.merge(&stats2);

        // After merge, has_no_null should be false (AND logic)
        assert!(!stats1.child_can_have_valid());
    }

    #[test]
    fn test_list_stats_data_serialize_deserialize() {
        let mut stats = ListStatsData::new_empty(LogicalType::Double);
        stats.set_child_has_null(true);
        stats.set_child_has_no_null(true);

        let serialized = stats.serialize();
        let deserialized = ListStatsData::deserialize(&serialized).unwrap();

        assert!(deserialized.child_can_have_null());
        assert!(deserialized.child_can_have_valid());
    }

    #[test]
    fn test_list_stats_data_set_flags() {
        let mut stats = ListStatsData::new_unknown();

        stats.set_child_has_null(false);
        assert!(!stats.child_can_have_null());

        stats.set_child_has_no_null(false);
        assert!(!stats.child_can_have_valid());
    }

    #[test]
    fn test_list_stats_data_default() {
        let stats = ListStatsData::default();
        assert!(stats.child_can_have_null());
        assert!(stats.child_can_have_valid());
    }

    #[test]
    fn test_list_stats_data_copy() {
        let mut stats = ListStatsData::new_empty(LogicalType::BigInt);
        stats.set_child_has_null(true);

        let copied = stats.copy();
        assert_eq!(copied.child_type(), Some(&LogicalType::BigInt));
        assert!(copied.child_can_have_null());
        assert!(copied.child_can_have_valid());
    }

    #[test]
    fn test_list_stats_data_nested_list() {
        // Test with nested list type: List<List<Integer>>
        let inner_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ListStatsData::new_empty(inner_type.clone());
        assert_eq!(stats.child_type(), Some(&inner_type));
    }

    // ========== ListStats tests ==========

    #[test]
    fn test_list_stats_create_unknown() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ListStats::create_unknown(ty);

        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::ListStats);
        assert!(stats.child_stats().is_some());
    }

    #[test]
    fn test_list_stats_create_empty() {
        let ty = LogicalType::List(Box::new(LogicalType::Varchar));
        let stats = ListStats::create_empty(ty);

        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::ListStats);
        assert!(stats.child_stats().is_some());
    }

    #[test]
    fn test_list_stats_get_child_stats() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ListStats::create_unknown(ty);

        let child = ListStats::get_child_stats(&stats);
        assert_eq!(child.get_stats_type(), StatisticsType::NumericStats);
    }

    #[test]
    fn test_list_stats_get_child_stats_mut() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let mut stats = ListStats::create_empty(ty);

        {
            let child = ListStats::get_child_stats_mut(&mut stats);
            child.set(super::super::types::StatsInfo::CanHaveNullValues);
        }

        let child = ListStats::get_child_stats(&stats);
        assert!(child.can_have_null());
    }

    #[test]
    fn test_list_stats_set_child_stats() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let mut stats = ListStats::create_empty(ty);

        let new_child = BaseStatistics::create_unknown(LogicalType::Integer);
        ListStats::set_child_stats(&mut stats, Some(new_child));

        let child = ListStats::get_child_stats(&stats);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_list_stats_set_child_stats_none() {
        let ty = LogicalType::List(Box::new(LogicalType::Double));
        let mut stats = ListStats::create_empty(ty);

        // Setting None should create unknown child stats
        ListStats::set_child_stats(&mut stats, None);

        let child = ListStats::get_child_stats(&stats);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_list_stats_merge() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let mut stats1 = ListStats::create_empty(ty.clone());
        let mut stats2 = ListStats::create_empty(ty);

        // Set different flags on each
        stats1.set(super::super::types::StatsInfo::CanHaveValidValues);
        stats2.set(super::super::types::StatsInfo::CanHaveNullValues);

        ListStats::merge(&mut stats1, &stats2);

        // After merge, should have both flags
        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());
    }

    #[test]
    fn test_list_stats_copy() {
        let ty = LogicalType::List(Box::new(LogicalType::BigInt));
        let mut stats1 = ListStats::create_empty(ty.clone());
        let stats2 = ListStats::create_unknown(ty);

        ListStats::copy(&mut stats1, &stats2);

        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());
    }

    #[test]
    fn test_list_stats_to_string() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ListStats::create_unknown(ty);

        let s = ListStats::to_string(&stats);
        assert!(s.starts_with('['));
        assert!(s.ends_with(']'));
    }

    #[test]
    fn test_list_stats_child_can_have_null() {
        let ty = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ListStats::create_unknown(ty);

        assert!(ListStats::child_can_have_null(&stats));
        assert!(ListStats::child_can_have_valid(&stats));
    }

    #[test]
    fn test_list_stats_get_child_type() {
        let ty = LogicalType::List(Box::new(LogicalType::Varchar));
        let stats = ListStats::create_unknown(ty);

        let child_type = ListStats::get_child_type(&stats);
        assert_eq!(child_type, Some(LogicalType::Varchar));
    }

    #[test]
    fn test_list_stats_nested_list() {
        // Test with nested list: List<List<Integer>>
        let inner_ty = LogicalType::List(Box::new(LogicalType::Integer));
        let outer_ty = LogicalType::List(Box::new(inner_ty.clone()));
        let stats = ListStats::create_unknown(outer_ty);

        assert_eq!(stats.get_stats_type(), StatisticsType::ListStats);

        let child = ListStats::get_child_stats(&stats);
        assert_eq!(child.get_stats_type(), StatisticsType::ListStats);

        // Get the inner child (Integer)
        let inner_child = ListStats::get_child_stats(child);
        assert_eq!(inner_child.get_stats_type(), StatisticsType::NumericStats);
    }

    #[test]
    #[should_panic(expected = "ListStats::get_child_stats called on stats that is not a list")]
    fn test_list_stats_get_child_stats_non_list_panics() {
        let stats = BaseStatistics::create_unknown(LogicalType::Integer);
        let _ = ListStats::get_child_stats(&stats);
    }
}
