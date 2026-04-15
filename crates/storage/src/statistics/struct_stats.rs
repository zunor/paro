//! ## Design
//! - StructStats is a static method struct that operates on BaseStatistics
//! - Contains child_stats array for each struct field
//! - Provides factory methods: CreateUnknown, CreateEmpty
//! - Provides accessors: GetChildStats (by index), SetChildStats
//! - Provides operations: Merge, Copy, ToString

use paro_common::types::LogicalType;

use super::base_statistics::BaseStatistics;
use super::types::StatisticsType;

/// Static methods for operating on struct statistics within BaseStatistics.
///
/// This mirrors the `StructStats` struct which provides static methods
/// for creating, accessing, and manipulating struct statistics.
///
/// # Example
/// ```ignore
/// use crate::statistics::{StructStats, BaseStatistics};
/// use paro_common::types::LogicalType;
///
/// let fields = vec![
///     ("a".to_string(), LogicalType::Integer),
///     ("b".to_string(), LogicalType::Varchar),
/// ];
/// let stats = StructStats::create_unknown(LogicalType::Struct(fields));
///
/// // Access child statistics by index
/// let child_a = StructStats::get_child_stats(&stats, 0);
/// ```
pub struct StructStats;

impl StructStats {
    /// Create struct statistics with unknown values.
    ///
    /// All child statistics are initialized as unknown
    /// (has_null=true, has_no_null=true).
    pub fn create_unknown(ty: LogicalType) -> BaseStatistics {
        if !matches!(ty, LogicalType::Struct(_)) {
            return BaseStatistics::create_unknown(ty);
        }
        BaseStatistics::create_unknown(ty)
    }

    /// Create struct statistics with empty values.
    ///
    /// All child statistics are initialized as empty
    /// (has_null=false, has_no_null=false).
    pub fn create_empty(ty: LogicalType) -> BaseStatistics {
        if !matches!(ty, LogicalType::Struct(_)) {
            return BaseStatistics::create_empty(ty);
        }
        BaseStatistics::create_empty(ty)
    }

    /// Get the number of child fields in the struct.
    pub fn get_child_count(stats: &BaseStatistics) -> usize {
        match stats.get_type() {
            LogicalType::Struct(fields) => fields.len(),
            _ => 0,
        }
    }

    /// Get all child statistics as a slice.
    ///
    /// # Panics
    /// Panics if the statistics is not for a struct type.
    pub fn get_child_stats_slice(stats: &BaseStatistics) -> &[BaseStatistics] {
        if stats.get_stats_type() != StatisticsType::StructStats {
            panic!("StructStats::get_child_stats_slice called on stats that is not a struct");
        }
        stats
            .child_stats()
            .expect("Struct stats should have child stats")
    }

    /// Get child statistics by index.
    ///
    /// # Panics
    /// Panics if the statistics is not for a struct type or index is out of bounds.
    pub fn get_child_stats(stats: &BaseStatistics, idx: usize) -> &BaseStatistics {
        if stats.get_stats_type() != StatisticsType::StructStats {
            panic!("StructStats::get_child_stats called on stats that is not a struct");
        }
        let children = stats
            .child_stats()
            .expect("Struct stats should have child stats");
        if idx >= children.len() {
            panic!(
                "StructStats::get_child_stats index {} out of bounds (len={})",
                idx,
                children.len()
            );
        }
        &children[idx]
    }

    /// Get mutable child statistics by index.
    ///
    /// # Panics
    /// Panics if the statistics is not for a struct type or index is out of bounds.
    pub fn get_child_stats_mut(stats: &mut BaseStatistics, idx: usize) -> &mut BaseStatistics {
        if stats.get_stats_type() != StatisticsType::StructStats {
            panic!("StructStats::get_child_stats_mut called on stats that is not a struct");
        }
        let children = stats
            .child_stats_mut()
            .expect("Struct stats should have child stats");
        if idx >= children.len() {
            panic!(
                "StructStats::get_child_stats_mut index {} out of bounds (len={})",
                idx,
                children.len()
            );
        }
        &mut children[idx]
    }

    /// Set child statistics at the given index.
    ///
    /// If `new_stats` is None, creates unknown child statistics.
    ///
    /// # Panics
    /// Panics if the statistics is not for a struct type or index is out of bounds.
    pub fn set_child_stats(
        stats: &mut BaseStatistics,
        idx: usize,
        new_stats: Option<BaseStatistics>,
    ) {
        if stats.get_stats_type() != StatisticsType::StructStats {
            panic!("StructStats::set_child_stats called on stats that is not a struct");
        }

        let child_type = Self::get_child_type(stats, idx);
        let child_stats = match new_stats {
            Some(s) => s,
            None => match child_type {
                Some(ty) => BaseStatistics::create_unknown(ty),
                None => return,
            },
        };

        if let Some(children) = stats.child_stats_mut() {
            if idx < children.len() {
                children[idx] = child_stats;
            }
        }
    }

    /// Get the type of a child field by index.
    pub fn get_child_type(stats: &BaseStatistics, idx: usize) -> Option<LogicalType> {
        match stats.get_type() {
            LogicalType::Struct(fields) => fields.get(idx).map(|(_, ty)| ty.clone()),
            _ => None,
        }
    }

    /// Get the name of a child field by index.
    pub fn get_child_name(stats: &BaseStatistics, idx: usize) -> Option<&str> {
        match stats.get_type() {
            LogicalType::Struct(fields) => fields.get(idx).map(|(name, _)| name.as_str()),
            _ => None,
        }
    }

    /// Merge another struct statistics into this one.
    ///
    /// Merges both the base validity flags and all child statistics.
    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if stats.get_stats_type() != StatisticsType::StructStats {
            return;
        }
        if other.get_stats_type() != StatisticsType::StructStats {
            return;
        }

        // BaseStatistics::merge already handles child stats merging
        stats.merge(other);
    }

    /// Copy struct statistics from another.
    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if stats.get_stats_type() != StatisticsType::StructStats {
            return;
        }
        if other.get_stats_type() != StatisticsType::StructStats {
            return;
        }

        stats.copy_from(other);
    }

    /// Convert struct statistics to a string representation.
    pub fn to_string(stats: &BaseStatistics) -> String {
        if stats.get_stats_type() != StatisticsType::StructStats {
            return stats.to_string();
        }

        let mut result = String::from(" {");
        if let LogicalType::Struct(fields) = stats.get_type() {
            if let Some(children) = stats.child_stats() {
                for (i, ((name, _), child)) in fields.iter().zip(children.iter()).enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(name);
                    result.push_str(": ");
                    result.push_str(&child.to_string());
                }
            }
        }
        result.push('}');
        result
    }

    /// Check if any child can have null values.
    pub fn any_child_can_have_null(stats: &BaseStatistics) -> bool {
        if stats.get_stats_type() != StatisticsType::StructStats {
            return true;
        }
        stats
            .child_stats()
            .map(|children| children.iter().any(|c| c.can_have_null()))
            .unwrap_or(true)
    }

    /// Check if all children can have non-null values.
    pub fn all_children_can_have_valid(stats: &BaseStatistics) -> bool {
        if stats.get_stats_type() != StatisticsType::StructStats {
            return true;
        }
        stats
            .child_stats()
            .map(|children| children.iter().all(|c| c.can_have_no_null()))
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::types::StatsInfo;

    fn make_struct_type() -> LogicalType {
        LogicalType::Struct(vec![
            ("a".to_string(), LogicalType::Integer),
            ("b".to_string(), LogicalType::Varchar),
        ])
    }

    #[test]
    fn test_struct_stats_create_unknown() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::StructStats);
        assert!(stats.child_stats().is_some());
        assert_eq!(stats.child_stats().unwrap().len(), 2);
    }

    #[test]
    fn test_struct_stats_create_empty() {
        let ty = make_struct_type();
        let stats = StructStats::create_empty(ty);

        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::StructStats);
        assert!(stats.child_stats().is_some());
        assert_eq!(stats.child_stats().unwrap().len(), 2);
    }

    #[test]
    fn test_struct_stats_get_child_count() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert_eq!(StructStats::get_child_count(&stats), 2);
    }

    #[test]
    fn test_struct_stats_get_child_stats() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        let child_a = StructStats::get_child_stats(&stats, 0);
        assert_eq!(child_a.get_stats_type(), StatisticsType::NumericStats);

        let child_b = StructStats::get_child_stats(&stats, 1);
        assert_eq!(child_b.get_stats_type(), StatisticsType::StringStats);
    }

    #[test]
    fn test_struct_stats_get_child_stats_slice() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        let children = StructStats::get_child_stats_slice(&stats);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_struct_stats_get_child_stats_mut() {
        let ty = make_struct_type();
        let mut stats = StructStats::create_empty(ty);

        {
            let child = StructStats::get_child_stats_mut(&mut stats, 0);
            child.set(StatsInfo::CanHaveNullValues);
        }

        let child = StructStats::get_child_stats(&stats, 0);
        assert!(child.can_have_null());
    }

    #[test]
    fn test_struct_stats_set_child_stats() {
        let ty = make_struct_type();
        let mut stats = StructStats::create_empty(ty);

        let new_child = BaseStatistics::create_unknown(LogicalType::Integer);
        StructStats::set_child_stats(&mut stats, 0, Some(new_child));

        let child = StructStats::get_child_stats(&stats, 0);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_struct_stats_set_child_stats_none() {
        let ty = make_struct_type();
        let mut stats = StructStats::create_empty(ty);

        // Setting None should create unknown child stats
        StructStats::set_child_stats(&mut stats, 1, None);

        let child = StructStats::get_child_stats(&stats, 1);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_struct_stats_get_child_type() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert_eq!(
            StructStats::get_child_type(&stats, 0),
            Some(LogicalType::Integer)
        );
        assert_eq!(
            StructStats::get_child_type(&stats, 1),
            Some(LogicalType::Varchar)
        );
        assert_eq!(StructStats::get_child_type(&stats, 2), None);
    }

    #[test]
    fn test_struct_stats_get_child_name() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert_eq!(StructStats::get_child_name(&stats, 0), Some("a"));
        assert_eq!(StructStats::get_child_name(&stats, 1), Some("b"));
        assert_eq!(StructStats::get_child_name(&stats, 2), None);
    }

    #[test]
    fn test_struct_stats_merge() {
        let ty = make_struct_type();
        let mut stats1 = StructStats::create_empty(ty.clone());
        let mut stats2 = StructStats::create_empty(ty);

        stats1.set(StatsInfo::CanHaveValidValues);
        stats2.set(StatsInfo::CanHaveNullValues);

        StructStats::merge(&mut stats1, &stats2);

        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());
    }

    #[test]
    fn test_struct_stats_copy() {
        let ty = make_struct_type();
        let mut stats1 = StructStats::create_empty(ty.clone());
        let stats2 = StructStats::create_unknown(ty);

        StructStats::copy(&mut stats1, &stats2);

        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());
    }

    #[test]
    fn test_struct_stats_to_string() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        let s = StructStats::to_string(&stats);
        assert!(s.contains("a:"));
        assert!(s.contains("b:"));
        assert!(s.starts_with(" {"));
        assert!(s.ends_with('}'));
    }

    #[test]
    fn test_struct_stats_any_child_can_have_null() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert!(StructStats::any_child_can_have_null(&stats));
    }

    #[test]
    fn test_struct_stats_all_children_can_have_valid() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);

        assert!(StructStats::all_children_can_have_valid(&stats));
    }

    #[test]
    fn test_struct_stats_nested_struct() {
        // Test with nested struct: Struct { x: Integer, inner: Struct { y: Double } }
        let inner_ty = LogicalType::Struct(vec![("y".to_string(), LogicalType::Double)]);
        let outer_ty = LogicalType::Struct(vec![
            ("x".to_string(), LogicalType::Integer),
            ("inner".to_string(), inner_ty),
        ]);
        let stats = StructStats::create_unknown(outer_ty);

        assert_eq!(stats.get_stats_type(), StatisticsType::StructStats);
        assert_eq!(StructStats::get_child_count(&stats), 2);

        let inner_stats = StructStats::get_child_stats(&stats, 1);
        assert_eq!(inner_stats.get_stats_type(), StatisticsType::StructStats);
        assert_eq!(StructStats::get_child_count(inner_stats), 1);
    }

    #[test]
    #[should_panic(expected = "StructStats::get_child_stats called on stats that is not a struct")]
    fn test_struct_stats_get_child_stats_non_struct_panics() {
        let stats = BaseStatistics::create_unknown(LogicalType::Integer);
        let _ = StructStats::get_child_stats(&stats, 0);
    }

    #[test]
    #[should_panic(expected = "index 5 out of bounds")]
    fn test_struct_stats_get_child_stats_out_of_bounds_panics() {
        let ty = make_struct_type();
        let stats = StructStats::create_unknown(ty);
        let _ = StructStats::get_child_stats(&stats, 5);
    }
}
