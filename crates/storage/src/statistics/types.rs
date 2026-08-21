// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - StatisticsType determines which type-specific statistics to use
//! - StatsInfo controls null/valid value flags
//! - GetStatsType maps LogicalType to StatisticsType

use paro_common::types::LogicalType;

/// A statistic that is safe to use as a correctness boundary.
///
/// `Guaranteed` means the bound covers the complete population represented by
/// the statistics object. `Empty` is the identity used while building an exact
/// summary; observing its first value produces a guarantee. `Unknown` is
/// deliberately contagious when non-empty summaries are merged: a bound
/// observed for only one input cannot constrain the union. Cost-model estimates
/// belong in separate metadata and must never be wrapped in this type.
#[derive(Debug, Clone, Copy, Default)]
pub(super) enum StatisticsBound<T> {
    #[default]
    Unknown,
    Empty,
    Guaranteed(T),
}

impl<T> StatisticsBound<T> {
    #[inline]
    pub(super) fn guaranteed(&self) -> Option<&T> {
        match self {
            Self::Unknown | Self::Empty => None,
            Self::Guaranteed(value) => Some(value),
        }
    }

    #[inline]
    pub(super) fn guaranteed_mut(&mut self) -> Option<&mut T> {
        match self {
            Self::Unknown | Self::Empty => None,
            Self::Guaranteed(value) => Some(value),
        }
    }

    #[inline]
    pub(super) fn is_guaranteed(&self) -> bool {
        matches!(self, Self::Guaranteed(_))
    }

    #[inline]
    pub(super) fn set_guaranteed(&mut self, value: T) {
        *self = Self::Guaranteed(value);
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        *self = Self::Unknown;
    }

    /// Fold an observed value into an exact summary.
    ///
    /// An unknown summary remains unknown because observing a suffix cannot
    /// establish a bound for the population that preceded it.
    pub(super) fn observe_with(&mut self, value: T, combine: impl FnOnce(&T, &T) -> T) {
        *self = match &*self {
            Self::Unknown => Self::Unknown,
            Self::Empty => Self::Guaranteed(value),
            Self::Guaranteed(current) => Self::Guaranteed(combine(current, &value)),
        };
    }

    /// Merge two complete-population bounds.
    ///
    /// The combiner is evaluated only when both inputs are guaranteed. If
    /// either input is unknown, the merged population has no guaranteed bound.
    pub(super) fn merge_with(&mut self, other: &Self, combine: impl FnOnce(&T, &T) -> T)
    where
        T: Copy,
    {
        *self = match (&*self, other) {
            (Self::Guaranteed(left), Self::Guaranteed(right)) => {
                Self::Guaranteed(combine(left, right))
            }
            (Self::Empty, other) => *other,
            (current, Self::Empty) => *current,
            _ => Self::Unknown,
        };
    }
}

/// Type of statistics for a column.
///
/// Determines which type-specific statistics structure is used
/// for min/max tracking and other optimizations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StatisticsType {
    /// Numeric statistics (min/max for integers, floats, etc.)
    NumericStats = 0,
    /// String statistics (min/max prefix, unicode flag, max length)
    StringStats = 1,
    /// List statistics (child element statistics)
    ListStats = 2,
    /// Struct statistics (per-field statistics)
    StructStats = 3,
    /// Base statistics only (no type-specific data)
    BaseStats = 4,
    /// Array statistics (fixed-size array, child element statistics)
    ArrayStats = 5,
}

impl StatisticsType {
    /// Get the statistics type for a logical type.
    ///
    /// Maps logical types to their appropriate statistics type:
    /// - Numeric types (integers, floats, decimals) -> NumericStats
    /// - String types (VARCHAR, BLOB) -> StringStats
    /// - List types -> ListStats
    /// - Struct types -> StructStats
    /// - Array types -> ArrayStats
    /// - Other types -> BaseStats
    pub fn from_logical_type(ty: &LogicalType) -> Self {
        match ty {
            // SQL NULL type has no specific statistics
            LogicalType::Null => StatisticsType::BaseStats,

            // Numeric types
            LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Decimal { .. } => StatisticsType::NumericStats,

            // String types
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => StatisticsType::StringStats,

            // Temporal types - use numeric stats for min/max
            LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => StatisticsType::NumericStats,

            // Interval - base stats only (no meaningful min/max)
            LogicalType::Interval | LogicalType::Uuid => StatisticsType::BaseStats,

            // Compound types
            LogicalType::List(_) => StatisticsType::ListStats,
            LogicalType::Struct(_) => StatisticsType::StructStats,
            LogicalType::Array(_, _) => StatisticsType::ArrayStats,

            // Literal types should not appear in statistics
            LogicalType::IntegerLiteral(_) | LogicalType::StringLiteral => {
                StatisticsType::BaseStats
            }

            // Unknown type
            LogicalType::Unknown => StatisticsType::BaseStats,
        }
    }

    /// Check if this statistics type supports min/max tracking.
    pub fn has_min_max(&self) -> bool {
        matches!(
            self,
            StatisticsType::NumericStats | StatisticsType::StringStats
        )
    }

    /// Check if this statistics type has child statistics.
    pub fn has_child_stats(&self) -> bool {
        matches!(
            self,
            StatisticsType::ListStats | StatisticsType::StructStats | StatisticsType::ArrayStats
        )
    }
}

/// Information about null/valid values in statistics.
///
/// Used to set and query the null/valid value flags in BaseStatistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StatsInfo {
    /// The data can contain NULL values
    CanHaveNullValues = 0,
    /// The data cannot contain NULL values
    CannotHaveNullValues = 1,
    /// The data can contain valid (non-NULL) values
    CanHaveValidValues = 2,
    /// The data cannot contain valid (non-NULL) values (all NULL)
    CannotHaveValidValues = 3,
    /// The data can contain both NULL and valid values
    CanHaveNullAndValidValues = 4,
}

impl StatsInfo {
    /// Check if this info indicates the data can have NULL values.
    pub fn can_have_null(&self) -> bool {
        matches!(
            self,
            StatsInfo::CanHaveNullValues | StatsInfo::CanHaveNullAndValidValues
        )
    }

    /// Check if this info indicates the data can have valid values.
    pub fn can_have_valid(&self) -> bool {
        matches!(
            self,
            StatsInfo::CanHaveValidValues | StatsInfo::CanHaveNullAndValidValues
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_type_from_numeric_types() {
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Integer),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::BigInt),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Double),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Boolean),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Decimal {
                precision: 10,
                scale: 2
            }),
            StatisticsType::NumericStats
        );
    }

    #[test]
    fn test_statistics_type_from_string_types() {
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Varchar),
            StatisticsType::StringStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::VarcharCollation("NOCASE".to_string())),
            StatisticsType::StringStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Blob),
            StatisticsType::StringStats
        );
    }

    #[test]
    fn test_statistics_type_from_temporal_types() {
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Date),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Timestamp),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::TimestampTz),
            StatisticsType::NumericStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Time),
            StatisticsType::NumericStats
        );
        // Interval has no meaningful min/max
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Interval),
            StatisticsType::BaseStats
        );
    }

    #[test]
    fn test_statistics_type_from_compound_types() {
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::List(Box::new(LogicalType::Integer))),
            StatisticsType::ListStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Struct(vec![(
                "a".to_string(),
                LogicalType::Integer
            )])),
            StatisticsType::StructStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Array(
                Box::new(LogicalType::Float),
                1536
            )),
            StatisticsType::ArrayStats
        );
    }

    #[test]
    fn test_statistics_type_from_special_types() {
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Null),
            StatisticsType::BaseStats
        );
        assert_eq!(
            StatisticsType::from_logical_type(&LogicalType::Unknown),
            StatisticsType::BaseStats
        );
    }

    #[test]
    fn test_statistics_type_has_min_max() {
        assert!(StatisticsType::NumericStats.has_min_max());
        assert!(StatisticsType::StringStats.has_min_max());
        assert!(!StatisticsType::ListStats.has_min_max());
        assert!(!StatisticsType::StructStats.has_min_max());
        assert!(!StatisticsType::BaseStats.has_min_max());
    }

    #[test]
    fn test_statistics_type_has_child_stats() {
        assert!(!StatisticsType::NumericStats.has_child_stats());
        assert!(!StatisticsType::StringStats.has_child_stats());
        assert!(StatisticsType::ListStats.has_child_stats());
        assert!(StatisticsType::StructStats.has_child_stats());
        assert!(StatisticsType::ArrayStats.has_child_stats());
        assert!(!StatisticsType::BaseStats.has_child_stats());
    }

    #[test]
    fn test_stats_info_can_have_null() {
        assert!(StatsInfo::CanHaveNullValues.can_have_null());
        assert!(!StatsInfo::CannotHaveNullValues.can_have_null());
        assert!(!StatsInfo::CanHaveValidValues.can_have_null());
        assert!(!StatsInfo::CannotHaveValidValues.can_have_null());
        assert!(StatsInfo::CanHaveNullAndValidValues.can_have_null());
    }

    #[test]
    fn test_stats_info_can_have_valid() {
        assert!(!StatsInfo::CanHaveNullValues.can_have_valid());
        assert!(!StatsInfo::CannotHaveNullValues.can_have_valid());
        assert!(StatsInfo::CanHaveValidValues.can_have_valid());
        assert!(!StatsInfo::CannotHaveValidValues.can_have_valid());
        assert!(StatsInfo::CanHaveNullAndValidValues.can_have_valid());
    }
}
