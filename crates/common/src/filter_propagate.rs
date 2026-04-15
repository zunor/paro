// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Filter Propagate Result
//!
//! Result of filter propagation through statistics for zone map pruning.
//!
//! ## Usage
//! This enum is used by statistics modules (NumericStats, StringStats) to determine
//! whether a filter can prune data segments based on min/max statistics (zone maps).

use std::fmt;

/// Result of filter propagation through statistics.
///
/// When checking a filter against segment statistics (zone maps), this enum
/// indicates whether the segment can be pruned or must be scanned.
///
/// # Examples
///
/// ```
/// use paro_common::filter_propagate::FilterPropagateResult;
///
/// // If segment min=10, max=20 and filter is "x > 100"
/// // The filter is always false for this segment
/// let result = FilterPropagateResult::FilterAlwaysFalse;
///
/// // If segment min=10, max=20 and filter is "x > 5"
/// // The filter is always true for this segment
/// let result = FilterPropagateResult::FilterAlwaysTrue;
///
/// // If segment min=10, max=20 and filter is "x > 15"
/// // Cannot determine without scanning
/// let result = FilterPropagateResult::NoPruningPossible;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum FilterPropagateResult {
    /// Cannot determine filter result from statistics alone.
    /// The segment must be scanned to evaluate the filter.
    #[default]
    NoPruningPossible = 0,

    /// Filter always evaluates to true for all values in this segment.
    /// The segment can be included without evaluating the filter.
    FilterAlwaysTrue = 1,

    /// Filter always evaluates to false for all values in this segment.
    /// The segment can be completely skipped (pruned).
    FilterAlwaysFalse = 2,

    /// Filter evaluates to true or NULL for all values in this segment.
    /// Used when NULL handling affects the result.
    FilterTrueOrNull = 3,

    /// Filter evaluates to false or NULL for all values in this segment.
    /// Used when NULL handling affects the result.
    FilterFalseOrNull = 4,
}

impl FilterPropagateResult {
    /// Returns true if the segment can be completely skipped.
    #[inline]
    pub fn can_prune(&self) -> bool {
        matches!(self, FilterPropagateResult::FilterAlwaysFalse)
    }

    /// Returns true if the filter can be skipped for this segment.
    #[inline]
    pub fn can_skip_filter(&self) -> bool {
        matches!(self, FilterPropagateResult::FilterAlwaysTrue)
    }

    /// Returns true if the result is definitive (not NoPruningPossible).
    #[inline]
    pub fn is_definitive(&self) -> bool {
        !matches!(self, FilterPropagateResult::NoPruningPossible)
    }

    /// Returns true if NULL values might affect the result.
    #[inline]
    pub fn involves_null(&self) -> bool {
        matches!(
            self,
            FilterPropagateResult::FilterTrueOrNull | FilterPropagateResult::FilterFalseOrNull
        )
    }
}

impl fmt::Display for FilterPropagateResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterPropagateResult::NoPruningPossible => write!(f, "NO_PRUNING_POSSIBLE"),
            FilterPropagateResult::FilterAlwaysTrue => write!(f, "FILTER_ALWAYS_TRUE"),
            FilterPropagateResult::FilterAlwaysFalse => write!(f, "FILTER_ALWAYS_FALSE"),
            FilterPropagateResult::FilterTrueOrNull => write!(f, "FILTER_TRUE_OR_NULL"),
            FilterPropagateResult::FilterFalseOrNull => write!(f, "FILTER_FALSE_OR_NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_prune() {
        assert!(!FilterPropagateResult::NoPruningPossible.can_prune());
        assert!(!FilterPropagateResult::FilterAlwaysTrue.can_prune());
        assert!(FilterPropagateResult::FilterAlwaysFalse.can_prune());
        assert!(!FilterPropagateResult::FilterTrueOrNull.can_prune());
        assert!(!FilterPropagateResult::FilterFalseOrNull.can_prune());
    }

    #[test]
    fn test_can_skip_filter() {
        assert!(!FilterPropagateResult::NoPruningPossible.can_skip_filter());
        assert!(FilterPropagateResult::FilterAlwaysTrue.can_skip_filter());
        assert!(!FilterPropagateResult::FilterAlwaysFalse.can_skip_filter());
        assert!(!FilterPropagateResult::FilterTrueOrNull.can_skip_filter());
        assert!(!FilterPropagateResult::FilterFalseOrNull.can_skip_filter());
    }

    #[test]
    fn test_is_definitive() {
        assert!(!FilterPropagateResult::NoPruningPossible.is_definitive());
        assert!(FilterPropagateResult::FilterAlwaysTrue.is_definitive());
        assert!(FilterPropagateResult::FilterAlwaysFalse.is_definitive());
        assert!(FilterPropagateResult::FilterTrueOrNull.is_definitive());
        assert!(FilterPropagateResult::FilterFalseOrNull.is_definitive());
    }

    #[test]
    fn test_involves_null() {
        assert!(!FilterPropagateResult::NoPruningPossible.involves_null());
        assert!(!FilterPropagateResult::FilterAlwaysTrue.involves_null());
        assert!(!FilterPropagateResult::FilterAlwaysFalse.involves_null());
        assert!(FilterPropagateResult::FilterTrueOrNull.involves_null());
        assert!(FilterPropagateResult::FilterFalseOrNull.involves_null());
    }

    #[test]
    fn test_default() {
        assert_eq!(
            FilterPropagateResult::default(),
            FilterPropagateResult::NoPruningPossible
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            FilterPropagateResult::NoPruningPossible.to_string(),
            "NO_PRUNING_POSSIBLE"
        );
        assert_eq!(
            FilterPropagateResult::FilterAlwaysTrue.to_string(),
            "FILTER_ALWAYS_TRUE"
        );
        assert_eq!(
            FilterPropagateResult::FilterAlwaysFalse.to_string(),
            "FILTER_ALWAYS_FALSE"
        );
        assert_eq!(
            FilterPropagateResult::FilterTrueOrNull.to_string(),
            "FILTER_TRUE_OR_NULL"
        );
        assert_eq!(
            FilterPropagateResult::FilterFalseOrNull.to_string(),
            "FILTER_FALSE_OR_NULL"
        );
    }

    #[test]
    fn test_repr_values() {
        // Verify repr(u8) values remain stable
        assert_eq!(FilterPropagateResult::NoPruningPossible as u8, 0);
        assert_eq!(FilterPropagateResult::FilterAlwaysTrue as u8, 1);
        assert_eq!(FilterPropagateResult::FilterAlwaysFalse as u8, 2);
        assert_eq!(FilterPropagateResult::FilterTrueOrNull as u8, 3);
        assert_eq!(FilterPropagateResult::FilterFalseOrNull as u8, 4);
    }

    #[test]
    fn test_equality() {
        assert_eq!(
            FilterPropagateResult::FilterAlwaysTrue,
            FilterPropagateResult::FilterAlwaysTrue
        );
        assert_ne!(
            FilterPropagateResult::FilterAlwaysTrue,
            FilterPropagateResult::FilterAlwaysFalse
        );
    }

    #[test]
    fn test_clone_copy() {
        let result = FilterPropagateResult::FilterAlwaysTrue;
        let cloned = result;
        let copied = result;
        assert_eq!(result, cloned);
        assert_eq!(result, copied);
    }
}
