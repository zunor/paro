// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - NumericValueUnion: Union type for storing min/max values of different numeric types
//! - NumericStatsData: Holds correctness-safe min/max bounds
//! - NumericStats: Static methods for operating on BaseStatistics with numeric data
//! - Type-safe access via GetReferenceUnsafe<T>() pattern
//!
//! This file implements both:
//! 1. NumericStatsData - the data structure for numeric statistics
//! 2. NumericStats - static methods for operating on BaseStatistics

use paro_common::expression_type::ExpressionType;
use paro_common::filter_propagate::FilterPropagateResult;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::cmp::Ordering;

use super::base_statistics::{BaseStatistics, StatsData};
use super::types::{StatisticsBound, StatsInfo};

/// Union for storing numeric min/max values.
///
/// This mirrors the `NumericValueUnion` which uses a C++ union
/// to store different numeric types in the same memory location.
/// In Rust, we use an enum to achieve type-safe storage.
#[derive(Debug, Clone, Copy)]
pub enum NumericValueUnion {
    /// Boolean value
    Boolean(bool),
    /// 8-bit signed integer
    TinyInt(i8),
    /// 16-bit signed integer
    SmallInt(i16),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer
    BigInt(i64),
    /// 128-bit signed integer
    HugeInt(i128),
    /// 8-bit unsigned integer
    UTinyInt(u8),
    /// 16-bit unsigned integer
    USmallInt(u16),
    /// 32-bit unsigned integer
    UInteger(u32),
    /// 64-bit unsigned integer
    UBigInt(u64),
    /// 128-bit unsigned integer
    UHugeInt(u128),
    /// 32-bit floating point
    Float(f32),
    /// 64-bit floating point
    Double(f64),
}

impl NumericValueUnion {
    /// Create a NumericValueUnion from a Value.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Boolean(v) => Some(NumericValueUnion::Boolean(*v)),
            Value::TinyInt(v) => Some(NumericValueUnion::TinyInt(*v)),
            Value::SmallInt(v) => Some(NumericValueUnion::SmallInt(*v)),
            Value::Integer(v) => Some(NumericValueUnion::Integer(*v)),
            Value::BigInt(v) => Some(NumericValueUnion::BigInt(*v)),
            Value::HugeInt(v) => Some(NumericValueUnion::HugeInt(*v)),
            Value::UTinyInt(v) => Some(NumericValueUnion::UTinyInt(*v)),
            Value::USmallInt(v) => Some(NumericValueUnion::USmallInt(*v)),
            Value::UInteger(v) => Some(NumericValueUnion::UInteger(*v)),
            Value::UBigInt(v) => Some(NumericValueUnion::UBigInt(*v)),
            Value::UHugeInt(v) => Some(NumericValueUnion::UHugeInt(*v)),
            Value::Float(v) => Some(NumericValueUnion::Float(*v)),
            Value::Double(v) => Some(NumericValueUnion::Double(*v)),
            Value::Decimal(v, _, _) => Some(NumericValueUnion::HugeInt(*v)),
            // Temporal types: Date stored as i32, Timestamp/Time as i64
            Value::Date(v) => Some(NumericValueUnion::Integer(*v)),
            Value::Timestamp(v) => Some(NumericValueUnion::BigInt(*v)),
            Value::TimestampTz(v) => Some(NumericValueUnion::BigInt(*v)),
            Value::Time(v) => Some(NumericValueUnion::BigInt(*v)),
            // Null, non-numeric, and Interval types return None
            Value::Null(_)
            | Value::Varchar(_)
            | Value::Blob(_)
            | Value::Uuid(_)
            | Value::Interval(_, _, _)
            | Value::List(_, _)
            | Value::Array(_, _, _)
            | Value::Struct(_, _) => None,
        }
    }

    /// Convert to a Value with the given logical type.
    pub fn to_value(&self, ty: &LogicalType) -> Value {
        match (self, ty) {
            (NumericValueUnion::HugeInt(v), LogicalType::Decimal { precision, scale }) => {
                Value::Decimal(*v, *precision, *scale)
            }
            (NumericValueUnion::BigInt(v), LogicalType::Decimal { precision, scale }) => {
                Value::Decimal(*v as i128, *precision, *scale)
            }
            (NumericValueUnion::Integer(v), LogicalType::Date) => Value::Date(*v),
            (NumericValueUnion::BigInt(v), LogicalType::Timestamp) => Value::Timestamp(*v),
            (NumericValueUnion::BigInt(v), LogicalType::TimestampTz) => Value::TimestampTz(*v),
            (NumericValueUnion::BigInt(v), LogicalType::Time) => Value::Time(*v),
            _ => match self {
                NumericValueUnion::Boolean(v) => Value::Boolean(*v),
                NumericValueUnion::TinyInt(v) => Value::TinyInt(*v),
                NumericValueUnion::SmallInt(v) => Value::SmallInt(*v),
                NumericValueUnion::Integer(v) => Value::Integer(*v),
                NumericValueUnion::BigInt(v) => Value::BigInt(*v),
                NumericValueUnion::HugeInt(v) => Value::HugeInt(*v),
                NumericValueUnion::UTinyInt(v) => Value::UTinyInt(*v),
                NumericValueUnion::USmallInt(v) => Value::USmallInt(*v),
                NumericValueUnion::UInteger(v) => Value::UInteger(*v),
                NumericValueUnion::UBigInt(v) => Value::UBigInt(*v),
                NumericValueUnion::UHugeInt(v) => Value::UHugeInt(*v),
                NumericValueUnion::Float(v) => Value::Float(*v),
                NumericValueUnion::Double(v) => Value::Double(*v),
            },
        }
    }

    /// Compare two NumericValueUnion values.
    /// Returns Ordering::Less if self < other, Ordering::Greater if self > other,
    /// Ordering::Equal if self == other.
    pub fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (NumericValueUnion::Boolean(a), NumericValueUnion::Boolean(b)) => a.cmp(b),
            (NumericValueUnion::TinyInt(a), NumericValueUnion::TinyInt(b)) => a.cmp(b),
            (NumericValueUnion::SmallInt(a), NumericValueUnion::SmallInt(b)) => a.cmp(b),
            (NumericValueUnion::Integer(a), NumericValueUnion::Integer(b)) => a.cmp(b),
            (NumericValueUnion::BigInt(a), NumericValueUnion::BigInt(b)) => a.cmp(b),
            (NumericValueUnion::HugeInt(a), NumericValueUnion::HugeInt(b)) => a.cmp(b),
            (NumericValueUnion::UTinyInt(a), NumericValueUnion::UTinyInt(b)) => a.cmp(b),
            (NumericValueUnion::USmallInt(a), NumericValueUnion::USmallInt(b)) => a.cmp(b),
            (NumericValueUnion::UInteger(a), NumericValueUnion::UInteger(b)) => a.cmp(b),
            (NumericValueUnion::UBigInt(a), NumericValueUnion::UBigInt(b)) => a.cmp(b),
            (NumericValueUnion::UHugeInt(a), NumericValueUnion::UHugeInt(b)) => a.cmp(b),
            (NumericValueUnion::Float(a), NumericValueUnion::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (NumericValueUnion::Double(a), NumericValueUnion::Double(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            // Mismatched types - should not happen in practice
            _ => Ordering::Equal,
        }
    }

    /// Get the i8 value (panics if not a TinyInt).
    #[inline]
    pub fn as_tinyint(&self) -> i8 {
        match self {
            NumericValueUnion::TinyInt(v) => *v,
            _ => panic!("NumericValueUnion is not a TinyInt"),
        }
    }

    /// Get the i16 value (panics if not a SmallInt).
    #[inline]
    pub fn as_smallint(&self) -> i16 {
        match self {
            NumericValueUnion::SmallInt(v) => *v,
            _ => panic!("NumericValueUnion is not a SmallInt"),
        }
    }

    /// Get the i32 value (panics if not an Integer).
    #[inline]
    pub fn as_integer(&self) -> i32 {
        match self {
            NumericValueUnion::Integer(v) => *v,
            _ => panic!("NumericValueUnion is not an Integer"),
        }
    }

    /// Get the i64 value (panics if not a BigInt).
    #[inline]
    pub fn as_bigint(&self) -> i64 {
        match self {
            NumericValueUnion::BigInt(v) => *v,
            _ => panic!("NumericValueUnion is not a BigInt"),
        }
    }

    /// Get the f32 value (panics if not a Float).
    #[inline]
    pub fn as_float(&self) -> f32 {
        match self {
            NumericValueUnion::Float(v) => *v,
            _ => panic!("NumericValueUnion is not a Float"),
        }
    }

    /// Get the f64 value (panics if not a Double).
    #[inline]
    pub fn as_double(&self) -> f64 {
        match self {
            NumericValueUnion::Double(v) => *v,
            _ => panic!("NumericValueUnion is not a Double"),
        }
    }
}

/// Numeric statistics data.
///
/// Contains correctness-safe bounds for the complete represented population.
#[derive(Debug, Clone)]
pub struct NumericStatsData {
    minimum: StatisticsBound<NumericValueUnion>,
    maximum: StatisticsBound<NumericValueUnion>,
}

impl Default for NumericStatsData {
    fn default() -> Self {
        Self {
            minimum: StatisticsBound::Unknown,
            maximum: StatisticsBound::Unknown,
        }
    }
}

impl NumericStatsData {
    /// Create new numeric stats data with unknown min/max.
    pub fn new_unknown() -> Self {
        Self {
            minimum: StatisticsBound::Unknown,
            maximum: StatisticsBound::Unknown,
        }
    }

    /// Create an exact summary for an empty population.
    pub fn new_empty() -> Self {
        Self {
            minimum: StatisticsBound::Empty,
            maximum: StatisticsBound::Empty,
        }
    }

    /// Check if both bounds cover the complete represented population.
    pub fn has_min_max(&self) -> bool {
        self.has_min() && self.has_max()
    }

    pub fn has_min(&self) -> bool {
        self.minimum.is_guaranteed()
    }

    pub fn has_max(&self) -> bool {
        self.maximum.is_guaranteed()
    }

    /// Check if the statistics represents a constant value (min == max).
    pub fn is_constant(&self) -> bool {
        if !self.has_min_max() {
            return false;
        }
        self.minimum
            .guaranteed()
            .zip(self.maximum.guaranteed())
            .is_some_and(|(minimum, maximum)| minimum.compare(maximum) == Ordering::Equal)
    }

    /// Get the minimum value as a Value.
    pub fn min_value(&self, ty: &LogicalType) -> Option<Value> {
        self.minimum.guaranteed().map(|value| value.to_value(ty))
    }

    /// Get the maximum value as a Value.
    pub fn max_value(&self, ty: &LogicalType) -> Option<Value> {
        self.maximum.guaranteed().map(|value| value.to_value(ty))
    }

    /// Set a normalized minimum value that matches the statistics type.
    fn set_guaranteed_min(&mut self, value: NumericValueUnion) {
        self.minimum.set_guaranteed(value);
    }

    /// Set a normalized maximum value that matches the statistics type.
    fn set_guaranteed_max(&mut self, value: NumericValueUnion) {
        self.maximum.set_guaranteed(value);
    }

    pub fn clear_min(&mut self) {
        self.minimum.clear();
    }

    pub fn clear_max(&mut self) {
        self.maximum.clear();
    }

    fn update_union(&mut self, value: NumericValueUnion) {
        self.minimum.observe_with(value, |minimum, candidate| {
            if candidate.compare(minimum) == Ordering::Less {
                *candidate
            } else {
                *minimum
            }
        });
        self.maximum.observe_with(value, |maximum, candidate| {
            if candidate.compare(maximum) == Ordering::Greater {
                *candidate
            } else {
                *maximum
            }
        });
    }

    /// Update the statistics with a new value.
    /// Updates min if new_value < min, updates max if new_value > max.
    pub fn update(&mut self, value: &Value) {
        if let Some(v) = NumericValueUnion::from_value(value) {
            self.update_union(v);
        }
    }

    /// Merge another complete-population summary into this one.
    ///
    /// Unknown is contagious per bound. Retaining one side's bound when the
    /// other side is unknown would turn an observation into a false guarantee.
    pub fn merge(&mut self, other: &NumericStatsData) {
        self.minimum.merge_with(&other.minimum, |left, right| {
            if right.compare(left) == Ordering::Less {
                *right
            } else {
                *left
            }
        });
        self.maximum.merge_with(&other.maximum, |left, right| {
            if right.compare(left) == Ordering::Greater {
                *right
            } else {
                *left
            }
        });
    }

    fn minimum(&self) -> Option<&NumericValueUnion> {
        self.minimum.guaranteed()
    }

    fn maximum(&self) -> Option<&NumericValueUnion> {
        self.maximum.guaranteed()
    }
}

// ============================================================================
// NumericStats - Static methods for operating on BaseStatistics
// ============================================================================

/// Static methods for operating on numeric statistics within BaseStatistics.
///
/// This mirrors the `NumericStats` struct which provides static methods
/// for creating, accessing, and manipulating numeric statistics.
///
/// ## Usage
/// ```ignore
/// let stats = NumericStats::create_unknown(LogicalType::Integer);
/// NumericStats::set_guaranteed_min(&mut stats, &Value::Integer(10));
/// NumericStats::set_guaranteed_max(&mut stats, &Value::Integer(100));
/// let bounds = NumericStats::guaranteed_bounds(&stats);
/// ```
pub struct NumericStats;

impl NumericStats {
    /// Create unknown statistics for a numeric type.
    /// "has_min" is false, "has_max" is false.
    /// This can be used when nothing is known about the data.
    pub fn create_unknown(data_type: LogicalType) -> BaseStatistics {
        BaseStatistics::create_unknown(data_type)
    }

    /// Create an exact empty accumulator for a numeric type.
    /// The first observed value establishes both bounds.
    pub fn create_empty(data_type: LogicalType) -> BaseStatistics {
        BaseStatistics::create_empty(data_type)
    }

    /// Returns true if the stats has a constant value (min == max).
    pub fn is_constant(stats: &BaseStatistics) -> bool {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.is_constant()
        } else {
            false
        }
    }

    /// Returns true if the stats has both a min and max value defined.
    pub fn has_min_max(stats: &BaseStatistics) -> bool {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.has_min_max()
        } else {
            false
        }
    }

    /// Returns true if the stats has a min value defined.
    pub fn has_min(stats: &BaseStatistics) -> bool {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.has_min()
        } else {
            false
        }
    }

    /// Returns true if the stats has a max value defined.
    pub fn has_max(stats: &BaseStatistics) -> bool {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.has_max()
        } else {
            false
        }
    }

    /// Returns the min value.
    /// Returns None if there is no min value.
    pub fn min(stats: &BaseStatistics) -> Option<Value> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.min_value(stats.get_type())
        } else {
            None
        }
    }

    /// Returns the max value.
    /// Returns None if there is no max value.
    pub fn max(stats: &BaseStatistics) -> Option<Value> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.max_value(stats.get_type())
        } else {
            None
        }
    }

    /// Return the pair of bounds that is guaranteed to cover the complete
    /// represented population.
    ///
    /// Correctness-sensitive consumers, such as lossless physical encodings,
    /// must use this paired API instead of independently consulting optimizer
    /// estimates or partially available statistics.
    pub fn guaranteed_bounds(stats: &BaseStatistics) -> Option<(Value, Value)> {
        if !stats.can_have_no_null() {
            return None;
        }
        let StatsData::Numeric(data) = stats.stats_data() else {
            return None;
        };
        Some((
            data.min_value(stats.get_type())?,
            data.max_value(stats.get_type())?,
        ))
    }

    /// Returns the min value or a NULL value if not set.
    pub fn min_or_null(stats: &BaseStatistics) -> Value {
        Self::min(stats).unwrap_or_else(|| Value::Null(stats.get_type().clone()))
    }

    /// Returns the max value or a NULL value if not set.
    pub fn max_or_null(stats: &BaseStatistics) -> Value {
        Self::max(stats).unwrap_or_else(|| Value::Null(stats.get_type().clone()))
    }

    /// Sets a correctness-safe minimum, normalizing lossless physical casts.
    pub fn set_guaranteed_min(stats: &mut BaseStatistics, val: &Value) {
        let normalized = Self::normalize_guaranteed_bound(stats.get_type(), val);
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            if val.is_null() {
                data.clear_min();
            } else if let Some(normalized) = normalized {
                data.set_guaranteed_min(normalized);
            } else {
                // A bound whose physical value does not match the statistics
                // type cannot be a correctness guarantee.
                data.clear_min();
            }
        }
    }

    /// Sets a correctness-safe maximum, normalizing lossless physical casts.
    pub fn set_guaranteed_max(stats: &mut BaseStatistics, val: &Value) {
        let normalized = Self::normalize_guaranteed_bound(stats.get_type(), val);
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            if val.is_null() {
                data.clear_max();
            } else if let Some(normalized) = normalized {
                data.set_guaranteed_max(normalized);
            } else {
                data.clear_max();
            }
        }
    }

    /// Normalize an externally supplied bound to the statistics' logical type.
    ///
    /// Optimizer propagation may carry a bound across a lossless cast. Keeping
    /// the source `Value` variant would make the physical union disagree with
    /// the target logical type and could corrupt durable field alignment.
    fn normalize_guaranteed_bound(
        data_type: &LogicalType,
        value: &Value,
    ) -> Option<NumericValueUnion> {
        if &value.logical_type() == data_type {
            return NumericValueUnion::from_value(value);
        }
        let source_type = value.logical_type();
        let is_semantic_numeric_cast = source_type.is_numeric() && data_type.is_numeric();
        let is_timestamp_cast = matches!(
            (&source_type, data_type),
            (LogicalType::Timestamp, LogicalType::TimestampTz)
                | (LogicalType::TimestampTz, LogicalType::Timestamp)
        );
        if is_semantic_numeric_cast || is_timestamp_cast {
            if let Ok(casted) = value.cast(data_type) {
                return NumericValueUnion::from_value(&casted);
            }
        }

        match data_type {
            LogicalType::TinyInt => Self::integral_as_i128(value)
                .and_then(|value| i8::try_from(value).ok())
                .map(NumericValueUnion::TinyInt),
            LogicalType::SmallInt => Self::integral_as_i128(value)
                .and_then(|value| i16::try_from(value).ok())
                .map(NumericValueUnion::SmallInt),
            LogicalType::Integer => Self::integral_as_i128(value)
                .and_then(|value| i32::try_from(value).ok())
                .map(NumericValueUnion::Integer),
            LogicalType::BigInt => Self::integral_as_i128(value)
                .and_then(|value| i64::try_from(value).ok())
                .map(NumericValueUnion::BigInt),
            LogicalType::HugeInt => Self::integral_as_i128(value).map(NumericValueUnion::HugeInt),
            LogicalType::UTinyInt => Self::integral_as_u128(value)
                .and_then(|value| u8::try_from(value).ok())
                .map(NumericValueUnion::UTinyInt),
            LogicalType::USmallInt => Self::integral_as_u128(value)
                .and_then(|value| u16::try_from(value).ok())
                .map(NumericValueUnion::USmallInt),
            LogicalType::UInteger => Self::integral_as_u128(value)
                .and_then(|value| u32::try_from(value).ok())
                .map(NumericValueUnion::UInteger),
            LogicalType::UBigInt => Self::integral_as_u128(value)
                .and_then(|value| u64::try_from(value).ok())
                .map(NumericValueUnion::UBigInt),
            LogicalType::UHugeInt => Self::integral_as_u128(value).map(NumericValueUnion::UHugeInt),
            _ => None,
        }
    }

    fn integral_as_i128(value: &Value) -> Option<i128> {
        match value {
            Value::TinyInt(value) => Some(*value as i128),
            Value::SmallInt(value) => Some(*value as i128),
            Value::Integer(value) => Some(*value as i128),
            Value::BigInt(value) => Some(*value as i128),
            Value::HugeInt(value) => Some(*value),
            Value::UTinyInt(value) => Some(*value as i128),
            Value::USmallInt(value) => Some(*value as i128),
            Value::UInteger(value) => Some(*value as i128),
            Value::UBigInt(value) => Some(*value as i128),
            Value::UHugeInt(value) => i128::try_from(*value).ok(),
            _ => None,
        }
    }

    fn integral_as_u128(value: &Value) -> Option<u128> {
        match value {
            Value::TinyInt(value) => u128::try_from(*value).ok(),
            Value::SmallInt(value) => u128::try_from(*value).ok(),
            Value::Integer(value) => u128::try_from(*value).ok(),
            Value::BigInt(value) => u128::try_from(*value).ok(),
            Value::HugeInt(value) => u128::try_from(*value).ok(),
            Value::UTinyInt(value) => Some(*value as u128),
            Value::USmallInt(value) => Some(*value as u128),
            Value::UInteger(value) => Some(*value as u128),
            Value::UBigInt(value) => Some(*value as u128),
            Value::UHugeInt(value) => Some(*value),
            _ => None,
        }
    }

    /// Update statistics with a new value.
    /// Updates min if new_value < min, updates max if new_value > max.
    pub fn update(stats: &mut BaseStatistics, value: &Value) {
        if value.is_null() {
            stats.set(StatsInfo::CanHaveNullValues);
            return;
        }

        stats.set(StatsInfo::CanHaveValidValues);

        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update(value);
        }
    }

    /// Get the numeric stats data from BaseStatistics.
    /// Returns None if the statistics is not numeric.
    pub fn get_data(stats: &BaseStatistics) -> Option<&NumericStatsData> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            Some(data)
        } else {
            None
        }
    }

    /// Get mutable numeric stats data from BaseStatistics.
    /// Returns None if the statistics is not numeric.
    pub fn get_data_mut(stats: &mut BaseStatistics) -> Option<&mut NumericStatsData> {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            Some(data)
        } else {
            None
        }
    }

    /// Convert statistics to a string representation.
    pub fn to_string(stats: &BaseStatistics) -> String {
        let min_str = Self::min_or_null(stats).to_string();
        let max_str = Self::max_or_null(stats).to_string();
        format!("[Min: {}, Max: {}]", min_str, max_str)
    }

    // ========== Type-specific update methods ==========

    #[inline]
    fn update_typed(stats: &mut BaseStatistics, value: NumericValueUnion) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update_union(value);
        }
        stats.set_has_no_null_fast();
    }

    #[inline]
    fn minimum_union(stats: &BaseStatistics) -> Option<&NumericValueUnion> {
        let StatsData::Numeric(data) = stats.stats_data() else {
            return None;
        };
        data.minimum()
    }

    #[inline]
    fn maximum_union(stats: &BaseStatistics) -> Option<&NumericValueUnion> {
        let StatsData::Numeric(data) = stats.stats_data() else {
            return None;
        };
        data.maximum()
    }

    /// Update statistics with a boolean value.
    #[inline]
    pub fn update_bool(stats: &mut BaseStatistics, value: bool) {
        Self::update_typed(stats, NumericValueUnion::Boolean(value));
    }

    /// Update statistics with an i8 value.
    #[inline]
    pub fn update_i8(stats: &mut BaseStatistics, value: i8) {
        Self::update_typed(stats, NumericValueUnion::TinyInt(value));
    }

    /// Update statistics with an i16 value.
    #[inline]
    pub fn update_i16(stats: &mut BaseStatistics, value: i16) {
        Self::update_typed(stats, NumericValueUnion::SmallInt(value));
    }

    /// Update statistics with an i32 value.
    #[inline]
    pub fn update_i32(stats: &mut BaseStatistics, value: i32) {
        Self::update_typed(stats, NumericValueUnion::Integer(value));
    }

    /// Update statistics with an i64 value.
    #[inline]
    pub fn update_i64(stats: &mut BaseStatistics, value: i64) {
        Self::update_typed(stats, NumericValueUnion::BigInt(value));
    }

    /// Update statistics with an i128 value.
    #[inline]
    pub fn update_i128(stats: &mut BaseStatistics, value: i128) {
        Self::update_typed(stats, NumericValueUnion::HugeInt(value));
    }

    /// Update statistics with a u8 value.
    #[inline]
    pub fn update_u8(stats: &mut BaseStatistics, value: u8) {
        Self::update_typed(stats, NumericValueUnion::UTinyInt(value));
    }

    /// Update statistics with a u16 value.
    #[inline]
    pub fn update_u16(stats: &mut BaseStatistics, value: u16) {
        Self::update_typed(stats, NumericValueUnion::USmallInt(value));
    }

    /// Update statistics with a u32 value.
    #[inline]
    pub fn update_u32(stats: &mut BaseStatistics, value: u32) {
        Self::update_typed(stats, NumericValueUnion::UInteger(value));
    }

    /// Update statistics with a u64 value.
    #[inline]
    pub fn update_u64(stats: &mut BaseStatistics, value: u64) {
        Self::update_typed(stats, NumericValueUnion::UBigInt(value));
    }

    /// Update statistics with a u128 value.
    #[inline]
    pub fn update_u128(stats: &mut BaseStatistics, value: u128) {
        Self::update_typed(stats, NumericValueUnion::UHugeInt(value));
    }

    /// Update statistics with an f32 value.
    #[inline]
    pub fn update_f32(stats: &mut BaseStatistics, value: f32) {
        Self::update_typed(stats, NumericValueUnion::Float(value));
    }

    /// Update statistics with an f64 value.
    #[inline]
    pub fn update_f64(stats: &mut BaseStatistics, value: f64) {
        Self::update_typed(stats, NumericValueUnion::Double(value));
    }

    // ========== Type-specific getter methods ==========

    /// Get the min value as i8.
    /// Panics if the statistics is not for TinyInt type.
    #[inline]
    pub fn get_min_i8(stats: &BaseStatistics) -> Option<i8> {
        Self::minimum_union(stats).map(NumericValueUnion::as_tinyint)
    }

    /// Get the max value as i8.
    #[inline]
    pub fn get_max_i8(stats: &BaseStatistics) -> Option<i8> {
        Self::maximum_union(stats).map(NumericValueUnion::as_tinyint)
    }

    /// Get the min value as i16.
    #[inline]
    pub fn get_min_i16(stats: &BaseStatistics) -> Option<i16> {
        Self::minimum_union(stats).map(NumericValueUnion::as_smallint)
    }

    /// Get the max value as i16.
    #[inline]
    pub fn get_max_i16(stats: &BaseStatistics) -> Option<i16> {
        Self::maximum_union(stats).map(NumericValueUnion::as_smallint)
    }

    /// Get the min value as i32.
    #[inline]
    pub fn get_min_i32(stats: &BaseStatistics) -> Option<i32> {
        Self::minimum_union(stats).map(NumericValueUnion::as_integer)
    }

    /// Get the max value as i32.
    #[inline]
    pub fn get_max_i32(stats: &BaseStatistics) -> Option<i32> {
        Self::maximum_union(stats).map(NumericValueUnion::as_integer)
    }

    /// Get the min value as i64.
    #[inline]
    pub fn get_min_i64(stats: &BaseStatistics) -> Option<i64> {
        Self::minimum_union(stats).map(NumericValueUnion::as_bigint)
    }

    /// Get the max value as i64.
    #[inline]
    pub fn get_max_i64(stats: &BaseStatistics) -> Option<i64> {
        Self::maximum_union(stats).map(NumericValueUnion::as_bigint)
    }

    /// Get the min value as f32.
    #[inline]
    pub fn get_min_f32(stats: &BaseStatistics) -> Option<f32> {
        Self::minimum_union(stats).map(NumericValueUnion::as_float)
    }

    /// Get the max value as f32.
    #[inline]
    pub fn get_max_f32(stats: &BaseStatistics) -> Option<f32> {
        Self::maximum_union(stats).map(NumericValueUnion::as_float)
    }

    /// Get the min value as f64.
    #[inline]
    pub fn get_min_f64(stats: &BaseStatistics) -> Option<f64> {
        Self::minimum_union(stats).map(NumericValueUnion::as_double)
    }

    /// Get the max value as f64.
    #[inline]
    pub fn get_max_f64(stats: &BaseStatistics) -> Option<f64> {
        Self::maximum_union(stats).map(NumericValueUnion::as_double)
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
    /// // If segment has min=10, max=100 and filter is "x > 50"
    /// // Result is NoPruningPossible (some values may satisfy)
    ///
    /// // If segment has min=10, max=100 and filter is "x > 200"
    /// // Result is FilterAlwaysFalse (can prune entire segment)
    ///
    /// // If segment has min=10, max=100 and filter is "x > 5"
    /// // Result is FilterAlwaysTrue (all values satisfy)
    /// ```
    pub fn check_zonemap(
        stats: &BaseStatistics,
        comparison_type: ExpressionType,
        constants: &[Value],
    ) -> FilterPropagateResult {
        // If we don't have min/max, we can't prune
        if !Self::has_min_max(stats) {
            return FilterPropagateResult::NoPruningPossible;
        }

        let Some((minimum, maximum)) =
            Self::get_data(stats).and_then(|data| data.minimum().zip(data.maximum()))
        else {
            return FilterPropagateResult::NoPruningPossible;
        };

        // For each constant, check if the filter could be satisfied
        for constant in constants {
            // Skip NULL constants - they don't help with pruning
            if constant.is_null() {
                continue;
            }

            let Some(const_union) = NumericValueUnion::from_value(constant) else {
                return FilterPropagateResult::NoPruningPossible;
            };

            let result =
                Self::check_zonemap_single(minimum, maximum, comparison_type, &const_union);

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

    /// Check zonemap for a single constant value.
    fn check_zonemap_single(
        min: &NumericValueUnion,
        max: &NumericValueUnion,
        comparison_type: ExpressionType,
        constant: &NumericValueUnion,
    ) -> FilterPropagateResult {
        match comparison_type {
            ExpressionType::CompareEqual | ExpressionType::CompareNotDistinctFrom => {
                // X = C
                // True if min == max == C (constant segment)
                // False if C < min or C > max (out of range)
                // Otherwise, need to scan
                if Self::constant_exact_range(min, max, constant) {
                    FilterPropagateResult::FilterAlwaysTrue
                } else if Self::constant_in_range(min, max, constant) {
                    FilterPropagateResult::NoPruningPossible
                } else {
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }

            ExpressionType::CompareNotEqual | ExpressionType::CompareDistinctFrom => {
                // X != C
                // True if C < min or C > max (out of range)
                // False if min == max == C (constant segment equals C)
                // Otherwise, need to scan
                if !Self::constant_in_range(min, max, constant) {
                    FilterPropagateResult::FilterAlwaysTrue
                } else if Self::constant_exact_range(min, max, constant) {
                    FilterPropagateResult::FilterAlwaysFalse
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            ExpressionType::CompareGreaterThanOrEqualTo => {
                // X >= C
                // True if min >= C (all values >= C)
                // False if max < C (all values < C)
                // Otherwise, need to scan
                if min.compare(constant) != Ordering::Less {
                    // min >= C
                    FilterPropagateResult::FilterAlwaysTrue
                } else if max.compare(constant) == Ordering::Less {
                    // max < C
                    FilterPropagateResult::FilterAlwaysFalse
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            ExpressionType::CompareGreaterThan => {
                // X > C
                // True if min > C (all values > C)
                // False if max <= C (all values <= C)
                // Otherwise, need to scan
                if min.compare(constant) == Ordering::Greater {
                    // min > C
                    FilterPropagateResult::FilterAlwaysTrue
                } else if max.compare(constant) != Ordering::Greater {
                    // max <= C
                    FilterPropagateResult::FilterAlwaysFalse
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            ExpressionType::CompareLessThanOrEqualTo => {
                // X <= C
                // True if max <= C (all values <= C)
                // False if min > C (all values > C)
                // Otherwise, need to scan
                if max.compare(constant) != Ordering::Greater {
                    // max <= C
                    FilterPropagateResult::FilterAlwaysTrue
                } else if min.compare(constant) == Ordering::Greater {
                    // min > C
                    FilterPropagateResult::FilterAlwaysFalse
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            ExpressionType::CompareLessThan => {
                // X < C
                // True if max < C (all values < C)
                // False if min >= C (all values >= C)
                // Otherwise, need to scan
                if max.compare(constant) == Ordering::Less {
                    // max < C
                    FilterPropagateResult::FilterAlwaysTrue
                } else if min.compare(constant) != Ordering::Less {
                    // min >= C
                    FilterPropagateResult::FilterAlwaysFalse
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }

            _ => {
                // Unsupported comparison type
                FilterPropagateResult::NoPruningPossible
            }
        }
    }

    /// Check if constant equals both min and max (constant segment).
    #[inline]
    fn constant_exact_range(
        min: &NumericValueUnion,
        max: &NumericValueUnion,
        constant: &NumericValueUnion,
    ) -> bool {
        constant.compare(min) == Ordering::Equal && constant.compare(max) == Ordering::Equal
    }

    /// Check if constant is within [min, max] range.
    #[inline]
    fn constant_in_range(
        min: &NumericValueUnion,
        max: &NumericValueUnion,
        constant: &NumericValueUnion,
    ) -> bool {
        // constant >= min && constant <= max
        constant.compare(min) != Ordering::Less && constant.compare(max) != Ordering::Greater
    }
}

#[cfg(test)]
#[path = "numeric_stats_tests.rs"]
mod tests;
