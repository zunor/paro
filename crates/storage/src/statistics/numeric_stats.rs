//! ## Design
//! - NumericValueUnion: Union type for storing min/max values of different numeric types
//! - NumericStatsData: Contains has_min, has_max flags and min/max values
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
use super::types::StatsInfo;

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

impl Default for NumericValueUnion {
    fn default() -> Self {
        NumericValueUnion::BigInt(0)
    }
}

impl NumericValueUnion {
    /// Create a new NumericValueUnion with the minimum value for the given type.
    pub fn min_value(ty: &LogicalType) -> Self {
        match ty {
            LogicalType::Boolean => NumericValueUnion::Boolean(false),
            LogicalType::TinyInt => NumericValueUnion::TinyInt(i8::MAX),
            LogicalType::SmallInt => NumericValueUnion::SmallInt(i16::MAX),
            LogicalType::Integer => NumericValueUnion::Integer(i32::MAX),
            LogicalType::BigInt => NumericValueUnion::BigInt(i64::MAX),
            LogicalType::HugeInt => NumericValueUnion::HugeInt(i128::MAX),
            LogicalType::UTinyInt => NumericValueUnion::UTinyInt(u8::MAX),
            LogicalType::USmallInt => NumericValueUnion::USmallInt(u16::MAX),
            LogicalType::UInteger => NumericValueUnion::UInteger(u32::MAX),
            LogicalType::UBigInt => NumericValueUnion::UBigInt(u64::MAX),
            LogicalType::UHugeInt => NumericValueUnion::UHugeInt(u128::MAX),
            LogicalType::Float => NumericValueUnion::Float(f32::INFINITY),
            LogicalType::Double => NumericValueUnion::Double(f64::INFINITY),
            LogicalType::Decimal { .. } => NumericValueUnion::HugeInt(i128::MAX),
            // Temporal types stored as i64
            LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => NumericValueUnion::BigInt(i64::MAX),
            _ => NumericValueUnion::BigInt(i64::MAX),
        }
    }

    /// Create a new NumericValueUnion with the maximum value for the given type.
    pub fn max_value(ty: &LogicalType) -> Self {
        match ty {
            LogicalType::Boolean => NumericValueUnion::Boolean(true),
            LogicalType::TinyInt => NumericValueUnion::TinyInt(i8::MIN),
            LogicalType::SmallInt => NumericValueUnion::SmallInt(i16::MIN),
            LogicalType::Integer => NumericValueUnion::Integer(i32::MIN),
            LogicalType::BigInt => NumericValueUnion::BigInt(i64::MIN),
            LogicalType::HugeInt => NumericValueUnion::HugeInt(i128::MIN),
            LogicalType::UTinyInt => NumericValueUnion::UTinyInt(u8::MIN),
            LogicalType::USmallInt => NumericValueUnion::USmallInt(u16::MIN),
            LogicalType::UInteger => NumericValueUnion::UInteger(u32::MIN),
            LogicalType::UBigInt => NumericValueUnion::UBigInt(u64::MIN),
            LogicalType::UHugeInt => NumericValueUnion::UHugeInt(u128::MIN),
            LogicalType::Float => NumericValueUnion::Float(f32::NEG_INFINITY),
            LogicalType::Double => NumericValueUnion::Double(f64::NEG_INFINITY),
            LogicalType::Decimal { .. } => NumericValueUnion::HugeInt(i128::MIN),
            // Temporal types stored as i64
            LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => NumericValueUnion::BigInt(i64::MIN),
            _ => NumericValueUnion::BigInt(i64::MIN),
        }
    }

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

    /// Convert to a Value, ignoring the logical type hint.
    /// This is useful when the type is already encoded in the union variant.
    #[allow(dead_code)]
    pub fn to_value_direct(&self) -> Value {
        match self {
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

    /// Get the boolean value (panics if not a boolean).
    #[inline]
    pub fn as_boolean(&self) -> bool {
        match self {
            NumericValueUnion::Boolean(v) => *v,
            _ => panic!("NumericValueUnion is not a boolean"),
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

    /// Get the i128 value (panics if not a HugeInt).
    #[inline]
    pub fn as_hugeint(&self) -> i128 {
        match self {
            NumericValueUnion::HugeInt(v) => *v,
            _ => panic!("NumericValueUnion is not a HugeInt"),
        }
    }

    /// Get the u8 value (panics if not a UTinyInt).
    #[inline]
    pub fn as_utinyint(&self) -> u8 {
        match self {
            NumericValueUnion::UTinyInt(v) => *v,
            _ => panic!("NumericValueUnion is not a UTinyInt"),
        }
    }

    /// Get the u16 value (panics if not a USmallInt).
    #[inline]
    pub fn as_usmallint(&self) -> u16 {
        match self {
            NumericValueUnion::USmallInt(v) => *v,
            _ => panic!("NumericValueUnion is not a USmallInt"),
        }
    }

    /// Get the u32 value (panics if not a UInteger).
    #[inline]
    pub fn as_uinteger(&self) -> u32 {
        match self {
            NumericValueUnion::UInteger(v) => *v,
            _ => panic!("NumericValueUnion is not a UInteger"),
        }
    }

    /// Get the u64 value (panics if not a UBigInt).
    #[inline]
    pub fn as_ubigint(&self) -> u64 {
        match self {
            NumericValueUnion::UBigInt(v) => *v,
            _ => panic!("NumericValueUnion is not a UBigInt"),
        }
    }

    /// Get the u128 value (panics if not a UHugeInt).
    #[inline]
    pub fn as_uhugeint(&self) -> u128 {
        match self {
            NumericValueUnion::UHugeInt(v) => *v,
            _ => panic!("NumericValueUnion is not a UHugeInt"),
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

    /// Set the boolean value (panics if not a boolean).
    #[inline]
    pub fn set_boolean(&mut self, value: bool) {
        *self = NumericValueUnion::Boolean(value);
    }

    /// Set the i8 value.
    #[inline]
    pub fn set_tinyint(&mut self, value: i8) {
        *self = NumericValueUnion::TinyInt(value);
    }

    /// Set the i16 value.
    #[inline]
    pub fn set_smallint(&mut self, value: i16) {
        *self = NumericValueUnion::SmallInt(value);
    }

    /// Set the i32 value.
    #[inline]
    pub fn set_integer(&mut self, value: i32) {
        *self = NumericValueUnion::Integer(value);
    }

    /// Set the i64 value.
    #[inline]
    pub fn set_bigint(&mut self, value: i64) {
        *self = NumericValueUnion::BigInt(value);
    }

    /// Set the i128 value.
    #[inline]
    pub fn set_hugeint(&mut self, value: i128) {
        *self = NumericValueUnion::HugeInt(value);
    }

    /// Set the u8 value.
    #[inline]
    pub fn set_utinyint(&mut self, value: u8) {
        *self = NumericValueUnion::UTinyInt(value);
    }

    /// Set the u16 value.
    #[inline]
    pub fn set_usmallint(&mut self, value: u16) {
        *self = NumericValueUnion::USmallInt(value);
    }

    /// Set the u32 value.
    #[inline]
    pub fn set_uinteger(&mut self, value: u32) {
        *self = NumericValueUnion::UInteger(value);
    }

    /// Set the u64 value.
    #[inline]
    pub fn set_ubigint(&mut self, value: u64) {
        *self = NumericValueUnion::UBigInt(value);
    }

    /// Set the u128 value.
    #[inline]
    pub fn set_uhugeint(&mut self, value: u128) {
        *self = NumericValueUnion::UHugeInt(value);
    }

    /// Set the f32 value.
    #[inline]
    pub fn set_float(&mut self, value: f32) {
        *self = NumericValueUnion::Float(value);
    }

    /// Set the f64 value.
    #[inline]
    pub fn set_double(&mut self, value: f64) {
        *self = NumericValueUnion::Double(value);
    }
}

/// Numeric statistics data.
///
/// Contains min/max values and flags indicating whether they are set.
/// This mirrors the `NumericStatsData` structure.
#[derive(Debug, Clone)]
pub struct NumericStatsData {
    /// Whether the statistics has a minimum value
    pub has_min: bool,
    /// Whether the statistics has a maximum value
    pub has_max: bool,
    /// The minimum value of the segment
    pub min: NumericValueUnion,
    /// The maximum value of the segment
    pub max: NumericValueUnion,
}

impl Default for NumericStatsData {
    fn default() -> Self {
        Self {
            has_min: false,
            has_max: false,
            min: NumericValueUnion::default(),
            max: NumericValueUnion::default(),
        }
    }
}

impl NumericStatsData {
    /// Create new numeric stats data with unknown min/max.
    pub fn new_unknown() -> Self {
        Self {
            has_min: false,
            has_max: false,
            min: NumericValueUnion::default(),
            max: NumericValueUnion::default(),
        }
    }

    /// Create new numeric stats data with empty min/max for the given type.
    /// Empty means min = MAX_VALUE, max = MIN_VALUE (inverted for updates).
    pub fn new_empty(ty: &LogicalType) -> Self {
        Self {
            has_min: true,
            has_max: true,
            min: NumericValueUnion::min_value(ty),
            max: NumericValueUnion::max_value(ty),
        }
    }

    /// Check if the statistics has both min and max values.
    pub fn has_min_max(&self) -> bool {
        self.has_min && self.has_max
    }

    /// Check if the statistics represents a constant value (min == max).
    pub fn is_constant(&self) -> bool {
        if !self.has_min_max() {
            return false;
        }
        self.min.compare(&self.max) == Ordering::Equal
    }

    /// Get the minimum value as a Value.
    pub fn min_value(&self, ty: &LogicalType) -> Option<Value> {
        if self.has_min {
            Some(self.min.to_value(ty))
        } else {
            None
        }
    }

    /// Get the maximum value as a Value.
    pub fn max_value(&self, ty: &LogicalType) -> Option<Value> {
        if self.has_max {
            Some(self.max.to_value(ty))
        } else {
            None
        }
    }

    /// Set the minimum value from a Value.
    pub fn set_min(&mut self, value: &Value) {
        if let Some(v) = NumericValueUnion::from_value(value) {
            self.min = v;
            self.has_min = true;
        }
    }

    /// Set the maximum value from a Value.
    pub fn set_max(&mut self, value: &Value) {
        if let Some(v) = NumericValueUnion::from_value(value) {
            self.max = v;
            self.has_max = true;
        }
    }

    /// Update the statistics with a new value.
    /// Updates min if new_value < min, updates max if new_value > max.
    pub fn update(&mut self, value: &Value) {
        if let Some(v) = NumericValueUnion::from_value(value) {
            if !self.has_min || v.compare(&self.min) == Ordering::Less {
                self.min = v;
                self.has_min = true;
            }
            if !self.has_max || v.compare(&self.max) == Ordering::Greater {
                self.max = v;
                self.has_max = true;
            }
        }
    }

    /// Merge another NumericStatsData into this one.
    pub fn merge(&mut self, other: &NumericStatsData) {
        if other.has_min && (!self.has_min || other.min.compare(&self.min) == Ordering::Less) {
            self.min = other.min;
            self.has_min = true;
        }
        if other.has_max && (!self.has_max || other.max.compare(&self.max) == Ordering::Greater) {
            self.max = other.max;
            self.has_max = true;
        }
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
/// NumericStats::set_min(&mut stats, &Value::Integer(10));
/// NumericStats::set_max(&mut stats, &Value::Integer(100));
/// let min = NumericStats::min(&stats);
/// ```
pub struct NumericStats;

impl NumericStats {
    /// Create unknown statistics for a numeric type.
    /// "has_min" is false, "has_max" is false.
    /// This can be used when nothing is known about the data.
    pub fn create_unknown(data_type: LogicalType) -> BaseStatistics {
        let mut stats = BaseStatistics::create_unknown(data_type.clone());
        // Set min/max to NULL values (unknown)
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.has_min = false;
            data.has_max = false;
        }
        stats
    }

    /// Create empty statistics for a numeric type.
    /// "min = MaxValue<type>, max = MinValue<type>" for update logic.
    /// This is used when incrementally constructing statistics.
    pub fn create_empty(data_type: LogicalType) -> BaseStatistics {
        let mut stats = BaseStatistics::create_empty(data_type.clone());
        // Empty stats have inverted min/max for update logic
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.has_min = true;
            data.has_max = true;
            data.min = NumericValueUnion::min_value(&data_type);
            data.max = NumericValueUnion::max_value(&data_type);
        }
        stats
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
            data.has_min
        } else {
            false
        }
    }

    /// Returns true if the stats has a max value defined.
    pub fn has_max(stats: &BaseStatistics) -> bool {
        if let StatsData::Numeric(data) = stats.stats_data() {
            data.has_max
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

    /// Returns the min value or a NULL value if not set.
    pub fn min_or_null(stats: &BaseStatistics) -> Value {
        Self::min(stats).unwrap_or_else(|| Value::Null(stats.get_type().clone()))
    }

    /// Returns the max value or a NULL value if not set.
    pub fn max_or_null(stats: &BaseStatistics) -> Value {
        Self::max(stats).unwrap_or_else(|| Value::Null(stats.get_type().clone()))
    }

    /// Sets the min value of the statistics.
    pub fn set_min(stats: &mut BaseStatistics, val: &Value) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            if val.is_null() {
                data.has_min = false;
            } else {
                data.set_min(val);
            }
        }
    }

    /// Sets the max value of the statistics.
    pub fn set_max(stats: &mut BaseStatistics, val: &Value) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            if val.is_null() {
                data.has_max = false;
            } else {
                data.set_max(val);
            }
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

    /// Merge another statistics into this one.
    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        // Merge validity flags
        if other.can_have_null() {
            stats.set(StatsInfo::CanHaveNullValues);
        }
        if other.can_have_no_null() {
            stats.set(StatsInfo::CanHaveValidValues);
        }

        // Merge numeric data
        if let (StatsData::Numeric(self_data), StatsData::Numeric(other_data)) =
            (stats.stats_data_mut(), other.stats_data())
        {
            // Merge min
            if other_data.has_min && self_data.has_min {
                if other_data.min.compare(&self_data.min) == Ordering::Less {
                    self_data.min = other_data.min;
                }
            } else if !other_data.has_min || !self_data.has_min {
                // If either doesn't have min, result doesn't have min
                self_data.has_min = false;
            }

            // Merge max
            if other_data.has_max && self_data.has_max {
                if other_data.max.compare(&self_data.max) == Ordering::Greater {
                    self_data.max = other_data.max;
                }
            } else if !other_data.has_max || !self_data.has_max {
                // If either doesn't have max, result doesn't have max
                self_data.has_max = false;
            }
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

    /// Update statistics with a boolean value.
    #[inline]
    pub fn update_bool(stats: &mut BaseStatistics, value: bool) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::Boolean(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an i8 value.
    #[inline]
    pub fn update_i8(stats: &mut BaseStatistics, value: i8) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::TinyInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an i16 value.
    #[inline]
    pub fn update_i16(stats: &mut BaseStatistics, value: i16) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::SmallInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an i32 value.
    #[inline]
    pub fn update_i32(stats: &mut BaseStatistics, value: i32) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::Integer(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an i64 value.
    #[inline]
    pub fn update_i64(stats: &mut BaseStatistics, value: i64) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::BigInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an i128 value.
    #[inline]
    pub fn update_i128(stats: &mut BaseStatistics, value: i128) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::HugeInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with a u8 value.
    #[inline]
    pub fn update_u8(stats: &mut BaseStatistics, value: u8) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::UTinyInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with a u16 value.
    #[inline]
    pub fn update_u16(stats: &mut BaseStatistics, value: u16) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::USmallInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with a u32 value.
    #[inline]
    pub fn update_u32(stats: &mut BaseStatistics, value: u32) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::UInteger(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with a u64 value.
    #[inline]
    pub fn update_u64(stats: &mut BaseStatistics, value: u64) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::UBigInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with a u128 value.
    #[inline]
    pub fn update_u128(stats: &mut BaseStatistics, value: u128) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::UHugeInt(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an f32 value.
    #[inline]
    pub fn update_f32(stats: &mut BaseStatistics, value: f32) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::Float(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    /// Update statistics with an f64 value.
    #[inline]
    pub fn update_f64(stats: &mut BaseStatistics, value: f64) {
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            let v = NumericValueUnion::Double(value);
            if !data.has_min || v.compare(&data.min) == Ordering::Less {
                data.min = v;
                data.has_min = true;
            }
            if !data.has_max || v.compare(&data.max) == Ordering::Greater {
                data.max = v;
                data.has_max = true;
            }
        }
        stats.set_has_no_null_fast();
    }

    // ========== Type-specific getter methods ==========

    /// Get the min value as i8.
    /// Panics if the statistics is not for TinyInt type.
    #[inline]
    pub fn get_min_i8(stats: &BaseStatistics) -> Option<i8> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_tinyint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as i8.
    #[inline]
    pub fn get_max_i8(stats: &BaseStatistics) -> Option<i8> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_tinyint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the min value as i16.
    #[inline]
    pub fn get_min_i16(stats: &BaseStatistics) -> Option<i16> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_smallint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as i16.
    #[inline]
    pub fn get_max_i16(stats: &BaseStatistics) -> Option<i16> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_smallint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the min value as i32.
    #[inline]
    pub fn get_min_i32(stats: &BaseStatistics) -> Option<i32> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_integer())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as i32.
    #[inline]
    pub fn get_max_i32(stats: &BaseStatistics) -> Option<i32> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_integer())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the min value as i64.
    #[inline]
    pub fn get_min_i64(stats: &BaseStatistics) -> Option<i64> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_bigint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as i64.
    #[inline]
    pub fn get_max_i64(stats: &BaseStatistics) -> Option<i64> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_bigint())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the min value as f32.
    #[inline]
    pub fn get_min_f32(stats: &BaseStatistics) -> Option<f32> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_float())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as f32.
    #[inline]
    pub fn get_max_f32(stats: &BaseStatistics) -> Option<f32> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_float())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the min value as f64.
    #[inline]
    pub fn get_min_f64(stats: &BaseStatistics) -> Option<f64> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_min {
                Some(data.min.as_double())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the max value as f64.
    #[inline]
    pub fn get_max_f64(stats: &BaseStatistics) -> Option<f64> {
        if let StatsData::Numeric(data) = stats.stats_data() {
            if data.has_max {
                Some(data.max.as_double())
            } else {
                None
            }
        } else {
            None
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

        let Some(data) = Self::get_data(stats) else {
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
                Self::check_zonemap_single(&data.min, &data.max, comparison_type, &const_union);

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
mod tests {
    use super::*;

    #[test]
    fn test_numeric_value_union_from_value() {
        // Test integer types
        assert!(matches!(
            NumericValueUnion::from_value(&Value::Integer(42)),
            Some(NumericValueUnion::Integer(42))
        ));
        assert!(matches!(
            NumericValueUnion::from_value(&Value::BigInt(123456789)),
            Some(NumericValueUnion::BigInt(123456789))
        ));
        assert!(matches!(
            NumericValueUnion::from_value(&Value::TinyInt(-10)),
            Some(NumericValueUnion::TinyInt(-10))
        ));

        // Test unsigned types
        assert!(matches!(
            NumericValueUnion::from_value(&Value::UInteger(100)),
            Some(NumericValueUnion::UInteger(100))
        ));

        // Test floating point
        let float_val = NumericValueUnion::from_value(&Value::Float(std::f32::consts::PI));
        assert!(matches!(float_val, Some(NumericValueUnion::Float(_))));

        let double_val = NumericValueUnion::from_value(&Value::Double(std::f64::consts::E));
        assert!(matches!(double_val, Some(NumericValueUnion::Double(_))));

        // Test boolean
        assert!(matches!(
            NumericValueUnion::from_value(&Value::Boolean(true)),
            Some(NumericValueUnion::Boolean(true))
        ));

        // Test non-numeric types return None
        assert!(NumericValueUnion::from_value(&Value::Varchar("hello".to_string())).is_none());
        assert!(NumericValueUnion::from_value(&Value::Null(LogicalType::Integer)).is_none());
    }

    #[test]
    fn test_numeric_value_union_to_value() {
        let union = NumericValueUnion::Integer(42);
        assert_eq!(union.to_value(&LogicalType::Integer), Value::Integer(42));

        let union = NumericValueUnion::BigInt(1000);
        assert_eq!(union.to_value(&LogicalType::BigInt), Value::BigInt(1000));
    }

    #[test]
    fn test_numeric_value_union_compare() {
        let a = NumericValueUnion::Integer(10);
        let b = NumericValueUnion::Integer(20);
        let c = NumericValueUnion::Integer(10);

        assert_eq!(a.compare(&b), Ordering::Less);
        assert_eq!(b.compare(&a), Ordering::Greater);
        assert_eq!(a.compare(&c), Ordering::Equal);

        // Test floating point comparison
        let f1 = NumericValueUnion::Double(1.5);
        let f2 = NumericValueUnion::Double(2.5);
        assert_eq!(f1.compare(&f2), Ordering::Less);
    }

    #[test]
    fn test_numeric_value_union_min_max_values() {
        // Test that min_value returns MAX for the type (for initial comparison)
        let min = NumericValueUnion::min_value(&LogicalType::Integer);
        assert!(matches!(min, NumericValueUnion::Integer(i32::MAX)));

        // Test that max_value returns MIN for the type (for initial comparison)
        let max = NumericValueUnion::max_value(&LogicalType::Integer);
        assert!(matches!(max, NumericValueUnion::Integer(i32::MIN)));

        // Test floating point
        let min_f = NumericValueUnion::min_value(&LogicalType::Double);
        assert!(matches!(min_f, NumericValueUnion::Double(f64::INFINITY)));

        let max_f = NumericValueUnion::max_value(&LogicalType::Double);
        assert!(matches!(
            max_f,
            NumericValueUnion::Double(f64::NEG_INFINITY)
        ));
    }

    #[test]
    fn test_numeric_stats_data_new_unknown() {
        let stats = NumericStatsData::new_unknown();
        assert!(!stats.has_min);
        assert!(!stats.has_max);
        assert!(!stats.has_min_max());
    }

    #[test]
    fn test_numeric_stats_data_new_empty() {
        let stats = NumericStatsData::new_empty(&LogicalType::Integer);
        assert!(stats.has_min);
        assert!(stats.has_max);
        assert!(stats.has_min_max());

        // Empty stats have inverted min/max for update logic
        assert!(matches!(stats.min, NumericValueUnion::Integer(i32::MAX)));
        assert!(matches!(stats.max, NumericValueUnion::Integer(i32::MIN)));
    }

    #[test]
    fn test_numeric_stats_data_update() {
        let mut stats = NumericStatsData::new_empty(&LogicalType::Integer);

        // Update with first value
        stats.update(&Value::Integer(50));
        assert_eq!(
            stats.min_value(&LogicalType::Integer),
            Some(Value::Integer(50))
        );
        assert_eq!(
            stats.max_value(&LogicalType::Integer),
            Some(Value::Integer(50))
        );

        // Update with smaller value
        stats.update(&Value::Integer(10));
        assert_eq!(
            stats.min_value(&LogicalType::Integer),
            Some(Value::Integer(10))
        );
        assert_eq!(
            stats.max_value(&LogicalType::Integer),
            Some(Value::Integer(50))
        );

        // Update with larger value
        stats.update(&Value::Integer(100));
        assert_eq!(
            stats.min_value(&LogicalType::Integer),
            Some(Value::Integer(10))
        );
        assert_eq!(
            stats.max_value(&LogicalType::Integer),
            Some(Value::Integer(100))
        );
    }

    #[test]
    fn test_numeric_stats_data_merge() {
        let mut stats1 = NumericStatsData::new_empty(&LogicalType::Integer);
        stats1.update(&Value::Integer(10));
        stats1.update(&Value::Integer(50));

        let mut stats2 = NumericStatsData::new_empty(&LogicalType::Integer);
        stats2.update(&Value::Integer(5));
        stats2.update(&Value::Integer(30));

        stats1.merge(&stats2);

        // After merge, min should be 5 (from stats2), max should be 50 (from stats1)
        assert_eq!(
            stats1.min_value(&LogicalType::Integer),
            Some(Value::Integer(5))
        );
        assert_eq!(
            stats1.max_value(&LogicalType::Integer),
            Some(Value::Integer(50))
        );
    }

    #[test]
    fn test_numeric_stats_data_is_constant() {
        let mut stats = NumericStatsData::new_empty(&LogicalType::Integer);

        // Not constant when no values
        assert!(!stats.is_constant());

        // Constant when only one value
        stats.update(&Value::Integer(42));
        assert!(stats.is_constant());

        // Not constant when different values
        stats.update(&Value::Integer(43));
        assert!(!stats.is_constant());
    }

    #[test]
    fn test_numeric_stats_data_set_min_max() {
        let mut stats = NumericStatsData::new_unknown();

        stats.set_min(&Value::Integer(10));
        assert!(stats.has_min);
        assert!(!stats.has_max);
        assert_eq!(
            stats.min_value(&LogicalType::Integer),
            Some(Value::Integer(10))
        );

        stats.set_max(&Value::Integer(100));
        assert!(stats.has_min);
        assert!(stats.has_max);
        assert_eq!(
            stats.max_value(&LogicalType::Integer),
            Some(Value::Integer(100))
        );
    }

    #[test]
    fn test_numeric_stats_data_floating_point() {
        let mut stats = NumericStatsData::new_empty(&LogicalType::Double);

        stats.update(&Value::Double(1.5));
        stats.update(&Value::Double(std::f64::consts::PI));
        stats.update(&Value::Double(-2.5));

        let min = stats.min_value(&LogicalType::Double);
        let max = stats.max_value(&LogicalType::Double);

        assert!(matches!(min, Some(Value::Double(v)) if (v - (-2.5)).abs() < f64::EPSILON));
        assert!(matches!(
            max,
            Some(Value::Double(v)) if (v - std::f64::consts::PI).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn test_numeric_value_union_accessors() {
        let mut union = NumericValueUnion::Integer(42);
        assert_eq!(union.as_integer(), 42);

        union.set_integer(100);
        assert_eq!(union.as_integer(), 100);

        let mut union = NumericValueUnion::Boolean(false);
        assert!(!union.as_boolean());
        union.set_boolean(true);
        assert!(union.as_boolean());
    }

    // ========== NumericStats tests ==========

    #[test]
    fn test_numeric_stats_create_unknown() {
        let stats = NumericStats::create_unknown(LogicalType::Integer);
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert!(!NumericStats::has_min(&stats));
        assert!(!NumericStats::has_max(&stats));
        assert!(!NumericStats::has_min_max(&stats));
    }

    #[test]
    fn test_numeric_stats_create_empty() {
        let stats = NumericStats::create_empty(LogicalType::Integer);
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert!(NumericStats::has_min(&stats));
        assert!(NumericStats::has_max(&stats));
        // Empty stats have inverted min/max (min=MAX, max=MIN), so has_min_max
        // returns true because both has_min and has_max are true.
        // The actual values are inverted for update logic.
        assert!(NumericStats::has_min_max(&stats));
    }

    #[test]
    fn test_numeric_stats_set_min_max() {
        let mut stats = NumericStats::create_unknown(LogicalType::Integer);

        NumericStats::set_min(&mut stats, &Value::Integer(10));
        assert!(NumericStats::has_min(&stats));
        assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));

        NumericStats::set_max(&mut stats, &Value::Integer(100));
        assert!(NumericStats::has_max(&stats));
        assert_eq!(NumericStats::max(&stats), Some(Value::Integer(100)));

        assert!(NumericStats::has_min_max(&stats));
    }

    #[test]
    fn test_numeric_stats_update() {
        let mut stats = NumericStats::create_empty(LogicalType::Integer);

        NumericStats::update(&mut stats, &Value::Integer(50));
        assert!(stats.can_have_no_null());
        assert_eq!(NumericStats::min(&stats), Some(Value::Integer(50)));
        assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));

        NumericStats::update(&mut stats, &Value::Integer(10));
        assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));
        assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));

        NumericStats::update(&mut stats, &Value::Integer(100));
        assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));
        assert_eq!(NumericStats::max(&stats), Some(Value::Integer(100)));
    }

    #[test]
    fn test_numeric_stats_update_with_null() {
        let mut stats = NumericStats::create_empty(LogicalType::Integer);

        NumericStats::update(&mut stats, &Value::Integer(50));
        assert!(!stats.can_have_null());
        assert!(stats.can_have_no_null());

        NumericStats::update(&mut stats, &Value::Null(LogicalType::Integer));
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());

        // Min/max should not change after null update
        assert_eq!(NumericStats::min(&stats), Some(Value::Integer(50)));
        assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));
    }

    #[test]
    fn test_numeric_stats_merge() {
        let mut stats1 = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats1, &Value::Integer(10));
        NumericStats::update(&mut stats1, &Value::Integer(50));

        let mut stats2 = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats2, &Value::Integer(5));
        NumericStats::update(&mut stats2, &Value::Integer(30));
        NumericStats::update(&mut stats2, &Value::Null(LogicalType::Integer));

        NumericStats::merge(&mut stats1, &stats2);

        // After merge, min should be 5 (from stats2), max should be 50 (from stats1)
        assert_eq!(NumericStats::min(&stats1), Some(Value::Integer(5)));
        assert_eq!(NumericStats::max(&stats1), Some(Value::Integer(50)));
        // stats2 had null, so merged stats should have null
        assert!(stats1.can_have_null());
    }

    #[test]
    fn test_numeric_stats_is_constant() {
        let mut stats = NumericStats::create_empty(LogicalType::Integer);

        NumericStats::update(&mut stats, &Value::Integer(42));
        assert!(NumericStats::is_constant(&stats));

        NumericStats::update(&mut stats, &Value::Integer(43));
        assert!(!NumericStats::is_constant(&stats));
    }

    #[test]
    fn test_numeric_stats_typed_update() {
        let mut stats = NumericStats::create_empty(LogicalType::Integer);

        NumericStats::update_i32(&mut stats, 50);
        assert_eq!(NumericStats::get_min_i32(&stats), Some(50));
        assert_eq!(NumericStats::get_max_i32(&stats), Some(50));

        NumericStats::update_i32(&mut stats, 10);
        assert_eq!(NumericStats::get_min_i32(&stats), Some(10));
        assert_eq!(NumericStats::get_max_i32(&stats), Some(50));

        NumericStats::update_i32(&mut stats, 100);
        assert_eq!(NumericStats::get_min_i32(&stats), Some(10));
        assert_eq!(NumericStats::get_max_i32(&stats), Some(100));
    }

    #[test]
    fn test_numeric_stats_typed_update_i64() {
        let mut stats = NumericStats::create_empty(LogicalType::BigInt);

        NumericStats::update_i64(&mut stats, 1000000000000i64);
        assert_eq!(NumericStats::get_min_i64(&stats), Some(1000000000000i64));
        assert_eq!(NumericStats::get_max_i64(&stats), Some(1000000000000i64));

        NumericStats::update_i64(&mut stats, -1000000000000i64);
        assert_eq!(NumericStats::get_min_i64(&stats), Some(-1000000000000i64));
        assert_eq!(NumericStats::get_max_i64(&stats), Some(1000000000000i64));
    }

    #[test]
    fn test_numeric_stats_typed_update_f64() {
        let mut stats = NumericStats::create_empty(LogicalType::Double);

        NumericStats::update_f64(&mut stats, std::f64::consts::PI);
        let min = NumericStats::get_min_f64(&stats);
        let max = NumericStats::get_max_f64(&stats);
        assert!(min.is_some());
        assert!((min.unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);
        assert!(max.is_some());
        assert!((max.unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);

        NumericStats::update_f64(&mut stats, -2.5);
        let min = NumericStats::get_min_f64(&stats);
        assert!(min.is_some());
        assert!((min.unwrap() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_numeric_stats_to_string() {
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let s = NumericStats::to_string(&stats);
        assert!(s.contains("Min:"));
        assert!(s.contains("Max:"));
        assert!(s.contains("10"));
        assert!(s.contains("100"));
    }

    #[test]
    fn test_numeric_stats_min_or_null() {
        let stats = NumericStats::create_unknown(LogicalType::Integer);
        let min = NumericStats::min_or_null(&stats);
        assert!(min.is_null());

        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(42));
        let min = NumericStats::min_or_null(&stats);
        assert_eq!(min, Value::Integer(42));
    }

    #[test]
    fn test_numeric_stats_get_data() {
        let stats = NumericStats::create_empty(LogicalType::Integer);
        let data = NumericStats::get_data(&stats);
        assert!(data.is_some());
        assert!(data.unwrap().has_min);
        assert!(data.unwrap().has_max);

        // String stats should return None
        let string_stats = BaseStatistics::create_empty(LogicalType::Varchar);
        let data = NumericStats::get_data(&string_stats);
        assert!(data.is_none());
    }

    // ========== CheckZonemap tests ==========

    #[test]
    fn test_check_zonemap_no_min_max() {
        let stats = NumericStats::create_unknown(LogicalType::Integer);
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_equal_in_range() {
        // Segment: min=10, max=100
        // Filter: x = 50 (in range)
        // Result: NoPruningPossible
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_equal_out_of_range() {
        // Segment: min=10, max=100
        // Filter: x = 200 (out of range)
        // Result: FilterAlwaysFalse (can prune)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(200)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_equal_constant_segment() {
        // Segment: min=50, max=50 (constant)
        // Filter: x = 50
        // Result: FilterAlwaysTrue
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(50));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_not_equal_out_of_range() {
        // Segment: min=10, max=100
        // Filter: x != 200 (out of range)
        // Result: FilterAlwaysTrue (all values satisfy)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotEqual,
            &[Value::Integer(200)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_not_equal_constant_segment() {
        // Segment: min=50, max=50 (constant)
        // Filter: x != 50
        // Result: FilterAlwaysFalse (no values satisfy)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(50));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotEqual,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_greater_than_always_true() {
        // Segment: min=100, max=200
        // Filter: x > 50
        // Result: FilterAlwaysTrue (min > 50)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(100));
        NumericStats::update(&mut stats, &Value::Integer(200));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_greater_than_always_false() {
        // Segment: min=10, max=100
        // Filter: x > 200
        // Result: FilterAlwaysFalse (max <= 200)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Integer(200)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_greater_than_no_pruning() {
        // Segment: min=10, max=100
        // Filter: x > 50
        // Result: NoPruningPossible (some values may satisfy)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_less_than_always_true() {
        // Segment: min=10, max=50
        // Filter: x < 100
        // Result: FilterAlwaysTrue (max < 100)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(50));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::Integer(100)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_less_than_always_false() {
        // Segment: min=100, max=200
        // Filter: x < 50
        // Result: FilterAlwaysFalse (min >= 50)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(100));
        NumericStats::update(&mut stats, &Value::Integer(200));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_greater_than_or_equal() {
        // Segment: min=100, max=200
        // Filter: x >= 100
        // Result: FilterAlwaysTrue (min >= 100)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(100));
        NumericStats::update(&mut stats, &Value::Integer(200));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThanOrEqualTo,
            &[Value::Integer(100)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_less_than_or_equal() {
        // Segment: min=10, max=100
        // Filter: x <= 100
        // Result: FilterAlwaysTrue (max <= 100)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThanOrEqualTo,
            &[Value::Integer(100)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_multiple_constants_in() {
        // Segment: min=10, max=100
        // Filter: x IN (5, 50, 200)
        // Result: NoPruningPossible (50 is in range)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(5), Value::Integer(50), Value::Integer(200)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_multiple_constants_all_out() {
        // Segment: min=10, max=100
        // Filter: x IN (5, 200, 300)
        // Result: FilterAlwaysFalse (all out of range)
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareEqual,
            &[Value::Integer(5), Value::Integer(200), Value::Integer(300)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_floating_point() {
        // Segment: min=1.5, max=10.5
        // Filter: x > 5.0
        // Result: NoPruningPossible
        let mut stats = NumericStats::create_empty(LogicalType::Double);
        NumericStats::update(&mut stats, &Value::Double(1.5));
        NumericStats::update(&mut stats, &Value::Double(10.5));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Double(5.0)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);

        // Filter: x > 20.0
        // Result: FilterAlwaysFalse
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Double(20.0)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }

    #[test]
    fn test_check_zonemap_bigint() {
        // Segment: min=1000000000000, max=2000000000000
        let mut stats = NumericStats::create_empty(LogicalType::BigInt);
        NumericStats::update(&mut stats, &Value::BigInt(1000000000000i64));
        NumericStats::update(&mut stats, &Value::BigInt(2000000000000i64));

        // Filter: x < 500000000000
        // Result: FilterAlwaysFalse
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::BigInt(500000000000i64)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

        // Filter: x > 500000000000
        // Result: FilterAlwaysTrue
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::BigInt(500000000000i64)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_boundary_cases() {
        // Segment: min=10, max=100
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(10));
        NumericStats::update(&mut stats, &Value::Integer(100));

        // x > 100 should be FilterAlwaysFalse (max is 100, not > 100)
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThan,
            &[Value::Integer(100)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

        // x >= 100 should be NoPruningPossible (max == 100)
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareGreaterThanOrEqualTo,
            &[Value::Integer(100)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);

        // x < 10 should be FilterAlwaysFalse (min is 10, not < 10)
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThan,
            &[Value::Integer(10)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

        // x <= 10 should be NoPruningPossible (min == 10)
        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareLessThanOrEqualTo,
            &[Value::Integer(10)],
        );
        assert_eq!(result, FilterPropagateResult::NoPruningPossible);
    }

    #[test]
    fn test_check_zonemap_not_distinct_from() {
        // IS NOT DISTINCT FROM behaves like = for non-null values
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(50));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareNotDistinctFrom,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
    }

    #[test]
    fn test_check_zonemap_distinct_from() {
        // IS DISTINCT FROM behaves like != for non-null values
        let mut stats = NumericStats::create_empty(LogicalType::Integer);
        NumericStats::update(&mut stats, &Value::Integer(50));

        let result = NumericStats::check_zonemap(
            &stats,
            ExpressionType::CompareDistinctFrom,
            &[Value::Integer(50)],
        );
        assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
    }
}
