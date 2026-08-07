// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - BaseStatistics is the core statistics class
//! - Uses a union (StatsData enum in Rust) for type-specific stats
//! - Child stats for nested types (List, Struct, Array)
//! - Factory methods: CreateUnknown, CreateEmpty, FromConstant

use paro_common::error as paro_error;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::numeric_stats::{NumericStats, NumericStatsData};
use super::string_stats::StringStatsData;
use super::types::{StatisticsType, StatsInfo};

/// Type-specific statistics data.
///
/// This enum mirrors the union of NumericStatsData and StringStatsData.
#[derive(Debug, Clone)]
pub enum StatsData {
    /// Numeric statistics (min/max for integers, floats, etc.)
    Numeric(NumericStatsData),
    /// String statistics (min/max prefix, unicode flag, max length)
    String(StringStatsData),
    /// Base statistics only (no type-specific data)
    Base,
}

impl Default for StatsData {
    fn default() -> Self {
        StatsData::Base
    }
}

/// Base statistics class.
///
/// This is the core statistics structure that tracks:
/// - Null/valid value flags
/// - Type-specific statistics (numeric min/max, string stats, etc.)
/// - Child statistics for nested types
#[derive(Debug, Clone)]
pub struct BaseStatistics {
    /// The logical type of the statistics
    data_type: LogicalType,
    /// Whether the data can have NULL values
    has_null: bool,
    /// Whether the data can have non-NULL values
    has_no_null: bool,
    /// Distinct count estimate
    distinct_count: usize,
    /// Type-specific stats (union-like enum)
    stats_data: StatsData,
    /// Child stats for nested types (List, Struct, Array)
    child_stats: Option<Vec<BaseStatistics>>,
}

impl Default for BaseStatistics {
    fn default() -> Self {
        Self {
            data_type: LogicalType::Unknown,
            has_null: true,
            has_no_null: true,
            distinct_count: 0,
            stats_data: StatsData::Base,
            child_stats: None,
        }
    }
}

impl BaseStatistics {
    /// Create a new BaseStatistics with the given type.
    pub fn new(data_type: LogicalType) -> Self {
        let stats_type = StatisticsType::from_logical_type(&data_type);
        let stats_data = match stats_type {
            StatisticsType::NumericStats => StatsData::Numeric(NumericStatsData::new_unknown()),
            StatisticsType::StringStats => StatsData::String(StringStatsData::new_unknown()),
            _ => StatsData::Base,
        };

        let child_stats = Self::construct_child_stats(&data_type);

        Self {
            data_type,
            has_null: false,
            has_no_null: false,
            distinct_count: 0,
            stats_data,
            child_stats,
        }
    }

    /// Construct child stats for nested types.
    fn construct_child_stats(data_type: &LogicalType) -> Option<Vec<BaseStatistics>> {
        match data_type {
            LogicalType::List(child_type) => {
                Some(vec![BaseStatistics::new(child_type.as_ref().clone())])
            }
            LogicalType::Array(child_type, _) => {
                Some(vec![BaseStatistics::new(child_type.as_ref().clone())])
            }
            LogicalType::Struct(fields) => {
                let children: Vec<BaseStatistics> = fields
                    .iter()
                    .map(|(_, ty)| BaseStatistics::new(ty.clone()))
                    .collect();
                Some(children)
            }
            _ => None,
        }
    }

    /// Creates a set of statistics for data that is unknown.
    /// "has_null" is true, "has_no_null" is true.
    /// This can be used when nothing is known about the data.
    pub fn create_unknown(data_type: LogicalType) -> Self {
        let stats_type = StatisticsType::from_logical_type(&data_type);
        let stats_data = match stats_type {
            StatisticsType::NumericStats => StatsData::Numeric(NumericStatsData::new_unknown()),
            StatisticsType::StringStats => StatsData::String(StringStatsData::new_unknown()),
            _ => StatsData::Base,
        };

        let child_stats = match &data_type {
            LogicalType::List(child_type) => Some(vec![BaseStatistics::create_unknown(
                child_type.as_ref().clone(),
            )]),
            LogicalType::Array(child_type, _) => Some(vec![BaseStatistics::create_unknown(
                child_type.as_ref().clone(),
            )]),
            LogicalType::Struct(fields) => {
                let children: Vec<BaseStatistics> = fields
                    .iter()
                    .map(|(_, ty)| BaseStatistics::create_unknown(ty.clone()))
                    .collect();
                Some(children)
            }
            _ => None,
        };

        Self {
            data_type,
            has_null: true,
            has_no_null: true,
            distinct_count: 0,
            stats_data,
            child_stats,
        }
    }

    /// Creates statistics for an empty database.
    /// "has_null" is false, "has_no_null" is false.
    /// This is used when incrementally constructing statistics.
    pub fn create_empty(data_type: LogicalType) -> Self {
        let stats_type = StatisticsType::from_logical_type(&data_type);
        let stats_data = match stats_type {
            StatisticsType::NumericStats => StatsData::Numeric(NumericStatsData::new_empty()),
            StatisticsType::StringStats => StatsData::String(StringStatsData::new_empty()),
            _ => StatsData::Base,
        };

        let child_stats = match &data_type {
            LogicalType::List(child_type) => Some(vec![BaseStatistics::create_empty(
                child_type.as_ref().clone(),
            )]),
            LogicalType::Array(child_type, _) => Some(vec![BaseStatistics::create_empty(
                child_type.as_ref().clone(),
            )]),
            LogicalType::Struct(fields) => {
                let children: Vec<BaseStatistics> = fields
                    .iter()
                    .map(|(_, ty)| BaseStatistics::create_empty(ty.clone()))
                    .collect();
                Some(children)
            }
            _ => None,
        };

        Self {
            data_type,
            has_null: false,
            has_no_null: false,
            distinct_count: 0,
            stats_data,
            child_stats,
        }
    }

    /// Get the statistics type for this statistics object.
    pub fn get_stats_type(&self) -> StatisticsType {
        StatisticsType::from_logical_type(&self.data_type)
    }

    /// Get the logical type.
    pub fn get_type(&self) -> &LogicalType {
        &self.data_type
    }

    /// Check if the data can have NULL values.
    pub fn can_have_null(&self) -> bool {
        self.has_null
    }

    /// Check if the data can have non-NULL values.
    pub fn can_have_no_null(&self) -> bool {
        self.has_no_null
    }

    /// Get the distinct count estimate.
    pub fn get_distinct_count(&self) -> usize {
        self.distinct_count
    }

    /// Set the distinct count estimate.
    pub fn set_distinct_count(&mut self, count: usize) {
        self.distinct_count = count;
    }

    /// Get the type-specific stats data.
    pub fn stats_data(&self) -> &StatsData {
        &self.stats_data
    }

    /// Get mutable reference to the type-specific stats data.
    pub fn stats_data_mut(&mut self) -> &mut StatsData {
        &mut self.stats_data
    }

    /// Get child stats for nested types.
    pub fn child_stats(&self) -> Option<&[BaseStatistics]> {
        self.child_stats.as_deref()
    }

    /// Get mutable child stats for nested types.
    pub fn child_stats_mut(&mut self) -> Option<&mut Vec<BaseStatistics>> {
        self.child_stats.as_mut()
    }

    /// Set a stats info flag.
    pub fn set(&mut self, info: StatsInfo) {
        match info {
            StatsInfo::CanHaveNullValues => self.set_has_null(),
            StatsInfo::CannotHaveNullValues => self.has_null = false,
            StatsInfo::CanHaveValidValues => self.set_has_no_null(),
            StatsInfo::CannotHaveValidValues => self.has_no_null = false,
            StatsInfo::CanHaveNullAndValidValues => {
                self.set_has_null();
                self.set_has_no_null();
            }
        }
    }

    /// Set that the data can have NULL values.
    /// For nested types, this propagates to children.
    pub fn set_has_null(&mut self) {
        self.has_null = true;
        // Propagate to struct children
        if let LogicalType::Struct(_) = &self.data_type {
            if let Some(children) = &mut self.child_stats {
                for child in children.iter_mut() {
                    child.set_has_null();
                }
            }
        }
    }

    /// Set that the data can have non-NULL values.
    /// For nested types, this propagates to children.
    pub fn set_has_no_null(&mut self) {
        self.has_no_null = true;
        // Propagate to struct children
        if let LogicalType::Struct(_) = &self.data_type {
            if let Some(children) = &mut self.child_stats {
                for child in children.iter_mut() {
                    child.set_has_no_null();
                }
            }
        }
    }

    /// Set has_null without propagating to children (fast path).
    #[inline]
    pub fn set_has_null_fast(&mut self) {
        self.has_null = true;
    }

    /// Set has_no_null without propagating to children (fast path).
    #[inline]
    pub fn set_has_no_null_fast(&mut self) {
        self.has_no_null = true;
    }

    /// Combine validity from two statistics.
    pub fn combine_validity(&mut self, left: &BaseStatistics, right: &BaseStatistics) {
        self.has_null = left.has_null || right.has_null;
        self.has_no_null = left.has_no_null || right.has_no_null;
    }

    /// Copy validity from another statistics.
    pub fn copy_validity(&mut self, other: &BaseStatistics) {
        self.has_null = other.has_null;
        self.has_no_null = other.has_no_null;
    }

    /// Check if the statistics represents a constant value.
    pub fn is_constant(&self) -> bool {
        match &self.stats_data {
            StatsData::Numeric(data) => data.is_constant(),
            StatsData::String(data) => data.is_constant(),
            StatsData::Base => false,
        }
    }

    /// Merge another statistics into this one.
    pub fn merge(&mut self, other: &BaseStatistics) {
        self.has_null = self.has_null || other.has_null;
        self.has_no_null = self.has_no_null || other.has_no_null;

        match (&mut self.stats_data, &other.stats_data) {
            (StatsData::Numeric(self_data), StatsData::Numeric(other_data)) => {
                self_data.merge(other_data);
            }
            (StatsData::String(self_data), StatsData::String(other_data)) => {
                self_data.merge(other_data);
            }
            _ => {}
        }

        // Merge child stats for nested types
        if let (Some(self_children), Some(other_children)) =
            (&mut self.child_stats, &other.child_stats)
        {
            for (self_child, other_child) in self_children.iter_mut().zip(other_children.iter()) {
                self_child.merge(other_child);
            }
        }
    }

    /// Copy from another statistics.
    pub fn copy_from(&mut self, other: &BaseStatistics) {
        self.has_null = other.has_null;
        self.has_no_null = other.has_no_null;
        self.distinct_count = other.distinct_count;
        self.stats_data = other.stats_data.clone();

        // Copy child stats
        if let Some(other_children) = &other.child_stats {
            let mut new_children = Vec::with_capacity(other_children.len());
            for other_child in other_children {
                let mut child = BaseStatistics::new(other_child.data_type.clone());
                child.copy_from(other_child);
                new_children.push(child);
            }
            self.child_stats = Some(new_children);
        }
    }

    /// Create a copy of this statistics.
    pub fn copy(&self) -> Self {
        let mut result = BaseStatistics::new(self.data_type.clone());
        result.copy_from(self);
        result
    }

    /// Fold one value into the statistics summary.
    pub fn observe_value(&mut self, value: &Value) {
        if value.is_null() {
            self.has_null = true;
            return;
        }

        self.has_no_null = true;

        match &mut self.stats_data {
            StatsData::Numeric(data) => {
                data.update(value);
            }
            StatsData::String(data) => {
                if let Value::Varchar(s) = value {
                    data.update(s);
                }
            }
            StatsData::Base => {}
        }
    }

    /// Return the minimum observed value when this statistics kind tracks one.
    pub fn min_value(&self) -> Option<Value> {
        match &self.stats_data {
            StatsData::Numeric(data) => data.min_value(&self.data_type),
            StatsData::String(data) => Some(Value::Varchar(data.min_string())),
            StatsData::Base => None,
        }
    }

    /// Return the maximum observed value when this statistics kind tracks one.
    pub fn max_value(&self) -> Option<Value> {
        match &self.stats_data {
            StatsData::Numeric(data) => data.max_value(&self.data_type),
            StatsData::String(data) => Some(Value::Varchar(data.max_string())),
            StatsData::Base => None,
        }
    }

    /// Serialize statistics into the current durable byte format.
    pub fn to_bytes(&self) -> paro_common::error::Result<Vec<u8>> {
        use std::io::Write;

        let mut buffer = Vec::new();

        // 1. Validity flags
        buffer
            .write_all(&[self.has_null as u8, self.has_no_null as u8])
            .map_err(|e| paro_error::internal(format!("Failed to write stats header: {}", e)))?;

        // 2. Distinct Count
        buffer.write_all(&(self.distinct_count as u64).to_le_bytes())?;

        // 3. Stats type
        let stats_type = self.get_stats_type() as u8;
        buffer.write_all(&[stats_type])?;

        // 4. Type-specific data
        match &self.stats_data {
            StatsData::Numeric(data) => {
                // Each bound carries its own presence byte. A second pair of
                // flags would duplicate state and permit invalid combinations.
                if let Some(min) = data.min_value(&self.data_type) {
                    Self::serialize_value(&Some(min), &mut buffer)?;
                } else {
                    Self::serialize_value(&None, &mut buffer)?;
                }
                if let Some(max) = data.max_value(&self.data_type) {
                    Self::serialize_value(&Some(max), &mut buffer)?;
                } else {
                    Self::serialize_value(&None, &mut buffer)?;
                }
            }
            StatsData::String(data) => {
                buffer.extend_from_slice(&data.serialize());
            }
            StatsData::Base => {}
        }

        Ok(buffer)
    }

    fn serialize_value(
        value: &Option<Value>,
        buffer: &mut Vec<u8>,
    ) -> paro_common::error::Result<()> {
        use std::io::Write;
        match value {
            Some(v) => {
                buffer.write_all(&[1])?;
                match v {
                    Value::Integer(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::BigInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::Boolean(b) => buffer.write_all(&[*b as u8])?,
                    Value::TinyInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::SmallInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::UTinyInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::USmallInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::UInteger(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::UBigInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::UHugeInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::HugeInt(i) => buffer.write_all(&i.to_le_bytes())?,
                    Value::Decimal(v, _, _) => buffer.write_all(&v.to_le_bytes())?,
                    Value::Float(f) => buffer.write_all(&f.to_le_bytes())?,
                    Value::Double(d) => buffer.write_all(&d.to_le_bytes())?,
                    Value::Date(days) => buffer.write_all(&days.to_le_bytes())?,
                    Value::Timestamp(micros) | Value::TimestampTz(micros) | Value::Time(micros) => {
                        buffer.write_all(&micros.to_le_bytes())?
                    }
                    _ => {
                        return Err(paro_error::not_implemented(
                            "Unsupported type for stats serialization",
                        ))
                    }
                }
            }
            None => buffer.write_all(&[0])?,
        }
        Ok(())
    }

    /// Deserialize statistics from the current durable byte format.
    pub fn from_bytes(bytes: &[u8], data_type: LogicalType) -> paro_common::error::Result<Self> {
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(bytes);
        let mut header = [0u8; 2];
        cursor
            .read_exact(&mut header)
            .map_err(|_| paro_error::internal("Invalid stats header"))?;

        let has_null = header[0] != 0;
        let has_no_null = header[1] != 0;

        // Distinct Count
        let mut dc_buf = [0u8; 8];
        cursor.read_exact(&mut dc_buf)?;
        let distinct_count = u64::from_le_bytes(dc_buf) as usize;

        // Stats type
        let mut st_buf = [0u8; 1];
        cursor.read_exact(&mut st_buf)?;

        let mut result = if has_no_null {
            BaseStatistics::new(data_type.clone())
        } else {
            BaseStatistics::create_empty(data_type.clone())
        };
        result.has_null = has_null;
        result.has_no_null = has_no_null;
        result.distinct_count = distinct_count;

        // Type-specific data
        match result.get_stats_type() {
            StatisticsType::NumericStats => {
                let minimum = Self::deserialize_value(&mut cursor, &data_type)?;
                let maximum = Self::deserialize_value(&mut cursor, &data_type)?;

                if let Some(minimum) = minimum {
                    NumericStats::set_guaranteed_min(&mut result, &minimum);
                }
                if let Some(maximum) = maximum {
                    NumericStats::set_guaranteed_max(&mut result, &maximum);
                }
            }
            StatisticsType::StringStats => {
                let remaining: Vec<u8> = cursor.bytes().filter_map(|b| b.ok()).collect();
                if let Some(string_data) = StringStatsData::deserialize(&remaining) {
                    result.stats_data = StatsData::String(string_data);
                }
            }
            _ => {}
        }

        Ok(result)
    }

    fn deserialize_value(
        cursor: &mut std::io::Cursor<&[u8]>,
        data_type: &LogicalType,
    ) -> paro_common::error::Result<Option<Value>> {
        use std::io::Read;
        let mut present = [0u8; 1];
        cursor.read_exact(&mut present).map_err(|_| {
            paro_error::data_corrupted("truncated numeric statistics bound presence tag")
        })?;

        match present[0] {
            0 => return Ok(None),
            1 => {}
            tag => {
                return Err(paro_error::data_corrupted(format!(
                    "invalid numeric statistics bound presence tag: {tag}"
                )))
            }
        }

        match data_type {
            LogicalType::Integer => {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Integer(i32::from_le_bytes(buf))))
            }
            LogicalType::BigInt => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::BigInt(i64::from_le_bytes(buf))))
            }
            LogicalType::Boolean => {
                let mut buf = [0u8; 1];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Boolean(buf[0] != 0)))
            }
            LogicalType::TinyInt => {
                let mut buf = [0u8; 1];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::TinyInt(i8::from_le_bytes(buf))))
            }
            LogicalType::UTinyInt => {
                let mut buf = [0u8; 1];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::UTinyInt(u8::from_le_bytes(buf))))
            }
            LogicalType::SmallInt => {
                let mut buf = [0u8; 2];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::SmallInt(i16::from_le_bytes(buf))))
            }
            LogicalType::USmallInt => {
                let mut buf = [0u8; 2];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::USmallInt(u16::from_le_bytes(buf))))
            }
            LogicalType::UInteger => {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::UInteger(u32::from_le_bytes(buf))))
            }
            LogicalType::Float => {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Float(f32::from_le_bytes(buf))))
            }
            LogicalType::Double => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Double(f64::from_le_bytes(buf))))
            }
            LogicalType::UBigInt => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::UBigInt(u64::from_le_bytes(buf))))
            }
            LogicalType::UHugeInt => {
                let mut buf = [0u8; 16];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::UHugeInt(u128::from_le_bytes(buf))))
            }
            LogicalType::HugeInt => {
                let mut buf = [0u8; 16];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::HugeInt(i128::from_le_bytes(buf))))
            }
            LogicalType::Decimal { precision, scale } => {
                let mut buf = [0u8; 16];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Decimal(
                    i128::from_le_bytes(buf),
                    *precision,
                    *scale,
                )))
            }
            LogicalType::Date => {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Date(i32::from_le_bytes(buf))))
            }
            LogicalType::Timestamp => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Timestamp(i64::from_le_bytes(buf))))
            }
            LogicalType::TimestampTz => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::TimestampTz(i64::from_le_bytes(buf))))
            }
            LogicalType::Time => {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                Ok(Some(Value::Time(i64::from_le_bytes(buf))))
            }
            _ => Err(paro_error::not_implemented(format!(
                "Unsupported type for stats deserialization: {:?}",
                data_type
            ))),
        }
    }
    /// Create statistics from a constant value.
    pub fn from_constant(value: &Value) -> Self {
        let data_type = value.logical_type();
        let stats_type = StatisticsType::from_logical_type(&data_type);

        let stats_data = if value.is_null() {
            match stats_type {
                StatisticsType::NumericStats => StatsData::Numeric(NumericStatsData::new_empty()),
                StatisticsType::StringStats => StatsData::String(StringStatsData::new_empty()),
                _ => StatsData::Base,
            }
        } else {
            match stats_type {
                StatisticsType::NumericStats => {
                    let mut data = NumericStatsData::new_empty();
                    data.update(value);
                    StatsData::Numeric(data)
                }
                StatisticsType::StringStats => {
                    let mut data = StringStatsData::new_empty();
                    if let Value::Varchar(s) = value {
                        data.update(s);
                    }
                    StatsData::String(data)
                }
                _ => StatsData::Base,
            }
        };

        // Handle child stats for nested types
        let child_stats = match &data_type {
            LogicalType::List(child_type) => {
                if value.is_null() {
                    Some(vec![BaseStatistics::create_empty(
                        child_type.as_ref().clone(),
                    )])
                } else if let Value::List(children, _) = value {
                    let mut child_stats = BaseStatistics::create_empty(child_type.as_ref().clone());
                    for child in children.iter() {
                        child_stats.merge(&BaseStatistics::from_constant(child));
                    }
                    Some(vec![child_stats])
                } else {
                    Some(vec![BaseStatistics::create_empty(
                        child_type.as_ref().clone(),
                    )])
                }
            }
            LogicalType::Struct(fields) => {
                // For struct, create child stats for each field
                let children: Vec<BaseStatistics> = fields
                    .iter()
                    .map(|(_, ty)| BaseStatistics::create_empty(ty.clone()))
                    .collect();
                Some(children)
            }
            _ => None,
        };

        let mut result = Self {
            data_type,
            has_null: false,
            has_no_null: false,
            distinct_count: 1,
            stats_data,
            child_stats,
        };

        if value.is_null() {
            result.set(StatsInfo::CanHaveNullValues);
            result.set(StatsInfo::CannotHaveValidValues);
        } else {
            result.set(StatsInfo::CannotHaveNullValues);
            result.set(StatsInfo::CanHaveValidValues);
        }

        result
    }

    /// Convert to a string representation.
    pub fn to_display_string(&self) -> String {
        let has_n = if self.has_null { "true" } else { "false" };
        let has_n_n = if self.has_no_null { "true" } else { "false" };

        let base_info = format!("[Has Null: {}, Has No Null: {}]", has_n, has_n_n);
        let distinct_info = if self.distinct_count > 0 {
            format!("[Approx Unique: {}]", self.distinct_count)
        } else {
            String::new()
        };

        let type_info = match &self.stats_data {
            StatsData::Numeric(data) => {
                let min_str = data
                    .min_value(&self.data_type)
                    .map_or_else(|| "None".to_string(), |value| value.to_string());
                let max_str = data
                    .max_value(&self.data_type)
                    .map_or_else(|| "None".to_string(), |value| value.to_string());
                format!("[Min: {}, Max: {}]", min_str, max_str)
            }
            StatsData::String(data) => {
                format!(
                    "[Min: {}, Max: {}, Unicode: {}, MaxLen: {:?}]",
                    data.min_string(),
                    data.max_string(),
                    data.has_unicode,
                    data.max_string_length()
                )
            }
            StatsData::Base => String::new(),
        };

        format!("{}{}{}", type_info, base_info, distinct_info)
    }
}

impl std::fmt::Display for BaseStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_unknown_numeric() {
        let stats = BaseStatistics::create_unknown(LogicalType::Integer);
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::NumericStats);
        assert!(matches!(stats.stats_data(), StatsData::Numeric(_)));
    }

    #[test]
    fn test_create_unknown_string() {
        let stats = BaseStatistics::create_unknown(LogicalType::Varchar);
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::StringStats);
        assert!(matches!(stats.stats_data(), StatsData::String(_)));
    }

    #[test]
    fn test_create_empty_numeric() {
        let stats = BaseStatistics::create_empty(LogicalType::BigInt);
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::NumericStats);
    }

    #[test]
    fn test_create_empty_string() {
        let stats = BaseStatistics::create_empty(LogicalType::Varchar);
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::StringStats);
    }

    #[test]
    fn test_create_unknown_list() {
        let stats =
            BaseStatistics::create_unknown(LogicalType::List(Box::new(LogicalType::Integer)));
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::ListStats);
        assert!(stats.child_stats().is_some());
        assert_eq!(stats.child_stats().unwrap().len(), 1);
    }

    #[test]
    fn test_create_unknown_struct() {
        let fields = vec![
            ("a".to_string(), LogicalType::Integer),
            ("b".to_string(), LogicalType::Varchar),
        ];
        let stats = BaseStatistics::create_unknown(LogicalType::Struct(fields));
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_stats_type(), StatisticsType::StructStats);
        assert!(stats.child_stats().is_some());
        assert_eq!(stats.child_stats().unwrap().len(), 2);
    }

    #[test]
    fn test_set_stats_info() {
        let mut stats = BaseStatistics::create_empty(LogicalType::Integer);
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());

        stats.set(StatsInfo::CanHaveNullValues);
        assert!(stats.can_have_null());

        stats.set(StatsInfo::CanHaveValidValues);
        assert!(stats.can_have_no_null());

        stats.set(StatsInfo::CannotHaveNullValues);
        assert!(!stats.can_have_null());

        stats.set(StatsInfo::CannotHaveValidValues);
        assert!(!stats.can_have_no_null());
    }

    #[test]
    fn test_merge_numeric() {
        let mut stats1 = BaseStatistics::create_empty(LogicalType::Integer);
        let mut stats2 = BaseStatistics::create_empty(LogicalType::Integer);

        // Update stats1 with value 10
        if let StatsData::Numeric(data) = stats1.stats_data_mut() {
            data.update(&Value::Integer(10));
        }
        stats1.set(StatsInfo::CanHaveValidValues);

        // Update stats2 with value 20
        if let StatsData::Numeric(data) = stats2.stats_data_mut() {
            data.update(&Value::Integer(20));
        }
        stats2.set(StatsInfo::CanHaveValidValues);
        stats2.set(StatsInfo::CanHaveNullValues);

        stats1.merge(&stats2);

        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());

        if let StatsData::Numeric(data) = stats1.stats_data() {
            assert_eq!(
                data.min_value(&LogicalType::Integer),
                Some(Value::Integer(10))
            );
            assert_eq!(
                data.max_value(&LogicalType::Integer),
                Some(Value::Integer(20))
            );
        } else {
            panic!("Expected numeric stats");
        }
    }

    #[test]
    fn test_numeric_partial_bound_round_trip_keeps_slot_alignment() {
        let mut stats = BaseStatistics::create_unknown(LogicalType::Integer);
        NumericStats::set_guaranteed_max(&mut stats, &Value::Integer(42));

        let bytes = stats.to_bytes().expect("statistics should serialize");
        let restored = BaseStatistics::from_bytes(&bytes, LogicalType::Integer)
            .expect("statistics should deserialize");
        let StatsData::Numeric(data) = restored.stats_data() else {
            panic!("expected numeric statistics");
        };
        assert_eq!(data.min_value(&LogicalType::Integer), None);
        assert_eq!(
            data.max_value(&LogicalType::Integer),
            Some(Value::Integer(42))
        );
    }

    #[test]
    fn test_temporal_bounds_round_trip_with_logical_value_types() {
        for value in [
            Value::Date(-7),
            Value::Timestamp(42),
            Value::TimestampTz(i64::MAX),
            Value::Time(123),
        ] {
            let stats = BaseStatistics::from_constant(&value);
            let bytes = stats.to_bytes().expect("statistics should serialize");
            let restored = BaseStatistics::from_bytes(&bytes, value.logical_type())
                .expect("statistics should deserialize");
            assert_eq!(restored.min_value(), Some(value.clone()));
            assert_eq!(restored.max_value(), Some(value));
        }
    }

    #[test]
    fn test_numeric_bound_deserialization_rejects_invalid_presence_tags() {
        const NUMERIC_PAYLOAD_OFFSET: usize = 2 + 8 + 1;

        let stats = BaseStatistics::from_constant(&Value::Integer(42));
        let mut bytes = stats.to_bytes().expect("statistics should serialize");
        bytes[NUMERIC_PAYLOAD_OFFSET] = 2;
        assert!(BaseStatistics::from_bytes(&bytes, LogicalType::Integer).is_err());

        let truncated =
            &stats.to_bytes().expect("statistics should serialize")[..NUMERIC_PAYLOAD_OFFSET];
        assert!(BaseStatistics::from_bytes(truncated, LogicalType::Integer).is_err());
    }

    #[test]
    fn test_merge_string() {
        let mut stats1 = BaseStatistics::create_empty(LogicalType::Varchar);
        let mut stats2 = BaseStatistics::create_empty(LogicalType::Varchar);

        if let StatsData::String(data) = stats1.stats_data_mut() {
            data.update("apple");
        }

        if let StatsData::String(data) = stats2.stats_data_mut() {
            data.update("zebra");
        }

        stats1.merge(&stats2);

        if let StatsData::String(data) = stats1.stats_data() {
            assert_eq!(data.min_string(), "apple");
            assert_eq!(data.max_string(), "zebra");
        } else {
            panic!("Expected string stats");
        }
    }

    #[test]
    fn test_from_constant_integer() {
        let stats = BaseStatistics::from_constant(&Value::Integer(42));
        assert!(!stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.get_distinct_count(), 1);

        if let StatsData::Numeric(data) = stats.stats_data() {
            assert_eq!(
                data.min_value(&LogicalType::Integer),
                Some(Value::Integer(42))
            );
            assert_eq!(
                data.max_value(&LogicalType::Integer),
                Some(Value::Integer(42))
            );
        } else {
            panic!("Expected numeric stats");
        }
    }

    #[test]
    fn test_from_constant_null() {
        let stats = BaseStatistics::from_constant(&Value::Null(LogicalType::Integer));
        assert!(stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.get_distinct_count(), 1);
    }

    #[test]
    fn test_from_constant_string() {
        let stats = BaseStatistics::from_constant(&Value::Varchar("hello".to_string()));
        assert!(!stats.can_have_null());
        assert!(stats.can_have_no_null());

        if let StatsData::String(data) = stats.stats_data() {
            assert_eq!(data.min_string(), "hello");
            assert_eq!(data.max_string(), "hello");
        } else {
            panic!("Expected string stats");
        }
    }

    #[test]
    fn test_copy() {
        let mut stats = BaseStatistics::create_empty(LogicalType::Integer);
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update(&Value::Integer(100));
        }
        stats.set(StatsInfo::CanHaveValidValues);
        stats.set_distinct_count(50);

        let copied = stats.copy();
        assert_eq!(copied.can_have_null(), stats.can_have_null());
        assert_eq!(copied.can_have_no_null(), stats.can_have_no_null());
        assert_eq!(copied.get_distinct_count(), 50);

        if let (StatsData::Numeric(orig), StatsData::Numeric(copy)) =
            (stats.stats_data(), copied.stats_data())
        {
            assert_eq!(
                orig.min_value(&LogicalType::Integer),
                copy.min_value(&LogicalType::Integer)
            );
            assert_eq!(
                orig.max_value(&LogicalType::Integer),
                copy.max_value(&LogicalType::Integer)
            );
        }
    }

    #[test]
    fn test_is_constant() {
        let mut stats = BaseStatistics::create_empty(LogicalType::Integer);
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update(&Value::Integer(42));
        }
        assert!(stats.is_constant());

        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update(&Value::Integer(43));
        }
        assert!(!stats.is_constant());
    }

    #[test]
    fn test_combine_validity() {
        let mut stats = BaseStatistics::create_empty(LogicalType::Integer);
        let left = BaseStatistics::create_unknown(LogicalType::Integer);
        let mut right = BaseStatistics::create_empty(LogicalType::Integer);
        right.set(StatsInfo::CanHaveValidValues);

        stats.combine_validity(&left, &right);
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
    }

    #[test]
    fn test_to_string() {
        let mut stats = BaseStatistics::create_empty(LogicalType::Integer);
        if let StatsData::Numeric(data) = stats.stats_data_mut() {
            data.update(&Value::Integer(10));
            data.update(&Value::Integer(100));
        }
        stats.set(StatsInfo::CanHaveValidValues);
        stats.set_distinct_count(50);

        let s = stats.to_string();
        assert!(s.contains("Has Null: false"));
        assert!(s.contains("Has No Null: true"));
        assert!(s.contains("Approx Unique: 50"));
    }

    #[test]
    fn test_nested_list_stats() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = BaseStatistics::create_empty(list_type);

        assert!(stats.child_stats().is_some());
        let children = stats.child_stats().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_stats_type(), StatisticsType::NumericStats);
    }

    #[test]
    fn test_nested_struct_stats() {
        let struct_type = LogicalType::Struct(vec![
            ("id".to_string(), LogicalType::Integer),
            ("name".to_string(), LogicalType::Varchar),
        ]);
        let stats = BaseStatistics::create_empty(struct_type);

        assert!(stats.child_stats().is_some());
        let children = stats.child_stats().unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].get_stats_type(), StatisticsType::NumericStats);
        assert_eq!(children[1].get_stats_type(), StatisticsType::StringStats);
    }
}
