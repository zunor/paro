// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use super::descriptor::ColumnDescriptor;
use super::types::AbiLogicalType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffsetWidth {
    U32,
    U64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BufferDevice {
    Host,
    UnifiedMemory { device_id: u16 },
    Cuda { device_id: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferLease {
    pub buffer_index: u16,
    pub offset: u64,
    pub len: u64,
    pub alignment: u32,
    pub generation: u64,
    pub device: BufferDevice,
}

impl BufferLease {
    pub fn host(buffer_index: u16, offset: u64, len: u64, alignment: u32) -> Self {
        Self {
            buffer_index,
            offset,
            len,
            alignment,
            generation: 0,
            device: BufferDevice::Host,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarValueRef {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    HugeInt(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UHugeInt(u128),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    Decimal {
        value: i128,
        precision: u8,
        scale: u8,
    },
    Utf8(String),
    Binary(Vec<u8>),
    Date(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    TimestampTzMicros(i64),
    IntervalMicros(i128),
    Uuid([u8; 16]),
    Json(String),
    Jsonb(Vec<u8>),
}

impl ScalarValueRef {
    pub fn logical_type(&self) -> Option<AbiLogicalType> {
        match self {
            ScalarValueRef::Null => None,
            ScalarValueRef::Boolean(_) => Some(AbiLogicalType::Boolean),
            ScalarValueRef::Int8(_) => Some(AbiLogicalType::Int8),
            ScalarValueRef::Int16(_) => Some(AbiLogicalType::Int16),
            ScalarValueRef::Int32(_) => Some(AbiLogicalType::Int32),
            ScalarValueRef::Int64(_) => Some(AbiLogicalType::Int64),
            ScalarValueRef::HugeInt(_) => Some(AbiLogicalType::HugeInt),
            ScalarValueRef::UInt8(_) => Some(AbiLogicalType::UInt8),
            ScalarValueRef::UInt16(_) => Some(AbiLogicalType::UInt16),
            ScalarValueRef::UInt32(_) => Some(AbiLogicalType::UInt32),
            ScalarValueRef::UInt64(_) => Some(AbiLogicalType::UInt64),
            ScalarValueRef::UHugeInt(_) => Some(AbiLogicalType::UHugeInt),
            ScalarValueRef::Float32(_) => Some(AbiLogicalType::Float32),
            ScalarValueRef::Float64(_) => Some(AbiLogicalType::Float64),
            ScalarValueRef::Decimal {
                precision, scale, ..
            } => Some(AbiLogicalType::Decimal {
                precision: *precision,
                scale: *scale,
            }),
            ScalarValueRef::Utf8(_) => Some(AbiLogicalType::Varchar),
            ScalarValueRef::Binary(_) => Some(AbiLogicalType::Blob),
            ScalarValueRef::Date(_) => Some(AbiLogicalType::Date),
            ScalarValueRef::TimeMicros(_) => Some(AbiLogicalType::Time),
            ScalarValueRef::TimestampMicros(_) => Some(AbiLogicalType::Timestamp),
            ScalarValueRef::TimestampTzMicros(_) => Some(AbiLogicalType::TimestampTz),
            ScalarValueRef::IntervalMicros(_) => Some(AbiLogicalType::Interval),
            ScalarValueRef::Uuid(_) => Some(AbiLogicalType::Uuid),
            ScalarValueRef::Json(_) => Some(AbiLogicalType::Json),
            ScalarValueRef::Jsonb(_) => Some(AbiLogicalType::Jsonb),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnLayout {
    FixedWidth {
        values: BufferLease,
        stride: u32,
    },
    VarLen {
        offsets: BufferLease,
        data: BufferLease,
        offset_width: OffsetWidth,
    },
    List {
        offsets: BufferLease,
        offset_width: OffsetWidth,
    },
    Struct,
    Dictionary {
        indices: BufferLease,
        dictionary: Box<ColumnDescriptor>,
    },
    Sequence {
        start: i64,
        step: i64,
    },
    Constant {
        value: ScalarValueRef,
    },
}

impl ColumnLayout {
    pub fn buffer_leases(&self) -> Vec<&BufferLease> {
        match self {
            ColumnLayout::FixedWidth { values, .. } => vec![values],
            ColumnLayout::VarLen { offsets, data, .. } => vec![offsets, data],
            ColumnLayout::List { offsets, .. } => vec![offsets],
            ColumnLayout::Struct
            | ColumnLayout::Sequence { .. }
            | ColumnLayout::Constant { .. } => Vec::new(),
            ColumnLayout::Dictionary { indices, .. } => vec![indices],
        }
    }
}
