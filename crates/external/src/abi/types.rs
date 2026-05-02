// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbiLogicalType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    HugeInt,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UHugeInt,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Varchar,
    Blob,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
    Array {
        element: Box<AbiLogicalType>,
        length: u32,
    },
    List(Box<AbiLogicalType>),
    Struct(Vec<AbiStructField>),
}

impl AbiLogicalType {
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            AbiLogicalType::Array { .. } | AbiLogicalType::List(_) | AbiLogicalType::Struct(_)
        )
    }

    pub fn is_varlen(&self) -> bool {
        matches!(
            self,
            AbiLogicalType::Varchar
                | AbiLogicalType::Blob
                | AbiLogicalType::Json
                | AbiLogicalType::Jsonb
                | AbiLogicalType::List(_)
        )
    }

    pub fn fixed_width_bytes(&self) -> Option<u32> {
        match self {
            AbiLogicalType::Boolean | AbiLogicalType::Int8 | AbiLogicalType::UInt8 => Some(1),
            AbiLogicalType::Int16 | AbiLogicalType::UInt16 => Some(2),
            AbiLogicalType::Int32
            | AbiLogicalType::UInt32
            | AbiLogicalType::Date
            | AbiLogicalType::Float32 => Some(4),
            AbiLogicalType::Int64
            | AbiLogicalType::UInt64
            | AbiLogicalType::Time
            | AbiLogicalType::Timestamp
            | AbiLogicalType::TimestampTz
            | AbiLogicalType::Float64 => Some(8),
            AbiLogicalType::HugeInt
            | AbiLogicalType::UHugeInt
            | AbiLogicalType::Interval
            | AbiLogicalType::Uuid => Some(16),
            AbiLogicalType::Decimal { precision, .. } => {
                Some(if *precision <= 18 { 8 } else { 16 })
            }
            AbiLogicalType::Array { element, length } => {
                element.fixed_width_bytes().map(|width| width * *length)
            }
            AbiLogicalType::Varchar
            | AbiLogicalType::Blob
            | AbiLogicalType::Json
            | AbiLogicalType::Jsonb
            | AbiLogicalType::List(_)
            | AbiLogicalType::Struct(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiStructField {
    pub name: String,
    pub data_type: AbiLogicalType,
    pub nullable: bool,
}
