// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{LogicalType, StringView};

/// Physical representation for logical types.
///
/// Maps to the underlying storage representation of data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PhysicalType {
    /// Boolean (1 byte)
    Bool = 1,
    /// 8-bit signed integer
    Int8 = 3,
    /// 16-bit signed integer
    Int16 = 5,
    /// 32-bit signed integer
    Int32 = 7,
    /// 64-bit signed integer
    Int64 = 9,
    /// 128-bit signed integer
    Int128 = 204,
    /// 8-bit unsigned integer
    UInt8 = 2,
    /// 16-bit unsigned integer
    UInt16 = 4,
    /// 32-bit unsigned integer
    UInt32 = 6,
    /// 64-bit unsigned integer
    UInt64 = 8,
    /// 128-bit unsigned integer
    UInt128 = 203,
    /// 32-bit floating point
    Float = 11,
    /// 64-bit floating point
    Double = 12,
    /// Variable-length string (StringView/string_t)
    Varchar = 200,
    /// Validity mask (bit array)
    Bit = 206,
    /// List (offset + length)
    List = 23,
    /// Struct (child vectors)
    Struct = 24,
    /// Fixed-size array (child vector)
    Array = 29,
}

impl PhysicalType {
    /// Returns the physical size of this type in bytes.
    pub fn size(&self) -> usize {
        match self {
            PhysicalType::Bool | PhysicalType::Int8 | PhysicalType::UInt8 => 1,
            PhysicalType::Int16 | PhysicalType::UInt16 => 2,
            PhysicalType::Int32 | PhysicalType::UInt32 | PhysicalType::Float => 4,
            PhysicalType::Int64 | PhysicalType::UInt64 | PhysicalType::Double => 8,
            PhysicalType::Int128 | PhysicalType::UInt128 => 16,
            PhysicalType::Varchar => StringView::SIZE,
            PhysicalType::List => 8, // list_entry_t size (u32 offset + u32 length)
            PhysicalType::Struct | PhysicalType::Array => 0,
            PhysicalType::Bit => 1,
        }
    }

    /// Check if this is a fixed-width numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Int128
                | PhysicalType::UInt8
                | PhysicalType::UInt16
                | PhysicalType::UInt32
                | PhysicalType::UInt64
                | PhysicalType::UInt128
                | PhysicalType::Float
                | PhysicalType::Double
        )
    }

    /// Check if this is an integer type.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Int128
                | PhysicalType::UInt8
                | PhysicalType::UInt16
                | PhysicalType::UInt32
                | PhysicalType::UInt64
                | PhysicalType::UInt128
        )
    }

    /// Get the physical type for a logical type.
    pub fn from_logical(logical: &LogicalType) -> Self {
        logical.physical_type()
    }
}
