// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # ARTKey - Radix-encoded keys for ART index
//!
//! ## Design
//! - Keys are radix-encoded for byte-by-byte comparison
//! - Signed integers: big-endian + sign bit flip (negative < positive)
//! - Unsigned integers: big-endian
//! - Floats: IEEE 754 transformation for correct ordering
//! - Strings: escape \x00 and \x01, null-terminated
//!
//! ## Key Encoding Rules
//! - All keys are encoded to preserve lexicographic ordering
//! - Signed integers flip the sign bit so negatives sort before positives
//! - Floats are transformed to preserve ordering (-inf < -x < -0 < +0 < +x < +inf < NaN)

use std::cmp::Ordering;
use std::ptr;

use paro_common::allocator::ArenaAllocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// Maximum key length for ART (8KB * prefix_count).
pub const MAX_KEY_LEN: usize = 8192;

/// Radix encoding utilities for ART keys.
///
/// Provides encoding/decoding functions that transform values into
/// byte sequences that preserve lexicographic ordering.
pub struct Radix;

impl Radix {
    /// Flip the sign bit of a byte (XOR with 0x80).
    #[inline]
    pub fn flip_sign(byte: u8) -> u8 {
        byte ^ 0x80
    }

    // ========== Encode Functions ==========

    /// Encode a boolean value.
    #[inline]
    pub fn encode_bool(data: &mut [u8], value: bool) {
        data[0] = if value { 1 } else { 0 };
    }

    /// Encode an i8 value (sign bit flip + big-endian).
    #[inline]
    pub fn encode_i8(data: &mut [u8], value: i8) {
        data[0] = Self::flip_sign(value as u8);
    }

    /// Encode an i16 value (sign bit flip + big-endian).
    #[inline]
    pub fn encode_i16(data: &mut [u8], value: i16) {
        let bytes = value.to_be_bytes();
        data[0] = Self::flip_sign(bytes[0]);
        data[1] = bytes[1];
    }

    /// Encode an i32 value (sign bit flip + big-endian).
    #[inline]
    pub fn encode_i32(data: &mut [u8], value: i32) {
        let bytes = value.to_be_bytes();
        data[0] = Self::flip_sign(bytes[0]);
        data[1..4].copy_from_slice(&bytes[1..4]);
    }

    /// Encode an i64 value (sign bit flip + big-endian).
    #[inline]
    pub fn encode_i64(data: &mut [u8], value: i64) {
        let bytes = value.to_be_bytes();
        data[0] = Self::flip_sign(bytes[0]);
        data[1..8].copy_from_slice(&bytes[1..8]);
    }

    /// Encode an i128 value (sign bit flip + big-endian).
    #[inline]
    pub fn encode_i128(data: &mut [u8], value: i128) {
        let bytes = value.to_be_bytes();
        data[0] = Self::flip_sign(bytes[0]);
        data[1..16].copy_from_slice(&bytes[1..16]);
    }

    /// Encode a u8 value.
    #[inline]
    pub fn encode_u8(data: &mut [u8], value: u8) {
        data[0] = value;
    }

    /// Encode a u16 value (big-endian).
    #[inline]
    pub fn encode_u16(data: &mut [u8], value: u16) {
        let bytes = value.to_be_bytes();
        data[..2].copy_from_slice(&bytes);
    }

    /// Encode a u32 value (big-endian).
    #[inline]
    pub fn encode_u32(data: &mut [u8], value: u32) {
        let bytes = value.to_be_bytes();
        data[..4].copy_from_slice(&bytes);
    }

    /// Encode a u64 value (big-endian).
    #[inline]
    pub fn encode_u64(data: &mut [u8], value: u64) {
        let bytes = value.to_be_bytes();
        data[..8].copy_from_slice(&bytes);
    }

    /// Encode a u128 value (big-endian).
    #[inline]
    pub fn encode_u128(data: &mut [u8], value: u128) {
        let bytes = value.to_be_bytes();
        data[..16].copy_from_slice(&bytes);
    }

    /// Encode a f32 value for correct ordering.
    ///
    /// Transformation ensures: -inf < -x < -0 < +0 < +x < +inf < NaN
    #[inline]
    pub fn encode_f32(data: &mut [u8], value: f32) {
        let encoded = Self::encode_float_bits(value);
        let bytes = encoded.to_be_bytes();
        data[..4].copy_from_slice(&bytes);
    }

    /// Encode a f64 value for correct ordering.
    ///
    /// Transformation ensures: -inf < -x < -0 < +0 < +x < +inf < NaN
    #[inline]
    pub fn encode_f64(data: &mut [u8], value: f64) {
        let encoded = Self::encode_double_bits(value);
        let bytes = encoded.to_be_bytes();
        data[..8].copy_from_slice(&bytes);
    }

    /// Encode float bits for correct ordering.
    ///
    /// - Zero: set sign bit (0x80000000)
    /// - NaN: UINT_MAX
    /// - +Infinity: UINT_MAX - 1
    /// - -Infinity: 0
    /// - Positive: set sign bit
    /// - Negative: complement all bits
    fn encode_float_bits(value: f32) -> u32 {
        // Handle zero
        if value == 0.0 {
            return 1u32 << 31;
        }
        // Handle NaN
        if value.is_nan() {
            return u32::MAX;
        }
        // Handle +infinity
        if value == f32::INFINITY {
            return u32::MAX - 1;
        }
        // Handle -infinity
        if value == f32::NEG_INFINITY {
            return 0;
        }

        let bits = value.to_bits();
        if (bits & (1u32 << 31)) == 0 {
            // Positive: set sign bit
            bits | (1u32 << 31)
        } else {
            // Negative: complement all bits
            !bits
        }
    }

    /// Encode double bits for correct ordering.
    fn encode_double_bits(value: f64) -> u64 {
        // Handle zero
        if value == 0.0 {
            return 1u64 << 63;
        }
        // Handle NaN
        if value.is_nan() {
            return u64::MAX;
        }
        // Handle +infinity
        if value == f64::INFINITY {
            return u64::MAX - 1;
        }
        // Handle -infinity
        if value == f64::NEG_INFINITY {
            return 0;
        }

        let bits = value.to_bits();
        if bits < (1u64 << 63) {
            // Positive: add sign bit
            bits + (1u64 << 63)
        } else {
            // Negative: complement all bits
            !bits
        }
    }

    // ========== Decode Functions ==========

    /// Decode a boolean value.
    #[inline]
    pub fn decode_bool(data: &[u8]) -> bool {
        data[0] != 0
    }

    /// Decode an i8 value.
    #[inline]
    pub fn decode_i8(data: &[u8]) -> i8 {
        Self::flip_sign(data[0]) as i8
    }

    /// Decode an i16 value.
    #[inline]
    pub fn decode_i16(data: &[u8]) -> i16 {
        let mut bytes = [0u8; 2];
        bytes[0] = Self::flip_sign(data[0]);
        bytes[1] = data[1];
        i16::from_be_bytes(bytes)
    }

    /// Decode an i32 value.
    #[inline]
    pub fn decode_i32(data: &[u8]) -> i32 {
        let mut bytes = [0u8; 4];
        bytes[0] = Self::flip_sign(data[0]);
        bytes[1..4].copy_from_slice(&data[1..4]);
        i32::from_be_bytes(bytes)
    }

    /// Decode an i64 value.
    #[inline]
    pub fn decode_i64(data: &[u8]) -> i64 {
        let mut bytes = [0u8; 8];
        bytes[0] = Self::flip_sign(data[0]);
        bytes[1..8].copy_from_slice(&data[1..8]);
        i64::from_be_bytes(bytes)
    }

    /// Decode an i128 value.
    #[inline]
    pub fn decode_i128(data: &[u8]) -> i128 {
        let mut bytes = [0u8; 16];
        bytes[0] = Self::flip_sign(data[0]);
        bytes[1..16].copy_from_slice(&data[1..16]);
        i128::from_be_bytes(bytes)
    }

    /// Decode a u8 value.
    #[inline]
    pub fn decode_u8(data: &[u8]) -> u8 {
        data[0]
    }

    /// Decode a u16 value.
    #[inline]
    pub fn decode_u16(data: &[u8]) -> u16 {
        u16::from_be_bytes([data[0], data[1]])
    }

    /// Decode a u32 value.
    #[inline]
    pub fn decode_u32(data: &[u8]) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&data[..4]);
        u32::from_be_bytes(bytes)
    }

    /// Decode a u64 value.
    #[inline]
    pub fn decode_u64(data: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[..8]);
        u64::from_be_bytes(bytes)
    }

    /// Decode a u128 value.
    #[inline]
    pub fn decode_u128(data: &[u8]) -> u128 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&data[..16]);
        u128::from_be_bytes(bytes)
    }

    /// Decode a f32 value.
    #[inline]
    pub fn decode_f32(data: &[u8]) -> f32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&data[..4]);
        let encoded = u32::from_be_bytes(bytes);
        Self::decode_float_bits(encoded)
    }

    /// Decode a f64 value.
    #[inline]
    pub fn decode_f64(data: &[u8]) -> f64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[..8]);
        let encoded = u64::from_be_bytes(bytes);
        Self::decode_double_bits(encoded)
    }

    /// Decode float bits back to f32.
    fn decode_float_bits(input: u32) -> f32 {
        // NaN
        if input == u32::MAX {
            return f32::NAN;
        }
        // +Infinity
        if input == u32::MAX - 1 {
            return f32::INFINITY;
        }
        // -Infinity
        if input == 0 {
            return f32::NEG_INFINITY;
        }

        let bits = if input & (1u32 << 31) != 0 {
            // Positive: flip sign bit
            input ^ (1u32 << 31)
        } else {
            // Negative: invert all bits
            !input
        };
        f32::from_bits(bits)
    }

    /// Decode double bits back to f64.
    fn decode_double_bits(input: u64) -> f64 {
        // NaN
        if input == u64::MAX {
            return f64::NAN;
        }
        // +Infinity
        if input == u64::MAX - 1 {
            return f64::INFINITY;
        }
        // -Infinity
        if input == 0 {
            return f64::NEG_INFINITY;
        }

        let bits = if input & (1u64 << 63) != 0 {
            // Positive: flip sign bit
            input ^ (1u64 << 63)
        } else {
            // Negative: invert all bits
            !input
        };
        f64::from_bits(bits)
    }
}

/// A radix-encoded key for ART index.
///
/// Keys are encoded to preserve lexicographic ordering when compared byte-by-byte.
/// The key data is allocated from an ArenaAllocator and is valid as long as the
/// arena is not reset or destroyed.
///
/// # String Encoding
/// Strings are escaped: \x00 and \x01 are prefixed with \x01, then null-terminated.
/// This ensures correct ordering and allows embedded nulls.
#[derive(Clone, Copy)]
pub struct ARTKey {
    /// Length of the key in bytes.
    pub len: usize,
    /// Pointer to the key data (owned by ArenaAllocator).
    pub data: *mut u8,
}

impl Default for ARTKey {
    fn default() -> Self {
        Self::empty()
    }
}

impl ARTKey {
    /// Create an empty key.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            len: 0,
            data: ptr::null_mut(),
        }
    }

    /// Create a key from existing data.
    #[inline]
    pub const fn new(data: *mut u8, len: usize) -> Self {
        Self { len, data }
    }

    /// Allocate a key with the given length from the arena.
    pub fn with_len(allocator: &mut ArenaAllocator, len: usize) -> Result<Self> {
        if len == 0 {
            return Ok(Self::empty());
        }
        let data = allocator.allocate(len)?;
        Ok(Self { len, data })
    }

    /// Check if the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get a byte at the given index.
    ///
    /// # Safety
    /// The index must be less than the key length.
    #[inline]
    pub fn get(&self, index: usize) -> u8 {
        debug_assert!(index < self.len, "Index out of bounds");
        // SAFETY: index is checked to be within bounds
        unsafe { *self.data.add(index) }
    }

    /// Get a byte at the given index (alias for `get`).
    #[inline]
    pub fn get_byte(&self, index: usize) -> u8 {
        self.get(index)
    }

    /// Set a byte at the given index.
    ///
    /// # Safety
    /// The index must be less than the key length.
    #[inline]
    pub fn set(&mut self, index: usize, value: u8) {
        debug_assert!(index < self.len, "Index out of bounds");
        // SAFETY: index is checked to be within bounds
        unsafe { *self.data.add(index) = value };
    }

    /// Check if the byte at the given depth matches.
    #[inline]
    pub fn byte_matches(&self, other: &ARTKey, depth: usize) -> bool {
        self.get(depth) == other.get(depth)
    }

    /// Get the key data as a slice.
    ///
    /// # Safety
    /// The key must have valid data pointer.
    pub fn as_slice(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            return &[];
        }
        // SAFETY: data is valid and len is correct
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    /// Get the key data as a mutable slice.
    ///
    /// # Safety
    /// The key must have valid data pointer.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.data.is_null() || self.len == 0 {
            return &mut [];
        }
        // SAFETY: data is valid and len is correct
        unsafe { std::slice::from_raw_parts_mut(self.data, self.len) }
    }

    // ========== Key Creation Functions ==========

    /// Create a key from a boolean value.
    pub fn from_bool(allocator: &mut ArenaAllocator, value: bool) -> Result<Self> {
        let mut key = Self::with_len(allocator, 1)?;
        Radix::encode_bool(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from an i8 value.
    pub fn from_i8(allocator: &mut ArenaAllocator, value: i8) -> Result<Self> {
        let mut key = Self::with_len(allocator, 1)?;
        Radix::encode_i8(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from an i16 value.
    pub fn from_i16(allocator: &mut ArenaAllocator, value: i16) -> Result<Self> {
        let mut key = Self::with_len(allocator, 2)?;
        Radix::encode_i16(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from an i32 value.
    pub fn from_i32(allocator: &mut ArenaAllocator, value: i32) -> Result<Self> {
        let mut key = Self::with_len(allocator, 4)?;
        Radix::encode_i32(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from an i64 value.
    pub fn from_i64(allocator: &mut ArenaAllocator, value: i64) -> Result<Self> {
        let mut key = Self::with_len(allocator, 8)?;
        Radix::encode_i64(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a row ID.
    ///
    /// Row IDs are stored as i64 values.
    pub fn from_row_id(allocator: &mut ArenaAllocator, row_id: i64) -> Self {
        Self::from_i64(allocator, row_id).expect("Failed to create row ID key")
    }

    /// Create a key from an i128 value.
    pub fn from_i128(allocator: &mut ArenaAllocator, value: i128) -> Result<Self> {
        let mut key = Self::with_len(allocator, 16)?;
        Radix::encode_i128(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a u8 value.
    pub fn from_u8(allocator: &mut ArenaAllocator, value: u8) -> Result<Self> {
        let mut key = Self::with_len(allocator, 1)?;
        Radix::encode_u8(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a u16 value.
    pub fn from_u16(allocator: &mut ArenaAllocator, value: u16) -> Result<Self> {
        let mut key = Self::with_len(allocator, 2)?;
        Radix::encode_u16(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a u32 value.
    pub fn from_u32(allocator: &mut ArenaAllocator, value: u32) -> Result<Self> {
        let mut key = Self::with_len(allocator, 4)?;
        Radix::encode_u32(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a u64 value.
    pub fn from_u64(allocator: &mut ArenaAllocator, value: u64) -> Result<Self> {
        let mut key = Self::with_len(allocator, 8)?;
        Radix::encode_u64(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a u128 value.
    pub fn from_u128(allocator: &mut ArenaAllocator, value: u128) -> Result<Self> {
        let mut key = Self::with_len(allocator, 16)?;
        Radix::encode_u128(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a f32 value.
    pub fn from_f32(allocator: &mut ArenaAllocator, value: f32) -> Result<Self> {
        let mut key = Self::with_len(allocator, 4)?;
        Radix::encode_f32(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a f64 value.
    pub fn from_f64(allocator: &mut ArenaAllocator, value: f64) -> Result<Self> {
        let mut key = Self::with_len(allocator, 8)?;
        Radix::encode_f64(key.as_mut_slice(), value);
        Ok(key)
    }

    /// Create a key from a string value.
    ///
    /// Strings are escaped: \x00 and \x01 are prefixed with \x01, then null-terminated.
    pub fn from_str(allocator: &mut ArenaAllocator, value: &str) -> Result<Self> {
        Self::from_bytes(allocator, value.as_bytes())
    }

    /// Create a key from a byte slice.
    ///
    /// Bytes are escaped: \x00 and \x01 are prefixed with \x01, then null-terminated.
    pub fn from_bytes(allocator: &mut ArenaAllocator, value: &[u8]) -> Result<Self> {
        // Count escape characters needed
        let mut escape_count = 0;
        for &byte in value {
            if byte <= 1 {
                escape_count += 1;
            }
        }

        // Allocate: original length + escapes + null terminator
        let key_len = value.len() + escape_count + 1;
        let mut key = Self::with_len(allocator, key_len)?;
        let data = key.as_mut_slice();

        // Copy with escaping
        let mut pos = 0;
        for &byte in value {
            if byte <= 1 {
                data[pos] = 0x01; // Escape prefix
                pos += 1;
            }
            data[pos] = byte;
            pos += 1;
        }

        // Null terminator
        data[pos] = 0x00;

        Ok(key)
    }

    /// Create a key from a LogicalType and raw value bytes.
    ///
    /// This is the main entry point for creating keys from typed values.
    pub fn create_key(
        allocator: &mut ArenaAllocator,
        logical_type: &LogicalType,
        value: &[u8],
    ) -> Result<Self> {
        match logical_type {
            LogicalType::Boolean => {
                let v = value[0] != 0;
                Self::from_bool(allocator, v)
            }
            LogicalType::TinyInt => {
                let v = value[0] as i8;
                Self::from_i8(allocator, v)
            }
            LogicalType::SmallInt => {
                let v = i16::from_le_bytes([value[0], value[1]]);
                Self::from_i16(allocator, v)
            }
            LogicalType::Integer => {
                let v = i32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                Self::from_i32(allocator, v)
            }
            LogicalType::BigInt => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&value[..8]);
                let v = i64::from_le_bytes(bytes);
                Self::from_i64(allocator, v)
            }
            LogicalType::HugeInt => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&value[..16]);
                let v = i128::from_le_bytes(bytes);
                Self::from_i128(allocator, v)
            }
            LogicalType::UTinyInt => Self::from_u8(allocator, value[0]),
            LogicalType::USmallInt => {
                let v = u16::from_le_bytes([value[0], value[1]]);
                Self::from_u16(allocator, v)
            }
            LogicalType::UInteger => {
                let v = u32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                Self::from_u32(allocator, v)
            }
            LogicalType::UBigInt => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&value[..8]);
                let v = u64::from_le_bytes(bytes);
                Self::from_u64(allocator, v)
            }
            LogicalType::UHugeInt => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&value[..16]);
                let v = u128::from_le_bytes(bytes);
                Self::from_u128(allocator, v)
            }
            LogicalType::Uuid => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&value[..16]);
                let v = u128::from_le_bytes(bytes);
                Self::from_u128(allocator, v)
            }
            LogicalType::Float => {
                let v = f32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                Self::from_f32(allocator, v)
            }
            LogicalType::Double => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&value[..8]);
                let v = f64::from_le_bytes(bytes);
                Self::from_f64(allocator, v)
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Jsonb => Self::from_bytes(allocator, value),
            _ => Err(paro_error::internal(format!(
                "Unsupported type for ART key: {:?}",
                logical_type
            ))),
        }
    }

    /// Create a key from a vector row value.
    pub fn from_vector_value(
        vector: &Vector,
        row_idx: usize,
        logical_type: &LogicalType,
        allocator: &mut ArenaAllocator,
    ) -> Result<Self> {
        if vector.is_null(row_idx) {
            return Err(paro_error::not_null_violation("index_column"));
        }

        match logical_type {
            LogicalType::Boolean => Self::from_bool(
                allocator,
                vector
                    .get_bool(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key boolean decode failed"))?,
            ),
            LogicalType::TinyInt => Self::from_i8(
                allocator,
                vector
                    .get_i8(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key tinyint decode failed"))?,
            ),
            LogicalType::SmallInt => Self::from_i16(
                allocator,
                vector
                    .get_i16(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key smallint decode failed"))?,
            ),
            LogicalType::Integer | LogicalType::Date => Self::from_i32(
                allocator,
                vector
                    .get_i32(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key i32 decode failed"))?,
            ),
            LogicalType::BigInt
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz => Self::from_i64(
                allocator,
                vector
                    .get_i64(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key i64 decode failed"))?,
            ),
            LogicalType::HugeInt => Self::from_i128(
                allocator,
                vector
                    .get_i128(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key i128 decode failed"))?,
            ),
            LogicalType::UTinyInt => Self::from_u8(
                allocator,
                vector
                    .get_u8(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key utinyint decode failed"))?,
            ),
            LogicalType::USmallInt => Self::from_u16(
                allocator,
                vector
                    .get_u16(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key usmallint decode failed"))?,
            ),
            LogicalType::UInteger => Self::from_u32(
                allocator,
                vector
                    .get_u32(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key uinteger decode failed"))?,
            ),
            LogicalType::UBigInt => Self::from_u64(
                allocator,
                vector
                    .get_u64(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key ubigint decode failed"))?,
            ),
            LogicalType::UHugeInt | LogicalType::Uuid => Self::from_u128(
                allocator,
                vector
                    .get_u128(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key u128 decode failed"))?,
            ),
            LogicalType::Float => Self::from_f32(
                allocator,
                vector
                    .get_f32(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key float decode failed"))?,
            ),
            LogicalType::Double => Self::from_f64(
                allocator,
                vector
                    .get_f64(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key double decode failed"))?,
            ),
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Jsonb => Self::from_str(
                allocator,
                vector
                    .get_string(row_idx)
                    .ok_or_else(|| paro_error::internal("ART key string decode failed"))?,
            ),
            other => Err(paro_error::not_supported(format!(
                "Unsupported type for ART key: {:?}",
                other
            ))),
        }
    }

    // ========== Key Operations ==========

    /// Concatenate this key with another key.
    ///
    /// Creates a new key containing this key's data followed by the other key's data.
    pub fn concat(&self, allocator: &mut ArenaAllocator, other: &ARTKey) -> Result<Self> {
        let new_len = self.len + other.len;
        let data = allocator.allocate(new_len)?;

        // SAFETY: data is freshly allocated with new_len bytes
        unsafe {
            if self.len > 0 {
                ptr::copy_nonoverlapping(self.data, data, self.len);
            }
            if other.len > 0 {
                ptr::copy_nonoverlapping(other.data, data.add(self.len), other.len);
            }
        }

        Ok(Self { len: new_len, data })
    }

    /// Get the row ID from a key (assumes key is exactly 8 bytes).
    ///
    /// Row IDs are stored as u64 in big-endian format.
    pub fn get_row_id(&self) -> i64 {
        debug_assert_eq!(self.len, 8, "Row ID key must be 8 bytes");
        Radix::decode_i64(self.as_slice())
    }

    /// Create a key from raw bytes without allocation.
    ///
    /// # Safety
    /// The data must remain valid for the lifetime of the key.
    /// This is typically used for temporary keys during iteration.
    #[inline]
    pub fn from_bytes_raw(data: &[u8]) -> Self {
        Self {
            len: data.len(),
            data: data.as_ptr() as *mut u8,
        }
    }

    /// Find the first position where this key differs from another key.
    ///
    /// Starts searching from the given position.
    /// Returns the index of the first differing byte.
    ///
    /// # Panics
    /// Panics if the keys are identical (corrupted index).
    pub fn get_mismatch_pos(&self, other: &ARTKey, start: usize) -> usize {
        debug_assert!(self.len <= other.len, "Self must be <= other in length");
        debug_assert!(start <= self.len, "Start must be <= self.len");

        let self_slice = self.as_slice();
        let other_slice = other.as_slice();

        for i in start..other.len {
            if i >= self.len || self_slice[i] != other_slice[i] {
                return i;
            }
        }

        // Keys are identical - this indicates a corrupted index
        panic!("Corrupted ART index - likely the same row id was inserted twice");
    }

    /// Verify that the key length does not exceed the maximum.
    ///
    /// # Errors
    /// Returns an error if the key is too long.
    pub fn verify_key_length(&self, max_len: usize) -> Result<()> {
        if self.len > max_len {
            return Err(paro_error::invalid_input(format!(
                "Key size of {} bytes exceeds the maximum size of {} bytes for this ART",
                self.len, max_len
            )));
        }
        Ok(())
    }
}

// ========== Comparison Operators ==========

impl PartialEq for ARTKey {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ARTKey {}

impl PartialOrd for ARTKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ARTKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let min_len = self.len.min(other.len);
        let self_slice = self.as_slice();
        let other_slice = other.as_slice();

        for i in 0..min_len {
            match self_slice[i].cmp(&other_slice[i]) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }

        // If all compared bytes are equal, shorter key is smaller
        self.len.cmp(&other.len)
    }
}

impl std::fmt::Debug for ARTKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ARTKey({} bytes: ", self.len)?;
        let slice = self.as_slice();
        for (i, &byte) in slice.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{:02x}", byte)?;
            if i >= 15 && self.len > 16 {
                write!(f, " ...")?;
                break;
            }
        }
        write!(f, ")")
    }
}

// SAFETY: ARTKey is just a pointer and length, the data is owned by ArenaAllocator
unsafe impl Send for ARTKey {}
unsafe impl Sync for ARTKey {}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::DefaultAllocator;
    use std::sync::Arc;

    fn create_arena() -> ArenaAllocator {
        let allocator = Arc::new(DefaultAllocator::new());
        ArenaAllocator::new(allocator)
    }

    // ========== Radix Encoding Tests ==========

    #[test]
    fn test_radix_encode_decode_bool() {
        let mut data = [0u8; 1];
        Radix::encode_bool(&mut data, true);
        assert!(Radix::decode_bool(&data));

        Radix::encode_bool(&mut data, false);
        assert!(!Radix::decode_bool(&data));
    }

    #[test]
    fn test_radix_encode_decode_i8() {
        let mut data = [0u8; 1];
        for value in [-128i8, -1, 0, 1, 127] {
            Radix::encode_i8(&mut data, value);
            assert_eq!(Radix::decode_i8(&data), value);
        }
    }

    #[test]
    fn test_radix_encode_decode_i16() {
        let mut data = [0u8; 2];
        for value in [i16::MIN, -1, 0, 1, i16::MAX] {
            Radix::encode_i16(&mut data, value);
            assert_eq!(Radix::decode_i16(&data), value);
        }
    }

    #[test]
    fn test_radix_encode_decode_i32() {
        let mut data = [0u8; 4];
        for value in [i32::MIN, -1, 0, 1, i32::MAX] {
            Radix::encode_i32(&mut data, value);
            assert_eq!(Radix::decode_i32(&data), value);
        }
    }

    #[test]
    fn test_radix_encode_decode_i64() {
        let mut data = [0u8; 8];
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            Radix::encode_i64(&mut data, value);
            assert_eq!(Radix::decode_i64(&data), value);
        }
    }

    #[test]
    fn test_radix_encode_decode_u64() {
        let mut data = [0u8; 8];
        for value in [0u64, 1, u64::MAX / 2, u64::MAX] {
            Radix::encode_u64(&mut data, value);
            assert_eq!(Radix::decode_u64(&data), value);
        }
    }

    #[test]
    fn test_radix_encode_decode_f32() {
        let mut data = [0u8; 4];
        let values = [
            f32::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1000.0,
            f32::INFINITY,
            f32::NAN,
        ];
        for value in values {
            Radix::encode_f32(&mut data, value);
            let decoded = Radix::decode_f32(&data);
            if value.is_nan() {
                assert!(decoded.is_nan());
            } else {
                assert_eq!(decoded, value);
            }
        }
    }

    #[test]
    fn test_radix_encode_decode_f64() {
        let mut data = [0u8; 8];
        let values = [
            f64::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1000.0,
            f64::INFINITY,
            f64::NAN,
        ];
        for value in values {
            Radix::encode_f64(&mut data, value);
            let decoded = Radix::decode_f64(&data);
            if value.is_nan() {
                assert!(decoded.is_nan());
            } else {
                assert_eq!(decoded, value);
            }
        }
    }

    #[test]
    fn test_radix_signed_ordering() {
        // Verify that encoded signed integers preserve ordering
        let mut data1 = [0u8; 4];
        let mut data2 = [0u8; 4];

        let values = [i32::MIN, -1000, -1, 0, 1, 1000, i32::MAX];
        for i in 0..values.len() - 1 {
            Radix::encode_i32(&mut data1, values[i]);
            Radix::encode_i32(&mut data2, values[i + 1]);
            assert!(
                data1 < data2,
                "Ordering failed: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_radix_float_ordering() {
        // Verify that encoded floats preserve ordering
        let mut data1 = [0u8; 4];
        let mut data2 = [0u8; 4];

        let values = [
            f32::NEG_INFINITY,
            -1000.0,
            -1.0,
            0.0,
            1.0,
            1000.0,
            f32::INFINITY,
        ];
        for i in 0..values.len() - 1 {
            Radix::encode_f32(&mut data1, values[i]);
            Radix::encode_f32(&mut data2, values[i + 1]);
            assert!(
                data1 < data2,
                "Ordering failed: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    // ========== ARTKey Tests ==========

    #[test]
    fn test_art_key_empty() {
        let key = ARTKey::empty();
        assert!(key.is_empty());
        assert_eq!(key.len, 0);
        assert!(key.data.is_null());
    }

    #[test]
    fn test_art_key_from_i32() {
        let mut arena = create_arena();
        let key = ARTKey::from_i32(&mut arena, 42).unwrap();
        assert_eq!(key.len, 4);
        assert!(!key.is_empty());
    }

    #[test]
    fn test_art_key_from_i64() {
        let mut arena = create_arena();
        let key = ARTKey::from_i64(&mut arena, -12345).unwrap();
        assert_eq!(key.len, 8);
    }

    #[test]
    fn test_art_key_from_str() {
        let mut arena = create_arena();
        let key = ARTKey::from_str(&mut arena, "hello").unwrap();
        // "hello" + null terminator = 6 bytes
        assert_eq!(key.len, 6);
        assert_eq!(key.as_slice()[5], 0x00); // Null terminator
    }

    #[test]
    fn test_art_key_from_str_with_escape() {
        let mut arena = create_arena();
        // String with \x00 byte
        let key = ARTKey::from_bytes(&mut arena, &[0x00, 0x01, 0x02]).unwrap();
        // 3 bytes + 2 escapes + null = 6 bytes
        assert_eq!(key.len, 6);
        // Check escaping: \x01 \x00 \x01 \x01 \x02 \x00
        let slice = key.as_slice();
        assert_eq!(slice[0], 0x01); // Escape for \x00
        assert_eq!(slice[1], 0x00);
        assert_eq!(slice[2], 0x01); // Escape for \x01
        assert_eq!(slice[3], 0x01);
        assert_eq!(slice[4], 0x02);
        assert_eq!(slice[5], 0x00); // Null terminator
    }

    #[test]
    fn test_art_key_from_vector_value() {
        let mut arena = create_arena();
        let vector = Vector::from_i32(&[-7, 42]);

        let key = ARTKey::from_vector_value(&vector, 1, &LogicalType::Integer, &mut arena).unwrap();
        let expected = ARTKey::from_i32(&mut arena, 42).unwrap();

        assert_eq!(key, expected);
    }

    #[test]
    fn test_art_key_from_vector_value_rejects_null() {
        let mut arena = create_arena();
        let vector = Vector::from_nullable_strings(&[Some("alpha"), None]);

        let result = ARTKey::from_vector_value(&vector, 1, &LogicalType::Varchar, &mut arena);
        assert!(result.is_err());
    }

    #[test]
    fn test_art_key_comparison() {
        let mut arena = create_arena();

        let key1 = ARTKey::from_i32(&mut arena, 10).unwrap();
        let key2 = ARTKey::from_i32(&mut arena, 20).unwrap();
        let key3 = ARTKey::from_i32(&mut arena, 10).unwrap();

        assert!(key1 < key2);
        assert!(key2 > key1);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_art_key_signed_comparison() {
        let mut arena = create_arena();

        let neg = ARTKey::from_i32(&mut arena, -100).unwrap();
        let zero = ARTKey::from_i32(&mut arena, 0).unwrap();
        let pos = ARTKey::from_i32(&mut arena, 100).unwrap();

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(neg < pos);
    }

    #[test]
    fn test_art_key_string_comparison() {
        let mut arena = create_arena();

        let key_a = ARTKey::from_str(&mut arena, "apple").unwrap();
        let key_b = ARTKey::from_str(&mut arena, "banana").unwrap();
        let key_a2 = ARTKey::from_str(&mut arena, "apple").unwrap();

        assert!(key_a < key_b);
        assert_eq!(key_a, key_a2);
    }

    #[test]
    fn test_art_key_concat() {
        let mut arena = create_arena();

        let key1 = ARTKey::from_i32(&mut arena, 42).unwrap();
        let key2 = ARTKey::from_i32(&mut arena, 100).unwrap();
        let combined = key1.concat(&mut arena, &key2).unwrap();

        assert_eq!(combined.len, 8);
    }

    #[test]
    fn test_art_key_get_row_id() {
        let mut arena = create_arena();

        let row_id: i64 = 12345;
        let key = ARTKey::from_i64(&mut arena, row_id).unwrap();
        assert_eq!(key.get_row_id(), row_id);
    }

    #[test]
    fn test_art_key_get_mismatch_pos() {
        let mut arena = create_arena();

        let key1 = ARTKey::from_str(&mut arena, "hello").unwrap();
        let key2 = ARTKey::from_str(&mut arena, "help").unwrap();

        // "hello" vs "help" - differ at position 3 ('l' vs 'p')
        let pos = key2.get_mismatch_pos(&key1, 0);
        assert_eq!(pos, 3);
    }

    #[test]
    fn test_art_key_verify_length() {
        let mut arena = create_arena();

        let key = ARTKey::from_str(&mut arena, "short").unwrap();
        assert!(key.verify_key_length(100).is_ok());
        assert!(key.verify_key_length(3).is_err());
    }

    #[test]
    fn test_art_key_byte_matches() {
        let mut arena = create_arena();

        let key1 = ARTKey::from_str(&mut arena, "hello").unwrap();
        let key2 = ARTKey::from_str(&mut arena, "help").unwrap();

        assert!(key1.byte_matches(&key2, 0)); // 'h' == 'h'
        assert!(key1.byte_matches(&key2, 1)); // 'e' == 'e'
        assert!(key1.byte_matches(&key2, 2)); // 'l' == 'l'
        assert!(!key1.byte_matches(&key2, 3)); // 'l' != 'p'
    }

    #[test]
    fn test_art_key_debug_format() {
        let mut arena = create_arena();
        let key = ARTKey::from_i32(&mut arena, 42).unwrap();
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("ARTKey"));
        assert!(debug_str.contains("4 bytes"));
    }

    #[test]
    fn test_art_key_from_f32() {
        let mut arena = create_arena();

        let key_neg = ARTKey::from_f32(&mut arena, -1.0).unwrap();
        let key_zero = ARTKey::from_f32(&mut arena, 0.0).unwrap();
        let key_pos = ARTKey::from_f32(&mut arena, 1.0).unwrap();

        assert!(key_neg < key_zero);
        assert!(key_zero < key_pos);
    }

    #[test]
    fn test_art_key_from_f64() {
        let mut arena = create_arena();

        let key_neg = ARTKey::from_f64(&mut arena, -1.0).unwrap();
        let key_zero = ARTKey::from_f64(&mut arena, 0.0).unwrap();
        let key_pos = ARTKey::from_f64(&mut arena, 1.0).unwrap();

        assert!(key_neg < key_zero);
        assert!(key_zero < key_pos);
    }
}
