// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Accounted memory and value hashing shared by aggregate modifiers.

use std::sync::Arc;

use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_storage::buffer::MemoryTag;

#[inline]
pub(crate) fn mix_row_hash(mut left: u64, right: u64) -> u64 {
    left ^= left >> 32;
    left = left.wrapping_mul(0xd6e8_feb8_6659_fd93);
    left ^ right
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0x9e37_79b9_7f4a_7c15 ^ bytes.len() as u64;
    for chunk in bytes.chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        hash = mix_row_hash(hash, u64::from_le_bytes(word));
    }
    hash
}

fn hash_logical_type(logical_type: &LogicalType) -> u64 {
    let tag = u64::from(logical_type.type_id());
    match logical_type {
        LogicalType::Decimal { precision, scale } => {
            mix_row_hash(mix_row_hash(tag, u64::from(*precision)), u64::from(*scale))
        }
        LogicalType::VarcharCollation(collation) => {
            mix_row_hash(tag, hash_bytes(collation.as_bytes()))
        }
        LogicalType::IntegerLiteral(value) => mix_row_hash(tag, *value as u64),
        LogicalType::Array(child, size) => {
            mix_row_hash(mix_row_hash(tag, hash_logical_type(child)), *size as u64)
        }
        LogicalType::List(child) => mix_row_hash(tag, hash_logical_type(child)),
        LogicalType::Struct(fields) => {
            let mut hash = mix_row_hash(tag, fields.len() as u64);
            for (name, ty) in fields {
                hash = mix_row_hash(hash, hash_bytes(name.as_bytes()));
                hash = mix_row_hash(hash, hash_logical_type(ty));
            }
            hash
        }
        _ => tag,
    }
}

pub(crate) fn hash_value(value: &Value) -> u64 {
    match value {
        Value::Null(ty) => mix_row_hash(0, hash_logical_type(ty)),
        Value::Boolean(value) => mix_row_hash(1, u64::from(*value)),
        Value::TinyInt(value) => mix_row_hash(2, *value as i64 as u64),
        Value::SmallInt(value) => mix_row_hash(3, *value as i64 as u64),
        Value::Integer(value) => mix_row_hash(4, *value as i64 as u64),
        Value::BigInt(value) => mix_row_hash(5, *value as u64),
        Value::HugeInt(value) => {
            mix_row_hash(mix_row_hash(6, *value as u64), (*value >> u64::BITS) as u64)
        }
        Value::UTinyInt(value) => mix_row_hash(7, u64::from(*value)),
        Value::USmallInt(value) => mix_row_hash(8, u64::from(*value)),
        Value::UInteger(value) => mix_row_hash(9, u64::from(*value)),
        Value::UBigInt(value) => mix_row_hash(10, *value),
        Value::UHugeInt(value) => mix_row_hash(
            mix_row_hash(11, *value as u64),
            (*value >> u64::BITS) as u64,
        ),
        Value::Float(value) => mix_row_hash(12, u64::from(value.to_bits())),
        Value::Double(value) => mix_row_hash(13, value.to_bits()),
        Value::Decimal(value, precision, scale) => mix_row_hash(
            mix_row_hash(
                mix_row_hash(14, *value as u64),
                (*value >> u64::BITS) as u64,
            ),
            (u64::from(*precision) << 8) | u64::from(*scale),
        ),
        Value::Varchar(value) => mix_row_hash(15, hash_bytes(value.as_bytes())),
        Value::Blob(value) => mix_row_hash(16, hash_bytes(value)),
        Value::Uuid(value) => mix_row_hash(
            mix_row_hash(17, *value as u64),
            (*value >> u64::BITS) as u64,
        ),
        Value::Date(value) => mix_row_hash(18, *value as i64 as u64),
        Value::Timestamp(value) => mix_row_hash(19, *value as u64),
        Value::TimestampTz(value) => mix_row_hash(20, *value as u64),
        Value::Time(value) => mix_row_hash(21, *value as u64),
        Value::Interval(months, days, micros) => mix_row_hash(
            mix_row_hash(22, *months as i64 as u64),
            mix_row_hash(*days as i64 as u64, *micros as u64),
        ),
        Value::List(values, ty) => {
            mix_row_hash(mix_row_hash(23, hash_values(values)), hash_logical_type(ty))
        }
        Value::Struct(values, fields) => {
            let mut hash = mix_row_hash(24, hash_values(values));
            for (name, ty) in fields {
                hash = mix_row_hash(hash, hash_bytes(name.as_bytes()));
                hash = mix_row_hash(hash, hash_logical_type(ty));
            }
            hash
        }
        Value::Array(values, ty, size) => mix_row_hash(
            mix_row_hash(mix_row_hash(25, hash_values(values)), hash_logical_type(ty)),
            *size as u64,
        ),
    }
}

fn hash_values(values: &[Value]) -> u64 {
    let mut hash = 0xa076_1d64_78bd_642f ^ values.len() as u64;
    for value in values {
        hash = mix_row_hash(hash, hash_value(value));
    }
    hash
}

pub(crate) fn aggregate_modifier_memory_context(
    owner: Arc<dyn paro_common::memory::MemoryOwner>,
) -> MemoryAccountingContext {
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}
