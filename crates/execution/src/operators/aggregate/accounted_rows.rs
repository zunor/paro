// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Accounted row collections for aggregate DISTINCT / ORDER BY modifiers.

use std::hash::{Hash, Hasher};
use std::mem::size_of_val;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedHashSet, MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryError,
    MemoryGrant, MemoryReleaseHandle, MemoryResult, PrecomputedHashBuildHasher,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::row::{RowLayout, RowStore, RowStoreBuilder, RowValidityType};

fn grant_for_context(memory: &MemoryAccountingContext) -> MemoryGrant {
    if let Some(owner) = memory.owner() {
        MemoryGrant::new(0, memory.domain(), owner).expect("zero-byte aggregate grant should fit")
    } else {
        MemoryGrant::detached(usize::MAX / 4, memory.domain())
    }
}

fn key_memory_usage(bytes: &[u8]) -> usize {
    size_of_val(bytes)
}

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

/// Accounted encoded key used by aggregate DISTINCT modifiers.
#[derive(Debug)]
pub(crate) struct AccountedDistinctKey {
    bytes: Box<[u8]>,
    hash: u64,
    release: MemoryReleaseHandle,
}

impl AccountedDistinctKey {
    pub(crate) fn from_chunk_row(
        memory: &MemoryAccountingContext,
        chunk: &Chunk,
        row_idx: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<Self> {
        encode_chunk_row_key(chunk, row_idx, scratch)?;
        Self::new(memory, scratch).map_err(Into::into)
    }

    fn new(memory: &MemoryAccountingContext, bytes: &[u8]) -> MemoryResult<Self> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| MemoryError::physical_allocation_failed(bytes.len()))?;
        owned.extend_from_slice(bytes);
        let bytes = owned.into_boxed_slice();
        let hash = hash_bytes(&bytes);
        let release = memory.retain(key_memory_usage(&bytes))?;
        Ok(Self {
            bytes,
            hash,
            release,
        })
    }
}

impl PartialEq for AccountedDistinctKey {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for AccountedDistinctKey {}

impl Hash for AccountedDistinctKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl Drop for AccountedDistinctKey {
    fn drop(&mut self) {
        self.release.release();
    }
}

#[derive(Debug)]
pub(crate) struct DistinctRowSet {
    key_memory: MemoryAccountingContext,
    row_types: Box<[LogicalType]>,
    rows: RowStoreBuilder,
    row_ordinals: Vec<u64>,
    keys: AccountedHashSet<AccountedDistinctKey, PrecomputedHashBuildHasher>,
}

impl DistinctRowSet {
    pub(crate) fn new(
        buffer_pool: Arc<BufferPool>,
        row_types: Vec<LogicalType>,
        memory: MemoryAccountingContext,
    ) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        let layout = Arc::new(RowLayout::from_types(
            row_types.clone(),
            RowValidityType::CanHaveNullValues,
        ));
        Self {
            key_memory: memory.clone(),
            row_types: row_types.into_boxed_slice(),
            rows: RowStoreBuilder::new_with_memory(
                buffer_pool,
                layout,
                MemoryTag::HashTable,
                memory,
            ),
            row_ordinals: Vec::new(),
            keys: AccountedHashSet::new_with_accounting_and_hasher(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
                PrecomputedHashBuildHasher,
            ),
        }
    }

    pub(crate) fn row_types(&self) -> &[LogicalType] {
        &self.row_types
    }

    pub(crate) fn try_insert_key_from_chunk(
        &mut self,
        chunk: &Chunk,
        row_idx: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<bool> {
        let key = AccountedDistinctKey::from_chunk_row(&self.key_memory, chunk, row_idx, scratch)?;
        self.keys
            .try_insert(key)
            .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate key: {e}")))
    }

    pub(crate) fn append_selected_rows(
        &mut self,
        chunk: &Chunk,
        sel: &SelectionVector,
        count: usize,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let start = self.rows.count();
        let appended = self.rows.append_selected(chunk, sel, count)?;
        if appended != count {
            return Err(paro_error::internal(format!(
                "distinct row-store append count mismatch: expected={count}, appended={appended}"
            )));
        }
        self.row_ordinals
            .try_reserve(appended)
            .map_err(|_| paro_error::out_of_memory("distinct row ordinal allocation failed"))?;
        self.row_ordinals
            .extend(start..start.saturating_add(appended as u64));
        Ok(())
    }

    pub(crate) fn into_rows(self) -> Result<DistinctRows> {
        let Self {
            key_memory: _,
            row_types: _,
            rows,
            row_ordinals,
            keys,
        } = self;
        drop(keys);
        let store = rows.try_seal()?;
        Ok(DistinctRows {
            store,
            row_ordinals: row_ordinals.into_boxed_slice(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct DistinctRows {
    store: RowStore,
    row_ordinals: Box<[u64]>,
}

impl DistinctRows {
    pub(crate) fn len(&self) -> usize {
        self.row_ordinals.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.row_ordinals.is_empty()
    }

    pub(crate) fn row_width(&self) -> usize {
        self.store.layout().types().len()
    }

    pub(crate) fn ordinals(&self) -> &[u64] {
        &self.row_ordinals
    }

    pub(crate) fn pin_ordinals(
        &self,
        ordinals: &[u64],
        ordering: paro_storage::row::Ordering,
    ) -> Result<paro_storage::row::PinnedRows<'_>> {
        self.store.pin_ordinals(ordinals, ordering)
    }
}

fn encode_chunk_row_key(chunk: &Chunk, row_idx: usize, scratch: &mut Vec<u8>) -> Result<()> {
    if row_idx >= chunk.size() {
        return Err(paro_error::internal(format!(
            "distinct key row index out of bounds: row_idx={row_idx}, row_count={}",
            chunk.size()
        )));
    }
    scratch.clear();
    for col_idx in 0..chunk.column_count() {
        let column = chunk.column(col_idx).ok_or_else(|| {
            paro_error::internal(format!("distinct key column not found: idx={col_idx}"))
        })?;
        encode_vector_value(column, row_idx, scratch)?;
    }
    Ok(())
}

fn encode_vector_value(column: &Vector, row_idx: usize, out: &mut Vec<u8>) -> Result<()> {
    if column.is_null(row_idx) {
        out.push(0);
        encode_logical_type(column.logical_type(), out);
        return Ok(());
    }

    out.push(1);
    match column.logical_type() {
        LogicalType::Boolean => out.push(u8::from(required(column.get_bool(row_idx), "bool")?)),
        LogicalType::TinyInt => out.push(required(column.get_i8(row_idx), "tinyint")? as u8),
        LogicalType::SmallInt => {
            out.extend_from_slice(&required(column.get_i16(row_idx), "smallint")?.to_le_bytes())
        }
        LogicalType::Integer => {
            out.extend_from_slice(&required(column.get_i32(row_idx), "integer")?.to_le_bytes())
        }
        LogicalType::BigInt => {
            out.extend_from_slice(&required(column.get_i64(row_idx), "bigint")?.to_le_bytes())
        }
        LogicalType::HugeInt => {
            out.extend_from_slice(&required(column.get_i128(row_idx), "hugeint")?.to_le_bytes())
        }
        LogicalType::UTinyInt => out.push(required(column.get_u8(row_idx), "utinyint")?),
        LogicalType::USmallInt => {
            out.extend_from_slice(&required(column.get_u16(row_idx), "usmallint")?.to_le_bytes())
        }
        LogicalType::UInteger => {
            out.extend_from_slice(&required(column.get_u32(row_idx), "uinteger")?.to_le_bytes())
        }
        LogicalType::UBigInt => {
            out.extend_from_slice(&required(column.get_u64(row_idx), "ubigint")?.to_le_bytes())
        }
        LogicalType::UHugeInt => {
            out.extend_from_slice(&required(column.get_u128(row_idx), "uhugeint")?.to_le_bytes())
        }
        LogicalType::Float => out.extend_from_slice(
            &required(column.get_f32(row_idx), "float")?
                .to_bits()
                .to_le_bytes(),
        ),
        LogicalType::Double => out.extend_from_slice(
            &required(column.get_f64(row_idx), "double")?
                .to_bits()
                .to_le_bytes(),
        ),
        LogicalType::Decimal { precision, scale } => {
            out.push(*precision);
            out.push(*scale);
            if *precision <= 18 {
                out.extend_from_slice(
                    &(required(column.get_i64(row_idx), "decimal")? as i128).to_le_bytes(),
                );
            } else {
                out.extend_from_slice(
                    &required(column.get_i128(row_idx), "decimal")?.to_le_bytes(),
                );
            }
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => append_len_prefixed_bytes(
            required(column.get_string(row_idx), "varchar")?.as_bytes(),
            out,
        ),
        LogicalType::Blob => {
            append_len_prefixed_bytes(required(column.get_blob(row_idx), "blob")?, out)
        }
        LogicalType::Uuid => {
            out.extend_from_slice(&required(column.get_u128(row_idx), "uuid")?.to_le_bytes())
        }
        LogicalType::Date => {
            out.extend_from_slice(&required(column.get_i32(row_idx), "date")?.to_le_bytes())
        }
        LogicalType::Timestamp | LogicalType::TimestampTz | LogicalType::Time => {
            out.extend_from_slice(&required(column.get_i64(row_idx), "timestamp")?.to_le_bytes())
        }
        LogicalType::Interval => {
            let (months, days, micros) = required(column.get_interval(row_idx), "interval")?;
            out.extend_from_slice(&months.to_le_bytes());
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&micros.to_le_bytes());
        }
        LogicalType::Array(_, _) | LogicalType::List(_) | LogicalType::Struct(_) => {
            encode_value(&column.get_value(row_idx), out);
        }
        LogicalType::Null
        | LogicalType::IntegerLiteral(_)
        | LogicalType::StringLiteral
        | LogicalType::Unknown => {
            encode_value(&column.get_value(row_idx), out);
        }
    }
    Ok(())
}

fn required<T>(value: Option<T>, label: &str) -> Result<T> {
    value.ok_or_else(|| paro_error::internal(format!("non-null distinct {label} value missing")))
}

fn append_len_prefixed_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode_logical_type(ty: &LogicalType, out: &mut Vec<u8>) {
    out.push(ty.type_id());
    match ty {
        LogicalType::Decimal { precision, scale } => {
            out.push(*precision);
            out.push(*scale);
        }
        LogicalType::VarcharCollation(collation) => {
            append_len_prefixed_bytes(collation.as_bytes(), out);
        }
        LogicalType::IntegerLiteral(value) => out.extend_from_slice(&value.to_le_bytes()),
        LogicalType::Array(child, size) => {
            encode_logical_type(child, out);
            out.extend_from_slice(&(*size as u64).to_le_bytes());
        }
        LogicalType::List(child) => encode_logical_type(child, out),
        LogicalType::Struct(fields) => {
            out.extend_from_slice(&(fields.len() as u64).to_le_bytes());
            for (name, field_type) in fields {
                append_len_prefixed_bytes(name.as_bytes(), out);
                encode_logical_type(field_type, out);
            }
        }
        _ => {}
    }
}

fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null(ty) => {
            out.push(0);
            encode_logical_type(ty, out);
        }
        Value::Boolean(value) => {
            out.push(1);
            out.push(u8::from(*value));
        }
        Value::TinyInt(value) => {
            out.push(2);
            out.push(*value as u8);
        }
        Value::SmallInt(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Integer(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::BigInt(value) => {
            out.push(5);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::HugeInt(value) => {
            out.push(6);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::UTinyInt(value) => {
            out.push(7);
            out.push(*value);
        }
        Value::USmallInt(value) => {
            out.push(8);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::UInteger(value) => {
            out.push(9);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::UBigInt(value) => {
            out.push(10);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::UHugeInt(value) => {
            out.push(11);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            out.push(12);
            out.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::Double(value) => {
            out.push(13);
            out.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::Decimal(value, precision, scale) => {
            out.push(14);
            out.extend_from_slice(&value.to_le_bytes());
            out.push(*precision);
            out.push(*scale);
        }
        Value::Varchar(value) => {
            out.push(15);
            append_len_prefixed_bytes(value.as_bytes(), out);
        }
        Value::Blob(value) => {
            out.push(16);
            append_len_prefixed_bytes(value, out);
        }
        Value::Uuid(value) => {
            out.push(17);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Date(value) => {
            out.push(18);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Timestamp(value) => {
            out.push(19);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::TimestampTz(value) => {
            out.push(20);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Time(value) => {
            out.push(21);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Interval(months, days, micros) => {
            out.push(22);
            out.extend_from_slice(&months.to_le_bytes());
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&micros.to_le_bytes());
        }
        Value::List(values, child_type) => {
            out.push(23);
            encode_logical_type(child_type, out);
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for value in values {
                encode_value(value, out);
            }
        }
        Value::Struct(values, fields) => {
            out.push(24);
            out.extend_from_slice(&(fields.len() as u64).to_le_bytes());
            for ((name, field_type), value) in fields.iter().zip(values.iter()) {
                append_len_prefixed_bytes(name.as_bytes(), out);
                encode_logical_type(field_type, out);
                encode_value(value, out);
            }
        }
        Value::Array(values, child_type, size) => {
            out.push(25);
            encode_logical_type(child_type, out);
            out.extend_from_slice(&(*size as u64).to_le_bytes());
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for value in values {
                encode_value(value, out);
            }
        }
    }
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
