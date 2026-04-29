// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! WAL entry types and binary encoding for catalog and data operations.

use crate::wal::txn_record::TxnRecord;
use crate::wal::wal_type::WalType;
use crate::{decode_frame, encode_record};
#[cfg(test)]
use paro_common::allocator::default_allocator;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::effect::CompactionCumulativePointAction;
use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::journal::JournalRecord;
use paro_common::types::LogicalType;
use std::sync::Arc;

/// Length of database identity bytes stored in WAL header metadata.
pub const WAL_DB_IDENTIFIER_LEN: usize = 16;

/// WAL file-level metadata written in the header.
///
/// This is used to verify WAL ownership before replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeaderMetadata {
    /// Persistent database identity from the data file header.
    pub db_identifier: [u8; WAL_DB_IDENTIFIER_LEN],
    /// Checkpoint iteration of the data file when this WAL started.
    pub checkpoint_iteration: u64,
}

impl WalHeaderMetadata {
    /// Zero/default metadata used when no storage identity is available.
    pub const ZERO: Self = Self {
        db_identifier: [0; WAL_DB_IDENTIFIER_LEN],
        checkpoint_iteration: 0,
    };

    /// Create metadata from explicit values.
    pub fn new(db_identifier: [u8; WAL_DB_IDENTIFIER_LEN], checkpoint_iteration: u64) -> Self {
        Self {
            db_identifier,
            checkpoint_iteration,
        }
    }
}

impl Default for WalHeaderMetadata {
    fn default() -> Self {
        Self::ZERO
    }
}

/// WAL entry header.
///
/// Each WAL entry starts with this header:
/// - size: Total size of the entry data (excluding header)
/// - checksum: Checksum of the entry data
#[derive(Debug, Clone, Copy)]
pub struct WalEntryHeader {
    /// Size of the entry data in bytes
    pub size: u64,
    /// Checksum of the entry data
    pub checksum: u64,
}

impl WalEntryHeader {
    /// Header size in bytes (size: u64 + checksum: u64)
    pub const SIZE: usize = 16;

    /// Create a new entry header.
    pub fn new(size: u64, checksum: u64) -> Self {
        Self { size, checksum }
    }

    /// Serialize the header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.size.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    /// Deserialize the header from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(paro_error::serialization_error(format!(
                "Invalid WAL header size: expected {}, got {}",
                Self::SIZE,
                bytes.len()
            )));
        }
        let size = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let checksum = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(Self { size, checksum })
    }
}

/// WAL entry representing a logged operation.
#[derive(Debug)]
pub enum WalEntry {
    /// WAL version header
    Version { version: u64 },

    /// Binary-framed durable journal record.
    JournalRecord { lsn: u64, record: JournalRecord },

    /// Unified transaction begin.
    TxnBegin { txn_id: u64, start_time: u64 },

    /// Unified transaction catalog op.
    TxnCatalogOp {
        seq: u32,
        op: paro_common::effect::CatalogTxnOp,
    },

    /// Unified transaction data op.
    TxnDataOp {
        seq: u32,
        op: paro_common::effect::PreparedDataOp,
    },

    /// Unified transaction post-commit hook.
    TxnPostCommitHook {
        seq: u32,
        hook: paro_common::effect::PostCommitHookDescriptor,
    },

    /// Unified transaction commit.
    TxnCommit { txn_id: u64, commit_id: u64 },

    /// Unified transaction abort.
    TxnAbort { txn_id: u64 },

    /// DELETE by primary key bytes (batch)
    PrimaryDelete { keys: Vec<Vec<u8>> },

    /// DELETE by physical row location triples `(rowset_id, segment_id, row_id)`.
    RowIdDelete { locations: Vec<(u64, u32, u32)> },

    /// Rowset commit (Tablet + Rowset versioned publish)
    RowsetCommit {
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: String,
    },

    /// Compaction publish intent (replace inputs with one output rowset)
    CompactionPublish {
        tablet_id: u64,
        plan_id: u64,
        job_id: u64,
        output_rowset_id: u64,
        output_start_version: i64,
        output_end_version: i64,
        cumulative_point_action: CompactionCumulativePointAction,
        output_rowset_path: String,
        replaced_inputs: Vec<u64>,
    },

    /// Flush marker
    Flush,
}

/// Column information for CREATE TABLE entries.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column type
    pub logical_type: LogicalType,
    /// Whether the column is nullable
    pub nullable: bool,
}

impl ColumnInfo {
    /// Create a new column info.
    pub fn new(name: String, logical_type: LogicalType, nullable: bool) -> Self {
        Self {
            name,
            logical_type,
            nullable,
        }
    }

    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Name length + name
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name_bytes);

        // Logical type (serialized with extended info for compound types)
        SerializedDataChunk::serialize_logical_type(&mut bytes, &self.logical_type);

        // Nullable flag
        bytes.push(if self.nullable { 1 } else { 0 });

        bytes
    }

    /// Deserialize from bytes.
    pub fn deserialize(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        // Read name length
        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error("Truncated column info"));
        }
        let name_len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        // Read name
        if *offset + name_len > bytes.len() {
            return Err(paro_error::serialization_error("Truncated column name"));
        }
        let name = String::from_utf8(bytes[*offset..*offset + name_len].to_vec())
            .map_err(|e| paro_error::serialization_error(format!("Invalid column name: {}", e)))?;
        *offset += name_len;

        // Read logical type (with extended info for compound types)
        let logical_type = SerializedDataChunk::deserialize_logical_type(bytes, offset)?;

        // Read nullable
        if *offset + 1 > bytes.len() {
            return Err(paro_error::serialization_error("Truncated nullable flag"));
        }
        let nullable = bytes[*offset] != 0;
        *offset += 1;

        Ok(Self {
            name,
            logical_type,
            nullable,
        })
    }
}

/// Constraint type codes serialized in WAL for CREATE TABLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalConstraintType {
    NotNull = 0,
    Unique = 1,
    PrimaryKey = 2,
    ForeignKey = 3,
    Check = 4,
}

impl WalConstraintType {
    #[inline]
    pub fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::NotNull),
            1 => Ok(Self::Unique),
            2 => Ok(Self::PrimaryKey),
            3 => Ok(Self::ForeignKey),
            4 => Ok(Self::Check),
            _ => Err(paro_error::serialization_error(format!(
                "Invalid WAL constraint type: {}",
                value
            ))),
        }
    }
}

/// Index constraint type codes serialized in legacy WAL CREATE INDEX entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalIndexConstraintType {
    None = 0,
    Unique = 1,
    Primary = 2,
    Foreign = 3,
}

impl WalIndexConstraintType {
    #[inline]
    pub fn to_byte(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Unique),
            2 => Ok(Self::Primary),
            3 => Ok(Self::Foreign),
            _ => Err(paro_error::serialization_error(format!(
                "Invalid WAL index constraint type: {}",
                value
            ))),
        }
    }
}

/// Table constraint payload persisted in WAL CREATE TABLE entries.
#[derive(Debug, Clone)]
pub struct TableConstraintInfo {
    /// Encoded with `WalConstraintType`
    pub constraint_type: u8,
    /// Referenced columns (0-based)
    pub columns: Vec<u32>,
    /// CHECK expression
    pub expression: Option<String>,
    /// Referenced table for foreign key
    pub referenced_table: Option<String>,
    /// Referenced columns for foreign key
    pub referenced_columns: Option<Vec<u32>>,
}

impl TableConstraintInfo {
    pub fn new(constraint_type: WalConstraintType, columns: Vec<u32>) -> Self {
        Self {
            constraint_type: constraint_type as u8,
            columns,
            expression: None,
            referenced_table: None,
            referenced_columns: None,
        }
    }

    pub fn constraint_type_enum(&self) -> Result<WalConstraintType> {
        WalConstraintType::from_byte(self.constraint_type)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.push(self.constraint_type);

        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for column in &self.columns {
            bytes.extend_from_slice(&column.to_le_bytes());
        }

        match &self.expression {
            Some(expr) => {
                bytes.push(1);
                let expr_bytes = expr.as_bytes();
                bytes.extend_from_slice(&(expr_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(expr_bytes);
            }
            None => bytes.push(0),
        }

        match &self.referenced_table {
            Some(table) => {
                bytes.push(1);
                let table_bytes = table.as_bytes();
                bytes.extend_from_slice(&(table_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(table_bytes);
            }
            None => bytes.push(0),
        }

        match &self.referenced_columns {
            Some(columns) => {
                bytes.push(1);
                bytes.extend_from_slice(&(columns.len() as u32).to_le_bytes());
                for column in columns {
                    bytes.extend_from_slice(&column.to_le_bytes());
                }
            }
            None => bytes.push(0),
        }

        bytes
    }

    pub fn deserialize(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        if *offset + 1 > bytes.len() {
            return Err(paro_error::serialization_error(
                "Truncated table constraint type",
            ));
        }
        let constraint_type = bytes[*offset];
        WalConstraintType::from_byte(constraint_type)?;
        *offset += 1;

        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error(
                "Truncated table constraint column count",
            ));
        }
        let column_count =
            u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if *offset + 4 > bytes.len() {
                return Err(paro_error::serialization_error(
                    "Truncated table constraint column id",
                ));
            }
            columns.push(u32::from_le_bytes(
                bytes[*offset..*offset + 4].try_into().unwrap(),
            ));
            *offset += 4;
        }

        let expression = read_optional_string(bytes, offset)?;
        let referenced_table = read_optional_string(bytes, offset)?;
        let referenced_columns = read_optional_u32_vec(bytes, offset)?;

        Ok(Self {
            constraint_type,
            columns,
            expression,
            referenced_table,
            referenced_columns,
        })
    }
}

/// CREATE INDEX metadata payload persisted in WAL.
#[derive(Debug, Clone)]
pub struct WalIndexInfo {
    pub table_name: String,
    pub index_name: String,
    pub index_type: String,
    /// Encoded as `WalIndexConstraintType`
    pub constraint_type: u8,
    pub column_ids: Vec<u32>,
    pub column_types: Vec<LogicalType>,
    pub fulltext_config: Option<String>,
}

impl WalIndexInfo {
    pub fn new(
        table_name: String,
        index_name: String,
        index_type: String,
        constraint_type: WalIndexConstraintType,
        column_ids: Vec<u32>,
        column_types: Vec<LogicalType>,
        fulltext_config: Option<String>,
    ) -> Self {
        Self {
            table_name,
            index_name,
            index_type,
            constraint_type: constraint_type.to_byte(),
            column_ids,
            column_types,
            fulltext_config,
        }
    }

    pub fn constraint_type_enum(&self) -> Result<WalIndexConstraintType> {
        WalIndexConstraintType::from_byte(self.constraint_type)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let table_name_bytes = self.table_name.as_bytes();
        bytes.extend_from_slice(&(table_name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(table_name_bytes);

        let index_name_bytes = self.index_name.as_bytes();
        bytes.extend_from_slice(&(index_name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(index_name_bytes);

        let index_type_bytes = self.index_type.as_bytes();
        bytes.extend_from_slice(&(index_type_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(index_type_bytes);

        bytes.push(self.constraint_type);

        bytes.extend_from_slice(&(self.column_ids.len() as u32).to_le_bytes());
        for column_id in &self.column_ids {
            bytes.extend_from_slice(&column_id.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.column_types.len() as u32).to_le_bytes());
        for logical_type in &self.column_types {
            SerializedDataChunk::serialize_logical_type(&mut bytes, logical_type);
        }

        match &self.fulltext_config {
            Some(config) => {
                bytes.push(1);
                let config_bytes = config.as_bytes();
                bytes.extend_from_slice(&(config_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(config_bytes);
            }
            None => bytes.push(0),
        }

        bytes
    }

    pub fn deserialize(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        let table_name = read_string(bytes, offset)?;
        let index_name = read_string(bytes, offset)?;
        let index_type = read_string(bytes, offset)?;

        if *offset + 1 > bytes.len() {
            return Err(paro_error::serialization_error(
                "Truncated index constraint type",
            ));
        }
        let constraint_type = bytes[*offset];
        *offset += 1;
        WalIndexConstraintType::from_byte(constraint_type)?;

        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error(
                "Truncated index column id count",
            ));
        }
        let column_id_count =
            u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        let mut column_ids = Vec::with_capacity(column_id_count);
        for _ in 0..column_id_count {
            if *offset + 4 > bytes.len() {
                return Err(paro_error::serialization_error("Truncated index column id"));
            }
            column_ids.push(u32::from_le_bytes(
                bytes[*offset..*offset + 4].try_into().unwrap(),
            ));
            *offset += 4;
        }

        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error(
                "Truncated index column type count",
            ));
        }
        let column_type_count =
            u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        let mut column_types = Vec::with_capacity(column_type_count);
        for _ in 0..column_type_count {
            column_types.push(SerializedDataChunk::deserialize_logical_type(
                bytes, offset,
            )?);
        }

        let fulltext_config = if *offset < bytes.len() {
            if *offset + 1 > bytes.len() {
                return Err(paro_error::serialization_error(
                    "Truncated index fulltext config flag",
                ));
            }
            let has_config = bytes[*offset] != 0;
            *offset += 1;
            if has_config {
                Some(read_string(bytes, offset)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            table_name,
            index_name,
            index_type,
            constraint_type,
            column_ids,
            column_types,
            fulltext_config,
        })
    }
}

/// Serialized Chunk for WAL storage.
///
/// This representation stores the complete data from a Chunk
/// so it can be reconstructed during WAL recovery.
///
/// ## Serialization Format
/// For each column:
/// - Validity mask bytes (ceil(row_count / 8) bytes)
/// - Raw data bytes (depends on type)
///
/// ## Supported Types
/// - Primitive types (Integer, BigInt, Float, Double, etc.)
/// - Varchar (stored as length-prefixed strings)
/// - Array types (stored recursively)
#[derive(Debug, Clone)]
pub struct SerializedDataChunk {
    /// Number of rows
    pub row_count: u64,
    /// Column types
    pub column_types: Vec<LogicalType>,
    /// Serialized column data
    pub data: Vec<u8>,
}

impl SerializedDataChunk {
    /// Create from a Chunk.
    ///
    /// Serializes the complete data including:
    /// - Row count
    /// - Column types  
    /// - For each column: validity mask + raw data
    pub fn from_chunk(chunk: &Chunk) -> Result<Self> {
        if (0..chunk.column_count()).any(|col_idx| {
            chunk
                .column(col_idx)
                .map(|col| col.vector_type() != paro_common::vector::VectorType::Flat)
                .unwrap_or(false)
        }) {
            let mut flattened_chunk = chunk.clone();
            flattened_chunk.try_flatten()?;
            return Self::from_chunk(&flattened_chunk);
        }

        let row_count = chunk.size() as u64;
        let column_types: Vec<LogicalType> = chunk.types();
        let mut data = Vec::new();

        // Serialize each column
        for col_idx in 0..chunk.column_count() {
            let col = chunk.column(col_idx).ok_or_else(|| {
                paro_error::serialization_error(format!("Column {} not found", col_idx))
            })?;

            Self::serialize_vector(&mut data, col, row_count as usize)?;
        }

        Ok(Self {
            row_count,
            column_types,
            data,
        })
    }

    /// Serialize a single vector to bytes.
    fn serialize_vector(
        data: &mut Vec<u8>,
        vector: &paro_common::vector::Vector,
        count: usize,
    ) -> Result<()> {
        use paro_common::vector::VectorType;

        // First, serialize validity mask
        let validity = vector.validity();
        let validity_bytes = count.div_ceil(8);

        // Write validity mask
        for byte_idx in 0..validity_bytes {
            let mut byte = 0u8;
            for bit_idx in 0..8 {
                let row_idx = byte_idx * 8 + bit_idx;
                if row_idx < count && validity.is_valid(row_idx) {
                    byte |= 1 << bit_idx;
                }
            }
            data.push(byte);
        }

        // Serialize data based on type
        let logical_type = vector.logical_type();
        match logical_type {
            LogicalType::Boolean => {
                for i in 0..count {
                    if validity.is_valid(i) {
                        let val: bool = unsafe { vector.get_flat(i) };
                        data.push(if val { 1 } else { 0 });
                    } else {
                        data.push(0);
                    }
                }
            }
            LogicalType::TinyInt => {
                for i in 0..count {
                    let val: i8 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0
                    };
                    data.push(val as u8);
                }
            }
            LogicalType::SmallInt => {
                for i in 0..count {
                    let val: i16 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Integer => {
                for i in 0..count {
                    let val: i32 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::BigInt => {
                for i in 0..count {
                    let val: i64 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Uuid => {
                for i in 0..count {
                    let val: u128 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Float => {
                for i in 0..count {
                    let val: f32 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0.0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Double => {
                for i in 0..count {
                    let val: f64 = if validity.is_valid(i) {
                        unsafe { vector.get_flat(i) }
                    } else {
                        0.0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Decimal { precision, .. } => {
                for i in 0..count {
                    let val = if validity.is_valid(i) {
                        if *precision <= 18 {
                            vector.get_i64(i).unwrap_or(0) as i128
                        } else {
                            vector.get_i128(i).unwrap_or(0)
                        }
                    } else {
                        0
                    };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                for i in 0..count {
                    if validity.is_valid(i) {
                        if let Some(s) = vector.get_string(i) {
                            let bytes = s.as_bytes();
                            data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                            data.extend_from_slice(bytes);
                        } else {
                            data.extend_from_slice(&0u32.to_le_bytes());
                        }
                    } else {
                        data.extend_from_slice(&0u32.to_le_bytes());
                    }
                }
            }
            LogicalType::Blob => {
                for i in 0..count {
                    if validity.is_valid(i) {
                        if let Some(b) = vector.get_blob(i) {
                            data.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            data.extend_from_slice(b);
                        } else {
                            data.extend_from_slice(&0u32.to_le_bytes());
                        }
                    } else {
                        data.extend_from_slice(&0u32.to_le_bytes());
                    }
                }
            }
            LogicalType::Array(child_type, array_size) => {
                // For Array types, serialize the child vector
                if let Some(child) = vector.child() {
                    let child_count = count * array_size;
                    Self::serialize_vector(data, child, child_count)?;
                } else {
                    // No child vector, write zeros
                    let child_size = child_type.physical_size();
                    let total_bytes = count * array_size * child_size;
                    data.resize(data.len() + total_bytes, 0);
                }
            }
            LogicalType::List(_child_type) => {
                let entry_bytes = count * 2 * std::mem::size_of::<u32>();
                if entry_bytes > 0 {
                    let entries: &[u8] = unsafe {
                        std::slice::from_raw_parts(vector.flat_data::<u8>(), entry_bytes)
                    };
                    data.extend_from_slice(entries);
                }
                if let Some(child) = vector.child() {
                    let child_count = child.len();
                    Self::serialize_vector(data, child, child_count)?;
                }
            }
            LogicalType::Struct(fields) => {
                let children = vector
                    .children()
                    .ok_or_else(|| paro_error::internal("Struct vector missing children"))?;
                if children.len() != fields.len() {
                    return Err(paro_error::internal(
                        "Struct child count mismatch during WAL serialization",
                    ));
                }
                for child in children.iter() {
                    Self::serialize_vector(data, child, count)?;
                }
            }
            _ => {
                // For unsupported types, write raw bytes based on physical size
                let type_size = logical_type.physical_size();
                if type_size > 0 && vector.vector_type() == VectorType::Flat {
                    let slice: &[u8] = unsafe {
                        std::slice::from_raw_parts(vector.flat_data::<u8>(), count * type_size)
                    };
                    data.extend_from_slice(slice);
                } else {
                    // Fallback: write zeros
                    data.resize(data.len() + count * type_size.max(1), 0);
                }
            }
        }

        Ok(())
    }

    /// Create an empty serialized chunk with the given schema.
    pub fn empty(column_types: Vec<LogicalType>) -> Self {
        Self {
            row_count: 0,
            column_types,
            data: Vec::new(),
        }
    }

    /// Deserialize to a Chunk.
    pub fn to_chunk_with_allocator(&self, allocator: Arc<dyn Allocator>) -> Result<Chunk> {
        if self.row_count == 0 {
            return Chunk::try_init_empty(&self.column_types, allocator);
        }

        let mut chunk =
            Chunk::try_initialize(&self.column_types, self.row_count as usize, allocator)?;
        let mut offset = 0;

        for col_idx in 0..self.column_types.len() {
            let col_type = &self.column_types[col_idx];
            let col = chunk.column_mut(col_idx).ok_or_else(|| {
                paro_error::serialization_error(format!("Column {} not found", col_idx))
            })?;

            Self::deserialize_vector(
                &self.data,
                &mut offset,
                col,
                self.row_count as usize,
                col_type,
            )?;
        }

        chunk.set_cardinality(self.row_count as usize);
        Ok(chunk)
    }

    #[cfg(test)]
    pub fn to_chunk(&self) -> Result<Chunk> {
        self.to_chunk_with_allocator(Arc::new(default_allocator()))
    }

    /// Deserialize a single vector from bytes.
    fn deserialize_vector(
        data: &[u8],
        offset: &mut usize,
        vector: &mut paro_common::vector::Vector,
        count: usize,
        logical_type: &LogicalType,
    ) -> Result<()> {
        // Read validity mask
        let validity_bytes = count.div_ceil(8);
        if *offset + validity_bytes > data.len() {
            return Err(paro_error::serialization_error("Truncated validity mask"));
        }

        // Set validity
        for byte_idx in 0..validity_bytes {
            let byte = data[*offset + byte_idx];
            for bit_idx in 0..8 {
                let row_idx = byte_idx * 8 + bit_idx;
                if row_idx < count {
                    if byte & (1 << bit_idx) != 0 {
                        vector.validity_mut().set_valid(row_idx);
                    } else {
                        vector.validity_mut().set_null(row_idx);
                    }
                }
            }
        }
        *offset += validity_bytes;

        // Deserialize data based on type
        match logical_type {
            LogicalType::Boolean => {
                for i in 0..count {
                    if *offset >= data.len() {
                        return Err(paro_error::serialization_error("Truncated boolean data"));
                    }
                    let val = data[*offset] != 0;
                    unsafe { vector.set_flat(i, val) };
                    *offset += 1;
                }
            }
            LogicalType::TinyInt => {
                for i in 0..count {
                    if *offset >= data.len() {
                        return Err(paro_error::serialization_error("Truncated tinyint data"));
                    }
                    let val = data[*offset] as i8;
                    unsafe { vector.set_flat(i, val) };
                    *offset += 1;
                }
            }
            LogicalType::SmallInt => {
                for i in 0..count {
                    if *offset + 2 > data.len() {
                        return Err(paro_error::serialization_error("Truncated smallint data"));
                    }
                    let val = i16::from_le_bytes(data[*offset..*offset + 2].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 2;
                }
            }
            LogicalType::Integer => {
                for i in 0..count {
                    if *offset + 4 > data.len() {
                        return Err(paro_error::serialization_error("Truncated integer data"));
                    }
                    let val = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 4;
                }
            }
            LogicalType::BigInt => {
                for i in 0..count {
                    if *offset + 8 > data.len() {
                        return Err(paro_error::serialization_error("Truncated bigint data"));
                    }
                    let val = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 8;
                }
            }
            LogicalType::Uuid => {
                for i in 0..count {
                    if *offset + 16 > data.len() {
                        return Err(paro_error::serialization_error("Truncated uuid data"));
                    }
                    let val = u128::from_le_bytes(data[*offset..*offset + 16].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 16;
                }
            }
            LogicalType::Float => {
                for i in 0..count {
                    if *offset + 4 > data.len() {
                        return Err(paro_error::serialization_error("Truncated float data"));
                    }
                    let val = f32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 4;
                }
            }
            LogicalType::Double => {
                for i in 0..count {
                    if *offset + 8 > data.len() {
                        return Err(paro_error::serialization_error("Truncated double data"));
                    }
                    let val = f64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                    unsafe { vector.set_flat(i, val) };
                    *offset += 8;
                }
            }
            LogicalType::Decimal { precision, .. } => {
                for i in 0..count {
                    if *offset + 16 > data.len() {
                        return Err(paro_error::serialization_error("Truncated decimal data"));
                    }
                    let val = i128::from_le_bytes(data[*offset..*offset + 16].try_into().unwrap());
                    *offset += 16;

                    if *precision <= 18 {
                        let narrow = i64::try_from(val).map_err(|_| {
                            paro_error::serialization_error("Decimal value exceeds i64 range")
                        })?;
                        unsafe { vector.set_flat(i, narrow) };
                    } else {
                        unsafe { vector.set_flat(i, val) };
                    }
                }
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                for i in 0..count {
                    if *offset + 4 > data.len() {
                        return Err(paro_error::serialization_error("Truncated varchar length"));
                    }
                    let len =
                        u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
                    *offset += 4;

                    if len == 0 {
                        if !vector.is_null(i) {
                            vector.set_string(i, "");
                        }
                        continue;
                    }

                    if *offset + len > data.len() {
                        return Err(paro_error::serialization_error("Truncated varchar data"));
                    }
                    let s =
                        String::from_utf8(data[*offset..*offset + len].to_vec()).map_err(|e| {
                            paro_error::serialization_error(format!("Invalid UTF-8: {}", e))
                        })?;
                    vector.set_string(i, &s);
                    *offset += len;
                }
            }
            LogicalType::Blob => {
                for i in 0..count {
                    if *offset + 4 > data.len() {
                        return Err(paro_error::serialization_error("Truncated blob length"));
                    }
                    let len =
                        u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
                    *offset += 4;

                    if len == 0 {
                        if !vector.is_null(i) {
                            vector.set_blob(i, &[]);
                        }
                        continue;
                    }

                    if *offset + len > data.len() {
                        return Err(paro_error::serialization_error("Truncated blob data"));
                    }
                    let bytes = &data[*offset..*offset + len];
                    vector.set_blob(i, bytes);
                    *offset += len;
                }
            }
            LogicalType::Array(child_type, array_size) => {
                // For Array types, deserialize the child vector
                if let Some(child) = vector.child_mut() {
                    let child_mut = std::sync::Arc::make_mut(child);
                    let child_count = count * array_size;
                    Self::deserialize_vector(data, offset, child_mut, child_count, child_type)?;
                }
            }
            LogicalType::List(child_type) => {
                let entry_bytes = count * 2 * std::mem::size_of::<u32>();
                if *offset + entry_bytes > data.len() {
                    return Err(paro_error::serialization_error("Truncated list entry data"));
                }
                if entry_bytes > 0 {
                    unsafe {
                        let dest = vector.flat_data_mut::<u8>();
                        std::ptr::copy_nonoverlapping(data[*offset..].as_ptr(), dest, entry_bytes);
                    }
                }
                *offset += entry_bytes;

                let mut child_count = 0usize;
                for i in 0..count {
                    let entry_ptr = unsafe { vector.flat_data::<u8>().add(i * 8) as *const u32 };
                    let off = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
                    let len = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };
                    child_count = child_count.max(off + len);
                }

                if let Some(child) = vector.child_mut() {
                    let child_mut = std::sync::Arc::make_mut(child);
                    Self::deserialize_vector(data, offset, child_mut, child_count, child_type)?;
                }
            }
            LogicalType::Struct(fields) => {
                let children = vector
                    .children_mut()
                    .ok_or_else(|| paro_error::internal("Struct vector missing children"))?;
                if children.len() != fields.len() {
                    return Err(paro_error::internal(
                        "Struct child count mismatch during WAL deserialization",
                    ));
                }
                for (idx, (_name, field_type)) in fields.iter().enumerate() {
                    let child_mut = Arc::make_mut(&mut children[idx]);
                    Self::deserialize_vector(data, offset, child_mut, count, field_type)?;
                }
            }
            _ => {
                // For unsupported types, read raw bytes based on physical size
                let type_size = logical_type.physical_size();
                if type_size > 0 {
                    let total_bytes = count * type_size;
                    if *offset + total_bytes > data.len() {
                        return Err(paro_error::serialization_error("Truncated raw data"));
                    }
                    // Copy raw bytes to vector buffer
                    unsafe {
                        let dest = vector.flat_data_mut::<u8>();
                        std::ptr::copy_nonoverlapping(data[*offset..].as_ptr(), dest, total_bytes);
                    }
                    *offset += total_bytes;
                }
            }
        }

        vector.set_len(count);
        Ok(())
    }

    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Row count
        bytes.extend_from_slice(&self.row_count.to_le_bytes());

        // Column count and types (with extended type info for Array)
        bytes.extend_from_slice(&(self.column_types.len() as u32).to_le_bytes());
        for col_type in &self.column_types {
            Self::serialize_logical_type(&mut bytes, col_type);
        }

        // Data length and data
        bytes.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.data);

        bytes
    }

    /// Serialize a LogicalType with extended info for nested types.
    pub fn serialize_logical_type(bytes: &mut Vec<u8>, logical_type: &LogicalType) {
        bytes.push(logical_type.type_id());

        // For Array types, also serialize child type and size
        if let LogicalType::Array(child_type, array_size) = logical_type {
            bytes.extend_from_slice(&(*array_size as u32).to_le_bytes());
            Self::serialize_logical_type(bytes, child_type);
        }

        // For Decimal types, serialize precision and scale
        if let LogicalType::Decimal { precision, scale } = logical_type {
            bytes.push(*precision);
            bytes.push(*scale);
        }

        // For List types, serialize child type
        if let LogicalType::List(child_type) = logical_type {
            Self::serialize_logical_type(bytes, child_type);
        }

        // For Struct types, serialize field names and types
        if let LogicalType::Struct(fields) = logical_type {
            bytes.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for (name, field_type) in fields {
                let name_bytes = name.as_bytes();
                bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(name_bytes);
                Self::serialize_logical_type(bytes, field_type);
            }
        }
    }

    /// Deserialize from bytes.
    pub fn deserialize(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        // Read row count
        if *offset + 8 > bytes.len() {
            return Err(paro_error::serialization_error("Truncated row count"));
        }
        let row_count = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
        *offset += 8;

        // Read column count
        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error("Truncated column count"));
        }
        let col_count =
            u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        // Read column types (with extended info)
        let mut column_types = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let logical_type = Self::deserialize_logical_type(bytes, offset)?;
            column_types.push(logical_type);
        }

        // Read data
        if *offset + 4 > bytes.len() {
            return Err(paro_error::serialization_error("Truncated data length"));
        }
        let data_len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        if *offset + data_len > bytes.len() {
            return Err(paro_error::serialization_error("Truncated data"));
        }
        let data = bytes[*offset..*offset + data_len].to_vec();
        *offset += data_len;

        Ok(Self {
            row_count,
            column_types,
            data,
        })
    }

    /// Deserialize a LogicalType with extended info for nested types.
    pub fn deserialize_logical_type(bytes: &[u8], offset: &mut usize) -> Result<LogicalType> {
        if *offset >= bytes.len() {
            return Err(paro_error::serialization_error("Truncated type ID"));
        }
        let type_id = bytes[*offset];
        *offset += 1;

        // Handle compound types that need extended info
        match type_id {
            // Decimal type (type_id = 16)
            16 => {
                if *offset + 2 > bytes.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated decimal precision/scale",
                    ));
                }
                let precision = bytes[*offset];
                let scale = bytes[*offset + 1];
                *offset += 2;

                Ok(LogicalType::Decimal { precision, scale })
            }
            // Array type (type_id = 17)
            17 => {
                if *offset + 4 > bytes.len() {
                    return Err(paro_error::serialization_error("Truncated array size"));
                }
                let array_size =
                    u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
                *offset += 4;

                let child_type = Self::deserialize_logical_type(bytes, offset)?;
                Ok(LogicalType::Array(Box::new(child_type), array_size))
            }
            // List type (type_id = 18)
            18 => {
                let child_type = Self::deserialize_logical_type(bytes, offset)?;
                Ok(LogicalType::List(Box::new(child_type)))
            }
            // Struct type (type_id = 19)
            19 => {
                if *offset + 4 > bytes.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated struct field count",
                    ));
                }
                let field_count =
                    u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
                *offset += 4;

                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    if *offset + 4 > bytes.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated struct field name length",
                        ));
                    }
                    let name_len =
                        u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap())
                            as usize;
                    *offset += 4;
                    if *offset + name_len > bytes.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated struct field name",
                        ));
                    }
                    let name = String::from_utf8(bytes[*offset..*offset + name_len].to_vec())
                        .map_err(|e| {
                            paro_error::serialization_error(format!(
                                "Invalid struct field name UTF-8: {}",
                                e
                            ))
                        })?;
                    *offset += name_len;

                    let field_type = Self::deserialize_logical_type(bytes, offset)?;
                    fields.push((name, field_type));
                }

                Ok(LogicalType::Struct(fields))
            }
            // Simple types
            _ => LogicalType::from_type_id(type_id),
        }
    }
}

impl WalEntry {
    /// Get the WAL type for this entry.
    pub fn wal_type(&self) -> WalType {
        match self {
            WalEntry::Version { .. } => WalType::WalVersion,
            WalEntry::JournalRecord { .. } => WalType::JournalRecord,
            WalEntry::TxnBegin { .. } => WalType::TxnBegin,
            WalEntry::TxnCatalogOp { .. } => WalType::TxnCatalogOp,
            WalEntry::TxnDataOp { .. } => WalType::TxnDataOp,
            WalEntry::TxnPostCommitHook { .. } => WalType::TxnPostCommitHook,
            WalEntry::TxnCommit { .. } => WalType::TxnCommit,
            WalEntry::TxnAbort { .. } => WalType::TxnAbort,
            WalEntry::PrimaryDelete { .. } => WalType::PrimaryDelete,
            WalEntry::RowIdDelete { .. } => WalType::RowIdDelete,
            WalEntry::RowsetCommit { .. } => WalType::RowsetCommit,
            WalEntry::CompactionPublish { .. } => WalType::CompactionPublish,
            WalEntry::Flush => WalType::WalFlush,
        }
    }

    /// Serialize the entry data (excluding type byte).
    pub fn serialize_data(&self) -> Vec<u8> {
        match self {
            WalEntry::Version { version } => version.to_le_bytes().to_vec(),

            WalEntry::JournalRecord { lsn, record } => {
                encode_record(record, *lsn).expect("journal record serialization")
            }

            WalEntry::TxnBegin { txn_id, start_time } => TxnRecord::Begin {
                txn_id: *txn_id,
                start_time: *start_time,
            }
            .serialize_data()
            .expect("txn begin serialization"),

            WalEntry::TxnCatalogOp { seq, op } => TxnRecord::CatalogOp {
                seq: *seq,
                op: op.clone(),
            }
            .serialize_data()
            .expect("txn catalog op serialization"),

            WalEntry::TxnDataOp { seq, op } => TxnRecord::DataOp {
                seq: *seq,
                op: op.clone(),
            }
            .serialize_data()
            .expect("txn data op serialization"),

            WalEntry::TxnPostCommitHook { seq, hook } => TxnRecord::PostCommitHook {
                seq: *seq,
                hook: hook.clone(),
            }
            .serialize_data()
            .expect("txn post-commit hook serialization"),

            WalEntry::TxnCommit { txn_id, commit_id } => TxnRecord::Commit {
                txn_id: *txn_id,
                commit_id: *commit_id,
            }
            .serialize_data()
            .expect("txn commit serialization"),

            WalEntry::TxnAbort { txn_id } => TxnRecord::Abort { txn_id: *txn_id }
                .serialize_data()
                .expect("txn abort serialization"),

            WalEntry::PrimaryDelete { keys } => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    bytes.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(k);
                }
                bytes
            }

            WalEntry::RowIdDelete { locations } => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&(locations.len() as u32).to_le_bytes());
                for (rowset_id, segment_id, row_id) in locations {
                    bytes.extend_from_slice(&rowset_id.to_le_bytes());
                    bytes.extend_from_slice(&segment_id.to_le_bytes());
                    bytes.extend_from_slice(&row_id.to_le_bytes());
                }
                bytes
            }

            WalEntry::RowsetCommit {
                tablet_id,
                rowset_id,
                start_version,
                end_version,
                rowset_path,
            } => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&tablet_id.to_le_bytes());
                bytes.extend_from_slice(&rowset_id.to_le_bytes());
                bytes.extend_from_slice(&start_version.to_le_bytes());
                bytes.extend_from_slice(&end_version.to_le_bytes());

                let path_bytes = rowset_path.as_bytes();
                bytes.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(path_bytes);
                bytes
            }

            WalEntry::CompactionPublish {
                tablet_id,
                plan_id,
                job_id,
                output_rowset_id,
                output_start_version,
                output_end_version,
                cumulative_point_action,
                output_rowset_path,
                replaced_inputs,
            } => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&tablet_id.to_le_bytes());
                bytes.extend_from_slice(&plan_id.to_le_bytes());
                bytes.extend_from_slice(&job_id.to_le_bytes());
                bytes.extend_from_slice(&output_rowset_id.to_le_bytes());
                bytes.extend_from_slice(&output_start_version.to_le_bytes());
                bytes.extend_from_slice(&output_end_version.to_le_bytes());
                bytes.push(encode_cumulative_point_action(*cumulative_point_action));
                let path_bytes = output_rowset_path.as_bytes();
                bytes.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(path_bytes);
                bytes.extend_from_slice(&(replaced_inputs.len() as u32).to_le_bytes());
                for input_rowset_id in replaced_inputs {
                    bytes.extend_from_slice(&input_rowset_id.to_le_bytes());
                }
                bytes
            }

            WalEntry::Flush => Vec::new(),
        }
    }

    /// Deserialize an entry from bytes (including type byte).
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(paro_error::serialization_error("Empty WAL entry"));
        }

        let wal_type = WalType::try_from(bytes[0])?;
        let data = &bytes[1..];
        let mut offset = 0;

        match wal_type {
            WalType::WalVersion => {
                if data.len() < 8 {
                    return Err(paro_error::serialization_error("Truncated version entry"));
                }
                let version = u64::from_le_bytes(data[0..8].try_into().unwrap());
                Ok(WalEntry::Version { version })
            }

            WalType::JournalRecord => {
                let frame = decode_frame(data)?;
                Ok(WalEntry::JournalRecord {
                    lsn: frame.header.lsn,
                    record: frame.record,
                })
            }

            WalType::TxnBegin => match TxnRecord::deserialize_data(data)? {
                TxnRecord::Begin { txn_id, start_time } => {
                    Ok(WalEntry::TxnBegin { txn_id, start_time })
                }
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::Begin, got {other:?}"
                ))),
            },

            WalType::TxnCatalogOp => match TxnRecord::deserialize_data(data)? {
                TxnRecord::CatalogOp { seq, op } => Ok(WalEntry::TxnCatalogOp { seq, op }),
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::CatalogOp, got {other:?}"
                ))),
            },

            WalType::TxnDataOp => match TxnRecord::deserialize_data(data)? {
                TxnRecord::DataOp { seq, op } => Ok(WalEntry::TxnDataOp { seq, op }),
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::DataOp, got {other:?}"
                ))),
            },

            WalType::TxnPostCommitHook => match TxnRecord::deserialize_data(data)? {
                TxnRecord::PostCommitHook { seq, hook } => {
                    Ok(WalEntry::TxnPostCommitHook { seq, hook })
                }
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::PostCommitHook, got {other:?}"
                ))),
            },

            WalType::TxnCommit => match TxnRecord::deserialize_data(data)? {
                TxnRecord::Commit { txn_id, commit_id } => {
                    Ok(WalEntry::TxnCommit { txn_id, commit_id })
                }
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::Commit, got {other:?}"
                ))),
            },

            WalType::TxnAbort => match TxnRecord::deserialize_data(data)? {
                TxnRecord::Abort { txn_id } => Ok(WalEntry::TxnAbort { txn_id }),
                other => Err(paro_error::serialization_error(format!(
                    "expected TxnRecord::Abort, got {other:?}"
                ))),
            },

            WalType::PrimaryDelete => {
                if offset + 4 > data.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated primary delete count",
                    ));
                }
                let count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    if offset + 4 > data.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated primary delete key len",
                        ));
                    }
                    let klen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + klen > data.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated primary delete key bytes",
                        ));
                    }
                    keys.push(data[offset..offset + klen].to_vec());
                    offset += klen;
                }
                Ok(WalEntry::PrimaryDelete { keys })
            }

            WalType::RowIdDelete => {
                if offset + 4 > data.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated row-id delete location count",
                    ));
                }
                let count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut locations = Vec::with_capacity(count);
                for _ in 0..count {
                    if offset + 8 + 4 + 4 > data.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated row-id delete location",
                        ));
                    }
                    let rowset_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let segment_id =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let row_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    locations.push((rowset_id, segment_id, row_id));
                }
                Ok(WalEntry::RowIdDelete { locations })
            }

            WalType::RowsetCommit => {
                if offset + 8 + 8 + 8 + 8 > data.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated RowsetCommit entry",
                    ));
                }
                let tablet_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let rowset_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let start_version =
                    i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let end_version = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;

                let rowset_path = read_string(data, &mut offset)?;

                Ok(WalEntry::RowsetCommit {
                    tablet_id,
                    rowset_id,
                    start_version,
                    end_version,
                    rowset_path,
                })
            }

            WalType::CompactionPublish => {
                if offset + (8 * 6) + 1 > data.len() {
                    return Err(paro_error::serialization_error(
                        "Truncated CompactionPublish entry",
                    ));
                }
                let tablet_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let plan_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let job_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let output_rowset_id =
                    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let output_start_version =
                    i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let output_end_version =
                    i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let cumulative_point_action =
                    decode_cumulative_point_action(read_u8(data, &mut offset)?)?;
                let output_rowset_path = read_string(data, &mut offset)?;
                let input_count = read_u32(data, &mut offset)? as usize;
                let mut replaced_inputs = Vec::with_capacity(input_count);
                for _ in 0..input_count {
                    if offset + 8 > data.len() {
                        return Err(paro_error::serialization_error(
                            "Truncated CompactionPublish input rowset id",
                        ));
                    }
                    replaced_inputs.push(u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    ));
                    offset += 8;
                }
                Ok(WalEntry::CompactionPublish {
                    tablet_id,
                    plan_id,
                    job_id,
                    output_rowset_id,
                    output_start_version,
                    output_end_version,
                    cumulative_point_action,
                    output_rowset_path,
                    replaced_inputs,
                })
            }

            WalType::WalFlush => Ok(WalEntry::Flush),

            _ => Err(paro_error::serialization_error(format!(
                "Unsupported WAL type for deserialization: {:?}",
                wal_type
            ))),
        }
    }
}

/// Helper function to read a length-prefixed string.
fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8> {
    if *offset + 1 > data.len() {
        return Err(paro_error::serialization_error("Truncated u8 value"));
    }
    let value = data[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > data.len() {
        return Err(paro_error::serialization_error("Truncated u32 value"));
    }
    let value = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(value)
}

fn encode_cumulative_point_action(action: CompactionCumulativePointAction) -> u8 {
    match action {
        CompactionCumulativePointAction::Preserve => 0,
        CompactionCumulativePointAction::AdvanceToOutputEndExclusive => 1,
    }
}

fn decode_cumulative_point_action(value: u8) -> Result<CompactionCumulativePointAction> {
    match value {
        0 => Ok(CompactionCumulativePointAction::Preserve),
        1 => Ok(CompactionCumulativePointAction::AdvanceToOutputEndExclusive),
        _ => Err(paro_error::serialization_error(format!(
            "Invalid cumulative point action code: {}",
            value
        ))),
    }
}

fn read_string(data: &[u8], offset: &mut usize) -> Result<String> {
    if *offset + 4 > data.len() {
        return Err(paro_error::serialization_error("Truncated string length"));
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;

    if *offset + len > data.len() {
        return Err(paro_error::serialization_error("Truncated string data"));
    }
    let s = String::from_utf8(data[*offset..*offset + len].to_vec())
        .map_err(|e| paro_error::serialization_error(format!("Invalid UTF-8 string: {}", e)))?;
    *offset += len;

    Ok(s)
}

fn read_optional_string(data: &[u8], offset: &mut usize) -> Result<Option<String>> {
    if *offset + 1 > data.len() {
        return Err(paro_error::serialization_error(
            "Truncated optional string marker",
        ));
    }

    let has_value = data[*offset] != 0;
    *offset += 1;
    if !has_value {
        return Ok(None);
    }

    read_string(data, offset).map(Some)
}

fn read_optional_u32_vec(data: &[u8], offset: &mut usize) -> Result<Option<Vec<u32>>> {
    if *offset + 1 > data.len() {
        return Err(paro_error::serialization_error(
            "Truncated optional u32 vector marker",
        ));
    }
    let has_value = data[*offset] != 0;
    *offset += 1;
    if !has_value {
        return Ok(None);
    }

    if *offset + 4 > data.len() {
        return Err(paro_error::serialization_error(
            "Truncated optional u32 vector count",
        ));
    }
    let count = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;

    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if *offset + 4 > data.len() {
            return Err(paro_error::serialization_error(
                "Truncated optional u32 vector value",
            ));
        }
        values.push(u32::from_le_bytes(
            data[*offset..*offset + 4].try_into().unwrap(),
        ));
        *offset += 4;
    }

    Ok(Some(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::test_utils::*;

    #[test]
    fn test_entry_header_roundtrip() {
        let header = WalEntryHeader::new(1024, 0xDEADBEEF);
        let bytes = header.to_bytes();
        let recovered = WalEntryHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.size, recovered.size);
        assert_eq!(header.checksum, recovered.checksum);
    }

    #[test]
    fn test_txn_begin_roundtrip() {
        let entry = WalEntry::TxnBegin {
            txn_id: 9,
            start_time: 42,
        };
        let mut bytes = vec![entry.wal_type() as u8];
        bytes.extend_from_slice(&entry.serialize_data());
        let recovered = WalEntry::deserialize(&bytes).unwrap();
        assert!(matches!(
            recovered,
            WalEntry::TxnBegin {
                txn_id: 9,
                start_time: 42
            }
        ));
    }

    #[test]
    fn test_row_id_delete_roundtrip() {
        let entry = WalEntry::RowIdDelete {
            locations: vec![(11, 0, 7), (11, 0, 9), (42, 3, 1024)],
        };

        let mut bytes = vec![entry.wal_type() as u8];
        bytes.extend_from_slice(&entry.serialize_data());

        let recovered = WalEntry::deserialize(&bytes).unwrap();
        match recovered {
            WalEntry::RowIdDelete { locations } => {
                assert_eq!(locations, vec![(11, 0, 7), (11, 0, 9), (42, 3, 1024)]);
            }
            _ => panic!("Wrong entry type"),
        }
    }

    #[test]
    fn test_serialized_data_chunk_integer_roundtrip() {
        // Create a Chunk with integer values
        let vec = test_i32_vector(&[1, 2, 3, 4, 5]);
        let chunk = test_chunk_from_vectors(vec![vec]);

        // Serialize
        let serialized = SerializedDataChunk::from_chunk(&chunk).unwrap();
        assert_eq!(serialized.row_count, 5);
        assert_eq!(serialized.column_types.len(), 1);

        // Deserialize
        let recovered = serialized.to_chunk().unwrap();
        assert_eq!(recovered.size(), 5);
        assert_eq!(recovered.column_count(), 1);

        // Verify values
        let col = recovered.column(0).unwrap();
        for i in 0..5 {
            let val: i32 = unsafe { col.get_flat(i) };
            assert_eq!(val, (i + 1) as i32);
        }
    }

    #[test]
    fn test_serialized_data_chunk_multiple_columns() {
        // Create a Chunk with multiple columns
        let int_vec = test_i32_vector(&[10, 20, 30]);
        let float_vec = test_f64_vector(&[1.5, 2.5, 3.5]);
        let chunk = test_chunk_from_vectors(vec![int_vec, float_vec]);

        // Serialize
        let serialized = SerializedDataChunk::from_chunk(&chunk).unwrap();
        assert_eq!(serialized.row_count, 3);
        assert_eq!(serialized.column_types.len(), 2);

        // Deserialize
        let recovered = serialized.to_chunk().unwrap();
        assert_eq!(recovered.size(), 3);
        assert_eq!(recovered.column_count(), 2);

        // Verify integer column
        let int_col = recovered.column(0).unwrap();
        assert_eq!(unsafe { int_col.get_flat::<i32>(0) }, 10);
        assert_eq!(unsafe { int_col.get_flat::<i32>(1) }, 20);
        assert_eq!(unsafe { int_col.get_flat::<i32>(2) }, 30);

        // Verify float column
        let float_col = recovered.column(1).unwrap();
        assert!((unsafe { float_col.get_flat::<f64>(0) } - 1.5).abs() < 0.001);
        assert!((unsafe { float_col.get_flat::<f64>(1) } - 2.5).abs() < 0.001);
        assert!((unsafe { float_col.get_flat::<f64>(2) } - 3.5).abs() < 0.001);
    }

    #[test]
    fn test_serialized_data_chunk_constant_vector_roundtrip() {
        use paro_common::types::LogicalType;

        let chunk =
            test_chunk_from_vectors(vec![test_constant::<u32>(LogicalType::UInteger, 42, 3)]);

        let serialized = SerializedDataChunk::from_chunk(&chunk).unwrap();
        let recovered = serialized.to_chunk().unwrap();
        let col = recovered.column(0).unwrap();

        assert_eq!(recovered.size(), 3);
        for row_idx in 0..3 {
            assert_eq!(col.get_u32(row_idx), Some(42));
        }
    }

    #[test]
    fn test_serialized_data_chunk_binary_roundtrip() {
        // Create a Chunk
        let vec = test_i64_vector(&[100, 200, 300]);
        let chunk = test_chunk_from_vectors(vec![vec]);

        // Serialize to SerializedDataChunk
        let serialized = SerializedDataChunk::from_chunk(&chunk).unwrap();

        // Serialize to bytes
        let bytes = serialized.serialize();

        // Deserialize from bytes
        let mut offset = 0;
        let recovered_serialized = SerializedDataChunk::deserialize(&bytes, &mut offset).unwrap();

        // Convert back to Chunk
        let recovered = recovered_serialized.to_chunk().unwrap();

        assert_eq!(recovered.size(), 3);
        let col = recovered.column(0).unwrap();
        assert_eq!(unsafe { col.get_flat::<i64>(0) }, 100);
        assert_eq!(unsafe { col.get_flat::<i64>(1) }, 200);
        assert_eq!(unsafe { col.get_flat::<i64>(2) }, 300);
    }
}
