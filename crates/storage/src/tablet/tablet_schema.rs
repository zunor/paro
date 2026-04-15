//! # TabletSchema
//!
//! Schema definition for Tablet columns.
//!
//! ## Key Design
//!
//! - Defines column metadata including type, encoding, compression
//! - Supports PRIMARY_KEYS model for upsert operations
//! - Tracks key columns and sort key columns separately
//! - Provides serialization for persistence

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;

/// Column ID type (unique within a tablet)
pub type ColumnId = u32;

/// Keys type for the tablet data model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum KeysType {
    /// Primary key model - supports upsert, delete by primary key
    /// Each row is uniquely identified by primary key columns
    #[default]
    PrimaryKeys = 0,

    /// Duplicate key model - allows duplicate keys
    /// All rows are stored, no deduplication
    DuplicateKeys = 1,

    /// Unique key model - similar to primary keys but with different merge semantics
    UniqueKeys = 2,

    /// Aggregate key model - rows with same key are aggregated
    AggregateKeys = 3,
}

impl std::fmt::Display for KeysType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeysType::PrimaryKeys => write!(f, "PRIMARY_KEYS"),
            KeysType::DuplicateKeys => write!(f, "DUP_KEYS"),
            KeysType::UniqueKeys => write!(f, "UNIQUE_KEYS"),
            KeysType::AggregateKeys => write!(f, "AGG_KEYS"),
        }
    }
}

/// Column definition within a TabletSchema.
#[derive(Debug, Clone)]
pub struct TabletColumn {
    /// Unique column ID within the tablet
    pub id: ColumnId,

    /// Column name
    pub name: String,

    /// Logical type of the column
    pub logical_type: LogicalType,

    /// Whether this column is a key column
    pub is_key: bool,

    /// Whether this column is nullable
    pub is_nullable: bool,

    /// Whether this column has a default value
    pub has_default_value: bool,

    /// Default value (serialized)
    pub default_value: Option<Vec<u8>>,

    /// Column length (for VARCHAR, CHAR types)
    pub length: u32,

    /// Precision (for DECIMAL types)
    pub precision: u32,

    /// Scale (for DECIMAL types)
    pub scale: u32,

    /// Aggregation type (for AGG_KEYS model)
    pub aggregation_type: Option<String>,

    /// Whether to build HNSW index for this column
    pub index_hnsw: bool,

    /// HNSW m parameter
    pub hnsw_m: usize,

    /// HNSW ef_construct parameter
    pub hnsw_ef_construct: usize,

    /// HNSW distance metric (0=Euclidean, 1=Cosine, 2=DotProduct, 3=Manhattan)
    pub hnsw_distance: u8,
}

impl TabletColumn {
    /// Create a new TabletColumn
    pub fn new(id: ColumnId, name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            id,
            name: name.into(),
            logical_type,
            is_key: false,
            is_nullable: true,
            has_default_value: false,
            default_value: None,
            length: 0,
            precision: 0,
            scale: 0,
            aggregation_type: None,
            index_hnsw: false,
            hnsw_m: 16,
            hnsw_ef_construct: 100,
            hnsw_distance: 0,
        }
    }

    /// Create a key column
    pub fn key(id: ColumnId, name: impl Into<String>, logical_type: LogicalType) -> Self {
        let mut col = Self::new(id, name, logical_type);
        col.is_key = true;
        col.is_nullable = false; // Key columns are typically not nullable
        col
    }

    /// Set nullable flag
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.is_nullable = nullable;
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: Vec<u8>) -> Self {
        self.has_default_value = true;
        self.default_value = Some(default);
        self
    }

    /// Set length (for string types)
    pub fn with_length(mut self, length: u32) -> Self {
        self.length = length;
        self
    }

    /// Set precision and scale (for decimal types)
    pub fn with_precision_scale(mut self, precision: u32, scale: u32) -> Self {
        self.precision = precision;
        self.scale = scale;
        self
    }

    /// Get the type size in bytes (for fixed-size types)
    pub fn type_size(&self) -> usize {
        self.logical_type.type_size()
    }

    /// Set HNSW index parameters
    pub fn with_hnsw_index(mut self, m: usize, ef_construct: usize, distance: u8) -> Self {
        self.index_hnsw = true;
        self.hnsw_m = m;
        self.hnsw_ef_construct = ef_construct;
        self.hnsw_distance = distance;
        self
    }
}

/// TabletSchema defines the schema for a Tablet.
#[derive(Debug, Clone)]
pub struct TabletSchema {
    /// Stable schema ID shared across tablets.
    schema_id: u64,

    /// Schema version under the same schema ID.
    schema_version: u32,

    /// All columns in the schema
    columns: Vec<TabletColumn>,

    /// Number of key columns (first N columns are keys)
    num_key_columns: usize,

    /// Keys type (PRIMARY_KEYS, DUP_KEYS, etc.)
    keys_type: KeysType,

    /// Sort key column indices (for ordering within segments)
    /// If empty, defaults to key columns
    sort_key_idxes: Vec<usize>,

    /// Number of short key columns (for short key index)
    num_short_key_columns: usize,

    /// Next column unique ID to assign
    next_column_unique_id: u32,

    /// Column name to index mapping (cached)
    name_to_index: HashMap<String, usize>,
}

impl TabletSchema {
    /// Create a new TabletSchema
    ///
    /// # Arguments
    /// * `schema_id` - Stable schema identifier
    /// * `columns` - Column definitions
    /// * `keys_type` - Data model type
    ///
    /// # Returns
    /// A new TabletSchema, or error if validation fails
    pub fn new(schema_id: u64, columns: Vec<TabletColumn>, keys_type: KeysType) -> Result<Self> {
        Self::with_version(schema_id, 1, columns, keys_type)
    }

    /// Create a schema with explicit schema version.
    pub fn with_version(
        schema_id: u64,
        schema_version: u32,
        columns: Vec<TabletColumn>,
        keys_type: KeysType,
    ) -> Result<Self> {
        if columns.is_empty() {
            return Err(paro_error::invalid_input(
                "Schema must have at least one column",
            ));
        }

        if schema_version == 0 {
            return Err(paro_error::invalid_input(
                "schema_version must be greater than 0",
            ));
        }

        // Count key columns (must be contiguous at the beginning)
        let num_key_columns = columns.iter().take_while(|c| c.is_key).count();

        // For PRIMARY_KEYS, must have at least one key column
        if keys_type == KeysType::PrimaryKeys && num_key_columns == 0 {
            return Err(paro_error::invalid_input(
                "PRIMARY_KEYS model requires at least one key column",
            ));
        }

        // Build name to index mapping
        let mut name_to_index = HashMap::new();
        for (idx, col) in columns.iter().enumerate() {
            if name_to_index.contains_key(&col.name) {
                return Err(paro_error::invalid_input(format!(
                    "Duplicate column name: {}",
                    col.name
                )));
            }
            name_to_index.insert(col.name.clone(), idx);
        }

        // Default sort key is the key columns
        let sort_key_idxes: Vec<usize> = (0..num_key_columns).collect();

        // Default short key columns (typically 3 or fewer)
        let num_short_key_columns = num_key_columns.min(3);

        // Find max column ID for next_column_unique_id
        let next_column_unique_id = columns.iter().map(|c| c.id).max().unwrap_or(0) + 1;

        Ok(Self {
            schema_id,
            schema_version,
            columns,
            num_key_columns,
            keys_type,
            sort_key_idxes,
            num_short_key_columns,
            next_column_unique_id,
            name_to_index,
        })
    }

    /// Create a schema from LogicalTypes (convenience method)
    ///
    /// Creates a simple schema with auto-generated column names and no keys.
    /// Useful for testing or simple tables.
    pub fn from_types(schema_id: u64, types: &[LogicalType]) -> Result<Self> {
        let columns: Vec<TabletColumn> = types
            .iter()
            .enumerate()
            .map(|(i, t)| TabletColumn::new(i as ColumnId, format!("col_{}", i), t.clone()))
            .collect();

        Self::new(schema_id, columns, KeysType::DuplicateKeys)
    }

    /// Legacy schema ID accessor.
    ///
    /// Prefer `schema_id()` for new code.
    pub fn id(&self) -> i64 {
        self.schema_id.min(i64::MAX as u64) as i64
    }

    /// Get stable schema identifier.
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// Get schema version.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Get number of columns
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get number of key columns
    pub fn num_key_columns(&self) -> usize {
        self.num_key_columns
    }

    /// Get keys type
    pub fn keys_type(&self) -> KeysType {
        self.keys_type
    }

    /// Get column by index
    pub fn column(&self, index: usize) -> Option<&TabletColumn> {
        self.columns.get(index)
    }

    /// Get column by ID
    pub fn column_by_id(&self, id: ColumnId) -> Option<&TabletColumn> {
        self.columns.iter().find(|c| c.id == id)
    }

    /// Get column by name
    pub fn column_by_name(&self, name: &str) -> Option<&TabletColumn> {
        self.name_to_index
            .get(name)
            .and_then(|&idx| self.columns.get(idx))
    }

    /// Get column index by name
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Get all columns
    pub fn columns(&self) -> &[TabletColumn] {
        &self.columns
    }

    /// Get key columns
    pub fn key_columns(&self) -> &[TabletColumn] {
        &self.columns[..self.num_key_columns]
    }

    /// Get value columns (non-key columns)
    pub fn value_columns(&self) -> &[TabletColumn] {
        &self.columns[self.num_key_columns..]
    }

    /// Get sort key column indices
    pub fn sort_key_idxes(&self) -> &[usize] {
        &self.sort_key_idxes
    }

    /// Set sort key column indices
    pub fn set_sort_key_idxes(&mut self, idxes: Vec<usize>) -> Result<()> {
        // Validate indices
        for &idx in &idxes {
            if idx >= self.columns.len() {
                return Err(paro_error::invalid_input(format!(
                    "Sort key index {} out of range (num_columns={})",
                    idx,
                    self.columns.len()
                )));
            }
        }
        self.sort_key_idxes = idxes;
        Ok(())
    }

    /// Get number of short key columns
    pub fn num_short_key_columns(&self) -> usize {
        self.num_short_key_columns
    }

    /// Set number of short key columns
    pub fn set_num_short_key_columns(&mut self, num: usize) {
        self.num_short_key_columns = num.min(self.num_key_columns);
    }

    /// Get next column unique ID
    pub fn next_column_unique_id(&self) -> u32 {
        self.next_column_unique_id
    }

    /// Set next column unique ID
    pub fn set_next_column_unique_id(&mut self, id: u32) {
        self.next_column_unique_id = id;
    }

    /// Get logical types for all columns
    pub fn logical_types(&self) -> Vec<LogicalType> {
        self.columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect()
    }

    /// Check if schema contains a column with the given name
    pub fn contains_column(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Serialize schema to bytes
    ///
    /// Binary format (little-endian):
    /// - schema_id: u64
    /// - keys_type: u8
    /// - num_columns: u32
    /// - num_short_key_columns: u32
    /// - next_column_unique_id: u32
    /// - sort_key_idxes_count: u32
    /// - sort_key_idxes: [u32]
    /// - columns: [TabletColumn]
    /// - schema_version: u32 (appended for backward compatibility)
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Schema ID
        data.extend_from_slice(&self.schema_id.to_le_bytes());

        // Keys type
        data.push(self.keys_type as u8);

        // Number of columns
        data.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // Short key columns
        data.extend_from_slice(&(self.num_short_key_columns as u32).to_le_bytes());

        // Next column unique ID
        data.extend_from_slice(&self.next_column_unique_id.to_le_bytes());

        // Sort key indices
        data.extend_from_slice(&(self.sort_key_idxes.len() as u32).to_le_bytes());
        for &idx in &self.sort_key_idxes {
            data.extend_from_slice(&(idx as u32).to_le_bytes());
        }

        // Columns
        for col in &self.columns {
            // Column ID
            data.extend_from_slice(&col.id.to_le_bytes());

            // Column name (length + bytes)
            let name_bytes = col.name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(name_bytes);

            // Logical type
            let type_id = col.logical_type.type_id();
            data.push(type_id);

            // Flags: is_key, is_nullable, has_default_value
            let flags = (col.is_key as u8)
                | ((col.is_nullable as u8) << 1)
                | ((col.has_default_value as u8) << 2);
            data.push(flags);

            // Column length, precision, scale
            data.extend_from_slice(&col.length.to_le_bytes());
            data.extend_from_slice(&col.precision.to_le_bytes());
            data.extend_from_slice(&col.scale.to_le_bytes());

            // Aggregation type (optional, length + bytes)
            if let Some(agg) = &col.aggregation_type {
                let agg_bytes = agg.as_bytes();
                data.extend_from_slice(&(agg_bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(agg_bytes);
            } else {
                data.extend_from_slice(&0u32.to_le_bytes());
            }

            // Default value (optional, length + bytes)
            if let Some(val) = &col.default_value {
                data.extend_from_slice(&(val.len() as u32).to_le_bytes());
                data.extend_from_slice(val);
            } else {
                data.extend_from_slice(&0u32.to_le_bytes());
            }

            // HNSW index info
            data.push(col.index_hnsw as u8);
            data.extend_from_slice(&(col.hnsw_m as u32).to_le_bytes());
            data.extend_from_slice(&(col.hnsw_ef_construct as u32).to_le_bytes());
            data.push(col.hnsw_distance);
        }

        // Appended schema version keeps old prefix layout intact.
        data.extend_from_slice(&self.schema_version.to_le_bytes());

        Ok(data)
    }

    /// Deserialize schema from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 21 {
            return Err(paro_error::internal("Invalid TabletSchema data: too short"));
        }

        let mut offset = 0;

        #[allow(unused_macros)]
        macro_rules! read_u64 {
            () => {{
                let val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                val
            }};
        }

        macro_rules! read_u32 {
            () => {{
                let val = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                val
            }};
        }

        macro_rules! read_u8 {
            () => {{
                let val = data[offset];
                offset += 1;
                val
            }};
        }

        // Schema ID
        let schema_id = read_u64!();

        // Keys type
        let keys_ty_byte = read_u8!();
        let keys_type = match keys_ty_byte {
            0 => KeysType::PrimaryKeys,
            1 => KeysType::DuplicateKeys,
            2 => KeysType::UniqueKeys,
            3 => KeysType::AggregateKeys,
            _ => {
                return Err(paro_error::internal(format!(
                    "Invalid KeysType: {}",
                    keys_ty_byte
                )));
            }
        };

        // Number of columns
        let num_columns = read_u32!() as usize;

        // Short key columns
        let num_short_key_columns = read_u32!() as usize;

        // Next column unique ID
        let next_column_unique_id = read_u32!();

        // Sort key indices
        let num_sort_keys = read_u32!() as usize;
        let mut sort_key_idxes = Vec::with_capacity(num_sort_keys);
        for _ in 0..num_sort_keys {
            sort_key_idxes.push(read_u32!() as usize);
        }

        // Columns
        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            // Column ID
            let col_id = read_u32!();

            // Column name
            let name_len = read_u32!() as usize;
            if offset + name_len > data.len() {
                return Err(paro_error::internal(
                    "Invalid TabletSchema data: truncated name",
                ));
            }
            let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
            offset += name_len;

            // Logical type
            let type_id = read_u8!();
            let logical_type = LogicalType::from_type_id(type_id).unwrap_or(LogicalType::Unknown);

            // Flags
            let flags = read_u8!();
            let is_key = (flags & 1) != 0;
            let is_nullable = (flags & 2) != 0;
            let has_default_value = (flags & 4) != 0;

            // Column length, precision, scale
            let length = read_u32!();
            let precision = read_u32!();
            let scale = read_u32!();

            let mut col = TabletColumn::new(col_id, name, logical_type);
            col.is_key = is_key;
            col.is_nullable = is_nullable;
            col.has_default_value = has_default_value;
            col.length = length;
            col.precision = precision;
            col.scale = scale;

            // Aggregation type
            let agg_len = read_u32!() as usize;
            if agg_len > 0 {
                if offset + agg_len > data.len() {
                    return Err(paro_error::internal(
                        "Invalid TabletSchema data: truncated aggregation type",
                    ));
                }
                col.aggregation_type =
                    Some(String::from_utf8_lossy(&data[offset..offset + agg_len]).to_string());
                offset += agg_len;
            }

            // Default value
            let val_len = read_u32!() as usize;
            if val_len > 0 {
                if offset + val_len > data.len() {
                    return Err(paro_error::internal(
                        "Invalid TabletSchema data: truncated default value",
                    ));
                }
                col.default_value = Some(data[offset..offset + val_len].to_vec());
                offset += val_len;
            }

            // HNSW index info
            if offset < data.len() {
                col.index_hnsw = read_u8!() != 0;
                col.hnsw_m = read_u32!() as usize;
                col.hnsw_ef_construct = read_u32!() as usize;
                if offset < data.len() {
                    col.hnsw_distance = read_u8!();
                }
            }

            columns.push(col);
        }

        // Old payloads do not carry schema_version.
        let schema_version = if offset + 4 <= data.len() {
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
        } else {
            1
        };

        let mut schema = Self::with_version(schema_id, schema_version, columns, keys_type)?;
        schema.num_short_key_columns = num_short_key_columns;
        schema.next_column_unique_id = next_column_unique_id;
        schema.sort_key_idxes = sort_key_idxes;
        Ok(schema)
    }
}

/// Shared pointer to TabletSchema (thread-safe)
pub type TabletSchemaRef = Arc<TabletSchema>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tablet_column_new() {
        let col = TabletColumn::new(0, "id", LogicalType::BigInt);
        assert_eq!(col.id, 0);
        assert_eq!(col.name, "id");
        assert!(!col.is_key);
        assert!(col.is_nullable);
    }

    #[test]
    fn test_tablet_column_key() {
        let col = TabletColumn::key(0, "pk", LogicalType::BigInt);
        assert!(col.is_key);
        assert!(!col.is_nullable);
    }

    #[test]
    fn test_tablet_schema_new() {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
            TabletColumn::new(2, "value", LogicalType::Integer),
        ];

        let schema = TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap();

        assert_eq!(schema.id(), 1);
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(schema.schema_version(), 1);
        assert_eq!(schema.num_columns(), 3);
        assert_eq!(schema.num_key_columns(), 1);
        assert_eq!(schema.keys_type(), KeysType::PrimaryKeys);
    }

    #[test]
    fn test_tablet_schema_column_access() {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
        ];

        let schema = TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap();

        assert!(schema.column(0).is_some());
        assert!(schema.column(2).is_none());

        assert!(schema.column_by_name("id").is_some());
        assert!(schema.column_by_name("unknown").is_none());

        assert_eq!(schema.field_index("name"), Some(1));
    }

    #[test]
    fn test_tablet_schema_from_types() {
        let types = vec![LogicalType::BigInt, LogicalType::Varchar];
        let schema = TabletSchema::from_types(1, &types).unwrap();

        assert_eq!(schema.num_columns(), 2);
        assert_eq!(schema.num_key_columns(), 0);
        assert_eq!(schema.keys_type(), KeysType::DuplicateKeys);
    }

    #[test]
    fn test_tablet_schema_serialize_deserialize() {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "data", LogicalType::Varchar),
        ];

        let schema = TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap();
        let bytes = schema.serialize().unwrap();
        let restored = TabletSchema::deserialize(&bytes).unwrap();

        assert_eq!(restored.id(), schema.id());
        assert_eq!(restored.schema_id(), schema.schema_id());
        assert_eq!(restored.schema_version(), schema.schema_version());
        assert_eq!(restored.num_columns(), schema.num_columns());
        assert_eq!(restored.field_index("id"), Some(0));
        assert_eq!(restored.field_index("data"), Some(1));
    }

    #[test]
    fn test_tablet_schema_serialize_deserialize_complex() {
        let mut col1 = TabletColumn::key(1, "pk", LogicalType::BigInt);
        col1 = col1.with_default(vec![1, 2, 3]);

        let mut col2 = TabletColumn::new(2, "val", LogicalType::Varchar);
        col2.length = 123;
        col2.aggregation_type = Some("SUM".to_string());

        let columns = vec![col1, col2];
        let mut schema = TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap();
        schema.set_sort_key_idxes(vec![0]).unwrap();
        schema.set_num_short_key_columns(1);

        let bytes = schema.serialize().unwrap();
        let restored = TabletSchema::deserialize(&bytes).unwrap();

        assert_eq!(restored.id(), 1);
        assert_eq!(restored.schema_id(), 1);
        assert_eq!(restored.schema_version(), 1);
        assert_eq!(restored.num_columns(), 2);
        assert_eq!(restored.num_short_key_columns(), 1);
        assert_eq!(restored.sort_key_idxes(), &[0]);

        let rcol1 = restored.column(0).unwrap();
        assert_eq!(rcol1.name, "pk");
        assert!(rcol1.is_key);
        assert!(rcol1.has_default_value);
        assert_eq!(rcol1.default_value.as_ref().unwrap(), &[1, 2, 3]);

        let rcol2 = restored.column(1).unwrap();
        assert_eq!(rcol2.name, "val");
        assert_eq!(rcol2.length, 123);
        assert_eq!(rcol2.aggregation_type.as_ref().unwrap(), "SUM");
    }

    #[test]
    fn test_tablet_schema_validation() {
        // Empty columns should fail
        let result = TabletSchema::new(1, vec![], KeysType::PrimaryKeys);
        assert!(result.is_err());

        // PRIMARY_KEYS without key columns should fail
        let columns = vec![TabletColumn::new(0, "data", LogicalType::Integer)];
        let result = TabletSchema::new(1, columns, KeysType::PrimaryKeys);
        assert!(result.is_err());

        // Duplicate column names should fail
        let columns = vec![
            TabletColumn::new(0, "col", LogicalType::Integer),
            TabletColumn::new(1, "col", LogicalType::Integer),
        ];
        let result = TabletSchema::new(1, columns, KeysType::DuplicateKeys);
        assert!(result.is_err());
    }

    #[test]
    fn test_tablet_schema_with_version() {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
        ];

        let schema = TabletSchema::with_version(88, 7, columns, KeysType::PrimaryKeys).unwrap();
        let restored = TabletSchema::deserialize(&schema.serialize().unwrap()).unwrap();

        assert_eq!(restored.schema_id(), 88);
        assert_eq!(restored.schema_version(), 7);
    }

    #[test]
    fn test_keys_type_display() {
        assert_eq!(format!("{}", KeysType::PrimaryKeys), "PRIMARY_KEYS");
        assert_eq!(format!("{}", KeysType::DuplicateKeys), "DUP_KEYS");
    }
}
