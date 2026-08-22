// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Index Catalog Entry
//!
//!
//! This module defines IndexCatalogEntry for index metadata.

use super::catalog_entry::{
    AlterInfo, CatalogEntry, CatalogObjectId, CatalogObjectRef, CatalogType, CreateInfo,
    DependencyList, DependencyType, InCatalogEntry, OnCreateConflict, SchemaEntryMeta,
    StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_storage::index::IndexConstraintType;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::{Arc, LazyLock, RwLock, Weak};

// --- Index Types ---

/// Physical column index (0-based position in table) (0-based position in table)
pub type ColumnId = u32;

/// Logical column index (for expression indexes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalIndex {
    pub index: ColumnId,
}

impl LogicalIndex {
    pub fn new(index: ColumnId) -> Self {
        Self { index }
    }
}

/// Index type enumeration.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexType {
    #[default]
    ART,
    HNSW,
    Sparse,
    FullText,
    BPlusTree,
    Hash,
    Custom,
}

impl IndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexType::ART => "ART",
            IndexType::HNSW => "HNSW",
            IndexType::Sparse => "SPARSE",
            IndexType::FullText => "FULLTEXT",
            IndexType::BPlusTree => "BTREE",
            IndexType::Hash => "HASH",
            IndexType::Custom => "CUSTOM",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ART" => IndexType::ART,
            "HNSW" => IndexType::HNSW,
            "SPARSE" | "SPARSE_VECTOR" => IndexType::Sparse,
            "FULLTEXT" | "FULL_TEXT" | "INVERTED" | "NGRAM" => IndexType::FullText,
            "BTREE" | "B+TREE" | "BPLUSTREE" => IndexType::BPlusTree,
            "HASH" => IndexType::Hash,
            _ => IndexType::Custom,
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            IndexType::ART => 0,
            IndexType::HNSW => 1,
            IndexType::Sparse => 2,
            IndexType::FullText => 3,
            IndexType::BPlusTree => 4,
            IndexType::Hash => 5,
            IndexType::Custom => 255,
        }
    }

    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0 => IndexType::ART,
            1 => IndexType::HNSW,
            2 => IndexType::Sparse,
            3 => IndexType::FullText,
            4 => IndexType::BPlusTree,
            5 => IndexType::Hash,
            _ => IndexType::Custom,
        }
    }

    /// Return true when the index currently supports metadata-only staged creation.
    pub fn supports_metadata_only_build(&self) -> bool {
        matches!(
            self,
            IndexType::ART | IndexType::HNSW | IndexType::Sparse | IndexType::FullText
        )
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Persistent build state for index metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexBuildState {
    #[default]
    Building,
    Ready,
    Failed,
}

impl IndexBuildState {
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Building => 0,
            Self::Ready => 1,
            Self::Failed => 2,
        }
    }

    pub fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Building),
            1 => Ok(Self::Ready),
            2 => Ok(Self::Failed),
            _ => Err(paro_error::serialization_error(format!(
                "Invalid index build state: {}",
                value
            ))),
        }
    }
}

/// Runtime coverage snapshot captured at index build/recovery validation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexCoverage {
    /// Visible table version used to validate index coverage.
    pub visible_version: i64,
    /// Number of visible segments at `visible_version`.
    pub visible_segment_count: u32,
    /// Number of segments that have index payloads.
    pub indexed_segment_count: u32,
}

impl IndexCoverage {
    pub fn from_counts(
        visible_version: i64,
        visible_segment_count: usize,
        indexed_segment_count: usize,
    ) -> Self {
        Self {
            visible_version,
            visible_segment_count: u32::try_from(visible_segment_count).unwrap_or(u32::MAX),
            indexed_segment_count: u32::try_from(indexed_segment_count).unwrap_or(u32::MAX),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.visible_segment_count == self.indexed_segment_count
    }

    pub fn matches_snapshot(&self, visible_version: i64, visible_segment_count: usize) -> bool {
        self.visible_version == visible_version
            && self.visible_segment_count
                == u32::try_from(visible_segment_count).unwrap_or(u32::MAX)
    }
}

/// Full-text index metadata needed for query-side index matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextIndexBinding {
    /// Physical column id in the table.
    pub column_id: LogicalIndex,
    /// Text search configuration, e.g. `simple`.
    pub config: String,
}

// ============================================================================
// CreateIndexInfo
// ============================================================================

/// Information needed to create an index.
///
#[derive(Debug, Clone)]
pub struct CreateIndexInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name the index is on
    pub table_name: String,
    /// Index name
    pub name: String,
    /// Column IDs to index
    pub column_ids: Vec<LogicalIndex>,
    /// Column types (for validation)
    pub column_types: Vec<LogicalType>,
    /// Index type
    pub index_type: IndexType,
    /// Constraint type
    pub constraint_type: IndexConstraintType,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// IF NOT EXISTS flag (legacy)
    pub if_not_exists: bool,
    /// Original SQL statement
    pub sql: Option<String>,
    /// Dependencies
    pub dependencies: DependencyList,
    /// Build lifecycle state persisted in catalog metadata
    pub build_state: IndexBuildState,
    /// Optional failure reason when build_state is Failed
    pub failure_reason: Option<String>,
    /// Optional full-text metadata used by planner/runtime matching.
    pub fulltext: Option<FullTextIndexBinding>,
    /// Provider-owned, persistent index configuration.
    ///
    /// This is the single source of truth for physical build and query policy.
    /// Runtime registration must not reconstruct it from table-column defaults.
    pub provider_config: Value,
    /// Optional coverage snapshot used to guard optimizer pushdown.
    pub coverage: Option<IndexCoverage>,
}

impl CreateIndexInfo {
    pub fn new(
        schema: String,
        table_name: String,
        name: String,
        column_ids: Vec<LogicalIndex>,
        column_types: Vec<LogicalType>,
    ) -> Self {
        Self {
            catalog: String::new(),
            schema,
            table_name,
            name,
            column_ids,
            column_types,
            index_type: IndexType::ART,
            constraint_type: IndexConstraintType::None,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            if_not_exists: false,
            sql: None,
            dependencies: DependencyList::new(),
            build_state: IndexBuildState::Building,
            failure_reason: None,
            fulltext: None,
            provider_config: Value::Object(Default::default()),
            coverage: None,
        }
    }

    pub fn with_catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn with_unique(mut self) -> Self {
        self.constraint_type = IndexConstraintType::Unique;
        self
    }

    pub fn with_primary(mut self) -> Self {
        self.constraint_type = IndexConstraintType::Primary;
        self
    }

    pub fn with_if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self.on_conflict = OnCreateConflict::IgnoreOnConflict;
        self
    }

    pub fn with_index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn with_sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    pub fn with_build_state(mut self, state: IndexBuildState) -> Self {
        self.build_state = state;
        self
    }

    pub fn with_failure_reason(mut self, reason: impl Into<String>) -> Self {
        self.build_state = IndexBuildState::Failed;
        self.failure_reason = Some(reason.into());
        self
    }

    pub fn clear_failure_reason(mut self) -> Self {
        self.failure_reason = None;
        self
    }

    pub fn with_fulltext_options(
        mut self,
        column_id: LogicalIndex,
        config: impl Into<String>,
    ) -> Self {
        self.fulltext = Some(FullTextIndexBinding {
            column_id,
            config: config.into(),
        });
        self
    }

    pub fn with_provider_config(mut self, provider_config: Value) -> Self {
        self.provider_config = provider_config;
        self
    }

    pub fn with_coverage(mut self, coverage: IndexCoverage) -> Self {
        self.coverage = Some(coverage);
        self
    }

    pub fn clear_coverage(mut self) -> Self {
        self.coverage = None;
        self
    }

    pub fn is_unique(&self) -> bool {
        self.constraint_type.is_unique()
    }

    pub fn is_primary(&self) -> bool {
        self.constraint_type.is_primary()
    }
}

// ============================================================================
// IndexCatalogEntry
// ============================================================================

/// Index catalog entry - metadata for an index.
///
#[derive(Debug)]
pub struct IndexCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// Table name the index is on
    pub table_name: String,
    /// Table OID
    pub table_oid: u64,
    /// Column IDs being indexed
    pub column_ids: Vec<LogicalIndex>,
    /// Column types
    pub column_types: Vec<LogicalType>,
    /// Index type
    pub index_type: IndexType,
    /// Constraint type
    pub constraint_type: IndexConstraintType,
    /// Original SQL statement
    pub sql: Option<String>,
    /// Build lifecycle state (Building/Ready/Failed)
    pub build_state: RwLock<IndexBuildState>,
    /// Optional failure reason
    pub failure_reason: RwLock<Option<String>>,
    /// Optional full-text metadata (config + source column).
    pub fulltext: Option<FullTextIndexBinding>,
    /// Provider-owned, persistent index configuration.
    pub provider_config: Value,
    /// Optional runtime coverage snapshot.
    pub coverage: RwLock<Option<IndexCoverage>>,
}

impl IndexCatalogEntry {
    /// Create a new index catalog entry from CreateIndexInfo
    pub fn new(
        info: CreateIndexInfo,
        table_oid: u64,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        Self::with_object_id(info, table_oid, timestamp, catalog, object_id)
    }

    pub fn with_object_id(
        info: CreateIndexInfo,
        table_oid: u64,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        let base = SchemaEntryMeta::with_dependencies(
            CatalogType::Index,
            catalog.clone(),
            info.schema.clone(),
            info.name,
            object_id,
            timestamp,
            {
                let mut deps = info.dependencies;
                // Add dependency on the table
                deps.add_dependency(
                    CatalogObjectRef::in_schema(
                        CatalogObjectId::from_raw(table_oid),
                        CatalogType::Table,
                        catalog,
                        None,
                        info.schema,
                        info.table_name.clone(),
                    ),
                    DependencyType::Automatic,
                );
                deps
            },
        );

        Self {
            base,
            table_name: info.table_name,
            table_oid,
            column_ids: info.column_ids,
            column_types: info.column_types,
            index_type: info.index_type,
            constraint_type: info.constraint_type,
            sql: info.sql,
            build_state: RwLock::new(info.build_state),
            failure_reason: RwLock::new(info.failure_reason),
            fulltext: info.fulltext,
            provider_config: info.provider_config,
            coverage: RwLock::new(info.coverage),
        }
    }

    pub fn get_column_ids(&self) -> &[LogicalIndex] {
        &self.column_ids
    }

    pub fn get_column_types(&self) -> &[LogicalType] {
        &self.column_types
    }

    pub fn is_unique(&self) -> bool {
        self.constraint_type.is_unique()
    }

    pub fn is_primary(&self) -> bool {
        self.constraint_type.is_primary()
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn build_state(&self) -> IndexBuildState {
        *self.build_state.read().unwrap()
    }

    pub fn failure_reason(&self) -> Option<String> {
        self.failure_reason.read().unwrap().clone()
    }

    pub fn fulltext_binding(&self) -> Option<&FullTextIndexBinding> {
        self.fulltext.as_ref()
    }

    pub fn coverage(&self) -> Option<IndexCoverage> {
        *self.coverage.read().unwrap()
    }

    pub fn is_ready(&self) -> bool {
        self.build_state() == IndexBuildState::Ready
    }

    pub fn is_failed(&self) -> bool {
        self.build_state() == IndexBuildState::Failed
    }

    pub fn mark_building(&self) {
        *self.build_state.write().unwrap() = IndexBuildState::Building;
        *self.failure_reason.write().unwrap() = None;
        *self.coverage.write().unwrap() = None;
    }

    pub fn mark_ready(&self) {
        *self.build_state.write().unwrap() = IndexBuildState::Ready;
        *self.failure_reason.write().unwrap() = None;
    }

    pub fn mark_ready_with_coverage(&self, coverage: Option<IndexCoverage>) {
        *self.build_state.write().unwrap() = IndexBuildState::Ready;
        *self.failure_reason.write().unwrap() = None;
        *self.coverage.write().unwrap() = coverage;
    }

    pub fn mark_failed(&self, reason: Option<String>) {
        *self.build_state.write().unwrap() = IndexBuildState::Failed;
        *self.failure_reason.write().unwrap() = reason;
        *self.coverage.write().unwrap() = None;
    }

    /// Convert to SQL CREATE INDEX statement
    pub fn to_sql(&self) -> String {
        if let Some(sql) = &self.sql {
            return sql.clone();
        }

        let mut sql = String::new();
        sql.push_str("CREATE ");

        if self.constraint_type.is_unique() {
            sql.push_str("UNIQUE ");
        }

        sql.push_str("INDEX ");
        sql.push_str(&self.base.base.name);
        sql.push_str(" ON ");
        sql.push_str(&self.base.schema_name);
        sql.push('.');
        sql.push_str(&self.table_name);
        sql.push_str(" (");

        for (i, col_id) in self.column_ids.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("column_{}", col_id.index));
        }

        sql.push_str(");");
        sql
    }

    /// Serialize the index entry to bytes
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        buffer.write_all(&self.base.base.object_id.raw().to_le_bytes())?;
        buffer.write_all(&self.base.base.timestamp().to_le_bytes())?;

        let name_bytes = self.base.base.name.as_bytes();
        buffer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(name_bytes)?;

        let schema_bytes = self.base.schema_name.as_bytes();
        buffer.write_all(&(schema_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(schema_bytes)?;

        let table_bytes = self.table_name.as_bytes();
        buffer.write_all(&(table_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(table_bytes)?;

        buffer.write_all(&self.table_oid.to_le_bytes())?;
        buffer.write_all(&[self.index_type.to_byte()])?;
        buffer.write_all(&[self.constraint_type.to_byte()])?;

        buffer.write_all(&(self.column_ids.len() as u32).to_le_bytes())?;
        for col_id in &self.column_ids {
            buffer.write_all(&col_id.index.to_le_bytes())?;
        }

        buffer.write_all(&(self.column_types.len() as u32).to_le_bytes())?;
        for col_type in &self.column_types {
            col_type.serialize(&mut buffer)?;
        }

        if let Some(sql) = &self.sql {
            buffer.write_all(&[1u8])?;
            let sql_bytes = sql.as_bytes();
            buffer.write_all(&(sql_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(sql_bytes)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        buffer.write_all(&[self.build_state().to_byte()])?;

        if let Some(reason) = self.failure_reason() {
            buffer.write_all(&[1u8])?;
            let reason_bytes = reason.as_bytes();
            buffer.write_all(&(reason_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(reason_bytes)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        if let Some(binding) = &self.fulltext {
            buffer.write_all(&[1u8])?;
            buffer.write_all(&binding.column_id.index.to_le_bytes())?;
            let config_bytes = binding.config.as_bytes();
            buffer.write_all(&(config_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(config_bytes)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        if let Some(coverage) = self.coverage() {
            buffer.write_all(&[1u8])?;
            buffer.write_all(&coverage.visible_version.to_le_bytes())?;
            buffer.write_all(&coverage.visible_segment_count.to_le_bytes())?;
            buffer.write_all(&coverage.indexed_segment_count.to_le_bytes())?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        let provider_config = serde_json::to_vec(&self.provider_config).map_err(|err| {
            paro_error::serialization_error(format!(
                "Failed to serialize index provider config: {err}"
            ))
        })?;
        buffer.write_all(&(provider_config.len() as u32).to_le_bytes())?;
        buffer.write_all(&provider_config)?;

        Ok(buffer)
    }

    /// Deserialize an index entry from bytes
    pub fn deserialize(bytes: &[u8], catalog: String) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut oid_buf = [0u8; 8];
        cursor.read_exact(&mut oid_buf)?;
        let oid = u64::from_le_bytes(oid_buf);

        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf)?;
        let timestamp = u64::from_le_bytes(ts_buf);

        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let mut name_bytes = vec![0u8; name_len];
        cursor.read_exact(&mut name_bytes)?;
        let index_name = String::from_utf8(name_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;

        cursor.read_exact(&mut len_buf)?;
        let schema_len = u32::from_le_bytes(len_buf) as usize;
        let mut schema_bytes = vec![0u8; schema_len];
        cursor.read_exact(&mut schema_bytes)?;
        let schema_name = String::from_utf8(schema_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;

        cursor.read_exact(&mut len_buf)?;
        let table_len = u32::from_le_bytes(len_buf) as usize;
        let mut table_bytes = vec![0u8; table_len];
        cursor.read_exact(&mut table_bytes)?;
        let table_name = String::from_utf8(table_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;

        cursor.read_exact(&mut ts_buf)?;
        let table_oid = u64::from_le_bytes(ts_buf);

        let mut byte_buf = [0u8; 1];
        cursor.read_exact(&mut byte_buf)?;
        let index_type = IndexType::from_byte(byte_buf[0]);

        cursor.read_exact(&mut byte_buf)?;
        let constraint_type = IndexConstraintType::from_byte(byte_buf[0])
            .ok_or_else(|| paro_error::internal("Invalid constraint type"))?;

        cursor.read_exact(&mut len_buf)?;
        let col_count = u32::from_le_bytes(len_buf) as usize;
        let mut column_ids = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            cursor.read_exact(&mut len_buf)?;
            let col_id = u32::from_le_bytes(len_buf);
            column_ids.push(LogicalIndex::new(col_id));
        }

        cursor.read_exact(&mut len_buf)?;
        let type_count = u32::from_le_bytes(len_buf) as usize;
        let mut column_types = Vec::with_capacity(type_count);
        for _ in 0..type_count {
            let col_type = LogicalType::deserialize(&mut cursor)?;
            column_types.push(col_type);
        }

        cursor.read_exact(&mut byte_buf)?;
        let sql = if byte_buf[0] == 1 {
            cursor.read_exact(&mut len_buf)?;
            let sql_len = u32::from_le_bytes(len_buf) as usize;
            let mut sql_bytes = vec![0u8; sql_len];
            cursor.read_exact(&mut sql_bytes)?;
            Some(
                String::from_utf8(sql_bytes)
                    .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?,
            )
        } else {
            None
        };

        let (build_state, failure_reason) = if (cursor.position() as usize) < bytes.len() {
            cursor.read_exact(&mut byte_buf)?;
            let state = IndexBuildState::from_byte(byte_buf[0])?;
            cursor.read_exact(&mut byte_buf)?;
            let reason = if byte_buf[0] == 1 {
                cursor.read_exact(&mut len_buf)?;
                let reason_len = u32::from_le_bytes(len_buf) as usize;
                let mut reason_bytes = vec![0u8; reason_len];
                cursor.read_exact(&mut reason_bytes)?;
                Some(
                    String::from_utf8(reason_bytes)
                        .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?,
                )
            } else {
                None
            };
            (state, reason)
        } else {
            (IndexBuildState::Ready, None)
        };

        let fulltext = if (cursor.position() as usize) < bytes.len() {
            cursor.read_exact(&mut byte_buf)?;
            if byte_buf[0] == 1 {
                cursor.read_exact(&mut len_buf)?;
                let column_id = LogicalIndex::new(u32::from_le_bytes(len_buf));
                cursor.read_exact(&mut len_buf)?;
                let config_len = u32::from_le_bytes(len_buf) as usize;
                let mut config_bytes = vec![0u8; config_len];
                cursor.read_exact(&mut config_bytes)?;
                let config = String::from_utf8(config_bytes)
                    .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;
                Some(FullTextIndexBinding { column_id, config })
            } else {
                None
            }
        } else {
            None
        };

        cursor.read_exact(&mut byte_buf)?;
        let coverage = if byte_buf[0] == 1 {
            let mut version_buf = [0u8; 8];
            cursor.read_exact(&mut version_buf)?;
            let visible_version = i64::from_le_bytes(version_buf);
            cursor.read_exact(&mut len_buf)?;
            let visible_segment_count = u32::from_le_bytes(len_buf);
            cursor.read_exact(&mut len_buf)?;
            let indexed_segment_count = u32::from_le_bytes(len_buf);
            Some(IndexCoverage {
                visible_version,
                visible_segment_count,
                indexed_segment_count,
            })
        } else if byte_buf[0] == 0 {
            None
        } else {
            return Err(paro_error::serialization_error(format!(
                "Invalid index coverage tag: {}",
                byte_buf[0]
            )));
        };

        cursor.read_exact(&mut len_buf)?;
        let config_len = u32::from_le_bytes(len_buf) as usize;
        let mut config_bytes = vec![0u8; config_len];
        cursor.read_exact(&mut config_bytes)?;
        let provider_config = serde_json::from_slice(&config_bytes).map_err(|err| {
            paro_error::serialization_error(format!("Invalid index provider config: {err}"))
        })?;
        if cursor.position() as usize != bytes.len() {
            return Err(paro_error::serialization_error(
                "Trailing bytes in index catalog entry",
            ));
        }

        let mut deps = DependencyList::new();
        deps.add_dependency(
            CatalogObjectRef::in_schema(
                CatalogObjectId::from_raw(table_oid),
                CatalogType::Table,
                catalog.clone(),
                None,
                schema_name.clone(),
                table_name.clone(),
            ),
            DependencyType::Automatic,
        );

        let base = SchemaEntryMeta::with_dependencies(
            CatalogType::Index,
            catalog,
            schema_name,
            index_name,
            CatalogObjectId::from_raw(oid),
            timestamp,
            deps,
        );

        Ok(Self {
            base,
            table_name,
            table_oid,
            column_ids,
            column_types,
            index_type,
            constraint_type,
            sql,
            build_state: RwLock::new(build_state),
            failure_reason: RwLock::new(failure_reason),
            fulltext,
            provider_config,
            coverage: RwLock::new(coverage),
        })
    }
}

// ============================================================================
// CatalogEntry trait implementation
// ============================================================================

impl CatalogEntry for IndexCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::Index
    }

    fn catalog_name(&self) -> &str {
        &self.base.base.catalog
    }

    fn timestamp(&self) -> u64 {
        self.base.base.timestamp()
    }

    fn set_timestamp(&self, ts: u64) {
        self.base.base.set_timestamp(ts);
    }

    fn is_deleted(&self) -> bool {
        self.base.base.is_deleted()
    }

    fn set_deleted(&self, deleted: bool) {
        self.base.base.set_deleted(deleted);
    }

    fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.child()
    }

    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        self.base.base.set_child(child);
    }

    fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.parent()
    }

    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        self.base.base.set_parent(parent);
    }

    fn is_temporary(&self) -> bool {
        self.base.base.temporary
    }

    fn is_internal(&self) -> bool {
        self.base.base.internal
    }

    fn comment(&self) -> Option<&str> {
        None
    }

    fn set_comment(&self, comment: Option<String>) {
        self.base.base.set_comment(comment);
    }

    fn tags(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn set_tags(&self, tags: HashMap<String, String>) {
        self.base.base.set_tags(tags);
    }

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER INDEX"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("INDEX copy"))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::Index,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

// ============================================================================
// StandardEntry trait implementation
// ============================================================================

impl StandardEntry for IndexCatalogEntry {
    fn schema_name(&self) -> &str {
        &self.base.schema_name
    }

    fn dependencies(&self) -> &DependencyList {
        static EMPTY: LazyLock<DependencyList> = LazyLock::new(DependencyList::new);
        &EMPTY
    }

    fn set_dependencies(&self, dependencies: DependencyList) {
        self.base.set_dependencies(dependencies);
    }
}

// ============================================================================
// InCatalogEntry trait implementation
// ============================================================================

impl InCatalogEntry for IndexCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index_info() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "users".to_string(),
            "idx_users_name".to_string(),
            vec![LogicalIndex::new(1)],
            vec![LogicalType::Varchar],
        )
        .with_unique();

        assert_eq!(info.schema, "public");
        assert_eq!(info.table_name, "users");
        assert_eq!(info.name, "idx_users_name");
        assert!(info.is_unique());
    }

    #[test]
    fn test_index_catalog_entry() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "users".to_string(),
            "idx_users_email".to_string(),
            vec![LogicalIndex::new(2)],
            vec![LogicalType::Varchar],
        )
        .with_unique();

        let entry = IndexCatalogEntry::new(
            info,
            42,
            100,
            "main".to_string(),
            CatalogObjectId::from_raw(10_001),
        );

        assert_eq!(entry.name(), "idx_users_email");
        assert_eq!(entry.table_name, "users");
        assert_eq!(entry.table_oid, 42);
        assert!(entry.is_unique());
    }

    #[test]
    fn test_to_sql() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "users".to_string(),
            "idx_users_id".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::BigInt],
        )
        .with_unique();

        let entry = IndexCatalogEntry::new(
            info,
            42,
            100,
            "main".to_string(),
            CatalogObjectId::from_raw(10_002),
        );
        let sql = entry.to_sql();

        assert!(sql.contains("CREATE UNIQUE INDEX"));
        assert!(sql.contains("idx_users_id"));
    }

    #[test]
    fn test_index_build_state_transitions() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "users".to_string(),
            "idx_users_status".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Integer],
        )
        .with_build_state(IndexBuildState::Building);

        let entry = IndexCatalogEntry::new(
            info,
            42,
            100,
            "main".to_string(),
            CatalogObjectId::from_raw(10_003),
        );
        assert_eq!(entry.build_state(), IndexBuildState::Building);
        assert_eq!(entry.failure_reason(), None);

        entry.mark_failed(Some("build failed".to_string()));
        assert!(entry.is_failed());
        assert_eq!(entry.failure_reason(), Some("build failed".to_string()));

        entry.mark_ready();
        assert!(entry.is_ready());
        assert_eq!(entry.failure_reason(), None);
    }

    #[test]
    fn test_index_catalog_entry_roundtrip_with_state() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "users".to_string(),
            "idx_users_roundtrip".to_string(),
            vec![LogicalIndex::new(1)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::HNSW)
        .with_provider_config(serde_json::json!({
            "version": 1,
            "dimension": 100,
            "distance": "cosine",
            "m": 24,
            "ef_construct": 100,
            "ef_search": 80,
            "plain_scan_threshold": 10_000,
            "filtered_plain_scan_threshold": 0,
            "build_seed": 42,
            "inline_threshold": {
                "enabled": true,
                "max_vector_count": 90_000,
                "max_graph_memory_bytes": 268_435_456_u64,
                "max_dimension": 100
            }
        }))
        .with_failure_reason("needs rebuild");

        let entry = IndexCatalogEntry::new(
            info,
            7,
            123,
            "main".to_string(),
            CatalogObjectId::from_raw(10_004),
        );
        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = IndexCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        assert_eq!(restored.base.base.name, "idx_users_roundtrip");
        assert_eq!(restored.object_id(), entry.object_id());
        assert_eq!(restored.index_type, IndexType::HNSW);
        assert_eq!(restored.provider_config, entry.provider_config);
        assert_eq!(restored.build_state(), IndexBuildState::Failed);
        assert_eq!(restored.failure_reason(), Some("needs rebuild".to_string()));

        let mut with_trailing_bytes = bytes;
        with_trailing_bytes.extend_from_slice(&[0xde, 0xad]);
        assert!(IndexCatalogEntry::deserialize(&with_trailing_bytes, "main".to_string()).is_err());
    }

    #[test]
    fn test_index_catalog_entry_roundtrip_with_fulltext_binding() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "docs".to_string(),
            "idx_docs_fts".to_string(),
            vec![LogicalIndex::new(2)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(2), "simple");

        let entry = IndexCatalogEntry::new(
            info,
            7,
            123,
            "main".to_string(),
            CatalogObjectId::from_raw(10_005),
        );
        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = IndexCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        let binding = restored
            .fulltext_binding()
            .expect("fulltext binding should roundtrip");
        assert_eq!(binding.column_id.index, 2);
        assert_eq!(binding.config, "simple");
    }

    #[test]
    fn test_index_catalog_entry_roundtrip_with_coverage() {
        let info = CreateIndexInfo::new(
            "public".to_string(),
            "docs".to_string(),
            "idx_docs_fts_cov".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(0), "simple")
        .with_coverage(IndexCoverage::from_counts(12, 4, 4))
        .with_build_state(IndexBuildState::Ready);

        let entry = IndexCatalogEntry::new(
            info,
            9,
            456,
            "main".to_string(),
            CatalogObjectId::from_raw(10_006),
        );
        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = IndexCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        let coverage = restored.coverage().expect("coverage should roundtrip");
        assert_eq!(coverage.visible_version, 12);
        assert_eq!(coverage.visible_segment_count, 4);
        assert_eq!(coverage.indexed_segment_count, 4);
        assert!(coverage.is_complete());
    }
}
