// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Table Catalog Entry
//!
//!
//! This module defines TableCatalogEntry for table metadata.

use super::catalog_entry::{
    AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo, DependencyList,
    InCatalogEntry, OnCreateConflict, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_storage::meta::TabletMetaManager;
use paro_storage::table::storage_descriptor::TableStorageDescriptor;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::{Arc, LazyLock, Weak};

// --- Constraint Types ---

/// Type of constraint.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintType {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey,
    Check,
}

impl ConstraintType {
    fn to_byte(self) -> u8 {
        match self {
            Self::NotNull => 0,
            Self::Unique => 1,
            Self::PrimaryKey => 2,
            Self::ForeignKey => 3,
            Self::Check => 4,
        }
    }

    fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::NotNull),
            1 => Ok(Self::Unique),
            2 => Ok(Self::PrimaryKey),
            3 => Ok(Self::ForeignKey),
            4 => Ok(Self::Check),
            _ => Err(paro_error::invalid_input(format!(
                "invalid table constraint type: {value}"
            ))),
        }
    }
}

/// Table constraint.
///
#[derive(Debug, Clone)]
pub struct Constraint {
    /// Constraint type
    pub constraint_type: ConstraintType,
    /// Column indices involved (for UNIQUE, PRIMARY KEY, FOREIGN KEY)
    pub columns: Vec<usize>,
    /// Expression (for CHECK constraints)
    pub expression: Option<String>,
    /// Referenced table (for FOREIGN KEY)
    pub referenced_table: Option<String>,
    /// Referenced columns (for FOREIGN KEY)
    pub referenced_columns: Option<Vec<usize>>,
}

impl Constraint {
    pub fn not_null(column_idx: usize) -> Self {
        Self {
            constraint_type: ConstraintType::NotNull,
            columns: vec![column_idx],
            expression: None,
            referenced_table: None,
            referenced_columns: None,
        }
    }

    pub fn unique(columns: Vec<usize>) -> Self {
        Self {
            constraint_type: ConstraintType::Unique,
            columns,
            expression: None,
            referenced_table: None,
            referenced_columns: None,
        }
    }

    pub fn primary_key(columns: Vec<usize>) -> Self {
        Self {
            constraint_type: ConstraintType::PrimaryKey,
            columns,
            expression: None,
            referenced_table: None,
            referenced_columns: None,
        }
    }

    pub fn check(expression: String) -> Self {
        Self {
            constraint_type: ConstraintType::Check,
            columns: Vec::new(),
            expression: Some(expression),
            referenced_table: None,
            referenced_columns: None,
        }
    }

    pub fn foreign_key(
        columns: Vec<usize>,
        referenced_table: String,
        referenced_columns: Vec<usize>,
    ) -> Self {
        Self {
            constraint_type: ConstraintType::ForeignKey,
            columns,
            expression: None,
            referenced_table: Some(referenced_table),
            referenced_columns: Some(referenced_columns),
        }
    }
}

fn write_usize_as_u32(buffer: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        paro_error::serialization_error(format!("{label} exceeds catalog format limit"))
    })?;
    buffer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> Result<()> {
    write_usize_as_u32(buffer, value.len(), "string length")?;
    buffer.write_all(value.as_bytes())?;
    Ok(())
}

fn write_optional_string(buffer: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            buffer.write_all(&[1])?;
            write_string(buffer, value)
        }
        None => {
            buffer.write_all(&[0])?;
            Ok(())
        }
    }
}

fn write_indices(buffer: &mut Vec<u8>, values: &[usize], label: &str) -> Result<()> {
    write_usize_as_u32(buffer, values.len(), label)?;
    for &value in values {
        write_usize_as_u32(buffer, value, label)?;
    }
    Ok(())
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_u32(cursor)? as usize;
    let remaining = cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize);
    if len > remaining {
        return Err(paro_error::invalid_input(format!(
            "catalog string length {len} exceeds remaining payload {remaining}"
        )));
    }
    let mut bytes = vec![0; len];
    cursor.read_exact(&mut bytes)?;
    String::from_utf8(bytes)
        .map_err(|error| paro_error::invalid_input(format!("invalid catalog UTF-8: {error}")))
}

fn read_optional_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>> {
    let mut marker = [0];
    cursor.read_exact(&mut marker)?;
    match marker[0] {
        0 => Ok(None),
        1 => read_string(cursor).map(Some),
        value => Err(paro_error::invalid_input(format!(
            "invalid optional-string marker: {value}"
        ))),
    }
}

fn read_indices(cursor: &mut Cursor<&[u8]>) -> Result<Vec<usize>> {
    let count = read_u32(cursor)? as usize;
    let remaining = cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize);
    if count > remaining / std::mem::size_of::<u32>() {
        return Err(paro_error::invalid_input(format!(
            "catalog index count {count} exceeds remaining payload"
        )));
    }
    (0..count)
        .map(|_| read_u32(cursor).map(|value| value as usize))
        .collect()
}

fn validate_constraint(
    constraint_type: ConstraintType,
    columns: &[usize],
    expression: Option<&str>,
    referenced_table: Option<&str>,
    referenced_columns: Option<&[usize]>,
    column_count: usize,
) -> Result<()> {
    if columns.iter().any(|&column| column >= column_count) {
        return Err(paro_error::invalid_input(
            "table constraint references an out-of-range column",
        ));
    }
    let valid = match constraint_type {
        ConstraintType::NotNull => columns.len() == 1,
        ConstraintType::Unique | ConstraintType::PrimaryKey => !columns.is_empty(),
        ConstraintType::ForeignKey => {
            !columns.is_empty()
                && referenced_table.is_some()
                && referenced_columns.is_some_and(|referenced| referenced.len() == columns.len())
        }
        ConstraintType::Check => expression.is_some(),
    };
    if !valid {
        return Err(paro_error::invalid_input(format!(
            "invalid {constraint_type:?} table constraint"
        )));
    }
    Ok(())
}

fn validate_constraints(constraints: &[Constraint], column_count: usize) -> Result<()> {
    for constraint in constraints {
        validate_constraint(
            constraint.constraint_type,
            &constraint.columns,
            constraint.expression.as_deref(),
            constraint.referenced_table.as_deref(),
            constraint.referenced_columns.as_deref(),
            column_count,
        )?;
    }
    Ok(())
}

// --- Column Definition ---

/// Column definition for a table.
///
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Column type
    pub logical_type: LogicalType,
    /// Default value expression (optional)
    pub default_value: Option<String>,
    /// NOT NULL constraint
    pub not_null: bool,
    /// Column comment
    pub comment: Option<String>,
}

impl ColumnDefinition {
    pub fn new(name: String, logical_type: LogicalType) -> Self {
        Self {
            name,
            logical_type,
            default_value: None,
            not_null: false,
            comment: None,
        }
    }

    pub fn with_not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn with_default(mut self, default: String) -> Self {
        self.default_value = Some(default);
        self
    }

    pub fn with_comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }
}

// --- Table Type ---

/// Type of table.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableType {
    #[default]
    BaseTable,
    View,
    Temporary,
    External,
}

impl TableType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TableType::BaseTable => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "TEMPORARY TABLE",
            TableType::External => "EXTERNAL TABLE",
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            TableType::BaseTable => 0,
            TableType::View => 1,
            TableType::Temporary => 2,
            TableType::External => 3,
        }
    }

    pub fn from_byte(byte: u8) -> Self {
        match byte {
            1 => TableType::View,
            2 => TableType::Temporary,
            3 => TableType::External,
            _ => TableType::BaseTable,
        }
    }
}

// --- CreateTableInfo ---

/// Information for creating a table.
///
#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDefinition>,
    /// Table constraints
    pub constraints: Vec<Constraint>,
    /// Table type
    pub table_type: TableType,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is temporary
    pub temporary: bool,
    /// Whether this is internal
    pub internal: bool,
    /// Original SQL
    pub sql: Option<String>,
    /// Comment
    pub comment: Option<String>,
    /// Dependencies
    pub dependencies: DependencyList,
}

impl CreateTableInfo {
    pub fn new(
        catalog: String,
        schema: String,
        name: String,
        columns: Vec<ColumnDefinition>,
    ) -> Self {
        Self {
            catalog,
            schema,
            name,
            columns,
            constraints: Vec::new(),
            table_type: TableType::BaseTable,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            temporary: false,
            internal: false,
            sql: None,
            comment: None,
            dependencies: DependencyList::new(),
        }
    }

    pub fn with_on_conflict(mut self, on_conflict: OnCreateConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    pub fn with_temporary(mut self) -> Self {
        self.temporary = true;
        self.table_type = TableType::Temporary;
        self
    }

    pub fn with_internal(mut self) -> Self {
        self.internal = true;
        self
    }

    pub fn with_sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    pub fn with_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn add_constraint(mut self, constraint: Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }
}

// --- Table Statistics (simplified) ---

/// Column statistics for a table.
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub has_null: bool,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub distinct_count: Option<u64>,
}

/// Table statistics.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: Vec<ColumnStatistics>,
}

impl TableStatistics {
    pub fn new_empty(column_count: usize) -> Self {
        Self {
            row_count: 0,
            column_stats: vec![ColumnStatistics::default(); column_count],
        }
    }

    pub fn align_column_count(&mut self, count: usize) {
        while self.column_stats.len() < count {
            self.column_stats.push(ColumnStatistics::default());
        }
    }
}

// --- TableCatalogEntry ---

/// Table catalog entry.
///
#[derive(Debug)]
pub struct TableCatalogEntry {
    /// Standard entry base (includes schema reference)
    pub base: SchemaEntryMeta,
    /// Table type
    pub table_type: TableType,
    /// Column definitions
    pub columns: Vec<ColumnDefinition>,
    /// Table constraints
    constraints: Vec<Constraint>,
    /// Reference to the underlying storage
    pub storage: Option<Arc<TableHandle>>,
    /// Stable storage descriptor persisted by catalog
    pub storage_descriptor: Option<TableStorageDescriptor>,
    /// Table statistics
    pub statistics: Option<TableStatistics>,
}

impl TableCatalogEntry {
    const SERIALIZATION_VERSION: u32 = 1;

    fn descriptor_from_storage(storage: &Arc<TableHandle>) -> Option<TableStorageDescriptor> {
        storage.to_descriptor().ok()
    }

    /// Validated constraints attached to this table.
    ///
    /// The collection is intentionally read-only outside the catalog entry:
    /// construction, replay, and schema evolution are the validation boundary.
    pub fn constraints(&self) -> &[Constraint] {
        &self.constraints
    }

    /// Create a new table catalog entry.
    pub fn new(
        catalog: String,
        schema_name: String,
        name: String,
        columns: Vec<ColumnDefinition>,
        storage: Arc<TableHandle>,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        Self::with_object_id(
            catalog,
            schema_name,
            name,
            columns,
            storage,
            object_id,
            timestamp,
        )
    }

    /// Create from CreateTableInfo
    pub fn from_info(
        info: CreateTableInfo,
        storage: Arc<TableHandle>,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Result<Self> {
        Self::from_info_with_object_id(info, storage, object_id, timestamp)
    }

    /// Create from CreateTableInfo with a specific persisted object identity.
    pub fn from_info_with_object_id(
        info: CreateTableInfo,
        storage: Arc<TableHandle>,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Result<Self> {
        let column_count = info.columns.len();
        validate_constraints(&info.constraints, column_count)?;
        let storage_descriptor = Self::descriptor_from_storage(&storage);
        let mut base = SchemaEntryMeta::new(
            CatalogType::Table,
            info.catalog,
            info.schema,
            info.name,
            object_id,
            timestamp,
        );
        base.base.internal = info.internal;
        base.base.temporary = info.temporary;
        base.set_dependencies(info.dependencies);

        Ok(Self {
            base,
            table_type: info.table_type,
            columns: info.columns,
            constraints: info.constraints,
            storage: Some(storage),
            storage_descriptor,
            statistics: Some(TableStatistics::new_empty(column_count)),
        })
    }

    /// Create with a specific object identity (e.g. deserialization / replay).
    pub fn with_object_id(
        catalog: String,
        schema_name: String,
        name: String,
        columns: Vec<ColumnDefinition>,
        storage: Arc<TableHandle>,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        let column_count = columns.len();
        let storage_descriptor = Self::descriptor_from_storage(&storage);
        Self {
            base: SchemaEntryMeta::new(
                CatalogType::Table,
                catalog,
                schema_name,
                name,
                object_id,
                timestamp,
            ),
            table_type: TableType::BaseTable,
            columns,
            constraints: Vec::new(),
            storage: Some(storage),
            storage_descriptor,
            statistics: Some(TableStatistics::new_empty(column_count)),
        }
    }

    /// Clone with new statistics
    pub fn clone_with_statistics(
        &self,
        statistics: Option<TableStatistics>,
        timestamp: u64,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            timestamp,
        );

        Self {
            base,
            table_type: self.table_type,
            columns: self.columns.clone(),
            constraints: self.constraints.clone(),
            storage: self.storage.clone(),
            storage_descriptor: self.storage_descriptor.clone(),
            statistics,
        }
    }

    pub fn clone_with_comment(&self, comment: Option<String>, timestamp: u64) -> Self {
        let cloned = self.clone_with_metadata(
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.columns.clone(),
            self.statistics.clone(),
            timestamp,
        );
        cloned.base.base.set_comment(comment);
        cloned
    }

    pub fn clone_with_new_name(&self, new_name: String, timestamp: u64) -> Self {
        self.clone_with_metadata(
            self.base.schema_name.clone(),
            new_name,
            self.columns.clone(),
            self.statistics.clone(),
            timestamp,
        )
    }

    pub fn clone_with_new_schema_and_name(
        &self,
        new_schema: String,
        new_name: String,
        timestamp: u64,
    ) -> Self {
        self.clone_with_metadata(
            new_schema,
            new_name,
            self.columns.clone(),
            self.statistics.clone(),
            timestamp,
        )
    }

    pub fn clone_with_renamed_column(
        &self,
        old_column_name: &str,
        new_column_name: String,
        timestamp: u64,
    ) -> Result<Self> {
        let column_idx = self.get_column_index(old_column_name).ok_or_else(|| {
            paro_error::catalog(format!(
                "Column '{}' does not exist in table '{}'",
                old_column_name, self.base.base.name
            ))
        })?;
        if self.get_column(&new_column_name).is_some() {
            return Err(paro_error::catalog(format!(
                "Column '{}' already exists in table '{}'",
                new_column_name, self.base.base.name
            )));
        }

        let mut new_columns = self.columns.clone();
        new_columns[column_idx].name = new_column_name;
        Ok(self.clone_with_metadata(
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            new_columns,
            self.statistics.clone(),
            timestamp,
        ))
    }

    pub fn clone_with_column_comments(
        &self,
        comments: &[(String, String)],
        timestamp: u64,
    ) -> Result<Self> {
        let mut new_columns = self.columns.clone();
        for (column_name, comment) in comments {
            let column_idx = self.get_column_index(column_name).ok_or_else(|| {
                paro_error::catalog(format!(
                    "Column '{}' does not exist in table '{}'",
                    column_name, self.base.base.name
                ))
            })?;
            new_columns[column_idx].comment = Some(comment.clone());
        }

        Ok(self.clone_with_metadata(
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            new_columns,
            self.statistics.clone(),
            timestamp,
        ))
    }

    /// Get the storage for this table
    pub fn get_storage(&self) -> Option<&Arc<TableHandle>> {
        self.storage.as_ref()
    }

    pub fn get_storage_descriptor(&self) -> Option<&TableStorageDescriptor> {
        self.storage_descriptor.as_ref()
    }

    /// Get the database name
    pub fn database_name(&self) -> &str {
        &self.base.base.catalog
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.base.base.name
    }

    /// Get statistics
    pub fn statistics(&self) -> Option<&TableStatistics> {
        self.statistics.as_ref()
    }

    /// Get mutable statistics
    pub fn statistics_mut(&mut self) -> Option<&mut TableStatistics> {
        self.statistics.as_mut()
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Add a column to the table
    ///
    pub fn add_column(&self, column: ColumnDefinition, timestamp: u64) -> Result<Self> {
        // Check if column already exists
        if self.get_column(&column.name).is_some() {
            return Err(paro_error::catalog(format!(
                "Column '{}' already exists in table '{}'",
                column.name, self.base.base.name
            )));
        }

        // Create new column list
        let mut new_columns = self.columns.clone();
        new_columns.push(column);

        // Update statistics to match new column count
        let mut new_stats = self.statistics.clone();
        if let Some(stats) = &mut new_stats {
            stats.align_column_count(new_columns.len());
        }

        // Create new entry with updated columns
        let base = SchemaEntryMeta::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            timestamp,
        );

        Ok(Self {
            base,
            table_type: self.table_type,
            columns: new_columns,
            constraints: self.constraints.clone(),
            storage: self.storage.clone(),
            storage_descriptor: self.storage_descriptor.clone(),
            statistics: new_stats,
        })
    }

    /// Remove a column from the table
    ///
    pub fn remove_column(&self, column_name: &str, timestamp: u64) -> Result<Self> {
        // Find column index
        let column_idx = self.get_column_index(column_name).ok_or_else(|| {
            paro_error::catalog(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.base.base.name
            ))
        })?;

        // Cannot remove last column
        if self.columns.len() == 1 {
            return Err(paro_error::catalog(
                "Cannot remove the last column from a table".to_string(),
            ));
        }

        // Create new column list without the removed column
        let mut new_columns = self.columns.clone();
        new_columns.remove(column_idx);

        // Update constraints to remove references to the removed column
        let new_constraints = self
            .constraints
            .iter()
            .filter_map(|c| {
                match c.constraint_type {
                    ConstraintType::NotNull => {
                        // Skip if this is the removed column
                        if c.columns.contains(&column_idx) {
                            None
                        } else {
                            // Adjust column indices
                            let mut new_c = c.clone();
                            new_c.columns = new_c
                                .columns
                                .iter()
                                .map(|&idx| if idx > column_idx { idx - 1 } else { idx })
                                .collect();
                            Some(new_c)
                        }
                    }
                    ConstraintType::Unique | ConstraintType::PrimaryKey => {
                        // Skip if any column is the removed one
                        if c.columns.contains(&column_idx) {
                            None
                        } else {
                            // Adjust column indices
                            let mut new_c = c.clone();
                            new_c.columns = new_c
                                .columns
                                .iter()
                                .map(|&idx| if idx > column_idx { idx - 1 } else { idx })
                                .collect();
                            Some(new_c)
                        }
                    }
                    ConstraintType::Check => Some(c.clone()),
                    ConstraintType::ForeignKey => {
                        // Skip if any column is the removed one
                        if c.columns.contains(&column_idx) {
                            None
                        } else {
                            // Adjust column indices
                            let mut new_c = c.clone();
                            new_c.columns = new_c
                                .columns
                                .iter()
                                .map(|&idx| if idx > column_idx { idx - 1 } else { idx })
                                .collect();
                            Some(new_c)
                        }
                    }
                }
            })
            .collect();

        // Update statistics
        let mut new_stats = self.statistics.clone();
        if let Some(stats) = &mut new_stats {
            stats.column_stats.remove(column_idx);
        }

        // Create new entry
        let base = SchemaEntryMeta::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            timestamp,
        );

        Ok(Self {
            base,
            table_type: self.table_type,
            columns: new_columns,
            constraints: new_constraints,
            storage: self.storage.clone(),
            storage_descriptor: self.storage_descriptor.clone(),
            statistics: new_stats,
        })
    }

    /// Alter a column (change type, default, etc.)
    ///
    pub fn alter_column(
        &self,
        column_name: &str,
        new_type: Option<LogicalType>,
        new_default: Option<Option<String>>,
        new_not_null: Option<bool>,
        timestamp: u64,
    ) -> Result<Self> {
        // Find column index
        let column_idx = self.get_column_index(column_name).ok_or_else(|| {
            paro_error::catalog(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.base.base.name
            ))
        })?;

        // Create new column list with modified column
        let mut new_columns = self.columns.clone();
        let col = &mut new_columns[column_idx];

        if let Some(new_type) = new_type {
            col.logical_type = new_type;
        }
        if let Some(new_default) = new_default {
            col.default_value = new_default;
        }
        if let Some(new_not_null) = new_not_null {
            col.not_null = new_not_null;
        }

        // Create new entry
        let base = SchemaEntryMeta::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            timestamp,
        );

        Ok(Self {
            base,
            table_type: self.table_type,
            columns: new_columns,
            constraints: self.constraints.clone(),
            storage: self.storage.clone(),
            storage_descriptor: self.storage_descriptor.clone(),
            statistics: self.statistics.clone(),
        })
    }

    /// Get statistics for the table
    ///
    pub fn get_statistics(&self) -> Option<&TableStatistics> {
        self.statistics.as_ref()
    }

    /// Update statistics for the table
    pub fn update_statistics(&mut self, statistics: TableStatistics) {
        self.statistics = Some(statistics);
    }

    fn clone_with_metadata(
        &self,
        schema_name: String,
        name: String,
        columns: Vec<ColumnDefinition>,
        statistics: Option<TableStatistics>,
        timestamp: u64,
    ) -> Self {
        let mut base = SchemaEntryMeta::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            schema_name,
            name,
            self.base.base.object_id,
            timestamp,
        );
        base.base.internal = self.base.base.internal;
        base.base.temporary = self.base.base.temporary;
        base.set_dependencies(self.base.dependencies());
        base.base.set_tags(self.base.base.tags());
        base.base.set_comment(self.base.base.comment());

        Self {
            base,
            table_type: self.table_type,
            columns,
            constraints: self.constraints.clone(),
            storage: self.storage.clone(),
            storage_descriptor: self.storage_descriptor.clone(),
            statistics,
        }
    }

    /// Convert to SQL CREATE TABLE statement
    pub fn to_sql(&self) -> String {
        let mut sql = format!(
            "CREATE TABLE {}.{} (\n",
            self.base.schema_name, self.base.base.name
        );

        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str(&format!("    {} {}", col.name, col.logical_type));
            if col.not_null {
                sql.push_str(" NOT NULL");
            }
            if let Some(default) = &col.default_value {
                sql.push_str(&format!(" DEFAULT {}", default));
            }
        }

        sql.push_str("\n);");
        sql
    }

    /// Serialize the table entry
    pub fn serialize(&self) -> Result<Vec<u8>> {
        validate_constraints(&self.constraints, self.columns.len())?;
        let mut buffer = Vec::new();

        buffer.write_all(&Self::SERIALIZATION_VERSION.to_le_bytes())?;

        // 1. OID
        buffer.write_all(&self.base.base.object_id.raw().to_le_bytes())?;

        // 2. Timestamp
        buffer.write_all(&self.base.base.timestamp().to_le_bytes())?;

        // 3. Name
        write_string(&mut buffer, &self.base.base.name)?;

        // 4. Schema Name
        write_string(&mut buffer, &self.base.schema_name)?;

        // 5. Table type
        buffer.write_all(&[self.table_type.to_byte()])?;

        // 6. Columns
        write_usize_as_u32(&mut buffer, self.columns.len(), "column count")?;
        for col in &self.columns {
            write_string(&mut buffer, &col.name)?;
            col.logical_type.serialize(&mut buffer)?;
            buffer.write_all(&[col.not_null as u8])?;
            write_optional_string(&mut buffer, col.default_value.as_deref())?;
            write_optional_string(&mut buffer, col.comment.as_deref())?;
        }

        // 7. Constraints
        write_usize_as_u32(&mut buffer, self.constraints.len(), "constraint count")?;
        for constraint in &self.constraints {
            buffer.write_all(&[constraint.constraint_type.to_byte()])?;
            write_indices(&mut buffer, &constraint.columns, "constraint columns")?;
            write_optional_string(&mut buffer, constraint.expression.as_deref())?;
            write_optional_string(&mut buffer, constraint.referenced_table.as_deref())?;
            match &constraint.referenced_columns {
                Some(columns) => {
                    buffer.write_all(&[1])?;
                    write_indices(&mut buffer, columns, "referenced constraint columns")?;
                }
                None => buffer.write_all(&[0])?,
            }
        }

        // 8. Storage descriptor
        // Persist only descriptor bytes; runtime storage is reconstructed during deserialize.
        let descriptor = if let Some(storage) = &self.storage {
            storage.to_descriptor()?
        } else if let Some(descriptor) = &self.storage_descriptor {
            descriptor.clone()
        } else {
            return Err(paro_error::invalid_input(
                "table storage descriptor missing in TableCatalogEntry",
            ));
        };
        let descriptor_bytes = descriptor.serialize()?;
        write_usize_as_u32(
            &mut buffer,
            descriptor_bytes.len(),
            "storage descriptor length",
        )?;
        buffer.write_all(&descriptor_bytes)?;

        // 9. Statistics (simplified)
        if let Some(stats) = &self.statistics {
            buffer.write_all(&[1u8])?;
            buffer.write_all(&stats.row_count.to_le_bytes())?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // 10. Optional table comment
        write_optional_string(&mut buffer, self.base.base.comment().as_deref())?;

        Ok(buffer)
    }

    /// Deserialize table entry from bytes
    pub fn deserialize(
        bytes: &[u8],
        catalog: String,
        meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let version = read_u32(&mut cursor)?;
        if version != Self::SERIALIZATION_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported table catalog format version: {version}"
            )));
        }

        // 1. OID
        let mut oid_buf = [0u8; 8];
        cursor.read_exact(&mut oid_buf)?;
        let oid = u64::from_le_bytes(oid_buf);

        // 2. Timestamp
        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf)?;
        let timestamp = u64::from_le_bytes(ts_buf);

        // 3. Name
        let mut len_buf = [0u8; 4];
        let name = read_string(&mut cursor)?;

        // 4. Schema Name
        let schema_name = read_string(&mut cursor)?;

        // 5. Table type
        let mut byte_buf = [0u8; 1];
        cursor.read_exact(&mut byte_buf)?;
        let table_type = TableType::from_byte(byte_buf[0]);

        // 6. Columns
        let col_count = read_u32(&mut cursor)? as usize;
        // Do not reserve from an untrusted count. Each decoder advances the
        // bounded cursor and fails at the first incomplete element.
        let mut columns = Vec::new();
        let mut column_types = Vec::new();

        for _ in 0..col_count {
            let col_name = read_string(&mut cursor)?;

            let col_type = LogicalType::deserialize(&mut cursor)?;
            column_types.push(col_type.clone());

            cursor.read_exact(&mut byte_buf)?;
            let not_null = byte_buf[0] != 0;

            let mut col_def = ColumnDefinition::new(col_name, col_type);
            if not_null {
                col_def = col_def.with_not_null();
            }
            col_def.default_value = read_optional_string(&mut cursor)?;
            col_def.comment = read_optional_string(&mut cursor)?;
            columns.push(col_def);
        }

        // 7. Constraints
        let constraint_count = read_u32(&mut cursor)? as usize;
        let mut constraints = Vec::new();
        for _ in 0..constraint_count {
            cursor.read_exact(&mut byte_buf)?;
            let constraint_type = ConstraintType::from_byte(byte_buf[0])?;
            let columns = read_indices(&mut cursor)?;
            let expression = read_optional_string(&mut cursor)?;
            let referenced_table = read_optional_string(&mut cursor)?;
            cursor.read_exact(&mut byte_buf)?;
            let referenced_columns = match byte_buf[0] {
                0 => None,
                1 => Some(read_indices(&mut cursor)?),
                marker => {
                    return Err(paro_error::invalid_input(format!(
                        "invalid referenced-columns marker: {marker}"
                    )));
                }
            };
            validate_constraint(
                constraint_type,
                &columns,
                expression.as_deref(),
                referenced_table.as_deref(),
                referenced_columns.as_deref(),
                col_count,
            )?;
            constraints.push(Constraint {
                constraint_type,
                columns,
                expression,
                referenced_table,
                referenced_columns,
            });
        }

        // 8. Storage descriptor
        cursor.read_exact(&mut len_buf)?;
        let descriptor_len = u32::from_le_bytes(len_buf) as usize;
        let remaining = bytes.len().saturating_sub(cursor.position() as usize);
        if descriptor_len > remaining {
            return Err(paro_error::invalid_input(format!(
                "storage descriptor length {descriptor_len} exceeds remaining payload {remaining}"
            )));
        }
        let mut descriptor_bytes = vec![0u8; descriptor_len];
        cursor.read_exact(&mut descriptor_bytes)?;
        let storage_descriptor = TableStorageDescriptor::deserialize(&descriptor_bytes)?;

        // Reconstruct TableHandle only through descriptor + optional TabletMetaManager.
        // This is the catalog recovery path used after restart.
        let storage = Arc::new(
            TableFactory::new(meta_manager)
                .open_from_descriptor(&column_types, &storage_descriptor)?,
        );

        // 9. Statistics marker (strict new-format path)
        if (cursor.position() as usize) >= bytes.len() {
            return Err(paro_error::invalid_input(
                "table catalog entry missing statistics marker",
            ));
        }

        let mut count_buf = [0u8; 8];
        cursor.read_exact(&mut byte_buf)?;
        let statistics = match byte_buf[0] {
            0 => Some(TableStatistics::new_empty(columns.len())),
            1 => {
                cursor.read_exact(&mut count_buf)?;
                let row_count = u64::from_le_bytes(count_buf);
                let mut stats = TableStatistics::new_empty(columns.len());
                stats.row_count = row_count;
                Some(stats)
            }
            marker => {
                return Err(paro_error::invalid_input(format!(
                    "invalid table catalog statistics marker: {}",
                    marker
                )));
            }
        };

        // 10. Table comment. The versioned format always carries the marker;
        // an early EOF is corruption rather than a legacy no-comment entry.
        let comment = read_optional_string(&mut cursor)?;

        if cursor.position() as usize != bytes.len() {
            return Err(paro_error::invalid_input(
                "table catalog entry contains trailing bytes",
            ));
        }

        let mut entry = Self::with_object_id(
            catalog,
            schema_name,
            name,
            columns,
            storage,
            CatalogObjectId::from_raw(oid),
            timestamp,
        );
        entry.table_type = table_type;
        entry.constraints = constraints;
        entry.statistics = statistics;
        entry.storage_descriptor = Some(storage_descriptor);
        entry.base.base.set_comment(comment);
        Ok(entry)
    }
}

// --- CatalogEntry trait implementation ---

impl CatalogEntry for TableCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::Table
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
        None // Limitation: can't return reference from RwLock
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

    fn alter(&self, info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        // Handle RENAME
        if let Some(new_name) = &info.new_name {
            let new_entry = TableCatalogEntry {
                base: SchemaEntryMeta::new(
                    CatalogType::Table,
                    self.base.base.catalog.clone(),
                    self.base.schema_name.clone(),
                    new_name.clone(),
                    self.base.base.object_id,
                    self.base.base.timestamp(),
                ),
                table_type: self.table_type,
                columns: self.columns.clone(),
                constraints: self.constraints.clone(),
                storage: self.storage.clone(),
                storage_descriptor: self.storage_descriptor.clone(),
                statistics: self.statistics.clone(),
            };
            return Ok(Arc::new(new_entry));
        }

        // Handle SET COMMENT
        if let Some(comment) = &info.new_comment {
            let new_entry =
                self.clone_with_comment(Some(comment.clone()), self.base.base.timestamp());
            return Ok(Arc::new(new_entry));
        }

        Err(paro_error::not_implemented("ALTER TABLE"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn on_drop(&self) -> Result<()> {
        // Clear storage if needed
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        Ok(Arc::new(self.clone_with_statistics(
            self.statistics.clone(),
            self.base.base.timestamp(),
        )))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::Table,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.temporary = self.base.base.temporary;
        info.internal = self.base.base.internal;
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {
        // No-op
    }

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        // Note: this standard-entry serializer only persists base catalog fields.
        // Full table payload serialization is handled by `TableCatalogEntry::serialize()`.
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

// --- StandardEntry trait implementation ---

impl StandardEntry for TableCatalogEntry {
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

// --- InCatalogEntry trait implementation ---

impl InCatalogEntry for TableCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
    use paro_storage::table::table_factory::TableFactory;
    use std::io::{Cursor, Read};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock};

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_test_meta_manager() -> Arc<TabletMetaManager> {
        static NEXT_TEST_META_ROOT: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
        let root = std::env::temp_dir().join(format!(
            "paro_catalog_table_entry_meta_{}_{}",
            std::process::id(),
            NEXT_TEST_META_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).unwrap());
        Arc::new(TabletMetaManager::with_store_and_data_root(store, &root))
    }

    fn create_table_with_meta_manager(
        types: &[LogicalType],
        meta_manager: Arc<TabletMetaManager>,
    ) -> TableHandle {
        TableFactory::new(Some(meta_manager))
            .create_table(types)
            .unwrap()
    }

    fn descriptor_offset(entry_bytes: &[u8]) -> usize {
        let mut cursor = Cursor::new(entry_bytes);
        assert_eq!(
            read_u32(&mut cursor).unwrap(),
            TableCatalogEntry::SERIALIZATION_VERSION
        );
        cursor.set_position(cursor.position() + 16); // oid + timestamp

        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf).unwrap();
        let name_len = u32::from_le_bytes(len_buf) as u64;
        cursor.set_position(cursor.position() + name_len);

        cursor.read_exact(&mut len_buf).unwrap();
        let schema_len = u32::from_le_bytes(len_buf) as u64;
        cursor.set_position(cursor.position() + schema_len);

        cursor.set_position(cursor.position() + 1); // table_type

        cursor.read_exact(&mut len_buf).unwrap();
        let column_count = u32::from_le_bytes(len_buf);
        for _ in 0..column_count {
            cursor.read_exact(&mut len_buf).unwrap();
            let col_name_len = u32::from_le_bytes(len_buf) as u64;
            cursor.set_position(cursor.position() + col_name_len);

            let _ = LogicalType::deserialize(&mut cursor).unwrap();
            cursor.set_position(cursor.position() + 1); // not_null
            let _ = read_optional_string(&mut cursor).unwrap();
            let _ = read_optional_string(&mut cursor).unwrap();
        }

        let constraint_count = read_u32(&mut cursor).unwrap();
        for _ in 0..constraint_count {
            cursor.set_position(cursor.position() + 1); // constraint type
            let _ = read_indices(&mut cursor).unwrap();
            let _ = read_optional_string(&mut cursor).unwrap();
            let _ = read_optional_string(&mut cursor).unwrap();
            let mut marker = [0];
            cursor.read_exact(&mut marker).unwrap();
            if marker[0] == 1 {
                let _ = read_indices(&mut cursor).unwrap();
            }
        }

        cursor.read_exact(&mut len_buf).unwrap();
        let descriptor_len = u32::from_le_bytes(len_buf) as u64;
        assert!(descriptor_len > 0);
        cursor.position() as usize
    }

    #[test]
    fn test_column_definition() {
        let col = ColumnDefinition::new("id".to_string(), LogicalType::BigInt)
            .with_not_null()
            .with_default("0".to_string());

        assert_eq!(col.name, "id");
        assert!(col.not_null);
        assert_eq!(col.default_value, Some("0".to_string()));
    }

    #[test]
    fn test_table_catalog_entry() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_001),
            100,
        );

        assert_eq!(entry.name(), "users");
        assert_eq!(entry.schema_name(), "public");
        assert_eq!(entry.catalog_name(), "main");
        assert_eq!(entry.timestamp(), 100);
        assert_eq!(entry.columns.len(), 2);
    }

    #[test]
    fn table_entry_rejects_invalid_constraints_before_publication() {
        let info = CreateTableInfo::new(
            "main".to_string(),
            "public".to_string(),
            "invalid".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
        )
        .with_constraints(vec![Constraint::unique(Vec::new())]);
        let storage = Arc::new(create_table(&[LogicalType::Integer]));

        let error =
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(10_020), 100)
                .expect_err("empty UNIQUE constraint must be rejected");

        assert!(error.to_string().contains("invalid Unique"));
    }

    #[test]
    fn serialization_revalidates_runtime_constraints() {
        let mut entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "invalid".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            Arc::new(create_table(&[LogicalType::Integer])),
            CatalogObjectId::from_raw(10_021),
            100,
        );
        entry.constraints = vec![Constraint::foreign_key(
            vec![0],
            "parent".to_string(),
            vec![0, 1],
        )];

        let error = entry
            .serialize()
            .expect_err("mismatched foreign key must not be persisted");

        assert!(error.to_string().contains("invalid ForeignKey"));
    }

    #[test]
    fn test_to_sql() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::BigInt).with_not_null(),
            ColumnDefinition::new("email".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::BigInt, LogicalType::Varchar]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "accounts".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_002),
            100,
        );

        let sql = entry.to_sql();
        assert!(sql.contains("CREATE TABLE"));
        assert!(sql.contains("public.accounts"));
        assert!(sql.contains("NOT NULL"));
    }

    #[test]
    fn test_serialization() {
        let meta_manager = create_test_meta_manager();
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::Integer, LogicalType::Varchar],
            meta_manager.clone(),
        ));
        let expected_descriptor = storage.to_descriptor().unwrap();

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "items".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_003),
            42,
        );

        let bytes = entry.serialize().unwrap();
        let restored =
            TableCatalogEntry::deserialize(&bytes, "main".to_string(), Some(meta_manager)).unwrap();

        assert_eq!(restored.name(), "items");
        assert_eq!(restored.columns.len(), 2);
        assert_eq!(
            restored.get_storage_descriptor().unwrap(),
            &expected_descriptor
        );
        assert_eq!(
            restored.get_storage().unwrap().to_descriptor().unwrap(),
            expected_descriptor
        );
        assert_eq!(restored.object_id(), entry.object_id());
    }

    #[test]
    fn test_serialization_preserves_comment() {
        let meta_manager = create_test_meta_manager();
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::Integer],
            meta_manager.clone(),
        ));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "commented_items".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_004),
            42,
        );
        entry
            .base
            .base
            .set_comment(Some("persisted comment".to_string()));

        let bytes = entry.serialize().unwrap();
        let restored =
            TableCatalogEntry::deserialize(&bytes, "main".to_string(), Some(meta_manager)).unwrap();

        assert_eq!(
            restored.base.base.comment(),
            Some("persisted comment".to_string())
        );
    }

    #[test]
    fn serialization_preserves_columns_and_constraints() {
        let meta_manager = create_test_meta_manager();
        let columns = vec![
            ColumnDefinition::new("tenant_id".to_string(), LogicalType::BigInt)
                .with_not_null()
                .with_default("0".to_string())
                .with_comment("tenant discriminator".to_string()),
            ColumnDefinition::new("item_id".to_string(), LogicalType::BigInt),
        ];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::BigInt, LogicalType::BigInt],
            meta_manager.clone(),
        ));
        let info = CreateTableInfo::new(
            "main".to_string(),
            "public".to_string(),
            "constrained_items".to_string(),
            columns,
        )
        .with_constraints(vec![
            Constraint::primary_key(vec![0, 1]),
            Constraint::check("item_id > 0".to_string()),
        ]);
        let entry =
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(10_005), 42)
                .unwrap();

        let restored = TableCatalogEntry::deserialize(
            &entry.serialize().unwrap(),
            "main".to_string(),
            Some(meta_manager),
        )
        .unwrap();

        assert_eq!(restored.columns[0].default_value.as_deref(), Some("0"));
        assert_eq!(
            restored.columns[0].comment.as_deref(),
            Some("tenant discriminator")
        );
        assert_eq!(restored.constraints.len(), 2);
        assert_eq!(
            restored.constraints[0].constraint_type,
            ConstraintType::PrimaryKey
        );
        assert_eq!(restored.constraints[0].columns, vec![0, 1]);
        assert_eq!(
            restored.constraints[1].expression.as_deref(),
            Some("item_id > 0")
        );
    }

    #[test]
    fn test_deserialize_rejects_legacy_storage_payload() {
        let meta_manager = create_test_meta_manager();
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::Integer],
            meta_manager.clone(),
        ));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "legacy_case".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_005),
            42,
        );

        let bytes = entry.serialize().unwrap();
        let mut corrupted = bytes.clone();
        let descriptor_offset = descriptor_offset(&bytes);
        corrupted[descriptor_offset] = b'X';

        let err =
            TableCatalogEntry::deserialize(&corrupted, "main".to_string(), Some(meta_manager))
                .unwrap_err()
                .to_string();
        assert!(err.contains("invalid table storage descriptor magic"));
    }

    #[test]
    fn deserialize_rejects_oversized_string_before_allocation() {
        let meta_manager = create_test_meta_manager();
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::Integer],
            meta_manager.clone(),
        ));
        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "bounded_allocation".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_006),
            42,
        );

        let mut corrupted = entry.serialize().unwrap();
        let name_length_offset = std::mem::size_of::<u32>() + 2 * std::mem::size_of::<u64>();
        corrupted[name_length_offset..name_length_offset + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());

        let err =
            TableCatalogEntry::deserialize(&corrupted, "main".to_string(), Some(meta_manager))
                .unwrap_err()
                .to_string();
        assert!(err.contains("catalog string length"));
        assert!(err.contains("exceeds remaining payload"));
    }

    #[test]
    fn test_add_column() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_006),
            100,
        );

        // Add a new column
        let new_col = ColumnDefinition::new("email".to_string(), LogicalType::Varchar);
        let new_entry = entry.add_column(new_col, 101).unwrap();

        assert_eq!(new_entry.columns.len(), 3);
        assert_eq!(new_entry.columns[2].name, "email");
        assert_eq!(new_entry.timestamp(), 101);

        // Try to add duplicate column
        let dup_col = ColumnDefinition::new("name".to_string(), LogicalType::Varchar);
        assert!(new_entry.add_column(dup_col, 102).is_err());
    }

    #[test]
    fn test_remove_column() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
            ColumnDefinition::new("email".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_007),
            100,
        );

        // Remove a column
        let new_entry = entry.remove_column("email", 101).unwrap();

        assert_eq!(new_entry.columns.len(), 2);
        assert_eq!(new_entry.columns[0].name, "id");
        assert_eq!(new_entry.columns[1].name, "name");
        assert_eq!(new_entry.timestamp(), 101);

        // Try to remove non-existent column
        assert!(new_entry.remove_column("nonexistent", 102).is_err());

        // Try to remove last column
        let single_col = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        let single_storage = Arc::new(create_table(&[LogicalType::Integer]));
        let single_entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "single".to_string(),
            single_col,
            single_storage,
            CatalogObjectId::from_raw(10_008),
            100,
        );
        assert!(single_entry.remove_column("id", 101).is_err());
    }

    #[test]
    fn test_alter_column() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_009),
            100,
        );

        // Change column type
        let new_entry = entry
            .alter_column("id", Some(LogicalType::BigInt), None, None, 101)
            .unwrap();

        assert_eq!(new_entry.columns[0].logical_type, LogicalType::BigInt);
        assert_eq!(new_entry.timestamp(), 101);

        // Set NOT NULL
        let new_entry2 = new_entry
            .alter_column("name", None, None, Some(true), 102)
            .unwrap();

        assert!(new_entry2.columns[1].not_null);

        // Set default value
        let new_entry3 = new_entry2
            .alter_column("name", None, Some(Some("'unknown'".to_string())), None, 103)
            .unwrap();

        assert_eq!(
            new_entry3.columns[1].default_value,
            Some("'unknown'".to_string())
        );

        // Try to alter non-existent column
        assert!(new_entry3
            .alter_column("nonexistent", Some(LogicalType::Integer), None, None, 104)
            .is_err());
    }

    #[test]
    fn test_constraints() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("email".to_string(), LogicalType::Varchar),
        ];

        let constraints = vec![
            Constraint::primary_key(vec![0]),
            Constraint::unique(vec![1]),
            Constraint::not_null(1),
        ];

        let info = CreateTableInfo::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
        )
        .with_constraints(constraints);

        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let entry =
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(10_010), 100)
                .unwrap();

        assert_eq!(entry.constraints.len(), 3);
        assert_eq!(
            entry.constraints[0].constraint_type,
            ConstraintType::PrimaryKey
        );
        assert_eq!(entry.constraints[1].constraint_type, ConstraintType::Unique);
        assert_eq!(
            entry.constraints[2].constraint_type,
            ConstraintType::NotNull
        );
    }

    #[test]
    fn test_remove_column_with_constraints() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("email".to_string(), LogicalType::Varchar),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];

        let constraints = vec![
            Constraint::primary_key(vec![0]),
            Constraint::unique(vec![1]),
            Constraint::not_null(2),
        ];

        let info = CreateTableInfo::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
        )
        .with_constraints(constraints);

        let storage = Arc::new(create_table(&[
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ]));
        let entry =
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(10_011), 100)
                .unwrap();

        // Remove middle column (email)
        let new_entry = entry.remove_column("email", 101).unwrap();

        // Should have 2 columns and 2 constraints (unique constraint removed)
        assert_eq!(new_entry.columns.len(), 2);
        assert_eq!(new_entry.constraints.len(), 2);

        // Primary key should still reference column 0
        assert_eq!(new_entry.constraints[0].columns, vec![0]);

        // NOT NULL should now reference column 1 (was column 2)
        assert_eq!(new_entry.constraints[1].columns, vec![1]);
    }

    #[test]
    fn test_get_statistics() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));

        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_012),
            100,
        );

        let stats = entry.get_statistics();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().row_count, 0);
        assert_eq!(stats.unwrap().column_stats.len(), 2);
    }

    #[test]
    fn test_clone_with_new_name_preserves_table_metadata() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_013),
            100,
        );

        let renamed = entry.clone_with_new_name("users_v2".to_string(), 200);
        assert_eq!(renamed.base.base.name, "users_v2");
        assert_eq!(renamed.base.base.object_id, entry.base.base.object_id);
        assert_eq!(renamed.base.schema_name, "public");
        assert_eq!(renamed.columns.len(), 2);
        assert!(renamed.get_storage().is_some());
    }

    #[test]
    fn test_clone_with_new_schema_and_name_updates_identity() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_014),
            100,
        );

        let moved = entry.clone_with_new_schema_and_name(
            "archive".to_string(),
            "users_v2".to_string(),
            200,
        );
        assert_eq!(moved.base.schema_name, "archive");
        assert_eq!(moved.base.base.name, "users_v2");
        assert_eq!(moved.base.base.object_id, entry.base.base.object_id);
        assert!(moved.get_storage().is_some());
    }

    #[test]
    fn test_clone_with_renamed_column_updates_column_name() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_015),
            100,
        );

        let renamed = entry
            .clone_with_renamed_column("name", "display_name".to_string(), 101)
            .expect("rename column");
        assert_eq!(renamed.columns[1].name, "display_name");
        assert_eq!(renamed.base.base.name, "users");
    }

    #[test]
    fn test_clone_with_column_comments_updates_target_columns() {
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
        ];
        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let entry = TableCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            columns,
            storage,
            CatalogObjectId::from_raw(10_016),
            100,
        );

        let commented = entry
            .clone_with_column_comments(&[("name".to_string(), "displayed name".to_string())], 101)
            .expect("comment column");
        assert_eq!(
            commented.columns[1].comment.as_deref(),
            Some("displayed name")
        );
        assert!(commented.columns[0].comment.is_none());
    }
}
