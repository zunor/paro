// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Schema Entry
//!
//!
//! This module defines `SchemaEntry` and schema-level entry routing.

use super::catalog_entry::{
    allocate_object_id, CatalogEntryMeta, CatalogObjectId, CatalogType, OnCreateConflict,
};
use super::{
    AggregateFunctionCatalogEntry, CatalogEntryEnum, CopyFunctionCatalogEntry, CreateIndexInfo,
    CreatePropertyGraphInfo, CreateSequenceInfo, CreateViewInfo, IndexCatalogEntry,
    PropertyGraphCatalogEntry, ScalarFunctionCatalogEntry, SequenceCatalogEntry, TableCatalogEntry,
    TableFunctionCatalogEntry, ViewCatalogEntry,
};
use crate::collection::{CatalogCollection, EntryLookup, SimilarCatalogEntry};
use crate::default::default_functions::{DefaultFunctionGenerator, DefaultTableFunctionGenerator};
use crate::default::default_views::DefaultViewGenerator;
use crate::default::DefaultGenerator;
use crate::mvcc::CatalogSnapshot;
use crate::schema::contents::SchemaContents;
use paro_common::error::{self as paro_error, Result};
use paro_storage::meta::TabletMetaManager;
use paro_storage::tablet::Tablet;
use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

// --- Info Types ---

/// Information for looking up an entry within a schema
#[derive(Debug, Clone)]
pub struct EntryLookupInfo {
    /// The type of entry to look up
    pub catalog_type: CatalogType,
    /// The name of the entry
    pub name: String,
}

impl EntryLookupInfo {
    pub fn new(catalog_type: CatalogType, name: String) -> Self {
        Self { catalog_type, name }
    }

    pub fn table(name: String) -> Self {
        Self::new(CatalogType::Table, name)
    }

    pub fn view(name: String) -> Self {
        Self::new(CatalogType::View, name)
    }

    pub fn index(name: String) -> Self {
        Self::new(CatalogType::Index, name)
    }

    pub fn sequence(name: String) -> Self {
        Self::new(CatalogType::Sequence, name)
    }

    pub fn function(name: String) -> Self {
        Self::new(CatalogType::ScalarFunction, name)
    }

    pub fn table_function(name: String) -> Self {
        Self::new(CatalogType::TableFunction, name)
    }

    pub fn copy_function(name: String) -> Self {
        Self::new(CatalogType::CopyFunction, name)
    }

    pub fn get_catalog_type(&self) -> CatalogType {
        self.catalog_type
    }

    pub fn get_entry_name(&self) -> &str {
        &self.name
    }
}

/// Information for dropping an entry
#[derive(Debug, Clone)]
pub struct DropEntryInfo {
    /// Entry type
    pub entry_type: CatalogType,
    /// Entry name
    pub name: String,
    /// Whether to treat a missing target as a no-op
    pub if_exists: bool,
    /// Whether to cascade drop dependent objects
    pub cascade: bool,
    /// Whether to allow dropping internal entries
    pub allow_drop_internal: bool,
}

impl DropEntryInfo {
    pub fn new(entry_type: CatalogType, name: String) -> Self {
        Self {
            entry_type,
            name,
            if_exists: false,
            cascade: false,
            allow_drop_internal: false,
        }
    }

    pub fn with_if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    pub fn with_cascade(mut self) -> Self {
        self.cascade = true;
        self
    }
}

/// Information for altering an entry
#[derive(Debug, Clone)]
pub struct ColumnCommentUpdate {
    pub column_name: String,
    pub comment: String,
}

#[derive(Debug, Clone)]
pub enum AlterEntryAction {
    Move {
        new_name: String,
        new_schema: Option<String>,
    },
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },
    SetTableComment {
        new_comment: String,
    },
    SetColumnComments {
        comments: Vec<ColumnCommentUpdate>,
    },
}

#[derive(Debug, Clone)]
pub struct AlterEntryInfo {
    /// Entry type
    pub entry_type: CatalogType,
    /// Entry name
    pub name: String,
    /// Alter action
    pub action: AlterEntryAction,
    /// Allow altering internal entries
    pub allow_internal: bool,
}

impl AlterEntryInfo {
    pub fn new(entry_type: CatalogType, name: String) -> Self {
        let current_name = name.clone();
        Self {
            entry_type,
            name,
            action: AlterEntryAction::Move {
                new_name: current_name,
                new_schema: None,
            },
            allow_internal: false,
        }
    }

    pub fn with_new_name(mut self, new_name: String) -> Self {
        let new_schema = match &self.action {
            AlterEntryAction::Move { new_schema, .. } => new_schema.clone(),
            _ => None,
        };
        self.action = AlterEntryAction::Move {
            new_name,
            new_schema,
        };
        self
    }

    pub fn with_new_schema(mut self, new_schema: String) -> Self {
        let new_name = match &self.action {
            AlterEntryAction::Move { new_name, .. } => new_name.clone(),
            _ => self.name.clone(),
        };
        self.action = AlterEntryAction::Move {
            new_name,
            new_schema: Some(new_schema),
        };
        self
    }

    pub fn with_renamed_column(mut self, old_column_name: String, new_column_name: String) -> Self {
        self.action = AlterEntryAction::RenameColumn {
            old_column_name,
            new_column_name,
        };
        self
    }

    pub fn with_new_comment(mut self, new_comment: String) -> Self {
        self.action = AlterEntryAction::SetTableComment { new_comment };
        self
    }

    pub fn with_column_comment(mut self, column_name: String, comment: String) -> Self {
        self.action = AlterEntryAction::SetColumnComments {
            comments: vec![ColumnCommentUpdate {
                column_name,
                comment,
            }],
        };
        self
    }

    pub fn with_column_comments(mut self, comments: Vec<ColumnCommentUpdate>) -> Self {
        self.action = AlterEntryAction::SetColumnComments { comments };
        self
    }

    pub fn with_allow_internal(mut self) -> Self {
        self.allow_internal = true;
        self
    }
}

/// Information for creating a schema
#[derive(Debug, Clone)]
pub struct CreateSchemaInfo {
    /// Schema name
    pub name: String,
    /// Catalog name
    pub catalog: String,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is an internal schema
    pub internal: bool,
}

impl CreateSchemaInfo {
    pub fn new(catalog: String, name: String) -> Self {
        Self {
            name,
            catalog,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            internal: false,
        }
    }

    pub fn with_on_conflict(mut self, on_conflict: OnCreateConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    pub fn with_internal(mut self) -> Self {
        self.internal = true;
        self
    }
}

/// Information for dropping a schema.
///
#[derive(Debug, Clone)]
pub struct DropSchemaInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub name: String,
    /// Whether to cascade drop dependent objects
    pub cascade: bool,
    /// Whether to ignore if not exists
    pub if_exists: bool,
}

/// Schema entry (namespace).
///
/// A schema contains multiple CatalogCollections for different entry types:
/// - tables: Tables
/// - views: Views (separate from tables in Paro)
/// - indexes: Index entries
/// - functions: Scalar and aggregate functions
/// - table_functions: Table-valued functions
/// - copy_functions: Copy functions
/// - sequences: Sequence entries
/// - types: User-defined types (future)
/// - collations: Collation entries (future)
#[derive(Debug)]
pub struct SchemaEntry {
    /// Base catalog entry
    pub base: CatalogEntryMeta,
    /// Family collections contained by this schema version.
    contents: Arc<SchemaContents>,
    /// Whether this is an internal schema
    pub internal: bool,
}

impl SchemaEntry {
    /// Create a new schema entry.
    pub fn new(catalog: String, name: String, gc_epoch: Arc<AtomicU64>, timestamp: u64) -> Self {
        Self::with_object_id(catalog, name, allocate_object_id(), gc_epoch, timestamp)
    }

    fn with_contents(
        catalog: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
        contents: Arc<SchemaContents>,
    ) -> Self {
        let mut schema = Self {
            base: CatalogEntryMeta::new(
                CatalogType::Schema,
                catalog.clone(),
                name.clone(),
                object_id,
                timestamp,
            ),
            contents,
            internal: false,
        };
        crate::default::default_schemas::configure_internal_schema(&mut schema);
        schema
    }

    pub fn with_object_id(
        catalog: String,
        name: String,
        object_id: CatalogObjectId,
        gc_epoch: Arc<AtomicU64>,
        timestamp: u64,
    ) -> Self {
        let contents = Arc::new(SchemaContents::new(&catalog, &name, object_id, gc_epoch));
        Self::with_contents(catalog, name, object_id, timestamp, contents)
    }

    /// Create a new schema from CreateSchemaInfo
    pub fn from_info(info: &CreateSchemaInfo, gc_epoch: Arc<AtomicU64>, timestamp: u64) -> Self {
        Self::from_info_with_object_id(info, allocate_object_id(), gc_epoch, timestamp)
    }

    /// `object_id` must be the persisted object identity from WAL / checkpoint.
    pub fn from_info_with_object_id(
        info: &CreateSchemaInfo,
        object_id: CatalogObjectId,
        gc_epoch: Arc<AtomicU64>,
        timestamp: u64,
    ) -> Self {
        let mut schema = Self::with_object_id(
            info.catalog.clone(),
            info.name.clone(),
            object_id,
            gc_epoch,
            timestamp,
        );
        schema.internal = info.internal;
        crate::default::default_schemas::configure_internal_schema(&mut schema);
        schema
    }

    /// Check if this is an internal schema
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    pub fn base(&self) -> &CatalogEntryMeta {
        &self.base
    }

    pub fn database_name(&self) -> &str {
        &self.base.catalog
    }

    pub fn name(&self) -> &str {
        &self.base.name
    }

    pub fn collection(&self, entry_type: CatalogType) -> Option<&Arc<CatalogCollection>> {
        self.contents.collection(entry_type)
    }

    pub(crate) fn contents_owner(&self) -> &Arc<SchemaContents> {
        &self.contents
    }

    fn require_collection(&self, entry_type: CatalogType) -> Result<&Arc<CatalogCollection>> {
        self.collection(entry_type).ok_or_else(|| {
            paro_error::internal(format!(
                "No schema collection for entry type {:?}",
                entry_type
            ))
        })
    }

    /// Handle entry drop (storage shutdown mark + background cleanup queue).
    fn on_drop_entry(
        &self,
        _transaction: &CatalogSnapshot,
        entry: &CatalogEntryEnum,
    ) -> Result<()> {
        let CatalogEntryEnum::Table(table) = entry else {
            return Ok(());
        };

        if let Some(storage) = table.get_storage() {
            return storage.mark_shutdown_and_schedule_sweep(true);
        }

        if let Some(descriptor) = table.get_storage_descriptor() {
            return Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(
                &descriptor.data_dir,
                true,
            );
        }

        Ok(())
    }

    /// Add an entry to the appropriate catalog set
    fn add_entry_internal(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<CatalogEntryEnum>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let entry_name = entry.name().to_string();
        let entry_type = entry.entry_type();
        self.materialize_default_entry(entry_type, &entry_name);
        let collection = self.require_collection(entry_type)?;

        // Handle conflict modes
        if on_conflict == OnCreateConflict::IgnoreOnConflict {
            if let Some(_existing) = collection.get_entry(
                transaction.transaction_id,
                transaction.start_time,
                &entry_name,
            ) {
                return Ok(None);
            }
        }

        if on_conflict == OnCreateConflict::ReplaceOnConflict {
            // Try to drop existing entry first
            if let Some(_existing) = collection.get_entry(
                transaction.transaction_id,
                transaction.start_time,
                &entry_name,
            ) {
                let _ = collection.stage_drop(transaction, &entry_name)?;
            }
        }

        match collection.stage_create(transaction, &entry_name, entry.clone()) {
            Ok(Some(_handle)) => Ok(Some(entry)),
            Ok(None) => {
                if on_conflict == OnCreateConflict::ErrorOnConflict {
                    Err(paro_error::object_exists(entry_type.as_str(), entry_name))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    fn materialize_default_entry(&self, entry_type: CatalogType, name: &str) {
        if !self.internal {
            return;
        }

        match entry_type {
            CatalogType::View => {
                let generator =
                    DefaultViewGenerator::new(self.base.catalog.clone(), self.base.name.clone());
                let _ = self
                    .contents
                    .views
                    .create_committed_entry_lazy(name, || generator.create_default_entry(name));
            }
            CatalogType::ScalarFunction | CatalogType::AggregateFunction => {
                let generator = DefaultFunctionGenerator::new(
                    self.base.catalog.clone(),
                    self.base.name.clone(),
                );
                let _ = self
                    .contents
                    .functions
                    .create_committed_entry_lazy(name, || generator.create_default_entry(name));
            }
            CatalogType::TableFunction => {
                let generator = DefaultTableFunctionGenerator::new(
                    self.base.catalog.clone(),
                    self.base.name.clone(),
                );
                let _ = self
                    .contents
                    .table_functions
                    .create_committed_entry_lazy(name, || generator.create_default_entry(name));
            }
            _ => {}
        }
    }

    fn materialize_default_family(&self, entry_type: CatalogType) {
        if !self.internal {
            return;
        }

        match entry_type {
            CatalogType::View => {
                let generator =
                    DefaultViewGenerator::new(self.base.catalog.clone(), self.base.name.clone());
                for name in generator.get_default_entries() {
                    let _ = self.contents.views.create_committed_entry_lazy(&name, || {
                        generator.create_default_entry(&name)
                    });
                }
            }
            CatalogType::ScalarFunction | CatalogType::AggregateFunction => {
                let generator = DefaultFunctionGenerator::new(
                    self.base.catalog.clone(),
                    self.base.name.clone(),
                );
                for name in generator.get_default_entries() {
                    let _ = self
                        .contents
                        .functions
                        .create_committed_entry_lazy(&name, || {
                            generator.create_default_entry(&name)
                        });
                }
            }
            CatalogType::TableFunction => {
                let generator = DefaultTableFunctionGenerator::new(
                    self.base.catalog.clone(),
                    self.base.name.clone(),
                );
                for name in generator.get_default_entries() {
                    let _ = self
                        .contents
                        .table_functions
                        .create_committed_entry_lazy(&name, || {
                            generator.create_default_entry(&name)
                        });
                }
            }
            _ => {}
        }
    }

    pub fn materialize_default_entries(&self, _transaction: &CatalogSnapshot) {
        self.materialize_default_family(CatalogType::View);
        self.materialize_default_family(CatalogType::ScalarFunction);
        self.materialize_default_family(CatalogType::TableFunction);
    }

    // --- Lookup helpers ---

    /// Get a table from this schema.
    pub fn get_table(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        self.contents
            .tables
            .get_entry(transaction_id, start_time, name)
    }

    /// Get a scalar function set from this schema.
    pub fn get_function(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        let entry = self
            .contents
            .functions
            .get_entry(transaction_id, start_time, name);
        if entry.is_some() {
            return entry;
        }
        self.materialize_default_entry(CatalogType::ScalarFunction, name);
        self.contents
            .functions
            .get_entry(transaction_id, start_time, name)
    }

    /// Get a table function from this schema.
    pub fn get_table_function(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        let entry = self
            .contents
            .table_functions
            .get_entry(transaction_id, start_time, name);
        if entry.is_some() {
            return entry;
        }
        self.materialize_default_entry(CatalogType::TableFunction, name);
        self.contents
            .table_functions
            .get_entry(transaction_id, start_time, name)
    }

    /// Get a copy function from this schema.
    pub fn get_copy_function(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        self.contents
            .copy_functions
            .get_entry(transaction_id, start_time, name)
    }

    /// Get a view from this schema.
    pub fn get_view(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        let entry = self
            .contents
            .views
            .get_entry(transaction_id, start_time, name);
        if entry.is_some() {
            return entry;
        }
        self.materialize_default_entry(CatalogType::View, name);
        self.contents
            .views
            .get_entry(transaction_id, start_time, name)
    }

    /// Get a sequence from this schema.
    pub fn get_sequence(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        self.contents
            .sequences
            .get_entry(transaction_id, start_time, name)
    }

    /// Get an index from this schema.
    pub fn get_index(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        self.contents
            .indexes
            .get_entry(transaction_id, start_time, name)
    }

    pub fn indexes_for_table(
        &self,
        transaction: &CatalogSnapshot,
        table_oid: CatalogObjectId,
    ) -> Vec<Arc<IndexCatalogEntry>> {
        let mut indexes = Vec::new();
        self.scan(transaction, CatalogType::Index, |entry| {
            if let CatalogEntryEnum::Index(index) = entry {
                if index.table_oid == table_oid.raw() {
                    indexes.push(Arc::clone(index));
                }
            }
        });
        indexes
    }

    /// Create a property graph entry in this schema.
    pub fn create_property_graph(
        &self,
        transaction: &CatalogSnapshot,
        mut info: CreatePropertyGraphInfo,
    ) -> Result<()> {
        if info.schema != self.base.name {
            return Err(paro_error::catalog(format!(
                "Schema mismatch for property graph: expected \"{}\", got \"{}\"",
                self.base.name, info.schema
            )));
        }
        if info.catalog.is_empty() {
            info.catalog = self.base.catalog.clone();
        }
        if info.catalog != self.base.catalog {
            return Err(paro_error::catalog(format!(
                "Catalog mismatch for property graph: expected \"{}\", got \"{}\"",
                self.base.catalog, info.catalog
            )));
        }

        if let Some(_existing) = self.contents.property_graphs.get_entry(
            transaction.transaction_id,
            transaction.start_time,
            &info.graph_name,
        ) {
            if info.if_not_exists {
                return Ok(());
            }
            return Err(paro_error::object_exists(
                "property graph",
                &info.graph_name,
            ));
        }

        let entry = Arc::new(PropertyGraphCatalogEntry::new(
            info.clone(),
            0,
            self.base.catalog.clone(),
        ));
        let catalog_entry = Arc::new(CatalogEntryEnum::PropertyGraph(entry));
        match self.contents.property_graphs.stage_create(
            transaction,
            &info.graph_name,
            catalog_entry,
        )? {
            Some(_handle) => Ok(()),
            None if info.if_not_exists => Ok(()),
            None => Err(paro_error::object_exists(
                "property graph",
                &info.graph_name,
            )),
        }
    }

    /// Get a property graph entry by name.
    pub fn get_property_graph(
        &self,
        transaction: &CatalogSnapshot,
        name: &str,
    ) -> Result<Arc<PropertyGraphCatalogEntry>> {
        match self.contents.property_graphs.get_entry(
            transaction.transaction_id,
            transaction.start_time,
            name,
        ) {
            Some(entry) => match entry.as_ref() {
                CatalogEntryEnum::PropertyGraph(graph) => Ok(graph.clone()),
                _ => Err(paro_error::internal(format!(
                    "Entry \"{}\" is not a property graph",
                    name
                ))),
            },
            None => Err(paro_error::object_not_found("property graph", name)),
        }
    }

    /// Drop a property graph entry by name.
    pub fn drop_property_graph(
        &self,
        transaction: &CatalogSnapshot,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let dropped = self
            .contents
            .property_graphs
            .stage_drop(transaction, name)?
            .is_some();
        if !dropped && !if_exists {
            return Err(paro_error::object_not_found("property graph", name));
        }
        Ok(())
    }

    // --- Serialization ---

    pub fn serialize_metadata(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.base.serialize(&mut buffer)?;
        Ok(buffer)
    }

    pub fn serialize_contents(&self) -> Result<Vec<u8>> {
        self.serialize_contents_at(u64::MAX)
    }

    pub fn serialize_contents_at(&self, snapshot_ts: u64) -> Result<Vec<u8>> {
        self.contents.serialize_payload_at(snapshot_ts)
    }

    pub fn serialize_at(&self, snapshot_ts: u64) -> Result<Vec<u8>> {
        let metadata = self.serialize_metadata()?;
        let contents = self.serialize_contents_at(snapshot_ts)?;

        let mut buffer = Vec::new();
        buffer.write_all(&(metadata.len() as u32).to_le_bytes())?;
        buffer.write_all(&metadata)?;
        buffer.write_all(&(contents.len() as u32).to_le_bytes())?;
        buffer.write_all(&contents)?;
        Ok(buffer)
    }

    /// Persist the schema entry to disk
    pub fn serialize(&self) -> Result<Vec<u8>> {
        self.serialize_at(u64::MAX)
    }

    pub fn deserialize_metadata(bytes: &[u8], gc_epoch: Arc<AtomicU64>) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut oid_buf = [0u8; 8];
        cursor.read_exact(&mut oid_buf)?;
        let oid = u64::from_le_bytes(oid_buf);

        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf)?;
        let timestamp = u64::from_le_bytes(ts_buf);

        let mut entry_type_buf = [0u8; 1];
        cursor.read_exact(&mut entry_type_buf)?;
        if entry_type_buf[0] != CatalogType::Schema as u8 {
            return Err(paro_error::internal(format!(
                "Expected schema metadata tag {}, found {}",
                CatalogType::Schema as u8,
                entry_type_buf[0]
            )));
        }

        let mut flags_buf = [0u8; 1];
        cursor.read_exact(&mut flags_buf)?;
        let flags = flags_buf[0];
        let internal = (flags & 1) != 0;
        let temporary = (flags & 2) != 0;

        let name = Self::read_string(&mut cursor, "schema name")?;
        let catalog = Self::read_string(&mut cursor, "catalog name")?;
        let comment = Self::read_optional_string(&mut cursor, "schema comment")?;
        let tags = Self::read_string_map(&mut cursor)?;

        if cursor.position() != bytes.len() as u64 {
            return Err(paro_error::internal(format!(
                "Schema metadata payload has trailing bytes: {}",
                bytes.len() as u64 - cursor.position()
            )));
        }

        let mut schema = Self::with_object_id(
            catalog,
            name,
            CatalogObjectId::from_raw(oid),
            gc_epoch,
            timestamp,
        );
        schema.base.internal = internal;
        schema.base.temporary = temporary;
        schema.base.set_comment(comment);
        schema.base.set_tags(tags);
        schema.internal = internal;
        if schema.internal {
            crate::default::default_schemas::configure_internal_schema(&mut schema);
        }
        Ok(schema)
    }

    /// Deserialize schema entry from bytes
    /// Deserialize schema entry from bytes
    pub fn deserialize(
        bytes: &[u8],
        _catalog: String,
        meta_manager: Option<Arc<TabletMetaManager>>,
        gc_epoch: Arc<AtomicU64>,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let metadata = Self::read_u32_payload(&mut cursor, "schema metadata")?;
        let contents = Self::read_u32_payload(&mut cursor, "schema contents")?;

        let mut schema = Self::deserialize_metadata(&metadata, gc_epoch)?;
        schema.install_contents(&contents, meta_manager)?;

        if cursor.position() != bytes.len() as u64 {
            return Err(paro_error::internal(format!(
                "Schema payload has trailing bytes: {}",
                bytes.len() as u64 - cursor.position()
            )));
        }

        Ok(schema)
    }

    pub fn install_contents(
        &mut self,
        payload: &[u8],
        meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Result<()> {
        self.contents
            .install_serialized_payload(payload, &self.base.catalog, meta_manager)
    }

    fn read_u32_payload(cursor: &mut Cursor<&[u8]>, field: &str) -> Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        cursor.read_exact(&mut payload).map_err(|err| {
            paro_error::internal(format!("Failed to read {} payload: {}", field, err))
        })?;
        Ok(payload)
    }

    fn read_string(cursor: &mut Cursor<&[u8]>, field: &str) -> Result<String> {
        let bytes = Self::read_u32_payload(cursor, field)?;
        String::from_utf8(bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in {}: {}", field, e)))
    }

    fn read_optional_string(cursor: &mut Cursor<&[u8]>, field: &str) -> Result<Option<String>> {
        let mut present_buf = [0u8; 1];
        cursor.read_exact(&mut present_buf)?;
        match present_buf[0] {
            0 => Ok(None),
            1 => Self::read_string(cursor, field).map(Some),
            value => Err(paro_error::internal(format!(
                "Invalid optional string flag for {}: {}",
                field, value
            ))),
        }
    }

    fn read_string_map(
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<std::collections::HashMap<String, String>> {
        let mut count_buf = [0u8; 4];
        cursor.read_exact(&mut count_buf)?;
        let count = u32::from_le_bytes(count_buf) as usize;
        let mut tags = std::collections::HashMap::with_capacity(count);
        for _ in 0..count {
            let key = Self::read_string(cursor, "schema tag key")?;
            let value = Self::read_string(cursor, "schema tag value")?;
            tags.insert(key, value);
        }
        Ok(tags)
    }
}

impl SchemaEntry {
    pub fn create_table(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<TableCatalogEntry>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let catalog_entry = Arc::new(CatalogEntryEnum::Table(entry));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_index(
        &self,
        transaction: &CatalogSnapshot,
        info: CreateIndexInfo,
        table: &TableCatalogEntry,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let writer_id = transaction.write_timestamp()?;
        // Check for duplicate index name.
        // Retry path: if previous build failed on the same table, replace that failed entry.
        if let Some(existing_entry) =
            self.contents
                .indexes
                .get_entry(writer_id, transaction.start_time, &info.name)
        {
            let can_retry = match existing_entry.as_ref() {
                CatalogEntryEnum::Index(existing_index) => {
                    existing_index.is_failed()
                        && existing_index.table_oid == table.base.base.object_id.raw()
                }
                _ => false,
            };

            if can_retry {
                let _ = self.contents.indexes.stage_drop(transaction, &info.name)?;
            } else {
                // Handle IF NOT EXISTS
                if info.if_not_exists {
                    return Ok(None);
                }
                return Err(paro_error::object_exists("index", &info.name));
            }
        }

        // Verify the table exists and matches
        if table.base.base.name != info.table_name {
            return Err(paro_error::catalog(format!(
                "Table name mismatch: expected \"{}\", got \"{}\"",
                info.table_name, table.base.base.name
            )));
        }

        let index_entry = IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            writer_id,
            self.base.catalog.clone(),
        );

        let catalog_entry = Arc::new(CatalogEntryEnum::Index(Arc::new(index_entry)));
        self.add_entry_internal(
            transaction,
            catalog_entry,
            OnCreateConflict::ErrorOnConflict,
        )
    }

    pub fn create_view(
        &self,
        transaction: &CatalogSnapshot,
        info: CreateViewInfo,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let timestamp = transaction.write_timestamp()?;

        let view_entry = ViewCatalogEntry::new(info, timestamp, self.base.catalog.clone());
        let catalog_entry = Arc::new(CatalogEntryEnum::View(Arc::new(view_entry)));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_sequence(
        &self,
        transaction: &CatalogSnapshot,
        info: CreateSequenceInfo,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let timestamp = transaction.write_timestamp()?;

        let seq_entry = SequenceCatalogEntry::new(info, timestamp, self.base.catalog.clone())?;
        let catalog_entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(seq_entry)));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_table_function(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<TableFunctionCatalogEntry>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let catalog_entry = Arc::new(CatalogEntryEnum::TableFunction(entry));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_copy_function(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<CopyFunctionCatalogEntry>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let catalog_entry = Arc::new(CatalogEntryEnum::CopyFunction(entry));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_scalar_function(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<ScalarFunctionCatalogEntry>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        // Handle ALTER_ON_CONFLICT for functions
        if on_conflict == OnCreateConflict::AlterOnConflict {
            let existing = self.contents.functions.get_entry(
                transaction.transaction_id,
                transaction.start_time,
                &entry.base.base.name,
            );
            if existing.is_some() {
                // For now, just replace the function
                // Full implementation would merge function overloads
                let catalog_entry = Arc::new(CatalogEntryEnum::ScalarFunction(entry));
                return self.add_entry_internal(
                    transaction,
                    catalog_entry,
                    OnCreateConflict::ReplaceOnConflict,
                );
            }
        }

        let catalog_entry = Arc::new(CatalogEntryEnum::ScalarFunction(entry));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn create_aggregate_function(
        &self,
        transaction: &CatalogSnapshot,
        entry: Arc<AggregateFunctionCatalogEntry>,
        on_conflict: OnCreateConflict,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        // Handle ALTER_ON_CONFLICT for functions
        if on_conflict == OnCreateConflict::AlterOnConflict {
            let existing = self.contents.functions.get_entry(
                transaction.transaction_id,
                transaction.start_time,
                &entry.base.base.name,
            );
            if existing.is_some() {
                let catalog_entry = Arc::new(CatalogEntryEnum::AggregateFunction(entry));
                return self.add_entry_internal(
                    transaction,
                    catalog_entry,
                    OnCreateConflict::ReplaceOnConflict,
                );
            }
        }

        let catalog_entry = Arc::new(CatalogEntryEnum::AggregateFunction(entry));
        self.add_entry_internal(transaction, catalog_entry, on_conflict)
    }

    pub fn lookup_entry(
        &self,
        transaction: &CatalogSnapshot,
        lookup_info: &EntryLookupInfo,
    ) -> Option<Arc<CatalogEntryEnum>> {
        if matches!(
            lookup_info.catalog_type,
            CatalogType::View
                | CatalogType::ScalarFunction
                | CatalogType::AggregateFunction
                | CatalogType::TableFunction
        ) {
            self.materialize_default_entry(lookup_info.catalog_type, &lookup_info.name);
        }
        let set = self.collection(lookup_info.catalog_type)?;
        set.get_entry(
            transaction.transaction_id,
            transaction.start_time,
            &lookup_info.name,
        )
    }

    pub fn lookup_entry_detailed(
        &self,
        transaction: &CatalogSnapshot,
        lookup_info: &EntryLookupInfo,
    ) -> EntryLookup {
        if matches!(
            lookup_info.catalog_type,
            CatalogType::View
                | CatalogType::ScalarFunction
                | CatalogType::AggregateFunction
                | CatalogType::TableFunction
        ) {
            self.materialize_default_entry(lookup_info.catalog_type, &lookup_info.name);
        }
        match self.collection(lookup_info.catalog_type) {
            Some(set) => set.get_entry_detailed(
                transaction.transaction_id,
                transaction.start_time,
                &lookup_info.name,
            ),
            None => EntryLookup {
                result: None,
                reason: crate::collection::EntryLookupFailure::NotPresent,
            },
        }
    }

    pub fn get_similar_entry(
        &self,
        transaction: &CatalogSnapshot,
        lookup_info: &EntryLookupInfo,
    ) -> SimilarCatalogEntry {
        if matches!(
            lookup_info.catalog_type,
            CatalogType::View
                | CatalogType::ScalarFunction
                | CatalogType::AggregateFunction
                | CatalogType::TableFunction
        ) {
            self.materialize_default_family(lookup_info.catalog_type);
        }
        match self.collection(lookup_info.catalog_type) {
            Some(set) => set.similar_entry(
                transaction.transaction_id,
                transaction.start_time,
                &lookup_info.name,
            ),
            None => SimilarCatalogEntry::default(),
        }
    }

    pub fn drop_entry(&self, transaction: &CatalogSnapshot, info: &DropEntryInfo) -> Result<bool> {
        let writer_id = transaction.write_timestamp()?;
        let set = self.require_collection(info.entry_type)?;

        // Check if entry exists
        let existing = set.get_entry(writer_id, transaction.start_time, &info.name);

        if existing.is_none() {
            return Ok(false);
        }

        // Check if dropping internal entry is allowed
        if let Some(entry) = &existing {
            if entry.is_internal() && !info.allow_drop_internal {
                return Err(paro_error::catalog(format!(
                    "Cannot drop internal {} \"{}\"",
                    info.entry_type.as_str(),
                    info.name
                )));
            }
        }

        // Handle on_drop for tables (clear local storage)
        if info.entry_type == CatalogType::Table {
            self.on_drop_entry(transaction, existing.as_ref().unwrap())?;
        }

        Ok(set.stage_drop(transaction, &info.name)?.is_some())
    }

    pub fn alter_entry(
        &self,
        transaction: &CatalogSnapshot,
        info: &AlterEntryInfo,
    ) -> Result<bool> {
        let writer_id = transaction.write_timestamp()?;
        let set = self.require_collection(info.entry_type)?;

        // Check if entry exists
        let existing = set.get_entry(writer_id, transaction.start_time, &info.name);

        if existing.is_none() {
            return Ok(false);
        }

        let existing_entry = existing.unwrap();

        // Check if altering internal entry is allowed
        if existing_entry.is_internal() && !info.allow_internal {
            return Err(paro_error::catalog(format!(
                "Cannot alter internal {} \"{}\"",
                info.entry_type.as_str(),
                info.name
            )));
        }

        match &info.action {
            AlterEntryAction::Move {
                new_name,
                new_schema,
            } => {
                if let Some(new_schema) = new_schema {
                    if !new_schema.eq_ignore_ascii_case(self.name()) {
                        // SET SCHEMA would require moving the entry to a different schema
                        // This is complex and requires coordination with the parent catalog
                        return Err(paro_error::not_implemented(
                            "ALTER SET SCHEMA is not yet implemented",
                        ));
                    }
                }

                if set
                    .get_entry(writer_id, transaction.start_time, new_name)
                    .is_some()
                {
                    return Err(paro_error::object_exists(
                        info.entry_type.as_str(),
                        new_name,
                    ));
                }

                let new_entry = match existing_entry.as_ref() {
                    CatalogEntryEnum::Table(table) => Arc::new(CatalogEntryEnum::Table(Arc::new(
                        table.clone_with_new_schema_and_name(
                            table.base.schema_name.clone(),
                            new_name.clone(),
                            0,
                        ),
                    ))),
                    _ => {
                        return Err(paro_error::not_supported(format!(
                            "ALTER {} RENAME is not supported yet",
                            info.entry_type.as_str()
                        )))
                    }
                };

                Ok(set
                    .stage_rename(transaction, &info.name, new_name, new_entry)?
                    .is_some())
            }
            _ => Ok(true),
        }
    }

    pub fn scan<F>(&self, transaction: &CatalogSnapshot, entry_type: CatalogType, mut callback: F)
    where
        F: FnMut(&CatalogEntryEnum),
    {
        self.materialize_default_family(entry_type);
        if let Some(set) = self.collection(entry_type) {
            set.scan_with_callback(
                transaction.transaction_id,
                transaction.start_time,
                |entry| callback(entry),
            );
        }
    }

    pub fn scan_committed<F>(&self, entry_type: CatalogType, mut callback: F)
    where
        F: FnMut(&CatalogEntryEnum),
    {
        self.materialize_default_family(entry_type);
        if let Some(set) = self.collection(entry_type) {
            for entry in set.scan_committed() {
                callback(&entry);
            }
        }
    }

    pub fn verify(&self) -> Result<()> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let mut object_ids = HashSet::new();
        let mut table_names = HashSet::new();
        let mut table_oids = HashSet::new();

        if let Some(tables) = self.collection(CatalogType::Table) {
            for entry in tables.scan(txn.transaction_id, txn.start_time) {
                self.verify_entry(entry.as_ref(), &mut object_ids)?;
                if let Some(table) = entry.as_table() {
                    table_names.insert(table.base.base.name.to_lowercase());
                    table_oids.insert(table.base.base.object_id.raw());
                }
            }
        }

        for kind in [
            CatalogType::View,
            CatalogType::Index,
            CatalogType::PropertyGraph,
            CatalogType::ScalarFunction,
            CatalogType::TableFunction,
            CatalogType::CopyFunction,
            CatalogType::Sequence,
            CatalogType::Type,
            CatalogType::Collation,
        ] {
            let Some(collection) = self.collection(kind) else {
                continue;
            };
            for entry in collection.scan(txn.transaction_id, txn.start_time) {
                self.verify_entry(entry.as_ref(), &mut object_ids)?;
                if let Some(index) = entry.as_index() {
                    let has_target = table_oids.contains(&index.table_oid)
                        || table_names.contains(&index.table_name.to_lowercase());
                    if !has_target {
                        return Err(paro_error::internal(format!(
                            "schema {} contains index {} that references missing table {} ({})",
                            self.base.name, index.base.base.name, index.table_name, index.table_oid
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn verify_entry(
        &self,
        entry: &CatalogEntryEnum,
        object_ids: &mut HashSet<CatalogObjectId>,
    ) -> Result<()> {
        if entry.name().is_empty() {
            return Err(paro_error::internal(format!(
                "schema {} contains {:?} entry with empty name",
                self.base.name,
                entry.entry_type()
            )));
        }
        if entry.catalog_name() != self.base.catalog {
            return Err(paro_error::internal(format!(
                "schema {} contains {:?} entry {} in catalog {} instead of {}",
                self.base.name,
                entry.entry_type(),
                entry.name(),
                entry.catalog_name(),
                self.base.catalog
            )));
        }
        if entry.schema_name() != Some(self.base.name.as_str()) {
            return Err(paro_error::internal(format!(
                "schema {} contains {:?} entry {} in schema {:?}",
                self.base.name,
                entry.entry_type(),
                entry.name(),
                entry.schema_name()
            )));
        }

        let object_id = entry.object_id();
        if !object_ids.insert(object_id) {
            return Err(paro_error::internal(format!(
                "schema {} reuses catalog object id {}",
                self.base.name,
                object_id.raw()
            )));
        }

        Ok(())
    }

    pub fn copy(&self) -> Result<SchemaEntry> {
        let mut new_schema = SchemaEntry::with_contents(
            self.base.catalog.clone(),
            self.base.name.clone(),
            self.base.object_id,
            self.base.timestamp(),
            Arc::clone(&self.contents),
        );
        new_schema.base.internal = self.base.internal;
        new_schema.base.temporary = self.base.temporary;
        new_schema.base.set_comment(self.base.comment());
        new_schema.base.set_tags(self.base.tags());
        new_schema.internal = self.internal;
        Ok(new_schema)
    }

    pub fn update_entry_timestamp(&self, entry_name: &str, commit_id: u64) {
        // Try to update in each catalog set
        // The entry could be a table, view, index, sequence, or function
        self.contents.tables.update_timestamp(entry_name, commit_id);
        self.contents.views.update_timestamp(entry_name, commit_id);
        self.contents
            .indexes
            .update_timestamp(entry_name, commit_id);
        self.contents
            .property_graphs
            .update_timestamp(entry_name, commit_id);
        self.contents
            .sequences
            .update_timestamp(entry_name, commit_id);
        self.contents
            .functions
            .update_timestamp(entry_name, commit_id);
        self.contents
            .table_functions
            .update_timestamp(entry_name, commit_id);
        self.contents.types.update_timestamp(entry_name, commit_id);
        self.contents
            .collations
            .update_timestamp(entry_name, commit_id);
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{ColumnDefinition, EdgeTableInfo, VertexTableInfo};
    use paro_common::types::LogicalType;
    use paro_function::table::{TableFunction, TableFunctionSet};
    use paro_parser::parse_one;
    use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use paro_storage::transaction::manager::TRANSACTION_ID_START;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock};

    fn create_test_meta_manager() -> Arc<TabletMetaManager> {
        static NEXT_TEST_META_ROOT: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
        let root = std::env::temp_dir().join(format!(
            "paro_catalog_schema_entry_meta_{}_{}",
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

    fn parse_query(sql: &str) -> Box<paro_parser::ast::Query> {
        match parse_one(sql).expect("Failed to parse SQL").stmt {
            paro_parser::ast::Statement::Query(q) => q,
            _ => panic!("Expected a SELECT statement"),
        }
    }

    fn test_schema(name: &str, timestamp: u64) -> SchemaEntry {
        SchemaEntry::new(
            "test_catalog".to_string(),
            name.to_string(),
            Arc::new(AtomicU64::new(0)),
            timestamp,
        )
    }

    fn deserialize_test_schema(
        bytes: &[u8],
        meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> SchemaEntry {
        SchemaEntry::deserialize(
            bytes,
            "test_catalog".to_string(),
            meta_manager,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap()
    }

    fn schema_from_info(info: &CreateSchemaInfo, timestamp: u64) -> SchemaEntry {
        SchemaEntry::from_info(info, Arc::new(AtomicU64::new(0)), timestamp)
    }

    #[test]
    fn test_schema_catalog_entry_new() {
        let schema = test_schema("public", 0);

        assert_eq!(schema.base.name, "public");
        assert_eq!(schema.base.catalog, "test_catalog");
        assert_eq!(schema.base.entry_type, CatalogType::Schema);
    }

    #[test]
    fn test_collection_accessors() {
        let schema = test_schema("public", 0);

        assert!(schema.collection(CatalogType::Table).is_some());
        assert!(schema.collection(CatalogType::View).is_some());
        assert!(schema.collection(CatalogType::Index).is_some());
        assert!(schema.collection(CatalogType::PropertyGraph).is_some());
        assert!(schema.collection(CatalogType::Sequence).is_some());
        assert!(schema.collection(CatalogType::ScalarFunction).is_some());
        assert!(schema.collection(CatalogType::TableFunction).is_some());
        assert!(schema.collection(CatalogType::CopyFunction).is_some());
        assert!(schema.collection(CatalogType::Schema).is_none());
    }

    #[test]
    fn test_create_view() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let query = parse_query("SELECT id, name FROM users");
        let info = CreateViewInfo::new("public".to_string(), "user_view".to_string(), query);

        let result = schema.create_view(&txn, info, OnCreateConflict::ErrorOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify view exists
        let lookup = EntryLookupInfo::view("user_view".to_string());
        let entry = schema.lookup_entry(&txn, &lookup);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().entry_type(), CatalogType::View);
    }

    #[test]
    fn test_create_sequence() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let info = CreateSequenceInfo::new("public".to_string(), "my_seq".to_string())
            .with_start_value(100)
            .with_increment(10);

        let result = schema.create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify sequence exists
        let lookup = EntryLookupInfo::sequence("my_seq".to_string());
        let entry = schema.lookup_entry(&txn, &lookup);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().entry_type(), CatalogType::Sequence);
    }

    #[test]
    fn test_create_table_function() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let mut set = TableFunctionSet::new("generate_series");
        set.add_function(TableFunction::new(
            "generate_series",
            vec![LogicalType::BigInt, LogicalType::BigInt],
        ));

        let entry = Arc::new(TableFunctionCatalogEntry::new(
            "test_catalog".to_string(),
            "public".to_string(),
            set,
            0,
        ));

        let result = schema.create_table_function(&txn, entry, OnCreateConflict::ErrorOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify function exists
        let lookup = EntryLookupInfo::table_function("generate_series".to_string());
        let entry = schema.lookup_entry(&txn, &lookup);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().entry_type(), CatalogType::TableFunction);
    }

    #[test]
    fn test_schema_table_descriptor_roundtrip() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let meta_manager = create_test_meta_manager();

        let columns = vec![ColumnDefinition::new("id".to_string(), LogicalType::BigInt)];
        let storage = Arc::new(create_table_with_meta_manager(
            &[LogicalType::BigInt],
            meta_manager.clone(),
        ));
        let expected_descriptor = storage.to_descriptor().unwrap();

        let table = Arc::new(TableCatalogEntry::new(
            "test_catalog".to_string(),
            "public".to_string(),
            "descriptor_tbl".to_string(),
            columns,
            storage,
            0,
        ));
        schema
            .create_table(&txn, table, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let bytes = schema.serialize().unwrap();
        let restored = deserialize_test_schema(&bytes, Some(meta_manager));

        let lookup = EntryLookupInfo::table("descriptor_tbl".to_string());
        let restored_entry = restored.lookup_entry(&txn, &lookup).unwrap();
        let restored_table = restored_entry.as_table().unwrap();

        assert_eq!(
            restored_table.get_storage_descriptor().unwrap(),
            &expected_descriptor
        );
        assert_eq!(
            restored_table
                .get_storage()
                .unwrap()
                .to_descriptor()
                .unwrap(),
            expected_descriptor
        );
    }

    #[test]
    fn test_schema_metadata_roundtrip_restores_views_and_sequences() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let view_info = CreateViewInfo::new(
            "public".to_string(),
            "active_users".to_string(),
            parse_query("SELECT 42 AS answer"),
        );
        schema
            .create_view(&txn, view_info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let sequence_info = CreateSequenceInfo::new("public".to_string(), "detail_seq".to_string())
            .with_start_value(7)
            .with_increment(3)
            .with_min_value(5)
            .with_max_value(99)
            .with_cycle();
        schema
            .create_sequence(&txn, sequence_info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let bytes = schema.serialize().unwrap();
        let restored = deserialize_test_schema(&bytes, None);

        let view_lookup = EntryLookupInfo::view("active_users".to_string());
        let view_entry = restored
            .lookup_entry(&txn, &view_lookup)
            .expect("view should survive schema roundtrip");
        assert_eq!(view_entry.entry_type(), CatalogType::View);

        let sequence_lookup = EntryLookupInfo::sequence("detail_seq".to_string());
        let sequence_entry = restored
            .lookup_entry(&txn, &sequence_lookup)
            .expect("sequence should survive schema roundtrip");
        let restored_sequence = sequence_entry.as_sequence().unwrap();
        let data = restored_sequence.get_data();
        assert_eq!(data.start_value, 7);
        assert_eq!(data.increment, 3);
        assert_eq!(data.min_value, 5);
        assert_eq!(data.max_value, 99);
        assert!(data.cycle);
    }

    #[test]
    fn test_schema_metadata_roundtrip_preserves_object_ids() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let view_info = CreateViewInfo::new(
            "public".to_string(),
            "oid_view".to_string(),
            parse_query("SELECT 7 AS id"),
        );
        schema
            .create_view(&txn, view_info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let sequence_info = CreateSequenceInfo::new("public".to_string(), "oid_seq".to_string());
        schema
            .create_sequence(&txn, sequence_info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let _original_schema_oid = schema.base.object_id;
        let original_view_oid = schema
            .lookup_entry(&txn, &EntryLookupInfo::view("oid_view".to_string()))
            .unwrap()
            .object_id();
        let original_sequence_oid = schema
            .lookup_entry(&txn, &EntryLookupInfo::sequence("oid_seq".to_string()))
            .unwrap()
            .object_id();

        let bytes = schema.serialize().unwrap();
        let restored = deserialize_test_schema(&bytes, None);

        assert_eq!(restored.base.object_id, schema.base.object_id);
        assert_eq!(
            restored
                .lookup_entry(&txn, &EntryLookupInfo::view("oid_view".to_string()))
                .unwrap()
                .object_id(),
            original_view_oid
        );
        assert_eq!(
            restored
                .lookup_entry(&txn, &EntryLookupInfo::sequence("oid_seq".to_string()))
                .unwrap()
                .object_id(),
            original_sequence_oid
        );
    }

    #[test]
    fn test_schema_metadata_roundtrip_restores_indexes() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let index_entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            CreateIndexInfo::new(
                "public".to_string(),
                "roundtrip_table".to_string(),
                "roundtrip_idx".to_string(),
                vec![crate::entry::LogicalIndex::new(0)],
                vec![LogicalType::Integer],
            )
            .with_catalog("test_catalog".to_string())
            .with_build_state(crate::entry::IndexBuildState::Ready),
            77,
            0,
            "test_catalog".to_string(),
        ))));
        schema
            .contents
            .indexes
            .install_committed(index_entry, crate::collection::InstallMode::RejectExisting)
            .unwrap();

        let original_index_oid = schema
            .lookup_entry(&txn, &EntryLookupInfo::index("roundtrip_idx".to_string()))
            .unwrap()
            .object_id();

        let bytes = schema.serialize().unwrap();
        let restored = deserialize_test_schema(&bytes, None);
        let restored_index = restored
            .lookup_entry(&txn, &EntryLookupInfo::index("roundtrip_idx".to_string()))
            .expect("index should survive schema roundtrip");

        assert_eq!(restored_index.object_id(), original_index_oid);
        assert_eq!(restored_index.entry_type(), CatalogType::Index);
    }

    #[test]
    fn test_schema_copy_shares_contents() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let view_info = CreateViewInfo::new(
            "public".to_string(),
            "copied_view".to_string(),
            parse_query("SELECT 99 AS id"),
        );
        schema
            .create_view(&txn, view_info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        let copied = schema.copy().unwrap();
        assert!(Arc::ptr_eq(&schema.contents, &copied.contents));
        assert!(copied
            .lookup_entry(&txn, &EntryLookupInfo::view("copied_view".to_string()))
            .is_some());
    }

    #[test]
    fn test_internal_schema_deserialize_restores_default_generators() {
        let schema = test_schema("pg_catalog", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let bytes = schema.serialize().unwrap();
        let restored = deserialize_test_schema(&bytes, None);

        assert!(restored.is_internal());

        let lookup = EntryLookupInfo::view("pg_prepared_statements".to_string());
        let entry = restored.lookup_entry(&txn, &lookup).unwrap();
        assert_eq!(entry.entry_type(), CatalogType::View);

        let table_function_lookup =
            EntryLookupInfo::table_function("paro_pg_prepared_statements".to_string());
        let table_function = restored
            .lookup_entry(&txn, &table_function_lookup)
            .expect("pg_catalog table function should be restored lazily after deserialize");
        assert_eq!(table_function.entry_type(), CatalogType::TableFunction);
    }

    #[test]
    fn test_drop_entry() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create a sequence
        let info = CreateSequenceInfo::new("public".to_string(), "drop_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Verify it exists
        let lookup = EntryLookupInfo::sequence("drop_seq".to_string());
        assert!(schema.lookup_entry(&txn, &lookup).is_some());

        // Drop it
        let drop_info = DropEntryInfo::new(CatalogType::Sequence, "drop_seq".to_string());
        let result = schema.drop_entry(&txn, &drop_info);
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Verify it's gone
        assert!(schema.lookup_entry(&txn, &lookup).is_none());
    }

    #[test]
    fn test_scan_entries() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create multiple sequences
        for i in 1..=3 {
            let info = CreateSequenceInfo::new("public".to_string(), format!("seq_{}", i));
            schema
                .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
                .unwrap();
        }

        // Scan sequences
        let mut count = 0;
        schema.scan(&txn, CatalogType::Sequence, |_entry| {
            count += 1;
        });
        assert_eq!(count, 3);
    }

    #[test]
    fn test_on_create_conflict_ignore() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create first sequence
        let info = CreateSequenceInfo::new("public".to_string(), "dup_seq".to_string());
        let result = schema.create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Try to create duplicate with IGNORE
        let info = CreateSequenceInfo::new("public".to_string(), "dup_seq".to_string());
        let result = schema.create_sequence(&txn, info, OnCreateConflict::IgnoreOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should return None, not error
    }

    #[test]
    fn test_on_create_conflict_error() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create first sequence
        let info = CreateSequenceInfo::new("public".to_string(), "err_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Try to create duplicate with ERROR
        let info = CreateSequenceInfo::new("public".to_string(), "err_seq".to_string());
        let result = schema.create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict);
        assert!(result.is_err());
    }

    #[test]
    fn test_on_create_conflict_replace() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create first view
        let query = parse_query("SELECT 1 AS num");
        let info = CreateViewInfo::new("public".to_string(), "replace_view".to_string(), query);
        schema
            .create_view(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Replace with new view
        let query = parse_query("SELECT 2 AS num");
        let info = CreateViewInfo::new("public".to_string(), "replace_view".to_string(), query);
        let result = schema.create_view(&txn, info, OnCreateConflict::ReplaceOnConflict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_entry_lookup_info() {
        let lookup = EntryLookupInfo::table("users".to_string());
        assert_eq!(lookup.get_catalog_type(), CatalogType::Table);
        assert_eq!(lookup.get_entry_name(), "users");

        let lookup = EntryLookupInfo::view("my_view".to_string());
        assert_eq!(lookup.get_catalog_type(), CatalogType::View);

        let lookup = EntryLookupInfo::sequence("my_seq".to_string());
        assert_eq!(lookup.get_catalog_type(), CatalogType::Sequence);
    }

    #[test]
    fn test_lookup_helpers() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create a sequence using new API
        let info = CreateSequenceInfo::new("public".to_string(), "lookup_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Access through the public lookup helpers
        let entry = schema.get_sequence(txn.transaction_id, txn.start_time, "lookup_seq");
        assert!(entry.is_some());
    }

    #[test]
    fn test_lookup_entry_detailed() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Lookup non-existent entry
        let lookup = EntryLookupInfo::sequence("nonexistent".to_string());
        let result = schema.lookup_entry_detailed(&txn, &lookup);
        assert!(result.result.is_none());
        assert_eq!(
            result.reason,
            crate::collection::EntryLookupFailure::NotPresent
        );

        // Create a sequence
        let info = CreateSequenceInfo::new("public".to_string(), "detail_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Lookup existing entry
        let lookup = EntryLookupInfo::sequence("detail_seq".to_string());
        let result = schema.lookup_entry_detailed(&txn, &lookup);
        assert!(result.result.is_some());
        assert_eq!(
            result.reason,
            crate::collection::EntryLookupFailure::Success
        );
    }

    #[test]
    fn test_get_similar_entry() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create some sequences
        for name in &["users_seq", "orders_seq", "products_seq"] {
            let info = CreateSequenceInfo::new("public".to_string(), name.to_string());
            schema
                .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
                .unwrap();
        }

        // Find similar entry
        let lookup = EntryLookupInfo::sequence("user_seq".to_string());
        let similar = schema.get_similar_entry(&txn, &lookup);
        assert_eq!(similar.name, "users_seq");
        assert!(similar.score > 0.7);
    }

    #[test]
    fn test_verify() {
        let schema = test_schema("public", 0);

        // Verify should succeed on empty schema
        assert!(schema.verify().is_ok());

        // Create some entries
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let info = CreateSequenceInfo::new("public".to_string(), "verify_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Verify should still succeed
        assert!(schema.verify().is_ok());
    }

    #[test]
    fn test_copy() {
        let schema = test_schema("public", 42);

        let copied = schema.copy().unwrap();
        assert_eq!(copied.base.name, "public");
        assert_eq!(copied.base.catalog, "test_catalog");
        assert_eq!(copied.base.timestamp(), 42);
    }

    #[test]
    fn test_create_schema_info() {
        let info = CreateSchemaInfo::new("my_catalog".to_string(), "my_schema".to_string())
            .with_on_conflict(OnCreateConflict::IgnoreOnConflict)
            .with_internal();

        assert_eq!(info.name, "my_schema");
        assert_eq!(info.catalog, "my_catalog");
        assert_eq!(info.on_conflict, OnCreateConflict::IgnoreOnConflict);
        assert!(info.internal);
    }

    #[test]
    fn test_from_info() {
        let info = CreateSchemaInfo::new("my_catalog".to_string(), "my_schema".to_string())
            .with_internal();

        let schema = schema_from_info(&info, 100);
        assert_eq!(schema.base.name, "my_schema");
        assert_eq!(schema.base.catalog, "my_catalog");
        assert!(schema.is_internal());
    }

    #[test]
    fn test_alter_entry_not_implemented() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Create a sequence
        let info = CreateSequenceInfo::new("public".to_string(), "alter_seq".to_string());
        schema
            .create_sequence(&txn, info, OnCreateConflict::ErrorOnConflict)
            .unwrap();

        // Try to rename (not implemented)
        let alter_info = AlterEntryInfo::new(CatalogType::Sequence, "alter_seq".to_string())
            .with_new_name("new_seq".to_string());
        let result = schema.alter_entry(&txn, &alter_info);
        assert!(result.is_err());
    }

    #[test]
    fn test_alter_entry_not_found() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Try to alter non-existent entry
        let alter_info = AlterEntryInfo::new(CatalogType::Sequence, "nonexistent".to_string());
        let result = schema.alter_entry(&txn, &alter_info);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Returns false for not found
    }

    #[test]
    fn test_drop_entry_not_found() {
        let schema = test_schema("public", 0);

        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        // Try to drop non-existent entry
        let drop_info = DropEntryInfo::new(CatalogType::Sequence, "nonexistent".to_string());
        let result = schema.drop_entry(&txn, &drop_info);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Returns false for not found
    }

    fn sample_property_graph_info(name: &str, if_not_exists: bool) -> CreatePropertyGraphInfo {
        CreatePropertyGraphInfo {
            catalog: "test_catalog".to_string(),
            schema: "public".to_string(),
            graph_name: name.to_string(),
            if_not_exists,
            vertex_tables: vec![VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 1001,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![1, 2],
            }],
            edge_tables: vec![EdgeTableInfo {
                table_name: "knows".to_string(),
                table_oid: 2001,
                key_column_ids: vec![0],
                source_key_column_ids: vec![1],
                source_vertex_table: "person".to_string(),
                source_ref_column_ids: vec![0],
                destination_key_column_ids: vec![2],
                destination_vertex_table: "person".to_string(),
                destination_ref_column_ids: vec![0],
                label: "Knows".to_string(),
                property_column_ids: vec![3],
            }],
        }
    }

    #[test]
    fn test_property_graph_create_get_drop() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        let info = sample_property_graph_info("social_network", false);
        schema.create_property_graph(&txn, info).unwrap();

        let graph = schema.get_property_graph(&txn, "social_network").unwrap();
        assert_eq!(graph.base.base.name, "social_network");
        assert_eq!(graph.info.vertex_tables.len(), 1);
        assert_eq!(graph.info.edge_tables.len(), 1);

        schema
            .drop_property_graph(&txn, "social_network", false)
            .unwrap();
        assert!(schema.get_property_graph(&txn, "social_network").is_err());
    }

    #[test]
    fn test_property_graph_if_not_exists() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        schema
            .create_property_graph(&txn, sample_property_graph_info("g1", false))
            .unwrap();

        let err = schema
            .create_property_graph(&txn, sample_property_graph_info("g1", false))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));

        schema
            .create_property_graph(&txn, sample_property_graph_info("g1", true))
            .unwrap();
    }

    #[test]
    fn test_property_graph_drop_if_exists() {
        let schema = test_schema("public", 0);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);

        schema.drop_property_graph(&txn, "missing", true).unwrap();

        let err = schema
            .drop_property_graph(&txn, "missing", false)
            .unwrap_err();
        assert!(err.to_string().contains("property graph"));
    }

    #[test]
    fn test_property_graph_rollback_visibility_restore() {
        let schema = test_schema("public", 0);

        let creating_txn = CatalogSnapshot::writer(TRANSACTION_ID_START + 1, 1);
        schema
            .create_property_graph(
                &creating_txn,
                sample_property_graph_info("rollback_graph", false),
            )
            .unwrap();

        let other_txn = CatalogSnapshot::writer(TRANSACTION_ID_START + 2, 2);
        assert!(schema
            .get_property_graph(&other_txn, "rollback_graph")
            .is_err());

        // Simulate transaction rollback: catalog commit path removes uncommitted entry.
        schema
            .contents
            .property_graphs
            .remove_entry("rollback_graph");
        assert!(schema
            .get_property_graph(&creating_txn, "rollback_graph")
            .is_err());
    }
}
