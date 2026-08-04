// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ParoCatalog - Main Catalog Implementation
//!
//! It manages:
//! - Schema set (CatalogCollection of SchemaEntry)
//! - Dependency graph for tracking object dependencies

use crate::catalog::{
    Catalog, DatabaseSize, EntryLookupInfo, MetadataBlockInfo, DEFAULT_SCHEMA, INFORMATION_SCHEMA,
    PG_CATALOG, SYSTEM_SCHEMA,
};
use crate::collection::{CatalogCollection, CatalogGcStats, CollectionLockKey, InstallMode};
use crate::default::DefaultGenerator;
use crate::dependency::{DependencyDelta, DependencyGraph};
use crate::entry::{
    CatalogEntryEnum, CatalogObjectId, CatalogObjectIdAllocator, CatalogType, CreateSchemaInfo,
    DependencyType, DropSchemaInfo, OnCreateConflict, OnEntryNotFound, PropertyGraphCatalogEntry,
    SchemaEntry,
};
use crate::mvcc::CatalogSnapshot;
use paro_common::error::{self as paro_error, Result};
use paro_storage::meta::{FileMetadataStore, MetadataStore, StorageManifest, TabletMetaManager};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// ParoCatalog Implementation
// ============================================================================

/// ParoCatalog is the main catalog implementation.
///
///
/// It manages schemas and their contents (tables, views, functions, etc.)
/// with MVCC support for transactional DDL.
#[derive(Debug)]
pub struct ParoCatalog {
    /// Catalog name (database name)
    name: String,
    /// Schema set (Arc because CatalogCollection::new returns Arc)
    schemas: Arc<CatalogCollection>,
    /// Identity-first dependency graph.
    dependency_graph: DependencyGraph,
    /// Whether this is a system catalog
    is_system: bool,
    /// Whether this is an in-memory catalog
    is_in_memory: bool,
    /// Database path (empty for in-memory)
    db_path: String,
    /// Object id allocator watermark shared with entry constructors.
    object_id_allocator: Arc<CatalogObjectIdAllocator>,
    /// Shared GC epoch for the full catalog.
    gc_epoch: Arc<AtomicU64>,
}

fn recursive_dir_size(path: &Path) -> u64 {
    let Ok(metadata) = fs::metadata(path) else {
        return 0;
    };

    if metadata.is_file() {
        return metadata.len();
    }

    if !metadata.is_dir() {
        return 0;
    }

    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };

    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| recursive_dir_size(&entry.path()))
        .fold(0u64, |acc, size| acc.saturating_add(size))
}

impl ParoCatalog {
    /// Create a new ParoCatalog.
    pub fn new(name: String) -> Self {
        Self::with_object_id_allocator(name, Arc::new(CatalogObjectIdAllocator::default()))
    }

    /// Create an in-memory catalog using the instance-owned object id allocator.
    pub fn with_object_id_allocator(
        name: String,
        object_id_allocator: Arc<CatalogObjectIdAllocator>,
    ) -> Self {
        let gc_epoch = Arc::new(AtomicU64::new(0));
        let schemas = CatalogCollection::new(
            format!("{}.schemas", name),
            CollectionLockKey::database_schemas(),
            Arc::clone(&gc_epoch),
        );
        Self {
            name: name.clone(),
            schemas,
            dependency_graph: DependencyGraph::new(),
            is_system: false,
            is_in_memory: true,
            db_path: String::new(),
            object_id_allocator,
            gc_epoch,
        }
    }

    /// Create a new ParoCatalog with path.
    pub fn with_path(name: String, path: String) -> Self {
        Self::with_path_and_object_id_allocator(
            name,
            path,
            Arc::new(CatalogObjectIdAllocator::default()),
        )
    }

    /// Create a file-backed catalog using the instance-owned object id allocator.
    pub fn with_path_and_object_id_allocator(
        name: String,
        path: String,
        object_id_allocator: Arc<CatalogObjectIdAllocator>,
    ) -> Self {
        let gc_epoch = Arc::new(AtomicU64::new(0));
        let schemas = CatalogCollection::new(
            format!("{}.schemas", name),
            CollectionLockKey::database_schemas(),
            Arc::clone(&gc_epoch),
        );
        Self {
            name: name.clone(),
            schemas,
            dependency_graph: DependencyGraph::new(),
            is_system: false,
            is_in_memory: path.is_empty(),
            db_path: path,
            object_id_allocator,
            gc_epoch,
        }
    }

    /// Get the catalog name (convenience method that works with Arc).
    ///
    /// This is a direct method on ParoCatalog that can be called through Arc,
    /// in addition to the Catalog trait method.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the next object id watermark without allocating a new object id.
    pub fn current_object_id(&self) -> u64 {
        self.object_id_allocator.current()
    }

    /// Allocate the next object id.
    pub fn next_object_id(&self) -> u64 {
        self.object_id_allocator.allocate().raw()
    }

    /// Ensure the allocator watermark is at least `watermark`.
    pub fn bump_object_id_allocator(&self, watermark: u64) -> u64 {
        self.object_id_allocator.advance_to(watermark)
    }

    pub fn object_id_allocator(&self) -> &Arc<CatalogObjectIdAllocator> {
        &self.object_id_allocator
    }

    pub fn dependency_graph(&self) -> &DependencyGraph {
        &self.dependency_graph
    }

    pub fn rebuild_dependency_graph(&self) -> Result<()> {
        let snapshot = CatalogSnapshot::read_only(u64::MAX);
        let rebuilt = self.build_dependency_graph_snapshot(&snapshot)?;
        self.dependency_graph.overwrite_with(&rebuilt);
        Ok(())
    }

    pub fn build_dependency_graph_snapshot(
        &self,
        snapshot: &CatalogSnapshot,
    ) -> Result<DependencyGraph> {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        for entry in self
            .schemas
            .scan(snapshot.transaction_id, snapshot.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = entry.as_ref() else {
                continue;
            };
            let schema_ref = entry.object_ref(None);
            let schema_id = schema_ref.id;
            delta.add_object(schema_ref);
            Self::collect_schema_dependency_state(snapshot, schema, schema_id, &mut delta);
        }
        delta.publish(&graph)?;
        Ok(graph)
    }

    fn collect_schema_dependency_state(
        snapshot: &CatalogSnapshot,
        schema: &SchemaEntry,
        schema_id: CatalogObjectId,
        delta: &mut DependencyDelta,
    ) {
        let txid = snapshot.transaction_id;
        let start_time = snapshot.start_time;
        for kind in [
            CatalogType::Table,
            CatalogType::View,
            CatalogType::Index,
            CatalogType::PropertyGraph,
            CatalogType::Routine,
            CatalogType::ScalarFunction,
            CatalogType::TableFunction,
            CatalogType::CopyFunction,
            CatalogType::Sequence,
            CatalogType::Type,
            CatalogType::Collation,
        ] {
            let Some(collection) = schema.collection(kind) else {
                continue;
            };
            Self::collect_collection_dependency_state(
                collection, txid, start_time, schema_id, delta,
            );
        }
    }

    fn collect_collection_dependency_state(
        collection: &Arc<CatalogCollection>,
        transaction_id: u64,
        start_time: u64,
        schema_id: CatalogObjectId,
        delta: &mut DependencyDelta,
    ) {
        for entry in collection.scan(transaction_id, start_time) {
            let entry_ref = entry.object_ref(Some(schema_id));
            let object_id = entry_ref.id;
            delta.add_object(entry_ref);
            delta.add_dependency(object_id, schema_id, DependencyType::OwnedBy);
            delta.add_dependencies(object_id, &entry.dependency_list());
        }
    }

    /// Get a schema by name (convenience method).
    ///
    /// This is a convenience wrapper that works with Arc<ParoCatalog>.
    pub fn get_schema(
        &self,
        transaction: &CatalogSnapshot,
        name: &str,
    ) -> Result<Arc<SchemaEntry>> {
        let lookup = EntryLookupInfo::schema(name.to_string());
        let schema = self
            .lookup_schema(transaction, &lookup, OnEntryNotFound::ThrowException)?
            .ok_or_else(|| paro_error::schema_not_found(name))?;
        if Self::is_default_schema_name(name) {
            let system_txn = CatalogSnapshot::read_only(u64::MAX);
            Self::materialize_default_entries(&schema, &system_txn);
        }
        Ok(schema)
    }

    /// Get a table from a schema (convenience method).
    ///
    /// This is a convenience wrapper that works with Arc<ParoCatalog>.
    pub fn get_table(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Arc<crate::entry::CatalogEntryEnum>> {
        let schema = self.get_schema(transaction, schema_name)?;
        schema
            .get_table(
                transaction.transaction_id,
                transaction.start_time,
                table_name,
            )
            .ok_or_else(|| paro_error::table_not_found(table_name))
    }

    /// Get a view from a schema (convenience method).
    ///
    /// This is a convenience wrapper that works with Arc<ParoCatalog>.
    pub fn get_view(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        view_name: &str,
    ) -> Result<Arc<crate::entry::CatalogEntryEnum>> {
        let schema = self.get_schema(transaction, schema_name)?;
        schema
            .get_view(
                transaction.transaction_id,
                transaction.start_time,
                view_name,
            )
            .ok_or_else(|| paro_error::object_not_found("view", view_name))
    }

    /// Get a table or view from a schema (convenience method).
    ///
    /// This is a convenience wrapper that works with Arc<ParoCatalog>.
    pub fn get_table_or_view(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        name: &str,
    ) -> Result<Arc<crate::entry::CatalogEntryEnum>> {
        let schema = self.get_schema(transaction, schema_name)?;

        // Try table first
        if let Some(entry) =
            schema.get_table(transaction.transaction_id, transaction.start_time, name)
        {
            return Ok(entry);
        }

        // Try view
        if let Some(entry) =
            schema.get_view(transaction.transaction_id, transaction.start_time, name)
        {
            return Ok(entry);
        }

        Err(paro_error::table_not_found(name))
    }

    /// Get any entry from a schema (convenience method).
    ///
    /// This is a convenience wrapper that works with Arc<ParoCatalog>.
    pub fn get_any_entry(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        entry_type: CatalogType,
        name: &str,
    ) -> Result<Arc<crate::entry::CatalogEntryEnum>> {
        let schema = self.get_schema(transaction, schema_name)?;

        // Use the appropriate getter based on entry type
        let entry = match entry_type {
            CatalogType::Table => {
                schema.get_table(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::View => {
                schema.get_view(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::Index => {
                schema.get_index(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::Sequence => {
                schema.get_sequence(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::Routine => {
                schema.get_routine(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::ScalarFunction | CatalogType::AggregateFunction => {
                schema.get_function(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::TableFunction => {
                schema.get_table_function(transaction.transaction_id, transaction.start_time, name)
            }
            CatalogType::CopyFunction => {
                schema.get_copy_function(transaction.transaction_id, transaction.start_time, name)
            }
            _ => None,
        };

        entry.ok_or_else(|| paro_error::object_not_found(entry_type.as_str(), name))
    }

    /// Drop a table (convenience method).
    pub fn drop_table(
        &self,
        transaction_id: u64,
        start_time: u64,
        schema_name: &str,
        table_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.drop_table_with_snapshot(&txn, schema_name, table_name, if_exists)
    }

    pub fn drop_table_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        table_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        use crate::entry::{CatalogEntryEnum, DropEntryInfo};

        let writer_id = transaction.write_timestamp()?;
        let schema = self.get_schema(transaction, schema_name)?;
        let table_entry = schema.get_table(writer_id, transaction.start_time, table_name);

        let mut blocking_graphs = Vec::new();
        for graph in self.scan_property_graphs(transaction) {
            let depends_on_table = graph
                .info
                .vertex_tables
                .iter()
                .any(|vertex| vertex.table_name.eq_ignore_ascii_case(table_name))
                || graph
                    .info
                    .edge_tables
                    .iter()
                    .any(|edge| edge.table_name.eq_ignore_ascii_case(table_name));
            if depends_on_table && graph.info.schema.eq_ignore_ascii_case(schema_name) {
                blocking_graphs.push(graph.info.graph_name.clone());
            }
        }

        if !blocking_graphs.is_empty() {
            blocking_graphs.sort();
            blocking_graphs.dedup();
            return Err(paro_error::dependent_objects_still_exist(format!(
                "cannot drop table \"{}\": referenced by property graph {}",
                table_name,
                blocking_graphs.join(", ")
            )));
        }

        let info = DropEntryInfo {
            entry_type: CatalogType::Table,
            name: table_name.to_string(),
            if_exists,
            cascade: false,
            allow_drop_internal: false,
        };

        let dropped = schema.drop_entry(transaction, &info)?;
        if !dropped && !if_exists {
            return Err(paro_error::table_not_found(table_name));
        }

        let table_oid = match table_entry.as_deref() {
            Some(CatalogEntryEnum::Table(table)) => Some(table.base.base.object_id.raw()),
            _ => None,
        };

        if let Some(table_oid) = table_oid {
            let mut dependent_indexes = Vec::new();
            for entry in schema
                .collection(CatalogType::Index)
                .expect("index collection")
                .scan(writer_id, transaction.start_time)
            {
                if let CatalogEntryEnum::Index(index) = entry.as_ref() {
                    if index.table_oid == table_oid
                        || index.table_name.eq_ignore_ascii_case(table_name)
                    {
                        dependent_indexes.push(index.base.base.name.clone());
                    }
                }
            }

            for index_name in dependent_indexes {
                let drop_index = DropEntryInfo {
                    entry_type: CatalogType::Index,
                    name: index_name,
                    if_exists: false,
                    cascade: false,
                    allow_drop_internal: false,
                };
                let _ = schema.drop_entry(transaction, &drop_index)?;
            }
        }

        Ok(())
    }

    /// Find a table by object id across all schemas.
    ///
    /// This method iterates through all schemas to find a table with the given object id.
    /// Returns (schema_name, table_name, table_entry) if found.
    ///
    /// # Arguments
    /// * `transaction` - The catalog transaction for visibility checks
    /// * `object_id` - The stable object identity of the table to find
    ///
    /// # Returns
    /// Some((schema_name, table_name, table_entry)) if found, None otherwise
    pub fn find_table_by_object_id(
        &self,
        transaction: &CatalogSnapshot,
        object_id: u64,
    ) -> Option<(String, String, Arc<crate::entry::TableCatalogEntry>)> {
        // Scan all schemas
        let schemas = self
            .schemas
            .scan(transaction.transaction_id, transaction.start_time);

        for schema_entry in schemas {
            if let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() {
                // Scan all tables in this schema
                let tables = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .scan(transaction.transaction_id, transaction.start_time);

                for table_entry in tables {
                    if let CatalogEntryEnum::Table(table) = table_entry.as_ref() {
                        if table.base.base.object_id.raw() == object_id {
                            return Some((
                                schema.base.name.clone(),
                                table.base.base.name.clone(),
                                table.clone(),
                            ));
                        }
                    }
                }
            }
        }

        None
    }

    pub fn scan_property_graphs(
        &self,
        transaction: &CatalogSnapshot,
    ) -> Vec<Arc<PropertyGraphCatalogEntry>> {
        let schemas = self
            .schemas
            .scan(transaction.transaction_id, transaction.start_time);
        let mut graphs = Vec::new();

        for schema_entry in schemas {
            if let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() {
                let entries = schema
                    .collection(CatalogType::PropertyGraph)
                    .expect("property graph collection")
                    .scan(transaction.transaction_id, transaction.start_time);
                for entry in entries {
                    if let CatalogEntryEnum::PropertyGraph(graph) = entry.as_ref() {
                        graphs.push(graph.clone());
                    }
                }
            }
        }

        graphs.sort_by(|lhs, rhs| lhs.info.graph_name.cmp(&rhs.info.graph_name));
        graphs
    }

    /// Drop a view (convenience method).
    pub fn drop_view(
        &self,
        transaction_id: u64,
        start_time: u64,
        schema_name: &str,
        view_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.drop_view_with_snapshot(&txn, schema_name, view_name, if_exists)
    }

    pub fn drop_view_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        view_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        use crate::entry::DropEntryInfo;

        let schema = self.get_schema(transaction, schema_name)?;

        let info = DropEntryInfo {
            entry_type: CatalogType::View,
            name: view_name.to_string(),
            if_exists,
            cascade: false,
            allow_drop_internal: false,
        };

        let dropped = schema.drop_entry(transaction, &info)?;
        if !dropped && !if_exists {
            return Err(paro_error::object_not_found("view", view_name));
        }

        Ok(())
    }

    /// Drop a schema (convenience method).
    pub fn drop_schema(
        &self,
        transaction_id: u64,
        start_time: u64,
        schema_name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.drop_schema_with_snapshot(&txn, schema_name, if_exists, cascade)
    }

    pub fn drop_schema_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let info = DropSchemaInfo {
            catalog: self.name.clone(),
            name: schema_name.to_string(),
            cascade,
            if_exists,
        };

        Catalog::drop_schema(self, transaction, &info)
    }

    /// Create a view (convenience method).
    pub fn create_view(
        &self,
        transaction_id: u64,
        start_time: u64,
        info: crate::entry::CreateViewInfo,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.create_view_with_snapshot(&txn, info)
    }

    /// Create a schema (convenience method).
    pub fn create_schema(&self, transaction_id: u64, schema_name: &str) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, 0);
        self.create_schema_with_snapshot(&txn, schema_name)
    }

    pub fn create_view_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        info: crate::entry::CreateViewInfo,
    ) -> Result<()> {
        let schema = self.get_schema(transaction, &info.schema)?;
        schema.create_view(transaction, info, OnCreateConflict::ErrorOnConflict)?;
        Ok(())
    }

    pub fn create_schema_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
    ) -> Result<()> {
        let info = CreateSchemaInfo {
            catalog: self.name.clone(),
            name: schema_name.to_string(),
            internal: false,
            on_conflict: OnCreateConflict::ErrorOnConflict,
        };

        Catalog::create_schema(self, transaction, &info)?;
        Ok(())
    }

    /// Create a table (convenience method).
    pub fn create_table(
        &self,
        transaction_id: u64,
        start_time: u64,
        schema_name: &str,
        table_name: &str,
        columns: Vec<crate::entry::ColumnDefinition>,
        storage: Arc<paro_storage::table::table_handle::TableHandle>,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.create_table_in_snapshot(&txn, schema_name, table_name, columns, storage)
    }

    pub fn create_table_in_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        schema_name: &str,
        table_name: &str,
        columns: Vec<crate::entry::ColumnDefinition>,
        storage: Arc<paro_storage::table::table_handle::TableHandle>,
    ) -> Result<()> {
        let info = crate::entry::CreateTableInfo::new(
            self.name.clone(),
            schema_name.to_string(),
            table_name.to_string(),
            columns,
        );
        self.create_table_with_snapshot(transaction, info, storage)
    }

    /// Create a table with explicit create metadata (constraints/conflict behavior).
    pub fn create_table_with_info(
        &self,
        transaction_id: u64,
        start_time: u64,
        info: crate::entry::CreateTableInfo,
        storage: Arc<paro_storage::table::table_handle::TableHandle>,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(transaction_id, start_time);
        self.create_table_with_snapshot(&txn, info, storage)
    }

    pub fn create_table_with_snapshot(
        &self,
        transaction: &CatalogSnapshot,
        mut info: crate::entry::CreateTableInfo,
        storage: Arc<paro_storage::table::table_handle::TableHandle>,
    ) -> Result<()> {
        use crate::entry::TableCatalogEntry;

        let schema = self.get_schema(transaction, &info.schema)?;
        let on_conflict = info.on_conflict;

        // Ensure catalog name is consistent with the attached database catalog.
        info.catalog = self.name.clone();

        // Create TableCatalogEntry with timestamp 0
        // The actual timestamp will be set by CatalogCollection::create_entry.
        // `from_info` keeps constraints and captures a descriptor from runtime storage.
        let table_entry = Arc::new(TableCatalogEntry::from_info(
            info,
            storage,
            self.object_id_allocator.allocate(),
            0,
        ));

        schema.create_table(transaction, table_entry, on_conflict)?;
        Ok(())
    }

    /// Create a system catalog.
    pub fn system_catalog() -> Self {
        let mut catalog = Self::new("system".to_string());
        catalog.is_system = true;
        catalog
    }

    /// Check if this is a system catalog.
    pub fn is_system_catalog(&self) -> bool {
        self.is_system
    }

    /// Get the schema catalog set.
    pub fn get_schema_collection(&self) -> &Arc<CatalogCollection> {
        &self.schemas
    }

    pub fn gc_epoch(&self) -> u64 {
        self.gc_epoch.load(Ordering::Relaxed)
    }

    pub fn gc_epoch_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.gc_epoch)
    }

    pub fn gc(&self, watermark: u64) -> CatalogGcStats {
        let mut stats = CatalogGcStats::default();
        let mut owners = HashSet::new();
        let mut worklist = Vec::new();

        self.schemas.for_each_chain_head(|_, head| {
            let mut current = Some(Arc::clone(head));
            while let Some(node) = current {
                if let Some(entry) = node.entry.as_ref() {
                    let CatalogEntryEnum::Schema(schema) = entry.as_ref() else {
                        current = node.child();
                        continue;
                    };
                    let owner = schema.contents_owner();
                    let ptr = Arc::as_ptr(owner) as usize;
                    if owners.insert(ptr) {
                        worklist.push(Arc::clone(owner));
                    }
                }
                current = node.child();
            }
        });

        for contents in worklist {
            stats.merge(&contents.gc(watermark));
        }
        stats.merge(&self.schemas.gc(watermark));
        stats
    }

    /// Internal schema creation.
    fn create_schema_internal(
        &self,
        transaction: &CatalogSnapshot,
        info: &CreateSchemaInfo,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        // Create schema with timestamp 0
        // The actual provisional timestamp will be staged by the schema collection.
        let schema = Arc::new(SchemaEntry::from_info(
            info,
            Arc::clone(&self.object_id_allocator),
            Arc::clone(&self.gc_epoch),
            0,
        ));
        let entry = Arc::new(CatalogEntryEnum::Schema(schema.clone()));
        self.schemas
            .stage_create(transaction, &info.name, Arc::clone(&entry))
            .map(|handle| handle.map(|_| entry))
    }

    /// Check if a schema name is a default schema.
    fn is_default_schema_name(name: &str) -> bool {
        let lower = name.to_lowercase();
        lower == DEFAULT_SCHEMA
            || lower == SYSTEM_SCHEMA
            || lower == INFORMATION_SCHEMA
            || lower == PG_CATALOG
    }

    fn materialize_default_schema(&self, name: &str) {
        if !Self::is_default_schema_name(name) {
            return;
        }

        let generator = crate::default::DefaultSchemaGenerator::new(
            self.name.clone(),
            Arc::clone(&self.object_id_allocator),
            Arc::clone(&self.gc_epoch),
        );
        if let Some(entry) = generator.create_default_entry(name) {
            let _ = self
                .schemas
                .install_committed(entry, InstallMode::RejectExisting);
        }
    }

    fn materialize_default_schemas(&self) {
        for schema_name in crate::default::default_schemas::default_schema_names() {
            self.materialize_default_schema(schema_name);
        }
    }

    fn materialize_default_entries(schema: &Arc<SchemaEntry>, transaction: &CatalogSnapshot) {
        schema.materialize_default_entries(transaction);
    }

    pub fn verify(&self) -> Result<()> {
        let snapshot = CatalogSnapshot::read_only(u64::MAX);
        let rebuilt = self.build_dependency_graph_snapshot(&snapshot)?;
        let mut object_ids = HashSet::new();

        for entry in self
            .schemas
            .scan(snapshot.transaction_id, snapshot.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = entry.as_ref() else {
                return Err(paro_error::internal(
                    "schema collection stored a non-schema entry",
                ));
            };
            if schema.base.name.is_empty() {
                return Err(paro_error::internal(
                    "catalog contains schema with empty name",
                ));
            }
            if schema.base.catalog != self.name {
                return Err(paro_error::internal(format!(
                    "catalog {} contains schema {} owned by {}",
                    self.name, schema.base.name, schema.base.catalog
                )));
            }
            if !object_ids.insert(schema.base.object_id) {
                return Err(paro_error::internal(format!(
                    "catalog {} reuses schema object id {}",
                    self.name,
                    schema.base.object_id.raw()
                )));
            }
            schema.verify()?;
        }

        let mut current_ids = self.dependency_graph.object_ids();
        let mut rebuilt_ids = rebuilt.object_ids();
        current_ids.sort_unstable();
        rebuilt_ids.sort_unstable();

        if current_ids != rebuilt_ids {
            return Err(paro_error::internal(format!(
                "catalog {} dependency graph object set does not match rebuild",
                self.name
            )));
        }

        for object_id in rebuilt_ids {
            if self.dependency_graph.object_ref(object_id) != rebuilt.object_ref(object_id) {
                return Err(paro_error::internal(format!(
                    "catalog {} dependency graph object metadata drifted for {}",
                    self.name,
                    object_id.raw()
                )));
            }
            if self.dependency_graph.incident_edges_of(object_id)
                != rebuilt.incident_edges_of(object_id)
            {
                return Err(paro_error::internal(format!(
                    "catalog {} dependency graph edges drifted for {}",
                    self.name,
                    object_id.raw()
                )));
            }
        }

        if self.dependency_graph.edge_count() != rebuilt.edge_count() {
            return Err(paro_error::internal(format!(
                "catalog {} dependency graph edge count does not match rebuild",
                self.name
            )));
        }

        Ok(())
    }
}

impl Catalog for ParoCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_catalog_type(&self) -> &str {
        "paro"
    }

    fn is_paro_catalog(&self) -> bool {
        true
    }

    fn initialize(&self, _load_builtin: bool) {
        let system_txn = CatalogSnapshot::read_only(u64::MAX);

        self.materialize_default_schemas();
        for schema_name in [DEFAULT_SCHEMA, PG_CATALOG, INFORMATION_SCHEMA] {
            if let Ok(schema) = self.get_schema(&system_txn, schema_name) {
                Self::materialize_default_entries(&schema, &system_txn);
            }
        }

        self.rebuild_dependency_graph()
            .expect("catalog dependency graph rebuild should succeed during initialization");

        #[cfg(debug_assertions)]
        self.verify()
            .expect("catalog invariant check failed during initialization");
    }

    fn create_schema(
        &self,
        transaction: &CatalogSnapshot,
        info: &CreateSchemaInfo,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        if info.name.is_empty() {
            return Err(paro_error::catalog("Schema name cannot be empty"));
        }

        // Only internal can create default schemas
        if !info.internal && Self::is_default_schema_name(&info.name) {
            return Ok(None);
        }

        let result = self.create_schema_internal(transaction, info)?;

        if result.is_none() {
            match info.on_conflict {
                OnCreateConflict::ErrorOnConflict => {
                    return Err(paro_error::schema_exists(&info.name));
                }
                OnCreateConflict::ReplaceOnConflict => {
                    // Drop and recreate
                    let drop_info = DropSchemaInfo {
                        catalog: info.catalog.clone(),
                        name: info.name.clone(),
                        cascade: false,
                        if_exists: false,
                    };
                    Catalog::drop_schema(self, transaction, &drop_info)?;

                    let result = self.create_schema_internal(transaction, info)?;
                    if result.is_none() {
                        return Err(paro_error::internal(
                            "Failed to create schema in CREATE_OR_REPLACE",
                        ));
                    }
                    return Ok(result);
                }
                OnCreateConflict::IgnoreOnConflict => {
                    return Ok(None);
                }
                OnCreateConflict::AlterOnConflict => {
                    // For schemas, ALTER is not applicable, treat as ignore
                    return Ok(None);
                }
            }
        }

        Ok(result)
    }

    fn scan_schemas<F>(&self, transaction: &CatalogSnapshot, mut callback: F)
    where
        F: FnMut(&SchemaEntry),
    {
        self.materialize_default_schemas();
        self.schemas.scan_with_callback(
            transaction.transaction_id,
            transaction.start_time,
            |entry| {
                if let CatalogEntryEnum::Schema(schema) = entry {
                    callback(schema);
                }
            },
        );
    }

    fn lookup_schema(
        &self,
        transaction: &CatalogSnapshot,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> Result<Option<Arc<SchemaEntry>>> {
        let name = &lookup.name;
        if name.is_empty() {
            return Err(paro_error::catalog("Schema name cannot be empty"));
        }

        let entry =
            self.schemas
                .get_entry(transaction.transaction_id, transaction.start_time, name);

        let entry = match entry {
            Some(entry) => Some(entry),
            None if Self::is_default_schema_name(name) => {
                self.materialize_default_schema(name);
                self.schemas
                    .get_entry(transaction.transaction_id, transaction.start_time, name)
            }
            None => None,
        };

        match entry {
            Some(e) => {
                if let CatalogEntryEnum::Schema(schema) = e.as_ref() {
                    Ok(Some(Arc::clone(schema)))
                } else {
                    Err(paro_error::wrong_object_type("schema", name))
                }
            }
            None => {
                if if_not_found == OnEntryNotFound::ThrowException {
                    Err(paro_error::schema_not_found(name))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn drop_schema(&self, transaction: &CatalogSnapshot, info: &DropSchemaInfo) -> Result<()> {
        if info.name.is_empty() {
            return Err(paro_error::catalog("Schema name cannot be empty"));
        }

        let dropped = self.schemas.stage_drop(transaction, &info.name)?.is_some();

        if !dropped && !info.if_exists {
            return Err(paro_error::schema_not_found(&info.name));
        }

        Ok(())
    }

    fn get_database_size(&self) -> DatabaseSize {
        let mut size = DatabaseSize::default();

        if self.is_in_memory || self.db_path.is_empty() {
            return size;
        }

        let root = Path::new(&self.db_path);
        if root.is_file() {
            size.bytes = recursive_dir_size(root);
            size.wal_size = recursive_dir_size(Path::new(&format!("{}.wal", self.db_path)));
        } else if root.is_dir() {
            size.bytes = size
                .bytes
                .saturating_add(recursive_dir_size(&root.join("tablets")));
            size.bytes = size
                .bytes
                .saturating_add(recursive_dir_size(&root.join("meta")));
            size.wal_size = recursive_dir_size(&root.join("wal"));
        }

        if size.bytes > 0 {
            size.block_size = 4096;
            size.block_count = size.bytes.div_ceil(size.block_size);
            size.used_blocks = size.block_count;
            size.free_blocks = 0;
        }

        size
    }

    fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
        // For in-memory catalogs, return empty vector
        if self.is_in_memory {
            return Vec::new();
        }

        if self.db_path.is_empty() {
            return Vec::new();
        }

        let meta_dir = Path::new(&self.db_path).join("meta");
        if !meta_dir.exists() {
            return Vec::new();
        }

        let Ok(store) = FileMetadataStore::new(&meta_dir) else {
            return Vec::new();
        };

        let mut info = Vec::new();

        if let Ok(entries) = store.scan_prefix("tablet/") {
            if !entries.is_empty() {
                info.push(MetadataBlockInfo {
                    block_id: 0,
                    block_type: "tablet_metadata".to_string(),
                    entry_count: u64::try_from(entries.len()).unwrap_or(u64::MAX),
                });
            }
        }

        if let Ok(Some(manifest)) = store.get(TabletMetaManager::manifest_key()) {
            if let Ok(decoded) = StorageManifest::from_json_bytes(&manifest) {
                info.push(MetadataBlockInfo {
                    block_id: 1,
                    block_type: "manifest".to_string(),
                    entry_count: u64::try_from(decoded.tablets.len()).unwrap_or(u64::MAX),
                });
            }
        }

        info
    }

    fn in_memory(&self) -> bool {
        self.is_in_memory
    }

    fn get_db_path(&self) -> String {
        self.db_path.clone()
    }

    fn get_dependency_graph(&self) -> Option<&DependencyGraph> {
        Some(&self.dependency_graph)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{CreateTypeInfo, TypeCatalogEntry};
    use paro_storage::transaction::manager::TRANSACTION_ID_START;

    fn committed_object_ids(schema: &SchemaEntry, kind: CatalogType) -> Vec<u64> {
        let mut ids = schema
            .collection(kind)
            .expect("collection")
            .scan_committed()
            .into_iter()
            .map(|entry| entry.object_id().raw())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    fn make_type_entry(schema: &str, name: &str) -> Arc<CatalogEntryEnum> {
        Arc::new(CatalogEntryEnum::Type(Arc::new(TypeCatalogEntry::new(
            CreateTypeInfo::new(
                "test_db".to_string(),
                schema.to_string(),
                name.to_string(),
                paro_common::types::LogicalType::Integer,
            ),
            CatalogObjectId::from_raw(10_001),
            0,
        ))))
    }

    fn publish_replace(
        collection: &Arc<CatalogCollection>,
        name: &str,
        entry: Arc<CatalogEntryEnum>,
        writer_id: u64,
        start_time: u64,
        commit_id: u64,
    ) {
        let snapshot = CatalogSnapshot::writer(writer_id, start_time);
        let handle = collection
            .stage_replace(&snapshot, name, entry)
            .expect("stage replace")
            .expect("replace handle");
        handle.publish(commit_id).expect("publish replace");
    }

    fn chain_timestamps(collection: &CatalogCollection, key: &str) -> Vec<u64> {
        let mut timestamps = Vec::new();
        let lower = key.to_lowercase();
        collection.for_each_chain_head(|name, head| {
            if name != lower {
                return;
            }
            let mut current = Some(Arc::clone(head));
            while let Some(node) = current {
                timestamps.push(node.timestamp());
                current = node.child();
            }
        });
        timestamps
    }

    #[test]
    fn test_paro_catalog_new() {
        let catalog = ParoCatalog::new("test_db".to_string());
        assert_eq!(catalog.name(), "test_db");
        assert!(catalog.is_paro_catalog());
        assert!(catalog.in_memory());
        assert!(!catalog.is_system_catalog());
    }

    #[test]
    fn test_standalone_catalogs_have_isolated_object_id_allocators() {
        let first = ParoCatalog::new("first".to_string());
        let second = ParoCatalog::new("second".to_string());

        assert_eq!(
            first.next_object_id(),
            CatalogObjectIdAllocator::FIRST_USER_OBJECT_ID
        );
        assert_eq!(
            second.next_object_id(),
            CatalogObjectIdAllocator::FIRST_USER_OBJECT_ID
        );
    }

    #[test]
    fn test_paro_catalog_with_path() {
        let catalog = ParoCatalog::with_path("test_db".to_string(), "/tmp/test.db".to_string());
        assert_eq!(catalog.get_db_path(), "/tmp/test.db");
        assert!(!catalog.in_memory());
    }

    #[test]
    fn test_system_catalog() {
        let catalog = ParoCatalog::system_catalog();
        assert!(catalog.is_system_catalog());
        assert_eq!(catalog.name(), "system");
    }

    #[test]
    fn test_initialize() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let lookup = EntryLookupInfo::schema(DEFAULT_SCHEMA.to_string());
        let schema = catalog.lookup_schema(&txn, &lookup, OnEntryNotFound::ReturnNull);

        assert!(schema.is_ok());
        assert!(schema.unwrap().is_some());
    }

    #[test]
    fn test_list_schemas() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema_names = catalog.list_schemas(&txn);

        assert!(schema_names.contains(&DEFAULT_SCHEMA.to_string()));
        assert!(schema_names.contains(&PG_CATALOG.to_string()));
    }

    #[test]
    fn test_get_schema() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);

        let txn = CatalogSnapshot::read_only(u64::MAX);

        // Should find public schema
        let schema = catalog.get_schema(&txn, DEFAULT_SCHEMA);
        assert!(schema.is_ok());

        // Should not find nonexistent schema
        let schema = catalog.get_schema(&txn, "nonexistent");
        assert!(schema.is_err());
    }

    #[test]
    fn test_create_internal_pg_catalog_schema_keeps_default_views() {
        let catalog = ParoCatalog::new("test_db".to_string());
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let info =
            CreateSchemaInfo::new("test_db".to_string(), PG_CATALOG.to_string()).with_internal();

        let entry = catalog
            .create_schema_internal(&txn, &info)
            .unwrap()
            .unwrap();
        let schema = entry.as_schema().unwrap();

        assert!(schema.is_internal());
        assert!(
            schema
                .get_view(txn.transaction_id, txn.start_time, "pg_prepared_statements")
                .is_some(),
            "internal pg_catalog schemas must retain lazy default views"
        );
    }

    #[test]
    fn test_default_materialization_does_not_burn_object_ids_after_first_install() {
        let catalog = ParoCatalog::new("test_db".to_string());
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, PG_CATALOG).unwrap();

        assert!(schema
            .get_view(txn.transaction_id, txn.start_time, "pg_prepared_statements")
            .is_some());
        let view_ids_before = committed_object_ids(schema.as_ref(), CatalogType::View);
        let function_ids_before =
            committed_object_ids(schema.as_ref(), CatalogType::ScalarFunction);
        let table_function_ids_before =
            committed_object_ids(schema.as_ref(), CatalogType::TableFunction);

        schema.materialize_default_entries(&txn);
        let view_ids_after = committed_object_ids(schema.as_ref(), CatalogType::View);
        let function_ids_after = committed_object_ids(schema.as_ref(), CatalogType::ScalarFunction);
        let table_function_ids_after =
            committed_object_ids(schema.as_ref(), CatalogType::TableFunction);
        assert_eq!(view_ids_after, view_ids_before);
        assert_eq!(function_ids_after, function_ids_before);
        assert_eq!(table_function_ids_after, table_function_ids_before);

        assert!(schema
            .get_view(txn.transaction_id, txn.start_time, "pg_prepared_statements")
            .is_some());
        assert_eq!(
            committed_object_ids(schema.as_ref(), CatalogType::View),
            view_ids_before
        );
    }

    #[test]
    fn test_concurrent_lazy_default_lookup_is_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test_db".to_string()));
        let create_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let info =
            CreateSchemaInfo::new("test_db".to_string(), PG_CATALOG.to_string()).with_internal();
        let entry = catalog
            .create_schema_internal(&create_txn, &info)
            .unwrap()
            .unwrap();
        let CatalogEntryEnum::Schema(schema) = entry.as_ref() else {
            panic!("expected schema entry");
        };
        let schema = Arc::clone(schema);
        let thread_count = 8;
        let barrier = Arc::new(std::sync::Barrier::new(thread_count));
        let seen_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        std::thread::scope(|scope| {
            for _ in 0..thread_count {
                let schema = Arc::clone(&schema);
                let barrier = Arc::clone(&barrier);
                let seen_ids = Arc::clone(&seen_ids);
                scope.spawn(move || {
                    let txn = CatalogSnapshot::read_only(u64::MAX);
                    barrier.wait();
                    let view = schema
                        .get_view(txn.transaction_id, txn.start_time, "pg_prepared_statements")
                        .unwrap();
                    seen_ids.lock().unwrap().push(view.object_id());
                });
            }
        });

        let seen_ids = seen_ids.lock().unwrap().clone();
        assert_eq!(seen_ids.len(), thread_count);
        assert!(seen_ids.windows(2).all(|pair| pair[0] == pair[1]));

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let installed = schema
            .collection(CatalogType::View)
            .expect("view collection")
            .scan_committed()
            .into_iter()
            .filter(|entry| entry.name() == "pg_prepared_statements")
            .count();
        assert_eq!(installed, 1);

        let view = schema
            .get_view(txn.transaction_id, txn.start_time, "pg_prepared_statements")
            .expect("default view should remain visible");
        assert_eq!(view.object_id(), seen_ids[0]);
    }

    #[test]
    fn test_is_default_schema() {
        assert!(ParoCatalog::is_default_schema_name("public"));
        assert!(ParoCatalog::is_default_schema_name("PUBLIC"));
        assert!(ParoCatalog::is_default_schema_name("system"));
        assert!(ParoCatalog::is_default_schema_name("information_schema"));
        assert!(ParoCatalog::is_default_schema_name("pg_catalog"));
        assert!(!ParoCatalog::is_default_schema_name("my_schema"));
    }

    #[test]
    fn test_get_catalog_type() {
        let catalog = ParoCatalog::new("test_db".to_string());
        assert_eq!(catalog.get_catalog_type(), "paro");
    }

    #[test]
    fn test_dependency_graph() {
        let catalog = ParoCatalog::new("test_db".to_string());
        assert!(catalog.get_dependency_graph().is_some());
    }

    #[test]
    fn test_get_metadata_info() {
        // In-memory catalog should return empty metadata info
        let catalog = ParoCatalog::new("test_db".to_string());
        let metadata = catalog.get_metadata_info();
        assert!(metadata.is_empty());

        // TODO: Implement metadata store integration for file-backed catalogs.
        let catalog = ParoCatalog::with_path("test_db".to_string(), "/tmp/test.db".to_string());
        let metadata = catalog.get_metadata_info();
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_gc_noops_for_empty_catalog() {
        let catalog = ParoCatalog::new("test_db".to_string());
        let stats = catalog.gc(100);
        assert_eq!(stats, CatalogGcStats::default());
    }

    #[test]
    fn test_gc_scans_tombstone_schema_contents_before_pruning_root_chain() {
        let catalog = ParoCatalog::new("test_db".to_string());
        let schema = Arc::new(SchemaEntry::with_object_id(
            "test_db".to_string(),
            "history".to_string(),
            CatalogObjectId::from_raw(42),
            Arc::clone(catalog.object_id_allocator()),
            catalog.gc_epoch_handle(),
            0,
        ));
        let schema_entry = Arc::new(CatalogEntryEnum::Schema(Arc::clone(&schema)));
        catalog
            .schemas
            .install_replayed(5, schema_entry, InstallMode::RejectExisting)
            .expect("install schema");

        let type_collection = schema
            .collection(CatalogType::Type)
            .expect("type collection");
        type_collection
            .install_replayed(
                5,
                make_type_entry("history", "event_status"),
                InstallMode::RejectExisting,
            )
            .expect("install first type version");
        publish_replace(
            type_collection,
            "event_status",
            make_type_entry("history", "event_status"),
            TRANSACTION_ID_START + 1,
            6,
            10,
        );
        publish_replace(
            type_collection,
            "event_status",
            make_type_entry("history", "event_status"),
            TRANSACTION_ID_START + 2,
            11,
            20,
        );

        let drop_snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 3, 21);
        let handle = catalog
            .schemas
            .stage_drop(&drop_snapshot, "history")
            .expect("stage drop")
            .expect("drop handle");
        handle.publish(30).expect("publish drop");

        let stats = catalog.gc(31);

        assert!(stats.chains_scanned >= 2);
        assert!(stats.chains_rebuilt >= 2);
        assert!(stats.nodes_pruned >= 3);
        assert_eq!(
            chain_timestamps(type_collection.as_ref(), "event_status"),
            vec![20]
        );
        assert_eq!(
            chain_timestamps(catalog.schemas.as_ref(), "history"),
            vec![30]
        );
    }
}
