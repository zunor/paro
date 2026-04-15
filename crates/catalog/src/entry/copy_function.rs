//! Copy Function Catalog Entry
//!

use super::catalog_entry::{
    allocate_object_id, AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_function::copy::CopyFunction;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

/// Copy function catalog entry.
#[derive(Debug)]
pub struct CopyFunctionCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// The copy function definition
    pub function: CopyFunction,
}

impl CopyFunctionCatalogEntry {
    pub fn new(
        catalog: String,
        schema_name: String,
        function: CopyFunction,
        timestamp: u64,
    ) -> Self {
        let oid = allocate_object_id();
        let base = SchemaEntryMeta::new(
            CatalogType::CopyFunction,
            catalog,
            schema_name,
            function.name.clone(),
            oid,
            timestamp,
        );

        Self { base, function }
    }

    pub fn new_internal(catalog: String, schema_name: String, function: CopyFunction) -> Self {
        let oid = allocate_object_id();
        let mut base = SchemaEntryMeta::new(
            CatalogType::CopyFunction,
            catalog,
            schema_name,
            function.name.clone(),
            oid,
            0,
        );
        base.base.internal = true;

        Self { base, function }
    }

    pub fn get_function(&self) -> &CopyFunction {
        &self.function
    }
}

impl CatalogEntry for CopyFunctionCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::CopyFunction
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
        Err(paro_error::not_implemented("ALTER COPY FUNCTION"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let new_entry = CopyFunctionCatalogEntry {
            base: SchemaEntryMeta::new(
                CatalogType::CopyFunction,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                self.base.base.name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            ),
            function: self.function.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let info = CreateInfo::new(
            CatalogType::CopyFunction,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        format!("-- COPY FUNCTION {} (built-in)", self.base.base.name)
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

impl StandardEntry for CopyFunctionCatalogEntry {
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

impl InCatalogEntry for CopyFunctionCatalogEntry {}
