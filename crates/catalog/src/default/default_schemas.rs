// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lazily materialized built-in schemas.

use super::DefaultGenerator;
use crate::entry::{CatalogEntryEnum, CatalogObjectIdAllocator, SchemaEntry};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

static DEFAULT_SCHEMAS: &[&str] = &["public", "pg_catalog", "information_schema"];

/// Schemas whose metadata is marked internal and protected from user drops.
static SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

pub struct DefaultSchemaGenerator {
    catalog_name: String,
    object_id_allocator: Arc<CatalogObjectIdAllocator>,
    gc_epoch: Arc<AtomicU64>,
}

impl DefaultSchemaGenerator {
    pub fn new(
        catalog_name: String,
        object_id_allocator: Arc<CatalogObjectIdAllocator>,
        gc_epoch: Arc<AtomicU64>,
    ) -> Self {
        Self {
            catalog_name,
            object_id_allocator,
            gc_epoch,
        }
    }

    pub fn is_default_schema_name(name: &str) -> bool {
        let lower = name.to_lowercase();
        DEFAULT_SCHEMAS.iter().any(|schema| *schema == lower)
    }
}

pub(crate) fn default_schema_names() -> &'static [&'static str] {
    DEFAULT_SCHEMAS
}

pub(crate) fn configure_internal_schema(schema: &mut SchemaEntry) {
    let lower = schema.base.name.to_lowercase();
    if !SYSTEM_SCHEMAS.contains(&lower.as_str()) {
        return;
    }

    schema.internal = true;
    schema.base.internal = true;
}

impl DefaultGenerator for DefaultSchemaGenerator {
    fn is_default_entry(&self, name: &str) -> bool {
        Self::is_default_schema_name(name)
    }

    fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
        if !self.is_default_entry(name) {
            return None;
        }

        let lower = name.to_lowercase();

        let mut schema = SchemaEntry::new(
            self.catalog_name.clone(),
            lower.clone(),
            Arc::clone(&self.object_id_allocator),
            Arc::clone(&self.gc_epoch),
            0,
        );

        configure_internal_schema(&mut schema);

        Some(Arc::new(CatalogEntryEnum::Schema(Arc::new(schema))))
    }

    fn get_default_entries(&self) -> Vec<String> {
        DEFAULT_SCHEMAS
            .iter()
            .map(|schema| schema.to_string())
            .collect()
    }
}
