// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for `paro-catalog` integration tests.
//!
//! Each integration test binary is a separate crate; not every test uses every helper.

#![allow(dead_code)]

use paro_common::types::LogicalType;
use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::{TableColumnSpec, TableHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

pub fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default()
        .create_table(types)
        .expect("TableFactory::create_table")
}

pub fn create_test_meta_manager() -> Arc<TabletMetaManager> {
    static NEXT_TEST_META_ROOT: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
    let root = std::env::temp_dir().join(format!(
        "paro_catalog_integration_meta_{}_{}",
        std::process::id(),
        NEXT_TEST_META_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).expect("create meta root");
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(root.join("meta")).expect("meta store"));
    Arc::new(TabletMetaManager::with_store_and_data_root(store, &root))
}

pub fn create_table_with_meta_manager(
    types: &[LogicalType],
    meta_manager: Arc<TabletMetaManager>,
) -> TableHandle {
    TableFactory::new(Some(meta_manager))
        .create_table(types)
        .expect("TableFactory::create_table with meta manager")
}

pub fn create_table_from_specs(specs: &[TableColumnSpec]) -> TableHandle {
    TableFactory::default()
        .create_table_from_specs(specs)
        .expect("TableFactory::create_table_from_specs")
}

pub fn create_table_from_specs_with_meta_manager(
    specs: &[TableColumnSpec],
    meta_manager: Arc<TabletMetaManager>,
) -> TableHandle {
    TableFactory::new(Some(meta_manager))
        .create_table_from_specs(specs)
        .expect("TableFactory::create_table_from_specs with meta manager")
}
