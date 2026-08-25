// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::search_registry::register_search_definition;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, IndexBuildState, IndexCoverage, IndexType,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error;
use paro_storage::table::table_handle::TableHandle;
use std::sync::Arc;

pub(crate) fn restore_search_registry_definitions(catalog: &Arc<ParoCatalog>) {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schemas = catalog
        .get_schema_collection()
        .scan(txn.transaction_id, txn.start_time);

    for schema_entry in schemas {
        let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
            continue;
        };

        for index_entry in schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Index(index) = index_entry.as_ref() else {
                continue;
            };
            if !matches!(
                index.index_type,
                IndexType::HNSW | IndexType::Sparse | IndexType::FullText
            ) {
                continue;
            }

            let Some(table_entry) =
                schema.get_table(txn.transaction_id, txn.start_time, &index.table_name)
            else {
                index.mark_failed(Some(format!(
                    "search index table '{}' missing during registry restore",
                    index.table_name
                )));
                continue;
            };
            let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                index.mark_failed(Some(format!(
                    "search index target '{}' is not a table",
                    index.table_name
                )));
                continue;
            };
            let Some(storage) = table.get_storage() else {
                index.mark_failed(Some(format!(
                    "search index table '{}' has no storage during registry restore",
                    index.table_name
                )));
                continue;
            };

            if let Err(err) = register_search_definition(storage.as_ref(), index.as_ref()) {
                index.mark_failed(Some(format!(
                    "search registry restore failed for index '{}': {}",
                    index.base.base.name, err
                )));
                continue;
            }

            match storage.search_generation_coverage(index.base.base.object_id.raw()) {
                Ok(Some(coverage)) => {
                    index.mark_ready_with_coverage(Some(IndexCoverage::from_counts(
                        coverage.visible_version,
                        coverage.visible_segment_count,
                        coverage.indexed_segment_count,
                    )))
                }
                Ok(None) => index.mark_failed(Some(format!(
                    "search generation missing after registry restore for index '{}'",
                    index.base.base.name
                ))),
                Err(err) => index.mark_failed(Some(format!(
                    "search coverage restore failed for index '{}': {}",
                    index.base.base.name, err
                ))),
            }
        }
    }
}

fn runtime_scalar_index_coverage(
    storage: &TableHandle,
    column_id: u32,
) -> error::Result<IndexCoverage> {
    let visible_version = storage.max_version();
    let segments = storage.collect_segments(visible_version)?;
    let visible_segment_count = segments.len();
    let indexed_segment_count = segments
        .iter()
        .filter(|(_, segment)| segment.has_complete_scalar_index(column_id))
        .count();

    Ok(IndexCoverage::from_counts(
        visible_version,
        visible_segment_count,
        indexed_segment_count,
    ))
}

pub(crate) fn restore_runtime_art_indexes(catalog: &Arc<ParoCatalog>) {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schemas = catalog
        .get_schema_collection()
        .scan(txn.transaction_id, txn.start_time);

    for schema_entry in schemas {
        let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
            continue;
        };

        for index_entry in schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Index(index) = index_entry.as_ref() else {
                continue;
            };
            if index.index_type != IndexType::ART {
                continue;
            }

            let Some(table_entry) =
                schema.get_table(txn.transaction_id, txn.start_time, &index.table_name)
            else {
                index.mark_failed(Some(format!(
                    "ART index table '{}' missing during recovery restore",
                    index.table_name
                )));
                continue;
            };
            let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                index.mark_failed(Some(format!(
                    "ART index target '{}' is not a table",
                    index.table_name
                )));
                continue;
            };
            let Some(storage) = table.get_storage() else {
                index.mark_failed(Some(format!(
                    "ART index table '{}' has no storage during recovery restore",
                    index.table_name
                )));
                continue;
            };

            let column_ids = index.get_column_ids();
            let [column_id] = column_ids else {
                index.mark_failed(Some(
                    "ART recovery requires exactly one indexed column".to_string(),
                ));
                continue;
            };
            let column_id = column_id.index;

            if index.build_state() == IndexBuildState::Failed {
                let _ = storage.release_art_index(&index.base.base.name, column_id);
                continue;
            }

            if let Err(err) = storage.install_art_index(&index.base.base.name, column_id) {
                index.mark_failed(Some(format!("ART runtime restore failed: {}", err)));
                continue;
            }

            match runtime_scalar_index_coverage(storage.as_ref(), column_id) {
                Ok(coverage) if coverage.is_complete() => {
                    index.mark_ready_with_coverage(Some(coverage));
                }
                Ok(coverage) => {
                    let _ = storage.release_art_index(&index.base.base.name, column_id);
                    index.mark_failed(Some(format!(
                        "ART coverage incomplete after recovery: indexed={}/visible={} (version={})",
                        coverage.indexed_segment_count,
                        coverage.visible_segment_count,
                        coverage.visible_version
                    )));
                }
                Err(err) => {
                    let _ = storage.release_art_index(&index.base.base.name, column_id);
                    index.mark_failed(Some(format!("ART coverage validation failed: {}", err)));
                }
            }
        }
    }
}
