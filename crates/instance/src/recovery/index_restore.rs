// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, IndexBuildState, IndexCoverage, IndexType,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error;
use paro_storage::table::table_handle::TableHandle;
use std::sync::Arc;

pub(crate) fn reconcile_fulltext_index_coverage(catalog: &Arc<ParoCatalog>) {
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
            if index.index_type != IndexType::FullText {
                continue;
            }

            let Some(binding) = index.fulltext_binding() else {
                index.mark_failed(Some(
                    "fulltext index metadata missing source binding".to_string(),
                ));
                continue;
            };

            let Some(table_entry) =
                schema.get_table(txn.transaction_id, txn.start_time, &index.table_name)
            else {
                index.mark_failed(Some(format!(
                    "fulltext index table '{}' missing during recovery validation",
                    index.table_name
                )));
                continue;
            };
            let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                index.mark_failed(Some(format!(
                    "fulltext index target '{}' is not a table",
                    index.table_name
                )));
                continue;
            };
            let Some(storage) = table.get_storage() else {
                index.mark_failed(Some(format!(
                    "fulltext index table '{}' has no storage during recovery validation",
                    index.table_name
                )));
                continue;
            };

            match storage.fulltext_index_coverage(binding.column_id.index) {
                Ok(coverage) if coverage.is_complete() => {
                    index.mark_ready_with_coverage(Some(IndexCoverage::from_counts(
                        coverage.visible_version,
                        coverage.visible_segment_count,
                        coverage.indexed_segment_count,
                    )));
                    storage.mark_declared_fulltext_index_with_config(
                        binding.column_id.index,
                        &binding.config,
                    );
                }
                Ok(coverage) => {
                    index.mark_failed(Some(format!(
                        "fulltext coverage incomplete after recovery: indexed={}/visible={} (version={})",
                        coverage.indexed_segment_count,
                        coverage.visible_segment_count,
                        coverage.visible_version
                    )));
                    storage.unmark_declared_fulltext_index(binding.column_id.index);
                }
                Err(err) => {
                    index.mark_failed(Some(format!(
                        "fulltext coverage validation failed: {}",
                        err
                    )));
                    storage.unmark_declared_fulltext_index(binding.column_id.index);
                }
            }
        }
    }
}

fn runtime_art_index_coverage(
    storage: &TableHandle,
    column_id: u32,
) -> error::Result<IndexCoverage> {
    let visible_version = storage.max_version();
    let segments = storage.collect_segments(visible_version)?;
    let visible_segment_count = segments.len();
    let indexed_segment_count = segments
        .iter()
        .filter(|(_, segment)| segment.art_index(column_id).is_some())
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
                storage.unmark_declared_art_index(column_id);
                let _ = storage.remove_runtime_art_index(column_id);
                continue;
            }

            storage.mark_declared_art_index(column_id);
            if let Err(err) = storage.build_runtime_art_index(column_id) {
                storage.unmark_declared_art_index(column_id);
                let _ = storage.remove_runtime_art_index(column_id);
                index.mark_failed(Some(format!("ART runtime restore failed: {}", err)));
                continue;
            }

            match runtime_art_index_coverage(storage.as_ref(), column_id) {
                Ok(coverage) if coverage.is_complete() => {
                    index.mark_ready_with_coverage(Some(coverage));
                }
                Ok(coverage) => {
                    storage.unmark_declared_art_index(column_id);
                    let _ = storage.remove_runtime_art_index(column_id);
                    index.mark_failed(Some(format!(
                        "ART coverage incomplete after recovery: indexed={}/visible={} (version={})",
                        coverage.indexed_segment_count,
                        coverage.visible_segment_count,
                        coverage.visible_version
                    )));
                }
                Err(err) => {
                    storage.unmark_declared_art_index(column_id);
                    let _ = storage.remove_runtime_art_index(column_id);
                    index.mark_failed(Some(format!("ART coverage validation failed: {}", err)));
                }
            }
        }
    }
}
