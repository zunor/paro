// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{IndexCatalogEntry, IndexType};
use paro_common::error::{self as paro_error, Result};
use paro_storage::search::{SearchIndexDefinition, SearchIndexKind};
use paro_storage::table::table_handle::TableHandle;
use serde_json::{json, Value};

pub(crate) fn register_search_definition(
    storage: &TableHandle,
    entry: &IndexCatalogEntry,
) -> Result<()> {
    let Some(definition) = search_definition_from_entry(storage, entry)? else {
        return Ok(());
    };
    storage.register_search_definition(definition)
}

pub(crate) fn unregister_search_definition_by_name(
    storage: &TableHandle,
    index_type: &str,
    index_name: &str,
) -> Result<()> {
    if search_kind(IndexType::from_str(index_type)).is_none() {
        return Ok(());
    }
    storage.unregister_search_definition_by_name(index_name)
}

pub(crate) fn search_kind(index_type: IndexType) -> Option<SearchIndexKind> {
    match index_type {
        IndexType::HNSW => Some(SearchIndexKind::Hnsw),
        IndexType::Sparse => Some(SearchIndexKind::Sparse),
        IndexType::FullText => Some(SearchIndexKind::FullText),
        _ => None,
    }
}

pub(crate) fn search_definition_from_entry(
    storage: &TableHandle,
    entry: &IndexCatalogEntry,
) -> Result<Option<SearchIndexDefinition>> {
    let Some(kind) = search_kind(entry.index_type) else {
        return Ok(None);
    };
    let column_ids = entry
        .get_column_ids()
        .iter()
        .map(|column| column.index)
        .collect::<Vec<_>>();
    let expression = search_expression(entry);
    let provider_config = search_provider_config(storage, entry)?;
    Ok(Some(SearchIndexDefinition {
        definition_id: entry.base.base.object_id.raw(),
        table_id: storage.tablet().table_id(),
        name: entry.base.base.name.clone(),
        kind,
        column_ids: column_ids.clone(),
        expression: expression.clone(),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            kind,
            &column_ids,
            expression.as_deref(),
            &provider_config,
        ),
        provider_config,
    }))
}

fn search_expression(entry: &IndexCatalogEntry) -> Option<String> {
    if entry.index_type != IndexType::FullText {
        return None;
    }
    let binding = entry.fulltext_binding()?;
    Some(format!(
        "to_tsvector('{}', col_{})",
        binding.config, binding.column_id.index
    ))
}

fn search_provider_config(storage: &TableHandle, entry: &IndexCatalogEntry) -> Result<Value> {
    match entry.index_type {
        IndexType::HNSW => {
            let [column] = entry.get_column_ids() else {
                return Err(paro_error::not_supported(
                    "HNSW search definition requires exactly one indexed column",
                ));
            };
            let schema = storage
                .tablet()
                .schema()
                .ok_or_else(|| paro_error::internal("table schema missing for HNSW config"))?;
            let column = schema.column_by_id(column.index).ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "HNSW index column {} not found in schema",
                    column.index
                ))
            })?;
            Ok(json!({
                "m": column.hnsw_m,
                "ef_construct": column.hnsw_ef_construct,
                "distance": column.hnsw_distance,
            }))
        }
        IndexType::Sparse => Ok(json!({})),
        IndexType::FullText => {
            let config = entry
                .fulltext_binding()
                .map(|binding| binding.config.clone())
                .unwrap_or_else(|| "simple".to_string());
            Ok(json!({ "config": config }))
        }
        _ => Ok(json!({})),
    }
}
