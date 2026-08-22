// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{IndexCatalogEntry, IndexType};
use paro_common::error::Result;
use paro_storage::search::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};
use paro_storage::table::table_handle::TableHandle;

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
    let provider_config = entry.provider_config.clone();
    Ok(Some(SearchIndexDefinition {
        definition_id: entry.base.base.object_id.raw(),
        table_id: storage.tablet().table_id(),
        name: entry.base.base.name.clone(),
        kind,
        column_ids: column_ids.clone(),
        expression: expression.clone(),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(kind),
        config_fingerprint: SearchIndexDefinition::try_compute_config_fingerprint(
            kind,
            &column_ids,
            expression.as_deref(),
            &provider_config,
        )?,
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
