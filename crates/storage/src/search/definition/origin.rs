// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde_json::json;

use crate::tablet::{ColumnId, TabletColumn, TabletSchema};
use paro_common::error::Result;
use paro_common::types::LogicalType;

use super::super::capability::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};

pub(crate) const SCHEMA_SEED_BIT: u64 = 1 << 63;

pub(crate) fn schema_seed_definition(
    table_id: u64,
    column: &TabletColumn,
) -> Result<SearchIndexDefinition> {
    let dimension = match &column.logical_type {
        LogicalType::Array(_, dimension) => *dimension as u64,
        _ => 0,
    };
    let provider_config = json!({
        "m": column.hnsw_m,
        "ef_construct": column.hnsw_ef_construct,
        "distance": column.hnsw_distance,
        "dimension": dimension,
    });
    Ok(SearchIndexDefinition {
        definition_id: SCHEMA_SEED_BIT | column.id as u64,
        table_id,
        name: format!("__schema_hnsw_col_{}", column.id),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![column.id],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[column.id],
            None,
            &provider_config,
        ),
        provider_config,
    })
}

pub(crate) fn restored_schema_seed_definition(
    table_id: u64,
    schema: &TabletSchema,
    dropped_definition: &SearchIndexDefinition,
) -> Result<Option<(ColumnId, SearchIndexDefinition)>> {
    if dropped_definition.kind != SearchIndexKind::Hnsw || dropped_definition.column_ids.len() != 1
    {
        return Ok(None);
    }
    let column_id = dropped_definition.column_ids[0];
    let Some(column) = schema.column_by_id(column_id) else {
        return Ok(None);
    };
    if !column.index_hnsw {
        return Ok(None);
    }
    Ok(Some((column_id, schema_seed_definition(table_id, column)?)))
}

pub(crate) fn hnsw_schema_seed_definitions(
    table_id: u64,
    schema: &TabletSchema,
) -> Vec<(ColumnId, Result<SearchIndexDefinition>)> {
    schema
        .columns()
        .iter()
        .filter(|column| column.index_hnsw)
        .map(|column| (column.id, schema_seed_definition(table_id, column)))
        .collect()
}
