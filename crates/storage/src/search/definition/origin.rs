// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::index::hnsw::{DistanceMetric, HnswSearchPolicy};
use crate::search::{
    HnswInlineConfig, HnswInlineThreshold, HnswProviderConfig, DEFAULT_HNSW_BUILD_SEED,
};
use crate::tablet::{ColumnId, TabletColumn, TabletSchema};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::super::capability::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};

pub(crate) const SCHEMA_SEED_BIT: u64 = 1 << 63;

pub(crate) fn schema_seed_definition(
    table_id: u64,
    column: &TabletColumn,
) -> Result<SearchIndexDefinition> {
    let dimension = match &column.logical_type {
        LogicalType::Array(inner, dimension) if matches!(inner.as_ref(), LogicalType::Float) => {
            u32::try_from(*dimension)
                .map_err(|_| paro_error::out_of_range("HNSW vector dimension"))?
        }
        other => {
            return Err(paro_error::not_supported(format!(
                "schema HNSW column requires VECTOR(N), got {other:?}"
            )))
        }
    };
    let distance = DistanceMetric::from_u8(column.hnsw_distance).ok_or_else(|| {
        paro_error::invalid_input(format!(
            "invalid HNSW distance tag {} on column {}",
            column.hnsw_distance, column.id
        ))
    })?;
    let defaults = HnswSearchPolicy::default();
    let inline = HnswInlineThreshold::DEFAULT;
    let provider_config = HnswProviderConfig {
        version: crate::search::HNSW_PROVIDER_CONFIG_VERSION,
        dimension,
        distance,
        m: u32::try_from(column.hnsw_m).map_err(|_| paro_error::out_of_range("HNSW m"))?,
        ef_construct: u32::try_from(column.hnsw_ef_construct)
            .map_err(|_| paro_error::out_of_range("HNSW ef_construct"))?,
        ef_search: u32::try_from(column.hnsw_ef_construct)
            .map_err(|_| paro_error::out_of_range("HNSW ef_search"))?,
        distance_cost: defaults.distance_cost,
        build_seed: DEFAULT_HNSW_BUILD_SEED,
        proposal_wave_size: crate::index::hnsw::DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
        warmup_point_count: crate::index::hnsw::DEFAULT_HNSW_WARMUP_POINT_COUNT,
        filter_columns: Vec::new(),
        filter_block_rows: crate::index::hnsw::DEFAULT_HNSW_FILTER_BLOCK_ROWS,
        filter_m: crate::index::hnsw::DEFAULT_HNSW_FILTER_M,
        inline_threshold: HnswInlineConfig {
            enabled: true,
            max_vector_count: inline.max_vector_count,
            max_graph_memory_bytes: inline.max_graph_memory_bytes,
            max_dimension: inline.max_dimension,
        },
    }
    .validated()?
    .to_value()?;
    Ok(SearchIndexDefinition {
        definition_id: SCHEMA_SEED_BIT | column.id as u64,
        table_id,
        name: format!("__schema_hnsw_col_{}", column.id),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![column.id],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::try_compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[column.id],
            None,
            &provider_config,
        )?,
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
