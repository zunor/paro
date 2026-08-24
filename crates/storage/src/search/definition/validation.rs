// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search definition validation against tablet schema.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::tablet::TabletRef;

use crate::search::capability::{SearchIndexDefinition, SearchIndexKind};

pub(crate) fn validate_definition(
    definition: &SearchIndexDefinition,
    tablet: &TabletRef,
) -> Result<()> {
    match definition.kind {
        SearchIndexKind::Sparse => validate_sparse_definition(definition, tablet),
        SearchIndexKind::Hnsw => validate_hnsw_definition(definition, tablet),
        SearchIndexKind::FullText => definition.fulltext_provider_config().map(|_| ()),
    }
}

fn validate_hnsw_definition(definition: &SearchIndexDefinition, tablet: &TabletRef) -> Result<()> {
    let [column_id] = definition.column_ids.as_slice() else {
        return Err(paro_error::invalid_input(
            "HNSW search definition requires exactly one vector column",
        ));
    };
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("table schema missing for HNSW definition"))?;
    let column = schema.column_by_id(*column_id).ok_or_else(|| {
        paro_error::column_not_found(format!(
            "HNSW index column {} not found in schema",
            column_id
        ))
    })?;
    let LogicalType::Array(inner, dimension) = &column.logical_type else {
        return Err(paro_error::not_supported(format!(
            "HNSW index requires VECTOR(N), got {:?} for column {}",
            column.logical_type, column_id
        )));
    };
    if !matches!(inner.as_ref(), LogicalType::Float) {
        return Err(paro_error::not_supported(format!(
            "HNSW index requires VECTOR(N), got {:?} for column {}",
            column.logical_type, column_id
        )));
    }
    let config = definition.hnsw_provider_config()?;
    if config.dimension != *dimension as u32 {
        return Err(paro_error::invalid_input(format!(
            "HNSW configured dimension {} does not match column {} dimension {}",
            config.dimension, column_id, dimension
        )));
    }
    for &filter_column_id in &config.filter_columns {
        if filter_column_id == *column_id {
            return Err(paro_error::invalid_input(
                "HNSW filter column cannot be the indexed vector column",
            ));
        }
        let filter_column = schema.column_by_id(filter_column_id).ok_or_else(|| {
            paro_error::column_not_found(format!(
                "HNSW filter column {filter_column_id} not found in schema"
            ))
        })?;
        if !crate::index::supports_ordered_bytes(&filter_column.logical_type) {
            return Err(paro_error::not_supported(format!(
                "HNSW predicate topology requires an orderable scalar filter column, got {:?} for column {}",
                filter_column.logical_type, filter_column_id
            )));
        }
    }
    Ok(())
}

fn validate_sparse_definition(
    definition: &SearchIndexDefinition,
    tablet: &TabletRef,
) -> Result<()> {
    let [column_id] = definition.column_ids.as_slice() else {
        return Err(paro_error::invalid_input(
            "Sparse search definition requires exactly one column",
        ));
    };
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("table schema missing for Sparse definition"))?;
    let column = schema.column_by_id(*column_id).ok_or_else(|| {
        paro_error::column_not_found(format!(
            "Sparse index column {} not found in schema",
            column_id
        ))
    })?;
    let config = definition.sparse_provider_config()?;
    match config.physical_encoding {
        crate::search::SparsePhysicalEncoding::BinaryV1 => {
            if !matches!(column.logical_type, LogicalType::Blob) {
                return Err(paro_error::not_supported(format!(
                    "Sparse index requires Blob binary sparse row image column, got {:?} for column {}",
                    column.logical_type, column_id
                )));
            }
        }
    }
    Ok(())
}
