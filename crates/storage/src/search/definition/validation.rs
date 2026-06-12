// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search definition validation against tablet schema.

use serde_json::Value;

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
        SearchIndexKind::FullText | SearchIndexKind::Hnsw => Ok(()),
    }
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
    let encoding = definition
        .provider_config
        .get("physical_encoding")
        .and_then(Value::as_str)
        .unwrap_or("binary-v1");
    match encoding {
        "binary-v1" | "typed-binary-v1" => {
            if !matches!(column.logical_type, LogicalType::Blob) {
                return Err(paro_error::not_supported(format!(
                    "Sparse index requires Blob binary sparse row image column, got {:?} for column {}",
                    column.logical_type, column_id
                )));
            }
        }
        _ => {
            return Err(paro_error::not_supported(format!(
                "Sparse index physical_encoding must be binary-v1, got {}",
                encoding
            )));
        }
    }
    Ok(())
}
