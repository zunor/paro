// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Provider-specific maintenance request derivation.

use serde_json::Value;

use paro_common::types::LogicalType;

use crate::tablet::TabletRef;

use crate::search::capability::{SearchIndexDefinition, SearchIndexKind};
use crate::search::maintenance::{HnswMaintenanceRequest, ProviderMaintenanceRequest};
use crate::search::manifest::LoadedManifest;

pub(crate) fn provider_maintenance_request_for_definition(
    definition: &SearchIndexDefinition,
    manifest: &LoadedManifest,
    tablet: &TabletRef,
) -> Option<ProviderMaintenanceRequest> {
    if definition.kind != SearchIndexKind::Hnsw {
        return None;
    }
    let dimension = hnsw_definition_dimension(definition, manifest, tablet);
    HnswMaintenanceRequest::new(
        definition,
        manifest.root.generation_id,
        manifest.tail_pending_entries.clone(),
        dimension,
        manifest.root.maintenance_state.recovery.priority,
    )
    .map(ProviderMaintenanceRequest::Hnsw)
}

fn hnsw_definition_dimension(
    definition: &SearchIndexDefinition,
    manifest: &LoadedManifest,
    tablet: &TabletRef,
) -> u32 {
    definition
        .provider_config
        .get("dimension")
        .and_then(Value::as_u64)
        .and_then(|dimension| u32::try_from(dimension).ok())
        .or_else(|| {
            manifest
                .root
                .generation_stats
                .hnsw_provider_stats()
                .map(|stats| stats.dimension)
        })
        .or_else(|| {
            let column_id = *definition.column_ids.first()?;
            let schema = tablet.schema()?;
            let column = schema.column_by_id(column_id)?;
            match &column.logical_type {
                LogicalType::Array(_, dimension) => u32::try_from(*dimension).ok(),
                _ => None,
            }
        })
        .unwrap_or_default()
}
