// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Provider-specific maintenance request derivation.

use paro_common::error as paro_error;
use paro_common::error::Result;

use crate::search::capability::SearchIndexKind;
use crate::search::generation::view::SearchDefinitionState;
use crate::search::maintenance::{HnswMaintenanceRequest, ProviderMaintenanceRequest};
use crate::search::manifest::LoadedManifest;

pub(crate) fn provider_maintenance_request_for_definition(
    state: &SearchDefinitionState,
    manifest: &LoadedManifest,
) -> Result<Option<ProviderMaintenanceRequest>> {
    if state.definition.kind != SearchIndexKind::Hnsw {
        return Ok(None);
    }
    let provider = state.hnsw_provider_config.as_deref().ok_or_else(|| {
        paro_error::data_corrupted("HNSW registry state is missing its provider contract")
    })?;
    Ok(HnswMaintenanceRequest::new(
        &state.definition,
        provider,
        manifest.root.generation_id,
        manifest.tail_pending_entries.clone(),
        manifest.root.maintenance_state.recovery.priority,
    )
    .map(ProviderMaintenanceRequest::Hnsw))
}
