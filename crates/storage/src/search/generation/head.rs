// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation head metadata helpers.

use paro_common::error::Result;

use crate::tablet::{SearchGenerationHeadMeta, TabletRef};

use super::view::SearchDefinitionState;
use crate::search::manifest::ManifestStore;

pub(crate) fn head_for_state(
    manifests: &ManifestStore,
    state: &SearchDefinitionState,
) -> Option<SearchGenerationHeadMeta> {
    state
        .manifest
        .as_ref()
        .map(|manifest| manifests.head_for_root(&manifest.root))
}

pub(crate) fn persist_head_for_state(
    tablet: &TabletRef,
    manifests: &ManifestStore,
    state: &SearchDefinitionState,
) -> Result<()> {
    if let Some(head) = head_for_state(manifests, state) {
        tablet.persist_search_generation_head(head)?;
    }
    Ok(())
}
