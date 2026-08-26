// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation head metadata helpers.

use paro_common::durability::{PreparedMaintenancePlan, PreparedTabletPlan};
use paro_common::effect::{
    SearchGenerationPublication, StorageCommitOp, TabletApplyOp, TabletMutation,
};
use paro_common::error::Result;
use paro_common::journal::MaintenanceKind;
use paro_journal::{
    ApplyRequest, JournalApplyRuntime, MaintenanceAppendContext, TabletApplyPart, WaitMode,
};
use std::sync::Arc;

use crate::tablet::{SearchGenerationHeadMeta, SearchGenerationPublishGuard, TabletRef};

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

/// Durably publish one already-installed immutable manifest revision.
///
/// Manifest fragments are fsynced before this function is called. The WAL
/// record is therefore the visibility boundary: a crash before append leaves
/// only unreachable files, while a crash after append is completed by the
/// same tablet mutation during recovery.
pub(crate) struct SearchGenerationPublishCompletion {
    runtime: Option<Arc<JournalApplyRuntime>>,
    context: Option<MaintenanceAppendContext>,
}

impl SearchGenerationPublishCompletion {
    pub(crate) fn finish(self) -> Result<()> {
        if let (Some(runtime), Some(context)) = (self.runtime, self.context) {
            runtime.submit(ApplyRequest {
                lsn: context.lsn,
                durable_batch_lsn: context.durable_batch_lsn,
                commit_id: None,
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: Vec::<TabletApplyPart>::new(),
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })?;
        }
        Ok(())
    }
}

pub(crate) fn publish_head_for_state(
    tablet: &TabletRef,
    manifests: &ManifestStore,
    state: &SearchDefinitionState,
    guard: &SearchGenerationPublishGuard<'_>,
) -> Result<SearchGenerationPublishCompletion> {
    let Some(head) = head_for_state(manifests, state) else {
        return Ok(SearchGenerationPublishCompletion {
            runtime: None,
            context: None,
        });
    };
    let mutation = TabletMutation::PublishSearchGeneration {
        publication: SearchGenerationPublication::AdvanceInstalled,
        generation_ref: manifests.generation_ref(head.definition_id, head.generation_id)?,
        head,
    };
    let maintenance_plan = PreparedMaintenancePlan {
        kind: MaintenanceKind::IndexBackfill,
        catalog_ops: Vec::new(),
        storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
            tablet_id: tablet.tablet_id(),
            mutations: vec![mutation.clone()],
        })],
        apply_descriptors: Vec::new(),
        deferred_tasks: Vec::new(),
        tablets: vec![PreparedTabletPlan::new(
            tablet.tablet_id(),
            tablet.prepare_token(tablet.max_version()),
        )],
    };
    let maintenance_context = tablet
        .journal_coordinator()
        .map(|coordinator| coordinator.submit_maintenance(maintenance_plan))
        .transpose()?;

    tablet.apply_search_generation_publish_guarded(&mutation, guard)?;
    Ok(SearchGenerationPublishCompletion {
        runtime: tablet.journal_apply_runtime(),
        context: maintenance_context,
    })
}
