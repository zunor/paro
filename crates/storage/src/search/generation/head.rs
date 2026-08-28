// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation head metadata helpers.

use paro_common::durability::{PreparedMaintenancePlan, PreparedTabletPlan};
use paro_common::effect::{
    SearchGenerationPublication, StorageCommitOp, TabletApplyOp, TabletMutation,
};
use paro_common::error::Result;
use paro_common::journal::MaintenanceKind;
use std::sync::Arc;

use crate::durable_maintenance::DurableMaintenanceApplyCompletion;
use crate::tablet::{
    SearchGenerationHeadMeta, SearchGenerationPublishGuard, SearchGenerationPublishOutcome,
    TabletRef,
};

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

pub(crate) struct SearchGenerationPublishCompletion {
    durable: Option<DurableMaintenanceApplyCompletion>,
    publication_result: Result<SearchGenerationPublishOutcome>,
}

impl SearchGenerationPublishCompletion {
    pub(crate) fn publication_succeeded(&self) -> bool {
        matches!(
            self.publication_result,
            Ok(SearchGenerationPublishOutcome::Advanced
                | SearchGenerationPublishOutcome::AlreadyCurrent)
        )
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        if let Some(durable) = self.durable.take() {
            return durable.finish();
        }
        self.publication_result
            .clone()
            .map(|outcome| match outcome {
                SearchGenerationPublishOutcome::Advanced
                | SearchGenerationPublishOutcome::AlreadyCurrent => (),
                SearchGenerationPublishOutcome::Superseded => unreachable!(
                    "online superseded generation publication is normalized before completion"
                ),
                SearchGenerationPublishOutcome::Retired => unreachable!(
                    "online retired generation publication is normalized before completion"
                ),
            })
    }
}

/// Durably publish one already-installed immutable manifest revision.
///
/// Manifest fragments are fsynced before this function is called. Once WAL
/// append succeeds, the returned owner is armed with an apply request. Normal
/// callers submit it synchronously after releasing registry locks; unwinding
/// or an early return submits it asynchronously from `Drop`, so a durable LSN
/// can never be omitted from the ordered apply runtime.
pub(crate) fn publish_head_for_state(
    tablet: &TabletRef,
    manifests: &ManifestStore,
    state: &SearchDefinitionState,
    guard: &SearchGenerationPublishGuard<'_>,
) -> Result<SearchGenerationPublishCompletion> {
    let Some(head) = head_for_state(manifests, state) else {
        return Ok(SearchGenerationPublishCompletion {
            durable: None,
            publication_result: Ok(SearchGenerationPublishOutcome::AlreadyCurrent),
        });
    };
    // The publication guard serializes this check with durable retirement.
    // Rejecting here is a normal canceled-maintenance outcome and, crucially,
    // happens before WAL append. Once append succeeds, inability to apply the
    // record is a fatal storage error rather than a DROP race.
    if tablet.is_search_definition_retired(head.definition_id) {
        return Err(paro_common::error::invalid_input(format!(
            "search definition {} was retired before maintenance publication",
            head.definition_id
        )));
    }
    match tablet.preflight_search_generation_publish_guarded(&head, guard)? {
        SearchGenerationPublishOutcome::Advanced => {}
        SearchGenerationPublishOutcome::AlreadyCurrent => {
            if let Some(manifest) = state.manifest.as_ref() {
                manifest.mark_revision_published();
            }
            return Ok(SearchGenerationPublishCompletion {
                durable: None,
                publication_result: Ok(SearchGenerationPublishOutcome::AlreadyCurrent),
            });
        }
        SearchGenerationPublishOutcome::Superseded => {
            return Err(paro_common::error::invalid_input(
                "online search generation publication was superseded before WAL append",
            ));
        }
        SearchGenerationPublishOutcome::Retired => {
            return Err(paro_common::error::invalid_input(format!(
                "search definition {} was retired before WAL append",
                head.definition_id
            )));
        }
    }
    let mutation = TabletMutation::PublishSearchGeneration {
        publication: SearchGenerationPublication::AdvanceInstalled,
        generation_ref: manifests.generation_ref(head.definition_id, head.generation_id)?,
        head,
    };
    let maintenance_plan = PreparedMaintenancePlan {
        kind: MaintenanceKind::SearchGenerationMaintenance,
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
    let coordinator = tablet.journal_coordinator();
    let runtime = if coordinator.is_some() {
        Some(tablet.journal_apply_runtime().ok_or_else(|| {
            paro_common::error::internal(
                "search generation maintenance requires a bound journal apply runtime",
            )
        })?)
    } else {
        None
    };
    let maintenance_context = coordinator
        .map(|coordinator| coordinator.submit_maintenance(maintenance_plan))
        .transpose()?;
    if maintenance_context.is_some() {
        // A durable WAL record now names this immutable revision. It is no
        // longer rollback-owned even if live apply subsequently fails; the
        // apply runtime will surface that fatal error and recovery must retain
        // the referenced files.
        if let Some(manifest) = state.manifest.as_ref() {
            manifest.mark_revision_published();
        }
    }

    let publication_result = tablet
        .apply_search_generation_publish_guarded(&mutation, guard)
        .and_then(|outcome| match outcome {
            SearchGenerationPublishOutcome::Advanced
            | SearchGenerationPublishOutcome::AlreadyCurrent => Ok(outcome),
            SearchGenerationPublishOutcome::Superseded => Err(paro_common::error::invalid_input(
                "online search generation publication was superseded by a newer durable head",
            )),
            SearchGenerationPublishOutcome::Retired => Err(paro_common::error::invalid_input(
                "online search generation publication targeted a retired definition",
            )),
        });
    if publication_result.is_ok() {
        if let Some(manifest) = state.manifest.as_ref() {
            manifest.mark_revision_published();
        }
    }
    let durable = maintenance_context
        .map(|context| {
            let tablet = Arc::clone(tablet);
            let tablet_id = tablet.tablet_id();
            let lsn = context.lsn;
            let completion = DurableMaintenanceApplyCompletion::arm(
                runtime
                    .as_ref()
                    .expect("runtime validated before WAL append")
                    .clone(),
                context.lsn,
                context.durable_batch_lsn,
                paro_common::journal::JournalPublicationWatermarks::maintenance(
                    context.maintenance_id,
                ),
                tablet_id,
                move || tablet.note_applied_lsn(lsn),
            );
            completion.record_terminal_result(publication_result.clone().map(|_| ()))?;
            Ok::<_, paro_common::error::ParoError>(completion)
        })
        .transpose()?;
    Ok(SearchGenerationPublishCompletion {
        durable,
        publication_result,
    })
}
