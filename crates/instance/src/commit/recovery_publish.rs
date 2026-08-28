// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Recovery commit publish-plan construction.

use paro_common::error::Result;
use paro_common::journal::JournalPublicationWatermarks;
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use paro_transaction::{ApplyTargetSet, CommitFrontier, RequiredPublishPlan};
use std::sync::Arc;

type ApplyWork = Box<dyn FnOnce() -> Result<()> + Send + 'static>;

pub struct RecoveryPublishPlanInput {
    pub frontier: Arc<CommitFrontier>,
    pub apply_targets: ApplyTargetSet,
    pub catalog_serial: bool,
    pub publication_watermarks: JournalPublicationWatermarks,
    pub catalog_pre: ApplyWork,
    pub tablet_parts: Vec<TabletApplyPart>,
    pub descriptor_phase: ApplyWork,
    pub catalog_post: ApplyWork,
}

pub fn build_recovery_required_publish_plan(
    input: RecoveryPublishPlanInput,
) -> RequiredPublishPlan {
    let RecoveryPublishPlanInput {
        frontier,
        apply_targets,
        catalog_serial,
        publication_watermarks,
        catalog_pre,
        tablet_parts,
        descriptor_phase,
        catalog_post,
    } = input;

    RequiredPublishPlan::new(
        Box::new(move |handle| {
            let published_handle = handle.clone();
            ApplyRequest {
                lsn: handle.durable_lsn(),
                durable_batch_lsn: handle.durable_batch_lsn(),
                commit_id: Some(handle.commit_ts().into_raw()),
                publication_watermarks,
                wait_mode: WaitMode::Published,
                catalog_serial,
                catalog_pre,
                tablet_parts,
                descriptor_phase,
                catalog_post,
                on_published: Box::new(move || {
                    frontier.mark_published(&published_handle);
                    Ok(())
                }),
            }
        }),
        apply_targets,
    )
}
