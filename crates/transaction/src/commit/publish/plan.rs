// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Required publish request factory.

use crate::commit::apply_target::ApplyTargetSet;
use crate::commit::durable_handle::DurableCommitHandle;
use paro_journal::ApplyRequest;
use std::fmt;

pub type BuildApplyRequest =
    Box<dyn FnOnce(DurableCommitHandle) -> ApplyRequest<()> + Send + 'static>;

pub struct RequiredPublishPlan {
    pub build_apply_request: BuildApplyRequest,
    pub apply_targets: ApplyTargetSet,
}

impl RequiredPublishPlan {
    pub fn new(build_apply_request: BuildApplyRequest, apply_targets: ApplyTargetSet) -> Self {
        Self {
            build_apply_request,
            apply_targets,
        }
    }

    #[cfg(test)]
    pub(crate) fn noop_for_tests() -> Self {
        use paro_journal::{TabletApplyPart, WaitMode};
        use std::sync::Arc;

        Self {
            build_apply_request: Box::new(|handle| ApplyRequest {
                lsn: handle.durable_lsn(),
                durable_batch_lsn: handle.durable_batch_lsn(),
                commit_id: Some(handle.commit_ts().into_raw()),
                publication_watermarks: paro_common::journal::JournalPublicationWatermarks::default(
                ),
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: Vec::<TabletApplyPart>::new(),
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            }),
            apply_targets: Arc::from([]),
        }
    }
}

impl fmt::Debug for RequiredPublishPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequiredPublishPlan")
            .field("apply_targets", &self.apply_targets)
            .finish_non_exhaustive()
    }
}
