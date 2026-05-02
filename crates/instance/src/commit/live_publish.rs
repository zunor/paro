// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Live commit publish-plan construction.

use paro_common::error::Result;
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use paro_transaction::{
    ApplyTargetSet, CommitBackpressureController, CommitFrontier, ParticipantDescriptor,
    PostApplyFinalizePlan, RequiredPublishPlan,
};
use std::sync::Arc;

type ApplyWork = Box<dyn FnOnce() -> Result<()> + Send + 'static>;
type CommitIdSink = Box<dyn FnOnce(u64) + Send + 'static>;
type CommitApplyWork = Box<dyn FnOnce(u64) -> Result<()> + Send + 'static>;

pub struct LivePublishPlanInput {
    pub post_apply_finalize: PostApplyFinalizePlan,
    pub frontier: Arc<CommitFrontier>,
    pub backpressure: Option<Arc<CommitBackpressureController>>,
    pub participants: Arc<[ParticipantDescriptor]>,
    pub apply_targets: ApplyTargetSet,
    pub catalog_serial: bool,
    pub catalog_pre: ApplyWork,
    pub on_commit_id_assigned: CommitIdSink,
    pub tablet_parts: Vec<TabletApplyPart>,
    pub descriptor_phase: CommitApplyWork,
    pub catalog_post: CommitApplyWork,
}

pub fn build_required_publish_plan(input: LivePublishPlanInput) -> RequiredPublishPlan {
    let LivePublishPlanInput {
        post_apply_finalize,
        frontier,
        backpressure,
        participants,
        apply_targets,
        catalog_serial,
        catalog_pre,
        on_commit_id_assigned,
        tablet_parts,
        descriptor_phase,
        catalog_post,
    } = input;

    RequiredPublishPlan::new(
        Box::new(move |handle| {
            let commit_id = handle.commit_ts().into_raw();
            on_commit_id_assigned(commit_id);
            let published_handle = handle.clone();
            ApplyRequest {
                lsn: handle.durable_lsn(),
                durable_batch_lsn: handle.durable_batch_lsn(),
                commit_id: Some(handle.commit_ts().into_raw()),
                wait_mode: WaitMode::Published,
                catalog_serial,
                catalog_pre,
                tablet_parts,
                descriptor_phase: Box::new(move || descriptor_phase(commit_id)),
                catalog_post: Box::new(move || catalog_post(commit_id)),
                on_published: Box::new(move || {
                    post_apply_finalize
                        .finalize_and_enqueue(&published_handle)
                        .map_err(|error| error.to_paro_error())?;
                    frontier.mark_published(&published_handle);
                    if let Some(backpressure) = backpressure {
                        backpressure
                            .record_published(published_handle.commit_ts(), participants.as_ref());
                    }
                    Ok(())
                }),
            }
        }),
        apply_targets,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_transaction::{CommitDurableBatch, CommitTs, DurableCommitHandle};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_handle() -> DurableCommitHandle {
        let batch = Arc::new(
            CommitDurableBatch::new(
                20,
                20,
                1,
                128,
                Arc::from([64_u32]),
                7,
                CommitTs::new(11),
                CommitTs::new(11),
            )
            .unwrap(),
        );
        batch.handle_at(0).unwrap()
    }

    #[test]
    fn live_publish_hook_finalizes_before_marking_published() {
        let frontier = Arc::new(CommitFrontier::new());
        let order = Arc::new(AtomicUsize::new(0));
        let finalize_order = Arc::clone(&order);
        let frontier_for_finalize = Arc::clone(&frontier);
        let plan = build_required_publish_plan(LivePublishPlanInput {
            post_apply_finalize: PostApplyFinalizePlan::new(move |handle| {
                assert_eq!(finalize_order.fetch_add(1, Ordering::SeqCst), 0);
                assert_eq!(
                    frontier_for_finalize.published_commit_id(),
                    CommitTs::new(0)
                );
                assert_eq!(handle.commit_ts(), CommitTs::new(11));
                Ok(())
            }),
            frontier: Arc::clone(&frontier),
            backpressure: None,
            participants: Arc::from([]),
            apply_targets: Arc::from([]),
            catalog_serial: false,
            catalog_pre: Box::new(|| Ok(())),
            on_commit_id_assigned: Box::new(|_| {}),
            tablet_parts: Vec::new(),
            descriptor_phase: Box::new(|_| Ok(())),
            catalog_post: Box::new(|_| Ok(())),
        });

        let request = (plan.build_apply_request)(test_handle());
        (request.on_published)().unwrap();

        assert_eq!(order.load(Ordering::SeqCst), 1);
        assert_eq!(frontier.published_commit_id(), CommitTs::new(11));
    }
}
