// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::job_orchestrator::run_job_with_lifecycle_and_search_inline_builders;
use crate::compaction::plan::types::{CompactionJobId, CompactionLifecycleState, CompactionPlan};
use crate::tablet::Tablet;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type LifecycleNotifier = Arc<dyn Fn(CompactionLifecycleState) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionTaskState {
    Init,
    Running,
    Failed,
    Success,
}

pub trait CompactionTask: Send + Sync {
    fn run(&mut self) -> Result<()>;

    fn stop(&mut self);

    fn state(&self) -> CompactionTaskState;

    fn context(&self) -> &CompactionPlan;
}

pub struct HorizontalCompactionTask {
    tablet: Arc<Tablet>,
    plan: CompactionPlan,
    allocator: Arc<dyn Allocator>,
    job_id: CompactionJobId,
    cancel_token: CancellationToken,
    lifecycle_notifier: Option<LifecycleNotifier>,
    state: CompactionTaskState,
}

impl HorizontalCompactionTask {
    pub fn new(tablet: Arc<Tablet>, plan: CompactionPlan, allocator: Arc<dyn Allocator>) -> Self {
        Self::new_with_job_id_and_cancel_token(
            tablet,
            plan,
            allocator,
            next_local_job_id(),
            CancellationToken::new(),
        )
    }

    pub fn new_with_job_id(
        tablet: Arc<Tablet>,
        plan: CompactionPlan,
        allocator: Arc<dyn Allocator>,
        job_id: CompactionJobId,
    ) -> Self {
        Self::new_with_job_id_and_cancel_token(
            tablet,
            plan,
            allocator,
            job_id,
            CancellationToken::new(),
        )
    }

    pub fn new_with_job_id_and_cancel_token(
        tablet: Arc<Tablet>,
        plan: CompactionPlan,
        allocator: Arc<dyn Allocator>,
        job_id: CompactionJobId,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            plan,
            tablet,
            allocator,
            job_id,
            cancel_token,
            lifecycle_notifier: None,
            state: CompactionTaskState::Init,
        }
    }

    pub fn with_lifecycle_notifier(mut self, notifier: LifecycleNotifier) -> Self {
        self.lifecycle_notifier = Some(notifier);
        self
    }
}

impl CompactionTask for HorizontalCompactionTask {
    fn run(&mut self) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(paro_error::query_canceled());
        }

        self.state = CompactionTaskState::Running;
        let notifier = self.lifecycle_notifier.clone();
        let search_inline_builders = self.tablet.search_inline_builders_for_compaction();
        run_job_with_lifecycle_and_search_inline_builders(
            &self.tablet,
            Arc::new(self.plan.clone()),
            self.job_id,
            self.allocator.clone(),
            search_inline_builders,
            self.cancel_token.clone(),
            move |state| {
                if let Some(notifier) = &notifier {
                    notifier(state);
                }
            },
        )?;
        self.state = CompactionTaskState::Success;
        Ok(())
    }

    fn stop(&mut self) {
        self.cancel_token.cancel();
    }

    fn state(&self) -> CompactionTaskState {
        self.state
    }

    fn context(&self) -> &CompactionPlan {
        &self.plan
    }
}

pub struct VerticalCompactionTask {
    inner: HorizontalCompactionTask,
}

impl VerticalCompactionTask {
    pub fn new(tablet: Arc<Tablet>, plan: CompactionPlan, allocator: Arc<dyn Allocator>) -> Self {
        Self {
            inner: HorizontalCompactionTask::new(tablet, plan, allocator),
        }
    }

    pub fn new_with_job_id(
        tablet: Arc<Tablet>,
        plan: CompactionPlan,
        allocator: Arc<dyn Allocator>,
        job_id: CompactionJobId,
    ) -> Self {
        Self {
            inner: HorizontalCompactionTask::new_with_job_id(tablet, plan, allocator, job_id),
        }
    }

    pub fn new_with_job_id_and_cancel_token(
        tablet: Arc<Tablet>,
        plan: CompactionPlan,
        allocator: Arc<dyn Allocator>,
        job_id: CompactionJobId,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            inner: HorizontalCompactionTask::new_with_job_id_and_cancel_token(
                tablet,
                plan,
                allocator,
                job_id,
                cancel_token,
            ),
        }
    }

    pub fn with_lifecycle_notifier(mut self, notifier: LifecycleNotifier) -> Self {
        self.inner = self.inner.with_lifecycle_notifier(notifier);
        self
    }
}

impl CompactionTask for VerticalCompactionTask {
    fn run(&mut self) -> Result<()> {
        self.inner.run()
    }

    fn stop(&mut self) {
        self.inner.stop();
    }

    fn state(&self) -> CompactionTaskState {
        self.inner.state()
    }

    fn context(&self) -> &CompactionPlan {
        self.inner.context()
    }
}

fn next_local_job_id() -> CompactionJobId {
    static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(1);
    CompactionJobId(NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::plan::types::{
        CompactionPlanId, CompactionReason, CumulativePointAction, ExecutionLayout, MergeSemantics,
        PolicyKind, ReadSnapshot,
    };
    use crate::rowset::RowsetSharedPtr;
    use crate::search::SearchInlineBuilderSet;
    use crate::tablet::{
        KeysType, RowsetPublishObserver, TabletColumn, TabletSchema, TabletSchemaRef, Version,
    };
    use paro_common::allocator::default_allocator;
    use paro_common::types::LogicalType;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug)]
    struct RecordingSearchObserver {
        calls: Arc<AtomicUsize>,
    }

    impl RowsetPublishObserver for RecordingSearchObserver {
        fn rowset_published(
            &self,
            _tablet_id: crate::tablet::TabletId,
            _version: i64,
            _rowset: RowsetSharedPtr,
            _search_updates: crate::tablet::SearchGenerationHeadUpdates,
        ) {
        }

        fn search_inline_builders_for_compaction(
            &self,
            _tablet_id: crate::tablet::TabletId,
        ) -> SearchInlineBuilderSet {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            SearchInlineBuilderSet::default()
        }
    }

    fn test_schema() -> TabletSchemaRef {
        Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::new(0, "v".to_string(), LogicalType::Integer)],
                KeysType::DuplicateKeys,
            )
            .expect("schema"),
        )
    }

    fn empty_plan(tablet_id: u64) -> CompactionPlan {
        CompactionPlan {
            plan_id: CompactionPlanId(1),
            tablet_id,
            policy_kind: PolicyKind::Cumulative,
            cumulative_point_action: CumulativePointAction::Preserve,
            execution_layout: ExecutionLayout::Horizontal,
            merge_semantics: MergeSemantics::Append,
            input_rowsets: Vec::new(),
            read_snapshot: ReadSnapshot {
                visible_version: 0,
                layout_epoch: 0,
                schema_epoch: None,
            },
            output_version: Version::singleton(0),
            output_rowset_id: 10,
            score: 1.0,
            reason: CompactionReason::CumulativePolicy,
            pk_delta_guard: None,
        }
    }

    #[test]
    fn horizontal_compaction_task_uses_search_inline_builder_observer() {
        let dir = tempfile::tempdir().unwrap();
        let tablet = Tablet::new(7, 100, 0, test_schema(), dir.path(), None).unwrap();
        tablet.init().unwrap();
        let tablet = Arc::new(tablet);
        let calls = Arc::new(AtomicUsize::new(0));
        let observer: Arc<dyn RowsetPublishObserver> = Arc::new(RecordingSearchObserver {
            calls: Arc::clone(&calls),
        });
        tablet.bind_rowset_publish_observer(Arc::downgrade(&observer));

        let mut task = HorizontalCompactionTask::new_with_job_id(
            Arc::clone(&tablet),
            empty_plan(tablet.tablet_id()),
            Arc::new(default_allocator()),
            CompactionJobId(99),
        );

        task.run().unwrap();

        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(task.state(), CompactionTaskState::Success);
    }
}
