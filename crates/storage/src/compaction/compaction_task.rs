// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::job_orchestrator::run_job_with_lifecycle;
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
        run_job_with_lifecycle(
            &self.tablet,
            Arc::new(self.plan.clone()),
            self.job_id,
            self.allocator.clone(),
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
