// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::BufferPool;
use crate::compaction::compaction_executor::CompactionExecutor;
use crate::compaction::compaction_task::{
    CompactionTask, HorizontalCompactionTask, VerticalCompactionTask,
};
use crate::compaction::plan::planner::CompactionPlanner;
use crate::compaction::plan::types::{
    CompactionJobId, CompactionLifecycleState, CompactionPlan, PolicyKind,
};
use crate::metrics::storage_metrics;
use crate::tablet::{Tablet, TabletId, TabletState};
use paro_common::allocator::{
    default_allocator, Allocator, BufferAllocator, BufferManager as CommonBufferManager, MemoryTag,
};
use paro_common::error::{self as paro_error, Result};
use paro_scheduler::scheduler::TaskScheduler;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const TABLET_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_TABLET_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct CompactionCandidate {
    tablet_id: TabletId,
    tablet: Arc<Tablet>,
    plan: CompactionPlan,
}

impl PartialEq for CompactionCandidate {
    fn eq(&self, other: &Self) -> bool {
        (self.plan.score - other.plan.score).abs() < f64::EPSILON
            && self.tablet_id == other.tablet_id
    }
}

impl Eq for CompactionCandidate {}

impl Ord for CompactionCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        match self
            .plan
            .score
            .partial_cmp(&other.plan.score)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Equal => {
                policy_priority(self.plan.policy_kind).cmp(&policy_priority(other.plan.policy_kind))
            }
            ord => ord,
        }
    }
}

impl PartialOrd for CompactionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactionSyncStats {
    pub registered: usize,
    pub unregistered: usize,
    pub total_registered: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionJobObservability {
    pub tablet_id: TabletId,
    pub plan_id: u64,
    pub job_id: u64,
    pub policy_kind: PolicyKind,
    pub lifecycle_state: CompactionLifecycleState,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CompactionObservability {
    pub registered_tablets: usize,
    pub queued_candidates: usize,
    pub running_tablets: Vec<TabletId>,
    pub failed_tablets: Vec<(TabletId, String)>,
    pub jobs: Vec<CompactionJobObservability>,
    pub suspended: bool,
}

pub struct CompactionManager {
    tablets: Arc<Mutex<HashMap<TabletId, Arc<Tablet>>>>,
    running_tablets: Arc<Mutex<HashSet<TabletId>>>,
    draining_tablets: Arc<Mutex<HashSet<TabletId>>>,
    failed_tablets: Arc<Mutex<HashMap<TabletId, String>>>,
    jobs: Arc<Mutex<HashMap<TabletId, CompactionJobObservability>>>,
    cancellation_tokens: Arc<Mutex<HashMap<TabletId, CancellationToken>>>,
    suspended: AtomicBool,
    candidate_queue_len: AtomicUsize,
    executor: Arc<CompactionExecutor>,
    compaction_allocator: Arc<dyn Allocator>,
    stop_tx: broadcast::Sender<()>,
}

impl CompactionManager {
    pub fn new(max_concurrency: usize) -> Self {
        Self::new_with_allocator(max_concurrency, Arc::new(default_allocator()))
    }

    pub fn new_with_scheduler(max_concurrency: usize, scheduler: Arc<TaskScheduler>) -> Self {
        Self::new_with_allocator_and_scheduler(
            max_concurrency,
            Arc::new(default_allocator()),
            scheduler,
        )
    }

    pub fn new_with_allocator(max_concurrency: usize, allocator: Arc<dyn Allocator>) -> Self {
        let (stop_tx, _) = broadcast::channel(1);
        Self {
            tablets: Arc::new(Mutex::new(HashMap::new())),
            running_tablets: Arc::new(Mutex::new(HashSet::new())),
            draining_tablets: Arc::new(Mutex::new(HashSet::new())),
            failed_tablets: Arc::new(Mutex::new(HashMap::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            suspended: AtomicBool::new(false),
            candidate_queue_len: AtomicUsize::new(0),
            executor: Arc::new(CompactionExecutor::new(max_concurrency)),
            compaction_allocator: allocator,
            stop_tx,
        }
    }

    pub fn new_with_allocator_and_scheduler(
        max_concurrency: usize,
        allocator: Arc<dyn Allocator>,
        scheduler: Arc<TaskScheduler>,
    ) -> Self {
        let (stop_tx, _) = broadcast::channel(1);
        Self {
            tablets: Arc::new(Mutex::new(HashMap::new())),
            running_tablets: Arc::new(Mutex::new(HashSet::new())),
            draining_tablets: Arc::new(Mutex::new(HashSet::new())),
            failed_tablets: Arc::new(Mutex::new(HashMap::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            suspended: AtomicBool::new(false),
            candidate_queue_len: AtomicUsize::new(0),
            executor: Arc::new(CompactionExecutor::new_with_scheduler(
                max_concurrency,
                scheduler,
            )),
            compaction_allocator: allocator,
            stop_tx,
        }
    }

    pub fn new_with_buffer_pool(max_concurrency: usize, buffer_pool: Arc<BufferPool>) -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
            buffer_pool as Arc<dyn CommonBufferManager>,
            MemoryTag::Compaction,
        ));
        Self::new_with_allocator(max_concurrency, allocator)
    }

    pub fn new_with_buffer_pool_and_scheduler(
        max_concurrency: usize,
        buffer_pool: Arc<BufferPool>,
        scheduler: Arc<TaskScheduler>,
    ) -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
            buffer_pool as Arc<dyn CommonBufferManager>,
            MemoryTag::Compaction,
        ));
        Self::new_with_allocator_and_scheduler(max_concurrency, allocator, scheduler)
    }

    pub fn register_tablet(&self, tablet: Arc<Tablet>) {
        let tablet_id = tablet.tablet_id();
        let mut tablets = self.tablets.lock().unwrap();
        tablets.insert(tablet_id, tablet);
        drop(tablets);
        self.draining_tablets.lock().unwrap().remove(&tablet_id);
        self.refresh_metrics();
    }

    pub fn unregister_tablet(&self, tablet_id: TabletId) -> Result<()> {
        self.drain_tablet(tablet_id, "unregister tablet", DEFAULT_TABLET_DRAIN_TIMEOUT)?;
        let mut tablets = self.tablets.lock().unwrap();
        tablets.remove(&tablet_id);
        drop(tablets);
        self.draining_tablets.lock().unwrap().remove(&tablet_id);
        self.cancellation_tokens.lock().unwrap().remove(&tablet_id);
        self.jobs.lock().unwrap().remove(&tablet_id);
        self.clear_failure(tablet_id);
        self.refresh_metrics();
        Ok(())
    }

    pub fn sync_tablets(
        &self,
        desired: HashMap<TabletId, Arc<Tablet>>,
    ) -> Result<CompactionSyncStats> {
        let desired_ids: HashSet<TabletId> = desired.keys().copied().collect();

        let stale_ids: Vec<TabletId> = {
            let tablets = self.tablets.lock().unwrap();
            tablets
                .keys()
                .filter(|id| !desired_ids.contains(id))
                .copied()
                .collect()
        };

        let mut registered = 0usize;
        let mut unregistered = 0usize;
        {
            let mut tablets = self.tablets.lock().unwrap();
            for (tablet_id, tablet) in desired {
                if tablets.insert(tablet_id, tablet).is_none() {
                    registered += 1;
                }
                self.draining_tablets.lock().unwrap().remove(&tablet_id);
            }
        }

        if !stale_ids.is_empty() {
            for tablet_id in &stale_ids {
                self.drain_tablet(
                    *tablet_id,
                    "tablet registry sync",
                    DEFAULT_TABLET_DRAIN_TIMEOUT,
                )?;
            }

            let mut tablets = self.tablets.lock().unwrap();
            let mut jobs = self.jobs.lock().unwrap();
            let mut draining = self.draining_tablets.lock().unwrap();
            let mut cancellation_tokens = self.cancellation_tokens.lock().unwrap();
            for tablet_id in &stale_ids {
                if tablets.remove(tablet_id).is_some() {
                    unregistered += 1;
                }
                jobs.remove(tablet_id);
                draining.remove(tablet_id);
                cancellation_tokens.remove(tablet_id);
                self.clear_failure(*tablet_id);
            }
        }

        self.refresh_metrics();
        Ok(CompactionSyncStats {
            registered,
            unregistered,
            total_registered: self.tablets.lock().unwrap().len(),
        })
    }

    pub fn suspend(&self, reason: &str) {
        if !self.suspended.swap(true, AtomicOrdering::AcqRel) {
            info!(reason = reason, "CompactionManager: scheduling suspended");
        }
    }

    pub fn resume(&self, reason: &str) {
        if self.suspended.swap(false, AtomicOrdering::AcqRel) {
            info!(reason = reason, "CompactionManager: scheduling resumed");
        }
    }

    pub fn is_suspended(&self) -> bool {
        self.suspended.load(AtomicOrdering::Acquire)
    }

    pub fn wait_for_idle(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self.running_task_count() == 0 {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(TABLET_DRAIN_POLL_INTERVAL);
        }
    }

    pub fn cancel_tablet_jobs(&self, tablet_id: TabletId, reason: &str) {
        self.draining_tablets.lock().unwrap().insert(tablet_id);
        let token = self
            .cancellation_tokens
            .lock()
            .unwrap()
            .get(&tablet_id)
            .cloned();
        if let Some(token) = token {
            token.cancel();
            info!(
                tablet_id,
                reason = reason,
                "CompactionManager: cancellation requested for tablet compaction"
            );
        }
    }

    pub fn drain_tablet(&self, tablet_id: TabletId, reason: &str, timeout: Duration) -> Result<()> {
        self.cancel_tablet_jobs(tablet_id, reason);
        let start = std::time::Instant::now();
        loop {
            let running = self.running_tablets.lock().unwrap().contains(&tablet_id);
            if !running {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(paro_error::internal(format!(
                    "timed out draining compaction for tablet {} after {}s ({})",
                    tablet_id,
                    timeout.as_secs(),
                    reason
                )));
            }
            std::thread::sleep(TABLET_DRAIN_POLL_INTERVAL);
        }
    }

    pub fn start(self: Arc<Self>) {
        let manager = self.clone();
        let mut stop_rx = self.stop_tx.subscribe();

        let runner = async move {
            info!("starting compaction scheduler");
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        manager.schedule().await;
                    }
                    _ = stop_rx.recv() => {
                        info!("stopping compaction scheduler");
                        break;
                    }
                }
            }
        };

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(runner);
            }
            Err(_) => {
                warn!("background compaction scheduling disabled: no Tokio runtime");
            }
        }
    }

    pub fn stop(&self) {
        let _ = self.stop_tx.send(());
    }

    pub async fn schedule(&self) {
        debug!("starting compaction scheduling round");
        if self.is_suspended() {
            self.candidate_queue_len.store(0, AtomicOrdering::Release);
            self.refresh_metrics();
            return;
        }

        let tablets_list: Vec<Arc<Tablet>> = {
            let tablets = self.tablets.lock().unwrap();
            tablets.values().cloned().collect()
        };

        if tablets_list.is_empty() {
            self.candidate_queue_len.store(0, AtomicOrdering::Release);
            self.refresh_metrics();
            return;
        }

        let mut candidates = BinaryHeap::new();
        {
            let running = self.running_tablets.lock().unwrap();
            let draining = self.draining_tablets.lock().unwrap();
            for tablet in tablets_list {
                if running.contains(&tablet.tablet_id())
                    || draining.contains(&tablet.tablet_id())
                    || tablet.state() == TabletState::Shutdown
                {
                    continue;
                }

                match CompactionPlanner::plan(&tablet) {
                    Ok(Some(plan)) => {
                        candidates.push(CompactionCandidate {
                            tablet_id: tablet.tablet_id(),
                            tablet,
                            plan,
                        });
                    }
                    Ok(None) => {}
                    Err(err) => {
                        self.record_failure(tablet.tablet_id(), format!("plan failed: {}", err));
                        error!(
                            "Failed to plan compaction for tablet {}: {}",
                            tablet.tablet_id(),
                            err
                        );
                    }
                }
            }
        }

        self.candidate_queue_len
            .store(candidates.len(), AtomicOrdering::Release);
        self.refresh_metrics();

        while let Some(candidate) = candidates.pop() {
            if self.is_suspended() {
                break;
            }

            let mut running = self.running_tablets.lock().unwrap();
            if running.contains(&candidate.tablet_id) {
                continue;
            }
            running.insert(candidate.tablet_id);
            drop(running);

            self.candidate_queue_len
                .store(candidates.len(), AtomicOrdering::Release);
            self.refresh_metrics();

            let job_id = allocate_compaction_job_id();
            let plan = candidate.plan.clone();
            let tablet_id = candidate.tablet_id;
            let cancel_token = CancellationToken::new();
            info!(
                tablet_id,
                plan_id = plan.plan_id.0,
                job_id = job_id.0,
                policy = %plan.policy_kind,
                score = plan.score,
                "submitting compaction job"
            );

            self.jobs.lock().unwrap().insert(
                tablet_id,
                CompactionJobObservability {
                    tablet_id,
                    plan_id: plan.plan_id.0,
                    job_id: job_id.0,
                    policy_kind: plan.policy_kind,
                    lifecycle_state: CompactionLifecycleState::Planned,
                    score: plan.score,
                },
            );

            let lifecycle_jobs = self.jobs.clone();
            let notifier = Arc::new(move |state: CompactionLifecycleState| {
                if let Some(job) = lifecycle_jobs.lock().unwrap().get_mut(&tablet_id) {
                    job.lifecycle_state = state;
                }
            });

            let task_allocator = self.compaction_allocator.clone();
            let task: Box<dyn CompactionTask> = match plan.execution_layout {
                crate::compaction::plan::types::ExecutionLayout::Vertical => Box::new(
                    VerticalCompactionTask::new_with_job_id_and_cancel_token(
                        candidate.tablet.clone(),
                        plan,
                        task_allocator,
                        job_id,
                        cancel_token.clone(),
                    )
                    .with_lifecycle_notifier(notifier),
                ),
                crate::compaction::plan::types::ExecutionLayout::Horizontal => Box::new(
                    HorizontalCompactionTask::new_with_job_id_and_cancel_token(
                        candidate.tablet.clone(),
                        plan,
                        task_allocator,
                        job_id,
                        cancel_token.clone(),
                    )
                    .with_lifecycle_notifier(notifier),
                ),
            };

            self.cancellation_tokens
                .lock()
                .unwrap()
                .insert(tablet_id, cancel_token);

            let running_tablets = self.running_tablets.clone();
            let failed_tablets = self.failed_tablets.clone();
            let jobs = self.jobs.clone();
            let cancellation_tokens = self.cancellation_tokens.clone();
            self.executor
                .submit_with_callback(candidate.tablet.clone(), task, move |result| {
                    let mut running = running_tablets.lock().unwrap();
                    running.remove(&tablet_id);
                    drop(running);

                    jobs.lock().unwrap().remove(&tablet_id);
                    cancellation_tokens.lock().unwrap().remove(&tablet_id);

                    match result {
                        Ok(()) => {
                            failed_tablets.lock().unwrap().remove(&tablet_id);
                        }
                        Err(reason) => {
                            failed_tablets.lock().unwrap().insert(tablet_id, reason);
                        }
                    }

                    storage_metrics()
                        .set_compaction_running_tablets(running_tablets.lock().unwrap().len());
                    debug!(
                        "CompactionManager: removed tablet {} from running set",
                        tablet_id
                    );
                });
        }

        self.candidate_queue_len.store(0, AtomicOrdering::Release);
        self.refresh_metrics();
    }

    pub fn running_task_count(&self) -> usize {
        self.running_tablets.lock().unwrap().len()
    }

    pub fn observability(&self) -> CompactionObservability {
        let mut running_tablets: Vec<TabletId> = self
            .running_tablets
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect();
        running_tablets.sort_unstable();

        let mut failed_tablets: Vec<(TabletId, String)> = self
            .failed_tablets
            .lock()
            .unwrap()
            .iter()
            .map(|(tablet_id, reason)| (*tablet_id, reason.clone()))
            .collect();
        failed_tablets.sort_by_key(|(tablet_id, _)| *tablet_id);

        let mut jobs: Vec<CompactionJobObservability> =
            self.jobs.lock().unwrap().values().cloned().collect();
        jobs.sort_by_key(|job| (job.tablet_id, job.job_id));

        CompactionObservability {
            registered_tablets: self.tablets.lock().unwrap().len(),
            queued_candidates: self.candidate_queue_len.load(AtomicOrdering::Acquire),
            running_tablets,
            failed_tablets,
            jobs,
            suspended: self.is_suspended(),
        }
    }

    fn record_failure(&self, tablet_id: TabletId, reason: String) {
        self.failed_tablets
            .lock()
            .unwrap()
            .insert(tablet_id, reason);
    }

    fn clear_failure(&self, tablet_id: TabletId) {
        self.failed_tablets.lock().unwrap().remove(&tablet_id);
    }

    fn refresh_metrics(&self) {
        let metrics = storage_metrics();
        metrics.set_compaction_queue_len(self.candidate_queue_len.load(AtomicOrdering::Acquire));
        metrics.set_compaction_running_tablets(self.running_task_count());
    }
}

fn policy_priority(kind: PolicyKind) -> u8 {
    match kind {
        PolicyKind::PrimaryKeyFull => 4,
        PolicyKind::SizeTiered => 3,
        PolicyKind::Cumulative => 2,
        PolicyKind::Base => 1,
    }
}

pub(crate) fn allocate_compaction_job_id() -> CompactionJobId {
    static NEXT_JOB_ID: OnceLock<AtomicU64> = OnceLock::new();
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(1)
        .max(1);
    let counter = NEXT_JOB_ID.get_or_init(|| AtomicU64::new(seed));
    CompactionJobId(counter.fetch_add(1, AtomicOrdering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;

    fn create_test_tablet(id: TabletId, data_dir: &std::path::Path) -> Arc<Tablet> {
        let mut columns = Vec::new();
        columns.push(TabletColumn::new(0, "pk".to_string(), LogicalType::Integer));
        columns[0].is_key = true;
        columns.push(TabletColumn::new(1, "v".to_string(), LogicalType::Integer));

        let schema = Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap());
        let tablet = Tablet::new(id, 100, 0, schema, data_dir, None).unwrap();
        tablet.init().unwrap();
        Arc::new(tablet)
    }

    #[tokio::test]
    async fn test_compaction_manager_registration() {
        let dir = tempfile::tempdir().unwrap();
        let manager = CompactionManager::new(2);
        let tablet = create_test_tablet(1, dir.path());

        manager.register_tablet(tablet.clone());
        assert_eq!(manager.tablets.lock().unwrap().len(), 1);

        manager.unregister_tablet(1).unwrap();
        assert_eq!(manager.tablets.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_compaction_manager_schedule_empty() {
        let dir = tempfile::tempdir().unwrap();
        let manager = CompactionManager::new(2);
        let tablet = create_test_tablet(1, dir.path());

        manager.register_tablet(tablet.clone());
        manager.schedule().await;

        assert_eq!(manager.running_task_count(), 0);
    }

    #[tokio::test]
    async fn test_compaction_manager_sync_and_observability() {
        let dir = tempfile::tempdir().unwrap();
        let manager = CompactionManager::new(2);
        let tablet1 = create_test_tablet(1, dir.path());
        let tablet2 = create_test_tablet(2, dir.path());

        let mut desired = HashMap::new();
        desired.insert(1, tablet1);
        desired.insert(2, tablet2);

        let stats = manager.sync_tablets(desired).unwrap();
        assert_eq!(stats.registered, 2);
        assert_eq!(stats.unregistered, 0);
        assert_eq!(stats.total_registered, 2);

        let obs = manager.observability();
        assert_eq!(obs.registered_tablets, 2);
        assert!(obs.running_tablets.is_empty());
        assert!(obs.failed_tablets.is_empty());
        assert!(obs.jobs.is_empty());

        manager.suspend("test");
        assert!(manager.is_suspended());
        manager.resume("test");
        assert!(!manager.is_suspended());
    }
}
