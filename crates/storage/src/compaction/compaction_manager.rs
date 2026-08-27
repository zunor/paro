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
use paro_common::error::{self as paro_error, codes, Result};
use paro_scheduler::scheduler::TaskScheduler;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const TABLET_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_TABLET_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Admission policy for background compaction while foreground statements are
/// active. Keeping the policy as data lets the instance resource governor tune
/// the read/write tradeoff without changing scheduler code.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionAdmissionPolicy {
    pub foreground_quiescence: Duration,
    /// Critical debt may shorten the bounded deferral interval, but it must
    /// not immediately steal workers from a short foreground burst.
    pub foreground_critical_deferral: Duration,
    pub foreground_max_deferral: Duration,
    /// A broad backlog needs continuous relief rather than one job per maximum
    /// deferral interval.
    pub critical_candidate_count: usize,
    /// A single tablet with a deep rowset stack must also trigger relief.
    pub critical_candidate_score: f64,
    /// Maximum jobs admitted per scheduling round while foreground work is
    /// active and compaction debt is critical.
    pub critical_relief_jobs: usize,
}

impl Default for CompactionAdmissionPolicy {
    fn default() -> Self {
        Self {
            foreground_quiescence: Duration::from_secs(2),
            // Critical debt halves the ordinary starvation bound while still
            // reserving a meaningful foreground-only interval for analytical
            // bursts and interactive workloads.
            foreground_critical_deferral: Duration::from_secs(60),
            foreground_max_deferral: Duration::from_secs(120),
            critical_candidate_count: 8,
            critical_candidate_score: 32.0,
            critical_relief_jobs: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct CompactionDebt {
    candidate_count: usize,
    max_candidate_score: f64,
}

impl CompactionDebt {
    fn is_critical(self, policy: CompactionAdmissionPolicy) -> bool {
        self.candidate_count >= policy.critical_candidate_count.max(1)
            || self.max_candidate_score >= policy.critical_candidate_score.max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceAdmission {
    Normal,
    StarvationRelief,
    DebtRelief,
    Deferred,
}

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
    pub max_queued_candidate_score: f64,
    pub running_tablets: Vec<TabletId>,
    pub failed_tablets: Vec<(TabletId, String)>,
    pub quarantined_tablets: Vec<TabletId>,
    pub jobs: Vec<CompactionJobObservability>,
    pub suspended: bool,
    pub foreground_statements: usize,
    pub foreground_deferred: bool,
}

pub struct CompactionManager {
    tablets: Arc<Mutex<HashMap<TabletId, Arc<Tablet>>>>,
    // Accepted compaction work for a tablet, including executor-queued tasks.
    // Registry/lifecycle drains must keep treating queued work as active.
    running_tablets: Arc<Mutex<HashSet<TabletId>>>,
    draining_tablets: Arc<Mutex<HashSet<TabletId>>>,
    failed_tablets: Arc<Mutex<HashMap<TabletId, String>>>,
    /// Tablets with a non-retryable version-graph failure. They remain
    /// registered and readable but are excluded from background compaction
    /// until an operator explicitly clears the quarantine after repair.
    quarantined_tablets: Arc<Mutex<HashSet<TabletId>>>,
    jobs: Arc<Mutex<HashMap<TabletId, CompactionJobObservability>>>,
    cancellation_tokens: Arc<Mutex<HashMap<TabletId, CancellationToken>>>,
    suspension_count: AtomicUsize,
    foreground_clock: Instant,
    foreground_statements: AtomicUsize,
    foreground_last_activity_ns: AtomicU64,
    foreground_deferral_since_ns: AtomicU64,
    candidate_queue_len: AtomicUsize,
    candidate_max_score_bits: AtomicU64,
    admission_policy: CompactionAdmissionPolicy,
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
        Self::new_with_allocator_and_admission_policy(
            max_concurrency,
            allocator,
            CompactionAdmissionPolicy::default(),
        )
    }

    pub fn new_with_allocator_and_admission_policy(
        max_concurrency: usize,
        allocator: Arc<dyn Allocator>,
        admission_policy: CompactionAdmissionPolicy,
    ) -> Self {
        let (stop_tx, _) = broadcast::channel(1);
        Self {
            tablets: Arc::new(Mutex::new(HashMap::new())),
            running_tablets: Arc::new(Mutex::new(HashSet::new())),
            draining_tablets: Arc::new(Mutex::new(HashSet::new())),
            failed_tablets: Arc::new(Mutex::new(HashMap::new())),
            quarantined_tablets: Arc::new(Mutex::new(HashSet::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            suspension_count: AtomicUsize::new(0),
            foreground_clock: Instant::now(),
            foreground_statements: AtomicUsize::new(0),
            foreground_last_activity_ns: AtomicU64::new(0),
            foreground_deferral_since_ns: AtomicU64::new(0),
            candidate_queue_len: AtomicUsize::new(0),
            candidate_max_score_bits: AtomicU64::new(0.0f64.to_bits()),
            admission_policy,
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
        Self::new_with_allocator_scheduler_and_admission_policy(
            max_concurrency,
            allocator,
            scheduler,
            CompactionAdmissionPolicy::default(),
        )
    }

    pub fn new_with_allocator_scheduler_and_admission_policy(
        max_concurrency: usize,
        allocator: Arc<dyn Allocator>,
        scheduler: Arc<TaskScheduler>,
        admission_policy: CompactionAdmissionPolicy,
    ) -> Self {
        let (stop_tx, _) = broadcast::channel(1);
        Self {
            tablets: Arc::new(Mutex::new(HashMap::new())),
            running_tablets: Arc::new(Mutex::new(HashSet::new())),
            draining_tablets: Arc::new(Mutex::new(HashSet::new())),
            failed_tablets: Arc::new(Mutex::new(HashMap::new())),
            quarantined_tablets: Arc::new(Mutex::new(HashSet::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            suspension_count: AtomicUsize::new(0),
            foreground_clock: Instant::now(),
            foreground_statements: AtomicUsize::new(0),
            foreground_last_activity_ns: AtomicU64::new(0),
            foreground_deferral_since_ns: AtomicU64::new(0),
            candidate_queue_len: AtomicUsize::new(0),
            candidate_max_score_bits: AtomicU64::new(0.0f64.to_bits()),
            admission_policy,
            executor: Arc::new(CompactionExecutor::new_with_scheduler(
                max_concurrency,
                scheduler,
            )),
            compaction_allocator: allocator,
            stop_tx,
        }
    }

    pub fn new_with_buffer_pool(max_concurrency: usize, buffer_pool: Arc<BufferPool>) -> Self {
        Self::new_with_buffer_pool_and_admission_policy(
            max_concurrency,
            buffer_pool,
            CompactionAdmissionPolicy::default(),
        )
    }

    pub fn new_with_buffer_pool_and_admission_policy(
        max_concurrency: usize,
        buffer_pool: Arc<BufferPool>,
        admission_policy: CompactionAdmissionPolicy,
    ) -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
            buffer_pool as Arc<dyn CommonBufferManager>,
            MemoryTag::Compaction,
        ));
        Self::new_with_allocator_and_admission_policy(max_concurrency, allocator, admission_policy)
    }

    pub fn new_with_buffer_pool_and_scheduler(
        max_concurrency: usize,
        buffer_pool: Arc<BufferPool>,
        scheduler: Arc<TaskScheduler>,
    ) -> Self {
        Self::new_with_buffer_pool_scheduler_and_admission_policy(
            max_concurrency,
            buffer_pool,
            scheduler,
            CompactionAdmissionPolicy::default(),
        )
    }

    pub fn new_with_buffer_pool_scheduler_and_admission_policy(
        max_concurrency: usize,
        buffer_pool: Arc<BufferPool>,
        scheduler: Arc<TaskScheduler>,
        admission_policy: CompactionAdmissionPolicy,
    ) -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
            buffer_pool as Arc<dyn CommonBufferManager>,
            MemoryTag::Compaction,
        ));
        Self::new_with_allocator_scheduler_and_admission_policy(
            max_concurrency,
            allocator,
            scheduler,
            admission_policy,
        )
    }

    pub fn register_tablet(&self, tablet: Arc<Tablet>) {
        let tablet_id = tablet.tablet_id();
        let mut tablets = self.tablets.lock().unwrap();
        tablets.insert(tablet_id, tablet);
        drop(tablets);
        self.draining_tablets.lock().unwrap().remove(&tablet_id);
        self.clear_tablet_quarantine(tablet_id);
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
        self.clear_tablet_quarantine(tablet_id);
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
            info!(
                stale_skip_count = stale_ids.len(),
                "CompactionManager: dropping stale tablet registrations during sync"
            );
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
                self.clear_tablet_quarantine(*tablet_id);
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
        if self.suspension_count.fetch_add(1, AtomicOrdering::AcqRel) == 0 {
            info!(reason = reason, "CompactionManager: scheduling suspended");
        }
    }

    pub fn resume(&self, reason: &str) {
        let previous = self
            .suspension_count
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |count| {
                count.checked_sub(1)
            })
            .unwrap_or(0);
        if previous == 1 {
            info!(reason = reason, "CompactionManager: scheduling resumed");
        }
    }

    pub fn is_suspended(&self) -> bool {
        self.suspension_count.load(AtomicOrdering::Acquire) > 0
    }

    /// Record a foreground statement without canceling already accepted work.
    /// The hot path is atomic-only; admission observes this lease and waits for
    /// a bounded foreground-quiescence interval before submitting new jobs.
    pub fn begin_foreground_statement(&self) {
        let _ = self.foreground_statements.fetch_update(
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
            |count| count.checked_add(1),
        );
        let now = self.foreground_now_ns();
        self.foreground_last_activity_ns
            .store(now, AtomicOrdering::Release);
        let _ = self.foreground_deferral_since_ns.compare_exchange(
            0,
            now,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        );
    }

    pub fn finish_foreground_statement(&self) {
        let _ = self.foreground_statements.fetch_update(
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
            |count| count.checked_sub(1),
        );
        self.foreground_last_activity_ns
            .store(self.foreground_now_ns(), AtomicOrdering::Release);
    }

    fn foreground_now_ns(&self) -> u64 {
        self.foreground_clock
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX - 1)) as u64
            + 1
    }

    fn foreground_blocks_new_work_at(&self, now_ns: u64) -> bool {
        if self.foreground_statements.load(AtomicOrdering::Acquire) > 0 {
            return true;
        }
        let last_activity = self
            .foreground_last_activity_ns
            .load(AtomicOrdering::Acquire);
        last_activity != 0
            && now_ns.saturating_sub(last_activity)
                < duration_ns(self.admission_policy.foreground_quiescence)
    }

    fn foreground_blocks_new_work(&self) -> bool {
        self.foreground_blocks_new_work_at(self.foreground_now_ns())
    }

    fn publish_candidate_debt(&self, debt: CompactionDebt) {
        self.candidate_queue_len
            .store(debt.candidate_count, AtomicOrdering::Release);
        self.candidate_max_score_bits
            .store(debt.max_candidate_score.to_bits(), AtomicOrdering::Release);
    }

    fn observed_candidate_debt(&self) -> CompactionDebt {
        CompactionDebt {
            candidate_count: self.candidate_queue_len.load(AtomicOrdering::Acquire),
            max_candidate_score: f64::from_bits(
                self.candidate_max_score_bits.load(AtomicOrdering::Acquire),
            ),
        }
    }

    fn maintenance_admission(&self, debt: CompactionDebt) -> MaintenanceAdmission {
        self.maintenance_admission_at(self.foreground_now_ns(), debt)
    }

    fn maintenance_admission_at(&self, now: u64, debt: CompactionDebt) -> MaintenanceAdmission {
        if !self.foreground_blocks_new_work_at(now) {
            self.foreground_deferral_since_ns
                .store(0, AtomicOrdering::Release);
            return MaintenanceAdmission::Normal;
        }

        let mut deferred_since = self
            .foreground_deferral_since_ns
            .load(AtomicOrdering::Acquire);
        if deferred_since == 0 {
            match self.foreground_deferral_since_ns.compare_exchange(
                0,
                now,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => deferred_since = now,
                Err(observed) => deferred_since = observed,
            }
        }

        let elapsed = now.saturating_sub(deferred_since);
        if debt.is_critical(self.admission_policy)
            && elapsed >= duration_ns(self.admission_policy.foreground_critical_deferral)
        {
            return MaintenanceAdmission::DebtRelief;
        }

        if elapsed >= duration_ns(self.admission_policy.foreground_max_deferral) {
            // Admit one job, then begin a fresh bounded deferral interval. This
            // prevents starvation without compaction cancel/restart livelock.
            self.foreground_deferral_since_ns
                .store(now, AtomicOrdering::Release);
            MaintenanceAdmission::StarvationRelief
        } else {
            MaintenanceAdmission::Deferred
        }
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
        let had_token = token.is_some();
        if let Some(token) = token {
            token.cancel();
        }

        let cancelled_pending = self.executor.cancel_pending_tablet(tablet_id, reason);
        if had_token || cancelled_pending > 0 {
            info!(
                tablet_id,
                reason = reason,
                cancelled_pending,
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
            if self.executor.drive_scheduler_for_drain(1) == 0 {
                std::thread::sleep(TABLET_DRAIN_POLL_INTERVAL);
            }
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
            self.publish_candidate_debt(CompactionDebt::default());
            self.refresh_metrics();
            return;
        }
        let tablets_list: Vec<Arc<Tablet>> = {
            let tablets = self.tablets.lock().unwrap();
            tablets.values().cloned().collect()
        };

        if tablets_list.is_empty() {
            self.publish_candidate_debt(CompactionDebt::default());
            self.refresh_metrics();
            return;
        }

        let mut candidates = BinaryHeap::new();
        {
            let running = self.running_tablets.lock().unwrap();
            let draining = self.draining_tablets.lock().unwrap();
            let quarantined = self.quarantined_tablets.lock().unwrap().clone();
            for tablet in tablets_list {
                if running.contains(&tablet.tablet_id())
                    || draining.contains(&tablet.tablet_id())
                    || quarantined.contains(&tablet.tablet_id())
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
                        let reason = format!("plan failed: {err}");
                        if err.is(codes::internal::DATA_CORRUPTED) {
                            self.quarantine_failure(tablet.tablet_id(), reason);
                            error!(
                                tablet_id = tablet.tablet_id(),
                                error = %err,
                                "Compaction planning found a non-retryable version-graph failure; tablet quarantined from compaction"
                            );
                        } else {
                            self.record_failure(tablet.tablet_id(), reason);
                            error!(
                                "Failed to plan compaction for tablet {}: {}",
                                tablet.tablet_id(),
                                err
                            );
                        }
                    }
                }
            }
        }

        let debt = CompactionDebt {
            candidate_count: candidates.len(),
            max_candidate_score: candidates
                .iter()
                .map(|candidate| candidate.plan.score)
                .filter(|score| score.is_finite())
                .fold(0.0, f64::max),
        };
        self.publish_candidate_debt(debt);
        self.refresh_metrics();

        let admission = self.maintenance_admission(debt);
        let admission_budget = match admission {
            MaintenanceAdmission::Normal => usize::MAX,
            MaintenanceAdmission::StarvationRelief => 1,
            MaintenanceAdmission::DebtRelief => self.admission_policy.critical_relief_jobs.max(1),
            MaintenanceAdmission::Deferred => return,
        };

        let mut submitted = 0usize;
        while let Some(candidate) = candidates.pop() {
            if self.is_suspended() {
                candidates.push(candidate);
                break;
            }
            if admission == MaintenanceAdmission::Normal && self.foreground_blocks_new_work() {
                candidates.push(candidate);
                break;
            }

            let mut running = self.running_tablets.lock().unwrap();
            if running.contains(&candidate.tablet_id) {
                continue;
            }
            running.insert(candidate.tablet_id);
            drop(running);

            self.publish_candidate_debt(CompactionDebt {
                candidate_count: candidates.len(),
                max_candidate_score: candidates
                    .iter()
                    .map(|candidate| candidate.plan.score)
                    .filter(|score| score.is_finite())
                    .fold(0.0, f64::max),
            });
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
            submitted += 1;
            if submitted >= admission_budget {
                break;
            }
        }

        let remaining_debt = CompactionDebt {
            candidate_count: candidates.len(),
            max_candidate_score: candidates
                .iter()
                .map(|candidate| candidate.plan.score)
                .filter(|score| score.is_finite())
                .fold(0.0, f64::max),
        };
        self.publish_candidate_debt(remaining_debt);
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

        let mut quarantined_tablets: Vec<TabletId> = self
            .quarantined_tablets
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect();
        quarantined_tablets.sort_unstable();

        let mut jobs: Vec<CompactionJobObservability> =
            self.jobs.lock().unwrap().values().cloned().collect();
        jobs.sort_by_key(|job| (job.tablet_id, job.job_id));

        let debt = self.observed_candidate_debt();
        CompactionObservability {
            registered_tablets: self.tablets.lock().unwrap().len(),
            queued_candidates: debt.candidate_count,
            max_queued_candidate_score: debt.max_candidate_score,
            running_tablets,
            failed_tablets,
            quarantined_tablets,
            jobs,
            suspended: self.is_suspended(),
            foreground_statements: self.foreground_statements.load(AtomicOrdering::Acquire),
            foreground_deferred: debt.candidate_count > 0
                && self.foreground_blocks_new_work()
                && !debt.is_critical(self.admission_policy),
        }
    }

    fn record_failure(&self, tablet_id: TabletId, reason: String) {
        self.failed_tablets
            .lock()
            .unwrap()
            .insert(tablet_id, reason);
    }

    fn quarantine_failure(&self, tablet_id: TabletId, reason: String) {
        self.record_failure(tablet_id, reason);
        self.quarantined_tablets.lock().unwrap().insert(tablet_id);
    }

    /// Re-admit a repaired tablet to compaction. Registration also clears a
    /// prior quarantine because it establishes a new tablet runtime identity.
    pub fn clear_tablet_quarantine(&self, tablet_id: TabletId) {
        self.quarantined_tablets.lock().unwrap().remove(&tablet_id);
        self.clear_failure(tablet_id);
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

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn policy_priority(kind: PolicyKind) -> u8 {
    match kind {
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
    use crate::compaction::compaction_task::{CompactionTask, CompactionTaskState};
    use crate::compaction::plan::types::{
        CompactionPlanId, CompactionReason, CumulativePointAction, ExecutionLayout, MergeSemantics,
        ReadSnapshot,
    };
    use crate::rowset::{Rowset, RowsetMeta};
    use crate::tablet::Version;
    use crate::tablet::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use paro_scheduler::scheduler::TaskScheduler;
    use std::sync::atomic::{AtomicBool, Ordering as StdAtomicOrdering};

    fn test_plan(tablet_id: TabletId) -> CompactionPlan {
        CompactionPlan {
            plan_id: CompactionPlanId(tablet_id),
            tablet_id,
            policy_kind: PolicyKind::Cumulative,
            cumulative_point_action: CumulativePointAction::AdvanceToOutputEndExclusive,
            execution_layout: ExecutionLayout::Horizontal,
            merge_semantics: MergeSemantics::Deduplicate,
            input_rowsets: Vec::new(),
            read_snapshot: ReadSnapshot {
                visible_version: 0,
                layout_epoch: 0,
                schema_epoch: None,
            },
            output_version: Version::singleton(0),
            output_rowset_id: tablet_id + 10_000,
            score: 1.0,
            reason: CompactionReason::CumulativePolicy,
            pk_delta_guard: None,
        }
    }

    struct CancelAwareTask {
        state: CompactionTaskState,
        plan: CompactionPlan,
        cancel_token: CancellationToken,
        started: Arc<AtomicBool>,
    }

    impl CancelAwareTask {
        fn new(
            tablet_id: TabletId,
            cancel_token: CancellationToken,
            started: Arc<AtomicBool>,
        ) -> Self {
            Self {
                state: CompactionTaskState::Init,
                plan: test_plan(tablet_id),
                cancel_token,
                started,
            }
        }
    }

    impl CompactionTask for CancelAwareTask {
        fn run(&mut self) -> Result<()> {
            self.state = CompactionTaskState::Running;
            self.started.store(true, StdAtomicOrdering::SeqCst);
            if self.cancel_token.is_cancelled() {
                self.state = CompactionTaskState::Failed;
                return Err(paro_error::query_canceled());
            }
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

    fn submit_test_task(
        manager: &CompactionManager,
        tablet: Arc<Tablet>,
        token: CancellationToken,
        started: Arc<AtomicBool>,
    ) {
        let tablet_id = tablet.tablet_id();
        manager.running_tablets.lock().unwrap().insert(tablet_id);
        manager
            .cancellation_tokens
            .lock()
            .unwrap()
            .insert(tablet_id, token.clone());

        let running_tablets = manager.running_tablets.clone();
        let failed_tablets = manager.failed_tablets.clone();
        let jobs = manager.jobs.clone();
        let cancellation_tokens = manager.cancellation_tokens.clone();
        let task = Box::new(CancelAwareTask::new(tablet_id, token, started));
        manager
            .executor
            .submit_with_callback(tablet, task, move |result| {
                running_tablets.lock().unwrap().remove(&tablet_id);
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
            });
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
    async fn data_corrupted_plan_quarantines_tablet_until_explicit_repair() {
        let dir = tempfile::tempdir().unwrap();
        let manager = CompactionManager::new(1);
        let tablet = create_test_tablet(7, dir.path());
        let rowset = Rowset::create(
            tablet.schema().expect("test tablet schema"),
            RowsetMeta::new(1, tablet.tablet_id(), Version::new(0, 2)),
            tablet.data_dir().join("crossing-rowset"),
        )
        .unwrap();
        tablet.add_rowset(Arc::new(rowset)).unwrap();
        tablet.set_cumulative_point(1);
        manager.register_tablet(Arc::clone(&tablet));

        manager.schedule().await;
        let observation = manager.observability();
        assert_eq!(observation.quarantined_tablets, vec![7]);
        assert!(observation.failed_tablets[0]
            .1
            .contains("crosses the cumulative point"));

        // A subsequent scheduler round skips the tablet instead of emitting
        // the same permanent planning failure again.
        let failure = observation.failed_tablets[0].1.clone();
        manager.schedule().await;
        assert_eq!(manager.observability().failed_tablets[0].1, failure);

        manager.clear_tablet_quarantine(7);
        assert!(manager.observability().quarantined_tablets.is_empty());
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
        manager.suspend("concurrent test");
        manager.resume("test");
        assert!(manager.is_suspended());
        manager.resume("concurrent test");
        assert!(!manager.is_suspended());
        // Extra resumes are harmless and cannot underflow the lease count.
        manager.resume("test");
        assert!(!manager.is_suspended());
    }

    #[test]
    fn foreground_admission_defers_without_canceling_and_has_starvation_relief() {
        let manager = CompactionManager::new(1);
        let no_debt = CompactionDebt::default();

        manager.begin_foreground_statement();
        assert_eq!(
            manager.maintenance_admission(no_debt),
            MaintenanceAdmission::Deferred
        );
        assert_eq!(
            manager.foreground_statements.load(AtomicOrdering::Acquire),
            1
        );

        let overdue = 1;
        manager
            .foreground_deferral_since_ns
            .store(overdue, AtomicOrdering::Release);
        assert_eq!(
            manager.maintenance_admission_at(
                duration_ns(manager.admission_policy.foreground_max_deferral) + 2,
                no_debt,
            ),
            MaintenanceAdmission::StarvationRelief
        );
        assert_eq!(
            manager.maintenance_admission(no_debt),
            MaintenanceAdmission::Deferred
        );

        manager.finish_foreground_statement();
        assert_eq!(
            manager.foreground_statements.load(AtomicOrdering::Acquire),
            0
        );
        manager
            .foreground_last_activity_ns
            .store(1, AtomicOrdering::Release);
        assert_eq!(
            manager.maintenance_admission_at(
                duration_ns(manager.admission_policy.foreground_quiescence) + 2,
                no_debt,
            ),
            MaintenanceAdmission::Normal
        );
    }

    #[test]
    fn critical_compaction_debt_shortens_but_does_not_remove_foreground_deferral() {
        let manager = CompactionManager::new_with_allocator_and_admission_policy(
            1,
            Arc::new(default_allocator()),
            CompactionAdmissionPolicy {
                critical_candidate_count: 3,
                critical_candidate_score: 20.0,
                critical_relief_jobs: 1,
                ..CompactionAdmissionPolicy::default()
            },
        );
        manager.begin_foreground_statement();

        assert_eq!(
            manager.maintenance_admission(CompactionDebt {
                candidate_count: 3,
                max_candidate_score: 1.0,
            }),
            MaintenanceAdmission::Deferred
        );
        manager
            .foreground_deferral_since_ns
            .store(1, AtomicOrdering::Release);
        let critical_deadline =
            duration_ns(manager.admission_policy.foreground_critical_deferral) + 2;

        assert_eq!(
            manager.maintenance_admission_at(
                critical_deadline,
                CompactionDebt {
                    candidate_count: 3,
                    max_candidate_score: 1.0,
                },
            ),
            MaintenanceAdmission::DebtRelief
        );
        assert_eq!(
            manager.maintenance_admission_at(
                critical_deadline,
                CompactionDebt {
                    candidate_count: 1,
                    max_candidate_score: 20.0,
                },
            ),
            MaintenanceAdmission::DebtRelief
        );
        assert_eq!(
            manager.maintenance_admission_at(
                critical_deadline,
                CompactionDebt {
                    candidate_count: 1,
                    max_candidate_score: 19.0,
                },
            ),
            MaintenanceAdmission::Deferred
        );
    }

    #[test]
    fn deferred_observability_preserves_compaction_debt() {
        let manager = CompactionManager::new(1);
        manager.begin_foreground_statement();
        manager
            .candidate_queue_len
            .store(2, AtomicOrdering::Release);
        manager
            .candidate_max_score_bits
            .store(7.5f64.to_bits(), AtomicOrdering::Release);

        let observation = manager.observability();
        assert_eq!(observation.queued_candidates, 2);
        assert_eq!(observation.max_queued_candidate_score, 7.5);
        assert!(observation.foreground_deferred);
    }

    #[test]
    fn drain_tablet_drives_scheduler_queued_cancelled_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Arc::new(TaskScheduler::new());
        let manager = CompactionManager::new_with_scheduler(1, scheduler);
        let tablet = create_test_tablet(1, dir.path());
        let token = CancellationToken::new();
        let started = Arc::new(AtomicBool::new(false));

        submit_test_task(&manager, tablet, token, started.clone());

        manager
            .drain_tablet(1, "tablet registry sync", Duration::from_secs(1))
            .unwrap();

        assert!(!manager.running_tablets.lock().unwrap().contains(&1));
        assert!(manager
            .cancellation_tokens
            .lock()
            .unwrap()
            .get(&1)
            .is_none());
        assert!(started.load(StdAtomicOrdering::SeqCst));
        assert!(manager.failed_tablets.lock().unwrap()[&1].contains("canceling statement"));
    }

    #[test]
    fn drain_tablet_cancels_executor_pending_compaction_without_running_queue_head() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Arc::new(TaskScheduler::new());
        let manager = CompactionManager::new_with_scheduler(1, scheduler);
        let queued_tablet = create_test_tablet(10, dir.path());
        let pending_tablet = create_test_tablet(11, dir.path());
        let queued_started = Arc::new(AtomicBool::new(false));
        let pending_started = Arc::new(AtomicBool::new(false));

        submit_test_task(
            &manager,
            queued_tablet,
            CancellationToken::new(),
            queued_started.clone(),
        );
        submit_test_task(
            &manager,
            pending_tablet,
            CancellationToken::new(),
            pending_started.clone(),
        );

        manager
            .drain_tablet(11, "tablet registry sync", Duration::from_secs(1))
            .unwrap();

        assert!(!manager.running_tablets.lock().unwrap().contains(&11));
        assert!(!queued_started.load(StdAtomicOrdering::SeqCst));
        assert!(!pending_started.load(StdAtomicOrdering::SeqCst));
        assert!(manager.failed_tablets.lock().unwrap()[&11]
            .contains("compaction canceled before execution"));
    }
}
