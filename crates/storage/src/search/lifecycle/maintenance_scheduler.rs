// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Maintenance scheduling, admission, and cost estimation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use paro_common::error::Result;

use crate::search::artifact::{ArtifactGcContext, ArtifactLocation, GcDecision};
use crate::search::capability::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};
use crate::search::inline_sink::{
    AdmissionDecision, AdmissionGrant, AdmissionRejectReason, AdmissionWaitReason, CostEstimate,
    FlushSearchMode, InlineAdmissionRequest, MaintenanceBenefit, MaintenanceCost, SearchAdmission,
};
use crate::search::maintenance::SearchMaintenanceAction;
use crate::search::manifest::{
    LoadedManifest, DELTA_BYTES_HARD_LIMIT, DELTA_BYTES_SOFT_LIMIT, DELTA_COUNT_HARD_LIMIT,
    DELTA_COUNT_SOFT_LIMIT,
};
use crate::search::stats::{CatchUpBacklogTier, MaintenancePriority, SearchDefinitionId, TableId};
use crate::search::tail::TailMutationKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaintenanceFairnessKey {
    pub database_id: u64,
    pub table_id: TableId,
    pub provider: SearchIndexKind,
}

impl MaintenanceFairnessKey {
    pub const fn for_definition(definition: &SearchIndexDefinition) -> Self {
        Self {
            database_id: 0,
            table_id: definition.table_id,
            provider: definition.kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceAdmissionRequest {
    pub definition_id: SearchDefinitionId,
    pub action: SearchMaintenanceAction,
    pub fairness_key: MaintenanceFairnessKey,
    pub priority: MaintenancePriority,
    pub backlog_tier: CatchUpBacklogTier,
    pub estimate: CostEstimate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceAdmissionGrant {
    pub grant_id: u64,
    pub budget: MaintenanceCost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceDatabaseDrainStatus {
    pub database_id: u64,
    pub active_grants: usize,
    pub is_drained: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAdmissionDecision {
    NotRequired,
    Admitted(MaintenanceAdmissionGrant),
    Deferred { reason: MaintenanceAdmissionReason },
    Rejected { reason: MaintenanceAdmissionReason },
}

impl Default for MaintenanceAdmissionDecision {
    fn default() -> Self {
        Self::NotRequired
    }
}

impl MaintenanceAdmissionDecision {
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admitted(_))
    }

    pub const fn grant(self) -> Option<MaintenanceAdmissionGrant> {
        match self {
            Self::Admitted(grant) => Some(grant),
            Self::NotRequired | Self::Deferred { .. } | Self::Rejected { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAdmissionReason {
    CpuBudget,
    IoReadBudget,
    IoWriteBudget,
    MemoryBudget,
    PublishBudget,
    ProviderConcurrency,
    TableFairness,
    DatabaseDraining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceAdmissionPolicy {
    pub cpu_ns_budget: u64,
    pub io_read_bytes_budget: u64,
    pub foreground_io_read_bytes_reserved: u64,
    pub io_write_bytes_budget: u64,
    pub memory_peak_bytes_budget: u64,
    pub publish_bytes_budget: u64,
    pub fulltext_concurrency: usize,
    pub sparse_concurrency: usize,
    pub hnsw_concurrency: usize,
    pub table_concurrency: usize,
    pub draining_database_id: Option<u64>,
}

impl Default for MaintenanceAdmissionPolicy {
    fn default() -> Self {
        Self {
            cpu_ns_budget: u64::MAX,
            io_read_bytes_budget: u64::MAX,
            foreground_io_read_bytes_reserved: 0,
            io_write_bytes_budget: u64::MAX,
            memory_peak_bytes_budget: u64::MAX,
            publish_bytes_budget: u64::MAX,
            fulltext_concurrency: usize::MAX,
            sparse_concurrency: usize::MAX,
            hnsw_concurrency: usize::MAX,
            table_concurrency: usize::MAX,
            draining_database_id: None,
        }
    }
}

impl MaintenanceAdmissionPolicy {
    pub const fn with_draining_database(mut self, database_id: u64) -> Self {
        self.draining_database_id = Some(database_id);
        self
    }

    const fn provider_limit(self, provider: SearchIndexKind) -> usize {
        match provider {
            SearchIndexKind::FullText => self.fulltext_concurrency,
            SearchIndexKind::Sparse => self.sparse_concurrency,
            SearchIndexKind::Hnsw => self.hnsw_concurrency,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DefinitionMaintenanceDecision {
    pub(crate) action: SearchMaintenanceAction,
    pub(crate) admission: MaintenanceAdmissionDecision,
    pub(crate) gc_decision: GcDecision,
    pub(crate) estimate: CostEstimate,
    pub(crate) manifest_delta_compaction_requested: bool,
    pub(crate) manifest_delta_pressure: ManifestDeltaPressure,
    pub(crate) sidecar_repack_requested: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManifestDeltaPressure {
    Healthy,
    Soft,
    Hard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveMaintenanceGrant {
    fairness_key: MaintenanceFairnessKey,
    budget: MaintenanceCost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceQueuedTask {
    pub(crate) task_id: u64,
    pub(crate) request: MaintenanceAdmissionRequest,
    pub(crate) grant: MaintenanceAdmissionGrant,
}

#[derive(Debug)]
struct MaintenanceSchedulerState {
    next_grant_id: AtomicU64,
    next_task_id: AtomicU64,
    active_grants: BTreeMap<u64, ActiveMaintenanceGrant>,
    queued_tasks: BTreeMap<u64, MaintenanceQueuedTask>,
    draining_databases: BTreeSet<u64>,
}

impl Default for MaintenanceSchedulerState {
    fn default() -> Self {
        Self {
            next_grant_id: AtomicU64::new(1),
            next_task_id: AtomicU64::new(1),
            active_grants: BTreeMap::new(),
            queued_tasks: BTreeMap::new(),
            draining_databases: BTreeSet::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MaintenanceScheduler {
    policy: MaintenanceAdmissionPolicy,
    state: Mutex<MaintenanceSchedulerState>,
}

impl Default for MaintenanceScheduler {
    fn default() -> Self {
        Self::with_policy(MaintenanceAdmissionPolicy::default())
    }
}

impl MaintenanceScheduler {
    pub(crate) fn with_policy(policy: MaintenanceAdmissionPolicy) -> Self {
        let draining_database_id = policy.draining_database_id;
        let scheduler = Self {
            policy,
            state: Mutex::new(MaintenanceSchedulerState::default()),
        };
        if let Some(database_id) = draining_database_id {
            scheduler.begin_database_drain(database_id);
        }
        scheduler
    }

    pub(crate) fn begin_database_drain(&self, database_id: u64) -> MaintenanceDatabaseDrainStatus {
        self.state
            .lock()
            .map(|mut state| {
                state.draining_databases.insert(database_id);
                database_drain_status(&state, database_id)
            })
            .unwrap_or(MaintenanceDatabaseDrainStatus {
                database_id,
                active_grants: usize::MAX,
                is_drained: false,
            })
    }

    pub(crate) fn release(&self, grant_id: u64) -> bool {
        self.state
            .lock()
            .map(|mut state| state.active_grants.remove(&grant_id).is_some())
            .unwrap_or(false)
    }

    pub(crate) fn scoped_task_lease(
        self: &Arc<Self>,
        task: &MaintenanceQueuedTask,
    ) -> MaintenanceGrantLease {
        MaintenanceGrantLease {
            scheduler: Arc::clone(self),
            grant_id: task.grant.grant_id,
            released: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn queued_task_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.queued_tasks.len())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn decide_definition(
        &self,
        definition: &SearchIndexDefinition,
        manifest: &LoadedManifest,
        gc_decision: GcDecision,
        gc_context: &ArtifactGcContext,
        delta_window_bytes: u64,
    ) -> DefinitionMaintenanceDecision {
        let mut decision = self.plan_definition(
            definition,
            manifest,
            gc_decision,
            gc_context,
            delta_window_bytes,
        );
        decision.admission = self
            .admit_requests(&[self.admission_request(definition, manifest, &decision)])
            .into_iter()
            .next()
            .unwrap_or(MaintenanceAdmissionDecision::Rejected {
                reason: MaintenanceAdmissionReason::CpuBudget,
            });
        decision
    }

    pub(crate) fn plan_definition(
        &self,
        definition: &SearchIndexDefinition,
        manifest: &LoadedManifest,
        gc_decision: GcDecision,
        gc_context: &ArtifactGcContext,
        delta_window_bytes: u64,
    ) -> DefinitionMaintenanceDecision {
        let mut action = match gc_decision {
            GcDecision::Skip => SearchMaintenanceAction::Skip,
            GcDecision::CompactOnly => SearchMaintenanceAction::Compact,
            GcDecision::Heal => SearchMaintenanceAction::CatchUp,
            GcDecision::Rebuild => SearchMaintenanceAction::Rebuild,
        };
        // A complete HNSW L0 digit is the write-admission liveness boundary.
        // Drain it before optional GC/repack work. Only HNSW may leave a
        // sub-target exact L0 idle: sealing it would create tiny immutable
        // graphs and permanent query fan-out. Full-text and sparse artifacts
        // are segment-local postings and have no graph-size reason to defer;
        // Required freshness likewise admits no tail delay.
        if manifest.root.maintenance_state.recovery.tail_pending_rows > 0
            && (definition.kind != SearchIndexKind::Hnsw
                || definition.freshness_policy == SearchFreshnessPolicy::Required
                || manifest.root.maintenance_state.recovery.priority != MaintenancePriority::Idle)
        {
            action = SearchMaintenanceAction::CatchUp;
        }
        let manifest_delta_pressure = manifest_delta_pressure(manifest, delta_window_bytes);
        let manifest_delta_compaction_requested =
            !matches!(manifest_delta_pressure, ManifestDeltaPressure::Healthy);
        if manifest_delta_compaction_requested && matches!(action, SearchMaintenanceAction::Skip) {
            action = SearchMaintenanceAction::CompactManifestDelta;
        }
        let sidecar_repack_requested = sidecar_repack_needed(definition.kind, manifest);
        if sidecar_repack_requested && matches!(action, SearchMaintenanceAction::Skip) {
            action = SearchMaintenanceAction::RepackSidecar;
        }
        let estimate = estimate_maintenance_cost_benefit(
            definition.kind,
            action,
            manifest,
            gc_context,
            delta_window_bytes,
            manifest_delta_compaction_requested,
            sidecar_repack_requested,
        );
        DefinitionMaintenanceDecision {
            action,
            admission: MaintenanceAdmissionDecision::NotRequired,
            gc_decision,
            estimate,
            manifest_delta_compaction_requested,
            manifest_delta_pressure,
            sidecar_repack_requested,
        }
    }

    pub(crate) fn admission_request(
        &self,
        definition: &SearchIndexDefinition,
        manifest: &LoadedManifest,
        decision: &DefinitionMaintenanceDecision,
    ) -> MaintenanceAdmissionRequest {
        let (priority, backlog_tier) = maintenance_request_tier(
            manifest.root.maintenance_state.recovery.priority,
            manifest.root.maintenance_state.recovery.backlog_tier,
            decision.manifest_delta_pressure,
        );
        MaintenanceAdmissionRequest {
            definition_id: definition.definition_id,
            action: decision.action,
            fairness_key: MaintenanceFairnessKey::for_definition(definition),
            priority,
            backlog_tier,
            estimate: decision.estimate,
        }
    }

    pub(crate) fn admit_requests(
        &self,
        requests: &[MaintenanceAdmissionRequest],
    ) -> Vec<MaintenanceAdmissionDecision> {
        self.admit_requests_inner(requests, false, None)
    }

    pub(crate) fn schedule_requests(
        &self,
        requests: &[MaintenanceAdmissionRequest],
    ) -> Vec<MaintenanceAdmissionDecision> {
        self.admit_requests_inner(requests, true, None)
    }

    /// Admit and enqueue at most one request from this scheduling quantum.
    ///
    /// Callers that execute one unit of work per fairness turn must not use
    /// [`Self::schedule_requests`]: every admitted request owns an active grant
    /// until its queued task is executed. Limiting admission here makes queue
    /// ownership match execution ownership instead of leaving unconsumed
    /// grants behind to throttle later turns.
    pub(crate) fn schedule_next_request(
        &self,
        requests: &[MaintenanceAdmissionRequest],
    ) -> Vec<MaintenanceAdmissionDecision> {
        self.admit_requests_inner(requests, true, Some(1))
    }

    pub(crate) fn pop_next_task(&self) -> Option<MaintenanceQueuedTask> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        let task_id = state
            .queued_tasks
            .iter()
            .max_by(|(_, left), (_, right)| maintenance_queued_task_cmp(left, right))
            .map(|(task_id, _)| *task_id)?;
        state.queued_tasks.remove(&task_id)
    }

    fn admit_requests_inner(
        &self,
        requests: &[MaintenanceAdmissionRequest],
        queue_admitted: bool,
        max_new_grants: Option<usize>,
    ) -> Vec<MaintenanceAdmissionDecision> {
        let Ok(mut state) = self.state.lock() else {
            return requests
                .iter()
                .map(|request| {
                    if matches!(request.action, SearchMaintenanceAction::Skip) {
                        MaintenanceAdmissionDecision::NotRequired
                    } else {
                        MaintenanceAdmissionDecision::Deferred {
                            reason: MaintenanceAdmissionReason::CpuBudget,
                        }
                    }
                })
                .collect();
        };
        let mut decisions = vec![MaintenanceAdmissionDecision::NotRequired; requests.len()];
        let mut remaining = MaintenanceCost {
            cpu_ns: self.policy.cpu_ns_budget,
            io_read_bytes: self
                .policy
                .io_read_bytes_budget
                .saturating_sub(self.policy.foreground_io_read_bytes_reserved),
            io_write_bytes: self.policy.io_write_bytes_budget,
            memory_peak_bytes: self.policy.memory_peak_bytes_budget,
            publish_bytes: self.policy.publish_bytes_budget,
        };
        let mut provider_counts = BTreeMap::<SearchIndexKind, usize>::new();
        let mut table_counts = BTreeMap::<(u64, TableId), usize>::new();
        for active in state.active_grants.values() {
            remaining = subtract_cost(remaining, active.budget);
            *provider_counts
                .entry(active.fairness_key.provider)
                .or_default() += 1;
            *table_counts
                .entry((
                    active.fairness_key.database_id,
                    active.fairness_key.table_id,
                ))
                .or_default() += 1;
        }
        let mut order = (0..requests.len()).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            maintenance_admission_rank(&requests[*right])
                .cmp(&maintenance_admission_rank(&requests[*left]))
                .then_with(|| {
                    requests[*left]
                        .definition_id
                        .cmp(&requests[*right].definition_id)
                })
        });

        let mut new_grants = 0usize;
        for index in order {
            let request = &requests[index];
            if matches!(request.action, SearchMaintenanceAction::Skip) {
                decisions[index] = MaintenanceAdmissionDecision::NotRequired;
                continue;
            }
            if max_new_grants.is_some_and(|limit| new_grants >= limit) {
                decisions[index] = MaintenanceAdmissionDecision::Deferred {
                    reason: MaintenanceAdmissionReason::TableFairness,
                };
                continue;
            }
            if state
                .draining_databases
                .contains(&request.fairness_key.database_id)
                || self
                    .policy
                    .draining_database_id
                    .is_some_and(|database_id| database_id == request.fairness_key.database_id)
            {
                decisions[index] = MaintenanceAdmissionDecision::Rejected {
                    reason: MaintenanceAdmissionReason::DatabaseDraining,
                };
                continue;
            }
            let table_key = (
                request.fairness_key.database_id,
                request.fairness_key.table_id,
            );
            if table_counts.get(&table_key).copied().unwrap_or_default()
                >= self.policy.table_concurrency
            {
                decisions[index] = MaintenanceAdmissionDecision::Deferred {
                    reason: MaintenanceAdmissionReason::TableFairness,
                };
                continue;
            }
            let provider = request.fairness_key.provider;
            if provider_counts.get(&provider).copied().unwrap_or_default()
                >= self.policy.provider_limit(provider)
            {
                decisions[index] = MaintenanceAdmissionDecision::Deferred {
                    reason: MaintenanceAdmissionReason::ProviderConcurrency,
                };
                continue;
            }
            if let Some(reason) = first_budget_exceeded(remaining, request.estimate.cost) {
                decisions[index] = MaintenanceAdmissionDecision::Deferred { reason };
                continue;
            }

            remaining = subtract_cost(remaining, request.estimate.cost);
            *provider_counts.entry(provider).or_default() += 1;
            *table_counts.entry(table_key).or_default() += 1;
            let grant_id = state.next_grant_id.fetch_add(1, Ordering::Relaxed);
            let grant = MaintenanceAdmissionGrant {
                grant_id,
                budget: request.estimate.cost,
            };
            state.active_grants.insert(
                grant_id,
                ActiveMaintenanceGrant {
                    fairness_key: request.fairness_key,
                    budget: request.estimate.cost,
                },
            );
            new_grants = new_grants.saturating_add(1);
            if queue_admitted {
                let task_id = state.next_task_id.fetch_add(1, Ordering::Relaxed);
                state.queued_tasks.insert(
                    task_id,
                    MaintenanceQueuedTask {
                        task_id,
                        request: request.clone(),
                        grant,
                    },
                );
            }
            decisions[index] = MaintenanceAdmissionDecision::Admitted(grant);
        }

        decisions
    }
}

fn maintenance_queued_task_cmp(
    left: &MaintenanceQueuedTask,
    right: &MaintenanceQueuedTask,
) -> std::cmp::Ordering {
    maintenance_admission_rank(&left.request)
        .cmp(&maintenance_admission_rank(&right.request))
        .then_with(|| right.task_id.cmp(&left.task_id))
}

fn database_drain_status(
    state: &MaintenanceSchedulerState,
    database_id: u64,
) -> MaintenanceDatabaseDrainStatus {
    let active_grants = state
        .active_grants
        .values()
        .filter(|grant| grant.fairness_key.database_id == database_id)
        .count();
    MaintenanceDatabaseDrainStatus {
        database_id,
        active_grants,
        is_drained: active_grants == 0,
    }
}

#[derive(Debug)]
pub(crate) struct MaintenanceGrantLease {
    scheduler: Arc<MaintenanceScheduler>,
    grant_id: u64,
    released: bool,
}

impl MaintenanceGrantLease {
    fn release_inner(&mut self) {
        if !self.released {
            self.scheduler.release(self.grant_id);
            self.released = true;
        }
    }
}

impl Drop for MaintenanceGrantLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[derive(Debug)]
pub(crate) struct InlineSearchAdmission {
    scheduler: Arc<MaintenanceScheduler>,
}

impl Default for InlineSearchAdmission {
    fn default() -> Self {
        Self {
            scheduler: Arc::new(MaintenanceScheduler::default()),
        }
    }
}

impl InlineSearchAdmission {
    #[cfg(test)]
    pub(crate) fn with_policy(policy: MaintenanceAdmissionPolicy) -> Self {
        Self {
            scheduler: Arc::new(MaintenanceScheduler::with_policy(policy)),
        }
    }

    pub(crate) fn with_scheduler(scheduler: Arc<MaintenanceScheduler>) -> Self {
        Self { scheduler }
    }
}

impl SearchAdmission for InlineSearchAdmission {
    fn request_inline_batch(
        &self,
        reqs: &[InlineAdmissionRequest],
    ) -> Result<Vec<AdmissionDecision>> {
        let mut responses = vec![None; reqs.len()];
        let mut schedulable_indices = Vec::new();
        let requests = reqs
            .iter()
            .enumerate()
            .filter_map(|(idx, req)| {
                if req
                    .hnsw_inline
                    .is_some_and(|estimate| !estimate.allows_inline())
                {
                    responses[idx] = Some(AdmissionDecision::Reject {
                        reason: AdmissionRejectReason::InlineThresholdExceeded,
                    });
                    return None;
                }
                schedulable_indices.push(idx);
                Some(MaintenanceAdmissionRequest {
                    definition_id: req.definition_id,
                    action: SearchMaintenanceAction::CatchUp,
                    fairness_key: MaintenanceFairnessKey {
                        database_id: 0,
                        table_id: req.table_id,
                        provider: req.provider,
                    },
                    priority: inline_flush_priority(req.flush_mode),
                    backlog_tier: CatchUpBacklogTier::Healthy,
                    estimate: CostEstimate {
                        cost: req.estimated_cost,
                        benefit: MaintenanceBenefit {
                            expected_tail_rows_drained: req.row_count,
                            ..Default::default()
                        },
                    },
                })
            })
            .collect::<Vec<_>>();
        let decisions = self.scheduler.admit_requests(&requests);
        for (idx, decision) in schedulable_indices.into_iter().zip(decisions) {
            let req = &reqs[idx];
            responses[idx] = Some(match decision {
                MaintenanceAdmissionDecision::Admitted(grant) => {
                    AdmissionDecision::Proceed(AdmissionGrant {
                        budget: grant.budget,
                        valid_until: Instant::now(),
                        grant_id: grant.grant_id,
                    })
                }
                MaintenanceAdmissionDecision::NotRequired => {
                    AdmissionDecision::Proceed(AdmissionGrant {
                        budget: req.estimated_cost,
                        valid_until: Instant::now(),
                        grant_id: 0,
                    })
                }
                MaintenanceAdmissionDecision::Deferred { reason } => AdmissionDecision::Wait {
                    deadline: Instant::now(),
                    reason: inline_wait_reason(reason),
                },
                MaintenanceAdmissionDecision::Rejected { reason } => AdmissionDecision::Reject {
                    reason: inline_reject_reason(reason),
                },
            });
        }
        Ok(responses
            .into_iter()
            .map(|decision| {
                decision.unwrap_or(AdmissionDecision::Reject {
                    reason: AdmissionRejectReason::InvalidRequest,
                })
            })
            .collect())
    }

    fn release(&self, grant_id: u64) {
        if grant_id != 0 {
            self.scheduler.release(grant_id);
        }
    }
}

const fn inline_flush_priority(mode: FlushSearchMode) -> MaintenancePriority {
    match mode {
        FlushSearchMode::InlineRequired => MaintenancePriority::Critical,
        FlushSearchMode::InlineIfAdmitted => MaintenancePriority::Opportunistic,
        FlushSearchMode::TailOnly => MaintenancePriority::Idle,
    }
}

const fn inline_wait_reason(reason: MaintenanceAdmissionReason) -> AdmissionWaitReason {
    match reason {
        MaintenanceAdmissionReason::CpuBudget => AdmissionWaitReason::CpuBudget,
        MaintenanceAdmissionReason::IoReadBudget
        | MaintenanceAdmissionReason::IoWriteBudget
        | MaintenanceAdmissionReason::PublishBudget
        | MaintenanceAdmissionReason::TableFairness => AdmissionWaitReason::IoBudget,
        MaintenanceAdmissionReason::MemoryBudget => AdmissionWaitReason::MemoryBudget,
        MaintenanceAdmissionReason::ProviderConcurrency => AdmissionWaitReason::ProviderConcurrency,
        MaintenanceAdmissionReason::DatabaseDraining => AdmissionWaitReason::ProviderConcurrency,
    }
}

const fn inline_reject_reason(reason: MaintenanceAdmissionReason) -> AdmissionRejectReason {
    match reason {
        MaintenanceAdmissionReason::DatabaseDraining => AdmissionRejectReason::ProviderDisabled,
        _ => AdmissionRejectReason::RequiredBudgetUnavailable,
    }
}

fn maintenance_admission_rank(
    request: &MaintenanceAdmissionRequest,
) -> (u8, u8, u64, u64, u64, u64) {
    let benefit = request.estimate.benefit;
    (
        maintenance_priority_rank(request.priority),
        backlog_tier_rank(request.backlog_tier),
        benefit.expected_tail_rows_drained,
        benefit.expected_open_cost_saved_us,
        benefit.expected_artifact_bytes_reclaimed,
        maintenance_action_rank(request.action),
    )
}

fn manifest_delta_pressure(
    manifest: &LoadedManifest,
    delta_window_bytes: u64,
) -> ManifestDeltaPressure {
    let delta_count = manifest.root.recent_delta_files.len();
    if delta_count > DELTA_COUNT_HARD_LIMIT || delta_window_bytes > DELTA_BYTES_HARD_LIMIT {
        ManifestDeltaPressure::Hard
    } else if delta_count > DELTA_COUNT_SOFT_LIMIT || delta_window_bytes > DELTA_BYTES_SOFT_LIMIT {
        ManifestDeltaPressure::Soft
    } else {
        ManifestDeltaPressure::Healthy
    }
}

fn maintenance_request_tier(
    priority: MaintenancePriority,
    backlog_tier: CatchUpBacklogTier,
    delta_pressure: ManifestDeltaPressure,
) -> (MaintenancePriority, CatchUpBacklogTier) {
    if matches!(delta_pressure, ManifestDeltaPressure::Hard) {
        (
            max_maintenance_priority(priority, MaintenancePriority::Critical),
            max_backlog_tier(backlog_tier, CatchUpBacklogTier::Degraded),
        )
    } else {
        (priority, backlog_tier)
    }
}

fn max_maintenance_priority(
    left: MaintenancePriority,
    right: MaintenancePriority,
) -> MaintenancePriority {
    if maintenance_priority_rank(left) >= maintenance_priority_rank(right) {
        left
    } else {
        right
    }
}

fn max_backlog_tier(left: CatchUpBacklogTier, right: CatchUpBacklogTier) -> CatchUpBacklogTier {
    if backlog_tier_rank(left) >= backlog_tier_rank(right) {
        left
    } else {
        right
    }
}

const fn maintenance_priority_rank(priority: MaintenancePriority) -> u8 {
    match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic => 1,
        MaintenancePriority::Elevated => 2,
        MaintenancePriority::Critical => 3,
    }
}

const fn backlog_tier_rank(tier: CatchUpBacklogTier) -> u8 {
    match tier {
        CatchUpBacklogTier::Healthy => 0,
        CatchUpBacklogTier::Elevated => 1,
        CatchUpBacklogTier::Degraded => 2,
    }
}

const fn maintenance_action_rank(action: SearchMaintenanceAction) -> u64 {
    match action {
        SearchMaintenanceAction::Skip => 0,
        SearchMaintenanceAction::CompactManifestDelta => 1,
        SearchMaintenanceAction::RepackSidecar => 2,
        SearchMaintenanceAction::CatchUp => 3,
        SearchMaintenanceAction::Compact => 4,
        SearchMaintenanceAction::Rebuild => 5,
    }
}

fn first_budget_exceeded(
    remaining: MaintenanceCost,
    cost: MaintenanceCost,
) -> Option<MaintenanceAdmissionReason> {
    if cost.cpu_ns > remaining.cpu_ns {
        Some(MaintenanceAdmissionReason::CpuBudget)
    } else if cost.io_read_bytes > remaining.io_read_bytes {
        Some(MaintenanceAdmissionReason::IoReadBudget)
    } else if cost.io_write_bytes > remaining.io_write_bytes {
        Some(MaintenanceAdmissionReason::IoWriteBudget)
    } else if cost.memory_peak_bytes > remaining.memory_peak_bytes {
        Some(MaintenanceAdmissionReason::MemoryBudget)
    } else if cost.publish_bytes > remaining.publish_bytes {
        Some(MaintenanceAdmissionReason::PublishBudget)
    } else {
        None
    }
}

fn subtract_cost(remaining: MaintenanceCost, cost: MaintenanceCost) -> MaintenanceCost {
    MaintenanceCost {
        cpu_ns: remaining.cpu_ns.saturating_sub(cost.cpu_ns),
        io_read_bytes: remaining.io_read_bytes.saturating_sub(cost.io_read_bytes),
        io_write_bytes: remaining.io_write_bytes.saturating_sub(cost.io_write_bytes),
        memory_peak_bytes: remaining
            .memory_peak_bytes
            .saturating_sub(cost.memory_peak_bytes),
        publish_bytes: remaining.publish_bytes.saturating_sub(cost.publish_bytes),
    }
}

const MAINTENANCE_PUBLISH_BASE_BYTES: u64 = 4 * 1024;
const MAINTENANCE_PUBLISH_ENTRY_BYTES: u64 = 256;
const DELTA_REPLAY_FIXED_OPEN_COST_US: u64 = 50;
const DELTA_REPLAY_BYTES_PER_US: u64 = 64 * 1024;
const SIDECAR_OPEN_FIXED_COST_US: u64 = 25;
const SIDECAR_TARGET_ARTIFACTS_PER_PACKAGE: usize = 64;
const FULLTEXT_CPU_NS_PER_ROW: u64 = 150_000;
const SPARSE_CPU_NS_PER_ROW: u64 = 80_000;
const HNSW_CPU_NS_PER_ROW: u64 = 1_500_000;
const MIN_ACTIVE_MEMORY_BYTES: u64 = 1024 * 1024;

fn estimate_maintenance_cost_benefit(
    kind: SearchIndexKind,
    action: SearchMaintenanceAction,
    manifest: &LoadedManifest,
    gc_context: &ArtifactGcContext,
    delta_window_bytes: u64,
    manifest_delta_compaction_requested: bool,
    sidecar_repack_requested: bool,
) -> CostEstimate {
    if matches!(action, SearchMaintenanceAction::Skip)
        && !manifest_delta_compaction_requested
        && !sidecar_repack_requested
    {
        return CostEstimate::default();
    }

    let tail_rows = manifest.root.maintenance_state.recovery.tail_pending_rows;
    let tail_rowsets = manifest
        .root
        .maintenance_state
        .recovery
        .tail_pending_rowsets as u64;
    let tail_bytes = manifest
        .tail_pending_entries
        .iter()
        .filter(|entry| entry.mutation != TailMutationKind::Delete)
        .map(|entry| entry.byte_count)
        .sum::<u64>();
    let artifact_bytes = gc_context.bytes_on_disk;
    let indexed_rows = manifest.root.generation_stats.indexed_rows;
    let delta_count = manifest.root.recent_delta_files.len() as u64;

    let mut estimate = match action {
        SearchMaintenanceAction::Skip | SearchMaintenanceAction::CompactManifestDelta => {
            CostEstimate::default()
        }
        SearchMaintenanceAction::RepackSidecar => estimate_sidecar_repack_cost_benefit(manifest),
        SearchMaintenanceAction::CatchUp => CostEstimate {
            cost: MaintenanceCost {
                cpu_ns: tail_rows.saturating_mul(provider_cpu_ns_per_row(kind)),
                io_read_bytes: tail_bytes,
                io_write_bytes: estimate_provider_artifact_bytes(kind, tail_bytes, tail_rows),
                memory_peak_bytes: estimate_provider_memory_peak(kind, tail_bytes, tail_rows),
                publish_bytes: estimate_manifest_publish_bytes(
                    delta_count,
                    tail_rowsets.saturating_add(1),
                ),
            },
            benefit: MaintenanceBenefit {
                expected_tail_rows_drained: tail_rows,
                ..Default::default()
            },
        },
        SearchMaintenanceAction::Compact => CostEstimate {
            cost: MaintenanceCost {
                cpu_ns: indexed_rows
                    .saturating_mul(provider_cpu_ns_per_row(kind) / 4)
                    .saturating_add(delta_count.saturating_mul(10_000)),
                io_read_bytes: artifact_bytes.saturating_add(delta_window_bytes),
                io_write_bytes: estimate_compacted_artifact_bytes(kind, artifact_bytes),
                memory_peak_bytes: estimate_provider_memory_peak(
                    kind,
                    artifact_bytes,
                    indexed_rows,
                ),
                publish_bytes: estimate_manifest_publish_bytes(
                    delta_count,
                    manifest.root.generation_stats.artifact_count as u64,
                ),
            },
            benefit: MaintenanceBenefit {
                expected_open_cost_saved_us: estimate_delta_open_cost_saved_us(
                    delta_count,
                    delta_window_bytes,
                ),
                expected_artifact_bytes_reclaimed: estimate_tombstone_reclaim_bytes(
                    artifact_bytes,
                    manifest.root.maintenance_state.tombstone_ratio_millis,
                ),
                ..Default::default()
            },
        },
        SearchMaintenanceAction::Rebuild => {
            let rebuild_rows = indexed_rows.saturating_add(tail_rows);
            let rebuild_input_bytes = artifact_bytes.saturating_add(tail_bytes);
            CostEstimate {
                cost: MaintenanceCost {
                    cpu_ns: rebuild_rows.saturating_mul(provider_cpu_ns_per_row(kind)),
                    io_read_bytes: rebuild_input_bytes.saturating_add(delta_window_bytes),
                    io_write_bytes: estimate_provider_artifact_bytes(
                        kind,
                        rebuild_input_bytes,
                        rebuild_rows,
                    ),
                    memory_peak_bytes: estimate_provider_memory_peak(
                        kind,
                        rebuild_input_bytes,
                        rebuild_rows,
                    ),
                    publish_bytes: estimate_manifest_publish_bytes(
                        delta_count,
                        (manifest.root.generation_stats.artifact_count as u64)
                            .saturating_add(tail_rowsets),
                    ),
                },
                benefit: MaintenanceBenefit {
                    expected_open_cost_saved_us: estimate_delta_open_cost_saved_us(
                        delta_count,
                        delta_window_bytes,
                    ),
                    expected_tail_rows_drained: tail_rows,
                    expected_artifact_bytes_reclaimed: estimate_tombstone_reclaim_bytes(
                        artifact_bytes,
                        manifest.root.maintenance_state.tombstone_ratio_millis,
                    ),
                },
            }
        }
    };
    if kind == SearchIndexKind::Hnsw {
        // Every newly-written HNSW payload is read back through its checksum
        // hierarchy before a durable generation head can name it. This is a
        // required publication cost, not optional background residency work,
        // and must therefore consume the same I/O envelope as construction.
        estimate.cost.io_read_bytes = estimate
            .cost
            .io_read_bytes
            .saturating_add(estimate.cost.io_write_bytes);
    }
    if manifest_delta_compaction_requested {
        add_manifest_delta_compaction_estimate(&mut estimate, manifest, delta_window_bytes);
    }
    if sidecar_repack_requested && !matches!(action, SearchMaintenanceAction::RepackSidecar) {
        add_sidecar_repack_estimate(&mut estimate, manifest);
    }
    estimate
}

pub(crate) fn sidecar_repack_needed(kind: SearchIndexKind, manifest: &LoadedManifest) -> bool {
    // HNSW artifacts are complete immutable graphs, not small postings that
    // benefit from sharing a package. Repacking a newly appended catch-up
    // graph would copy the multi-gigabyte base graph merely to save one file
    // open, while blocking the only maintenance lane needed to keep ingest
    // fresh. Graph fan-out is reduced by generation compaction, which also
    // improves query execution; byte-for-byte package consolidation does not.
    if kind == SearchIndexKind::Hnsw {
        return false;
    }
    let stats = sidecar_package_stats(manifest);
    stats.package_count > sidecar_package_target(stats.artifact_count)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SidecarPackageStats {
    package_count: usize,
    artifact_count: usize,
    artifact_bytes: u64,
    row_count: u64,
}

fn sidecar_package_stats(manifest: &LoadedManifest) -> SidecarPackageStats {
    let mut file_ids = BTreeSet::new();
    let mut stats = SidecarPackageStats::default();
    for artifact in &manifest.artifacts.artifacts {
        if let ArtifactLocation::SidecarArtifactFile { file_id, .. } = artifact.location {
            file_ids.insert(file_id);
            stats.artifact_count += 1;
            stats.artifact_bytes = stats
                .artifact_bytes
                .saturating_add(artifact.stats.bytes_on_disk);
            stats.row_count = stats.row_count.saturating_add(artifact.stats.row_count);
        }
    }
    stats.package_count = file_ids.len();
    stats
}

const fn sidecar_package_target(artifact_count: usize) -> usize {
    if artifact_count == 0 {
        0
    } else {
        artifact_count.div_ceil(SIDECAR_TARGET_ARTIFACTS_PER_PACKAGE)
    }
}

fn estimate_sidecar_repack_cost_benefit(manifest: &LoadedManifest) -> CostEstimate {
    let stats = sidecar_package_stats(manifest);
    if stats.artifact_count == 0 {
        return CostEstimate::default();
    }
    CostEstimate {
        cost: MaintenanceCost {
            cpu_ns: stats.row_count.saturating_mul(5_000),
            io_read_bytes: stats.artifact_bytes,
            io_write_bytes: stats.artifact_bytes,
            memory_peak_bytes: MIN_ACTIVE_MEMORY_BYTES,
            publish_bytes: estimate_manifest_publish_bytes(0, stats.artifact_count as u64),
        },
        benefit: MaintenanceBenefit {
            expected_open_cost_saved_us: stats
                .package_count
                .saturating_sub(sidecar_package_target(stats.artifact_count))
                as u64
                * SIDECAR_OPEN_FIXED_COST_US,
            expected_artifact_bytes_reclaimed: stats.artifact_bytes,
            ..Default::default()
        },
    }
}

fn add_sidecar_repack_estimate(estimate: &mut CostEstimate, manifest: &LoadedManifest) {
    let repack = estimate_sidecar_repack_cost_benefit(manifest);
    estimate.cost.cpu_ns = estimate.cost.cpu_ns.saturating_add(repack.cost.cpu_ns);
    estimate.cost.io_read_bytes = estimate
        .cost
        .io_read_bytes
        .saturating_add(repack.cost.io_read_bytes);
    estimate.cost.io_write_bytes = estimate
        .cost
        .io_write_bytes
        .saturating_add(repack.cost.io_write_bytes);
    estimate.cost.memory_peak_bytes = estimate
        .cost
        .memory_peak_bytes
        .max(repack.cost.memory_peak_bytes);
    estimate.cost.publish_bytes = estimate
        .cost
        .publish_bytes
        .saturating_add(repack.cost.publish_bytes);
    estimate.benefit.expected_open_cost_saved_us = estimate
        .benefit
        .expected_open_cost_saved_us
        .saturating_add(repack.benefit.expected_open_cost_saved_us);
    estimate.benefit.expected_artifact_bytes_reclaimed = estimate
        .benefit
        .expected_artifact_bytes_reclaimed
        .saturating_add(repack.benefit.expected_artifact_bytes_reclaimed);
}

fn add_manifest_delta_compaction_estimate(
    estimate: &mut CostEstimate,
    manifest: &LoadedManifest,
    delta_window_bytes: u64,
) {
    let delta_count = manifest.root.recent_delta_files.len() as u64;
    let compacted_entry_count = (manifest.root.generation_stats.artifact_count as u64)
        .saturating_add(manifest.tail_pending_entries.len() as u64);
    estimate.cost.cpu_ns = estimate
        .cost
        .cpu_ns
        .saturating_add(delta_count.saturating_mul(10_000));
    estimate.cost.io_read_bytes = estimate
        .cost
        .io_read_bytes
        .saturating_add(delta_window_bytes);
    estimate.cost.io_write_bytes = estimate
        .cost
        .io_write_bytes
        .saturating_add(estimate_manifest_publish_bytes(0, compacted_entry_count));
    estimate.cost.memory_peak_bytes = estimate.cost.memory_peak_bytes.max(if delta_count == 0 {
        0
    } else {
        MIN_ACTIVE_MEMORY_BYTES
    });
    estimate.cost.publish_bytes =
        estimate
            .cost
            .publish_bytes
            .saturating_add(estimate_manifest_publish_bytes(
                delta_count,
                compacted_entry_count,
            ));
    estimate.benefit.expected_open_cost_saved_us =
        estimate.benefit.expected_open_cost_saved_us.saturating_add(
            estimate_delta_open_cost_saved_us(delta_count, delta_window_bytes),
        );
}

fn provider_cpu_ns_per_row(kind: SearchIndexKind) -> u64 {
    match kind {
        SearchIndexKind::FullText => FULLTEXT_CPU_NS_PER_ROW,
        SearchIndexKind::Sparse => SPARSE_CPU_NS_PER_ROW,
        SearchIndexKind::Hnsw => HNSW_CPU_NS_PER_ROW,
    }
}

fn estimate_provider_artifact_bytes(
    kind: SearchIndexKind,
    source_bytes: u64,
    row_count: u64,
) -> u64 {
    let scaled = match kind {
        SearchIndexKind::FullText => scale_bytes(source_bytes, 45),
        SearchIndexKind::Sparse => scale_bytes(source_bytes, 30),
        SearchIndexKind::Hnsw => scale_bytes(source_bytes, 180),
    };
    if row_count == 0 {
        scaled
    } else {
        scaled.max(1024)
    }
}

fn estimate_compacted_artifact_bytes(kind: SearchIndexKind, artifact_bytes: u64) -> u64 {
    match kind {
        SearchIndexKind::FullText => scale_bytes(artifact_bytes, 85),
        SearchIndexKind::Sparse => scale_bytes(artifact_bytes, 80),
        SearchIndexKind::Hnsw => scale_bytes(artifact_bytes, 95),
    }
}

fn estimate_provider_memory_peak(kind: SearchIndexKind, source_bytes: u64, row_count: u64) -> u64 {
    let bytes_driven = match kind {
        SearchIndexKind::FullText => scale_bytes(source_bytes, 80),
        SearchIndexKind::Sparse => scale_bytes(source_bytes, 50),
        SearchIndexKind::Hnsw => scale_bytes(source_bytes, 250),
    };
    let row_driven = row_count.saturating_mul(match kind {
        SearchIndexKind::FullText => 256,
        SearchIndexKind::Sparse => 128,
        SearchIndexKind::Hnsw => 6 * 1024,
    });
    let peak = bytes_driven.max(row_driven);
    if peak == 0 {
        0
    } else {
        peak.max(MIN_ACTIVE_MEMORY_BYTES)
    }
}

fn estimate_manifest_publish_bytes(delta_count: u64, entry_count: u64) -> u64 {
    if delta_count == 0 && entry_count == 0 {
        0
    } else {
        MAINTENANCE_PUBLISH_BASE_BYTES.saturating_add(
            delta_count
                .saturating_add(entry_count)
                .saturating_mul(MAINTENANCE_PUBLISH_ENTRY_BYTES),
        )
    }
}

fn estimate_delta_open_cost_saved_us(delta_count: u64, delta_window_bytes: u64) -> u64 {
    delta_count
        .saturating_mul(DELTA_REPLAY_FIXED_OPEN_COST_US)
        .saturating_add(div_ceil_u64(delta_window_bytes, DELTA_REPLAY_BYTES_PER_US))
}

fn estimate_tombstone_reclaim_bytes(artifact_bytes: u64, tombstone_ratio_millis: u32) -> u64 {
    artifact_bytes.saturating_mul(tombstone_ratio_millis.min(1000) as u64) / 1000
}

fn scale_bytes(bytes: u64, percent: u64) -> u64 {
    bytes.saturating_mul(percent).saturating_add(99) / 100
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
    if value == 0 {
        0
    } else {
        value.saturating_sub(1) / divisor + 1
    }
}
