// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Thin transaction commit facade and participant contracts.

use crate::participant_state::ParticipantStateSet;
use crate::sync::Mutex;
use crate::types::{
    CommitTs, DatabaseId, ParticipantId, ParticipantKind, ReadTs, TxnId, TxnResourceKey,
};
use crate::view::{ReadDependency, TransactionView};
use crate::{LockMode, LockRequest, LockResource};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PARTICIPANT_DESCRIPTOR_VERSION: u16 = 1;
pub const COMMITTED_TXN_RECORD_VERSION: u16 = 1;
pub const MAINTENANCE_RECORD_VERSION: u16 = 1;
pub const DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE: usize = 256;
pub const DEFAULT_MAX_GROUP_COMMIT_FENCE_US: u64 = 500;
pub const DEFAULT_MAX_UNPUBLISHED_COMMITS: u64 = 1024;
pub const DEFAULT_MAX_PARTICIPANT_APPLY_LAG: u64 = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRecordVersionError {
    UnsupportedCommittedRecordVersion {
        found: u16,
        expected: u16,
    },
    UnsupportedParticipantDescriptorVersion {
        participant_id: ParticipantId,
        found: u16,
        expected: u16,
    },
}

impl fmt::Display for CommitRecordVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCommittedRecordVersion { found, expected } => write!(
                f,
                "unsupported committed transaction record version {found}, expected {expected}"
            ),
            Self::UnsupportedParticipantDescriptorVersion {
                participant_id,
                found,
                expected,
            } => write!(
                f,
                "unsupported participant descriptor version {found} for participant {participant_id}, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for CommitRecordVersionError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CommitAckPolicy {
    #[default]
    RequiredPublished,
    DurableOnlyAsync,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    Serializable,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(u32);

impl CommandId {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn into_raw(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrozenReadSet {
    dependency_count: usize,
    coarsened: bool,
    dependencies: Vec<ReadDependency>,
}

impl FrozenReadSet {
    #[inline]
    pub const fn empty() -> Self {
        Self {
            dependency_count: 0,
            coarsened: false,
            dependencies: Vec::new(),
        }
    }

    #[inline]
    pub const fn from_dependency_count(dependency_count: usize) -> Self {
        Self {
            dependency_count,
            coarsened: false,
            dependencies: Vec::new(),
        }
    }

    #[inline]
    pub fn from_dependencies(dependencies: Vec<ReadDependency>) -> Self {
        Self {
            dependency_count: dependencies.len(),
            coarsened: false,
            dependencies,
        }
    }

    #[inline]
    pub fn from_dependencies_with_coarsening(
        dependencies: Vec<ReadDependency>,
        coarsened: bool,
    ) -> Self {
        Self {
            dependency_count: dependencies.len(),
            coarsened,
            dependencies,
        }
    }

    #[inline]
    pub const fn dependency_count(&self) -> usize {
        self.dependency_count
    }

    #[inline]
    pub const fn is_coarsened(&self) -> bool {
        self.coarsened
    }

    #[inline]
    pub fn dependencies(&self) -> &[ReadDependency] {
        &self.dependencies
    }

    #[inline]
    pub fn storage_snapshot_count(&self) -> usize {
        self.dependencies
            .iter()
            .filter(|dependency| matches!(dependency, ReadDependency::Tablet { .. }))
            .count()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrozenLockSet {
    locks: Vec<LockRequest>,
}

impl FrozenLockSet {
    #[inline]
    pub const fn empty() -> Self {
        Self { locks: Vec::new() }
    }

    #[inline]
    pub fn from_locks(locks: Vec<LockRequest>) -> Self {
        Self { locks }
    }

    #[inline]
    pub fn held_lock_count(&self) -> usize {
        self.locks.len()
    }

    #[inline]
    pub fn locks(&self) -> &[LockRequest] {
        &self.locks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParticipantRole {
    Required,
    Deferred,
}

impl ParticipantRole {
    #[inline]
    pub const fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }

    #[inline]
    pub const fn is_deferred(self) -> bool {
        matches!(self, Self::Deferred)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParticipantDescriptor {
    pub descriptor_version: u16,
    pub participant_id: ParticipantId,
    pub kind: ParticipantKind,
    pub resource_key: TxnResourceKey,
    pub role: ParticipantRole,
}

impl ParticipantDescriptor {
    #[inline]
    pub const fn new(
        participant_id: ParticipantId,
        kind: ParticipantKind,
        resource_key: TxnResourceKey,
    ) -> Self {
        Self::with_descriptor_version(
            PARTICIPANT_DESCRIPTOR_VERSION,
            participant_id,
            kind,
            resource_key,
        )
    }

    #[inline]
    pub const fn with_descriptor_version(
        descriptor_version: u16,
        participant_id: ParticipantId,
        kind: ParticipantKind,
        resource_key: TxnResourceKey,
    ) -> Self {
        Self {
            descriptor_version,
            participant_id,
            kind,
            resource_key,
            role: ParticipantRole::Required,
        }
    }

    #[inline]
    pub const fn with_role(mut self, role: ParticipantRole) -> Self {
        self.role = role;
        self
    }

    #[inline]
    pub const fn required(self) -> Self {
        self.with_role(ParticipantRole::Required)
    }

    #[inline]
    pub const fn deferred(self) -> Self {
        self.with_role(ParticipantRole::Deferred)
    }

    #[inline]
    pub const fn is_required(&self) -> bool {
        self.role.is_required()
    }

    #[inline]
    pub const fn is_deferred(&self) -> bool {
        self.role.is_deferred()
    }

    #[inline]
    pub fn validate_version(&self) -> std::result::Result<(), CommitRecordVersionError> {
        if self.descriptor_version != PARTICIPANT_DESCRIPTOR_VERSION {
            return Err(
                CommitRecordVersionError::UnsupportedParticipantDescriptorVersion {
                    participant_id: self.participant_id,
                    found: self.descriptor_version,
                    expected: PARTICIPANT_DESCRIPTOR_VERSION,
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRequestValidationError {
    CoordinatorDatabaseMismatch {
        coordinator: DatabaseId,
        request: DatabaseId,
    },
    PlanDatabaseMismatch {
        request: DatabaseId,
        plan: DatabaseId,
    },
    ParticipantDatabaseMismatch {
        expected: DatabaseId,
        actual: DatabaseId,
        participant_id: ParticipantId,
        kind: ParticipantKind,
    },
}

impl fmt::Display for CommitRequestValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoordinatorDatabaseMismatch {
                coordinator,
                request,
            } => write!(
                f,
                "commit coordinator database mismatch: coordinator={} request={}",
                coordinator, request
            ),
            Self::PlanDatabaseMismatch { request, plan } => write!(
                f,
                "commit plan database mismatch: request={} plan={}",
                request, plan
            ),
            Self::ParticipantDatabaseMismatch {
                expected,
                actual,
                participant_id,
                kind,
            } => write!(
                f,
                "commit participant database mismatch: expected={} actual={} participant_id={} kind={:?}",
                expected, actual, participant_id, kind
            ),
        }
    }
}

impl std::error::Error for CommitRequestValidationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    ValidationFailed,
    DurableAppendFailed,
    Backpressure,
    UserRollback,
    CoordinatorShutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedParticipant {
    descriptor: ParticipantDescriptor,
    role: ParticipantRole,
    prepared_bytes: usize,
    write_count: usize,
}

impl PreparedParticipant {
    #[inline]
    pub const fn new(
        descriptor: ParticipantDescriptor,
        required: bool,
        prepared_bytes: usize,
        write_count: usize,
    ) -> Self {
        let role = if required {
            ParticipantRole::Required
        } else {
            ParticipantRole::Deferred
        };
        Self::with_role(descriptor, role, prepared_bytes, write_count)
    }

    #[inline]
    pub const fn with_role(
        descriptor: ParticipantDescriptor,
        role: ParticipantRole,
        prepared_bytes: usize,
        write_count: usize,
    ) -> Self {
        Self {
            descriptor: descriptor.with_role(role),
            role,
            prepared_bytes,
            write_count,
        }
    }

    #[inline]
    pub const fn descriptor(&self) -> &ParticipantDescriptor {
        &self.descriptor
    }

    #[inline]
    pub const fn is_required(&self) -> bool {
        self.role.is_required()
    }

    #[inline]
    pub const fn role(&self) -> ParticipantRole {
        self.role
    }

    #[inline]
    pub const fn prepared_bytes(&self) -> usize {
        self.prepared_bytes
    }

    #[inline]
    pub const fn write_count(&self) -> usize {
        self.write_count
    }
}

pub trait PreparedCommitPart {
    fn prepared_participant(&self) -> &PreparedParticipant;

    #[inline]
    fn descriptor(&self) -> &ParticipantDescriptor {
        self.prepared_participant().descriptor()
    }

    #[inline]
    fn is_required(&self) -> bool {
        self.prepared_participant().is_required()
    }

    #[inline]
    fn prepared_bytes(&self) -> usize {
        self.prepared_participant().prepared_bytes()
    }

    #[inline]
    fn write_count(&self) -> usize {
        self.prepared_participant().write_count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPlan {
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub frozen_read_set: FrozenReadSet,
    pub lock_set: FrozenLockSet,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommitPlan {
    #[inline]
    pub fn from_request(request: &CommitRequest) -> Self {
        Self {
            database_id: request.database_id,
            txn_id: request.txn_id,
            read_ts: request.read_ts,
            isolation: request.isolation,
            frozen_read_set: request.frozen_read_set.clone(),
            lock_set: request.lock_set.clone(),
            participants: request.participants.clone(),
        }
    }

    #[inline]
    pub fn contains_participant(&self, descriptor: &ParticipantDescriptor) -> bool {
        self.participants
            .iter()
            .any(|candidate| candidate == descriptor)
    }

    #[inline]
    pub fn required_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
    }

    #[inline]
    pub fn deferred_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_deferred())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationContext {
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub participant_count: usize,
    pub lock_count: usize,
}

impl ValidationContext {
    #[inline]
    pub fn from_plan(plan: &CommitPlan) -> Self {
        Self {
            read_ts: plan.read_ts,
            isolation: plan.isolation,
            participant_count: plan.participants.len(),
            lock_count: plan.lock_set.held_lock_count(),
        }
    }
}

pub trait CommitParticipant {
    type Prepared: PreparedCommitPart;
    type Error;

    fn prepare(&self, view: &TransactionView) -> std::result::Result<Self::Prepared, Self::Error>;

    fn validate(
        &self,
        plan: &CommitPlan,
        ctx: &ValidationContext,
    ) -> std::result::Result<(), Self::Error>;

    fn descriptor(
        &self,
        prepared: &Self::Prepared,
    ) -> std::result::Result<ParticipantDescriptor, Self::Error>;

    fn abort(&self, reason: AbortReason) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTxnRecord {
    pub record_version: u16,
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommittedTxnRecord {
    #[inline]
    pub fn new(request: &CommitRequest, commit_ts: CommitTs) -> Self {
        Self {
            record_version: COMMITTED_TXN_RECORD_VERSION,
            database_id: request.database_id,
            txn_id: request.txn_id,
            read_ts: request.read_ts,
            commit_ts,
            participants: request.participants.clone(),
        }
    }

    #[inline]
    pub fn required_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
    }

    #[inline]
    pub fn deferred_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_deferred())
    }

    pub fn validate_versions(&self) -> std::result::Result<(), CommitRecordVersionError> {
        if self.record_version != COMMITTED_TXN_RECORD_VERSION {
            return Err(
                CommitRecordVersionError::UnsupportedCommittedRecordVersion {
                    found: self.record_version,
                    expected: COMMITTED_TXN_RECORD_VERSION,
                },
            );
        }
        for participant in &self.participants {
            participant.validate_version()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceRecord {
    pub record_version: u16,
    pub maintenance_id: u64,
}

impl MaintenanceRecord {
    #[inline]
    pub const fn new(maintenance_id: u64) -> Self {
        Self {
            record_version: MAINTENANCE_RECORD_VERSION,
            maintenance_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedRecord {
    Transaction(CommittedTxnRecord),
    Maintenance(MaintenanceRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishResult {
    pub required: bool,
    pub published_ts: Option<CommitTs>,
}

impl PublishResult {
    #[inline]
    pub const fn required(published_ts: CommitTs) -> Self {
        Self {
            required: true,
            published_ts: Some(published_ts),
        }
    }

    #[inline]
    pub const fn deferred() -> Self {
        Self {
            required: false,
            published_ts: None,
        }
    }
}

pub trait CommittedRecordApplier {
    type Error;

    fn applies_to(&self, descriptor: &ParticipantDescriptor) -> bool;

    fn apply_required(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> std::result::Result<PublishResult, Self::Error>;

    fn apply_deferred(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> std::result::Result<PublishResult, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitSequencerOptions {
    pub max_group_commit_batch_size: usize,
    pub max_group_commit_fence_us: u64,
    pub adaptive_batch_sizing: bool,
    pub parallel_fence_groups: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitBackpressureOptions {
    pub max_unpublished_commits: u64,
    pub max_participant_apply_lag: u64,
}

impl Default for CommitBackpressureOptions {
    fn default() -> Self {
        Self {
            max_unpublished_commits: DEFAULT_MAX_UNPUBLISHED_COMMITS,
            max_participant_apply_lag: DEFAULT_MAX_PARTICIPANT_APPLY_LAG,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitBackpressureError {
    GlobalLag {
        durable_ts: CommitTs,
        published_ts: CommitTs,
        lag: u64,
        limit: u64,
    },
    ParticipantLag {
        descriptor: ParticipantDescriptor,
        durable_ts: CommitTs,
        published_ts: CommitTs,
        lag: u64,
        limit: u64,
    },
}

impl fmt::Display for CommitBackpressureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalLag {
                durable_ts,
                published_ts,
                lag,
                limit,
            } => write!(
                f,
                "durable-published lag backpressure: durable_ts={durable_ts} published_ts={published_ts} lag={lag} limit={limit}"
            ),
            Self::ParticipantLag {
                descriptor,
                durable_ts,
                published_ts,
                lag,
                limit,
            } => write!(
                f,
                "participant apply lag backpressure: participant={:?} durable_ts={durable_ts} published_ts={published_ts} lag={lag} limit={limit}",
                descriptor
            ),
        }
    }
}

impl std::error::Error for CommitBackpressureError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitBackpressureSnapshot {
    pub durable_ts: CommitTs,
    pub published_ts: CommitTs,
    pub durable_published_lag: u64,
    pub durable_published_lag_ms: u64,
    pub participant_count: usize,
    pub max_participant_apply_lag: u64,
    pub throttle_count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ParticipantLagState {
    durable_ts: CommitTs,
    published_ts: CommitTs,
}

#[derive(Debug, Default)]
struct CommitBackpressureState {
    participant_lag: HashMap<ParticipantDescriptor, ParticipantLagState>,
}

#[derive(Debug)]
pub struct CommitBackpressureController {
    options: CommitBackpressureOptions,
    durable_ts: AtomicU64,
    published_ts: AtomicU64,
    durable_observed_ms: AtomicU64,
    published_observed_ms: AtomicU64,
    throttle_count: AtomicU64,
    state: Mutex<CommitBackpressureState>,
}

impl CommitBackpressureController {
    pub fn new(options: CommitBackpressureOptions) -> Self {
        Self {
            options,
            durable_ts: AtomicU64::new(0),
            published_ts: AtomicU64::new(0),
            durable_observed_ms: AtomicU64::new(0),
            published_observed_ms: AtomicU64::new(0),
            throttle_count: AtomicU64::new(0),
            state: Mutex::new(CommitBackpressureState::default()),
        }
    }

    #[inline]
    pub fn options(&self) -> CommitBackpressureOptions {
        self.options
    }

    pub fn sync_frontiers(&self, durable_ts: CommitTs, published_ts: CommitTs) {
        fetch_max_relaxed(&self.durable_ts, durable_ts.into_raw());
        fetch_max_relaxed(&self.published_ts, published_ts.into_raw());
        let now = unix_epoch_ms();
        fetch_max_relaxed(&self.durable_observed_ms, now);
        fetch_max_relaxed(&self.published_observed_ms, now);
    }

    pub fn admit(&self, plan: &CommitPlan) -> std::result::Result<(), CommitBackpressureError> {
        let durable_ts = CommitTs::new(self.durable_ts.load(Ordering::Acquire));
        let published_ts = CommitTs::new(self.published_ts.load(Ordering::Acquire));
        let global_lag = durable_ts
            .into_raw()
            .saturating_sub(published_ts.into_raw());
        if self.options.max_unpublished_commits > 0
            && global_lag >= self.options.max_unpublished_commits
        {
            self.throttle_count.fetch_add(1, Ordering::Relaxed);
            return Err(CommitBackpressureError::GlobalLag {
                durable_ts,
                published_ts,
                lag: global_lag,
                limit: self.options.max_unpublished_commits,
            });
        }

        if self.options.max_participant_apply_lag > 0 {
            let state = self.state.lock();
            for descriptor in plan
                .participants
                .iter()
                .filter(|descriptor| descriptor.is_required())
            {
                let Some(lag_state) = state.participant_lag.get(descriptor).copied() else {
                    continue;
                };
                let lag = lag_state
                    .durable_ts
                    .into_raw()
                    .saturating_sub(lag_state.published_ts.into_raw());
                if lag >= self.options.max_participant_apply_lag {
                    self.throttle_count.fetch_add(1, Ordering::Relaxed);
                    return Err(CommitBackpressureError::ParticipantLag {
                        descriptor: descriptor.clone(),
                        durable_ts: lag_state.durable_ts,
                        published_ts: lag_state.published_ts,
                        lag,
                        limit: self.options.max_participant_apply_lag,
                    });
                }
            }
        }
        Ok(())
    }

    pub fn record_durable(&self, commit_ts: CommitTs, participants: &[ParticipantDescriptor]) {
        fetch_max_relaxed(&self.durable_ts, commit_ts.into_raw());
        fetch_max_relaxed(&self.durable_observed_ms, unix_epoch_ms());
        let mut state = self.state.lock();
        for descriptor in participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
        {
            let entry = state.participant_lag.entry(descriptor.clone()).or_default();
            entry.durable_ts = entry.durable_ts.max(commit_ts);
        }
    }

    pub fn record_published(&self, commit_ts: CommitTs, participants: &[ParticipantDescriptor]) {
        fetch_max_relaxed(&self.published_ts, commit_ts.into_raw());
        fetch_max_relaxed(&self.published_observed_ms, unix_epoch_ms());
        let mut state = self.state.lock();
        for descriptor in participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
        {
            let entry = state.participant_lag.entry(descriptor.clone()).or_default();
            entry.published_ts = entry.published_ts.max(commit_ts);
        }
        state
            .participant_lag
            .retain(|_, lag| lag.durable_ts.into_raw() > lag.published_ts.into_raw());
    }

    pub fn snapshot(&self) -> CommitBackpressureSnapshot {
        let durable_ts = CommitTs::new(self.durable_ts.load(Ordering::Acquire));
        let published_ts = CommitTs::new(self.published_ts.load(Ordering::Acquire));
        let state = self.state.lock();
        let max_participant_apply_lag = state
            .participant_lag
            .values()
            .map(|lag| {
                lag.durable_ts
                    .into_raw()
                    .saturating_sub(lag.published_ts.into_raw())
            })
            .max()
            .unwrap_or(0);
        CommitBackpressureSnapshot {
            durable_ts,
            published_ts,
            durable_published_lag: durable_ts
                .into_raw()
                .saturating_sub(published_ts.into_raw()),
            durable_published_lag_ms: if durable_ts > published_ts {
                self.durable_observed_ms
                    .load(Ordering::Acquire)
                    .saturating_sub(self.published_observed_ms.load(Ordering::Acquire))
            } else {
                0
            },
            participant_count: state.participant_lag.len(),
            max_participant_apply_lag,
            throttle_count: self.throttle_count.load(Ordering::Relaxed),
        }
    }
}

fn unix_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

impl Default for CommitSequencerOptions {
    fn default() -> Self {
        Self {
            max_group_commit_batch_size: DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE,
            max_group_commit_fence_us: DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
            adaptive_batch_sizing: false,
            parallel_fence_groups: false,
        }
    }
}

impl CommitSequencerOptions {
    #[inline]
    fn effective_batch_size(self) -> usize {
        self.max_group_commit_batch_size.max(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSequencingPlan {
    pub plan: CommitPlan,
    pub write_set: Vec<LockResource>,
    pub validation_epoch: u64,
    pub ssi_effect_epoch: u64,
    pub estimated_bytes: usize,
}

impl CommitSequencingPlan {
    #[inline]
    pub fn new(plan: CommitPlan, write_set: Vec<LockResource>) -> Self {
        Self {
            plan,
            write_set,
            validation_epoch: 0,
            ssi_effect_epoch: 0,
            estimated_bytes: 0,
        }
    }

    #[inline]
    pub fn from_commit_plan(plan: CommitPlan) -> Self {
        let write_set = write_set_from_lock_set(&plan.lock_set);
        Self::new(plan, write_set)
    }

    #[inline]
    pub const fn with_validation_epoch(mut self, validation_epoch: u64) -> Self {
        self.validation_epoch = validation_epoch;
        self
    }

    #[inline]
    pub const fn with_ssi_effect_epoch(mut self, ssi_effect_epoch: u64) -> Self {
        self.ssi_effect_epoch = ssi_effect_epoch;
        self
    }

    #[inline]
    pub const fn with_estimated_bytes(mut self, estimated_bytes: usize) -> Self {
        self.estimated_bytes = estimated_bytes;
        self
    }
}

fn write_set_from_lock_set(lock_set: &FrozenLockSet) -> Vec<LockResource> {
    lock_set
        .locks()
        .iter()
        .filter(|request| request.mode.is_write_intent())
        .filter(|request| !is_shadowed_table_intent(lock_set, request))
        .map(|request| request.resource.clone())
        .collect()
}

fn is_shadowed_table_intent(lock_set: &FrozenLockSet, request: &LockRequest) -> bool {
    if request.mode != LockMode::IX {
        return false;
    }
    let LockResource::Table {
        namespace,
        table_id,
    } = request.resource
    else {
        return false;
    };

    lock_set.locks().iter().any(|other| {
        other.mode.is_write_intent()
            && !matches!(other.resource, LockResource::Table { .. })
            && other.resource.namespace() == namespace
            && other.resource.table_id() == Some(table_id)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFenceRejectReason {
    BatchSizeLimit,
    FenceBudgetExceeded {
        elapsed_us: u64,
        limit_us: u64,
    },
    InBatchWriteConflict,
    SsiEpochAdvanced {
        validation_epoch: u64,
        batch_effect_epoch: u64,
    },
    SsiStateEpochAdvanced {
        validation_epoch: u64,
        current_epoch: u64,
    },
    CommitTimestampExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedCommitPlan {
    pub plan: CommitSequencingPlan,
    pub reason: CommitFenceRejectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightAcceptedPlan {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub write_set: Vec<LockResource>,
    pub ssi_effect_epoch: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InFlightCommitBatch {
    accepted: Vec<InFlightAcceptedPlan>,
    write_set: Vec<LockResource>,
    max_ssi_effect_epoch: u64,
}

impl InFlightCommitBatch {
    #[inline]
    pub fn accepted(&self) -> &[InFlightAcceptedPlan] {
        &self.accepted
    }

    #[inline]
    pub fn write_set(&self) -> &[LockResource] {
        &self.write_set
    }

    #[inline]
    pub const fn max_ssi_effect_epoch(&self) -> u64 {
        self.max_ssi_effect_epoch
    }

    pub fn reject_reason_for(
        &self,
        plan: &CommitSequencingPlan,
    ) -> Option<CommitFenceRejectReason> {
        if plan.plan.isolation == IsolationLevel::Serializable
            && plan.validation_epoch < self.max_ssi_effect_epoch
        {
            return Some(CommitFenceRejectReason::SsiEpochAdvanced {
                validation_epoch: plan.validation_epoch,
                batch_effect_epoch: self.max_ssi_effect_epoch,
            });
        }
        if self.conflicts_with_write_set(&plan.write_set) {
            return Some(CommitFenceRejectReason::InBatchWriteConflict);
        }
        None
    }

    #[inline]
    pub fn conflicts_with_write_set(&self, write_set: &[LockResource]) -> bool {
        write_set.iter().any(|resource| {
            self.write_set
                .iter()
                .any(|seen| seen.conflicts_with(resource))
        })
    }

    fn accept(&mut self, plan: &CommitSequencingPlan, commit_ts: CommitTs) {
        self.write_set.extend(plan.write_set.iter().cloned());
        self.max_ssi_effect_epoch = self.max_ssi_effect_epoch.max(plan.ssi_effect_epoch);
        self.accepted.push(InFlightAcceptedPlan {
            txn_id: plan.plan.txn_id,
            read_ts: plan.plan.read_ts,
            commit_ts,
            write_set: plan.write_set.clone(),
            ssi_effect_epoch: plan.ssi_effect_epoch,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedCommit {
    pub commit_ts: CommitTs,
    pub plan: CommitSequencingPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedCommitBatch {
    pub accepted: Vec<SequencedCommit>,
    pub rejected: Vec<RejectedCommitPlan>,
    pub fence_duration_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSequencerError<E> {
    Append {
        error: E,
        provisional_start: CommitTs,
        provisional_count: usize,
        accepted: Vec<SequencedCommit>,
        rejected: Vec<RejectedCommitPlan>,
        fence_duration_us: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitSequencerMetrics {
    pub batches: u64,
    pub accepted_plans: u64,
    pub rejected_plans: u64,
    pub append_failures: u64,
    pub fence_duration_us_total: u64,
    pub fence_duration_us_peak: u64,
    pub reject_batch_size_limit: u64,
    pub reject_fence_budget_exceeded: u64,
    pub reject_in_batch_write_conflict: u64,
    pub reject_ssi_epoch_advanced: u64,
    pub reject_commit_timestamp_exhausted: u64,
}

impl CommitSequencerMetrics {
    fn observe_batch(&mut self, accepted: usize, rejected: &[RejectedCommitPlan], fence_us: u64) {
        self.batches = self.batches.saturating_add(1);
        self.accepted_plans = self.accepted_plans.saturating_add(accepted as u64);
        self.rejected_plans = self.rejected_plans.saturating_add(rejected.len() as u64);
        self.fence_duration_us_total = self.fence_duration_us_total.saturating_add(fence_us);
        self.fence_duration_us_peak = self.fence_duration_us_peak.max(fence_us);
        for rejected in rejected {
            self.observe_reject(rejected.reason);
        }
    }

    fn observe_reject(&mut self, reason: CommitFenceRejectReason) {
        match reason {
            CommitFenceRejectReason::BatchSizeLimit => {
                self.reject_batch_size_limit = self.reject_batch_size_limit.saturating_add(1)
            }
            CommitFenceRejectReason::FenceBudgetExceeded { .. } => {
                self.reject_fence_budget_exceeded =
                    self.reject_fence_budget_exceeded.saturating_add(1)
            }
            CommitFenceRejectReason::InBatchWriteConflict => {
                self.reject_in_batch_write_conflict =
                    self.reject_in_batch_write_conflict.saturating_add(1)
            }
            CommitFenceRejectReason::SsiEpochAdvanced { .. }
            | CommitFenceRejectReason::SsiStateEpochAdvanced { .. } => {
                self.reject_ssi_epoch_advanced = self.reject_ssi_epoch_advanced.saturating_add(1)
            }
            CommitFenceRejectReason::CommitTimestampExhausted => {
                self.reject_commit_timestamp_exhausted =
                    self.reject_commit_timestamp_exhausted.saturating_add(1)
            }
        }
    }
}

#[derive(Debug)]
pub struct CommitSequencer {
    options: CommitSequencerOptions,
    state: Mutex<CommitSequencerState>,
}

#[derive(Debug)]
struct CommitSequencerState {
    next_commit_ts: CommitTs,
    metrics: CommitSequencerMetrics,
}

impl CommitSequencer {
    pub fn new(next_commit_ts: CommitTs, options: CommitSequencerOptions) -> Self {
        Self {
            options,
            state: Mutex::new(CommitSequencerState {
                next_commit_ts,
                metrics: CommitSequencerMetrics::default(),
            }),
        }
    }

    #[inline]
    pub fn with_next_commit_ts(next_commit_ts: CommitTs) -> Self {
        Self::new(next_commit_ts, CommitSequencerOptions::default())
    }

    #[inline]
    pub fn next_commit_ts(&self) -> CommitTs {
        self.state.lock().next_commit_ts
    }

    #[inline]
    pub fn metrics_snapshot(&self) -> CommitSequencerMetrics {
        self.state.lock().metrics
    }

    pub fn sync_next_commit_ts_with(&self, min_committed_version: CommitTs) {
        let mut state = self.state.lock();
        if let Some(next) = commit_ts_at(min_committed_version, 1) {
            state.next_commit_ts = state.next_commit_ts.max(next);
        } else {
            state.next_commit_ts = CommitTs::new(u64::MAX);
        }
    }

    pub fn sequence_batch<E>(
        &self,
        plans: impl IntoIterator<Item = CommitSequencingPlan>,
        append: impl FnOnce(&[SequencedCommit]) -> std::result::Result<(), E>,
    ) -> std::result::Result<SequencedCommitBatch, CommitSequencerError<E>> {
        self.sequence_batch_with_fence(plans, |_, _| None, append)
    }

    pub fn sequence_batch_with_fence<E>(
        &self,
        plans: impl IntoIterator<Item = CommitSequencingPlan>,
        mut final_fence: impl FnMut(
            &CommitSequencingPlan,
            &InFlightCommitBatch,
        ) -> Option<CommitFenceRejectReason>,
        append: impl FnOnce(&[SequencedCommit]) -> std::result::Result<(), E>,
    ) -> std::result::Result<SequencedCommitBatch, CommitSequencerError<E>> {
        let mut state = self.state.lock();
        let base_commit_ts = state.next_commit_ts;
        let fence_started_at = Instant::now();
        let mut in_flight = InFlightCommitBatch::default();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let max_batch = self.options.effective_batch_size();

        for plan in plans {
            if accepted.len() >= max_batch {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::BatchSizeLimit,
                });
                continue;
            }

            let elapsed_us = elapsed_us_since(fence_started_at);
            if elapsed_us >= self.options.max_group_commit_fence_us {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::FenceBudgetExceeded {
                        elapsed_us,
                        limit_us: self.options.max_group_commit_fence_us,
                    },
                });
                continue;
            }

            if let Some(reason) = in_flight.reject_reason_for(&plan) {
                rejected.push(RejectedCommitPlan { plan, reason });
                continue;
            }

            if let Some(reason) = final_fence(&plan, &in_flight) {
                rejected.push(RejectedCommitPlan { plan, reason });
                continue;
            }

            let Some(commit_ts) = commit_ts_at(base_commit_ts, accepted.len()) else {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::CommitTimestampExhausted,
                });
                continue;
            };

            in_flight.accept(&plan, commit_ts);
            accepted.push(SequencedCommit { commit_ts, plan });
        }

        let fence_duration_us = elapsed_us_since(fence_started_at);
        if accepted.is_empty() {
            state
                .metrics
                .observe_batch(accepted.len(), &rejected, fence_duration_us);
            return Ok(SequencedCommitBatch {
                accepted,
                rejected,
                fence_duration_us,
            });
        }

        match append(&accepted) {
            Ok(()) => {
                if let Some(next) = commit_ts_at(base_commit_ts, accepted.len()) {
                    state.next_commit_ts = next;
                } else {
                    state.next_commit_ts = CommitTs::new(u64::MAX);
                }
                state
                    .metrics
                    .observe_batch(accepted.len(), &rejected, fence_duration_us);
                Ok(SequencedCommitBatch {
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
            Err(error) => {
                state.metrics.append_failures = state.metrics.append_failures.saturating_add(1);
                state
                    .metrics
                    .observe_batch(accepted.len(), &rejected, fence_duration_us);
                Err(CommitSequencerError::Append {
                    error,
                    provisional_start: base_commit_ts,
                    provisional_count: accepted.len(),
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
        }
    }
}

impl Default for CommitSequencer {
    fn default() -> Self {
        Self::with_next_commit_ts(CommitTs::new(1))
    }
}

#[inline]
fn commit_ts_at(base: CommitTs, offset: usize) -> Option<CommitTs> {
    base.into_raw()
        .checked_add(u64::try_from(offset).ok()?)
        .map(CommitTs::new)
}

#[inline]
fn elapsed_us_since(started_at: Instant) -> u64 {
    started_at.elapsed().as_micros().min(u64::MAX as u128) as u64
}

#[inline]
fn fetch_max_relaxed(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub transaction_view: TransactionView,
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub command_id: CommandId,
    pub ack_policy: CommitAckPolicy,
    pub frozen_read_set: FrozenReadSet,
    pub participant_states: ParticipantStateSet,
    pub lock_set: FrozenLockSet,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommitRequest {
    pub fn new(
        database_id: DatabaseId,
        txn_id: TxnId,
        transaction_view: TransactionView,
        ack_policy: CommitAckPolicy,
        lock_set: FrozenLockSet,
        mut participants: Vec<ParticipantDescriptor>,
    ) -> Self {
        let frozen_read_set = transaction_view.frozen_read_set();
        let participant_states = transaction_view.participant_states().clone();
        for state in participant_states.iter() {
            let descriptor = ParticipantDescriptor::new(
                state.participant_id(),
                state.participant_kind(),
                state.resource_key(),
            );
            if !participants.contains(&descriptor) {
                participants.push(descriptor);
            }
        }
        Self {
            database_id,
            txn_id,
            read_ts: transaction_view.read_ts(),
            isolation: transaction_view.isolation_level(),
            command_id: transaction_view.command_id(),
            transaction_view,
            ack_policy,
            frozen_read_set,
            participant_states,
            lock_set,
            participants,
        }
    }

    #[inline]
    pub fn commit_plan(&self) -> CommitPlan {
        CommitPlan::from_request(self)
    }

    #[inline]
    pub fn validation_context(&self) -> ValidationContext {
        ValidationContext::from_plan(&self.commit_plan())
    }

    #[inline]
    pub fn committed_record(&self, commit_ts: CommitTs) -> CommittedTxnRecord {
        CommittedTxnRecord::new(self, commit_ts)
    }

    #[inline]
    pub fn add_participant(&mut self, participant: ParticipantDescriptor) {
        if !self.participants.contains(&participant) {
            self.participants.push(participant);
        }
    }

    #[inline]
    pub fn add_participants(
        &mut self,
        participants: impl IntoIterator<Item = ParticipantDescriptor>,
    ) {
        for participant in participants {
            self.add_participant(participant);
        }
    }

    #[inline]
    pub fn required_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
    }

    #[inline]
    pub fn deferred_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_deferred())
    }

    pub fn validate_single_database(
        &self,
        coordinator_database_id: DatabaseId,
        plan: Option<&CommitPlan>,
    ) -> std::result::Result<(), CommitRequestValidationError> {
        if self.database_id != coordinator_database_id {
            return Err(CommitRequestValidationError::CoordinatorDatabaseMismatch {
                coordinator: coordinator_database_id,
                request: self.database_id,
            });
        }
        if let Some(plan) = plan {
            if plan.database_id != self.database_id {
                return Err(CommitRequestValidationError::PlanDatabaseMismatch {
                    request: self.database_id,
                    plan: plan.database_id,
                });
            }
        }
        for participant in &self.participants {
            let participant_database_id = participant.resource_key.database_id();
            if participant_database_id != self.database_id {
                return Err(CommitRequestValidationError::ParticipantDatabaseMismatch {
                    expected: self.database_id,
                    actual: participant_database_id,
                    participant_id: participant.participant_id,
                    kind: participant.kind,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitTicket {
    pub commit_ts: CommitTs,
    pub durable_lsn: u64,
    pub durable_batch_lsn: u64,
}

impl CommitTicket {
    #[inline]
    pub const fn new(commit_ts: CommitTs, durable_lsn: u64, durable_batch_lsn: u64) -> Self {
        Self {
            commit_ts,
            durable_lsn,
            durable_batch_lsn,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCoordinatorError<E> {
    InvalidRequest {
        error: CommitRequestValidationError,
    },
    Backpressure {
        error: CommitBackpressureError,
    },
    Rejected {
        rejected: RejectedCommitPlan,
        accepted: Vec<SequencedCommit>,
        fence_duration_us: u64,
    },
    DurableAppend {
        error: E,
        provisional_start: CommitTs,
        provisional_count: usize,
        accepted: Vec<SequencedCommit>,
        rejected: Vec<RejectedCommitPlan>,
        fence_duration_us: u64,
    },
    MissingTicket {
        commit_ts: CommitTs,
    },
    PostDurable {
        ticket: CommitTicket,
        error: E,
    },
    Publish {
        ticket: CommitTicket,
        error: E,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredPublishOutcome {
    Completed,
    Queued,
}

impl RequiredPublishOutcome {
    #[inline]
    pub const fn is_completed(self) -> bool {
        matches!(self, Self::Completed)
    }
}

#[derive(Debug, Clone)]
pub struct CommitCoordinator {
    database_id: DatabaseId,
    sequencer: Arc<CommitSequencer>,
    backpressure: Arc<CommitBackpressureController>,
    completion_lock: Arc<Mutex<()>>,
}

impl CommitCoordinator {
    #[inline]
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            sequencer: Arc::new(CommitSequencer::default()),
            backpressure: Arc::new(CommitBackpressureController::new(
                CommitBackpressureOptions::default(),
            )),
            completion_lock: Arc::new(Mutex::new(())),
        }
    }

    #[inline]
    pub fn with_sequencer(database_id: DatabaseId, sequencer: Arc<CommitSequencer>) -> Self {
        Self::with_sequencer_and_backpressure(
            database_id,
            sequencer,
            Arc::new(CommitBackpressureController::new(
                CommitBackpressureOptions::default(),
            )),
        )
    }

    #[inline]
    pub fn with_sequencer_and_backpressure(
        database_id: DatabaseId,
        sequencer: Arc<CommitSequencer>,
        backpressure: Arc<CommitBackpressureController>,
    ) -> Self {
        Self {
            database_id,
            sequencer,
            backpressure,
            completion_lock: Arc::new(Mutex::new(())),
        }
    }

    #[inline]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    #[inline]
    pub fn sync_commit_ts_with(&self, min_committed_version: CommitTs) {
        self.sequencer
            .sync_next_commit_ts_with(min_committed_version);
    }

    #[inline]
    pub fn next_commit_ts(&self) -> CommitTs {
        self.sequencer.next_commit_ts()
    }

    #[inline]
    pub fn metrics_snapshot(&self) -> CommitSequencerMetrics {
        self.sequencer.metrics_snapshot()
    }

    #[inline]
    pub fn sync_backpressure_frontiers(&self, durable_ts: CommitTs, published_ts: CommitTs) {
        self.backpressure.sync_frontiers(durable_ts, published_ts);
    }

    #[inline]
    pub fn backpressure_snapshot(&self) -> CommitBackpressureSnapshot {
        self.backpressure.snapshot()
    }

    #[inline]
    pub fn mark_required_published(
        &self,
        commit_ts: CommitTs,
        participants: &[ParticipantDescriptor],
    ) {
        self.backpressure.record_published(commit_ts, participants);
    }

    #[allow(clippy::result_large_err)]
    pub fn execute_transaction<E>(
        &self,
        request: &CommitRequest,
        sequencing_plan: CommitSequencingPlan,
        final_fence: impl FnMut(
            &CommitSequencingPlan,
            &InFlightCommitBatch,
        ) -> Option<CommitFenceRejectReason>,
        durable_append: impl FnOnce(CommitTs) -> std::result::Result<CommitTicket, E>,
        post_durable: impl FnOnce(&CommitTicket) -> std::result::Result<(), E>,
        publish_required: impl FnOnce(&CommitTicket) -> std::result::Result<RequiredPublishOutcome, E>,
    ) -> std::result::Result<CommitTicket, CommitCoordinatorError<E>> {
        request
            .validate_single_database(self.database_id, Some(&sequencing_plan.plan))
            .map_err(|error| CommitCoordinatorError::InvalidRequest { error })?;
        debug_assert_eq!(sequencing_plan.plan.txn_id, request.txn_id);
        self.backpressure
            .admit(&sequencing_plan.plan)
            .map_err(|error| CommitCoordinatorError::Backpressure { error })?;

        let completion_guard = self.completion_lock.lock();
        let mut ticket = None;
        let batch = self
            .sequencer
            .sequence_batch_with_fence([sequencing_plan], final_fence, |accepted| {
                let Some(accepted) = accepted.first() else {
                    return Ok(());
                };
                ticket = Some(durable_append(accepted.commit_ts)?);
                Ok(())
            })
            .map_err(|error| match error {
                CommitSequencerError::Append {
                    error,
                    provisional_start,
                    provisional_count,
                    accepted,
                    rejected,
                    fence_duration_us,
                } => CommitCoordinatorError::DurableAppend {
                    error,
                    provisional_start,
                    provisional_count,
                    accepted,
                    rejected,
                    fence_duration_us,
                },
            })?;

        let SequencedCommitBatch {
            accepted,
            rejected,
            fence_duration_us,
        } = batch;

        if let Some(rejected) = rejected.into_iter().next() {
            return Err(CommitCoordinatorError::Rejected {
                rejected,
                accepted,
                fence_duration_us,
            });
        }

        let Some(accepted) = accepted.first() else {
            return Err(CommitCoordinatorError::MissingTicket {
                commit_ts: self.next_commit_ts(),
            });
        };
        let ticket = ticket.ok_or(CommitCoordinatorError::MissingTicket {
            commit_ts: accepted.commit_ts,
        })?;
        self.backpressure
            .record_durable(ticket.commit_ts, &request.participants);
        post_durable(&ticket)
            .map_err(|error| CommitCoordinatorError::PostDurable { ticket, error })?;
        drop(completion_guard);
        let publish_outcome = publish_required(&ticket)
            .map_err(|error| CommitCoordinatorError::Publish { ticket, error })?;
        if publish_outcome.is_completed() {
            self.mark_required_published(ticket.commit_ts, &request.participants);
        }
        Ok(ticket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LockMode, LockNamespace, LockRequest, LockResource, ParticipantStateSet, ReadSnapshot,
        ReadTrackerHandle, TableId, WriterId,
    };

    fn namespace() -> LockNamespace {
        LockNamespace::single_tenant(DatabaseId::new(1))
    }

    fn pk_resource(key_hash: u64) -> LockResource {
        LockResource::primary_key(namespace(), TableId::new(10), 20, key_hash)
    }

    fn table_resource() -> LockResource {
        LockResource::Table {
            namespace: namespace(),
            table_id: TableId::new(10),
        }
    }

    fn plan(txn_id: u64, read_ts: u64, write_set: Vec<LockResource>) -> CommitSequencingPlan {
        let lock_set = FrozenLockSet::from_locks(
            write_set
                .iter()
                .cloned()
                .map(|resource| LockRequest::new(resource, LockMode::X))
                .collect(),
        );
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(txn_id),
            TransactionView::autocommit(ReadTs::new(read_ts)),
            CommitAckPolicy::RequiredPublished,
            lock_set,
            Vec::new(),
        );
        CommitSequencingPlan::new(request.commit_plan(), write_set)
    }

    fn serializable_plan(
        txn_id: u64,
        read_ts: u64,
        write_set: Vec<LockResource>,
    ) -> CommitSequencingPlan {
        let mut plan = plan(txn_id, read_ts, write_set);
        plan.plan.isolation = IsolationLevel::Serializable;
        plan
    }

    #[test]
    fn commit_plan_write_set_drops_shadowed_table_intent() {
        let table = table_resource();
        let key = pk_resource(44);
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(7),
            TransactionView::autocommit(ReadTs::new(3)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::from_locks(vec![
                LockRequest::new(table.clone(), LockMode::IX),
                LockRequest::new(key.clone(), LockMode::X),
            ]),
            Vec::new(),
        );

        let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());

        assert_eq!(plan.write_set, vec![key]);
    }

    #[test]
    fn commit_plan_write_set_keeps_table_intent_without_finer_write() {
        let table = table_resource();
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(7),
            TransactionView::autocommit(ReadTs::new(3)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::from_locks(vec![LockRequest::new(table.clone(), LockMode::IX)]),
            Vec::new(),
        );

        let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());

        assert_eq!(plan.write_set, vec![table]);
    }

    #[test]
    fn commit_request_carries_fixed_semantic_fields() {
        let tracker = ReadTrackerHandle::recording();
        let view = TransactionView::new(
            WriterId::new(42),
            ReadTs::new(12),
            ReadSnapshot::without_lease(ReadTs::new(11)),
            IsolationLevel::Snapshot,
            CommandId::new(3),
            tracker,
            ParticipantStateSet::empty(),
        );
        view.read_tracker().record_table_read(TableId::new(99));
        view.read_tracker()
            .record_tablet_read(TableId::new(99), 7, ReadTs::new(11), 3, 2);
        let lock = LockRequest::new(
            LockResource::Table {
                namespace: LockNamespace::single_tenant(DatabaseId::new(7)),
                table_id: TableId::new(99),
            },
            LockMode::IX,
        );

        let request = CommitRequest::new(
            DatabaseId::new(7),
            TxnId::new(42),
            view,
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::from_locks(vec![lock]),
            Vec::new(),
        );

        assert_eq!(request.database_id, DatabaseId::new(7));
        assert_eq!(request.txn_id, TxnId::new(42));
        assert_eq!(request.read_ts, ReadTs::new(11));
        assert_eq!(request.command_id.into_raw(), 3);
        assert_eq!(request.frozen_read_set.dependency_count(), 2);
        assert_eq!(request.frozen_read_set.storage_snapshot_count(), 1);
        assert_eq!(request.lock_set.held_lock_count(), 1);
        assert!(request.participant_states.is_empty());
        let plan = request.commit_plan();
        let ctx = ValidationContext::from_plan(&plan);
        assert_eq!(ctx.read_ts, ReadTs::new(11));
        assert_eq!(ctx.participant_count, 0);
    }

    #[test]
    fn coordinator_sequences_durable_ticket_and_post_durable_hook() {
        let view = TransactionView::autocommit(ReadTs::new(50));
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(100),
            view,
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );

        let coordinator = CommitCoordinator::new(DatabaseId::new(9));
        let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());
        let mut post_durable_seen = false;
        let ticket = coordinator
            .execute_transaction(
                &request,
                plan,
                |_, _| None,
                |commit_ts| Ok::<_, ()>(CommitTicket::new(commit_ts, 7, 9)),
                |ticket| {
                    assert_eq!(ticket.commit_ts, CommitTs::new(1));
                    post_durable_seen = true;
                    Ok::<_, ()>(())
                },
                |_| Ok::<_, ()>(RequiredPublishOutcome::Completed),
            )
            .unwrap();

        assert_eq!(ticket, CommitTicket::new(CommitTs::new(1), 7, 9));
        assert!(post_durable_seen);
        assert_eq!(coordinator.next_commit_ts(), CommitTs::new(2));
    }

    #[test]
    fn committed_record_carries_versions_and_participants() {
        let descriptor = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
        );
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(100),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![descriptor.clone()],
        );

        let record = request.committed_record(CommitTs::new(77));
        assert_eq!(record.record_version, COMMITTED_TXN_RECORD_VERSION);
        assert_eq!(record.commit_ts, CommitTs::new(77));
        assert_eq!(record.participants, vec![descriptor]);
        assert_eq!(
            record.participants[0].descriptor_version,
            PARTICIPANT_DESCRIPTOR_VERSION
        );
        record
            .validate_versions()
            .expect("fresh committed record versions are supported");
    }

    #[test]
    fn committed_record_rejects_unsupported_versions() {
        let descriptor = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
        );
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(100),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![descriptor],
        );

        let mut bad_record = request.committed_record(CommitTs::new(77));
        bad_record.record_version = COMMITTED_TXN_RECORD_VERSION + 1;
        assert!(matches!(
            bad_record.validate_versions(),
            Err(CommitRecordVersionError::UnsupportedCommittedRecordVersion { .. })
        ));

        let mut bad_participant = request.committed_record(CommitTs::new(78));
        bad_participant.participants[0].descriptor_version = PARTICIPANT_DESCRIPTOR_VERSION + 1;
        assert!(matches!(
            bad_participant.validate_versions(),
            Err(CommitRecordVersionError::UnsupportedParticipantDescriptorVersion { .. })
        ));
    }

    #[test]
    fn participant_roles_split_required_and_deferred_sets() {
        let required = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
        );
        let deferred = ParticipantDescriptor::new(
            ParticipantId::new(2),
            ParticipantKind::Search,
            TxnResourceKey::table(
                ParticipantKind::Search,
                DatabaseId::new(9),
                TableId::new(10),
            ),
        )
        .deferred();
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(101),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![required.clone(), deferred.clone()],
        );

        assert_eq!(request.required_participants().count(), 1);
        assert_eq!(request.deferred_participants().count(), 1);
        let record = request.committed_record(CommitTs::new(77));
        assert_eq!(
            record.required_participants().cloned().collect::<Vec<_>>(),
            vec![required]
        );
        assert_eq!(
            record.deferred_participants().cloned().collect::<Vec<_>>(),
            vec![deferred]
        );
    }

    #[test]
    fn commit_request_rejects_cross_database_participant() {
        let participant = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(10)),
        );
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(100),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![participant],
        );

        let err = request
            .validate_single_database(DatabaseId::new(9), Some(&request.commit_plan()))
            .unwrap_err();

        match err {
            CommitRequestValidationError::ParticipantDatabaseMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, DatabaseId::new(9));
                assert_eq!(actual, DatabaseId::new(10));
            }
            other => panic!("unexpected validation error: {other:?}"),
        }
    }

    #[test]
    fn coordinator_rejects_request_for_different_database() {
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(100),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );
        let coordinator = CommitCoordinator::new(DatabaseId::new(10));
        let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());

        let err = coordinator
            .execute_transaction(
                &request,
                plan,
                |_, _| None,
                |_| Ok::<_, ()>(CommitTicket::new(CommitTs::new(1), 1, 1)),
                |_| Ok::<_, ()>(()),
                |_| Ok::<_, ()>(RequiredPublishOutcome::Completed),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            CommitCoordinatorError::InvalidRequest {
                error: CommitRequestValidationError::CoordinatorDatabaseMismatch { .. }
            }
        ));
    }

    #[test]
    fn backpressure_rejects_global_and_required_participant_lag() {
        let controller = CommitBackpressureController::new(CommitBackpressureOptions {
            max_unpublished_commits: 2,
            max_participant_apply_lag: 1,
        });
        let required = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
        );
        let deferred = ParticipantDescriptor::new(
            ParticipantId::new(2),
            ParticipantKind::Search,
            TxnResourceKey::table(
                ParticipantKind::Search,
                DatabaseId::new(9),
                TableId::new(10),
            ),
        )
        .deferred();
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(102),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![required.clone(), deferred.clone()],
        );
        let plan = request.commit_plan();

        controller.record_durable(CommitTs::new(1), &[required.clone(), deferred]);
        assert!(matches!(
            controller.admit(&plan),
            Err(CommitBackpressureError::ParticipantLag { lag: 1, .. })
        ));
        controller.record_published(CommitTs::new(1), &[required]);
        assert!(controller.admit(&plan).is_ok());

        controller.sync_frontiers(CommitTs::new(3), CommitTs::new(1));
        assert!(matches!(
            controller.admit(&plan),
            Err(CommitBackpressureError::GlobalLag { lag: 2, .. })
        ));
        assert_eq!(controller.snapshot().throttle_count, 2);
    }

    #[test]
    fn queued_required_publish_keeps_backpressure_lag_until_hook_marks_published() {
        let required = ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
        );
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(103),
            TransactionView::autocommit(ReadTs::new(50)),
            CommitAckPolicy::DurableOnlyAsync,
            FrozenLockSet::empty(),
            vec![required],
        );
        let coordinator = CommitCoordinator::new(DatabaseId::new(9));
        let ticket = coordinator
            .execute_transaction(
                &request,
                CommitSequencingPlan::from_commit_plan(request.commit_plan()),
                |_, _| None,
                |commit_ts| Ok::<_, ()>(CommitTicket::new(commit_ts, 1, 1)),
                |_| Ok::<_, ()>(()),
                |_| Ok::<_, ()>(RequiredPublishOutcome::Queued),
            )
            .unwrap();

        let queued = coordinator.backpressure_snapshot();
        assert_eq!(queued.durable_ts, CommitTs::new(1));
        assert_eq!(queued.published_ts, CommitTs::new(0));
        assert_eq!(queued.max_participant_apply_lag, 1);

        coordinator.mark_required_published(ticket.commit_ts, &request.participants);
        let published = coordinator.backpressure_snapshot();
        assert_eq!(published.published_ts, CommitTs::new(1));
        assert_eq!(published.max_participant_apply_lag, 0);
    }

    #[test]
    fn coordinator_checks_backpressure_before_allocating_commit_ts() {
        let backpressure = Arc::new(CommitBackpressureController::new(
            CommitBackpressureOptions {
                max_unpublished_commits: 1,
                max_participant_apply_lag: 1,
            },
        ));
        backpressure.sync_frontiers(CommitTs::new(10), CommitTs::new(9));
        let coordinator = CommitCoordinator::with_sequencer_and_backpressure(
            DatabaseId::new(9),
            Arc::new(CommitSequencer::new(
                CommitTs::new(11),
                CommitSequencerOptions::default(),
            )),
            backpressure,
        );
        let request = CommitRequest::new(
            DatabaseId::new(9),
            TxnId::new(103),
            TransactionView::autocommit(ReadTs::new(9)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );

        let err = coordinator
            .execute_transaction(
                &request,
                CommitSequencingPlan::from_commit_plan(request.commit_plan()),
                |_, _| None,
                |commit_ts| Ok::<_, ()>(CommitTicket::new(commit_ts, 1, 1)),
                |_| Ok::<_, ()>(()),
                |_| Ok::<_, ()>(RequiredPublishOutcome::Completed),
            )
            .unwrap_err();
        assert!(matches!(err, CommitCoordinatorError::Backpressure { .. }));
        assert_eq!(coordinator.next_commit_ts(), CommitTs::new(11));
    }

    #[test]
    fn commit_sequencer_advances_only_after_append_success() {
        let sequencer = CommitSequencer::new(CommitTs::new(10), CommitSequencerOptions::default());
        let first = plan(1, 9, vec![pk_resource(1)]);
        let err = sequencer
            .sequence_batch(vec![first], |_| Err::<(), _>("append failed"))
            .unwrap_err();
        assert_eq!(sequencer.next_commit_ts(), CommitTs::new(10));
        let CommitSequencerError::Append {
            provisional_start,
            provisional_count,
            ..
        } = err;
        assert_eq!(provisional_start, CommitTs::new(10));
        assert_eq!(provisional_count, 1);

        let second = plan(2, 9, vec![pk_resource(2)]);
        let batch = sequencer
            .sequence_batch(vec![second], |_| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(10));
        assert_eq!(sequencer.next_commit_ts(), CommitTs::new(11));
        assert_eq!(sequencer.metrics_snapshot().append_failures, 1);
    }

    #[test]
    fn in_flight_batch_rejects_later_conflicting_write() {
        let sequencer = CommitSequencer::new(CommitTs::new(20), CommitSequencerOptions::default());
        let first = plan(1, 18, vec![pk_resource(7)]);
        let second = plan(2, 18, vec![pk_resource(7)]);
        let batch = sequencer
            .sequence_batch(vec![first, second], |accepted| {
                assert_eq!(accepted.len(), 1);
                Ok::<_, ()>(())
            })
            .unwrap();

        assert_eq!(batch.accepted.len(), 1);
        assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(20));
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(
            batch.rejected[0].reason,
            CommitFenceRejectReason::InBatchWriteConflict
        );
        let metrics = sequencer.metrics_snapshot();
        assert_eq!(metrics.reject_in_batch_write_conflict, 1);
    }

    #[test]
    fn group_commit_batch_size_limit_is_hard_cap() {
        let options = CommitSequencerOptions {
            max_group_commit_batch_size: 1,
            ..CommitSequencerOptions::default()
        };
        let sequencer = CommitSequencer::new(CommitTs::new(30), options);
        let batch = sequencer
            .sequence_batch(
                vec![
                    plan(1, 29, vec![pk_resource(1)]),
                    plan(2, 29, vec![pk_resource(2)]),
                ],
                |_| Ok::<_, ()>(()),
            )
            .unwrap();

        assert_eq!(batch.accepted.len(), 1);
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(
            batch.rejected[0].reason,
            CommitFenceRejectReason::BatchSizeLimit
        );
        assert_eq!(sequencer.next_commit_ts(), CommitTs::new(31));
    }

    #[test]
    fn group_commit_fence_budget_records_reject_reason() {
        let options = CommitSequencerOptions {
            max_group_commit_fence_us: 0,
            ..CommitSequencerOptions::default()
        };
        let sequencer = CommitSequencer::new(CommitTs::new(40), options);
        let batch = sequencer
            .sequence_batch(
                vec![plan(1, 39, vec![pk_resource(1)])],
                |_| -> std::result::Result<(), ()> {
                    panic!("append must not run when fence rejects all plans")
                },
            )
            .unwrap();

        assert!(batch.accepted.is_empty());
        assert_eq!(batch.rejected.len(), 1);
        assert!(matches!(
            batch.rejected[0].reason,
            CommitFenceRejectReason::FenceBudgetExceeded { limit_us: 0, .. }
        ));
        assert_eq!(sequencer.next_commit_ts(), CommitTs::new(40));
        assert_eq!(sequencer.metrics_snapshot().reject_fence_budget_exceeded, 1);
    }

    #[test]
    fn in_flight_batch_rejects_stale_ssi_epoch() {
        let sequencer = CommitSequencer::new(CommitTs::new(50), CommitSequencerOptions::default());
        let first = serializable_plan(1, 49, vec![pk_resource(1)])
            .with_validation_epoch(10)
            .with_ssi_effect_epoch(11);
        let second = serializable_plan(2, 49, vec![pk_resource(2)]).with_validation_epoch(10);

        let batch = sequencer
            .sequence_batch(vec![first, second], |_| Ok::<_, ()>(()))
            .unwrap();

        assert_eq!(batch.accepted.len(), 1);
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(
            batch.rejected[0].reason,
            CommitFenceRejectReason::SsiEpochAdvanced {
                validation_epoch: 10,
                batch_effect_epoch: 11
            }
        );
    }

    #[test]
    fn in_flight_ssi_epoch_does_not_reject_snapshot_plan() {
        let sequencer = CommitSequencer::new(CommitTs::new(55), CommitSequencerOptions::default());
        let first = serializable_plan(1, 54, vec![pk_resource(1)])
            .with_validation_epoch(10)
            .with_ssi_effect_epoch(11);
        let second = plan(2, 54, vec![pk_resource(2)]).with_validation_epoch(10);

        let batch = sequencer
            .sequence_batch(vec![first, second], |_| Ok::<_, ()>(()))
            .unwrap();

        assert_eq!(batch.accepted.len(), 2);
        assert!(batch.rejected.is_empty());
    }

    #[test]
    fn external_final_fence_runs_before_commit_timestamp_assignment() {
        let sequencer = CommitSequencer::new(CommitTs::new(60), CommitSequencerOptions::default());
        let first = serializable_plan(1, 59, vec![pk_resource(1)]).with_validation_epoch(12);
        let second = serializable_plan(2, 59, vec![pk_resource(2)]).with_validation_epoch(12);

        let batch = sequencer
            .sequence_batch_with_fence(
                vec![first, second],
                |plan, in_flight| {
                    if plan.plan.txn_id == TxnId::new(2) {
                        assert_eq!(in_flight.accepted().len(), 1);
                        return Some(CommitFenceRejectReason::SsiStateEpochAdvanced {
                            validation_epoch: plan.validation_epoch,
                            current_epoch: 13,
                        });
                    }
                    None
                },
                |accepted| {
                    assert_eq!(accepted.len(), 1);
                    Ok::<_, ()>(())
                },
            )
            .unwrap();

        assert_eq!(batch.accepted.len(), 1);
        assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(60));
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(
            batch.rejected[0].reason,
            CommitFenceRejectReason::SsiStateEpochAdvanced {
                validation_epoch: 12,
                current_epoch: 13
            }
        );
        assert_eq!(sequencer.next_commit_ts(), CommitTs::new(61));
        assert_eq!(sequencer.metrics_snapshot().reject_ssi_epoch_advanced, 1);
    }
}
