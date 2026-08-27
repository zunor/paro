// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sharded pessimistic lock manager skeleton.

use crate::sync::Mutex;
use crate::types::{DatabaseId, TableId, TxnId};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LockNamespace {
    pub tenant_id: u64,
    pub database_id: DatabaseId,
}

impl LockNamespace {
    pub const fn new(tenant_id: u64, database_id: DatabaseId) -> Self {
        Self {
            tenant_id,
            database_id,
        }
    }

    pub const fn single_tenant(database_id: DatabaseId) -> Self {
        Self::new(0, database_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockResource {
    Database {
        namespace: LockNamespace,
    },
    Schema {
        namespace: LockNamespace,
        schema_id: u64,
    },
    Table {
        namespace: LockNamespace,
        table_id: TableId,
    },
    Tablet {
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
    },
    PrimaryKey {
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
        key_hash: u64,
    },
    RowId {
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
        rowset_id: u64,
        segment_id: u32,
        row_offset: u32,
    },
    Range {
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
        start_hash: u64,
        end_hash: u64,
    },
    Predicate {
        namespace: LockNamespace,
        table_id: TableId,
        predicate_hash: u64,
    },
    CatalogObject {
        namespace: LockNamespace,
        object_kind: u16,
        object_id: u64,
    },
}

impl LockResource {
    pub fn primary_key(
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
        key_hash: u64,
    ) -> Self {
        Self::PrimaryKey {
            namespace,
            table_id,
            tablet_id,
            key_hash,
        }
    }

    pub fn row_id(
        namespace: LockNamespace,
        table_id: TableId,
        tablet_id: u64,
        rowset_id: u64,
        segment_id: u32,
        row_offset: u32,
    ) -> Self {
        Self::RowId {
            namespace,
            table_id,
            tablet_id,
            rowset_id,
            segment_id,
            row_offset,
        }
    }

    pub fn conflicts_with(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        if self.namespace() != other.namespace() {
            return false;
        }

        match (self, other) {
            (Self::Database { .. }, _) | (_, Self::Database { .. }) => true,
            (
                Self::Table { table_id: left, .. },
                Self::Table {
                    table_id: right, ..
                },
            ) => left == right,
            (Self::Table { table_id, .. }, other) | (other, Self::Table { table_id, .. }) => {
                other.table_id() == Some(*table_id)
            }
            (
                Self::Tablet {
                    table_id: lt,
                    tablet_id: left,
                    ..
                },
                Self::Tablet {
                    table_id: rt,
                    tablet_id: right,
                    ..
                },
            ) => lt == rt && left == right,
            (
                Self::Tablet {
                    table_id,
                    tablet_id,
                    ..
                },
                other,
            )
            | (
                other,
                Self::Tablet {
                    table_id,
                    tablet_id,
                    ..
                },
            ) => other.tablet_identity() == Some((*table_id, *tablet_id)),
            (
                Self::PrimaryKey {
                    table_id: lt,
                    tablet_id: ltab,
                    key_hash,
                    ..
                },
                Self::Range {
                    table_id: rt,
                    tablet_id: rtab,
                    start_hash,
                    end_hash,
                    ..
                },
            )
            | (
                Self::Range {
                    table_id: rt,
                    tablet_id: rtab,
                    start_hash,
                    end_hash,
                    ..
                },
                Self::PrimaryKey {
                    table_id: lt,
                    tablet_id: ltab,
                    key_hash,
                    ..
                },
            ) => lt == rt && ltab == rtab && hash_in_range(*key_hash, *start_hash, *end_hash),
            (
                Self::Range {
                    table_id: lt,
                    tablet_id: ltab,
                    start_hash: ls,
                    end_hash: le,
                    ..
                },
                Self::Range {
                    table_id: rt,
                    tablet_id: rtab,
                    start_hash: rs,
                    end_hash: re,
                    ..
                },
            ) => lt == rt && ltab == rtab && ranges_overlap(*ls, *le, *rs, *re),
            (Self::Predicate { table_id: left, .. }, other)
            | (other, Self::Predicate { table_id: left, .. }) => other.table_id() == Some(*left),
            (
                Self::Schema {
                    schema_id: left, ..
                },
                Self::Schema {
                    schema_id: right, ..
                },
            ) => left == right,
            (
                Self::CatalogObject {
                    object_kind: lk,
                    object_id: li,
                    ..
                },
                Self::CatalogObject {
                    object_kind: rk,
                    object_id: ri,
                    ..
                },
            ) => lk == rk && li == ri,
            _ => false,
        }
    }

    pub fn namespace(&self) -> LockNamespace {
        match self {
            Self::Database { namespace }
            | Self::Schema { namespace, .. }
            | Self::Table { namespace, .. }
            | Self::Tablet { namespace, .. }
            | Self::PrimaryKey { namespace, .. }
            | Self::RowId { namespace, .. }
            | Self::Range { namespace, .. }
            | Self::Predicate { namespace, .. }
            | Self::CatalogObject { namespace, .. } => *namespace,
        }
    }

    pub fn table_id(&self) -> Option<TableId> {
        match self {
            Self::Table { table_id, .. }
            | Self::Tablet { table_id, .. }
            | Self::PrimaryKey { table_id, .. }
            | Self::RowId { table_id, .. }
            | Self::Range { table_id, .. }
            | Self::Predicate { table_id, .. } => Some(*table_id),
            Self::Database { .. } | Self::Schema { .. } | Self::CatalogObject { .. } => None,
        }
    }

    pub fn tablet_identity(&self) -> Option<(TableId, u64)> {
        match self {
            Self::Tablet {
                table_id,
                tablet_id,
                ..
            }
            | Self::PrimaryKey {
                table_id,
                tablet_id,
                ..
            }
            | Self::RowId {
                table_id,
                tablet_id,
                ..
            }
            | Self::Range {
                table_id,
                tablet_id,
                ..
            } => Some((*table_id, *tablet_id)),
            _ => None,
        }
    }

    pub fn conflict_domain(&self) -> (LockNamespace, Option<TableId>) {
        (self.namespace(), self.table_id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LockMode {
    IS = 1,
    IX = 2,
    S = 3,
    X = 4,
    RangeS = 5,
    RangeX = 6,
    PredicateX = 7,
    SchemaStability = 8,
    SchemaModification = 9,
}

impl LockMode {
    pub fn compatible_with(self, other: Self) -> bool {
        use LockMode::*;
        matches!(
            (self, other),
            (IS, IS | IX | S | RangeS | SchemaStability)
                | (IX, IS | IX | SchemaStability)
                | (S, IS | S | RangeS | SchemaStability)
                | (RangeS, IS | S | RangeS | SchemaStability)
                | (SchemaStability, IS | IX | S | RangeS | SchemaStability)
        )
    }

    pub const fn is_write_intent(self) -> bool {
        matches!(
            self,
            Self::IX | Self::X | Self::RangeX | Self::PredicateX | Self::SchemaModification
        )
    }

    /// Whether this lock identifies a resource actually modified by commit.
    ///
    /// `IX` is deliberately excluded: it is an ancestor routing declaration,
    /// not evidence that the ancestor object itself changed. Treating it as a
    /// commit write serializes independent row/key writers with maintenance
    /// publications which only share the same table intent.
    pub const fn is_commit_write(self) -> bool {
        matches!(
            self,
            Self::X | Self::RangeX | Self::PredicateX | Self::SchemaModification
        )
    }

    fn strength(self) -> u8 {
        match self {
            Self::IS => 1,
            Self::IX => 2,
            Self::S | Self::RangeS | Self::SchemaStability => 3,
            Self::X | Self::RangeX | Self::PredicateX | Self::SchemaModification => 4,
        }
    }

    pub fn strongest(self, other: Self) -> Self {
        if self.strength() >= other.strength() {
            self
        } else {
            other
        }
    }
}

/// Return whether two grants conflict after accounting for multi-granularity
/// intent locks.  `LockMode::compatible_with` is the same-resource matrix; an
/// intent lock on an ancestor is deliberately compatible with locks on its
/// descendants.  Treating the ancestor IX as an ordinary peer of a child X
/// serializes every writer on the table and defeats the purpose of intent
/// locking.
fn lock_grants_conflict(
    held_resource: &LockResource,
    held_mode: LockMode,
    requested_resource: &LockResource,
    requested_mode: LockMode,
) -> bool {
    if !held_resource.conflicts_with(requested_resource) {
        return false;
    }
    if held_resource != requested_resource
        && (proper_ancestor_intent(held_resource, held_mode, requested_resource)
            || proper_ancestor_intent(requested_resource, requested_mode, held_resource))
    {
        return false;
    }
    !held_mode.compatible_with(requested_mode) || !requested_mode.compatible_with(held_mode)
}

fn proper_ancestor_intent(
    ancestor: &LockResource,
    mode: LockMode,
    descendant: &LockResource,
) -> bool {
    if !matches!(
        mode,
        LockMode::IS | LockMode::IX | LockMode::SchemaStability
    ) {
        return false;
    }
    match ancestor {
        LockResource::Database { .. } => ancestor.namespace() == descendant.namespace(),
        LockResource::Table { table_id, .. } => {
            descendant.table_id() == Some(*table_id)
                && !matches!(descendant, LockResource::Table { .. })
        }
        LockResource::Tablet {
            table_id,
            tablet_id,
            ..
        } => {
            descendant.tablet_identity() == Some((*table_id, *tablet_id))
                && !matches!(descendant, LockResource::Tablet { .. })
        }
        LockResource::Schema { .. }
        | LockResource::PrimaryKey { .. }
        | LockResource::RowId { .. }
        | LockResource::Range { .. }
        | LockResource::Predicate { .. }
        | LockResource::CatalogObject { .. } => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRequest {
    pub resource: LockResource,
    pub mode: LockMode,
}

impl LockRequest {
    pub fn new(resource: LockResource, mode: LockMode) -> Self {
        Self { resource, mode }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShardedLockManagerOptions {
    pub shard_count: usize,
    pub lock_escalation_threshold: usize,
    pub lock_escalation_failure_action: LockEscalationFailureAction,
}

impl Default for ShardedLockManagerOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            lock_escalation_threshold: 1024,
            lock_escalation_failure_action: LockEscalationFailureAction::KeepFineGrained,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockAcquireError {
    WouldWait {
        blockers: Vec<TxnId>,
    },
    WouldWound {
        victims: Vec<TxnId>,
    },
    WouldWoundAndWait {
        victims: Vec<TxnId>,
        blockers: Vec<TxnId>,
    },
}

impl LockAcquireError {
    fn has_wait_blockers(&self) -> bool {
        match self {
            Self::WouldWait { blockers } | Self::WouldWoundAndWait { blockers, .. } => {
                !blockers.is_empty()
            }
            Self::WouldWound { .. } => false,
        }
    }

    fn has_wound_victims(&self) -> bool {
        match self {
            Self::WouldWound { victims } | Self::WouldWoundAndWait { victims, .. } => {
                !victims.is_empty()
            }
            Self::WouldWait { .. } => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockManagerStats {
    pub shard_count: usize,
    pub lock_count: usize,
    pub granted_count: usize,
    pub lock_wait_count: u64,
    pub lock_wait_duration_us: u64,
    pub lock_wound_wait_abort_count: u64,
    pub lock_deadlock_abort_count: u64,
}

/// Lock-free counters suitable for statement-path telemetry snapshots.
///
/// `LockManagerStats` intentionally includes the exact number of live lock
/// resources and holders, which requires visiting every shard.  Most runtime
/// observers only need contention counters and must not serialize foreground
/// statements on the lock table merely to collect them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockManagerContentionStats {
    pub lock_wait_count: u64,
    pub lock_wait_duration_us: u64,
    pub lock_wound_wait_abort_count: u64,
    pub lock_deadlock_abort_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockEscalationFailureAction {
    KeepFineGrained,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockEscalationPolicy {
    pub threshold: usize,
    pub failure_action: LockEscalationFailureAction,
}

impl LockEscalationPolicy {
    pub const fn disabled() -> Self {
        Self {
            threshold: usize::MAX,
            failure_action: LockEscalationFailureAction::KeepFineGrained,
        }
    }

    pub const fn try_tablet(threshold: usize) -> Self {
        Self {
            threshold,
            failure_action: LockEscalationFailureAction::KeepFineGrained,
        }
    }

    pub const fn abort_on_failure(threshold: usize) -> Self {
        Self {
            threshold,
            failure_action: LockEscalationFailureAction::Abort,
        }
    }

    fn enabled(self) -> bool {
        self.threshold != usize::MAX
    }
}

#[derive(Debug, Clone)]
pub struct ShardedLockManager {
    inner: Arc<ShardedLockManagerInner>,
}

#[derive(Debug)]
struct ShardedLockManagerInner {
    shards: Box<[Mutex<LockTableShard>]>,
    coarse: Mutex<LockTableShard>,
    coarse_active_count: AtomicUsize,
    coarse_epoch: AtomicU64,
    lock_wait_count: AtomicU64,
    lock_wait_duration_us: AtomicU64,
    lock_wound_wait_abort_count: AtomicU64,
    lock_deadlock_abort_count: AtomicU64,
    escalation_policy: LockEscalationPolicy,
}

#[derive(Debug, Default)]
struct LockTableShard {
    locks: HashMap<LockResource, LockState>,
}

#[derive(Debug, Default)]
struct LockState {
    granted: Vec<LockHolder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockHolder {
    txn_id: TxnId,
    mode: LockMode,
    ref_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantOutcome {
    inserted_holder: bool,
    previous_mode: Option<LockMode>,
}

#[derive(Debug)]
pub struct TxnLockSet {
    manager: Arc<ShardedLockManagerInner>,
    txn_id: TxnId,
    locks: Vec<GrantedLock>,
    released: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrantedLock {
    resource: LockResource,
    mode: LockMode,
    location: LockLocation,
    previous_mode: Option<LockMode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockLocation {
    FineShard(usize),
    Coarse,
}

impl ShardedLockManager {
    pub fn new(options: ShardedLockManagerOptions) -> Self {
        assert!(options.shard_count > 0, "lock manager needs shards");
        let shards = (0..options.shard_count)
            .map(|_| Mutex::new(LockTableShard::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            inner: Arc::new(ShardedLockManagerInner {
                shards,
                coarse: Mutex::new(LockTableShard::default()),
                coarse_active_count: AtomicUsize::new(0),
                coarse_epoch: AtomicU64::new(0),
                lock_wait_count: AtomicU64::new(0),
                lock_wait_duration_us: AtomicU64::new(0),
                lock_wound_wait_abort_count: AtomicU64::new(0),
                lock_deadlock_abort_count: AtomicU64::new(0),
                escalation_policy: LockEscalationPolicy {
                    threshold: options.lock_escalation_threshold,
                    failure_action: options.lock_escalation_failure_action,
                },
            }),
        }
    }

    pub fn with_shards(shard_count: usize) -> Self {
        Self::new(ShardedLockManagerOptions {
            shard_count,
            ..ShardedLockManagerOptions::default()
        })
    }

    pub fn lock_many(
        &self,
        txn_id: TxnId,
        requests: impl IntoIterator<Item = LockRequest>,
    ) -> std::result::Result<TxnLockSet, LockAcquireError> {
        let requests = normalize_requests(requests);
        let mut granted = Vec::with_capacity(requests.len());

        for request in requests {
            match self.inner.try_grant(txn_id, &request) {
                Ok(mut locks) => granted.append(&mut locks),
                Err(err) => {
                    self.inner.release_locks(txn_id, &granted);
                    self.inner.record_acquire_error(&err);
                    return Err(err);
                }
            }
        }

        let mut lock_set = TxnLockSet {
            manager: self.inner.clone(),
            txn_id,
            locks: granted,
            released: false,
        };
        if let Err(err) = self.inner.try_escalate(&mut lock_set) {
            self.inner.record_acquire_error(&err);
            return Err(err);
        }
        Ok(lock_set)
    }

    pub fn lock_one(
        &self,
        txn_id: TxnId,
        resource: LockResource,
        mode: LockMode,
    ) -> std::result::Result<TxnLockSet, LockAcquireError> {
        self.lock_many(txn_id, [LockRequest::new(resource, mode)])
    }

    pub fn stats(&self) -> LockManagerStats {
        let mut lock_count = 0;
        let mut granted_count = 0;
        for shard in self.inner.shards.iter() {
            let shard = shard.lock();
            lock_count += shard.locks.len();
            granted_count += shard.granted_count();
        }
        {
            let coarse = self.inner.coarse.lock();
            lock_count += coarse.locks.len();
            granted_count += coarse.granted_count();
        }
        LockManagerStats {
            shard_count: self.inner.shards.len(),
            lock_count,
            granted_count,
            lock_wait_count: self.inner.lock_wait_count.load(Ordering::Acquire),
            lock_wait_duration_us: self.inner.lock_wait_duration_us.load(Ordering::Acquire),
            lock_wound_wait_abort_count: self
                .inner
                .lock_wound_wait_abort_count
                .load(Ordering::Acquire),
            lock_deadlock_abort_count: self.inner.lock_deadlock_abort_count.load(Ordering::Acquire),
        }
    }

    #[inline]
    pub fn contention_stats(&self) -> LockManagerContentionStats {
        LockManagerContentionStats {
            lock_wait_count: self.inner.lock_wait_count.load(Ordering::Acquire),
            lock_wait_duration_us: self.inner.lock_wait_duration_us.load(Ordering::Acquire),
            lock_wound_wait_abort_count: self
                .inner
                .lock_wound_wait_abort_count
                .load(Ordering::Acquire),
            lock_deadlock_abort_count: self.inner.lock_deadlock_abort_count.load(Ordering::Acquire),
        }
    }

    pub fn lock_escalation_threshold(&self) -> usize {
        self.inner.escalation_policy.threshold
    }

    pub fn lock_escalation_policy(&self) -> LockEscalationPolicy {
        self.inner.escalation_policy
    }

    pub fn has_lock_conflicting_with(&self, resource: &LockResource) -> bool {
        self.inner.has_lock_conflicting_with(resource)
    }
}

impl Default for ShardedLockManager {
    fn default() -> Self {
        Self::new(ShardedLockManagerOptions::default())
    }
}

impl TxnLockSet {
    pub fn len(&self) -> usize {
        self.locks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locks.is_empty()
    }

    pub fn lock_requests(&self) -> Vec<LockRequest> {
        normalize_requests(
            self.locks
                .iter()
                .map(|lock| LockRequest::new(lock.resource.clone(), lock.mode)),
        )
    }

    pub fn mode_for(&self, resource: &LockResource) -> Option<LockMode> {
        self.locks
            .iter()
            .filter(|lock| &lock.resource == resource)
            .map(|lock| lock.mode)
            .reduce(LockMode::strongest)
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.manager.release_locks(self.txn_id, &self.locks);
        self.released = true;
    }
}

impl Drop for TxnLockSet {
    fn drop(&mut self) {
        self.release_inner();
    }
}

impl ShardedLockManagerInner {
    fn try_grant(
        &self,
        txn_id: TxnId,
        request: &LockRequest,
    ) -> std::result::Result<Vec<GrantedLock>, LockAcquireError> {
        if Self::is_coarse_resource(&request.resource) {
            self.try_grant_coarse(txn_id, request)
        } else {
            self.try_grant_fine(txn_id, request)
        }
    }

    fn record_acquire_error(&self, err: &LockAcquireError) {
        if err.has_wait_blockers() {
            self.lock_wait_count.fetch_add(1, Ordering::Relaxed);
        }
        if err.has_wound_victims() {
            self.lock_wound_wait_abort_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn release_locks(&self, txn_id: TxnId, locks: &[GrantedLock]) {
        for lock in locks.iter().rev() {
            match lock.location {
                LockLocation::FineShard(shard_index) => {
                    let mut shard = self.shards[shard_index].lock();
                    shard.release(txn_id, &lock.resource, lock.previous_mode);
                }
                LockLocation::Coarse => {
                    self.release_coarse_lock(txn_id, &lock.resource, lock.previous_mode)
                }
            }
        }
    }

    fn shard_index(&self, resource: &LockResource) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        resource.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn try_grant_fine(
        &self,
        txn_id: TxnId,
        request: &LockRequest,
    ) -> std::result::Result<Vec<GrantedLock>, LockAcquireError> {
        let coarse_epoch = self.coarse_epoch.load(Ordering::Acquire);
        if self.coarse_active_count.load(Ordering::Acquire) > 0 {
            let blockers = self.coarse_blockers(txn_id, request);
            if !blockers.is_empty() {
                return Err(classify_wound_wait(txn_id, blockers));
            }
        }

        let shard_index = self.shard_index(&request.resource);
        let mut shard = self.shards[shard_index].lock();

        if self.coarse_epoch.load(Ordering::Acquire) != coarse_epoch
            || self.coarse_active_count.load(Ordering::Acquire) > 0
        {
            let blockers = self.coarse_blockers(txn_id, request);
            if !blockers.is_empty() {
                return Err(classify_wound_wait(txn_id, blockers));
            }
        }

        let blockers = shard.blockers(txn_id, request);
        if !blockers.is_empty() {
            return Err(classify_wound_wait(txn_id, blockers));
        }

        let outcome = shard.grant(txn_id, request);
        Ok(vec![GrantedLock {
            resource: request.resource.clone(),
            mode: request.mode,
            location: LockLocation::FineShard(shard_index),
            previous_mode: outcome.previous_mode,
        }])
    }

    fn try_grant_coarse(
        &self,
        txn_id: TxnId,
        request: &LockRequest,
    ) -> std::result::Result<Vec<GrantedLock>, LockAcquireError> {
        let grant_outcome = {
            let mut coarse = self.coarse.lock();
            let blockers = coarse.blockers(txn_id, request);
            if !blockers.is_empty() {
                return Err(classify_wound_wait(txn_id, blockers));
            }
            let outcome = coarse.grant(txn_id, request);
            if outcome.inserted_holder {
                self.coarse_active_count.fetch_add(1, Ordering::AcqRel);
            }
            self.coarse_epoch.fetch_add(1, Ordering::AcqRel);
            outcome
        };

        let blockers = self.fine_blockers(txn_id, request);
        if !blockers.is_empty() {
            self.release_coarse_lock(txn_id, &request.resource, grant_outcome.previous_mode);
            return Err(classify_wound_wait(txn_id, blockers));
        }

        Ok(vec![GrantedLock {
            resource: request.resource.clone(),
            mode: request.mode,
            location: LockLocation::Coarse,
            previous_mode: grant_outcome.previous_mode,
        }])
    }

    fn coarse_blockers(&self, txn_id: TxnId, request: &LockRequest) -> Vec<TxnId> {
        if self.coarse_active_count.load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        let coarse = self.coarse.lock();
        coarse.blockers(txn_id, request)
    }

    fn fine_blockers(&self, txn_id: TxnId, request: &LockRequest) -> Vec<TxnId> {
        let mut blockers = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.lock();
            blockers.extend(shard.blockers(txn_id, request));
        }
        normalize_txn_ids(&mut blockers);
        blockers
    }

    fn release_coarse_lock(
        &self,
        txn_id: TxnId,
        resource: &LockResource,
        previous_mode: Option<LockMode>,
    ) {
        let removed_holder = {
            let mut coarse = self.coarse.lock();
            coarse.release(txn_id, resource, previous_mode)
        };
        if removed_holder {
            self.coarse_active_count.fetch_sub(1, Ordering::AcqRel);
        }
        self.coarse_epoch.fetch_add(1, Ordering::AcqRel);
    }

    fn is_coarse_resource(resource: &LockResource) -> bool {
        matches!(
            resource,
            LockResource::Database { .. }
                | LockResource::Schema { .. }
                | LockResource::Table { .. }
                | LockResource::Tablet { .. }
                | LockResource::Range { .. }
                | LockResource::Predicate { .. }
        )
    }

    fn has_lock_conflicting_with(&self, resource: &LockResource) -> bool {
        if self.coarse_active_count.load(Ordering::Acquire) > 0 {
            let coarse = self.coarse.lock();
            if coarse.has_conflicting(resource) {
                return true;
            }
        }

        if !Self::is_coarse_resource(resource) {
            let shard = self.shards[self.shard_index(resource)].lock();
            return shard.has_conflicting(resource);
        }

        self.shards.iter().any(|shard| {
            let shard = shard.lock();
            shard.has_conflicting(resource)
        })
    }

    fn try_escalate(&self, lock_set: &mut TxnLockSet) -> std::result::Result<(), LockAcquireError> {
        let policy = self.escalation_policy;
        if !policy.enabled() || lock_set.locks.len() < policy.threshold {
            return Ok(());
        }

        let mut counts: HashMap<(LockNamespace, TableId, u64), usize> = HashMap::new();
        for lock in &lock_set.locks {
            if let Some(identity) = fine_tablet_identity(&lock.resource) {
                *counts.entry(identity).or_default() += 1;
            }
        }

        for ((namespace, table_id, tablet_id), count) in counts {
            if count < policy.threshold
                || has_tablet_lock(&lock_set.locks, namespace, table_id, tablet_id)
            {
                continue;
            }
            let tablet_resource = LockResource::Tablet {
                namespace,
                table_id,
                tablet_id,
            };
            let request = LockRequest::new(tablet_resource.clone(), LockMode::X);
            match self.try_grant(lock_set.txn_id, &request) {
                Ok(mut coarse_locks) => {
                    let mut fine_locks = Vec::new();
                    lock_set.locks.retain(|lock| {
                        let is_fine = fine_tablet_identity(&lock.resource)
                            == Some((namespace, table_id, tablet_id));
                        if is_fine {
                            fine_locks.push(lock.clone());
                        }
                        !is_fine
                    });
                    self.release_locks(lock_set.txn_id, &fine_locks);
                    lock_set.locks.append(&mut coarse_locks);
                }
                Err(err) if policy.failure_action == LockEscalationFailureAction::Abort => {
                    return Err(err);
                }
                Err(_) => {}
            }
        }

        Ok(())
    }
}

fn fine_tablet_identity(resource: &LockResource) -> Option<(LockNamespace, TableId, u64)> {
    match resource {
        LockResource::PrimaryKey {
            namespace,
            table_id,
            tablet_id,
            ..
        }
        | LockResource::RowId {
            namespace,
            table_id,
            tablet_id,
            ..
        } => Some((*namespace, *table_id, *tablet_id)),
        _ => None,
    }
}

fn has_tablet_lock(
    locks: &[GrantedLock],
    namespace: LockNamespace,
    table_id: TableId,
    tablet_id: u64,
) -> bool {
    locks.iter().any(|lock| {
        matches!(
            lock.resource,
            LockResource::Tablet {
                namespace: held_namespace,
                table_id: held_table_id,
                tablet_id: held_tablet_id
            } if held_namespace == namespace
                && held_table_id == table_id
                && held_tablet_id == tablet_id
        )
    })
}

impl LockTableShard {
    fn granted_count(&self) -> usize {
        self.locks
            .values()
            .map(|state| state.granted.len())
            .sum::<usize>()
    }

    fn blockers(&self, txn_id: TxnId, request: &LockRequest) -> Vec<TxnId> {
        let mut blockers = Vec::new();
        for (resource, state) in &self.locks {
            if !resource.conflicts_with(&request.resource) {
                continue;
            }
            for holder in &state.granted {
                if holder.txn_id == txn_id {
                    continue;
                }
                if lock_grants_conflict(resource, holder.mode, &request.resource, request.mode) {
                    blockers.push(holder.txn_id);
                }
            }
        }
        normalize_txn_ids(&mut blockers);
        blockers
    }

    fn grant(&mut self, txn_id: TxnId, request: &LockRequest) -> GrantOutcome {
        let state = self.locks.entry(request.resource.clone()).or_default();
        if let Some(holder) = state
            .granted
            .iter_mut()
            .find(|holder| holder.txn_id == txn_id)
        {
            let previous_mode = holder.mode;
            holder.mode = holder.mode.strongest(request.mode);
            holder.ref_count = holder.ref_count.saturating_add(1);
            return GrantOutcome {
                inserted_holder: false,
                previous_mode: Some(previous_mode),
            };
        }
        state.granted.push(LockHolder {
            txn_id,
            mode: request.mode,
            ref_count: 1,
        });
        GrantOutcome {
            inserted_holder: true,
            previous_mode: None,
        }
    }

    fn release(
        &mut self,
        txn_id: TxnId,
        resource: &LockResource,
        previous_mode: Option<LockMode>,
    ) -> bool {
        let Some(state) = self.locks.get_mut(resource) else {
            return false;
        };
        for holder in &mut state.granted {
            if holder.txn_id == txn_id && holder.ref_count > 1 {
                holder.ref_count -= 1;
                if let Some(previous_mode) = previous_mode {
                    holder.mode = previous_mode;
                }
                return false;
            }
        }
        let before = state.granted.len();
        state.granted.retain(|holder| holder.txn_id != txn_id);
        let removed_holder = state.granted.len() != before;
        if state.granted.is_empty() {
            self.locks.remove(resource);
        }
        removed_holder
    }

    fn has_conflicting(&self, resource: &LockResource) -> bool {
        self.locks
            .iter()
            .any(|(held, state)| held.conflicts_with(resource) && !state.granted.is_empty())
    }
}

fn classify_wound_wait(txn_id: TxnId, mut blockers: Vec<TxnId>) -> LockAcquireError {
    normalize_txn_ids(&mut blockers);
    let requester = txn_id.into_raw();
    let mut victims = Vec::new();
    let mut wait_blockers = Vec::new();
    for blocker in blockers {
        if requester < blocker.into_raw() {
            victims.push(blocker);
        } else {
            wait_blockers.push(blocker);
        }
    }

    match (victims.is_empty(), wait_blockers.is_empty()) {
        (false, true) => LockAcquireError::WouldWound { victims },
        (true, false) => LockAcquireError::WouldWait {
            blockers: wait_blockers,
        },
        (false, false) => LockAcquireError::WouldWoundAndWait {
            victims,
            blockers: wait_blockers,
        },
        (true, true) => LockAcquireError::WouldWait {
            blockers: Vec::new(),
        },
    }
}

fn normalize_txn_ids(txn_ids: &mut Vec<TxnId>) {
    txn_ids.sort_unstable();
    txn_ids.dedup();
}

fn normalize_requests(requests: impl IntoIterator<Item = LockRequest>) -> Vec<LockRequest> {
    let mut requests = requests.into_iter().collect::<Vec<_>>();
    requests.sort_by(|left, right| left.resource.cmp(&right.resource));
    let mut normalized: Vec<LockRequest> = Vec::with_capacity(requests.len());
    for request in requests {
        if let Some(last) = normalized.last_mut() {
            if last.resource == request.resource {
                last.mode = last.mode.strongest(request.mode);
                continue;
            }
        }
        normalized.push(request);
    }
    normalized
}

fn hash_in_range(hash: u64, start: u64, end: u64) -> bool {
    if start <= end {
        start <= hash && hash <= end
    } else {
        hash >= start || hash <= end
    }
}

fn ranges_overlap(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    hash_in_range(left_start, right_start, right_end)
        || hash_in_range(left_end, right_start, right_end)
        || hash_in_range(right_start, left_start, left_end)
        || hash_in_range(right_end, left_start, left_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns() -> LockNamespace {
        LockNamespace::single_tenant(DatabaseId::new(1))
    }

    fn pk(key_hash: u64) -> LockResource {
        LockResource::primary_key(ns(), TableId::new(10), 20, key_hash)
    }

    #[test]
    fn shared_locks_are_compatible() {
        let manager = ShardedLockManager::with_shards(4);
        let first = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::S)
            .unwrap();
        let second = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::S)
            .unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(manager.stats().granted_count, 2);
    }

    #[test]
    fn younger_conflicting_txn_waits_for_older_owner() {
        let manager = ShardedLockManager::with_shards(4);
        let _owner = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::X)
            .unwrap_err();
        assert_eq!(
            err,
            LockAcquireError::WouldWait {
                blockers: vec![TxnId::new(10)]
            }
        );
    }

    #[test]
    fn older_conflicting_txn_reports_wound_victim() {
        let manager = ShardedLockManager::with_shards(4);
        let _owner = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::X)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap_err();
        assert_eq!(
            err,
            LockAcquireError::WouldWound {
                victims: vec![TxnId::new(11)]
            }
        );
    }

    #[test]
    fn mixed_age_blockers_report_wounds_and_waits_separately() {
        let manager = ShardedLockManager::with_shards(4);
        let _older = manager.lock_one(TxnId::new(5), pk(1), LockMode::S).unwrap();
        let _younger = manager
            .lock_one(TxnId::new(15), pk(1), LockMode::S)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap_err();
        assert_eq!(
            err,
            LockAcquireError::WouldWoundAndWait {
                victims: vec![TxnId::new(15)],
                blockers: vec![TxnId::new(5)]
            }
        );
    }

    #[test]
    fn lock_manager_stats_track_wait_and_wound_rejections() {
        let manager = ShardedLockManager::with_shards(4);
        let _older = manager.lock_one(TxnId::new(5), pk(1), LockMode::S).unwrap();
        let _younger = manager
            .lock_one(TxnId::new(15), pk(1), LockMode::S)
            .unwrap();

        let _ = manager
            .lock_one(TxnId::new(20), pk(1), LockMode::X)
            .unwrap_err();
        let stats = manager.stats();
        assert_eq!(stats.lock_wait_count, 1);
        assert_eq!(stats.lock_wound_wait_abort_count, 0);

        let _ = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap_err();
        let stats = manager.stats();
        assert_eq!(stats.lock_wait_count, 2);
        assert_eq!(stats.lock_wound_wait_abort_count, 1);
        assert_eq!(stats.lock_wait_duration_us, 0);
        assert_eq!(stats.lock_deadlock_abort_count, 0);
    }

    #[test]
    fn lock_many_rolls_back_partial_grants_on_conflict() {
        let manager = ShardedLockManager::with_shards(4);
        let _owner = manager
            .lock_one(TxnId::new(10), pk(2), LockMode::X)
            .unwrap();

        let err = manager
            .lock_many(
                TxnId::new(11),
                [
                    LockRequest::new(pk(1), LockMode::X),
                    LockRequest::new(pk(2), LockMode::X),
                ],
            )
            .unwrap_err();

        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
        assert_eq!(manager.stats().granted_count, 1);
    }

    #[test]
    fn range_lock_conflicts_with_key_in_range() {
        let manager = ShardedLockManager::with_shards(1);
        let range = LockResource::Range {
            namespace: ns(),
            table_id: TableId::new(10),
            tablet_id: 20,
            start_hash: 10,
            end_hash: 20,
        };
        let _owner = manager
            .lock_one(TxnId::new(10), range, LockMode::RangeX)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(11), pk(15), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn table_lock_conflicts_with_sharded_key_lock() {
        let manager = ShardedLockManager::with_shards(8);
        let _owner = manager
            .lock_one(TxnId::new(10), pk(15), LockMode::X)
            .unwrap();
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };

        let err = manager
            .lock_one(TxnId::new(11), table, LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
        assert_eq!(manager.stats().granted_count, 1);
    }

    #[test]
    fn table_intent_locks_allow_disjoint_child_writers() {
        let manager = ShardedLockManager::with_shards(8);
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };
        let _first_intent = manager
            .lock_one(TxnId::new(10), table.clone(), LockMode::IX)
            .unwrap();
        let _second_intent = manager
            .lock_one(TxnId::new(11), table, LockMode::IX)
            .unwrap();

        let _first_key = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap();
        let _second_key = manager
            .lock_one(TxnId::new(11), pk(2), LockMode::X)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn table_shared_lock_still_blocks_child_writer() {
        let manager = ShardedLockManager::with_shards(8);
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };
        let _reader = manager
            .lock_one(TxnId::new(10), table, LockMode::S)
            .unwrap();

        let err = manager
            .lock_one(TxnId::new(11), pk(2), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn coarse_locks_are_not_replicated_to_every_fine_shard() {
        let manager = ShardedLockManager::with_shards(8);
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };

        let lock = manager
            .lock_one(TxnId::new(10), table.clone(), LockMode::X)
            .unwrap();

        assert_eq!(lock.len(), 1);
        assert_eq!(manager.stats().lock_count, 1);
        assert_eq!(manager.stats().granted_count, 1);

        let err = manager
            .lock_one(TxnId::new(11), pk(42), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn duplicate_lock_sets_release_by_reference_count() {
        let manager = ShardedLockManager::with_shards(4);
        let first = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap();
        let second = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap();

        drop(second);
        let err = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));

        drop(first);
        let third = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::X)
            .unwrap();
        assert_eq!(third.len(), 1);
    }

    #[test]
    fn nested_stronger_lock_release_restores_previous_mode() {
        let manager = ShardedLockManager::with_shards(4);
        let shared = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::S)
            .unwrap();
        let exclusive = manager
            .lock_one(TxnId::new(10), pk(1), LockMode::X)
            .unwrap();

        drop(exclusive);
        let compatible_shared = manager
            .lock_one(TxnId::new(11), pk(1), LockMode::S)
            .unwrap();
        let err = manager
            .lock_one(TxnId::new(12), pk(1), LockMode::X)
            .unwrap_err();

        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
        drop(compatible_shared);
        drop(shared);
    }

    #[test]
    fn schema_stability_allows_dml_intent_but_blocks_schema_modification() {
        let manager = ShardedLockManager::with_shards(4);
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };
        let schema = LockResource::Schema {
            namespace: ns(),
            schema_id: 7,
        };
        let _dml = manager
            .lock_many(
                TxnId::new(10),
                [
                    LockRequest::new(table, LockMode::IX),
                    LockRequest::new(schema.clone(), LockMode::SchemaStability),
                ],
            )
            .unwrap();

        let compatible = manager
            .lock_one(TxnId::new(11), schema.clone(), LockMode::SchemaStability)
            .unwrap();
        assert_eq!(compatible.len(), 1);

        let err = manager
            .lock_one(TxnId::new(12), schema, LockMode::SchemaModification)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn fine_grained_locks_escalate_to_tablet_lock() {
        let manager = ShardedLockManager::new(ShardedLockManagerOptions {
            shard_count: 4,
            lock_escalation_threshold: 2,
            ..ShardedLockManagerOptions::default()
        });
        let locks = manager
            .lock_many(
                TxnId::new(10),
                [
                    LockRequest::new(pk(1), LockMode::X),
                    LockRequest::new(pk(2), LockMode::X),
                ],
            )
            .unwrap();

        assert!(locks
            .locks
            .iter()
            .any(|lock| matches!(lock.resource, LockResource::Tablet { .. })));
        let err = manager
            .lock_one(TxnId::new(11), pk(3), LockMode::X)
            .unwrap_err();
        assert!(matches!(err, LockAcquireError::WouldWait { .. }));
    }

    #[test]
    fn coarse_scan_preserves_mixed_wound_wait_semantics() {
        let manager = ShardedLockManager::with_shards(8);
        let _older = manager.lock_one(TxnId::new(5), pk(1), LockMode::S).unwrap();
        let _younger = manager
            .lock_one(TxnId::new(15), pk(2), LockMode::S)
            .unwrap();
        let table = LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        };

        let err = manager
            .lock_one(TxnId::new(10), table, LockMode::X)
            .unwrap_err();
        assert_eq!(
            err,
            LockAcquireError::WouldWoundAndWait {
                victims: vec![TxnId::new(15)],
                blockers: vec![TxnId::new(5)]
            }
        );
        assert_eq!(manager.stats().granted_count, 2);
    }
}
