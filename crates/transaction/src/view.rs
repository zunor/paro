// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction read view and read dependency recording skeleton.

use crate::read_dependency_index::{
    IndexedReadTracker, ReadDependencyIndex, ReadDependencyIndexMark, ReadDependencyRollback,
};
use crate::sync::Mutex;
use crate::{
    CommandId, FrozenReadSet, IsolationLevel, LockResource, ParticipantStateSet, ReadSnapshotLease,
    ReadTs, TableId, TxnResourceKey, WriterId,
};
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct ReadSnapshot {
    read_ts: ReadTs,
    lease: Option<Arc<ReadSnapshotLease>>,
}

impl ReadSnapshot {
    pub fn new(read_ts: ReadTs, lease: Option<Arc<ReadSnapshotLease>>) -> Self {
        Self { read_ts, lease }
    }

    pub fn without_lease(read_ts: ReadTs) -> Self {
        Self::new(read_ts, None)
    }

    #[inline]
    pub fn read_ts(&self) -> ReadTs {
        self.read_ts
    }

    #[inline]
    pub fn lease(&self) -> Option<Arc<ReadSnapshotLease>> {
        self.lease.clone()
    }

    /// Logical committed-version upper bound for this snapshot.
    ///
    /// A row/catalog version is visible when `commit_ts <= read_ts`. This is
    /// intentionally not `start_time - 1`; use `ReadTs::visible_before_start()`
    /// only when the value represents a transaction start timestamp.
    #[inline]
    pub fn visible_version(&self) -> u64 {
        self.read_ts.into_raw()
    }

    #[inline]
    pub fn visible_version_i64(&self) -> i64 {
        i64::try_from(self.visible_version()).unwrap_or(i64::MAX)
    }
}

impl std::fmt::Debug for ReadSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("read_ts", &self.read_ts)
            .field("has_lease", &self.lease.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsOfTimestampError {
    BeforeGcWatermark {
        requested: ReadTs,
        oldest_available: ReadTs,
    },
}

impl fmt::Display for AsOfTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeGcWatermark {
                requested,
                oldest_available,
            } => write!(
                f,
                "AS OF timestamp {requested} is older than the oldest retained snapshot {oldest_available}",
            ),
        }
    }
}

impl std::error::Error for AsOfTimestampError {}

#[inline]
pub fn validate_as_of_timestamp(
    requested: ReadTs,
    oldest_available: ReadTs,
) -> std::result::Result<(), AsOfTimestampError> {
    if requested < oldest_available {
        return Err(AsOfTimestampError::BeforeGcWatermark {
            requested,
            oldest_available,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadTrackingPolicy {
    #[default]
    Noop,
    Record,
    Serializable,
    PointCritical,
    RangeCritical,
    AnalyticalScan,
    SafeSnapshotPreferred,
    SafeSnapshot,
}

impl ReadTrackingPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Record => "record",
            Self::Serializable => "serializable",
            Self::PointCritical => "point_critical",
            Self::RangeCritical => "range_critical",
            Self::AnalyticalScan => "analytical_scan",
            Self::SafeSnapshotPreferred => "safe_snapshot_preferred",
            Self::SafeSnapshot => "safe_snapshot",
        }
    }

    pub fn from_user_hint(value: &str) -> Option<Self> {
        let normalized = value
            .trim()
            .trim_matches(|ch| ch == '\'' || ch == '"')
            .to_ascii_lowercase()
            .replace('-', "_");
        match normalized.as_str() {
            "point" | "point_critical" | "exact_point" => Some(Self::PointCritical),
            "range" | "range_critical" | "exact_range" => Some(Self::RangeCritical),
            "analytical" | "analytical_scan" | "coarse_scan" => Some(Self::AnalyticalScan),
            "safe_snapshot" | "safe_snapshot_preferred" | "safe" => {
                Some(Self::SafeSnapshotPreferred)
            }
            _ => None,
        }
    }

    #[inline]
    pub const fn is_read_only_hint(self) -> bool {
        matches!(self, Self::SafeSnapshotPreferred | Self::AnalyticalScan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadWritePromotion {
    Promoted,
    MustRestartImplicitOk,
    MustRestartUserVisible,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReadDependency {
    Table {
        table_id: TableId,
    },
    Row {
        table_id: TableId,
        row_id: u64,
    },
    Tablet {
        table_id: TableId,
        tablet_id: u64,
        read_ts: ReadTs,
        layout_epoch: u64,
        rowset_count: usize,
    },
    Rowset {
        table_id: TableId,
        tablet_id: u64,
        rowset_id: u64,
        read_ts: ReadTs,
        layout_epoch: u64,
    },
    KeyRange {
        table_id: TableId,
        start_hash: u64,
        end_hash: u64,
    },
    Predicate {
        table_id: TableId,
        predicate_hash: u64,
    },
    AnalyticalScan {
        table_id: TableId,
    },
    Generation {
        resource_key: TxnResourceKey,
        generation: u64,
    },
}

impl ReadDependency {
    #[inline]
    pub const fn table_id(&self) -> Option<TableId> {
        match self {
            Self::Table { table_id }
            | Self::Row { table_id, .. }
            | Self::Tablet { table_id, .. }
            | Self::Rowset { table_id, .. }
            | Self::KeyRange { table_id, .. }
            | Self::Predicate { table_id, .. }
            | Self::AnalyticalScan { table_id } => Some(*table_id),
            Self::Generation { resource_key, .. } => resource_key.table_id(),
        }
    }

    #[inline]
    pub fn table_marker(&self) -> Option<Self> {
        match self {
            Self::AnalyticalScan { table_id } => Some(Self::AnalyticalScan {
                table_id: *table_id,
            }),
            _ => self.table_id().map(|table_id| Self::Table { table_id }),
        }
    }

    #[inline]
    pub const fn is_coarse_scan_marker(&self) -> bool {
        matches!(self, Self::AnalyticalScan { .. })
    }

    #[inline]
    pub const fn estimated_bytes(&self) -> usize {
        match self {
            Self::Table { .. } => 32,
            Self::Row { .. } => 40,
            Self::Tablet { .. } => 56,
            Self::Rowset { .. } => 56,
            Self::KeyRange { .. } => 48,
            Self::Predicate { .. } => 40,
            Self::AnalyticalScan { .. } => 32,
            Self::Generation { .. } => 48,
        }
    }

    pub fn conflicts_with_write(&self, write: &LockResource) -> bool {
        match self {
            Self::Table { table_id } => write_matches_table(write, *table_id),
            Self::Row { table_id, .. } => write_matches_table(write, *table_id),
            Self::Tablet {
                table_id,
                tablet_id,
                ..
            } => write_matches_tablet(write, *table_id, *tablet_id),
            Self::Rowset {
                table_id,
                tablet_id,
                rowset_id,
                ..
            } => write_matches_rowset(write, *table_id, *tablet_id, *rowset_id),
            Self::KeyRange {
                table_id,
                start_hash,
                end_hash,
            } => write_matches_key_range(write, *table_id, *start_hash, *end_hash),
            Self::Predicate { table_id, .. } | Self::AnalyticalScan { table_id } => {
                write_matches_table(write, *table_id)
            }
            Self::Generation { resource_key, .. } => {
                if let Some(table_id) = resource_key.table_id() {
                    write_matches_table(write, table_id)
                } else {
                    matches!(
                        write,
                        LockResource::Database { .. } | LockResource::CatalogObject { .. }
                    )
                }
            }
        }
    }
}

fn write_matches_table(write: &LockResource, table_id: TableId) -> bool {
    matches!(write, LockResource::Database { .. }) || write.table_id() == Some(table_id)
}

fn write_matches_tablet(write: &LockResource, table_id: TableId, tablet_id: u64) -> bool {
    match write {
        LockResource::Database { .. } => true,
        LockResource::Table {
            table_id: write_table,
            ..
        }
        | LockResource::Predicate {
            table_id: write_table,
            ..
        } => *write_table == table_id,
        LockResource::Tablet {
            table_id: write_table,
            tablet_id: write_tablet,
            ..
        }
        | LockResource::PrimaryKey {
            table_id: write_table,
            tablet_id: write_tablet,
            ..
        }
        | LockResource::RowId {
            table_id: write_table,
            tablet_id: write_tablet,
            ..
        }
        | LockResource::Range {
            table_id: write_table,
            tablet_id: write_tablet,
            ..
        } => *write_table == table_id && *write_tablet == tablet_id,
        LockResource::Schema { .. } | LockResource::CatalogObject { .. } => false,
    }
}

fn write_matches_rowset(
    write: &LockResource,
    table_id: TableId,
    tablet_id: u64,
    rowset_id: u64,
) -> bool {
    match write {
        LockResource::RowId {
            table_id: write_table,
            tablet_id: write_tablet,
            rowset_id: write_rowset,
            ..
        } => *write_table == table_id && *write_tablet == tablet_id && *write_rowset == rowset_id,
        other => write_matches_tablet(other, table_id, tablet_id),
    }
}

fn write_matches_key_range(
    write: &LockResource,
    table_id: TableId,
    start_hash: u64,
    end_hash: u64,
) -> bool {
    match write {
        LockResource::PrimaryKey {
            table_id: write_table,
            key_hash,
            ..
        } => *write_table == table_id && hash_in_range(*key_hash, start_hash, end_hash),
        LockResource::Range {
            table_id: write_table,
            start_hash: write_start,
            end_hash: write_end,
            ..
        } => {
            *write_table == table_id
                && ranges_overlap(start_hash, end_hash, *write_start, *write_end)
        }
        other => write_matches_table(other, table_id),
    }
}

fn hash_in_range(value: u64, start: u64, end: u64) -> bool {
    if start <= end {
        value >= start && value <= end
    } else {
        value >= start || value <= end
    }
}

fn ranges_overlap(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    hash_in_range(left_start, right_start, right_end)
        || hash_in_range(left_end, right_start, right_end)
        || hash_in_range(right_start, left_start, left_end)
        || hash_in_range(right_end, left_start, left_end)
}

pub trait ReadRecorder: Send + Sync + std::fmt::Debug {
    fn record(&self, dependency: ReadDependency);

    fn record_batch(&self, dependencies: &[ReadDependency]) {
        for dependency in dependencies {
            self.record(dependency.clone());
        }
    }

    fn frozen_read_set(&self) -> FrozenReadSet;
}

#[derive(Debug, Default)]
pub struct RecordingReadTracker {
    dependencies: Mutex<Vec<ReadDependency>>,
}

impl RecordingReadTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, dependency: ReadDependency) {
        self.dependencies.lock().push(dependency);
    }

    pub fn dependencies(&self) -> Vec<ReadDependency> {
        self.dependencies.lock().clone()
    }

    pub fn mark_savepoint(&self) -> usize {
        self.dependencies.lock().len()
    }

    pub fn rollback_to_savepoint(&self, dependency_count: usize) {
        let mut dependencies = self.dependencies.lock();
        if dependency_count < dependencies.len() {
            dependencies.truncate(dependency_count);
        }
    }

    pub fn frozen_read_set(&self) -> FrozenReadSet {
        FrozenReadSet::from_dependencies(self.dependencies())
    }
}

impl ReadRecorder for RecordingReadTracker {
    fn record(&self, dependency: ReadDependency) {
        Self::record(self, dependency);
    }

    fn record_batch(&self, dependencies: &[ReadDependency]) {
        self.dependencies.lock().extend_from_slice(dependencies);
    }

    fn frozen_read_set(&self) -> FrozenReadSet {
        Self::frozen_read_set(self)
    }
}

impl ReadRecorder for IndexedReadTracker {
    fn record(&self, dependency: ReadDependency) {
        Self::record(self, dependency);
    }

    fn record_batch(&self, dependencies: &[ReadDependency]) {
        Self::record_batch(self, dependencies.iter().cloned());
    }

    fn frozen_read_set(&self) -> FrozenReadSet {
        Self::frozen_read_set(self)
    }
}

#[derive(Debug, Clone, Default)]
pub enum ReadTrackerHandle {
    #[default]
    Noop,
    SafeSnapshot,
    Recording(Arc<RecordingReadTracker>),
    Serializable {
        tracker: Arc<IndexedReadTracker>,
        policy: ReadTrackingPolicy,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadTrackerSavepointMark {
    state: ReadTrackerSavepointState,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum ReadTrackerSavepointState {
    #[default]
    Noop,
    Recording {
        dependency_count: usize,
    },
    Serializable {
        mark: ReadDependencyIndexMark,
    },
}

impl ReadTrackerSavepointMark {
    #[inline]
    pub fn dependency_count(&self) -> usize {
        match &self.state {
            ReadTrackerSavepointState::Noop => 0,
            ReadTrackerSavepointState::Recording { dependency_count } => *dependency_count,
            ReadTrackerSavepointState::Serializable { mark } => mark.dependency_count,
        }
    }

    #[inline]
    pub fn coarsening_epoch(&self) -> u64 {
        match &self.state {
            ReadTrackerSavepointState::Serializable { mark } => mark.coarsening_epoch,
            ReadTrackerSavepointState::Noop | ReadTrackerSavepointState::Recording { .. } => 0,
        }
    }
}

impl ReadTrackerHandle {
    pub fn noop() -> Self {
        Self::Noop
    }

    pub fn recording() -> Self {
        Self::Recording(Arc::new(RecordingReadTracker::new()))
    }

    pub fn safe_snapshot() -> Self {
        Self::SafeSnapshot
    }

    pub fn serializable(
        index: Arc<ReadDependencyIndex>,
        txn_id: crate::TxnId,
        read_ts: ReadTs,
    ) -> Self {
        Self::serializable_with_policy(index, txn_id, read_ts, ReadTrackingPolicy::Serializable)
    }

    pub fn serializable_with_policy(
        index: Arc<ReadDependencyIndex>,
        txn_id: crate::TxnId,
        read_ts: ReadTs,
        policy: ReadTrackingPolicy,
    ) -> Self {
        Self::Serializable {
            tracker: ReadDependencyIndex::tracker(index, txn_id, read_ts),
            policy,
        }
    }

    #[inline]
    pub fn policy(&self) -> ReadTrackingPolicy {
        match self {
            Self::Noop => ReadTrackingPolicy::Noop,
            Self::SafeSnapshot => ReadTrackingPolicy::SafeSnapshot,
            Self::Recording(_) => ReadTrackingPolicy::Record,
            Self::Serializable { policy, .. } => *policy,
        }
    }

    #[inline]
    pub fn is_safe_snapshot(&self) -> bool {
        matches!(self, Self::SafeSnapshot)
    }

    #[inline]
    pub fn record_table_read(&self, table_id: TableId) {
        self.record(ReadDependency::Table { table_id });
    }

    #[inline]
    pub fn record_row_read(&self, table_id: TableId, row_id: u64) {
        self.record(ReadDependency::Row { table_id, row_id });
    }

    #[inline]
    pub fn record_row_reads(&self, table_id: TableId, row_ids: impl IntoIterator<Item = u64>) {
        self.record_dependencies(
            row_ids
                .into_iter()
                .map(|row_id| ReadDependency::Row { table_id, row_id }),
        );
    }

    #[inline]
    pub fn record_tablet_read(
        &self,
        table_id: TableId,
        tablet_id: u64,
        read_ts: ReadTs,
        layout_epoch: u64,
        rowset_count: usize,
    ) {
        self.record(ReadDependency::Tablet {
            table_id,
            tablet_id,
            read_ts,
            layout_epoch,
            rowset_count,
        });
    }

    #[inline]
    pub fn record_rowset_read(
        &self,
        table_id: TableId,
        tablet_id: u64,
        rowset_id: u64,
        read_ts: ReadTs,
        layout_epoch: u64,
    ) {
        self.record(ReadDependency::Rowset {
            table_id,
            tablet_id,
            rowset_id,
            read_ts,
            layout_epoch,
        });
    }

    #[inline]
    pub fn record_key_range(&self, table_id: TableId, start_hash: u64, end_hash: u64) {
        self.record(ReadDependency::KeyRange {
            table_id,
            start_hash,
            end_hash,
        });
    }

    #[inline]
    pub fn record_key_ranges(
        &self,
        table_id: TableId,
        ranges: impl IntoIterator<Item = (u64, u64)>,
    ) {
        self.record_dependencies(ranges.into_iter().map(|(start_hash, end_hash)| {
            ReadDependency::KeyRange {
                table_id,
                start_hash,
                end_hash,
            }
        }));
    }

    #[inline]
    pub fn record_predicate(&self, table_id: TableId, predicate_hash: u64) {
        self.record(ReadDependency::Predicate {
            table_id,
            predicate_hash,
        });
    }

    #[inline]
    pub fn record_generation(&self, resource_key: TxnResourceKey, generation: u64) {
        self.record(ReadDependency::Generation {
            resource_key,
            generation,
        });
    }

    #[inline]
    pub fn record(&self, dependency: ReadDependency) {
        match self {
            Self::Noop | Self::SafeSnapshot => {}
            Self::Recording(tracker) => tracker.record(dependency),
            Self::Serializable { tracker, policy } => {
                if let Some(dependency) = dependency_for_policy(*policy, dependency) {
                    tracker.record(dependency);
                }
            }
        }
    }

    pub fn record_dependencies(&self, dependencies: impl IntoIterator<Item = ReadDependency>) {
        match self {
            Self::Noop | Self::SafeSnapshot => {}
            Self::Recording(tracker) => {
                let dependencies = dependencies.into_iter().collect::<Vec<_>>();
                tracker.record_batch(&dependencies);
            }
            Self::Serializable { tracker, policy } => {
                tracker.record_batch(
                    dependencies
                        .into_iter()
                        .filter_map(|dependency| dependency_for_policy(*policy, dependency)),
                );
            }
        }
    }

    pub fn frozen_read_set(&self) -> FrozenReadSet {
        match self {
            Self::Noop | Self::SafeSnapshot => FrozenReadSet::empty(),
            Self::Recording(tracker) => tracker.frozen_read_set(),
            Self::Serializable { tracker, .. } => tracker.frozen_read_set(),
        }
    }

    pub fn recorded_dependencies(&self) -> Vec<ReadDependency> {
        match self {
            Self::Noop | Self::SafeSnapshot => Vec::new(),
            Self::Recording(tracker) => tracker.dependencies(),
            Self::Serializable { tracker, .. } => tracker.dependencies(),
        }
    }

    pub fn mark_savepoint(&self) -> ReadTrackerSavepointMark {
        let state = match self {
            Self::Noop | Self::SafeSnapshot => ReadTrackerSavepointState::Noop,
            Self::Recording(tracker) => ReadTrackerSavepointState::Recording {
                dependency_count: tracker.mark_savepoint(),
            },
            Self::Serializable { tracker, .. } => ReadTrackerSavepointState::Serializable {
                mark: tracker.mark_savepoint(),
            },
        };
        ReadTrackerSavepointMark { state }
    }

    pub fn rollback_to_savepoint(&self, mark: &ReadTrackerSavepointMark) -> ReadDependencyRollback {
        match (self, &mark.state) {
            (
                Self::Recording(tracker),
                ReadTrackerSavepointState::Recording { dependency_count },
            ) => {
                let removed_dependencies = tracker
                    .dependencies()
                    .len()
                    .saturating_sub(*dependency_count);
                tracker.rollback_to_savepoint(*dependency_count);
                ReadDependencyRollback {
                    removed_dependencies,
                    preserved_due_to_coarsening: false,
                }
            }
            (
                Self::Serializable { tracker, .. },
                ReadTrackerSavepointState::Serializable { mark },
            ) => tracker.rollback_to_savepoint(*mark),
            _ => ReadDependencyRollback::default(),
        }
    }
}

fn dependency_for_policy(
    policy: ReadTrackingPolicy,
    dependency: ReadDependency,
) -> Option<ReadDependency> {
    match policy {
        ReadTrackingPolicy::Noop | ReadTrackingPolicy::SafeSnapshot => None,
        ReadTrackingPolicy::AnalyticalScan | ReadTrackingPolicy::SafeSnapshotPreferred => {
            match dependency.table_id() {
                Some(table_id) => Some(ReadDependency::AnalyticalScan { table_id }),
                None => Some(dependency),
            }
        }
        ReadTrackingPolicy::Record
        | ReadTrackingPolicy::Serializable
        | ReadTrackingPolicy::PointCritical
        | ReadTrackingPolicy::RangeCritical => Some(dependency),
    }
}

#[derive(Debug, Clone)]
pub struct TransactionView {
    writer_id: WriterId,
    start_time: ReadTs,
    read_snapshot: ReadSnapshot,
    as_of_ts: Option<ReadTs>,
    isolation_level: IsolationLevel,
    command_id: CommandId,
    read_tracker: ReadTrackerHandle,
    participant_states: ParticipantStateSet,
}

impl TransactionView {
    pub fn new(
        writer_id: WriterId,
        start_time: ReadTs,
        read_snapshot: ReadSnapshot,
        isolation_level: IsolationLevel,
        command_id: CommandId,
        read_tracker: ReadTrackerHandle,
        participant_states: ParticipantStateSet,
    ) -> Self {
        Self {
            writer_id,
            start_time,
            read_snapshot,
            as_of_ts: None,
            isolation_level,
            command_id,
            read_tracker,
            participant_states,
        }
    }

    pub fn autocommit(read_ts: ReadTs) -> Self {
        Self::new(
            WriterId::permanent(),
            read_ts,
            ReadSnapshot::without_lease(read_ts),
            IsolationLevel::Snapshot,
            CommandId::new(0),
            ReadTrackerHandle::noop(),
            ParticipantStateSet::empty(),
        )
    }

    #[inline]
    pub fn with_as_of_ts(mut self, as_of_ts: ReadTs) -> Self {
        self.as_of_ts = Some(as_of_ts);
        self
    }

    #[inline]
    pub fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    #[inline]
    pub fn start_time(&self) -> ReadTs {
        self.start_time
    }

    #[inline]
    pub fn read_snapshot(&self) -> &ReadSnapshot {
        &self.read_snapshot
    }

    #[inline]
    pub fn read_ts(&self) -> ReadTs {
        self.read_snapshot.read_ts()
    }

    #[inline]
    pub fn as_of_ts(&self) -> Option<ReadTs> {
        self.as_of_ts
    }

    #[inline]
    pub fn is_time_travel(&self) -> bool {
        self.as_of_ts.is_some()
    }

    #[inline]
    pub fn effective_read_ts(&self) -> ReadTs {
        self.as_of_ts
            .unwrap_or_else(|| self.read_snapshot.read_ts())
    }

    #[inline]
    pub fn validate_as_of_watermark(
        &self,
        oldest_available: ReadTs,
    ) -> std::result::Result<(), AsOfTimestampError> {
        if let Some(as_of_ts) = self.as_of_ts {
            validate_as_of_timestamp(as_of_ts, oldest_available)?;
        }
        Ok(())
    }

    /// Logical committed-version upper bound for this transaction view.
    ///
    /// Normal reads use the read snapshot's `read_ts`; explicit time-travel
    /// reads use `as_of_ts`. Callers should use this for `commit_ts <= read_ts`
    /// visibility checks. It does not subtract one from `start_time`.
    #[inline]
    pub fn visible_version(&self) -> u64 {
        self.effective_read_ts().into_raw()
    }

    #[inline]
    pub fn visible_version_i64(&self) -> i64 {
        i64::try_from(self.visible_version()).unwrap_or(i64::MAX)
    }

    #[inline]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    #[inline]
    pub fn command_id(&self) -> CommandId {
        self.command_id
    }

    #[inline]
    pub fn read_tracker(&self) -> &ReadTrackerHandle {
        &self.read_tracker
    }

    #[inline]
    pub fn read_tracking_policy(&self) -> ReadTrackingPolicy {
        self.read_tracker.policy()
    }

    #[inline]
    pub fn participant_states(&self) -> &ParticipantStateSet {
        &self.participant_states
    }

    pub fn frozen_read_set(&self) -> FrozenReadSet {
        self.read_tracker.frozen_read_set()
    }
}

impl Default for TransactionView {
    fn default() -> Self {
        Self::autocommit(ReadTs::new(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseId, ParticipantKind};

    #[test]
    fn noop_tracker_freezes_empty_read_set_without_allocation_state() {
        let view = TransactionView::autocommit(ReadTs::new(7));

        view.read_tracker().record_table_read(TableId::new(11));

        assert_eq!(view.visible_version(), 7);
        assert_eq!(view.read_tracking_policy(), ReadTrackingPolicy::Noop);
        assert_eq!(view.frozen_read_set().dependency_count(), 0);
    }

    #[test]
    fn as_of_timestamp_overrides_visibility_without_changing_txn_read_ts() {
        let view = TransactionView::new(
            WriterId::permanent(),
            ReadTs::new(21),
            ReadSnapshot::without_lease(ReadTs::new(20)),
            IsolationLevel::Snapshot,
            CommandId::new(0),
            ReadTrackerHandle::noop(),
            ParticipantStateSet::empty(),
        )
        .with_as_of_ts(ReadTs::new(9));

        assert!(view.is_time_travel());
        assert_eq!(view.read_ts(), ReadTs::new(20));
        assert_eq!(view.effective_read_ts(), ReadTs::new(9));
        assert_eq!(view.visible_version(), 9);
        assert_eq!(view.visible_version_i64(), 9);
    }

    #[test]
    fn as_of_timestamp_before_gc_watermark_is_rejected() {
        let view = TransactionView::autocommit(ReadTs::new(30)).with_as_of_ts(ReadTs::new(11));

        let err = view
            .validate_as_of_watermark(ReadTs::new(12))
            .expect_err("AS OF before retained watermark must fail");

        assert_eq!(
            err,
            AsOfTimestampError::BeforeGcWatermark {
                requested: ReadTs::new(11),
                oldest_available: ReadTs::new(12),
            }
        );
        assert!(view.validate_as_of_watermark(ReadTs::new(11)).is_ok());
    }

    #[test]
    fn recording_tracker_captures_tablet_dependency() {
        let tracker = ReadTrackerHandle::recording();
        let view = TransactionView::new(
            WriterId::permanent(),
            ReadTs::new(9),
            ReadSnapshot::without_lease(ReadTs::new(8)),
            IsolationLevel::Snapshot,
            CommandId::new(2),
            tracker,
            ParticipantStateSet::empty(),
        );

        view.read_tracker()
            .record_tablet_read(TableId::new(3), 4, view.read_ts(), 5, 6);

        assert_eq!(view.frozen_read_set().dependency_count(), 1);
        assert_eq!(
            view.read_tracker().recorded_dependencies(),
            vec![ReadDependency::Tablet {
                table_id: TableId::new(3),
                tablet_id: 4,
                read_ts: ReadTs::new(8),
                layout_epoch: 5,
                rowset_count: 6,
            }]
        );

        let key = TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(1));
        view.read_tracker().record_generation(key, 10);
        assert_eq!(view.frozen_read_set().dependency_count(), 2);
    }

    #[test]
    fn recording_tracker_savepoint_rollback_truncates_dependencies() {
        let tracker = ReadTrackerHandle::recording();
        tracker.record_table_read(TableId::new(1));
        let mark = tracker.mark_savepoint();
        tracker.record_predicate(TableId::new(2), 99);

        tracker.rollback_to_savepoint(&mark);

        assert_eq!(
            tracker.recorded_dependencies(),
            vec![ReadDependency::Table {
                table_id: TableId::new(1)
            }]
        );
        assert_eq!(mark.dependency_count(), 1);
    }

    #[test]
    fn serializable_tracker_savepoint_rollback_uses_index_mark() {
        let index = Arc::new(ReadDependencyIndex::with_shards(2));
        let tracker = ReadTrackerHandle::serializable(
            Arc::clone(&index),
            crate::TxnId::new(77),
            ReadTs::new(10),
        );
        tracker.record_table_read(TableId::new(1));
        let mark = tracker.mark_savepoint();
        tracker.record_predicate(TableId::new(2), 33);

        tracker.rollback_to_savepoint(&mark);

        assert_eq!(
            tracker.recorded_dependencies(),
            vec![ReadDependency::Table {
                table_id: TableId::new(1)
            }]
        );
        assert_eq!(index.stats().dependency_count, 1);
    }
}
