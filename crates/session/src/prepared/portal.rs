// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::FetchDirection;
use paro_transaction::{ReadSnapshot, ReadSnapshotLease, ReadSnapshotLeaseOwner, ReadTs};

use crate::result::retained_store::SessionRetainedResultStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorHoldability {
    WithoutHold,
    WithHold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrollMode {
    NoScroll,
    Scroll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    Text,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalSnapshotRetentionPolicy {
    Materialized,
    Pinned,
    TimeoutMaterialize,
    RequireClose,
}

impl PortalSnapshotRetentionPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Pinned => "pinned",
            Self::TimeoutMaterialize => "timeout_materialize",
            Self::RequireClose => "require_close",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PortalSnapshotRetention {
    read_ts: ReadTs,
    policy: PortalSnapshotRetentionPolicy,
    lease: Option<Arc<ReadSnapshotLease>>,
}

impl PortalSnapshotRetention {
    pub fn materialized(read_ts: ReadTs) -> Self {
        Self {
            read_ts,
            policy: PortalSnapshotRetentionPolicy::Materialized,
            lease: None,
        }
    }

    pub fn pinned(snapshot: &ReadSnapshot, session_id: u64, portal_id: impl AsRef<str>) -> Self {
        let lease = snapshot.lease();
        if let Some(lease) = &lease {
            lease.bind_owner(ReadSnapshotLeaseOwner::for_portal(
                session_id,
                portal_id.as_ref(),
            ));
        }
        Self {
            read_ts: snapshot.read_ts(),
            policy: if lease.is_some() {
                PortalSnapshotRetentionPolicy::Pinned
            } else {
                PortalSnapshotRetentionPolicy::Materialized
            },
            lease,
        }
    }

    pub fn with_policy(mut self, policy: PortalSnapshotRetentionPolicy) -> Self {
        self.policy = policy;
        self
    }

    #[inline]
    pub fn read_ts(&self) -> ReadTs {
        self.read_ts
    }

    #[inline]
    pub fn policy(&self) -> PortalSnapshotRetentionPolicy {
        self.policy
    }

    #[inline]
    pub fn lease(&self) -> Option<&Arc<ReadSnapshotLease>> {
        self.lease.as_ref()
    }

    pub fn pin_duration_us(&self) -> Option<u64> {
        self.lease
            .as_ref()
            .map(|lease| u64::try_from(lease.pinned_duration().as_micros()).unwrap_or(u64::MAX))
    }

    pub fn owner(&self) -> Option<ReadSnapshotLeaseOwner> {
        self.lease.as_ref().map(|lease| lease.owner())
    }
}

#[derive(Clone)]
pub struct MaterializedPortalData {
    store: SessionRetainedResultStore,
    row_count: usize,
}

impl std::fmt::Debug for MaterializedPortalData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializedPortalData")
            .field("store", &self.store)
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl MaterializedPortalData {
    pub fn new(store: SessionRetainedResultStore) -> Self {
        let row_count = store.row_count();
        Self { store, row_count }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    fn chunk_range(&self, start: usize, end: usize) -> Result<Vec<Chunk>, String> {
        self.store.chunk_range(start, end)
    }

    pub fn fetch(
        &self,
        position: i64,
        direction: &FetchDirection,
        scroll_mode: ScrollMode,
        move_only: bool,
    ) -> Result<FetchOutcome, String> {
        if !matches!(scroll_mode, ScrollMode::Scroll) && !is_forward_only(direction) {
            return Err("cursor can only scan forward".to_string());
        }

        let total = self.row_count as i64;
        let base = normalize_position(position, total);
        let (new_position, start, end) = match direction {
            FetchDirection::Next => {
                if base >= total - 1 {
                    (total, total, total)
                } else {
                    let row = base + 1;
                    (row, row, row + 1)
                }
            }
            FetchDirection::Prior => {
                let row = if base >= total { total - 1 } else { base - 1 };
                if row < 0 {
                    (-1, 0, 0)
                } else {
                    (row, row, row + 1)
                }
            }
            FetchDirection::First => {
                if total == 0 {
                    (-1, 0, 0)
                } else {
                    (0, 0, 1)
                }
            }
            FetchDirection::Last => {
                if total == 0 {
                    (-1, 0, 0)
                } else {
                    (total - 1, total - 1, total)
                }
            }
            FetchDirection::ForwardAll => {
                let start = (base + 1).clamp(0, total) as usize;
                (total, start as i64, total)
            }
            FetchDirection::BackwardAll => {
                let end = base.clamp(0, total) as usize;
                let new_position = if end == 0 { -1 } else { 0 };
                (new_position, 0, end as i64)
            }
            FetchDirection::Count(count) | FetchDirection::ForwardCount(count) => {
                forward_range(base, *count, total)
            }
            FetchDirection::BackwardCount(count) => backward_range(base, *count, total),
            FetchDirection::Absolute(count) => absolute_range(*count, total),
            FetchDirection::Relative(count) => relative_range(base, *count, total),
        };

        let moved_rows = (end - start).max(0) as usize;
        let rows = if move_only {
            Vec::new()
        } else {
            self.chunk_range(start.max(0) as usize, end.max(0) as usize)?
        };

        Ok(FetchOutcome {
            new_position,
            moved_rows,
            rows,
            at_end: matches!(
                direction,
                FetchDirection::Next
                    | FetchDirection::ForwardAll
                    | FetchDirection::Count(_)
                    | FetchDirection::ForwardCount(_)
            ) && end >= total,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionCursorHandle {
    materialized: MaterializedPortalData,
}

impl ExecutionCursorHandle {
    pub fn materialized(materialized: MaterializedPortalData) -> Self {
        Self { materialized }
    }

    pub fn row_count(&self) -> usize {
        self.materialized.row_count()
    }

    pub fn fetch(
        &self,
        position: i64,
        direction: &FetchDirection,
        scroll_mode: ScrollMode,
        move_only: bool,
    ) -> Result<FetchOutcome, String> {
        self.materialized
            .fetch(position, direction, scroll_mode, move_only)
    }
}

#[derive(Debug, Clone)]
pub struct PortalCursor {
    pub position: i64,
    pub execution: ExecutionCursorHandle,
}

#[derive(Debug, Clone)]
pub enum PortalExecutionState {
    Ready,
    Active(PortalCursor),
    Exhausted { position: i64 },
}

#[derive(Debug, Clone)]
pub struct FetchOutcome {
    pub new_position: i64,
    pub moved_rows: usize,
    pub rows: Vec<Chunk>,
    pub at_end: bool,
}

pub fn values_to_text(values: &[Option<LogicalType>]) -> String {
    let items = values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown".to_string())
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", items.join(","))
}

pub fn bind_value_types(values: &[Value]) -> Vec<Option<LogicalType>> {
    values.iter().map(infer_value_type).collect()
}

fn infer_value_type(value: &Value) -> Option<LogicalType> {
    match value {
        Value::Null(ty) => Some(ty.clone()),
        Value::Boolean(_) => Some(LogicalType::Boolean),
        Value::TinyInt(_) => Some(LogicalType::TinyInt),
        Value::SmallInt(_) => Some(LogicalType::SmallInt),
        Value::Integer(_) => Some(LogicalType::Integer),
        Value::BigInt(_) => Some(LogicalType::BigInt),
        Value::UTinyInt(_) => Some(LogicalType::UTinyInt),
        Value::USmallInt(_) => Some(LogicalType::USmallInt),
        Value::UInteger(_) => Some(LogicalType::UInteger),
        Value::UBigInt(_) => Some(LogicalType::UBigInt),
        Value::HugeInt(_) => Some(LogicalType::HugeInt),
        Value::UHugeInt(_) => Some(LogicalType::UHugeInt),
        Value::Float(_) => Some(LogicalType::Float),
        Value::Double(_) => Some(LogicalType::Double),
        Value::Decimal(_, precision, scale) => Some(LogicalType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        Value::Varchar(_) => Some(LogicalType::Varchar),
        Value::Blob(_) => Some(LogicalType::Blob),
        Value::Date(_) => Some(LogicalType::Date),
        Value::Time(_) => Some(LogicalType::Time),
        Value::Timestamp(_) => Some(LogicalType::Timestamp),
        Value::TimestampTz(_) => Some(LogicalType::TimestampTz),
        Value::Interval(_, _, _) => Some(LogicalType::Interval),
        Value::Uuid(_) => Some(LogicalType::Uuid),
        Value::Array(_, ty, size) => Some(LogicalType::Array(Box::new(ty.clone()), *size)),
        Value::List(_, ty) => Some(LogicalType::List(Box::new(ty.clone()))),
        Value::Struct(_, fields) => Some(LogicalType::Struct(fields.clone())),
    }
}

fn is_forward_only(direction: &FetchDirection) -> bool {
    matches!(
        direction,
        FetchDirection::Next
            | FetchDirection::ForwardAll
            | FetchDirection::Count(_)
            | FetchDirection::ForwardCount(_)
    )
}

fn normalize_position(position: i64, total: i64) -> i64 {
    position.clamp(-1, total)
}

fn forward_range(base: i64, count: i64, total: i64) -> (i64, i64, i64) {
    if count <= 0 {
        return (base, 0, 0);
    }
    let start = (base + 1).clamp(0, total);
    let end = (start + count).min(total);
    let new_position = if end == start { total } else { end - 1 };
    (new_position, start, end)
}

fn backward_range(base: i64, count: i64, total: i64) -> (i64, i64, i64) {
    if count <= 0 {
        return (base, 0, 0);
    }
    let end = if base >= total { total } else { base.max(0) };
    let start = (end - count).max(0);
    let new_position = if start >= end { -1 } else { start };
    (new_position, start, end)
}

fn absolute_range(target: i64, total: i64) -> (i64, i64, i64) {
    if target == 0 {
        return (-1, 0, 0);
    }
    let row = if target > 0 {
        target - 1
    } else {
        total + target
    };
    if row < 0 || row >= total {
        (if target > 0 { total } else { -1 }, 0, 0)
    } else {
        (row, row, row + 1)
    }
}

fn relative_range(base: i64, offset: i64, total: i64) -> (i64, i64, i64) {
    if offset == 0 {
        return (base, 0, 0);
    }
    let origin = if base >= total { total } else { base };
    let row = origin + offset;
    if row < 0 || row >= total {
        (if offset > 0 { total } else { -1 }, 0, 0)
    } else {
        (row, row, row + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_transaction::RetentionRegistry;

    #[test]
    fn portal_snapshot_retention_binds_pinned_lease_owner() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let lease = Arc::new(registry.lease_read_snapshot(ReadTs::new(77)).unwrap());
        let snapshot = ReadSnapshot::new(ReadTs::new(77), Some(lease.clone()));

        let retention = PortalSnapshotRetention::pinned(&snapshot, 42, "portal_a");

        assert_eq!(retention.read_ts(), ReadTs::new(77));
        assert_eq!(retention.policy(), PortalSnapshotRetentionPolicy::Pinned);
        assert!(retention.pin_duration_us().is_some());
        assert_eq!(lease.owner().owner_session_id, Some(42));
        assert_eq!(lease.owner().portal_id.as_deref(), Some("portal_a"));
    }

    #[test]
    fn materialized_portal_snapshot_does_not_pin_retention() {
        let retention = PortalSnapshotRetention::materialized(ReadTs::new(12));

        assert_eq!(retention.read_ts(), ReadTs::new(12));
        assert_eq!(
            retention.policy(),
            PortalSnapshotRetentionPolicy::Materialized
        );
        assert!(retention.lease().is_none());
        assert_eq!(retention.pin_duration_us(), None);
    }
}
