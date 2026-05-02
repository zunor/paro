// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit-id and publish visibility frontier shared by commit runtime users.

use super::fetch_max_relaxed;
use crate::sync::{Condvar, Mutex};
use crate::types::CommitTs;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

const NO_FAILURE_WATERMARK: u64 = 0;
const NO_STALE_BYTES: u64 = u64::MAX;

/// Durable commit progress visible to `CommitFrontier`.
///
/// Implementors must report the encoded logical commit-record byte count used
/// for durable-to-published backlog accounting; legacy tickets without that
/// byte count intentionally do not implement this trait.
pub trait CommitFrontierHandle {
    fn commit_ts(&self) -> CommitTs;
    fn commit_record_bytes(&self) -> u32;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishFailureCause {
    Phase1 {
        durable_lsn: u64,
        error_code: u32,
        message: Arc<str>,
    },
    Submit {
        durable_lsn: u64,
        error_code: u32,
        message: Arc<str>,
    },
    Apply {
        durable_lsn: u64,
        error_code: u32,
        message: Arc<str>,
    },
}

impl PublishFailureCause {
    #[inline]
    pub fn phase1(message: impl Into<Arc<str>>) -> Self {
        Self::phase1_with_diagnostics(0, 0, message)
    }

    #[inline]
    pub fn phase1_with_diagnostics(
        durable_lsn: u64,
        error_code: u32,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self::Phase1 {
            durable_lsn,
            error_code,
            message: message.into(),
        }
    }

    #[inline]
    pub fn submit(message: impl Into<Arc<str>>) -> Self {
        Self::submit_with_diagnostics(0, 0, message)
    }

    #[inline]
    pub fn submit_with_diagnostics(
        durable_lsn: u64,
        error_code: u32,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self::Submit {
            durable_lsn,
            error_code,
            message: message.into(),
        }
    }

    #[inline]
    pub fn apply(message: impl Into<Arc<str>>) -> Self {
        Self::apply_with_diagnostics(0, 0, message)
    }

    #[inline]
    pub fn apply_with_diagnostics(
        durable_lsn: u64,
        error_code: u32,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self::Apply {
            durable_lsn,
            error_code,
            message: message.into(),
        }
    }
}

impl fmt::Display for PublishFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Phase1 {
                durable_lsn,
                error_code,
                message,
            } => write!(
                f,
                "phase1 publish failure: lsn={durable_lsn} error_code={error_code} message={message}"
            ),
            Self::Submit {
                durable_lsn,
                error_code,
                message,
            } => write!(
                f,
                "publish submit failure: lsn={durable_lsn} error_code={error_code} message={message}"
            ),
            Self::Apply {
                durable_lsn,
                error_code,
                message,
            } => write!(
                f,
                "apply publish failure: lsn={durable_lsn} error_code={error_code} message={message}"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishFailure {
    pub first_blocked_commit_ts: CommitTs,
    pub cause: PublishFailureCause,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishWaitError {
    PublishFailed(PublishFailure),
}

impl fmt::Display for PublishWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PublishFailed(failure) => write!(
                f,
                "commit publish failed at {}, cause={}",
                failure.first_blocked_commit_ts, failure.cause
            ),
        }
    }
}

impl std::error::Error for PublishWaitError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitFrontierMetrics {
    pub wait_count: u64,
    pub wait_wake_count: u64,
    pub notify_all_count: u64,
    pub notify_suppressed_count: u64,
    pub publish_failure_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFrontierSnapshot {
    pub durable_commit_id: CommitTs,
    pub published_commit_id: CommitTs,
    pub durable_commit_bytes: u64,
    pub published_commit_bytes: u64,
    pub durable_to_published_bytes_lag: Option<u64>,
    pub stale_bytes_at_poison: Option<u64>,
    pub publish_failure_watermark: Option<CommitTs>,
    pub publish_failure: Option<PublishFailure>,
    pub metrics: CommitFrontierMetrics,
}

#[derive(Debug, Default)]
struct FrontierMetricCounters {
    wait_count: AtomicU64,
    wait_wake_count: AtomicU64,
    notify_all_count: AtomicU64,
    notify_suppressed_count: AtomicU64,
    publish_failure_count: AtomicU64,
}

impl FrontierMetricCounters {
    fn snapshot(&self) -> CommitFrontierMetrics {
        CommitFrontierMetrics {
            wait_count: self.wait_count.load(Ordering::Relaxed),
            wait_wake_count: self.wait_wake_count.load(Ordering::Relaxed),
            notify_all_count: self.notify_all_count.load(Ordering::Relaxed),
            notify_suppressed_count: self.notify_suppressed_count.load(Ordering::Relaxed),
            publish_failure_count: self.publish_failure_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct CommitFrontier {
    durable_commit_id: AtomicU64,
    published_commit_id: AtomicU64,
    durable_commit_bytes: AtomicU64,
    published_commit_bytes: AtomicU64,
    stale_bytes_at_poison: AtomicU64,
    publish_failure_watermark: AtomicU64,
    publish_failure_cause: Mutex<Option<PublishFailure>>,
    published_commit_changed: Condvar,
    published_commit_wait: Mutex<()>,
    waiter_count: AtomicUsize,
    metrics: FrontierMetricCounters,
}

impl CommitFrontier {
    pub fn new() -> Self {
        Self {
            durable_commit_id: AtomicU64::new(0),
            published_commit_id: AtomicU64::new(0),
            durable_commit_bytes: AtomicU64::new(0),
            published_commit_bytes: AtomicU64::new(0),
            stale_bytes_at_poison: AtomicU64::new(NO_STALE_BYTES),
            publish_failure_watermark: AtomicU64::new(NO_FAILURE_WATERMARK),
            publish_failure_cause: Mutex::new(None),
            published_commit_changed: Condvar::new(),
            published_commit_wait: Mutex::new(()),
            waiter_count: AtomicUsize::new(0),
            metrics: FrontierMetricCounters::default(),
        }
    }

    /// Aligns commit-id frontiers with externally recovered state.
    ///
    /// This is used during bootstrap/recovery before admission opens. It does
    /// not synthesize byte accounting because recovered byte lag is not live
    /// backpressure state.
    pub fn sync_commit_ids(&self, durable_commit_id: CommitTs, published_commit_id: CommitTs) {
        let durable_raw = durable_commit_id
            .into_raw()
            .max(published_commit_id.into_raw());
        fetch_max_relaxed(&self.durable_commit_id, durable_raw);
        fetch_max_relaxed(&self.published_commit_id, published_commit_id.into_raw());
        self.notify_waiters();
    }

    /// Aligns only the durable commit-id frontier for legacy append bridges.
    ///
    /// New queue-drain code should prefer `mark_durable(&handle)` so durable
    /// byte accounting stays precise.
    pub fn sync_durable_commit_id(&self, durable_commit_id: CommitTs) {
        fetch_max_relaxed(&self.durable_commit_id, durable_commit_id.into_raw());
    }

    pub fn mark_durable(&self, handle: &(impl CommitFrontierHandle + ?Sized)) {
        fetch_max_relaxed(&self.durable_commit_id, handle.commit_ts().into_raw());
        self.durable_commit_bytes
            .fetch_add(handle.commit_record_bytes() as u64, Ordering::Release);
    }

    pub fn mark_published(&self, handle: &(impl CommitFrontierHandle + ?Sized)) {
        fetch_max_relaxed(&self.published_commit_id, handle.commit_ts().into_raw());
        self.published_commit_bytes
            .fetch_add(handle.commit_record_bytes() as u64, Ordering::Release);
        self.notify_waiters();
    }

    pub fn mark_publish_failed(&self, commit_ts: CommitTs, cause: PublishFailureCause) {
        let commit_raw = commit_ts.into_raw();
        let mut failure_guard = self.publish_failure_cause.lock();

        if failure_guard
            .as_ref()
            .is_some_and(|failure| failure.first_blocked_commit_ts <= commit_ts)
        {
            return;
        }

        let stale_lag = self
            .durable_commit_bytes
            .load(Ordering::Acquire)
            .saturating_sub(self.published_commit_bytes.load(Ordering::Acquire));
        let _ = self.stale_bytes_at_poison.compare_exchange(
            NO_STALE_BYTES,
            stale_lag,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        *failure_guard = Some(PublishFailure {
            first_blocked_commit_ts: commit_ts,
            cause,
        });
        self.publish_failure_watermark
            .store(commit_raw, Ordering::Release);
        self.metrics
            .publish_failure_count
            .fetch_add(1, Ordering::Relaxed);
        drop(failure_guard);
        self.notify_waiters();
    }

    #[inline]
    pub fn durable_commit_id(&self) -> CommitTs {
        CommitTs::new(self.durable_commit_id.load(Ordering::Acquire))
    }

    #[inline]
    pub fn published_commit_id(&self) -> CommitTs {
        CommitTs::new(self.published_commit_id.load(Ordering::Acquire))
    }

    pub fn durable_to_published_bytes_lag(&self) -> Option<u64> {
        if self.is_poisoned() {
            return None;
        }
        Some(
            self.durable_commit_bytes
                .load(Ordering::Acquire)
                .saturating_sub(self.published_commit_bytes.load(Ordering::Acquire)),
        )
    }

    pub fn stale_bytes_at_poison(&self) -> Option<u64> {
        match self.stale_bytes_at_poison.load(Ordering::Acquire) {
            NO_STALE_BYTES => None,
            bytes => Some(bytes),
        }
    }

    pub fn publish_failure_watermark(&self) -> Option<CommitTs> {
        match self.publish_failure_watermark.load(Ordering::Acquire) {
            NO_FAILURE_WATERMARK => None,
            commit_ts => Some(CommitTs::new(commit_ts)),
        }
    }

    pub fn publish_failure(&self) -> Option<PublishFailure> {
        self.publish_failure_cause.lock().clone()
    }

    pub fn is_poisoned(&self) -> bool {
        self.publish_failure_watermark.load(Ordering::Acquire) != NO_FAILURE_WATERMARK
    }

    pub fn wait_for_published_at_least(&self, floor: CommitTs) -> Result<(), PublishWaitError> {
        if self.is_published_or_failed(floor)? {
            return Ok(());
        }
        self.waiter_count.fetch_add(1, Ordering::AcqRel);
        let _waiter = WaiterRegistration {
            waiter_count: &self.waiter_count,
        };
        self.metrics.wait_count.fetch_add(1, Ordering::Relaxed);

        let mut guard = self.published_commit_wait.lock();
        loop {
            if self.is_published_or_failed(floor)? {
                return Ok(());
            }
            guard = self.published_commit_changed.wait(guard);
            self.metrics.wait_wake_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> CommitFrontierSnapshot {
        let durable_commit_bytes = self.durable_commit_bytes.load(Ordering::Acquire);
        let published_commit_bytes = self.published_commit_bytes.load(Ordering::Acquire);
        CommitFrontierSnapshot {
            durable_commit_id: self.durable_commit_id(),
            published_commit_id: self.published_commit_id(),
            durable_commit_bytes,
            published_commit_bytes,
            durable_to_published_bytes_lag: self.durable_to_published_bytes_lag(),
            stale_bytes_at_poison: self.stale_bytes_at_poison(),
            publish_failure_watermark: self.publish_failure_watermark(),
            publish_failure: self.publish_failure(),
            metrics: self.metrics.snapshot(),
        }
    }

    fn is_published_or_failed(&self, floor: CommitTs) -> Result<bool, PublishWaitError> {
        if self.published_commit_id().into_raw() >= floor.into_raw() {
            return Ok(true);
        }
        if let Some(failure) = self.failure_at_or_below(floor) {
            return Err(PublishWaitError::PublishFailed(failure));
        }
        Ok(false)
    }

    fn failure_at_or_below(&self, floor: CommitTs) -> Option<PublishFailure> {
        let watermark = self.publish_failure_watermark.load(Ordering::Acquire);
        if watermark != NO_FAILURE_WATERMARK && watermark <= floor.into_raw() {
            return self.publish_failure_cause.lock().clone();
        }
        None
    }

    fn notify_waiters(&self) {
        if self.waiter_count.load(Ordering::Acquire) > 0 {
            self.metrics
                .notify_all_count
                .fetch_add(1, Ordering::Relaxed);
            self.published_commit_changed.notify_all();
        } else {
            self.metrics
                .notify_suppressed_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Default for CommitFrontier {
    fn default() -> Self {
        Self::new()
    }
}

struct WaiterRegistration<'a> {
    waiter_count: &'a AtomicUsize,
}

impl Drop for WaiterRegistration<'_> {
    fn drop(&mut self) {
        self.waiter_count.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct TestHandle {
        commit_ts: CommitTs,
        bytes: u32,
    }

    impl CommitFrontierHandle for TestHandle {
        fn commit_ts(&self) -> CommitTs {
            self.commit_ts
        }

        fn commit_record_bytes(&self) -> u32 {
            self.bytes
        }
    }

    #[test]
    fn frontier_tracks_commit_ids_and_byte_lag() {
        let frontier = CommitFrontier::new();
        frontier.mark_durable(&TestHandle {
            commit_ts: CommitTs::new(3),
            bytes: 128,
        });
        frontier.mark_published(&TestHandle {
            commit_ts: CommitTs::new(2),
            bytes: 64,
        });

        let snapshot = frontier.snapshot();
        assert_eq!(snapshot.durable_commit_id, CommitTs::new(3));
        assert_eq!(snapshot.published_commit_id, CommitTs::new(2));
        assert_eq!(snapshot.durable_to_published_bytes_lag, Some(64));
    }

    #[test]
    fn publish_failure_records_first_blocked_commit_and_stale_lag() {
        let frontier = CommitFrontier::new();
        frontier.mark_durable(&TestHandle {
            commit_ts: CommitTs::new(8),
            bytes: 300,
        });
        frontier.mark_published(&TestHandle {
            commit_ts: CommitTs::new(6),
            bytes: 100,
        });
        frontier.mark_publish_failed(
            CommitTs::new(8),
            PublishFailureCause::apply("tablet publish failed"),
        );
        frontier.mark_publish_failed(
            CommitTs::new(7),
            PublishFailureCause::submit("submit failed earlier"),
        );

        let failure = frontier.publish_failure().unwrap();
        assert_eq!(failure.first_blocked_commit_ts, CommitTs::new(7));
        assert_eq!(frontier.durable_to_published_bytes_lag(), None);
        assert_eq!(frontier.stale_bytes_at_poison(), Some(200));
        assert!(matches!(
            frontier.wait_for_published_at_least(CommitTs::new(8)),
            Err(PublishWaitError::PublishFailed(_))
        ));
    }

    #[test]
    fn wait_returns_immediately_when_already_published() {
        let frontier = CommitFrontier::new();
        frontier.mark_published(&TestHandle {
            commit_ts: CommitTs::new(4),
            bytes: 1,
        });

        frontier
            .wait_for_published_at_least(CommitTs::new(3))
            .unwrap();
        assert_eq!(frontier.snapshot().metrics.wait_count, 0);
    }

    #[test]
    fn sync_commit_ids_bootstraps_without_byte_accounting() {
        let frontier = CommitFrontier::new();
        frontier.sync_commit_ids(CommitTs::new(10), CommitTs::new(8));

        let snapshot = frontier.snapshot();
        assert_eq!(snapshot.durable_commit_id, CommitTs::new(10));
        assert_eq!(snapshot.published_commit_id, CommitTs::new(8));
        assert_eq!(snapshot.durable_commit_bytes, 0);
        assert_eq!(snapshot.published_commit_bytes, 0);
    }
}
