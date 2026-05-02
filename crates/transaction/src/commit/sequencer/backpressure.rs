// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit publish-lag backpressure.

use super::super::{
    fetch_max_relaxed, CommitPlan, ParticipantDescriptor, DEFAULT_MAX_PARTICIPANT_APPLY_LAG,
    DEFAULT_MAX_UNPUBLISHED_COMMITS,
};
use crate::sync::Mutex;
use crate::types::CommitTs;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
