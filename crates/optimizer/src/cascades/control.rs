// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-local cooperative search control, independent of deterministic work
//! credits. A search deadline ends exploration; statement cancellation ends the
//! statement. Neither is an advisory failed equivalence rule.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use paro_common::error::Result;
use paro_context::StatementCancellation;

#[derive(Debug)]
pub struct SearchControl {
    started: Instant,
    optional_time_limit: Option<Duration>,
    optional: AtomicBool,
    deadline_reached: AtomicBool,
    /// First cooperative checkpoint that observed the optional deadline.
    /// `u64::MAX` is the unset sentinel; recording this separately from the
    /// return timestamp keeps timeout tail work visible to diagnostics.
    deadline_at_us: AtomicU64,
    cancellation: Option<StatementCancellation>,
}

/// A required physical incumbent may be needed for another root/requirement
/// after optional search expired. Its phase does not refresh the query clock
/// or erase incompleteness, and cancellation still interrupts it.
pub(crate) struct IncumbentPhase {
    control: Arc<SearchControl>,
    previous_optional: bool,
}

impl Drop for IncumbentPhase {
    fn drop(&mut self) {
        self.control
            .optional
            .store(self.previous_optional, Ordering::Relaxed);
    }
}

impl SearchControl {
    pub fn new(optional_time_limit: Option<Duration>) -> Self {
        Self {
            started: Instant::now(),
            optional_time_limit,
            optional: AtomicBool::new(false),
            deadline_reached: AtomicBool::new(false),
            deadline_at_us: AtomicU64::new(u64::MAX),
            cancellation: None,
        }
    }

    pub fn set_cancellation(&mut self, cancellation: StatementCancellation) {
        self.cancellation = Some(cancellation);
    }

    pub fn begin_optional(&self) {
        self.optional.store(true, Ordering::Relaxed);
    }

    pub(crate) fn incumbent_phase(self: &Arc<Self>) -> IncumbentPhase {
        IncumbentPhase {
            control: self.clone(),
            previous_optional: self.optional.swap(false, Ordering::Relaxed),
        }
    }

    /// False asks a caller to stop at its transaction boundary. The incumbent
    /// phase still checks cancellation, but cannot return an absent baseline.
    pub fn checkpoint(&self) -> Result<bool> {
        if let Some(cancellation) = &self.cancellation {
            cancellation.check()?;
        }
        if !self.optional.load(Ordering::Relaxed) {
            return Ok(true);
        }
        if self
            .optional_time_limit
            .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            let elapsed_us = self.elapsed_us();
            let _ = self.deadline_at_us.compare_exchange(
                u64::MAX,
                elapsed_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            self.deadline_reached.store(true, Ordering::Relaxed);
        }
        Ok(!self.deadline_reached.load(Ordering::Relaxed))
    }

    pub fn deadline_reached(&self) -> bool {
        self.deadline_reached.load(Ordering::Relaxed)
    }

    /// Elapsed time from creation of the query-local control clock.  The
    /// optional deadline intentionally includes incumbent construction, so
    /// reporting must use this clock rather than a phase-local timer.
    pub fn elapsed_us(&self) -> u64 {
        self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
    }

    pub fn optional_time_limit_us(&self) -> Option<u64> {
        self.optional_time_limit
            .map(|limit| limit.as_micros().min(u128::from(u64::MAX)) as u64)
    }

    /// Elapsed time at the first checkpoint that observed the deadline.
    pub fn deadline_elapsed_us(&self) -> Option<u64> {
        match self.deadline_at_us.load(Ordering::Relaxed) {
            u64::MAX => None,
            elapsed_us => Some(elapsed_us),
        }
    }

    #[cfg(test)]
    pub(crate) fn expire(&self) {
        let _ = self.deadline_at_us.compare_exchange(
            u64::MAX,
            self.elapsed_us(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.deadline_reached.store(true, Ordering::Relaxed);
    }
}
