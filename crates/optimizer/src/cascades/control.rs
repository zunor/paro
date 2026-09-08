// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-local cooperative search control, independent of deterministic work
//! credits. A search deadline ends exploration; statement cancellation ends the
//! statement. Neither is an advisory failed equivalence rule.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use paro_common::error::Result;
use paro_context::StatementCancellation;

#[derive(Debug)]
pub struct SearchControl {
    started: Instant,
    optional_time_limit: Option<Duration>,
    optional: AtomicBool,
    deadline_reached: AtomicBool,
    cancellation: Option<StatementCancellation>,
}

impl SearchControl {
    pub fn new(optional_time_limit: Option<Duration>) -> Self {
        Self {
            started: Instant::now(),
            optional_time_limit,
            optional: AtomicBool::new(false),
            deadline_reached: AtomicBool::new(false),
            cancellation: None,
        }
    }

    pub fn set_cancellation(&mut self, cancellation: StatementCancellation) {
        self.cancellation = Some(cancellation);
    }

    pub fn begin_optional(&self) {
        self.optional.store(true, Ordering::Relaxed);
    }

    /// False asks a caller to stop at its transaction boundary. The incumbent
    /// phase still checks cancellation, but cannot return an absent baseline.
    pub fn checkpoint(&self) -> Result<bool> {
        if let Some(cancellation) = &self.cancellation {
            cancellation.check()?;
        }
        if self.optional.load(Ordering::Relaxed)
            && self
                .optional_time_limit
                .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            self.deadline_reached.store(true, Ordering::Relaxed);
        }
        Ok(!self.deadline_reached.load(Ordering::Relaxed))
    }

    pub fn deadline_reached(&self) -> bool {
        self.deadline_reached.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn expire(&self) {
        self.deadline_reached.store(true, Ordering::Relaxed);
    }
}
