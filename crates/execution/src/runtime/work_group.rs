// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared completion accounting for scheduler-dispatched task groups.

use std::sync::{Condvar, Mutex};

use paro_common::error::{ParoError, Result};

use super::RUNTIME_WAIT_TIMEOUT;

pub(crate) struct WorkGroupCompletion {
    inner: Mutex<WorkGroupCompletionInner>,
    cv: Condvar,
}

struct WorkGroupCompletionInner {
    remaining: usize,
    error: Option<ParoError>,
}

impl WorkGroupCompletion {
    pub(crate) fn new(task_count: usize) -> Self {
        Self {
            inner: Mutex::new(WorkGroupCompletionInner {
                remaining: task_count,
                error: None,
            }),
            cv: Condvar::new(),
        }
    }

    pub(crate) fn finish(&self, result: Result<()>) {
        let mut inner = self.inner.lock().expect("work group lock poisoned");
        if let Err(error) = result {
            if inner.error.is_none() {
                inner.error = Some(error);
            }
        }
        inner.remaining = inner.remaining.saturating_sub(1);
        self.cv.notify_all();
    }

    pub(crate) fn cancel_queued(&self, count: usize) {
        if count == 0 {
            return;
        }
        let mut inner = self.inner.lock().expect("work group lock poisoned");
        inner.remaining = inner.remaining.saturating_sub(count);
        self.cv.notify_all();
    }

    pub(crate) fn snapshot(&self) -> Result<Option<usize>> {
        let inner = self.inner.lock().expect("work group lock poisoned");
        if let Some(error) = &inner.error {
            return Err(error.clone());
        }
        if inner.remaining == 0 {
            return Ok(None);
        }
        Ok(Some(inner.remaining))
    }

    pub(crate) fn remaining(&self) -> usize {
        self.inner
            .lock()
            .expect("work group lock poisoned")
            .remaining
    }

    pub(crate) fn wait_for_worker_completion_with_timeout(&self) {
        let guard = self.inner.lock().expect("work group lock poisoned");
        let _ = self
            .cv
            .wait_timeout(guard, RUNTIME_WAIT_TIMEOUT)
            .expect("work group condvar poisoned");
    }
}
