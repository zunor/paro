// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Thread-affine batched memory delta accumulator.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

/// Batches small allocation deltas before publishing to shared counters.
#[derive(Debug)]
pub struct MemoryAccumulator {
    pending_delta: Cell<isize>,
    effective_threshold: usize,
    force_flush_threshold: usize,
    _not_send: PhantomData<Rc<()>>,
}

impl MemoryAccumulator {
    pub fn new(effective_threshold: usize, force_flush_threshold: usize) -> Self {
        Self {
            pending_delta: Cell::new(0),
            effective_threshold,
            force_flush_threshold,
            _not_send: PhantomData,
        }
    }

    pub fn with_default_thresholds() -> Self {
        Self::new(64 * 1024, 256 * 1024)
    }

    #[inline]
    pub fn pending_delta(&self) -> isize {
        self.pending_delta.get()
    }

    pub fn record(&self, delta: isize) -> Option<isize> {
        let next = self.pending_delta.get().saturating_add(delta);
        self.pending_delta.set(next);
        if self.should_flush(next) {
            Some(self.take_pending())
        } else {
            None
        }
    }

    pub fn force_flush(&self) -> Option<isize> {
        let pending = self.take_pending();
        if pending == 0 {
            None
        } else {
            Some(pending)
        }
    }

    fn should_flush(&self, value: isize) -> bool {
        let positive = value >= self.effective_threshold as isize;
        let negative = value <= -(self.force_flush_threshold as isize);
        positive || negative
    }

    fn take_pending(&self) -> isize {
        self.pending_delta.replace(0)
    }
}

impl Default for MemoryAccumulator {
    fn default() -> Self {
        Self::with_default_thresholds()
    }
}
