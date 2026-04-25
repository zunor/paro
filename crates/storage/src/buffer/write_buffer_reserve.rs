// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Write-buffer reservation surface for MemTable backpressure.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Shared write-buffer budget used by memtables to decide backpressure.
pub trait WriteBufferReserve: Send + Sync + fmt::Debug {
    fn try_acquire(&self, bytes: usize) -> bool;
    fn release(&self, bytes: usize);
    fn reserved_bytes(&self) -> usize;
}

/// Per-memtable reservation handle.
pub struct WriteBufferReservation {
    reserve: Arc<dyn WriteBufferReserve>,
    bytes: AtomicUsize,
}

impl WriteBufferReservation {
    pub fn new(reserve: Arc<dyn WriteBufferReserve>) -> Self {
        Self {
            reserve,
            bytes: AtomicUsize::new(0),
        }
    }

    pub fn reserved_bytes(&self) -> usize {
        self.bytes.load(Ordering::Acquire)
    }

    pub fn resize(&self, bytes: usize) -> bool {
        let current = self.bytes.load(Ordering::Acquire);
        if bytes == current {
            return true;
        }
        if bytes < current {
            let release = current - bytes;
            self.bytes.store(bytes, Ordering::Release);
            self.reserve.release(release);
            return true;
        }

        let grow = bytes - current;
        if !self.reserve.try_acquire(grow) {
            return false;
        }
        self.bytes.store(bytes, Ordering::Release);
        true
    }

    pub fn clear(&self) {
        let current = self.bytes.swap(0, Ordering::AcqRel);
        self.reserve.release(current);
    }
}

impl fmt::Debug for WriteBufferReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBufferReservation")
            .field("bytes", &self.reserved_bytes())
            .finish_non_exhaustive()
    }
}

impl Drop for WriteBufferReservation {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Fixed-size reserve useful for tests and local storage-only writers.
#[derive(Debug)]
pub struct FixedWriteBufferReserve {
    limit: usize,
    reserved: AtomicUsize,
}

impl FixedWriteBufferReserve {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            reserved: AtomicUsize::new(0),
        }
    }
}

impl WriteBufferReserve for FixedWriteBufferReserve {
    fn try_acquire(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.limit {
                return false;
            }
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    fn reserved_bytes(&self) -> usize {
        self.reserved.load(Ordering::Acquire)
    }
}
