// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-pool memory reclaim protocol.

use std::fmt;
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::memory::{MemoryError, MemoryResult};
use paro_scheduler::task::InterruptState;
use paro_storage::buffer::BufferPool;

/// Relative cost of freeing memory from a reclaimer.
///
/// Lower-cost reclaimers are tried first. The variants intentionally describe
/// ownership shape rather than concrete operators, so new spillable structures
/// can join the same ordering without expanding the trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpillCost {
    /// Pure accounting or already-materialized spill state.
    AccountingRelease,
    /// Cache eviction that does not change query semantics.
    CacheEviction,
    /// Move operator payload to spillable storage.
    SpillToDisk,
    /// Repartition/rebuild before payload can be reclaimed.
    Repartition,
}

impl Default for SpillCost {
    fn default() -> Self {
        Self::SpillToDisk
    }
}

/// Result of one reclaim attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimStats {
    pub requested_bytes: usize,
    pub reclaimed_bytes: usize,
    pub spilled_bytes: usize,
}

impl ReclaimStats {
    pub fn new(requested_bytes: usize, reclaimed_bytes: usize, spilled_bytes: usize) -> Self {
        Self {
            requested_bytes,
            reclaimed_bytes,
            spilled_bytes,
        }
    }

    pub fn empty(requested_bytes: usize) -> Self {
        Self::new(requested_bytes, 0, 0)
    }
}

/// Asynchronous reclaim completion handle.
///
/// The handle is idempotent: completing it more than once keeps the first result.
/// Waiters are scheduler interrupt states; when reclaim completes, each waiter is
/// called exactly once. Dropping the handle does not cancel work because spill
/// file ownership stays with the reclaimer/operator that started it.
#[derive(Clone)]
pub struct ReclaimHandle {
    inner: Arc<ReclaimHandleInner>,
}

struct ReclaimHandleInner {
    name: String,
    result: Mutex<Option<MemoryResult<ReclaimStats>>>,
    waiters: Mutex<Vec<InterruptState>>,
}

impl ReclaimHandle {
    pub fn pending(name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ReclaimHandleInner {
                name: name.into(),
                result: Mutex::new(None),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn ready(name: impl Into<String>, result: MemoryResult<ReclaimStats>) -> Self {
        let handle = Self::pending(name);
        handle.complete(result);
        handle
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn is_complete(&self) -> bool {
        self.inner.result.lock().unwrap().is_some()
    }

    pub fn result(&self) -> Option<MemoryResult<ReclaimStats>> {
        self.inner.result.lock().unwrap().clone()
    }

    pub fn reclaimed_bytes(&self) -> Option<usize> {
        self.result()
            .and_then(|result| result.ok())
            .map(|stats| stats.reclaimed_bytes)
    }

    pub fn wait(&self, interrupt: InterruptState) {
        if self.is_complete() {
            let _ = interrupt.callback();
            return;
        }

        let mut waiters = self.inner.waiters.lock().unwrap();
        if self.is_complete() {
            drop(waiters);
            let _ = interrupt.callback();
            return;
        }
        waiters.push(interrupt);
    }

    pub fn complete(&self, result: MemoryResult<ReclaimStats>) {
        {
            let mut slot = self.inner.result.lock().unwrap();
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }

        let waiters = {
            let mut waiters = self.inner.waiters.lock().unwrap();
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters {
            let _ = waiter.callback();
        }
    }
}

impl fmt::Debug for ReclaimHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReclaimHandle")
            .field("name", &self.name())
            .field("is_complete", &self.is_complete())
            .finish()
    }
}

/// Outcome of an operator-level capacity grow attempt.
#[derive(Debug, Clone)]
pub enum GrowOutcome {
    Granted,
    Blocked(ReclaimHandle),
}

impl GrowOutcome {
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked(_))
    }
}

/// Reclaim callback registered with a query pool.
///
/// Implementations must be idempotent: calling reclaim with the same target
/// after capacity was already freed returns zero rather than failing. Synchronous
/// reclaim should make bounded progress or return zero. Asynchronous reclaim
/// returns a `ReclaimHandle`; cancellation is cooperative and best-effort, and
/// spill file ownership remains with the operator/storage structure until its
/// normal drop path consumes or deletes those files.
pub trait Reclaimer: Send + Sync + fmt::Debug {
    fn name(&self) -> &str;

    fn reclaimable_bytes(&self) -> usize;

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats>;

    fn start_reclaim(
        &self,
        target_bytes: usize,
        interrupt: Option<InterruptState>,
    ) -> MemoryResult<ReclaimHandle> {
        let handle = ReclaimHandle::ready(self.name(), self.reclaim_sync(target_bytes));
        if let Some(interrupt) = interrupt {
            if !handle.is_complete() {
                handle.wait(interrupt);
            }
        }
        Ok(handle)
    }

    fn spill_cost(&self) -> SpillCost;
}

/// Shared cache eviction reclaimer backed by the process buffer pool.
#[derive(Debug)]
pub struct BufferPoolReclaimer {
    name: String,
    buffer_pool: Arc<BufferPool>,
    tag: MemoryTag,
}

impl BufferPoolReclaimer {
    pub fn new(buffer_pool: Arc<BufferPool>, tag: MemoryTag) -> Self {
        Self {
            name: format!("shared_cache:{tag:?}"),
            buffer_pool,
            tag,
        }
    }
}

impl Reclaimer for BufferPoolReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.buffer_pool.used_memory()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(0));
        }

        let before = self.buffer_pool.used_memory();
        if before == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        let limit = before.saturating_sub(target_bytes);
        let result = self.buffer_pool.evict_blocks(self.tag, 0, limit, None);
        let after = self.buffer_pool.used_memory();
        let reclaimed = before.saturating_sub(after);
        if reclaimed == 0 && !result.success {
            return Err(MemoryError::reclaim_failed(format!(
                "buffer pool reclaimer {} could not evict {} bytes",
                self.name, target_bytes
            )));
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, 0))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::CacheEviction
    }
}
