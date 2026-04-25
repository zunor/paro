// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned retained query results.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryError, MemoryOwner,
    MemoryResult,
};
use paro_execution::memory_runtime::{MemoryArbitrator, RetainedChunkVec};

fn saturating_sub(counter: &AtomicUsize, bytes: usize) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(bytes))
    });
}

#[derive(Debug)]
pub struct SessionMemoryBudget {
    capacity_bytes: AtomicUsize,
    issued_bytes: AtomicUsize,
    retained_bytes: AtomicUsize,
    arbitrator: Arc<MemoryArbitrator>,
}

impl SessionMemoryBudget {
    pub fn new(capacity_bytes: usize, arbitrator: Arc<MemoryArbitrator>) -> Self {
        Self {
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            issued_bytes: AtomicUsize::new(0),
            retained_bytes: AtomicUsize::new(0),
            arbitrator,
        }
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes.load(Ordering::Acquire)
    }

    pub fn set_capacity_bytes(&self, bytes: usize) {
        self.capacity_bytes.store(bytes, Ordering::Release);
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }

    pub fn issued_bytes(&self) -> usize {
        self.issued_bytes.load(Ordering::Acquire)
    }

    pub fn available_bytes(&self) -> usize {
        self.capacity_bytes().saturating_sub(self.issued_bytes())
    }
}

impl MemoryOwner for SessionMemoryBudget {
    fn acquire_capacity(&self, domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut issued = self.issued_bytes.load(Ordering::Acquire);
        loop {
            let capacity = self.capacity_bytes();
            let Some(next) = issued.checked_add(bytes) else {
                return Err(MemoryError::quota_exhausted(domain, bytes, 0));
            };
            if next > capacity {
                return Err(MemoryError::quota_exhausted(
                    domain,
                    bytes,
                    capacity.saturating_sub(issued),
                ));
            }
            match self.issued_bytes.compare_exchange_weak(
                issued,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => issued = actual,
            }
        }
    }

    fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
        saturating_sub(&self.issued_bytes, bytes);
    }

    fn record_allocation(
        &self,
        _domain: MemoryDomain,
        _tag: MemoryTag,
        _class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        self.retained_bytes.fetch_add(bytes, Ordering::AcqRel);
        self.arbitrator.add_session_retained_bytes(bytes);
    }

    fn release_allocation(
        &self,
        _domain: MemoryDomain,
        _tag: MemoryTag,
        _class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        saturating_sub(&self.retained_bytes, bytes);
        self.arbitrator.release_session_retained_bytes(bytes);
    }
}

#[derive(Clone)]
pub struct SessionRetainedResultStore {
    inner: Arc<Mutex<SessionRetainedResultInner>>,
}

#[derive(Debug)]
struct SessionRetainedResultInner {
    chunks: RetainedChunkVec,
    row_count: usize,
}

impl SessionRetainedResultStore {
    pub fn new(session_budget: Arc<SessionMemoryBudget>) -> Self {
        let owner: Arc<dyn MemoryOwner> = session_budget;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
        );
        Self {
            inner: Arc::new(Mutex::new(SessionRetainedResultInner {
                chunks: RetainedChunkVec::new(memory),
                row_count: 0,
            })),
        }
    }

    pub fn from_chunks(
        session_budget: Arc<SessionMemoryBudget>,
        chunks: Vec<Chunk>,
    ) -> MemoryResult<Self> {
        let store = Self::new(session_budget);
        for chunk in chunks {
            store.append(chunk)?;
        }
        Ok(store)
    }

    pub fn append(&self, chunk: Chunk) -> MemoryResult<()> {
        let mut inner = self
            .inner
            .lock()
            .expect("session retained result store lock poisoned");
        inner.row_count = inner.row_count.saturating_add(chunk.len());
        if let Err(err) = inner.chunks.push(chunk) {
            inner.row_count = inner.chunks.row_count();
            return Err(err);
        }
        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.inner
            .lock()
            .expect("session retained result store lock poisoned")
            .row_count
    }

    pub fn retained_bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("session retained result store lock poisoned")
            .chunks
            .retained_bytes()
    }

    pub fn chunk_range(&self, start: usize, end: usize) -> Result<Vec<Chunk>, String> {
        if start >= end {
            return Ok(Vec::new());
        }

        let inner = self
            .inner
            .lock()
            .expect("session retained result store lock poisoned");
        let mut out = Vec::new();
        let mut offset = 0usize;
        for chunk in inner.chunks.iter() {
            if offset >= end {
                break;
            }
            let chunk_end = offset + chunk.len();
            if chunk_end <= start {
                offset = chunk_end;
                continue;
            }

            let slice_start = start.saturating_sub(offset);
            let slice_end = (end - offset).min(chunk.len());
            let mut sliced = chunk.clone();
            sliced
                .try_slice_range(slice_start, slice_end - slice_start)
                .map_err(|err| err.to_string())?;
            out.push(sliced);
            offset = chunk_end;
        }
        Ok(out)
    }
}

impl std::fmt::Debug for SessionRetainedResultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self
            .inner
            .lock()
            .expect("session retained result store lock poisoned");
        f.debug_struct("SessionRetainedResultStore")
            .field("chunks", &inner.chunks.len())
            .field("row_count", &inner.row_count)
            .field("retained_bytes", &inner.chunks.retained_bytes())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::test_utils::{test_chunk_from_vectors, test_i32_vector};

    #[test]
    fn retained_store_counts_rows_and_releases_session_bytes_on_drop() {
        let arbitrator = Arc::new(MemoryArbitrator::new(1024 * 1024));
        let budget = Arc::new(SessionMemoryBudget::new(1024 * 1024, arbitrator.clone()));
        let store = SessionRetainedResultStore::new(budget.clone());
        let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]);
        let retained_bytes = chunk.get_allocation_size();

        store.append(chunk).unwrap();
        assert_eq!(store.row_count(), 3);
        assert_eq!(store.retained_bytes(), retained_bytes);
        assert!(budget.retained_bytes() >= retained_bytes);
        assert_eq!(arbitrator.session_retained_bytes(), budget.retained_bytes());

        let rows = store.chunk_range(1, 3).unwrap();
        assert_eq!(rows.iter().map(Chunk::len).sum::<usize>(), 2);

        drop(store);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(arbitrator.session_retained_bytes(), 0);
    }
}
