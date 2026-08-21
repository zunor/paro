// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-query logical memory pool.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use paro_common::allocator::{MemoryTag, MEMORY_TAG_COUNT};
use paro_common::memory::{
    MemoryAccountingClass, MemoryDomain, MemoryError, MemoryOwner, MemoryResult,
    MEMORY_DOMAIN_COUNT,
};
use paro_context::{QueryMemoryRegistration, QueryMemoryTarget};

use super::{
    GrowOutcome, MemoryDomainTagBytes, MemoryRuntimeStats, MemoryTagBytes,
    PipelineAdmissionController, ReclaimHandle, ReclaimStats, Reclaimer, SpillCost,
};

const DEFAULT_UNBOUNDED_QUERY_CAPACITY: usize = usize::MAX / 4;
const CAPACITY_WRITE_LOCK: usize = 1usize << (usize::BITS - 1);
const CAPACITY_READER_MASK: usize = CAPACITY_WRITE_LOCK - 1;

struct CapacityReadGuard<'a>(&'a AtomicUsize);

impl Drop for CapacityReadGuard<'_> {
    fn drop(&mut self) {
        let previous = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & CAPACITY_READER_MASK > 0);
    }
}

struct CapacityWriteGuard<'a>(&'a AtomicUsize);

impl Drop for CapacityWriteGuard<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// Per-query memory pool. Capacity checks use issued bytes, not observed usage.
pub struct QueryMemoryPool {
    capacity_bytes: AtomicUsize,
    capacity_gate: AtomicUsize,
    issued_bytes: AtomicUsize,
    non_revocable_bytes: AtomicUsize,
    revocable_bytes: AtomicUsize,
    spill_bytes: AtomicUsize,
    prefetch_bytes: AtomicUsize,
    metadata_bytes: AtomicUsize,
    domain_tag_bytes: [[AtomicUsize; MEMORY_TAG_COUNT]; MEMORY_DOMAIN_COUNT],
    leaked_grant_bytes: AtomicUsize,
    local_refill_count: AtomicUsize,
    local_refill_bytes: AtomicUsize,
    reclaim_attempt_count: AtomicUsize,
    reclaimed_bytes: AtomicUsize,
    reclaim_spilled_bytes: AtomicUsize,
    reclaim_latency_us: AtomicUsize,
    spill_latency_us: AtomicUsize,
    output_buffer_bytes: AtomicUsize,
    peer_reclaim_in_progress: AtomicBool,
    reclaimers: Mutex<Vec<Arc<dyn Reclaimer>>>,
    admission: Arc<PipelineAdmissionController>,
    registration: Mutex<Option<QueryMemoryRegistration>>,
}

impl QueryMemoryPool {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            capacity_gate: AtomicUsize::new(0),
            issued_bytes: AtomicUsize::new(0),
            non_revocable_bytes: AtomicUsize::new(0),
            revocable_bytes: AtomicUsize::new(0),
            spill_bytes: AtomicUsize::new(0),
            prefetch_bytes: AtomicUsize::new(0),
            metadata_bytes: AtomicUsize::new(0),
            domain_tag_bytes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicUsize::new(0))),
            leaked_grant_bytes: AtomicUsize::new(0),
            local_refill_count: AtomicUsize::new(0),
            local_refill_bytes: AtomicUsize::new(0),
            reclaim_attempt_count: AtomicUsize::new(0),
            reclaimed_bytes: AtomicUsize::new(0),
            reclaim_spilled_bytes: AtomicUsize::new(0),
            reclaim_latency_us: AtomicUsize::new(0),
            spill_latency_us: AtomicUsize::new(0),
            output_buffer_bytes: AtomicUsize::new(0),
            peer_reclaim_in_progress: AtomicBool::new(false),
            reclaimers: Mutex::new(Vec::new()),
            admission: Arc::new(PipelineAdmissionController::for_current_parallelism()),
            registration: Mutex::new(None),
        }
    }

    pub fn unbounded() -> Self {
        Self::new(DEFAULT_UNBOUNDED_QUERY_CAPACITY)
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes.load(Ordering::Relaxed)
    }

    pub fn set_capacity_bytes(&self, capacity_bytes: usize) {
        let _guard = self.capacity_write_guard();
        self.capacity_bytes.store(capacity_bytes, Ordering::Release);
    }

    fn relinquish_unused_capacity(&self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }

        let _guard = self.capacity_write_guard();
        let capacity = self.capacity_bytes.load(Ordering::Acquire);
        let issued = self.issued_bytes.load(Ordering::Acquire);
        let relinquished = target_bytes.min(capacity.saturating_sub(issued));
        if relinquished > 0 {
            self.capacity_bytes
                .store(capacity - relinquished, Ordering::Release);
        }
        relinquished
    }

    fn grant_capacity(&self, target_bytes: usize, max_capacity: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }

        let _guard = self.capacity_write_guard();
        let capacity = self.capacity_bytes.load(Ordering::Acquire);
        let granted = target_bytes.min(max_capacity.saturating_sub(capacity));
        if granted > 0 {
            self.capacity_bytes
                .store(capacity + granted, Ordering::Release);
        }
        granted
    }

    fn capacity_read_guard(&self) -> CapacityReadGuard<'_> {
        let mut state = self.capacity_gate.load(Ordering::Acquire);
        loop {
            if state & CAPACITY_WRITE_LOCK != 0
                || state & CAPACITY_READER_MASK == CAPACITY_READER_MASK
            {
                std::hint::spin_loop();
                state = self.capacity_gate.load(Ordering::Acquire);
                continue;
            }
            match self.capacity_gate.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return CapacityReadGuard(&self.capacity_gate),
                Err(actual) => state = actual,
            }
        }
    }

    fn capacity_write_guard(&self) -> CapacityWriteGuard<'_> {
        let mut state = self.capacity_gate.load(Ordering::Acquire);
        loop {
            if state & CAPACITY_WRITE_LOCK != 0 {
                std::hint::spin_loop();
                state = self.capacity_gate.load(Ordering::Acquire);
                continue;
            }
            match self.capacity_gate.compare_exchange_weak(
                state,
                state | CAPACITY_WRITE_LOCK,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        while self.capacity_gate.load(Ordering::Acquire) != CAPACITY_WRITE_LOCK {
            std::hint::spin_loop();
        }
        CapacityWriteGuard(&self.capacity_gate)
    }

    pub fn attach_registration(&self, registration: QueryMemoryRegistration) {
        *self
            .registration
            .lock()
            .expect("query memory registration lock poisoned") = Some(registration);
    }

    pub fn registered_query_id(&self) -> Option<u64> {
        self.registration()
            .as_ref()
            .map(QueryMemoryRegistration::query_id)
    }

    pub fn detach_registration(&self) {
        if let Some(registration) = self
            .registration
            .lock()
            .expect("query memory registration lock poisoned")
            .take()
        {
            registration
                .coordinator()
                .unregister_query(registration.query_id());
        }
    }

    pub fn issued_bytes(&self) -> usize {
        self.issued_bytes.load(Ordering::Relaxed)
    }

    pub fn published_used_bytes(&self) -> usize {
        self.non_revocable_bytes()
            .saturating_add(self.revocable_bytes())
            .saturating_add(self.spill_bytes())
            .saturating_add(self.prefetch_bytes())
            .saturating_add(self.metadata_bytes())
    }

    pub fn non_revocable_bytes(&self) -> usize {
        self.non_revocable_bytes.load(Ordering::Relaxed)
    }

    pub fn revocable_bytes(&self) -> usize {
        self.revocable_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaimable_bytes(&self) -> usize {
        self.revocable_bytes()
            .saturating_add(self.prefetch_bytes.load(Ordering::Relaxed))
    }

    pub fn spill_bytes(&self) -> usize {
        self.spill_bytes.load(Ordering::Relaxed)
    }

    pub fn prefetch_bytes(&self) -> usize {
        self.prefetch_bytes.load(Ordering::Relaxed)
    }

    pub fn metadata_bytes(&self) -> usize {
        self.metadata_bytes.load(Ordering::Relaxed)
    }

    pub fn leaked_grant_bytes(&self) -> usize {
        self.leaked_grant_bytes.load(Ordering::Relaxed)
    }

    pub fn local_refill_count(&self) -> usize {
        self.local_refill_count.load(Ordering::Relaxed)
    }

    pub fn local_refill_bytes(&self) -> usize {
        self.local_refill_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaim_attempt_count(&self) -> usize {
        self.reclaim_attempt_count.load(Ordering::Relaxed)
    }

    pub fn reclaimed_bytes(&self) -> usize {
        self.reclaimed_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaim_spilled_bytes(&self) -> usize {
        self.reclaim_spilled_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaim_latency_us(&self) -> usize {
        self.reclaim_latency_us.load(Ordering::Relaxed)
    }

    pub fn spill_latency_us(&self) -> usize {
        self.spill_latency_us.load(Ordering::Relaxed)
    }

    pub fn output_buffer_bytes(&self) -> usize {
        self.output_buffer_bytes.load(Ordering::Relaxed)
    }

    pub fn tag_memory_snapshot(&self) -> Vec<MemoryTagBytes> {
        MemoryTag::all()
            .iter()
            .filter_map(|tag| {
                let bytes = MemoryDomain::all().iter().fold(0usize, |total, domain| {
                    total.saturating_add(
                        self.domain_tag_bytes[domain.as_index()][tag.as_index()]
                            .load(Ordering::Relaxed),
                    )
                });
                (bytes > 0).then_some(MemoryTagBytes { tag: *tag, bytes })
            })
            .collect()
    }

    pub fn domain_tag_memory_snapshot(&self) -> Vec<MemoryDomainTagBytes> {
        MemoryDomain::all()
            .iter()
            .flat_map(|domain| {
                MemoryTag::all().iter().filter_map(move |tag| {
                    let bytes = self.domain_tag_bytes[domain.as_index()][tag.as_index()]
                        .load(Ordering::Relaxed);
                    (bytes > 0).then_some(MemoryDomainTagBytes {
                        domain: *domain,
                        tag: *tag,
                        bytes,
                    })
                })
            })
            .collect()
    }

    pub fn runtime_stats(&self) -> MemoryRuntimeStats {
        MemoryRuntimeStats {
            capacity_bytes: self.capacity_bytes(),
            issued_bytes: self.issued_bytes(),
            published_used_bytes: self.published_used_bytes(),
            non_revocable_bytes: self.non_revocable_bytes(),
            revocable_bytes: self.revocable_bytes(),
            spill_bytes: self.spill_bytes(),
            prefetch_bytes: self.prefetch_bytes(),
            metadata_bytes: self.metadata_bytes(),
            reclaimable_bytes: self.reclaimable_bytes(),
            leaked_grant_bytes: self.leaked_grant_bytes(),
            local_refill_count: self.local_refill_count(),
            local_refill_bytes: self.local_refill_bytes(),
            reclaim_attempt_count: self.reclaim_attempt_count(),
            reclaimed_bytes: self.reclaimed_bytes(),
            spilled_bytes: self.reclaim_spilled_bytes(),
            reclaim_latency_us: self.reclaim_latency_us(),
            spill_latency_us: self.spill_latency_us(),
            output_buffer_bytes: self.output_buffer_bytes(),
            tag_bytes: self.tag_memory_snapshot(),
            domain_tag_bytes: self.domain_tag_memory_snapshot(),
        }
    }

    pub fn available_bytes(&self) -> usize {
        self.capacity_bytes()
            .saturating_sub(self.issued_bytes.load(Ordering::Relaxed))
    }

    pub fn admission_controller(&self) -> Arc<PipelineAdmissionController> {
        self.admission.clone()
    }

    pub fn register_reclaimer(&self, reclaimer: Arc<dyn Reclaimer>) {
        let mut reclaimers = self
            .reclaimers
            .lock()
            .expect("query memory reclaimer lock poisoned");
        if reclaimers
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &reclaimer))
        {
            return;
        }
        reclaimers.push(reclaimer);
    }

    pub fn register_reclaimer_once_by_name(&self, reclaimer: Arc<dyn Reclaimer>) {
        let mut reclaimers = self
            .reclaimers
            .lock()
            .expect("query memory reclaimer lock poisoned");
        if reclaimers
            .iter()
            .any(|existing| existing.name() == reclaimer.name())
        {
            return;
        }
        reclaimers.push(reclaimer);
    }

    pub fn unregister_reclaimer_by_name(&self, name: &str) -> usize {
        let mut reclaimers = self
            .reclaimers
            .lock()
            .expect("query memory reclaimer lock poisoned");
        let before = reclaimers.len();
        reclaimers.retain(|reclaimer| reclaimer.name() != name);
        before.saturating_sub(reclaimers.len())
    }

    #[cfg(test)]
    pub fn reclaimer_count(&self) -> usize {
        self.reclaimers
            .lock()
            .expect("query memory reclaimer lock poisoned")
            .len()
    }

    pub fn try_grow(&self, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut reclaimed_once = false;
        loop {
            match self.try_issue(bytes) {
                Ok(()) => return Ok(()),
                Err(err @ MemoryError::QuotaExhausted { .. }) if !reclaimed_once => {
                    reclaimed_once = true;
                    let target = quota_deficit(&err);
                    if self.reclaim(target)? > 0 {
                        continue;
                    }
                    if self.request_peer_capacity(target)? > 0 {
                        continue;
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn try_grow_or_block(
        &self,
        bytes: usize,
        interrupt: Option<paro_scheduler::task::InterruptState>,
    ) -> MemoryResult<GrowOutcome> {
        if bytes == 0 {
            return Ok(GrowOutcome::Granted);
        }

        match self.try_issue(bytes) {
            Ok(()) => Ok(GrowOutcome::Granted),
            Err(err @ MemoryError::QuotaExhausted { .. }) => {
                let target = quota_deficit(&err);
                let Some(handle) = self.start_reclaim(target, interrupt)? else {
                    if self.request_peer_capacity(target)? > 0 {
                        self.try_grow(bytes)?;
                        return Ok(GrowOutcome::Granted);
                    }
                    return Err(err);
                };
                match handle.result() {
                    Some(Ok(stats)) if stats.reclaimed_bytes > 0 => {
                        self.try_grow(bytes)?;
                        Ok(GrowOutcome::Granted)
                    }
                    Some(Ok(_)) => {
                        if self.request_peer_capacity(target)? > 0 {
                            self.try_grow(bytes)?;
                            Ok(GrowOutcome::Granted)
                        } else {
                            Err(err)
                        }
                    }
                    Some(Err(reclaim_err)) => Err(reclaim_err),
                    None => Ok(GrowOutcome::Blocked(handle)),
                }
            }
            Err(err) => Err(err),
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .issued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    pub fn reclaim(&self, target_bytes: usize) -> MemoryResult<usize> {
        if target_bytes == 0 {
            return Ok(0);
        }

        let mut reclaimed = 0usize;
        let mut first_error = None;
        for reclaimer in self.reclaimers_by_cost() {
            if reclaimed < target_bytes {
                if reclaimer.reclaimable_bytes() == 0 {
                    continue;
                }
                self.reclaim_attempt_count.fetch_add(1, Ordering::Relaxed);
                let started_at = Instant::now();
                match reclaimer.reclaim_sync(target_bytes - reclaimed) {
                    Ok(stats) => {
                        self.record_reclaim_stats(&stats, started_at);
                        reclaimed = reclaimed.saturating_add(stats.reclaimed_bytes)
                    }
                    Err(err) => {
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            }
        }
        if reclaimed == 0 {
            if let Some(err) = first_error {
                return Err(err);
            }
        }
        Ok(reclaimed)
    }

    pub fn start_reclaim(
        &self,
        target_bytes: usize,
        interrupt: Option<paro_scheduler::task::InterruptState>,
    ) -> MemoryResult<Option<ReclaimHandle>> {
        if target_bytes == 0 {
            return Ok(None);
        }

        let mut first_error = None;
        for reclaimer in self.reclaimers_by_cost() {
            if reclaimer.reclaimable_bytes() == 0 {
                continue;
            }
            self.reclaim_attempt_count.fetch_add(1, Ordering::Relaxed);
            let started_at = Instant::now();
            match reclaimer.start_reclaim(target_bytes, interrupt.clone()) {
                Ok(handle) => {
                    if let Some(Ok(stats)) = handle.result() {
                        self.record_reclaim_stats(&stats, started_at);
                        if stats.reclaimed_bytes == 0 {
                            continue;
                        }
                    }
                    return Ok(Some(handle));
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(None)
    }

    fn try_issue(&self, bytes: usize) -> MemoryResult<()> {
        // Capacity mutations take the write side of this gate. Keeping the
        // read side across the issued CAS prevents a concurrent shrink from
        // donating headroom that this request has already consumed.
        let _guard = self.capacity_read_guard();
        let mut current = self.issued_bytes.load(Ordering::Relaxed);
        loop {
            let capacity = self.capacity_bytes();
            let Some(next) = current.checked_add(bytes) else {
                return Err(MemoryError::quota_exhausted(
                    MemoryDomain::Host,
                    bytes,
                    capacity.saturating_sub(current),
                ));
            };
            if next > capacity {
                return Err(MemoryError::quota_exhausted(
                    MemoryDomain::Host,
                    bytes,
                    capacity.saturating_sub(current),
                ));
            }
            match self.issued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    fn reclaimers_by_cost(&self) -> Vec<Arc<dyn Reclaimer>> {
        let reclaimers = self
            .reclaimers
            .lock()
            .expect("query memory reclaimer lock poisoned")
            .clone();
        let mut accounting = Vec::new();
        let mut cache = Vec::new();
        let mut spill = Vec::new();
        let mut repartition = Vec::new();
        for reclaimer in reclaimers {
            match reclaimer.spill_cost() {
                SpillCost::AccountingRelease => accounting.push(reclaimer),
                SpillCost::CacheEviction => cache.push(reclaimer),
                SpillCost::SpillToDisk => spill.push(reclaimer),
                SpillCost::Repartition => repartition.push(reclaimer),
            }
        }
        accounting.extend(cache);
        accounting.extend(spill);
        accounting.extend(repartition);
        accounting
    }

    fn registration(&self) -> Option<QueryMemoryRegistration> {
        self.registration
            .lock()
            .expect("query memory registration lock poisoned")
            .clone()
    }

    fn request_peer_capacity(&self, target_bytes: usize) -> MemoryResult<usize> {
        struct PeerReclaimGuard<'a>(&'a AtomicBool);

        impl Drop for PeerReclaimGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        if self.peer_reclaim_in_progress.swap(true, Ordering::AcqRel) {
            return Ok(0);
        }
        let _guard = PeerReclaimGuard(&self.peer_reclaim_in_progress);

        let Some(registration) = self.registration() else {
            return Ok(0);
        };
        registration
            .coordinator()
            .request_additional_capacity(registration.query_id(), target_bytes)
    }

    fn record_reclaim_stats(&self, stats: &ReclaimStats, started_at: Instant) {
        let elapsed_us = started_at.elapsed().as_micros() as usize;
        self.reclaimed_bytes
            .fetch_add(stats.reclaimed_bytes, Ordering::Relaxed);
        self.reclaim_spilled_bytes
            .fetch_add(stats.spilled_bytes, Ordering::Relaxed);
        self.reclaim_latency_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        if stats.spilled_bytes > 0 {
            self.spill_latency_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
        }
    }
}

impl QueryMemoryTarget for QueryMemoryPool {
    fn capacity_bytes(&self) -> usize {
        QueryMemoryPool::capacity_bytes(self)
    }

    fn set_capacity_bytes(&self, bytes: usize) {
        QueryMemoryPool::set_capacity_bytes(self, bytes);
    }

    fn relinquish_unused_capacity(&self, bytes: usize) -> usize {
        QueryMemoryPool::relinquish_unused_capacity(self, bytes)
    }

    fn grant_capacity(&self, bytes: usize, max_capacity: usize) -> usize {
        QueryMemoryPool::grant_capacity(self, bytes, max_capacity)
    }

    fn issued_bytes(&self) -> usize {
        QueryMemoryPool::issued_bytes(self)
    }

    fn reclaimable_bytes(&self) -> usize {
        QueryMemoryPool::reclaimable_bytes(self)
    }

    fn reclaim(&self, target_bytes: usize) -> MemoryResult<usize> {
        QueryMemoryPool::reclaim(self, target_bytes)
    }
}

impl MemoryOwner for QueryMemoryPool {
    fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
        self.try_grow(bytes)
    }

    fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
        self.release(bytes);
    }

    fn record_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        self.add_class_bytes(class, bytes);
        self.add_tag_bytes(domain, tag, bytes);
    }

    fn release_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        self.sub_class_bytes(class, bytes);
        self.sub_tag_bytes(domain, tag, bytes);
    }

    fn reclassify_allocation(
        &self,
        _domain: MemoryDomain,
        _tag: MemoryTag,
        from: MemoryAccountingClass,
        to: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 || from == to {
            return;
        }
        self.sub_class_bytes(from, bytes);
        self.add_class_bytes(to, bytes);
    }

    fn record_leaked_grant(&self, _domain: MemoryDomain, bytes: usize) {
        if bytes > 0 {
            self.leaked_grant_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn record_local_refill(&self, _domain: MemoryDomain, bytes: usize) {
        if bytes > 0 {
            self.local_refill_count.fetch_add(1, Ordering::Relaxed);
            self.local_refill_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn record_output_buffer_bytes(&self, _domain: MemoryDomain, bytes: usize) {
        let _ = self.output_buffer_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(bytes)),
        );
    }
}

fn quota_deficit(err: &MemoryError) -> usize {
    match err {
        MemoryError::QuotaExhausted {
            requested,
            available,
            ..
        } => requested.saturating_sub(*available),
        _ => 0,
    }
}

fn saturating_sub(counter: &AtomicUsize, bytes: usize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
}

impl QueryMemoryPool {
    fn add_class_bytes(&self, class: MemoryAccountingClass, bytes: usize) {
        self.class_counter(class)
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn sub_class_bytes(&self, class: MemoryAccountingClass, bytes: usize) {
        let _ = self.class_counter(class).fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    fn add_tag_bytes(&self, domain: MemoryDomain, tag: MemoryTag, bytes: usize) {
        self.domain_tag_bytes[domain.as_index()][tag.as_index()]
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn sub_tag_bytes(&self, domain: MemoryDomain, tag: MemoryTag, bytes: usize) {
        saturating_sub(
            &self.domain_tag_bytes[domain.as_index()][tag.as_index()],
            bytes,
        );
    }

    fn class_counter(&self, class: MemoryAccountingClass) -> &AtomicUsize {
        match class {
            MemoryAccountingClass::NonRevocable => &self.non_revocable_bytes,
            MemoryAccountingClass::Revocable => &self.revocable_bytes,
            MemoryAccountingClass::Spill => &self.spill_bytes,
            MemoryAccountingClass::Prefetch => &self.prefetch_bytes,
            MemoryAccountingClass::Metadata => &self.metadata_bytes,
        }
    }
}

impl fmt::Debug for QueryMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMemoryPool")
            .field("capacity_bytes", &self.capacity_bytes())
            .field("issued_bytes", &self.issued_bytes())
            .field("published_used_bytes", &self.published_used_bytes())
            .field("non_revocable_bytes", &self.non_revocable_bytes())
            .field("revocable_bytes", &self.revocable_bytes())
            .field("spill_bytes", &self.spill_bytes())
            .field("prefetch_bytes", &self.prefetch_bytes())
            .field("metadata_bytes", &self.metadata_bytes())
            .field("leaked_grant_bytes", &self.leaked_grant_bytes())
            .field("local_refill_count", &self.local_refill_count())
            .field("local_refill_bytes", &self.local_refill_bytes())
            .field("reclaim_attempt_count", &self.reclaim_attempt_count())
            .field("reclaimed_bytes", &self.reclaimed_bytes())
            .field("reclaim_spilled_bytes", &self.reclaim_spilled_bytes())
            .field("admission_used_slots", &self.admission.used_slots())
            .field("admission_max_slots", &self.admission.max_slots())
            .finish()
    }
}

impl Drop for QueryMemoryPool {
    fn drop(&mut self) {
        if let Ok(mut registration) = self.registration.lock() {
            if let Some(registration) = registration.take() {
                registration
                    .coordinator()
                    .unregister_query(registration.query_id());
            }
        }
    }
}
