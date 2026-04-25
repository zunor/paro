// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Delta accounting for large runtime structures that do not yet allocate
//! through grant-backed allocators.

use std::sync::{Arc, Mutex};

use paro_common::allocator::{Allocator, ArenaAllocator, MemoryTag};
use paro_common::memory::{
    MemoryAccountingClass, MemoryDomain, MemoryError, MemoryOwner, MemoryOwnerAllocator,
    MemoryResult,
};

use super::{LocalMemoryGrant, OperatorMemoryAccount};

#[derive(Debug, Default)]
struct ExternalMemoryState {
    current_bytes: usize,
    peak_bytes: usize,
    minimum_reservation_bytes: usize,
}

/// Single-task external memory tracker backed by a `LocalMemoryGrant`.
#[derive(Debug)]
pub struct LocalExternalMemoryTracker {
    grant: LocalMemoryGrant,
    tag: MemoryTag,
    class: MemoryAccountingClass,
    state: ExternalMemoryState,
}

impl LocalExternalMemoryTracker {
    pub fn new(grant: LocalMemoryGrant, tag: MemoryTag, class: MemoryAccountingClass) -> Self {
        Self {
            grant,
            tag,
            class,
            state: ExternalMemoryState::default(),
        }
    }

    pub fn set_accounted_bytes(&mut self, bytes: usize) -> MemoryResult<()> {
        if bytes > self.state.current_bytes {
            self.grant.retain_external_allocation(
                self.tag,
                self.class,
                bytes - self.state.current_bytes,
            )?;
        } else if self.state.current_bytes > bytes {
            self.grant.release_external_allocation(
                self.tag,
                self.class,
                self.state.current_bytes - bytes,
            );
        }
        self.state.current_bytes = bytes;
        self.state.peak_bytes = self
            .state
            .peak_bytes
            .max(bytes)
            .max(self.state.minimum_reservation_bytes);
        Ok(())
    }

    pub fn clear(&mut self) {
        if self.state.current_bytes > 0 {
            self.grant
                .release_external_allocation(self.tag, self.class, self.state.current_bytes);
            self.state.current_bytes = 0;
        }
        self.state.minimum_reservation_bytes = 0;
    }

    pub fn accounted_bytes(&self) -> usize {
        self.state.current_bytes
    }

    pub fn reservation_bytes(&self) -> usize {
        self.state
            .current_bytes
            .max(self.state.minimum_reservation_bytes)
    }

    pub fn minimum_reservation_bytes(&self) -> usize {
        self.state.minimum_reservation_bytes
    }

    pub fn set_minimum_reservation_bytes(&mut self, bytes: usize) {
        self.state.minimum_reservation_bytes = bytes;
        self.state.peak_bytes = self.state.peak_bytes.max(bytes);
    }

    pub fn peak_bytes(&self) -> usize {
        self.state.peak_bytes
    }

    pub fn accounted_allocator(&self) -> Arc<dyn Allocator> {
        let owner = self
            .grant
            .grant()
            .owner()
            .expect("local external memory tracker must be owner-backed");
        Arc::new(MemoryOwnerAllocator::new(
            self.grant.allocator().clone(),
            owner,
            self.grant.domain(),
            self.tag,
            self.class,
        ))
    }

    pub fn arena_allocator(&self) -> ArenaAllocator {
        ArenaAllocator::new(self.accounted_allocator())
    }
}

impl Drop for LocalExternalMemoryTracker {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Shareable external memory tracker backed by an operator account.
#[derive(Debug)]
pub struct OperatorExternalMemoryTracker {
    account: Arc<OperatorMemoryAccount>,
    domain: MemoryDomain,
    tag: MemoryTag,
    class: MemoryAccountingClass,
    state: Mutex<ExternalMemoryState>,
}

impl OperatorExternalMemoryTracker {
    pub fn new(
        account: Arc<OperatorMemoryAccount>,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            account,
            domain,
            tag,
            class,
            state: Mutex::new(ExternalMemoryState::default()),
        }
    }

    pub fn account(&self) -> Arc<OperatorMemoryAccount> {
        self.account.clone()
    }

    pub fn set_accounted_bytes(&self, bytes: usize) -> MemoryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        if bytes > state.current_bytes {
            self.account.retain_external_allocation(
                self.domain,
                self.tag,
                self.class,
                bytes - state.current_bytes,
            )?;
        } else if state.current_bytes > bytes {
            self.account.release_external_allocation(
                self.domain,
                self.tag,
                self.class,
                state.current_bytes - bytes,
            );
        }
        state.current_bytes = bytes;
        state.peak_bytes = state
            .peak_bytes
            .max(bytes)
            .max(state.minimum_reservation_bytes);
        Ok(())
    }

    pub fn clear_accounted_bytes(&self) -> MemoryResult<()> {
        self.set_accounted_bytes(0)
    }

    pub fn reclaim_accounted_bytes(&self, target_bytes: usize) -> MemoryResult<usize> {
        if target_bytes == 0 {
            return Ok(0);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        let reclaimed = target_bytes.min(state.current_bytes);
        if reclaimed == 0 {
            return Ok(0);
        }
        self.account
            .release_external_allocation(self.domain, self.tag, self.class, reclaimed);
        state.current_bytes -= reclaimed;
        state.minimum_reservation_bytes = state.minimum_reservation_bytes.saturating_sub(reclaimed);
        Ok(reclaimed)
    }

    pub fn clear(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(err) => err.into_inner(),
        };
        if state.current_bytes > 0 {
            self.account.release_external_allocation(
                self.domain,
                self.tag,
                self.class,
                state.current_bytes,
            );
            state.current_bytes = 0;
        }
        state.minimum_reservation_bytes = 0;
    }

    pub fn accounted_bytes(&self) -> MemoryResult<usize> {
        let state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        Ok(state.current_bytes)
    }

    pub fn reservation_bytes(&self) -> MemoryResult<usize> {
        let state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        Ok(state.current_bytes.max(state.minimum_reservation_bytes))
    }

    pub fn remaining_bytes(&self) -> MemoryResult<usize> {
        self.accounted_bytes()
    }

    pub fn minimum_reservation_bytes(&self) -> MemoryResult<usize> {
        let state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        Ok(state.minimum_reservation_bytes)
    }

    pub fn set_minimum_reservation_bytes(&self, bytes: usize) -> MemoryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        state.minimum_reservation_bytes = bytes;
        state.peak_bytes = state.peak_bytes.max(bytes);
        Ok(())
    }

    pub fn peak_bytes(&self) -> MemoryResult<usize> {
        let state = self
            .state
            .lock()
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        Ok(state.peak_bytes)
    }

    pub fn can_acquire_capacity(&self, bytes: usize) -> MemoryResult<bool> {
        if bytes == 0 {
            return Ok(true);
        }
        match self.account.try_grow(bytes) {
            Ok(()) => {
                self.account.release(bytes);
                Ok(true)
            }
            Err(MemoryError::QuotaExhausted { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn accounted_allocator(&self, inner_allocator: Arc<dyn Allocator>) -> Arc<dyn Allocator> {
        let owner: Arc<dyn MemoryOwner> = self.account.clone();
        Arc::new(MemoryOwnerAllocator::new(
            inner_allocator,
            owner,
            self.domain,
            self.tag,
            self.class,
        ))
    }

    fn lock_state_lossy(&self) -> std::sync::MutexGuard<'_, ExternalMemoryState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(err) => err.into_inner(),
        }
    }
}

impl MemoryOwner for OperatorExternalMemoryTracker {
    fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
        self.account.try_grow(bytes)
    }

    fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
        self.account.release(bytes);
    }

    fn record_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        self.account.record_allocation(domain, tag, class, bytes);
        let mut state = self.lock_state_lossy();
        state.current_bytes = state.current_bytes.saturating_add(bytes);
        state.peak_bytes = state
            .peak_bytes
            .max(state.current_bytes)
            .max(state.minimum_reservation_bytes);
    }

    fn release_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        self.account.release_allocation(domain, tag, class, bytes);
        let mut state = self.lock_state_lossy();
        state.current_bytes = state.current_bytes.saturating_sub(bytes);
    }
}

impl Drop for OperatorExternalMemoryTracker {
    fn drop(&mut self) {
        self.clear();
    }
}
