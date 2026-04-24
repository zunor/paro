// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Retained chunk accounting for blocked pipeline tasks.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::memory::{
    AllocationLedger, MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
    MemoryResult,
};

use super::OperatorMemoryAccount;

#[derive(Debug)]
pub struct PipelineRetainedMemory {
    account: Arc<OperatorMemoryAccount>,
    ledger: AllocationLedger,
    accounted_bytes: usize,
}

impl PipelineRetainedMemory {
    pub fn new(account: Arc<OperatorMemoryAccount>) -> Self {
        Self {
            ledger: Self::new_ledger(&account),
            account,
            accounted_bytes: 0,
        }
    }

    fn new_ledger(account: &Arc<OperatorMemoryAccount>) -> AllocationLedger {
        let owner: Arc<dyn MemoryOwner> = account.clone();
        let metadata_memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::Metadata,
            MemoryAccountingClass::Metadata,
        );
        AllocationLedger::new_with_accounting(
            metadata_memory
                .grant()
                .expect("zero-byte pipeline retained ledger grant should fit"),
            MemoryTag::Metadata,
            MemoryAccountingClass::Metadata,
        )
    }

    pub fn retained_bytes(&self) -> usize {
        self.accounted_bytes
    }

    pub fn refresh<'a, I>(&mut self, chunks: I) -> MemoryResult<()>
    where
        I: IntoIterator<Item = &'a Chunk>,
    {
        let mut next_ledger = Self::new_ledger(&self.account);
        // untracked_small_metadata: refresh scratch is bounded by retained chunk vector ids;
        // persistent ledger metadata is owner-accounted.
        let mut entries = Vec::new();
        for chunk in chunks {
            chunk.collect_allocation_entries(&mut entries);
        }
        for (id, bytes) in entries {
            next_ledger.add(id, bytes)?;
        }

        let new_bytes = next_ledger.total_bytes();
        if new_bytes > self.accounted_bytes {
            self.account.retain_external_allocation(
                MemoryDomain::Host,
                MemoryTag::Allocator,
                MemoryAccountingClass::NonRevocable,
                new_bytes - self.accounted_bytes,
            )?;
        } else if self.accounted_bytes > new_bytes {
            self.account.release_external_allocation(
                MemoryDomain::Host,
                MemoryTag::Allocator,
                MemoryAccountingClass::NonRevocable,
                self.accounted_bytes - new_bytes,
            );
        }

        self.ledger = next_ledger;
        self.accounted_bytes = new_bytes;
        Ok(())
    }

    pub fn clear(&mut self) {
        if self.accounted_bytes > 0 {
            self.account.release_external_allocation(
                MemoryDomain::Host,
                MemoryTag::Allocator,
                MemoryAccountingClass::NonRevocable,
                self.accounted_bytes,
            );
        }
        let _ = self.ledger.clear();
        self.accounted_bytes = 0;
    }
}

impl Drop for PipelineRetainedMemory {
    fn drop(&mut self) {
        self.clear();
    }
}
