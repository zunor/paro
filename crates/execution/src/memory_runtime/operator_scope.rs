// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-call operator memory scope.

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::memory::{
    GrantAllocator, GrantArena, MemoryAccountingClass, MemoryAccumulator, MemoryGrant,
    MemoryOwnerAllocator, MemoryReleaseHandle, MemoryResult,
};

use super::LocalMemoryGrant;

/// Stack-local memory scope created for a single operator call.
#[derive(Debug)]
pub struct OperatorMemoryScope<'a> {
    grant: &'a LocalMemoryGrant,
    accumulator: MemoryAccumulator,
}

impl<'a> OperatorMemoryScope<'a> {
    pub fn new(grant: &'a LocalMemoryGrant) -> Self {
        Self {
            grant,
            accumulator: MemoryAccumulator::default(),
        }
    }

    pub fn local_grant(&self) -> Option<&LocalMemoryGrant> {
        Some(self.grant)
    }

    pub fn child_scope(&self) -> Self {
        Self::new(self.grant)
    }

    pub fn grant_allocator(&self) -> GrantAllocator<'_> {
        self.grant_allocator_for(
            self.local_grant()
                .map(LocalMemoryGrant::tag)
                .unwrap_or(MemoryTag::Allocator),
            self.local_grant()
                .map(LocalMemoryGrant::accounting_class)
                .unwrap_or(MemoryAccountingClass::NonRevocable),
        )
    }

    pub fn grant_allocator_for(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
    ) -> GrantAllocator<'_> {
        GrantAllocator::new(
            self.grant.allocator(),
            self.grant.grant(),
            &self.accumulator,
            tag,
            accounting_class,
        )
    }

    pub fn grant_arena(&self) -> GrantArena<'_> {
        self.grant_arena_for(
            self.local_grant()
                .map(LocalMemoryGrant::tag)
                .unwrap_or(MemoryTag::Allocator),
            self.local_grant()
                .map(LocalMemoryGrant::accounting_class)
                .unwrap_or(MemoryAccountingClass::NonRevocable),
        )
    }

    pub fn grant_arena_for(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
    ) -> GrantArena<'_> {
        GrantArena::new(
            self.grant.allocator().clone(),
            self.grant.grant(),
            &self.accumulator,
            tag,
            accounting_class,
        )
    }

    /// Build an owner-backed allocator for APIs that require `Arc<dyn Allocator>`.
    ///
    /// `GrantAllocator<'_>` is the hottest stack-only path, but vectors and
    /// chunks store allocators for later reallocation/free. This adapter keeps
    /// those allocations hard-gated by the same operator owner.
    pub fn accounted_allocator_for(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
    ) -> Arc<dyn Allocator> {
        let owner = self
            .grant
            .grant()
            .owner()
            .expect("operator memory scope must be owner-backed");
        Arc::new(MemoryOwnerAllocator::new(
            self.grant.allocator().clone(),
            owner,
            self.grant.domain(),
            tag,
            accounting_class,
        ))
    }

    pub fn split_sub_grant(&self, bytes: usize) -> MemoryResult<MemoryGrant> {
        self.grant.split_sub_grant(bytes)
    }

    pub fn retain_external_allocation_handle(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        bytes: usize,
    ) -> MemoryResult<MemoryReleaseHandle> {
        self.grant
            .retain_external_allocation_handle(tag, accounting_class, bytes)
    }

    pub fn force_flush(&self) -> Option<isize> {
        self.accumulator.force_flush()
    }
}
