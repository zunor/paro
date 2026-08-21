// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Low-level memory runtime primitives.

mod accounting_context;
mod accumulator;
mod allocation_ledger;
pub mod collections;
mod domain;
mod error;
mod grant_allocator;
mod grant_arena;
mod grant_buffer;
mod grant_handle;
mod memory_grant;
mod owner;
mod owner_allocator;

pub use accounting_context::MemoryAccountingContext;
pub use accumulator::MemoryAccumulator;
pub use allocation_ledger::{AllocationEntry, AllocationId, AllocationLedger};
pub use collections::{
    AccountedBytesMut, AccountedHashMap, AccountedHashSet, AccountedString, AccountedVec,
    PrecomputedHashBuildHasher,
};
pub use domain::{MemoryDomain, MEMORY_DOMAIN_COUNT};
pub use error::{MemoryError, MemoryResult};
pub use grant_allocator::GrantAllocator;
pub use grant_arena::GrantArena;
pub use grant_buffer::{GrantAllocation, GrantBuffer};
pub use grant_handle::MemoryGrantHandle;
pub use memory_grant::{MemoryGrant, MemoryReleaseHandle};
pub use owner::{DetachedMemoryOwner, MemoryAccountingClass, MemoryOwner};
pub use owner_allocator::MemoryOwnerAllocator;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::allocator::{Allocator, DefaultAllocator, MemoryTag};

    use super::*;

    #[derive(Debug, Default)]
    struct TestOwner {
        capacity: AtomicUsize,
        used: AtomicUsize,
    }

    impl MemoryOwner for TestOwner {
        fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
            self.capacity.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
            self.capacity.fetch_sub(bytes, Ordering::SeqCst);
        }

        fn record_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            _class: MemoryAccountingClass,
            bytes: usize,
        ) {
            self.used.fetch_add(bytes, Ordering::SeqCst);
        }

        fn release_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            _class: MemoryAccountingClass,
            bytes: usize,
        ) {
            self.used.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    #[test]
    fn memory_grant_consumes_refunds_and_splits_capacity() {
        let owner = Arc::new(TestOwner::default());
        let grant = MemoryGrant::new(128, MemoryDomain::Host, owner.clone()).unwrap();

        grant.try_consume(32).unwrap();
        assert_eq!(grant.available_bytes(), 96);
        assert_eq!(grant.used_bytes(), 32);

        grant.refund(16);
        assert_eq!(grant.available_bytes(), 112);
        assert_eq!(grant.used_bytes(), 16);

        let child = grant.split(64).unwrap();
        assert_eq!(grant.reserved_bytes(), 64);
        assert_eq!(child.reserved_bytes(), 64);
        drop(child);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 64);

        drop(grant);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn grant_buffer_transfers_capacity_to_release_handle() {
        let owner = Arc::new(TestOwner::default());
        let grant = MemoryGrant::new(256, MemoryDomain::Host, owner.clone()).unwrap();
        let accumulator = MemoryAccumulator::default();
        let allocator: Arc<dyn crate::allocator::Allocator> = Arc::new(DefaultAllocator::new());
        let grant_allocator = GrantAllocator::new(
            &allocator,
            &grant,
            &accumulator,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
        );

        {
            let buffer = grant_allocator.allocate_buffer(64).unwrap();
            assert_eq!(buffer.size(), 64);
            assert_eq!(owner.capacity.load(Ordering::SeqCst), 256);
            assert_eq!(owner.used.load(Ordering::SeqCst), 64);
            assert_eq!(grant.reserved_bytes(), 192);
        }

        assert_eq!(owner.capacity.load(Ordering::SeqCst), 192);
        assert_eq!(owner.used.load(Ordering::SeqCst), 0);
        drop(grant);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn memory_release_handle_releases_once() {
        let owner = Arc::new(TestOwner::default());
        let grant = MemoryGrant::new(128, MemoryDomain::Host, owner.clone()).unwrap();
        let handle = grant.release_handle(
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            32,
        );
        grant.try_consume(32).unwrap();
        grant.commit_consumed(32);

        owner.record_allocation(
            MemoryDomain::Host,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            32,
        );
        handle.release();
        handle.release();

        assert_eq!(owner.used.load(Ordering::SeqCst), 0);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 96);
        drop(grant);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn accounted_vec_and_ledger_track_capacity_metadata() {
        let grant = MemoryGrant::detached(4096, MemoryDomain::Host);
        let mut vec = AccountedVec::new(grant);
        vec.try_push(10_u64).unwrap();
        vec.try_push(20_u64).unwrap();
        assert_eq!(&*vec, &[10, 20]);

        let ledger_grant = MemoryGrant::detached(4096, MemoryDomain::Host);
        let mut ledger = AllocationLedger::new(ledger_grant);
        assert_eq!(ledger.add(AllocationId(1), 128).unwrap(), 128);
        assert_eq!(ledger.add(AllocationId(1), 128).unwrap(), 0);
        assert_eq!(ledger.total_bytes(), 128);
        assert_eq!(ledger.remove(AllocationId(1)), 0);
        assert_eq!(ledger.remove(AllocationId(1)), 128);
        assert!(ledger.is_empty());
    }

    #[test]
    fn spill_owner_allocator_observes_bytes_without_consuming_capacity() {
        let owner = Arc::new(TestOwner::default());
        let allocator = MemoryOwnerAllocator::new(
            Arc::new(DefaultAllocator::new()),
            owner.clone(),
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Spill,
        );
        let ptr = allocator.allocate(64).expect("spill allocation");
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
        assert_eq!(owner.used.load(Ordering::SeqCst), 64);
        let ptr = allocator.reallocate(ptr, 64, 96).expect("grow spill");
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
        assert_eq!(owner.used.load(Ordering::SeqCst), 96);
        allocator.free(ptr, 96);
        assert_eq!(owner.capacity.load(Ordering::SeqCst), 0);
        assert_eq!(owner.used.load(Ordering::SeqCst), 0);
    }
}
