// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! High-level execution memory runtime API.

mod accounted_buffer;
mod admission;
mod arbitrator;
mod external_tracker;
mod local_grant;
mod memory_demand;
mod observability;
mod operator_account;
mod operator_scope;
mod pipeline_retained;
mod prefetch_lease;
mod query_pool;
mod reclaimer;
mod retained_chunks;
mod retained_handle;
mod shared_object;
mod system_reserve;

pub use accounted_buffer::AccountedBuffer;
pub use admission::{AdmissionWaiterId, PipelineAdmissionController, PipelineAdmissionGuard};
pub use arbitrator::MemoryArbitrator;
pub use external_tracker::{LocalExternalMemoryTracker, OperatorExternalMemoryTracker};
pub use local_grant::{
    LocalMemoryGrant, DEFAULT_LOCAL_INITIAL_GRANT_BYTES, DEFAULT_LOCAL_REFILL_CAP_BYTES,
};
pub use memory_demand::{MemoryDemand, MemoryDomainDemand};
pub use observability::{MemoryDomainTagBytes, MemoryRuntimeStats, MemoryTagBytes};
pub use operator_account::{CacheAligned, ColdCounters, HotCounters, OperatorMemoryAccount};
pub use operator_scope::OperatorMemoryScope;
pub use pipeline_retained::PipelineRetainedMemory;
pub use prefetch_lease::PrefetchLease;
pub use query_pool::QueryMemoryPool;
pub use reclaimer::{
    BufferPoolReclaimer, GrowOutcome, ReclaimHandle, ReclaimStats, Reclaimer, SpillCost,
};
pub use retained_chunks::RetainedChunkVec;
pub use retained_handle::RetainedMemoryHandle;
pub use shared_object::{SharedRetainedObject, SharedRetainedObjectState};
pub use system_reserve::{SystemReserve, SystemReserveClass, SystemReserveReservation};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use paro_common::allocator::{DefaultAllocator, MemoryTag};
    use paro_common::chunk::Chunk;
    use paro_common::memory::{
        MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
    };
    use paro_common::types::LogicalType;
    use paro_context::{QueryMemoryBudgetSpec, QueryMemoryCoordinator, QueryMemoryTarget};

    use super::*;

    #[test]
    fn local_scope_allocates_against_operator_account() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let owner: Arc<dyn paro_common::memory::MemoryOwner> = account.clone();
        let allocator = Arc::new(DefaultAllocator::new());
        let local = LocalMemoryGrant::new(
            owner,
            256,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            allocator,
        )
        .unwrap();

        {
            let scope = OperatorMemoryScope::new(&local);
            let grant_allocator =
                scope.grant_allocator_for(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
            let ptr = grant_allocator.allocate(64).unwrap();
            assert_eq!(account.issued_bytes(), 256);
            assert_eq!(account.revocable_bytes(), 64);
            assert_eq!(pool.revocable_bytes(), 64);
            grant_allocator.free(ptr, 64);
            assert_eq!(account.revocable_bytes(), 0);
            assert_eq!(pool.revocable_bytes(), 0);
        }

        drop(local);
        assert_eq!(pool.issued_bytes(), 0);
    }

    #[test]
    fn memory_demand_updates_existing_domain() {
        let mut demand = MemoryDemand::host(10, 20);
        demand.update_in_place(paro_common::memory::MemoryDomain::Host, 5);
        assert_eq!(demand.domains()[0].desired_bytes, 10);
        demand.update_in_place(paro_common::memory::MemoryDomain::Host, 64);
        assert_eq!(demand.domains()[0].desired_bytes, 64);
    }

    #[derive(Debug)]
    struct TestReclaimer {
        pool: Arc<QueryMemoryPool>,
        available: AtomicUsize,
    }

    impl Reclaimer for TestReclaimer {
        fn name(&self) -> &str {
            "test_reclaimer"
        }

        fn reclaimable_bytes(&self) -> usize {
            self.available.load(Ordering::Acquire)
        }

        fn reclaim_sync(
            &self,
            target_bytes: usize,
        ) -> paro_common::memory::MemoryResult<ReclaimStats> {
            let release = target_bytes.min(self.available.swap(0, Ordering::AcqRel));
            self.pool.release(release);
            self.pool.release_allocation(
                MemoryDomain::Host,
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
                release,
            );
            Ok(ReclaimStats::new(target_bytes, release, release))
        }

        fn spill_cost(&self) -> SpillCost {
            SpillCost::AccountingRelease
        }
    }

    #[test]
    fn query_pool_reclaims_before_quota_failure() {
        let pool = Arc::new(QueryMemoryPool::new(120));
        pool.try_grow(100).unwrap();
        let reclaimer: Arc<dyn Reclaimer> = Arc::new(TestReclaimer {
            pool: pool.clone(),
            available: AtomicUsize::new(40),
        });
        pool.register_reclaimer(reclaimer);

        pool.try_grow(50).unwrap();
        assert_eq!(pool.issued_bytes(), 120);
    }

    #[test]
    fn query_pool_reclaimer_lifecycle_dedupes_and_unregisters_by_name() {
        let pool = Arc::new(QueryMemoryPool::new(120));
        pool.register_reclaimer_once_by_name(Arc::new(TestReclaimer {
            pool: pool.clone(),
            available: AtomicUsize::new(10),
        }));
        pool.register_reclaimer_once_by_name(Arc::new(TestReclaimer {
            pool: pool.clone(),
            available: AtomicUsize::new(20),
        }));
        assert_eq!(pool.reclaimer_count(), 1);

        assert_eq!(pool.unregister_reclaimer_by_name("test_reclaimer"), 1);
        assert_eq!(pool.unregister_reclaimer_by_name("test_reclaimer"), 0);
        assert_eq!(pool.reclaimer_count(), 0);
    }

    #[test]
    fn admission_controller_blocks_and_wakes_waiter() {
        let controller = Arc::new(PipelineAdmissionController::new(1));
        let first = controller
            .try_acquire(paro_scheduler::task::InterruptState::new())
            .expect("first slot should be admitted");
        let signal = paro_scheduler::task::InterruptDoneSignalState::new();
        let blocked = controller.try_acquire(paro_scheduler::task::InterruptState::with_signal(
            signal.downgrade(),
        ));
        assert!(blocked.is_none());
        assert_eq!(controller.blocked_waiters(), 1);

        drop(first);
        assert_eq!(controller.blocked_waiters(), 0);
    }

    #[test]
    fn admission_controller_dedupes_stable_waiter_registration() {
        let controller = Arc::new(PipelineAdmissionController::new(1));
        let first = controller
            .try_acquire(paro_scheduler::task::InterruptState::new())
            .expect("first slot should be admitted");
        let waiter = AdmissionWaiterId(7);

        assert!(controller
            .try_acquire_for(waiter, paro_scheduler::task::InterruptState::new())
            .is_none());
        assert!(controller
            .try_acquire_for(waiter, paro_scheduler::task::InterruptState::new())
            .is_none());
        assert_eq!(controller.blocked_waiters(), 1);

        drop(first);
        assert_eq!(controller.blocked_waiters(), 0);
    }

    #[test]
    fn prefetch_lease_accounts_inflight_bytes_as_prefetch() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let lease = PrefetchLease::new(account.clone(), 512);

        assert!(paro_storage::buffer::PrefetchBudget::try_acquire(
            &lease, 256
        ));
        assert_eq!(lease.inflight_bytes(), 256);
        assert_eq!(account.prefetch_bytes(), 256);
        assert_eq!(pool.prefetch_bytes(), 256);

        paro_storage::buffer::PrefetchBudget::release(&lease, 256);
        assert_eq!(lease.inflight_bytes(), 0);
        assert_eq!(account.prefetch_bytes(), 0);
        assert_eq!(pool.prefetch_bytes(), 0);
    }

    #[test]
    fn memory_arbitrator_subtracts_session_retained_bytes() {
        let arbitrator = MemoryArbitrator::new(1024);
        arbitrator.set_shared_cache_floor(128);
        arbitrator.set_system_reserve_bytes(64);
        arbitrator.add_session_retained_bytes(256);

        assert_eq!(arbitrator.available_for_queries(), 576);
        arbitrator.release_session_retained_bytes(128);
        assert_eq!(arbitrator.session_retained_bytes(), 128);
        assert_eq!(arbitrator.available_for_queries(), 704);
    }

    #[test]
    fn memory_arbitrator_splits_capacity_across_groups() {
        let arbitrator = Arc::new(MemoryArbitrator::new(1_000));
        let pool_a = Arc::new(QueryMemoryPool::new(1_000));
        let target_a: Arc<dyn QueryMemoryTarget> = pool_a.clone();
        let registration_a = arbitrator.clone().register_query(
            QueryMemoryBudgetSpec::new(1, Some("a".to_string()), 1_000, None),
            Arc::downgrade(&target_a),
        );
        pool_a.attach_registration(registration_a);

        let pool_b = Arc::new(QueryMemoryPool::new(1_000));
        let target_b: Arc<dyn QueryMemoryTarget> = pool_b.clone();
        let registration_b = arbitrator.clone().register_query(
            QueryMemoryBudgetSpec::new(2, Some("b".to_string()), 1_000, None),
            Arc::downgrade(&target_b),
        );
        pool_b.attach_registration(registration_b);

        assert_eq!(pool_a.capacity_bytes(), 500);
        assert_eq!(pool_b.capacity_bytes(), 500);
    }

    #[test]
    fn query_pool_reclaims_from_peer_before_quota_failure() {
        let arbitrator = Arc::new(MemoryArbitrator::new(200));
        let pool_a = Arc::new(QueryMemoryPool::new(200));
        let target_a: Arc<dyn QueryMemoryTarget> = pool_a.clone();
        let registration_a = arbitrator.clone().register_query(
            QueryMemoryBudgetSpec::new(1, Some("a".to_string()), 200, None),
            Arc::downgrade(&target_a),
        );
        pool_a.attach_registration(registration_a);

        let pool_b = Arc::new(QueryMemoryPool::new(200));
        let target_b: Arc<dyn QueryMemoryTarget> = pool_b.clone();
        let registration_b = arbitrator.clone().register_query(
            QueryMemoryBudgetSpec::new(2, Some("b".to_string()), 200, None),
            Arc::downgrade(&target_b),
        );
        pool_b.attach_registration(registration_b);
        pool_b.try_grow(100).unwrap();
        pool_b.record_allocation(
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
            60,
        );
        pool_b.register_reclaimer(Arc::new(TestReclaimer {
            pool: pool_b.clone(),
            available: AtomicUsize::new(60),
        }));

        pool_a.try_grow(150).unwrap();
        assert_eq!(pool_a.issued_bytes(), 150);
        assert_eq!(pool_b.issued_bytes(), 50);
    }

    #[test]
    fn system_reserve_reduces_query_capacity_and_releases_on_drop() {
        let arbitrator = Arc::new(MemoryArbitrator::new(1_024));
        let reserve = Arc::new(SystemReserve::new(arbitrator.clone()));
        let hold = reserve
            .try_acquire(SystemReserveClass::Maintenance, 256)
            .unwrap();

        assert_eq!(arbitrator.system_reserve_bytes(), 256);
        assert_eq!(arbitrator.available_for_queries(), 768);

        drop(hold);
        assert_eq!(arbitrator.system_reserve_bytes(), 0);
        assert_eq!(arbitrator.available_for_queries(), 1_024);
    }

    #[test]
    fn query_pool_can_block_on_async_reclaim() {
        #[derive(Debug)]
        struct AsyncTestReclaimer {
            handle: ReclaimHandle,
        }

        impl Reclaimer for AsyncTestReclaimer {
            fn name(&self) -> &str {
                "async_test_reclaimer"
            }

            fn reclaimable_bytes(&self) -> usize {
                64
            }

            fn reclaim_sync(
                &self,
                target_bytes: usize,
            ) -> paro_common::memory::MemoryResult<ReclaimStats> {
                Ok(ReclaimStats::empty(target_bytes))
            }

            fn start_reclaim(
                &self,
                _target_bytes: usize,
                interrupt: Option<paro_scheduler::task::InterruptState>,
            ) -> paro_common::memory::MemoryResult<ReclaimHandle> {
                if let Some(interrupt) = interrupt {
                    self.handle.wait(interrupt);
                }
                Ok(self.handle.clone())
            }

            fn spill_cost(&self) -> SpillCost {
                SpillCost::SpillToDisk
            }
        }

        let pool = Arc::new(QueryMemoryPool::new(100));
        pool.try_grow(100).unwrap();
        let handle = ReclaimHandle::pending("async_test_reclaimer");
        pool.register_reclaimer(Arc::new(AsyncTestReclaimer {
            handle: handle.clone(),
        }));

        let outcome = pool
            .try_grow_or_block(10, None)
            .expect("async reclaim should block");
        assert!(outcome.is_blocked());
        assert!(!handle.is_complete());
    }

    #[test]
    fn local_grant_refill_and_revoke_update_issued_bytes() {
        let pool = Arc::new(QueryMemoryPool::new(512));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let owner: Arc<dyn paro_common::memory::MemoryOwner> = account.clone();
        let allocator = Arc::new(DefaultAllocator::new());
        let local = LocalMemoryGrant::new(
            owner,
            64,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            allocator,
        )
        .unwrap();

        assert_eq!(pool.issued_bytes(), 64);
        assert_eq!(local.refill_local(128, 256).unwrap(), 256);
        assert_eq!(pool.issued_bytes(), 320);

        account.request_revoke(80);
        assert_eq!(account.sync_revoke(&local), 80);
        assert_eq!(pool.issued_bytes(), 240);
    }

    #[test]
    fn local_external_memory_tracker_accounts_delta_and_clear() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let owner: Arc<dyn paro_common::memory::MemoryOwner> = account.clone();
        let allocator = Arc::new(DefaultAllocator::new());
        let local = LocalMemoryGrant::new(
            owner,
            0,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
            allocator,
        )
        .unwrap();
        let mut tracker = LocalExternalMemoryTracker::new(
            local,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );

        tracker.set_accounted_bytes(96).unwrap();
        assert_eq!(tracker.accounted_bytes(), 96);
        assert_eq!(tracker.reservation_bytes(), 96);
        assert_eq!(account.issued_bytes(), 96);
        assert_eq!(account.revocable_bytes(), 96);
        assert_eq!(pool.revocable_bytes(), 96);

        tracker.set_minimum_reservation_bytes(160);
        tracker.set_accounted_bytes(128).unwrap();
        assert_eq!(tracker.reservation_bytes(), 160);
        assert_eq!(tracker.peak_bytes(), 160);
        assert_eq!(account.issued_bytes(), 128);
        assert_eq!(account.revocable_bytes(), 128);

        tracker.set_accounted_bytes(64).unwrap();
        assert_eq!(account.issued_bytes(), 64);
        assert_eq!(account.revocable_bytes(), 64);

        tracker.clear();
        assert_eq!(tracker.accounted_bytes(), 0);
        assert_eq!(tracker.reservation_bytes(), 0);
        assert_eq!(account.issued_bytes(), 0);
        assert_eq!(pool.revocable_bytes(), 0);
    }

    #[test]
    fn operator_external_memory_tracker_survives_source_handoff() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let sink_tracker = Arc::new(OperatorExternalMemoryTracker::new(
            account.clone(),
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        ));

        sink_tracker.set_minimum_reservation_bytes(256).unwrap();
        sink_tracker.set_accounted_bytes(128).unwrap();
        assert_eq!(sink_tracker.reservation_bytes().unwrap(), 256);
        assert_eq!(sink_tracker.peak_bytes().unwrap(), 256);

        let source_tracker = sink_tracker.clone();
        drop(sink_tracker);
        assert_eq!(account.issued_bytes(), 128);
        assert_eq!(pool.revocable_bytes(), 128);

        source_tracker.clear();
        assert_eq!(account.issued_bytes(), 0);
        assert_eq!(pool.revocable_bytes(), 0);
    }

    #[test]
    fn operator_external_memory_tracker_reclaims_accounted_capacity() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let tracker = Arc::new(OperatorExternalMemoryTracker::new(
            account.clone(),
            MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        ));

        tracker.set_accounted_bytes(512).unwrap();
        assert_eq!(account.issued_bytes(), 512);
        assert_eq!(pool.revocable_bytes(), 512);

        assert_eq!(tracker.reclaim_accounted_bytes(200).unwrap(), 200);
        assert_eq!(tracker.accounted_bytes().unwrap(), 312);
        assert_eq!(account.issued_bytes(), 312);
        assert_eq!(pool.revocable_bytes(), 312);

        assert_eq!(tracker.reclaim_accounted_bytes(999).unwrap(), 312);
        assert_eq!(tracker.accounted_bytes().unwrap(), 0);
        assert_eq!(account.issued_bytes(), 0);
        assert_eq!(pool.revocable_bytes(), 0);
    }

    #[test]
    fn operator_account_reclassifies_without_changing_issued_capacity() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = OperatorMemoryAccount::new(pool.clone());

        account
            .retain_external_allocation(
                paro_common::memory::MemoryDomain::Host,
                MemoryTag::OrderBy,
                MemoryAccountingClass::Revocable,
                160,
            )
            .unwrap();
        assert_eq!(account.issued_bytes(), 160);
        assert_eq!(account.revocable_bytes(), 160);
        assert_eq!(account.reclaimable_bytes(), 160);
        assert_eq!(pool.published_used_bytes(), 160);
        assert_eq!(pool.revocable_bytes(), 160);

        account.reclassify(
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
            MemoryAccountingClass::Spill,
            96,
        );
        assert_eq!(account.issued_bytes(), 160);
        assert_eq!(account.revocable_bytes(), 64);
        assert_eq!(account.spill_bytes(), 96);
        assert_eq!(pool.published_used_bytes(), 160);
        assert_eq!(pool.revocable_bytes(), 64);
        assert_eq!(pool.spill_bytes(), 96);

        account.release_external_allocation(
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
            64,
        );
        account.release_external_allocation(
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Spill,
            96,
        );
        assert_eq!(account.issued_bytes(), 0);
        assert_eq!(pool.published_used_bytes(), 0);
    }

    #[test]
    fn shared_retained_object_reclassifies_revocable_bytes_on_rebind() {
        let pool = Arc::new(QueryMemoryPool::new(1024));
        let account = Arc::new(OperatorMemoryAccount::new(pool.clone()));
        let tracker = Arc::new(OperatorExternalMemoryTracker::new(
            account.clone(),
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        ));
        let tracker_owner: Arc<dyn MemoryOwner> = tracker.clone();
        let retained = Arc::new(SharedRetainedObject::new(
            "test_retained_hash_table",
            tracker_owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
        ));
        let retained_owner: Arc<dyn MemoryOwner> = retained.clone();
        let memory = MemoryAccountingContext::from_owner(
            retained_owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );

        let release = memory.retain(128).unwrap();
        assert_eq!(retained.retained_bytes(), 128);
        assert_eq!(retained.reclaimable_bytes(), 0);
        assert_eq!(account.non_revocable_bytes(), 128);
        assert_eq!(account.revocable_bytes(), 0);

        retained.rebind_reclaimer();
        assert_eq!(retained.reclaimable_bytes(), 128);
        assert_eq!(account.non_revocable_bytes(), 0);
        assert_eq!(account.revocable_bytes(), 128);

        release.release();
        assert_eq!(retained.retained_bytes(), 0);
        assert_eq!(account.issued_bytes(), 0);
        assert_eq!(pool.published_used_bytes(), 0);
    }

    #[test]
    fn pipeline_retained_memory_deduplicates_shared_chunk_buffers() {
        let pool = Arc::new(QueryMemoryPool::new(4096));
        let account = Arc::new(OperatorMemoryAccount::new(pool));
        let mut retained = PipelineRetainedMemory::new(account.clone());
        let allocator = Arc::new(DefaultAllocator::new());
        let chunk = Chunk::try_initialize(&[LogicalType::Integer], 8, allocator).unwrap();
        let clone = chunk.clone();
        let allocation_size = chunk.get_allocation_size();

        retained.refresh(vec![&chunk, &clone]).unwrap();
        assert_eq!(retained.retained_bytes(), allocation_size);
        assert_eq!(account.non_revocable_bytes(), allocation_size);
        assert!(account.metadata_bytes() > 0);
        assert!(account.issued_bytes() >= allocation_size);

        retained.clear();
        assert_eq!(retained.retained_bytes(), 0);
        assert_eq!(account.non_revocable_bytes(), 0);
        drop(retained);
        assert_eq!(account.issued_bytes(), 0);
    }
}
