// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use paro_external_abi::{ColumnBatchLease, LeaseOwnership, LeaseState};

use crate::data_plane::lease::{CrossDomainReuseStrategy, LeaseReclaimPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArenaNamespace {
    pub tenant: String,
    pub security_domain: String,
    pub arena_name: String,
}

impl ArenaNamespace {
    pub fn security_domain_key(&self) -> String {
        format!("{}:{}", self.tenant, self.security_domain)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArenaKind {
    Input,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArenaBacking {
    Anonymous,
    MemfdSealed,
    MemfdSecret,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArenaConfig {
    pub namespace: ArenaNamespace,
    pub kind: ArenaKind,
    pub backing: ArenaBacking,
    pub premap_bytes: u64,
    pub buffer_count: u16,
    pub reclaim_policy: LeaseReclaimPolicy,
}

impl ArenaConfig {
    pub fn bytes_per_buffer(&self) -> u64 {
        self.premap_bytes / u64::from(self.buffer_count.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaAllocation {
    pub lease_id: u64,
    pub buffer_index: u16,
    pub offset: u64,
    pub len: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArenaHandle {
    pub lease_id: u64,
    pub query_epoch: u64,
    pub generation: u64,
    pub namespace_key: String,
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FreeSlice {
    buffer_index: u16,
    offset: u64,
    len: u64,
    generation: u64,
    security_domain_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArenaLeaseRecord {
    allocation: ArenaAllocation,
    lease: ColumnBatchLease,
    security_domain_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ArenaStats {
    pub live_leases: usize,
    pub reused_allocations: usize,
    pub reclaimed_allocations: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ArenaError {
    #[error("arena out of capacity for {requested_bytes} bytes")]
    OutOfCapacity { requested_bytes: u64 },
    #[error("lease {lease_id} not found")]
    LeaseNotFound { lease_id: u64 },
    #[error("lease {lease_id} is not in state {expected:?}")]
    UnexpectedState { lease_id: u64, expected: LeaseState },
    #[error("arena handle namespace mismatch: expected {expected}, got {actual}")]
    HandleNamespaceMismatch { expected: String, actual: String },
    #[error(
        "arena handle query epoch mismatch for lease {lease_id}: expected {expected}, got {actual}"
    )]
    HandleQueryEpochMismatch {
        lease_id: u64,
        expected: u64,
        actual: u64,
    },
    #[error(transparent)]
    Lease(#[from] paro_external_abi::LeaseError),
}

#[derive(Debug)]
pub struct SharedArena {
    config: ArenaConfig,
    buffers: Vec<Vec<u8>>,
    next_offsets: Vec<u64>,
    next_lease_id: u64,
    next_generation: u64,
    next_handle_nonce: u64,
    live: BTreeMap<u64, ArenaLeaseRecord>,
    free_list: VecDeque<FreeSlice>,
    stats: ArenaStats,
}

impl SharedArena {
    pub fn new(config: ArenaConfig) -> Self {
        let bytes_per_buffer = config.bytes_per_buffer() as usize;
        let buffer_count = usize::from(config.buffer_count.max(1));

        Self {
            config,
            buffers: vec![vec![0_u8; bytes_per_buffer]; buffer_count],
            next_offsets: vec![0; buffer_count],
            next_lease_id: 1,
            next_generation: 1,
            next_handle_nonce: 1,
            live: BTreeMap::new(),
            free_list: VecDeque::new(),
            stats: ArenaStats::default(),
        }
    }

    pub fn config(&self) -> &ArenaConfig {
        &self.config
    }

    pub fn stats(&self) -> ArenaStats {
        self.stats
    }

    pub fn reserve(
        &mut self,
        len: u64,
        ownership: LeaseOwnership,
    ) -> Result<(ArenaAllocation, ColumnBatchLease), ArenaError> {
        if let Some(index) = self.free_list.iter().position(|slice| slice.len >= len) {
            let free = self
                .free_list
                .remove(index)
                .expect("free slice present at index");
            self.apply_reuse_strategy(&free);
            let allocation = ArenaAllocation {
                lease_id: self.next_lease_id,
                buffer_index: free.buffer_index,
                offset: free.offset,
                len,
                generation: free.generation,
            };
            return Ok(self.attach_new_lease(allocation, ownership, true));
        }

        let bytes_per_buffer = self.config.bytes_per_buffer();
        for (buffer_index, next_offset) in self.next_offsets.iter_mut().enumerate() {
            if *next_offset + len <= bytes_per_buffer {
                let allocation = ArenaAllocation {
                    lease_id: self.next_lease_id,
                    buffer_index: buffer_index as u16,
                    offset: *next_offset,
                    len,
                    generation: self.next_generation,
                };
                *next_offset += len;
                return Ok(self.attach_new_lease(allocation, ownership, false));
            }
        }

        Err(ArenaError::OutOfCapacity {
            requested_bytes: len,
        })
    }

    pub fn begin_write(&mut self, lease_id: u64) -> Result<(), ArenaError> {
        let record = self
            .live
            .get_mut(&lease_id)
            .ok_or(ArenaError::LeaseNotFound { lease_id })?;
        if record.lease.state != LeaseState::Allocated {
            return Err(ArenaError::UnexpectedState {
                lease_id,
                expected: LeaseState::Allocated,
            });
        }
        record.lease.begin_write()?;
        Ok(())
    }

    pub fn commit(
        &mut self,
        lease_id: u64,
        completion_fence: u64,
        payload_checksum: Option<u32>,
        columns: Vec<paro_external_abi::ColumnDescriptor>,
    ) -> Result<(), ArenaError> {
        let record = self
            .live
            .get_mut(&lease_id)
            .ok_or(ArenaError::LeaseNotFound { lease_id })?;
        if record.lease.state != LeaseState::Writing {
            return Err(ArenaError::UnexpectedState {
                lease_id,
                expected: LeaseState::Writing,
            });
        }
        record
            .lease
            .commit(completion_fence, payload_checksum, columns)?;
        Ok(())
    }

    pub fn abort(&mut self, lease_id: u64) -> Result<(), ArenaError> {
        let record = self
            .live
            .get_mut(&lease_id)
            .ok_or(ArenaError::LeaseNotFound { lease_id })?;
        record.lease.abort()?;
        Ok(())
    }

    pub fn release(&mut self, lease_id: u64) -> Result<(), ArenaError> {
        let mut record = self
            .live
            .remove(&lease_id)
            .ok_or(ArenaError::LeaseNotFound { lease_id })?;
        if record.lease.state != LeaseState::Committed {
            return Err(ArenaError::UnexpectedState {
                lease_id,
                expected: LeaseState::Committed,
            });
        }
        record.lease.release()?;
        self.enqueue_free(record);
        Ok(())
    }

    pub fn reclaim_worker_epoch(&mut self, worker_epoch: u64) -> usize {
        let lease_ids = self
            .live
            .iter()
            .filter_map(|(lease_id, record)| {
                record
                    .lease
                    .reclaimable_by_worker_epoch(worker_epoch)
                    .then_some(*lease_id)
            })
            .collect::<Vec<_>>();
        self.reclaim_ids(&lease_ids)
    }

    pub fn reclaim_query_epoch(&mut self, query_epoch: u64) -> usize {
        let lease_ids = self
            .live
            .iter()
            .filter_map(|(lease_id, record)| {
                record
                    .lease
                    .orphaned_for_query_epoch(query_epoch)
                    .then_some(*lease_id)
            })
            .collect::<Vec<_>>();
        self.reclaim_ids(&lease_ids)
    }

    pub fn reclaim_host_epoch(&mut self, host_epoch: u64) -> usize {
        let lease_ids = self
            .live
            .iter()
            .filter_map(|(lease_id, record)| {
                record
                    .lease
                    .orphaned_for_host_epoch(host_epoch)
                    .then_some(*lease_id)
            })
            .collect::<Vec<_>>();
        self.reclaim_ids(&lease_ids)
    }

    pub fn contains_lease(&self, lease_id: u64) -> bool {
        self.live.contains_key(&lease_id)
    }

    pub fn lease(&self, lease_id: u64) -> Option<&ColumnBatchLease> {
        self.live.get(&lease_id).map(|record| &record.lease)
    }

    pub fn issue_handle(&mut self, lease_id: u64) -> Result<ArenaHandle, ArenaError> {
        let record = self
            .live
            .get(&lease_id)
            .ok_or(ArenaError::LeaseNotFound { lease_id })?;
        let handle = ArenaHandle {
            lease_id,
            query_epoch: record.lease.ownership.owner_query_epoch,
            generation: record.allocation.generation,
            namespace_key: self.config.namespace.security_domain_key(),
            nonce: self.next_handle_nonce,
        };
        self.next_handle_nonce += 1;
        Ok(handle)
    }

    pub fn validate_handle(
        &self,
        handle: &ArenaHandle,
        query_epoch: u64,
    ) -> Result<&ColumnBatchLease, ArenaError> {
        let expected_namespace = self.config.namespace.security_domain_key();
        if handle.namespace_key != expected_namespace {
            return Err(ArenaError::HandleNamespaceMismatch {
                expected: expected_namespace,
                actual: handle.namespace_key.clone(),
            });
        }
        let lease = self
            .lease(handle.lease_id)
            .ok_or(ArenaError::LeaseNotFound {
                lease_id: handle.lease_id,
            })?;
        if handle.query_epoch != query_epoch {
            return Err(ArenaError::HandleQueryEpochMismatch {
                lease_id: handle.lease_id,
                expected: query_epoch,
                actual: handle.query_epoch,
            });
        }
        Ok(lease)
    }

    fn attach_new_lease(
        &mut self,
        allocation: ArenaAllocation,
        ownership: LeaseOwnership,
        reused: bool,
    ) -> (ArenaAllocation, ColumnBatchLease) {
        let lease = ColumnBatchLease::new(allocation.lease_id, 0, ownership);
        let record = ArenaLeaseRecord {
            allocation,
            lease: lease.clone(),
            security_domain_key: self.config.namespace.security_domain_key(),
        };
        self.live.insert(allocation.lease_id, record);
        self.stats.live_leases = self.live.len();
        self.next_lease_id += 1;
        if reused {
            self.stats.reused_allocations += 1;
        } else {
            self.next_generation += 1;
        }
        (allocation, lease)
    }

    fn reclaim_ids(&mut self, lease_ids: &[u64]) -> usize {
        let mut reclaimed = 0;
        for lease_id in lease_ids {
            if let Some(record) = self.live.remove(lease_id) {
                self.enqueue_free(record);
                reclaimed += 1;
            }
        }
        self.stats.live_leases = self.live.len();
        self.stats.reclaimed_allocations += reclaimed;
        reclaimed
    }

    fn enqueue_free(&mut self, record: ArenaLeaseRecord) {
        self.free_list.push_back(FreeSlice {
            buffer_index: record.allocation.buffer_index,
            offset: record.allocation.offset,
            len: record.allocation.len,
            generation: record.allocation.generation,
            security_domain_key: record.security_domain_key,
        });
        self.stats.live_leases = self.live.len();
    }

    fn apply_reuse_strategy(&mut self, free: &FreeSlice) {
        let cross_domain = free.security_domain_key != self.config.namespace.security_domain_key();
        match self.config.reclaim_policy.reuse_strategy(cross_domain) {
            CrossDomainReuseStrategy::TrustSameDomain => {}
            CrossDomainReuseStrategy::ZeroFill => {
                self.zero_fill(free);
            }
            CrossDomainReuseStrategy::GenerationBump => {
                self.next_generation += 1;
            }
            CrossDomainReuseStrategy::ZeroFillAndGenerationBump => {
                self.zero_fill(free);
                self.next_generation += 1;
            }
        }
    }

    fn zero_fill(&mut self, free: &FreeSlice) {
        let buffer = &mut self.buffers[usize::from(free.buffer_index)];
        let start = free.offset as usize;
        let end = start + free.len as usize;
        buffer[start..end].fill(0);
    }
}

#[cfg(test)]
mod tests {
    use paro_external_abi::LeaseOwnership;

    use crate::data_plane::lease::LeaseReclaimPolicy;

    use super::{ArenaBacking, ArenaConfig, ArenaKind, ArenaNamespace, SharedArena};

    fn arena() -> SharedArena {
        SharedArena::new(ArenaConfig {
            namespace: ArenaNamespace {
                tenant: "tenant-a".to_string(),
                security_domain: "domain-a".to_string(),
                arena_name: "input".to_string(),
            },
            kind: ArenaKind::Input,
            backing: ArenaBacking::MemfdSealed,
            premap_bytes: 1024,
            buffer_count: 2,
            reclaim_policy: LeaseReclaimPolicy::default(),
        })
    }

    #[test]
    fn arena_reuses_released_slice_before_bumping() {
        let mut arena = arena();
        let ownership = LeaseOwnership {
            owner_worker_epoch: 1,
            owner_host_epoch: 2,
            owner_query_epoch: 3,
        };
        let (allocation, _) = arena.reserve(64, ownership).expect("initial reserve");
        arena.begin_write(allocation.lease_id).expect("begin write");
        arena
            .commit(allocation.lease_id, 9, None, Vec::new())
            .expect("commit");
        arena.release(allocation.lease_id).expect("release");

        let (reused, _) = arena.reserve(32, ownership).expect("reuse reserve");
        assert_eq!(allocation.buffer_index, reused.buffer_index);
        assert_eq!(allocation.offset, reused.offset);
        assert_eq!(arena.stats().reused_allocations, 1);
    }

    #[test]
    fn arena_reclaims_orphaned_epochs() {
        let mut arena = arena();
        let ownership = LeaseOwnership {
            owner_worker_epoch: 9,
            owner_host_epoch: 7,
            owner_query_epoch: 11,
        };
        let (allocation, _) = arena.reserve(64, ownership).expect("reserve");
        arena.begin_write(allocation.lease_id).expect("begin write");
        assert_eq!(arena.reclaim_worker_epoch(9), 1);
        assert!(!arena.contains_lease(allocation.lease_id));
    }

    #[test]
    fn arena_handles_are_scoped_to_namespace_and_query_epoch() {
        let mut arena = arena();
        let ownership = LeaseOwnership {
            owner_worker_epoch: 1,
            owner_host_epoch: 2,
            owner_query_epoch: 44,
        };
        let (allocation, _) = arena.reserve(64, ownership).expect("reserve");
        let handle = arena.issue_handle(allocation.lease_id).expect("handle");
        assert_eq!(
            arena
                .validate_handle(&handle, 44)
                .expect("validate")
                .ownership,
            ownership
        );
        assert!(matches!(
            arena.validate_handle(&handle, 45),
            Err(super::ArenaError::HandleQueryEpochMismatch { .. })
        ));

        let other = SharedArena::new(ArenaConfig {
            namespace: ArenaNamespace {
                tenant: "tenant-b".to_string(),
                security_domain: "domain-b".to_string(),
                arena_name: "input".to_string(),
            },
            kind: ArenaKind::Input,
            backing: ArenaBacking::MemfdSealed,
            premap_bytes: 1024,
            buffer_count: 2,
            reclaim_policy: LeaseReclaimPolicy::default(),
        });
        assert!(matches!(
            other.validate_handle(&handle, 44),
            Err(super::ArenaError::HandleNamespaceMismatch { .. })
        ));
    }
}
