// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::descriptor::{ColumnDescriptor, ColumnDescriptorError};

pub const CURRENT_ABI_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseState {
    Allocated,
    Writing,
    Committed,
    Released,
    Aborted,
}

impl LeaseState {
    pub fn can_transition_to(self, next: LeaseState) -> bool {
        matches!(
            (self, next),
            (LeaseState::Allocated, LeaseState::Writing)
                | (LeaseState::Allocated, LeaseState::Aborted)
                | (LeaseState::Writing, LeaseState::Committed)
                | (LeaseState::Writing, LeaseState::Aborted)
                | (LeaseState::Committed, LeaseState::Released)
        )
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LeaseError {
    #[error("lease {lease_id} cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        lease_id: u64,
        from: LeaseState,
        to: LeaseState,
    },
    #[error("lease {lease_id} descriptor validation failed: {source}")]
    InvalidDescriptor {
        lease_id: u64,
        #[source]
        source: Box<ColumnDescriptorError>,
    },
    #[error("lease {lease_id} is visible to host epoch {expected_host_epoch} / query epoch {expected_query_epoch}, found host {actual_host_epoch} / query {actual_query_epoch}")]
    EpochMismatch {
        lease_id: u64,
        expected_host_epoch: u64,
        expected_query_epoch: u64,
        actual_host_epoch: u64,
        actual_query_epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseOwnership {
    pub owner_worker_epoch: u64,
    pub owner_host_epoch: u64,
    pub owner_query_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnBatchLease {
    pub version: u16,
    pub lease_id: u64,
    pub row_count: u32,
    pub state: LeaseState,
    pub ownership: LeaseOwnership,
    pub completion_fence: u64,
    pub payload_checksum: Option<u32>,
    pub columns: Vec<ColumnDescriptor>,
}

impl ColumnBatchLease {
    pub fn new(lease_id: u64, row_count: u32, ownership: LeaseOwnership) -> Self {
        Self {
            version: CURRENT_ABI_VERSION,
            lease_id,
            row_count,
            state: LeaseState::Allocated,
            ownership,
            completion_fence: 0,
            payload_checksum: None,
            columns: Vec::new(),
        }
    }

    pub fn transition_to(&mut self, next: LeaseState) -> Result<(), LeaseError> {
        if self.state.can_transition_to(next) {
            self.state = next;
            return Ok(());
        }
        Err(LeaseError::InvalidTransition {
            lease_id: self.lease_id,
            from: self.state,
            to: next,
        })
    }

    pub fn begin_write(&mut self) -> Result<(), LeaseError> {
        self.transition_to(LeaseState::Writing)
    }

    pub fn commit(
        &mut self,
        completion_fence: u64,
        payload_checksum: Option<u32>,
        columns: Vec<ColumnDescriptor>,
    ) -> Result<(), LeaseError> {
        for column in &columns {
            column
                .validate()
                .map_err(|source| LeaseError::InvalidDescriptor {
                    lease_id: self.lease_id,
                    source: Box::new(source),
                })?;
        }

        self.transition_to(LeaseState::Committed)?;
        self.completion_fence = completion_fence;
        self.payload_checksum = payload_checksum;
        self.columns = columns;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), LeaseError> {
        self.transition_to(LeaseState::Aborted)
    }

    pub fn release(&mut self) -> Result<(), LeaseError> {
        self.transition_to(LeaseState::Released)
    }

    pub fn ensure_visible_to(&self, host_epoch: u64, query_epoch: u64) -> Result<(), LeaseError> {
        if self.ownership.owner_host_epoch == host_epoch
            && self.ownership.owner_query_epoch == query_epoch
        {
            return Ok(());
        }

        Err(LeaseError::EpochMismatch {
            lease_id: self.lease_id,
            expected_host_epoch: host_epoch,
            expected_query_epoch: query_epoch,
            actual_host_epoch: self.ownership.owner_host_epoch,
            actual_query_epoch: self.ownership.owner_query_epoch,
        })
    }

    pub fn reclaimable_by_worker_epoch(&self, worker_epoch: u64) -> bool {
        self.ownership.owner_worker_epoch == worker_epoch
            && matches!(self.state, LeaseState::Allocated | LeaseState::Writing)
    }

    pub fn orphaned_for_host_epoch(&self, host_epoch: u64) -> bool {
        self.ownership.owner_host_epoch != host_epoch
            && matches!(
                self.state,
                LeaseState::Allocated | LeaseState::Writing | LeaseState::Committed
            )
    }

    pub fn orphaned_for_query_epoch(&self, query_epoch: u64) -> bool {
        self.ownership.owner_query_epoch == query_epoch
            && matches!(
                self.state,
                LeaseState::Allocated | LeaseState::Writing | LeaseState::Committed
            )
    }
}
