// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable commit record facade and publish participant contracts.

use super::{
    CommitRecordVersionError, CommitRequest, ParticipantDescriptor, COMMITTED_TXN_RECORD_VERSION,
    MAINTENANCE_RECORD_VERSION,
};
use crate::types::{CommitTs, DatabaseId, ReadTs, TxnId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTxnRecord {
    pub record_version: u16,
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommittedTxnRecord {
    #[inline]
    pub fn new(request: &CommitRequest, commit_ts: CommitTs) -> Self {
        Self {
            record_version: COMMITTED_TXN_RECORD_VERSION,
            database_id: request.database_id,
            txn_id: request.txn_id,
            read_ts: request.read_ts,
            commit_ts,
            participants: request.participants.clone(),
        }
    }

    #[inline]
    pub fn required_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_required())
    }

    #[inline]
    pub fn deferred_participants(&self) -> impl Iterator<Item = &ParticipantDescriptor> {
        self.participants
            .iter()
            .filter(|descriptor| descriptor.is_deferred())
    }

    pub fn validate_versions(&self) -> std::result::Result<(), CommitRecordVersionError> {
        if self.record_version != COMMITTED_TXN_RECORD_VERSION {
            return Err(
                CommitRecordVersionError::UnsupportedCommittedRecordVersion {
                    found: self.record_version,
                    expected: COMMITTED_TXN_RECORD_VERSION,
                },
            );
        }
        for participant in &self.participants {
            participant.validate_version()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceRecord {
    pub record_version: u16,
    pub maintenance_id: u64,
}

impl MaintenanceRecord {
    #[inline]
    pub const fn new(maintenance_id: u64) -> Self {
        Self {
            record_version: MAINTENANCE_RECORD_VERSION,
            maintenance_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedRecord {
    Transaction(CommittedTxnRecord),
    Maintenance(MaintenanceRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishResult {
    pub required: bool,
    pub published_ts: Option<CommitTs>,
}

impl PublishResult {
    #[inline]
    pub const fn required(published_ts: CommitTs) -> Self {
        Self {
            required: true,
            published_ts: Some(published_ts),
        }
    }

    #[inline]
    pub const fn deferred() -> Self {
        Self {
            required: false,
            published_ts: None,
        }
    }
}

pub trait CommittedRecordApplier {
    type Error;

    fn applies_to(&self, descriptor: &ParticipantDescriptor) -> bool;

    fn apply_required(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> std::result::Result<PublishResult, Self::Error>;

    fn apply_deferred(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> std::result::Result<PublishResult, Self::Error>;
}
