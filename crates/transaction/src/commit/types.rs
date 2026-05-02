// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit scalar types and participant descriptors.

use crate::types::{DatabaseId, ParticipantId, ParticipantKind, TxnResourceKey};
use crate::view::ReadDependency;
use crate::LockRequest;
use std::fmt;

pub const PARTICIPANT_DESCRIPTOR_VERSION: u16 = 1;
pub const COMMITTED_TXN_RECORD_VERSION: u16 = 1;
pub const MAINTENANCE_RECORD_VERSION: u16 = 1;
pub const DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE: usize = 256;
pub const DEFAULT_MAX_GROUP_COMMIT_FENCE_US: u64 = 500;
pub const DEFAULT_MAX_UNPUBLISHED_COMMITS: u64 = 1024;
pub const DEFAULT_MAX_PARTICIPANT_APPLY_LAG: u64 = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRecordVersionError {
    UnsupportedCommittedRecordVersion {
        found: u16,
        expected: u16,
    },
    UnsupportedParticipantDescriptorVersion {
        participant_id: ParticipantId,
        found: u16,
        expected: u16,
    },
}

impl fmt::Display for CommitRecordVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCommittedRecordVersion { found, expected } => write!(
                f,
                "unsupported committed transaction record version {found}, expected {expected}"
            ),
            Self::UnsupportedParticipantDescriptorVersion {
                participant_id,
                found,
                expected,
            } => write!(
                f,
                "unsupported participant descriptor version {found} for participant {participant_id}, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for CommitRecordVersionError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CommitAckPolicy {
    #[default]
    RequiredPublished,
    DurableOnlyAsync,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    Serializable,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(u32);

impl CommandId {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn into_raw(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrozenReadSet {
    dependency_count: usize,
    coarsened: bool,
    dependencies: Vec<ReadDependency>,
}

impl FrozenReadSet {
    #[inline]
    pub const fn empty() -> Self {
        Self {
            dependency_count: 0,
            coarsened: false,
            dependencies: Vec::new(),
        }
    }

    #[inline]
    pub const fn from_dependency_count(dependency_count: usize) -> Self {
        Self {
            dependency_count,
            coarsened: false,
            dependencies: Vec::new(),
        }
    }

    #[inline]
    pub fn from_dependencies(dependencies: Vec<ReadDependency>) -> Self {
        Self {
            dependency_count: dependencies.len(),
            coarsened: false,
            dependencies,
        }
    }

    #[inline]
    pub fn from_dependencies_with_coarsening(
        dependencies: Vec<ReadDependency>,
        coarsened: bool,
    ) -> Self {
        Self {
            dependency_count: dependencies.len(),
            coarsened,
            dependencies,
        }
    }

    #[inline]
    pub const fn dependency_count(&self) -> usize {
        self.dependency_count
    }

    #[inline]
    pub const fn is_coarsened(&self) -> bool {
        self.coarsened
    }

    #[inline]
    pub fn dependencies(&self) -> &[ReadDependency] {
        &self.dependencies
    }

    #[inline]
    pub fn storage_snapshot_count(&self) -> usize {
        self.dependencies
            .iter()
            .filter(|dependency| matches!(dependency, ReadDependency::Tablet { .. }))
            .count()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrozenLockSet {
    locks: Vec<LockRequest>,
}

impl FrozenLockSet {
    #[inline]
    pub const fn empty() -> Self {
        Self { locks: Vec::new() }
    }

    #[inline]
    pub fn from_locks(locks: Vec<LockRequest>) -> Self {
        Self { locks }
    }

    #[inline]
    pub fn held_lock_count(&self) -> usize {
        self.locks.len()
    }

    #[inline]
    pub fn locks(&self) -> &[LockRequest] {
        &self.locks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParticipantRole {
    Required,
    Deferred,
}

impl ParticipantRole {
    #[inline]
    pub const fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }

    #[inline]
    pub const fn is_deferred(self) -> bool {
        matches!(self, Self::Deferred)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParticipantDescriptor {
    pub descriptor_version: u16,
    pub participant_id: ParticipantId,
    pub kind: ParticipantKind,
    pub resource_key: TxnResourceKey,
    pub role: ParticipantRole,
}

impl ParticipantDescriptor {
    #[inline]
    pub const fn new(
        participant_id: ParticipantId,
        kind: ParticipantKind,
        resource_key: TxnResourceKey,
    ) -> Self {
        Self::with_descriptor_version(
            PARTICIPANT_DESCRIPTOR_VERSION,
            participant_id,
            kind,
            resource_key,
        )
    }

    #[inline]
    pub const fn with_descriptor_version(
        descriptor_version: u16,
        participant_id: ParticipantId,
        kind: ParticipantKind,
        resource_key: TxnResourceKey,
    ) -> Self {
        Self {
            descriptor_version,
            participant_id,
            kind,
            resource_key,
            role: ParticipantRole::Required,
        }
    }

    #[inline]
    pub const fn with_role(mut self, role: ParticipantRole) -> Self {
        self.role = role;
        self
    }

    #[inline]
    pub const fn required(self) -> Self {
        self.with_role(ParticipantRole::Required)
    }

    #[inline]
    pub const fn deferred(self) -> Self {
        self.with_role(ParticipantRole::Deferred)
    }

    #[inline]
    pub const fn is_required(&self) -> bool {
        self.role.is_required()
    }

    #[inline]
    pub const fn is_deferred(&self) -> bool {
        self.role.is_deferred()
    }

    #[inline]
    pub fn validate_version(&self) -> std::result::Result<(), CommitRecordVersionError> {
        if self.descriptor_version != PARTICIPANT_DESCRIPTOR_VERSION {
            return Err(
                CommitRecordVersionError::UnsupportedParticipantDescriptorVersion {
                    participant_id: self.participant_id,
                    found: self.descriptor_version,
                    expected: PARTICIPANT_DESCRIPTOR_VERSION,
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRequestValidationError {
    RuntimeDatabaseMismatch {
        runtime: DatabaseId,
        request: DatabaseId,
    },
    PlanDatabaseMismatch {
        request: DatabaseId,
        plan: DatabaseId,
    },
    ParticipantDatabaseMismatch {
        expected: DatabaseId,
        actual: DatabaseId,
        participant_id: ParticipantId,
        kind: ParticipantKind,
    },
}

impl fmt::Display for CommitRequestValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeDatabaseMismatch { runtime, request } => write!(
                f,
                "commit runtime database mismatch: runtime={} request={}",
                runtime, request
            ),
            Self::PlanDatabaseMismatch { request, plan } => write!(
                f,
                "commit plan database mismatch: request={} plan={}",
                request, plan
            ),
            Self::ParticipantDatabaseMismatch {
                expected,
                actual,
                participant_id,
                kind,
            } => write!(
                f,
                "commit participant database mismatch: expected={} actual={} participant_id={} kind={:?}",
                expected, actual, participant_id, kind
            ),
        }
    }
}

impl std::error::Error for CommitRequestValidationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    ValidationFailed,
    DurableAppendFailed,
    Backpressure,
    UserRollback,
    CoordinatorShutdown,
}
