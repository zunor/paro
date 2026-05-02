// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepared commit plans and request validation inputs.

use super::{
    AbortReason, CommandId, CommitAckPolicy, CommitRequestValidationError, CommittedTxnRecord,
    FrozenLockSet, FrozenReadSet, IsolationLevel, ParticipantDescriptor, ParticipantRole,
};
use crate::participant_state::ParticipantStateSet;
use crate::types::{CommitTs, DatabaseId, ReadTs, TxnId};
use crate::view::TransactionView;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedParticipant {
    descriptor: ParticipantDescriptor,
    role: ParticipantRole,
    prepared_bytes: usize,
    write_count: usize,
}

impl PreparedParticipant {
    #[inline]
    pub const fn new(
        descriptor: ParticipantDescriptor,
        required: bool,
        prepared_bytes: usize,
        write_count: usize,
    ) -> Self {
        let role = if required {
            ParticipantRole::Required
        } else {
            ParticipantRole::Deferred
        };
        Self::with_role(descriptor, role, prepared_bytes, write_count)
    }

    #[inline]
    pub const fn with_role(
        descriptor: ParticipantDescriptor,
        role: ParticipantRole,
        prepared_bytes: usize,
        write_count: usize,
    ) -> Self {
        Self {
            descriptor: descriptor.with_role(role),
            role,
            prepared_bytes,
            write_count,
        }
    }

    #[inline]
    pub const fn descriptor(&self) -> &ParticipantDescriptor {
        &self.descriptor
    }

    #[inline]
    pub const fn is_required(&self) -> bool {
        self.role.is_required()
    }

    #[inline]
    pub const fn role(&self) -> ParticipantRole {
        self.role
    }

    #[inline]
    pub const fn prepared_bytes(&self) -> usize {
        self.prepared_bytes
    }

    #[inline]
    pub const fn write_count(&self) -> usize {
        self.write_count
    }
}

pub trait PreparedCommitPart {
    fn prepared_participant(&self) -> &PreparedParticipant;

    #[inline]
    fn descriptor(&self) -> &ParticipantDescriptor {
        self.prepared_participant().descriptor()
    }

    #[inline]
    fn is_required(&self) -> bool {
        self.prepared_participant().is_required()
    }

    #[inline]
    fn prepared_bytes(&self) -> usize {
        self.prepared_participant().prepared_bytes()
    }

    #[inline]
    fn write_count(&self) -> usize {
        self.prepared_participant().write_count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPlan {
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub frozen_read_set: FrozenReadSet,
    pub lock_set: FrozenLockSet,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommitPlan {
    #[inline]
    pub fn from_request(request: &CommitRequest) -> Self {
        Self {
            database_id: request.database_id,
            txn_id: request.txn_id,
            read_ts: request.read_ts,
            isolation: request.isolation,
            frozen_read_set: request.frozen_read_set.clone(),
            lock_set: request.lock_set.clone(),
            participants: request.participants.clone(),
        }
    }

    #[inline]
    pub fn contains_participant(&self, descriptor: &ParticipantDescriptor) -> bool {
        self.participants
            .iter()
            .any(|candidate| candidate == descriptor)
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationContext {
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub participant_count: usize,
    pub lock_count: usize,
}

impl ValidationContext {
    #[inline]
    pub fn from_plan(plan: &CommitPlan) -> Self {
        Self {
            read_ts: plan.read_ts,
            isolation: plan.isolation,
            participant_count: plan.participants.len(),
            lock_count: plan.lock_set.held_lock_count(),
        }
    }
}

pub trait CommitParticipant {
    type Prepared: PreparedCommitPart;
    type Error;

    fn prepare(&self, view: &TransactionView) -> std::result::Result<Self::Prepared, Self::Error>;

    fn validate(
        &self,
        plan: &CommitPlan,
        ctx: &ValidationContext,
    ) -> std::result::Result<(), Self::Error>;

    fn descriptor(
        &self,
        prepared: &Self::Prepared,
    ) -> std::result::Result<ParticipantDescriptor, Self::Error>;

    fn abort(&self, reason: AbortReason) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
    pub transaction_view: TransactionView,
    pub read_ts: ReadTs,
    pub isolation: IsolationLevel,
    pub command_id: CommandId,
    pub ack_policy: CommitAckPolicy,
    pub frozen_read_set: FrozenReadSet,
    pub participant_states: ParticipantStateSet,
    pub lock_set: FrozenLockSet,
    pub participants: Vec<ParticipantDescriptor>,
}

impl CommitRequest {
    pub fn new(
        database_id: DatabaseId,
        txn_id: TxnId,
        transaction_view: TransactionView,
        ack_policy: CommitAckPolicy,
        lock_set: FrozenLockSet,
        mut participants: Vec<ParticipantDescriptor>,
    ) -> Self {
        let frozen_read_set = transaction_view.frozen_read_set();
        let participant_states = transaction_view.participant_states().clone();
        for state in participant_states.iter() {
            let descriptor = ParticipantDescriptor::new(
                state.participant_id(),
                state.participant_kind(),
                state.resource_key(),
            );
            if !participants.contains(&descriptor) {
                participants.push(descriptor);
            }
        }
        Self {
            database_id,
            txn_id,
            read_ts: transaction_view.read_ts(),
            isolation: transaction_view.isolation_level(),
            command_id: transaction_view.command_id(),
            transaction_view,
            ack_policy,
            frozen_read_set,
            participant_states,
            lock_set,
            participants,
        }
    }

    #[inline]
    pub fn commit_plan(&self) -> CommitPlan {
        CommitPlan::from_request(self)
    }

    #[inline]
    pub fn validation_context(&self) -> ValidationContext {
        ValidationContext::from_plan(&self.commit_plan())
    }

    #[inline]
    pub fn committed_record(&self, commit_ts: CommitTs) -> CommittedTxnRecord {
        CommittedTxnRecord::new(self, commit_ts)
    }

    #[inline]
    pub fn add_participant(&mut self, participant: ParticipantDescriptor) {
        if !self.participants.contains(&participant) {
            self.participants.push(participant);
        }
    }

    #[inline]
    pub fn add_participants(
        &mut self,
        participants: impl IntoIterator<Item = ParticipantDescriptor>,
    ) {
        for participant in participants {
            self.add_participant(participant);
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

    pub fn validate_single_database(
        &self,
        runtime_database_id: DatabaseId,
        plan: Option<&CommitPlan>,
    ) -> std::result::Result<(), CommitRequestValidationError> {
        if self.database_id != runtime_database_id {
            return Err(CommitRequestValidationError::RuntimeDatabaseMismatch {
                runtime: runtime_database_id,
                request: self.database_id,
            });
        }
        if let Some(plan) = plan {
            if plan.database_id != self.database_id {
                return Err(CommitRequestValidationError::PlanDatabaseMismatch {
                    request: self.database_id,
                    plan: plan.database_id,
                });
            }
        }
        for participant in &self.participants {
            let participant_database_id = participant.resource_key.database_id();
            if participant_database_id != self.database_id {
                return Err(CommitRequestValidationError::ParticipantDatabaseMismatch {
                    expected: self.database_id,
                    actual: participant_database_id,
                    participant_id: participant.participant_id,
                    kind: participant.kind,
                });
            }
        }
        Ok(())
    }
}
