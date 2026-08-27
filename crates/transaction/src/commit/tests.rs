// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    CommitTs, DatabaseId, LockMode, LockNamespace, LockRequest, LockResource, ParticipantId,
    ParticipantKind, ParticipantStateSet, ReadSnapshot, ReadTrackerHandle, ReadTs, TableId,
    TransactionView, TxnId, TxnResourceKey, WriterId,
};
fn namespace() -> LockNamespace {
    LockNamespace::single_tenant(DatabaseId::new(1))
}

fn pk_resource(key_hash: u64) -> LockResource {
    LockResource::primary_key(namespace(), TableId::new(10), 20, key_hash)
}

fn table_resource() -> LockResource {
    LockResource::Table {
        namespace: namespace(),
        table_id: TableId::new(10),
    }
}

fn plan(txn_id: u64, read_ts: u64, write_set: Vec<LockResource>) -> CommitSequencingPlan {
    let lock_set = FrozenLockSet::from_locks(
        write_set
            .iter()
            .cloned()
            .map(|resource| LockRequest::new(resource, LockMode::X))
            .collect(),
    );
    let request = CommitRequest::new(
        DatabaseId::new(1),
        TxnId::new(txn_id),
        TransactionView::autocommit(ReadTs::new(read_ts)),
        CommitAckPolicy::RequiredPublished,
        lock_set,
        Vec::new(),
    );
    CommitSequencingPlan::new(request.commit_plan(), write_set)
}

fn serializable_plan(
    txn_id: u64,
    read_ts: u64,
    write_set: Vec<LockResource>,
) -> CommitSequencingPlan {
    let mut plan = plan(txn_id, read_ts, write_set);
    plan.plan.isolation = IsolationLevel::Serializable;
    plan
}

#[test]
fn commit_plan_write_set_drops_shadowed_table_intent() {
    let table = table_resource();
    let key = pk_resource(44);
    let request = CommitRequest::new(
        DatabaseId::new(1),
        TxnId::new(7),
        TransactionView::autocommit(ReadTs::new(3)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::from_locks(vec![
            LockRequest::new(table.clone(), LockMode::IX),
            LockRequest::new(key.clone(), LockMode::X),
        ]),
        Vec::new(),
    );

    let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());

    assert_eq!(plan.write_set, vec![key]);
}

#[test]
fn commit_plan_write_set_keeps_table_intent_without_finer_write() {
    let table = table_resource();
    let request = CommitRequest::new(
        DatabaseId::new(1),
        TxnId::new(7),
        TransactionView::autocommit(ReadTs::new(3)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::from_locks(vec![LockRequest::new(table.clone(), LockMode::IX)]),
        Vec::new(),
    );

    let plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());

    assert_eq!(plan.write_set, vec![table]);
}

#[test]
fn commit_request_carries_fixed_semantic_fields() {
    let tracker = ReadTrackerHandle::recording();
    let view = TransactionView::new(
        WriterId::new(42),
        ReadTs::new(12),
        ReadSnapshot::without_lease(ReadTs::new(11)),
        IsolationLevel::Snapshot,
        CommandId::new(3),
        tracker,
        ParticipantStateSet::empty(),
    );
    view.read_tracker().record_table_read(TableId::new(99));
    view.read_tracker()
        .record_tablet_read(TableId::new(99), 7, ReadTs::new(11), 3, 2);
    let lock = LockRequest::new(
        LockResource::Table {
            namespace: LockNamespace::single_tenant(DatabaseId::new(7)),
            table_id: TableId::new(99),
        },
        LockMode::IX,
    );

    let request = CommitRequest::new(
        DatabaseId::new(7),
        TxnId::new(42),
        view,
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::from_locks(vec![lock]),
        Vec::new(),
    );

    assert_eq!(request.database_id, DatabaseId::new(7));
    assert_eq!(request.txn_id, TxnId::new(42));
    assert_eq!(request.read_ts, ReadTs::new(11));
    assert_eq!(request.command_id.into_raw(), 3);
    assert_eq!(request.frozen_read_set.dependency_count(), 2);
    assert_eq!(request.frozen_read_set.storage_snapshot_count(), 1);
    assert_eq!(request.lock_set.held_lock_count(), 1);
    assert!(request.participant_states.is_empty());
    let plan = request.commit_plan();
    let ctx = ValidationContext::from_plan(&plan);
    assert_eq!(ctx.read_ts, ReadTs::new(11));
    assert_eq!(ctx.participant_count, 0);
}

#[test]
fn committed_record_carries_versions_and_participants() {
    let descriptor = ParticipantDescriptor::new(
        ParticipantId::new(1),
        ParticipantKind::Storage,
        TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
    );
    let request = CommitRequest::new(
        DatabaseId::new(9),
        TxnId::new(100),
        TransactionView::autocommit(ReadTs::new(50)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::empty(),
        vec![descriptor.clone()],
    );

    let record = request.committed_record(CommitTs::new(77));
    assert_eq!(record.record_version, COMMITTED_TXN_RECORD_VERSION);
    assert_eq!(record.commit_ts, CommitTs::new(77));
    assert_eq!(record.participants, vec![descriptor]);
    assert_eq!(
        record.participants[0].descriptor_version,
        PARTICIPANT_DESCRIPTOR_VERSION
    );
    record
        .validate_versions()
        .expect("fresh committed record versions are supported");
}

#[test]
fn committed_record_rejects_unsupported_versions() {
    let descriptor = ParticipantDescriptor::new(
        ParticipantId::new(1),
        ParticipantKind::Storage,
        TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
    );
    let request = CommitRequest::new(
        DatabaseId::new(9),
        TxnId::new(100),
        TransactionView::autocommit(ReadTs::new(50)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::empty(),
        vec![descriptor],
    );

    let mut bad_record = request.committed_record(CommitTs::new(77));
    bad_record.record_version = COMMITTED_TXN_RECORD_VERSION + 1;
    assert!(matches!(
        bad_record.validate_versions(),
        Err(CommitRecordVersionError::UnsupportedCommittedRecordVersion { .. })
    ));

    let mut bad_participant = request.committed_record(CommitTs::new(78));
    bad_participant.participants[0].descriptor_version = PARTICIPANT_DESCRIPTOR_VERSION + 1;
    assert!(matches!(
        bad_participant.validate_versions(),
        Err(CommitRecordVersionError::UnsupportedParticipantDescriptorVersion { .. })
    ));
}

#[test]
fn participant_roles_split_required_and_deferred_sets() {
    let required = ParticipantDescriptor::new(
        ParticipantId::new(1),
        ParticipantKind::Storage,
        TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
    );
    let deferred = ParticipantDescriptor::new(
        ParticipantId::new(2),
        ParticipantKind::Search,
        TxnResourceKey::table(
            ParticipantKind::Search,
            DatabaseId::new(9),
            TableId::new(10),
        ),
    )
    .deferred();
    let request = CommitRequest::new(
        DatabaseId::new(9),
        TxnId::new(101),
        TransactionView::autocommit(ReadTs::new(50)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::empty(),
        vec![required.clone(), deferred.clone()],
    );

    assert_eq!(request.required_participants().count(), 1);
    assert_eq!(request.deferred_participants().count(), 1);
    let record = request.committed_record(CommitTs::new(77));
    assert_eq!(
        record.required_participants().cloned().collect::<Vec<_>>(),
        vec![required]
    );
    assert_eq!(
        record.deferred_participants().cloned().collect::<Vec<_>>(),
        vec![deferred]
    );
}

#[test]
fn commit_request_rejects_cross_database_participant() {
    let participant = ParticipantDescriptor::new(
        ParticipantId::new(1),
        ParticipantKind::Storage,
        TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(10)),
    );
    let request = CommitRequest::new(
        DatabaseId::new(9),
        TxnId::new(100),
        TransactionView::autocommit(ReadTs::new(50)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::empty(),
        vec![participant],
    );

    let err = request
        .validate_single_database(DatabaseId::new(9), Some(&request.commit_plan()))
        .unwrap_err();

    match err {
        CommitRequestValidationError::ParticipantDatabaseMismatch {
            expected, actual, ..
        } => {
            assert_eq!(expected, DatabaseId::new(9));
            assert_eq!(actual, DatabaseId::new(10));
        }
        other => panic!("unexpected validation error: {other:?}"),
    }
}

#[test]
fn backpressure_rejects_global_and_required_participant_lag() {
    let controller = CommitBackpressureController::new(CommitBackpressureOptions {
        max_unpublished_commits: 2,
        max_participant_apply_lag: 1,
    });
    let required = ParticipantDescriptor::new(
        ParticipantId::new(1),
        ParticipantKind::Storage,
        TxnResourceKey::database(ParticipantKind::Storage, DatabaseId::new(9)),
    );
    let deferred = ParticipantDescriptor::new(
        ParticipantId::new(2),
        ParticipantKind::Search,
        TxnResourceKey::table(
            ParticipantKind::Search,
            DatabaseId::new(9),
            TableId::new(10),
        ),
    )
    .deferred();
    let request = CommitRequest::new(
        DatabaseId::new(9),
        TxnId::new(102),
        TransactionView::autocommit(ReadTs::new(50)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::empty(),
        vec![required.clone(), deferred.clone()],
    );
    let plan = request.commit_plan();

    controller.record_durable(CommitTs::new(1), &[required.clone(), deferred]);
    assert!(matches!(
        controller.admit(&plan),
        Err(CommitBackpressureError::ParticipantLag { lag: 1, .. })
    ));
    controller.record_published(CommitTs::new(1), &[required]);
    assert!(controller.admit(&plan).is_ok());

    controller.sync_frontiers(CommitTs::new(3), CommitTs::new(1));
    assert!(matches!(
        controller.admit(&plan),
        Err(CommitBackpressureError::GlobalLag { lag: 2, .. })
    ));
    assert_eq!(controller.snapshot().throttle_count, 2);
}

#[test]
fn commit_sequencer_advances_only_after_append_success() {
    let sequencer = CommitSequencer::new(CommitTs::new(10), CommitSequencerOptions::default());
    let first = plan(1, 9, vec![pk_resource(1)]);
    let err = sequencer
        .sequence_batch(vec![first], |_| Err::<(), _>("append failed"))
        .unwrap_err();
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(10));
    let CommitSequencerError::Append {
        provisional_start,
        provisional_count,
        ..
    } = err;
    assert_eq!(provisional_start, CommitTs::new(10));
    assert_eq!(provisional_count, 1);

    let second = plan(2, 9, vec![pk_resource(2)]);
    let batch = sequencer
        .sequence_batch(vec![second], |_| Ok::<_, ()>(()))
        .unwrap();
    assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(10));
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(11));
    assert_eq!(sequencer.metrics_snapshot().append_failures, 1);
}

#[test]
fn in_flight_batch_rejects_later_conflicting_write() {
    let sequencer = CommitSequencer::new(CommitTs::new(20), CommitSequencerOptions::default());
    let first = plan(1, 18, vec![pk_resource(7)]);
    let second = plan(2, 18, vec![pk_resource(7)]);
    let batch = sequencer
        .sequence_batch(vec![first, second], |accepted| {
            assert_eq!(accepted.len(), 1);
            Ok::<_, ()>(())
        })
        .unwrap();

    assert_eq!(batch.accepted.len(), 1);
    assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(20));
    assert_eq!(batch.rejected.len(), 1);
    assert_eq!(
        batch.rejected[0].reason,
        CommitFenceRejectReason::InBatchWriteConflict
    );
    let metrics = sequencer.metrics_snapshot();
    assert_eq!(metrics.reject_in_batch_write_conflict, 1);
}

#[test]
fn in_flight_batch_accepts_disjoint_primary_key_writes() {
    let sequencer = CommitSequencer::new(CommitTs::new(20), CommitSequencerOptions::default());
    let first = plan(1, 18, vec![pk_resource(7)]);
    let second = plan(2, 18, vec![pk_resource(8)]);
    let batch = sequencer
        .sequence_batch(vec![first, second], |accepted| {
            assert_eq!(accepted.len(), 2);
            Ok::<_, ()>(())
        })
        .unwrap();

    assert_eq!(batch.accepted.len(), 2);
    assert!(batch.rejected.is_empty());
    assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(20));
    assert_eq!(batch.accepted[1].commit_ts, CommitTs::new(21));
}

#[test]
fn group_commit_batch_size_limit_is_hard_cap() {
    let options = CommitSequencerOptions {
        max_group_commit_batch_size: 1,
        ..CommitSequencerOptions::default()
    };
    let sequencer = CommitSequencer::new(CommitTs::new(30), options);
    let batch = sequencer
        .sequence_batch(
            vec![
                plan(1, 29, vec![pk_resource(1)]),
                plan(2, 29, vec![pk_resource(2)]),
            ],
            |_| Ok::<_, ()>(()),
        )
        .unwrap();

    assert_eq!(batch.accepted.len(), 1);
    assert_eq!(batch.rejected.len(), 1);
    assert_eq!(
        batch.rejected[0].reason,
        CommitFenceRejectReason::BatchSizeLimit
    );
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(31));
}

#[test]
fn group_commit_fence_budget_records_reject_reason() {
    let options = CommitSequencerOptions {
        max_group_commit_fence_us: 0,
        ..CommitSequencerOptions::default()
    };
    let sequencer = CommitSequencer::new(CommitTs::new(40), options);
    let batch = sequencer
        .sequence_batch(
            vec![plan(1, 39, vec![pk_resource(1)])],
            |_| -> std::result::Result<(), ()> {
                panic!("append must not run when fence rejects all plans")
            },
        )
        .unwrap();

    assert!(batch.accepted.is_empty());
    assert_eq!(batch.rejected.len(), 1);
    assert!(matches!(
        batch.rejected[0].reason,
        CommitFenceRejectReason::FenceBudgetExceeded { limit_us: 0, .. }
    ));
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(40));
    assert_eq!(sequencer.metrics_snapshot().reject_fence_budget_exceeded, 1);
}

#[test]
fn in_flight_batch_rejects_stale_ssi_epoch() {
    let sequencer = CommitSequencer::new(CommitTs::new(50), CommitSequencerOptions::default());
    let first = serializable_plan(1, 49, vec![pk_resource(1)])
        .with_validation_epoch(10)
        .with_ssi_effect_epoch(11);
    let second = serializable_plan(2, 49, vec![pk_resource(2)]).with_validation_epoch(10);

    let batch = sequencer
        .sequence_batch(vec![first, second], |_| Ok::<_, ()>(()))
        .unwrap();

    assert_eq!(batch.accepted.len(), 1);
    assert_eq!(batch.rejected.len(), 1);
    assert_eq!(
        batch.rejected[0].reason,
        CommitFenceRejectReason::SsiEpochAdvanced {
            validation_epoch: 10,
            batch_effect_epoch: 11
        }
    );
}

#[test]
fn in_flight_ssi_epoch_does_not_reject_snapshot_plan() {
    let sequencer = CommitSequencer::new(CommitTs::new(55), CommitSequencerOptions::default());
    let first = serializable_plan(1, 54, vec![pk_resource(1)])
        .with_validation_epoch(10)
        .with_ssi_effect_epoch(11);
    let second = plan(2, 54, vec![pk_resource(2)]).with_validation_epoch(10);

    let batch = sequencer
        .sequence_batch(vec![first, second], |_| Ok::<_, ()>(()))
        .unwrap();

    assert_eq!(batch.accepted.len(), 2);
    assert!(batch.rejected.is_empty());
}

#[test]
fn external_final_fence_runs_before_commit_timestamp_assignment() {
    let sequencer = CommitSequencer::new(CommitTs::new(60), CommitSequencerOptions::default());
    let first = serializable_plan(1, 59, vec![pk_resource(1)]).with_validation_epoch(12);
    let second = serializable_plan(2, 59, vec![pk_resource(2)]).with_validation_epoch(12);

    let batch = sequencer
        .sequence_batch_with_fence(
            vec![first, second],
            |plan, in_flight| {
                if plan.plan.txn_id == TxnId::new(2) {
                    assert_eq!(in_flight.accepted().len(), 1);
                    return Some(CommitFenceRejectReason::SsiStateEpochAdvanced {
                        validation_epoch: plan.validation_epoch,
                        current_epoch: 13,
                    });
                }
                None
            },
            |accepted| {
                assert_eq!(accepted.len(), 1);
                Ok::<_, ()>(())
            },
        )
        .unwrap();

    assert_eq!(batch.accepted.len(), 1);
    assert_eq!(batch.accepted[0].commit_ts, CommitTs::new(60));
    assert_eq!(batch.rejected.len(), 1);
    assert_eq!(
        batch.rejected[0].reason,
        CommitFenceRejectReason::SsiStateEpochAdvanced {
            validation_epoch: 12,
            current_epoch: 13
        }
    );
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(61));
    assert_eq!(sequencer.metrics_snapshot().reject_ssi_epoch_advanced, 1);
}

#[test]
fn ordered_batch_advances_clock_only_after_append_success() {
    let sequencer = CommitSequencer::default();
    let batch = sequencer
        .sequence_ordered_batch(
            vec![
                OrderedCommitPlan {
                    sequencing_plan: plan(1, 1, Vec::new()),
                    payload: 10_u64,
                },
                OrderedCommitPlan {
                    sequencing_plan: plan(2, 1, Vec::new()),
                    payload: 20_u64,
                },
            ],
            |_, _| None,
            |commit_ts, ordered| (commit_ts, ordered.payload),
            |accepted| {
                assert_eq!(accepted[0].0, CommitTs::new(1));
                assert_eq!(accepted[1].0, CommitTs::new(2));
                Ok::<_, CommitSequencerAppendError<&'static str, _>>(accepted.len())
            },
        )
        .unwrap();

    assert_eq!(batch.append_output, Some(2));
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(3));
}

#[test]
fn ordered_batch_keeps_clock_on_append_failed() {
    let sequencer = CommitSequencer::default();
    let err = sequencer
        .sequence_ordered_batch(
            vec![OrderedCommitPlan {
                sequencing_plan: plan(1, 1, Vec::new()),
                payload: 10_u64,
            }],
            |_, _| None,
            |commit_ts, ordered| (commit_ts, ordered.payload),
            |accepted| {
                Err::<usize, _>(CommitSequencerAppendError::append_failed(
                    "append failed",
                    accepted,
                ))
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        CommitSequencerOrderedError::Append {
            durable_committed: false,
            ..
        }
    ));
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(1));
}

#[test]
fn ordered_batch_advances_clock_on_durable_committed_error() {
    let sequencer = CommitSequencer::default();
    let err = sequencer
        .sequence_ordered_batch(
            vec![OrderedCommitPlan {
                sequencing_plan: plan(1, 1, Vec::new()),
                payload: 10_u64,
            }],
            |_, _| None,
            |commit_ts, ordered| (commit_ts, ordered.payload),
            |accepted| {
                Err::<usize, _>(CommitSequencerAppendError::durable_committed(
                    "protocol violation",
                    accepted,
                ))
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        CommitSequencerOrderedError::Append {
            durable_committed: true,
            ..
        }
    ));
    assert_eq!(sequencer.next_commit_ts(), CommitTs::new(2));
}
