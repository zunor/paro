// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable-record apply runtime contracts.

use paro_common::effect::{StorageCommitOp, TabletMutation};
use paro_common::error::Result;
use paro_common::journal::{CommitRecord, CommittedRecord, MaintenanceRecord};

pub use crate::apply_queue::{
    ApplyCompletion, ApplyCompletionFallbackAck, ApplyErrorSource, ApplyFatalSink, ApplyPhase,
    ApplyRequest, ApplyRuntimeError, ApplySubmitResult, JournalApplyError,
    JournalApplyMetricsSnapshot, JournalApplyRuntime, JournalPublicationObserver,
    RecoveryPlaceholderRecordKind, TabletApplyPart,
};
pub use crate::waiter::WaitMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationKind {
    PublishRowset,
    ApplyPrimaryDelete,
    ApplyDeletePatch,
    PublishCompaction,
    PublishSearchGeneration,
    RetireSearchGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MutationIdentity {
    pub commit_ts: u64,
    pub tablet_id: u64,
    pub mutation_kind: MutationKind,
    pub artifact_id: u64,
}

pub trait VisibilityPublisher {
    fn publish_transaction(&self, record: &CommitRecord, wait_mode: WaitMode) -> Result<()>;
}

pub trait MaintenanceApplyHandler {
    fn apply_maintenance(&self, record: &MaintenanceRecord, wait_mode: WaitMode) -> Result<()>;
}

pub fn publish_committed_record(
    record: &CommittedRecord,
    visibility: &impl VisibilityPublisher,
    maintenance: &impl MaintenanceApplyHandler,
    wait_mode: WaitMode,
) -> Result<()> {
    match record {
        CommittedRecord::Transaction(record) => visibility.publish_transaction(record, wait_mode),
        CommittedRecord::Maintenance(record) => maintenance.apply_maintenance(record, wait_mode),
    }
}

pub fn mutation_identities(
    commit_ts: u64,
    storage_ops: &[StorageCommitOp],
) -> Vec<MutationIdentity> {
    let mutation_count = storage_ops
        .iter()
        .map(|op| match op {
            StorageCommitOp::Tablet(tablet) => tablet.mutations.len(),
        })
        .sum();
    let mut identities = Vec::with_capacity(mutation_count);
    for op in storage_ops {
        let StorageCommitOp::Tablet(tablet) = op;
        for mutation in &tablet.mutations {
            identities.push(MutationIdentity {
                commit_ts,
                tablet_id: tablet.tablet_id,
                mutation_kind: mutation_kind(mutation),
                artifact_id: mutation_artifact_id(mutation),
            });
        }
    }
    identities
}

pub fn mutation_identity_for_tablet(
    commit_ts: u64,
    tablet_id: u64,
    mutation: &TabletMutation,
) -> MutationIdentity {
    MutationIdentity {
        commit_ts,
        tablet_id,
        mutation_kind: mutation_kind(mutation),
        artifact_id: mutation_artifact_id(mutation),
    }
}

fn mutation_kind(mutation: &TabletMutation) -> MutationKind {
    match mutation {
        TabletMutation::PublishRowset { .. } => MutationKind::PublishRowset,
        TabletMutation::ApplyPrimaryDelete { .. } => MutationKind::ApplyPrimaryDelete,
        TabletMutation::ApplyDeletePatch { .. } => MutationKind::ApplyDeletePatch,
        TabletMutation::PublishCompaction { .. } => MutationKind::PublishCompaction,
        TabletMutation::PublishSearchGeneration { .. } => MutationKind::PublishSearchGeneration,
        TabletMutation::RetireSearchGeneration { .. } => MutationKind::RetireSearchGeneration,
    }
}

fn mutation_artifact_id(mutation: &TabletMutation) -> u64 {
    mutation.stable_artifact_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::effect::{
        ArtifactNamespace, ArtifactRef, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline,
        DeletePatchRef, DeletePatchSegment, StorageCommitOp, TabletApplyOp, VersionSpan,
    };
    use paro_common::journal::{
        JournalRecordMetadata, MaintenanceKind, COMMIT_RECORD_VERSION, MAINTENANCE_RECORD_VERSION,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct RecordingVisibility {
        commit: AtomicU64,
    }

    impl VisibilityPublisher for RecordingVisibility {
        fn publish_transaction(&self, record: &CommitRecord, _wait_mode: WaitMode) -> Result<()> {
            self.commit.store(record.commit_id, Ordering::Release);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingMaintenance {
        maintenance: AtomicU64,
    }

    impl MaintenanceApplyHandler for RecordingMaintenance {
        fn apply_maintenance(
            &self,
            record: &MaintenanceRecord,
            _wait_mode: WaitMode,
        ) -> Result<()> {
            self.maintenance
                .store(record.maintenance_id, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn committed_record_dispatches_by_variant() {
        let visibility = RecordingVisibility::default();
        let maintenance = RecordingMaintenance::default();

        publish_committed_record(
            &CommittedRecord::Transaction(CommitRecord {
                record_version: COMMIT_RECORD_VERSION,
                metadata: JournalRecordMetadata::transaction(&[], &[], &[], &[]),
                txn_id: 1,
                start_time: 1,
                commit_id: 7,
                catalog_ops: Vec::new(),
                storage_ops: Vec::new(),
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
            }),
            &visibility,
            &maintenance,
            WaitMode::Published,
        )
        .unwrap();
        publish_committed_record(
            &CommittedRecord::Maintenance(MaintenanceRecord {
                record_version: MAINTENANCE_RECORD_VERSION,
                metadata: JournalRecordMetadata::maintenance(&[], &[], &[], &[]),
                maintenance_id: 11,
                kind: MaintenanceKind::Compaction,
                catalog_ops: Vec::new(),
                storage_ops: Vec::new(),
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
            }),
            &visibility,
            &maintenance,
            WaitMode::Published,
        )
        .unwrap();

        assert_eq!(visibility.commit.load(Ordering::Acquire), 7);
        assert_eq!(maintenance.maintenance.load(Ordering::Acquire), 11);
    }

    #[test]
    fn mutation_identities_capture_commit_tablet_kind_and_artifact() {
        let identities = mutation_identities(
            13,
            &[StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: 41,
                mutations: vec![TabletMutation::PublishRowset {
                    rowset_id: 99,
                    version_span: VersionSpan { start: 13, end: 13 },
                    rowset_ref: ArtifactRef {
                        namespace: ArtifactNamespace::CanonicalRowset,
                        locator: vec!["99".to_string()],
                    },
                }],
            })],
        );

        assert_eq!(
            identities,
            vec![MutationIdentity {
                commit_ts: 13,
                tablet_id: 41,
                mutation_kind: MutationKind::PublishRowset,
                artifact_id: 99,
            }]
        );
    }

    #[test]
    fn mutation_identities_assign_stable_delete_artifacts() {
        let ops = [StorageCommitOp::Tablet(TabletApplyOp {
            tablet_id: 41,
            mutations: vec![
                TabletMutation::ApplyPrimaryDelete {
                    keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                },
                TabletMutation::ApplyDeletePatch {
                    patch: DeletePatchRef::Inline(DeletePatchInline {
                        encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                        row_count: 2,
                        groups: vec![DeletePatchGroup {
                            rowset_id: 99,
                            segments: vec![DeletePatchSegment {
                                segment_id: 7,
                                row_offsets_delta: vec![3, 5],
                            }],
                        }],
                    }),
                    deleted_row_count: 2,
                },
            ],
        })];

        let first = mutation_identities(13, &ops);
        let second = mutation_identities(13, &ops);

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_ne!(first[0].artifact_id, 0);
        assert_ne!(first[1].artifact_id, 0);
        assert_ne!(first[0].artifact_id, first[1].artifact_id);
    }
}
