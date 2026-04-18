// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::effect::{
    ApplyDescriptor, CatalogTxnOp, DeferredTask, StorageCommitOp, TabletMutation, VersionSpan,
};
use crate::error as paro_error;
use crate::error::Result;
use crate::journal::{CommitRecord, MaintenanceKind, MaintenanceRecord};

/// Admission token captured during prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareToken {
    pub visible_version: i64,
    pub rowset_epoch: u64,
    pub schema_epoch: Option<u64>,
}

/// One tablet-level participant carried by a prepared plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTabletPlan {
    pub tablet_id: u64,
    pub token: PrepareToken,
}

impl PreparedTabletPlan {
    pub const fn new(tablet_id: u64, token: PrepareToken) -> Self {
        Self { tablet_id, token }
    }
}

/// Commit payload after prepare and before durable sequencing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCommitPlan {
    pub txn_id: u64,
    pub start_time: u64,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
    pub tablets: Vec<PreparedTabletPlan>,
}

impl PreparedCommitPlan {
    pub fn into_record(self, commit_id: u64) -> CommitRecord {
        CommitRecord {
            txn_id: self.txn_id,
            start_time: self.start_time,
            commit_id,
            catalog_ops: self.catalog_ops,
            storage_ops: rewrite_rowset_versions(self.storage_ops, commit_id)
                .expect("commit_id fits supported rowset version range"),
            apply_descriptors: self.apply_descriptors,
            deferred_tasks: self.deferred_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.catalog_ops.is_empty()
            && self.storage_ops.is_empty()
            && self.apply_descriptors.is_empty()
            && self.deferred_tasks.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMaintenancePlan {
    pub kind: MaintenanceKind,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
    pub tablets: Vec<PreparedTabletPlan>,
}

impl PreparedMaintenancePlan {
    pub fn into_record(self, maintenance_id: u64) -> MaintenanceRecord {
        MaintenanceRecord {
            maintenance_id,
            kind: self.kind,
            catalog_ops: self.catalog_ops,
            storage_ops: self.storage_ops,
            apply_descriptors: self.apply_descriptors,
            deferred_tasks: self.deferred_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.catalog_ops.is_empty()
            && self.storage_ops.is_empty()
            && self.apply_descriptors.is_empty()
            && self.deferred_tasks.is_empty()
    }
}

fn rewrite_rowset_versions(
    storage_ops: Vec<StorageCommitOp>,
    commit_id: u64,
) -> Result<Vec<StorageCommitOp>> {
    let commit_version = i64::try_from(commit_id)
        .map_err(|_| paro_error::invalid_input("commit_id exceeds supported version range"))?;

    Ok(storage_ops
        .into_iter()
        .map(|op| match op {
            StorageCommitOp::Tablet(mut tablet) => {
                for mutation in &mut tablet.mutations {
                    if let TabletMutation::PublishRowset { version_span, .. } = mutation {
                        *version_span = VersionSpan {
                            start: commit_version,
                            end: commit_version,
                        };
                    }
                }
                StorageCommitOp::Tablet(tablet)
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::{
        ArtifactNamespace, ArtifactRef, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline,
        DeletePatchRef, DeletePatchSegment, TabletApplyOp,
    };

    #[test]
    fn prepared_commit_plan_rewrites_rowset_versions_to_commit_id() {
        let record = PreparedCommitPlan {
            txn_id: 7,
            start_time: 11,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: 99,
                mutations: vec![
                    TabletMutation::PublishRowset {
                        rowset_id: 123,
                        version_span: VersionSpan { start: 1, end: 9 },
                        rowset_ref: ArtifactRef {
                            namespace: ArtifactNamespace::CanonicalRowset,
                            locator: vec!["rowset_123".to_string()],
                        },
                    },
                    TabletMutation::ApplyDeletePatch {
                        patch: DeletePatchRef::Inline(DeletePatchInline {
                            encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                            row_count: 1,
                            groups: vec![DeletePatchGroup {
                                rowset_id: 123,
                                segments: vec![DeletePatchSegment {
                                    segment_id: 0,
                                    row_offsets_delta: vec![7],
                                }],
                            }],
                        }),
                        deleted_row_count: 1,
                    },
                ],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
        .into_record(42);

        let StorageCommitOp::Tablet(tablet) = &record.storage_ops[0];
        match &tablet.mutations[0] {
            TabletMutation::PublishRowset { version_span, .. } => {
                assert_eq!(*version_span, VersionSpan { start: 42, end: 42 });
            }
            other => panic!("expected PublishRowset mutation, got {other:?}"),
        }
        match &tablet.mutations[1] {
            TabletMutation::ApplyDeletePatch {
                deleted_row_count, ..
            } => {
                assert_eq!(*deleted_row_count, 1);
            }
            other => panic!("expected ApplyDeletePatch mutation, got {other:?}"),
        }
    }
}
