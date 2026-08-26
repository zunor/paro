// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable journal record schema shared across runtime append and recovery.

use crate::ddl::{
    DdlChange, DdlChangeRecord, DdlDependencyRef, DdlObjectKey, DdlStorageDescriptor,
};
use crate::effect::{
    ApplyDescriptor, CatalogTxnOp, DeferredTask, SearchGenerationHeadMeta,
    StagedArtifactDescriptor, StorageCommitOp, TabletMutation,
};
use serde::{Deserialize, Serialize};

/// Journal frame schema version used by the binary codec.
/// Version 6 makes search-generation publication mode explicit and identities
/// every immutable root revision independently.
pub const JOURNAL_FORMAT_VERSION: u16 = 7;
pub const COMMIT_RECORD_VERSION: u16 = 2;
pub const MAINTENANCE_RECORD_VERSION: u16 = 2;
pub const JOURNAL_RECORD_METADATA_VERSION: u16 = 1;
pub const JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION: u16 = 1;

/// One durable journal record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalRecord {
    Commit(CommitRecord),
    Maintenance(MaintenanceRecord),
    CheckpointFence(CheckpointFence),
}

/// Durable committed record variants consumed by the apply runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedRecord {
    Transaction(CommitRecord),
    Maintenance(MaintenanceRecord),
}

impl From<CommittedRecord> for JournalRecord {
    fn from(record: CommittedRecord) -> Self {
        match record {
            CommittedRecord::Transaction(record) => Self::Commit(record),
            CommittedRecord::Maintenance(record) => Self::Maintenance(record),
        }
    }
}

/// Durable transaction commit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub record_version: u16,
    pub metadata: JournalRecordMetadata,
    pub txn_id: u64,
    pub start_time: u64,
    pub commit_id: u64,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
}

impl CommitRecord {
    pub fn expected_metadata(&self) -> JournalRecordMetadata {
        JournalRecordMetadata::transaction(
            &self.catalog_ops,
            &self.storage_ops,
            &self.apply_descriptors,
            &self.deferred_tasks,
        )
    }

    pub fn metadata_matches_payload(&self) -> bool {
        self.metadata == self.expected_metadata()
    }
}

/// Durable maintenance record.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceRecord {
    pub record_version: u16,
    pub metadata: JournalRecordMetadata,
    pub maintenance_id: u64,
    pub kind: MaintenanceKind,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
}

impl MaintenanceRecord {
    pub fn expected_metadata(&self) -> JournalRecordMetadata {
        JournalRecordMetadata::maintenance(
            &self.catalog_ops,
            &self.storage_ops,
            &self.apply_descriptors,
            &self.deferred_tasks,
        )
    }

    pub fn metadata_matches_payload(&self) -> bool {
        self.metadata == self.expected_metadata()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceKind {
    Compaction,
    IndexBackfill,
    SearchGenerationMaintenance,
    MaterializedViewRefresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalRecordKind {
    Transaction,
    Maintenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecordMetadata {
    pub metadata_version: u16,
    pub participant_descriptor_version: u16,
    pub record_kind: JournalRecordKind,
    pub cdc: CdcMetadata,
    pub change: JournalChangeDescriptor,
    pub replay: JournalReplayDescriptor,
}

impl JournalRecordMetadata {
    pub fn transaction(
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
        apply_descriptors: &[ApplyDescriptor],
        deferred_tasks: &[DeferredTask],
    ) -> Self {
        Self::from_parts(
            JournalRecordKind::Transaction,
            catalog_ops,
            storage_ops,
            apply_descriptors,
            deferred_tasks,
        )
    }

    pub fn maintenance(
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
        apply_descriptors: &[ApplyDescriptor],
        deferred_tasks: &[DeferredTask],
    ) -> Self {
        Self::from_parts(
            JournalRecordKind::Maintenance,
            catalog_ops,
            storage_ops,
            apply_descriptors,
            deferred_tasks,
        )
    }

    fn from_parts(
        record_kind: JournalRecordKind,
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
        apply_descriptors: &[ApplyDescriptor],
        deferred_tasks: &[DeferredTask],
    ) -> Self {
        let change =
            JournalChangeDescriptor::from_parts(catalog_ops, storage_ops, apply_descriptors);
        let replay = JournalReplayDescriptor::from_parts(
            catalog_ops,
            storage_ops,
            apply_descriptors,
            deferred_tasks,
        );
        let cdc = CdcMetadata::from_parts(record_kind, catalog_ops, storage_ops);
        Self {
            metadata_version: JOURNAL_RECORD_METADATA_VERSION,
            participant_descriptor_version: JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION,
            record_kind,
            cdc,
            change,
            replay,
        }
    }
}

impl Default for JournalRecordMetadata {
    fn default() -> Self {
        Self {
            metadata_version: JOURNAL_RECORD_METADATA_VERSION,
            participant_descriptor_version: JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION,
            record_kind: JournalRecordKind::Transaction,
            cdc: CdcMetadata::default(),
            change: JournalChangeDescriptor::default(),
            replay: JournalReplayDescriptor::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcMetadata {
    pub catalog_change_count: u32,
    pub storage_mutation_count: u32,
    pub user_data_change: bool,
    pub maintenance_change: bool,
}

impl CdcMetadata {
    fn from_parts(
        record_kind: JournalRecordKind,
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
    ) -> Self {
        let storage_mutation_count = storage_mutation_count(storage_ops);
        Self {
            catalog_change_count: usize_to_u32_saturating(catalog_ops.len()),
            storage_mutation_count,
            user_data_change: matches!(record_kind, JournalRecordKind::Transaction)
                && (!catalog_ops.is_empty() || storage_mutation_count > 0),
            maintenance_change: matches!(record_kind, JournalRecordKind::Maintenance),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalChangeDescriptor {
    pub catalog_objects: Vec<DdlObjectKey>,
    pub object_ids: Vec<u64>,
    pub tablet_ids: Vec<u64>,
    pub artifacts: Vec<JournalArtifactDescriptor>,
    pub schema_dependencies: Vec<JournalSchemaDependency>,
}

impl JournalChangeDescriptor {
    fn from_parts(
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
        apply_descriptors: &[ApplyDescriptor],
    ) -> Self {
        let mut descriptor = Self::default();
        for op in catalog_ops {
            descriptor.record_catalog_change(&op.change);
        }
        for op in storage_ops {
            descriptor.record_storage_op(op);
        }
        for apply_descriptor in apply_descriptors {
            descriptor.record_apply_descriptor(apply_descriptor);
        }
        descriptor.object_ids.sort_unstable();
        descriptor.object_ids.dedup();
        descriptor.tablet_ids.sort_unstable();
        descriptor.tablet_ids.dedup();
        descriptor
    }

    fn record_catalog_change(&mut self, record: &DdlChangeRecord) {
        push_unique(&mut self.catalog_objects, record.key.clone());
        if let Some(object_id) = ddl_change_object_id(&record.change) {
            push_nonzero_u64(&mut self.object_ids, object_id);
        }
        match &record.change {
            DdlChange::CreateTable(payload) => {
                if let Some(storage) = &payload.storage {
                    self.record_storage_schema_dependency(storage);
                    push_nonzero_u64(&mut self.object_ids, storage.table_id);
                    push_nonzero_u64(&mut self.tablet_ids, storage.tablet_id);
                }
            }
            DdlChange::CreateView(payload) => {
                self.record_dependency_refs(&payload.dependencies);
            }
            DdlChange::CreatePropertyGraph(payload) => {
                for vertex in &payload.vertex_tables {
                    push_nonzero_u64(&mut self.object_ids, vertex.table_oid);
                }
                for edge in &payload.edge_tables {
                    push_nonzero_u64(&mut self.object_ids, edge.table_oid);
                }
            }
            _ => {}
        }
    }

    fn record_storage_schema_dependency(&mut self, storage: &DdlStorageDescriptor) {
        push_unique(
            &mut self.schema_dependencies,
            JournalSchemaDependency {
                schema_id: Some(storage.schema_id),
                schema_name: None,
                schema_version: Some(storage.schema_version),
                schema_hash: Some(storage.schema_hash),
            },
        );
    }

    fn record_dependency_refs(&mut self, dependencies: &[DdlDependencyRef]) {
        for dependency in dependencies {
            push_nonzero_u64(&mut self.object_ids, dependency.object.object_id);
            push_unique(
                &mut self.schema_dependencies,
                JournalSchemaDependency {
                    schema_id: dependency.object.schema_id,
                    schema_name: dependency.object.schema_name.clone(),
                    schema_version: None,
                    schema_hash: None,
                },
            );
        }
    }

    fn record_storage_op(&mut self, op: &StorageCommitOp) {
        match op {
            StorageCommitOp::Tablet(tablet) => {
                push_nonzero_u64(&mut self.tablet_ids, tablet.tablet_id);
                for mutation in &tablet.mutations {
                    self.artifacts.push(JournalArtifactDescriptor {
                        tablet_id: Some(tablet.tablet_id),
                        artifact_id: mutation.stable_artifact_id(),
                        kind: JournalArtifactKind::from_mutation(mutation),
                        descriptor_checksum_crc32c: checksum_serialized(mutation),
                    });
                }
            }
        }
    }

    fn record_apply_descriptor(&mut self, descriptor: &ApplyDescriptor) {
        match descriptor {
            ApplyDescriptor::PublishStagedArtifact(
                StagedArtifactDescriptor::PropertyGraphBuild {
                    object, staging, ..
                },
            ) => {
                push_unique(&mut self.catalog_objects, object.clone());
                self.artifacts.push(JournalArtifactDescriptor {
                    tablet_id: None,
                    artifact_id: checksum_serialized(staging) as u64,
                    kind: JournalArtifactKind::ApplyDescriptor,
                    descriptor_checksum_crc32c: checksum_serialized(descriptor),
                });
            }
            ApplyDescriptor::PublishStagedArtifact(StagedArtifactDescriptor::BulkLoadRowset(
                artifact,
            )) => {
                push_unique(&mut self.catalog_objects, artifact.table_object.clone());
                push_nonzero_u64(&mut self.object_ids, artifact.table_id);
                push_nonzero_u64(&mut self.tablet_ids, artifact.tablet_id);
                self.artifacts.push(JournalArtifactDescriptor {
                    tablet_id: Some(artifact.tablet_id),
                    artifact_id: artifact.rowset_id,
                    kind: JournalArtifactKind::BulkLoadRowset,
                    descriptor_checksum_crc32c: checksum_serialized(descriptor),
                });
            }
            ApplyDescriptor::PublishStagedArtifact(
                StagedArtifactDescriptor::SearchGenerationBuild(artifact),
            ) => {
                push_unique(&mut self.catalog_objects, artifact.table_object.clone());
                push_nonzero_u64(&mut self.object_ids, artifact.table_id);
                push_nonzero_u64(&mut self.tablet_ids, artifact.tablet_id);
                self.artifacts.push(JournalArtifactDescriptor {
                    tablet_id: Some(artifact.tablet_id),
                    artifact_id: SearchGenerationHeadMeta::stable_artifact_id(
                        artifact.definition_id,
                        artifact.generation_id,
                    ),
                    kind: JournalArtifactKind::ApplyDescriptor,
                    descriptor_checksum_crc32c: checksum_serialized(descriptor),
                });
            }
            ApplyDescriptor::RuntimeTransition(_) | ApplyDescriptor::Cleanup(_) => {
                self.artifacts.push(JournalArtifactDescriptor {
                    tablet_id: None,
                    artifact_id: checksum_serialized(descriptor) as u64,
                    kind: JournalArtifactKind::ApplyDescriptor,
                    descriptor_checksum_crc32c: checksum_serialized(descriptor),
                });
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalArtifactKind {
    Rowset,
    PrimaryDelete,
    DeletePatch,
    CompactionOutput,
    SearchGeneration,
    SearchGenerationRetirement,
    ApplyDescriptor,
    BulkLoadRowset,
}

impl JournalArtifactKind {
    fn from_mutation(mutation: &TabletMutation) -> Self {
        match mutation {
            TabletMutation::PublishRowset { .. } => Self::Rowset,
            TabletMutation::ApplyPrimaryDelete { .. } => Self::PrimaryDelete,
            TabletMutation::ApplyDeletePatch { .. } => Self::DeletePatch,
            TabletMutation::PublishCompaction { .. } => Self::CompactionOutput,
            TabletMutation::PublishSearchGeneration { .. } => Self::SearchGeneration,
            TabletMutation::RetireSearchGeneration { .. } => Self::SearchGenerationRetirement,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalArtifactDescriptor {
    pub tablet_id: Option<u64>,
    pub artifact_id: u64,
    pub kind: JournalArtifactKind,
    pub descriptor_checksum_crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JournalSchemaDependency {
    pub schema_id: Option<u64>,
    pub schema_name: Option<String>,
    pub schema_version: Option<u32>,
    pub schema_hash: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalReplayDescriptor {
    pub catalog_op_count: u32,
    pub storage_op_count: u32,
    pub storage_mutation_count: u32,
    pub apply_descriptor_count: u32,
    pub deferred_task_count: u32,
    pub catalog_ops_checksum_crc32c: u32,
    pub storage_ops_checksum_crc32c: u32,
    pub apply_descriptors_checksum_crc32c: u32,
    pub deferred_tasks_checksum_crc32c: u32,
}

impl JournalReplayDescriptor {
    fn from_parts(
        catalog_ops: &[CatalogTxnOp],
        storage_ops: &[StorageCommitOp],
        apply_descriptors: &[ApplyDescriptor],
        deferred_tasks: &[DeferredTask],
    ) -> Self {
        Self {
            catalog_op_count: usize_to_u32_saturating(catalog_ops.len()),
            storage_op_count: usize_to_u32_saturating(storage_ops.len()),
            storage_mutation_count: storage_mutation_count(storage_ops),
            apply_descriptor_count: usize_to_u32_saturating(apply_descriptors.len()),
            deferred_task_count: usize_to_u32_saturating(deferred_tasks.len()),
            catalog_ops_checksum_crc32c: checksum_serialized(catalog_ops),
            storage_ops_checksum_crc32c: checksum_serialized(storage_ops),
            apply_descriptors_checksum_crc32c: checksum_serialized(apply_descriptors),
            deferred_tasks_checksum_crc32c: checksum_serialized(deferred_tasks),
        }
    }
}

/// Checkpoint fence carried by the journal stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointFence {
    pub checkpoint_marker: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoverySummary {
    pub max_lsn: u64,
    pub max_commit_id: u64,
    pub max_maintenance_id: u64,
    pub max_catalog_commit_id: u64,
    pub max_seen_object_id: u64,
}

fn storage_mutation_count(storage_ops: &[StorageCommitOp]) -> u32 {
    let count = storage_ops
        .iter()
        .map(|op| match op {
            StorageCommitOp::Tablet(tablet) => tablet.mutations.len(),
        })
        .sum::<usize>();
    usize_to_u32_saturating(count)
}

fn ddl_change_object_id(change: &DdlChange) -> Option<u64> {
    match change {
        DdlChange::CreateSchema(payload) => Some(payload.object_id),
        DdlChange::CreateTable(payload) => Some(payload.object_id),
        DdlChange::CreateView(payload) => Some(payload.object_id),
        DdlChange::CreateIndex(payload) => Some(payload.object_id),
        DdlChange::CreatePropertyGraph(payload) => Some(payload.object_id),
        DdlChange::CreateSequence(payload) => Some(payload.object_id),
        DdlChange::CreateRoutine(payload) => Some(payload.object_id.max(payload.routine_id)),
        DdlChange::DropSchema(_)
        | DdlChange::DropTable(_)
        | DdlChange::DropView(_)
        | DdlChange::DropIndex(_)
        | DdlChange::DropPropertyGraph(_)
        | DdlChange::DropSequence(_)
        | DdlChange::DropRoutine(_)
        | DdlChange::AlterEntry(_) => None,
    }
    .filter(|object_id| *object_id != 0)
}

fn push_nonzero_u64(values: &mut Vec<u64>, value: u64) {
    if value != 0 && !values.contains(&value) {
        values.push(value);
    }
}

fn push_unique<T>(values: &mut Vec<T>, value: T)
where
    T: PartialEq,
{
    if !values.contains(&value) {
        values.push(value);
    }
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn checksum_serialized<T>(value: &T) -> u32
where
    T: Serialize + ?Sized,
{
    bincode::serialize(value)
        .map(|bytes| crc32c::crc32c(&bytes))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::{CreateSchemaPayload, DdlObjectKind};
    use crate::effect::{
        ArtifactNamespace, ArtifactRef, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline,
        DeletePatchRef, DeletePatchSegment, StagingArtifactId, TabletApplyOp, VersionSpan,
    };

    #[test]
    fn transaction_metadata_describes_catalog_storage_and_artifacts() {
        let catalog_ops = vec![CatalogTxnOp {
            change: DdlChangeRecord {
                key: DdlObjectKey::new("postgres", None::<String>, "s1", DdlObjectKind::Schema),
                change: DdlChange::CreateSchema(CreateSchemaPayload {
                    object_id: 77,
                    if_not_exists: false,
                }),
            },
        }];
        let storage_ops = vec![StorageCommitOp::Tablet(TabletApplyOp {
            tablet_id: 41,
            mutations: vec![
                TabletMutation::PublishRowset {
                    rowset_id: 99,
                    version_span: VersionSpan { start: 13, end: 13 },
                    rowset_ref: ArtifactRef {
                        namespace: ArtifactNamespace::CanonicalRowset,
                        locator: vec!["rowset_99".to_string()],
                    },
                },
                TabletMutation::ApplyDeletePatch {
                    patch: DeletePatchRef::Inline(DeletePatchInline {
                        encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                        row_count: 1,
                        groups: vec![DeletePatchGroup {
                            rowset_id: 99,
                            segments: vec![DeletePatchSegment {
                                segment_id: 0,
                                row_offsets_delta: vec![7],
                            }],
                        }],
                    }),
                    deleted_row_count: 1,
                },
            ],
        })];
        let apply_descriptors = vec![ApplyDescriptor::PublishStagedArtifact(
            StagedArtifactDescriptor::PropertyGraphBuild {
                object: DdlObjectKey::new(
                    "postgres",
                    Some("public"),
                    "g",
                    DdlObjectKind::PropertyGraph,
                ),
                staging: StagingArtifactId::new(7, vec!["graph".to_string(), "g".to_string()]),
                schema_fingerprint: "fp".to_string(),
            },
        )];

        let metadata =
            JournalRecordMetadata::transaction(&catalog_ops, &storage_ops, &apply_descriptors, &[]);

        assert_eq!(metadata.metadata_version, JOURNAL_RECORD_METADATA_VERSION);
        assert_eq!(
            metadata.participant_descriptor_version,
            JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION
        );
        assert_eq!(metadata.record_kind, JournalRecordKind::Transaction);
        assert_eq!(metadata.cdc.catalog_change_count, 1);
        assert_eq!(metadata.cdc.storage_mutation_count, 2);
        assert!(metadata.cdc.user_data_change);
        assert!(!metadata.cdc.maintenance_change);
        assert_eq!(metadata.change.object_ids, vec![77]);
        assert_eq!(metadata.change.tablet_ids, vec![41]);
        assert_eq!(metadata.change.artifacts.len(), 3);
        assert!(metadata
            .change
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == JournalArtifactKind::Rowset
                && artifact.artifact_id == 99
                && artifact.descriptor_checksum_crc32c != 0));
        assert_eq!(metadata.replay.catalog_op_count, 1);
        assert_eq!(metadata.replay.storage_op_count, 1);
        assert_eq!(metadata.replay.storage_mutation_count, 2);
        assert_eq!(metadata.replay.apply_descriptor_count, 1);
        assert_ne!(metadata.replay.storage_ops_checksum_crc32c, 0);
    }

    #[test]
    fn maintenance_metadata_marks_non_cdc_layout_change() {
        let metadata = JournalRecordMetadata::maintenance(&[], &[], &[], &[]);
        assert_eq!(metadata.record_kind, JournalRecordKind::Maintenance);
        assert!(!metadata.cdc.user_data_change);
        assert!(metadata.cdc.maintenance_change);
    }
}
