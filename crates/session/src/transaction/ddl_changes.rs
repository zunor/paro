// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::collection::StagedCatalogMutation;
use paro_catalog::dependency::DependencyDelta;
use paro_catalog::entry::{CreateIndexInfo, IndexCatalogEntry, IndexCoverage, TableCatalogEntry};
use paro_common::ddl::{DdlChangeRecord, DdlObjectKey};
use paro_common::effect::{
    CleanupDescriptor, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
    StagedArtifactDescriptor,
};
use paro_context::DdlExecutionProfile;
use paro_storage::index::BoundIndex;
use paro_storage::table::table_handle::TableHandle;

use super::index_backfill::IndexBackfillPlan;

pub struct IndexPostCommitAction {
    pub entry: Arc<IndexCatalogEntry>,
    pub table: Arc<TableCatalogEntry>,
    pub info: CreateIndexInfo,
    pub built_index: Option<Arc<dyn BoundIndex>>,
    pub coverage: Option<IndexCoverage>,
    pub(crate) backfill: Option<IndexBackfillPlan>,
}

pub struct TableDropCleanupAction {
    pub storage: Arc<TableHandle>,
    pub move_to_trash: bool,
}

#[allow(clippy::large_enum_variant)]
pub enum TransientCatalogRuntime {
    CreateIndex(IndexPostCommitAction),
    DropTable(TableDropCleanupAction),
}

pub struct PreparedCatalogOp {
    pub record: DdlChangeRecord,
    pub profile: DdlExecutionProfile,
    pub catalog: Option<StagedCatalogMutation>,
    pub dependencies: Option<DependencyDelta>,
    pub dml_targets: Vec<DdlObjectKey>,
    pub staged_artifacts: Vec<StagedArtifactDescriptor>,
    pub runtime_transitions: Vec<RuntimeTransitionDescriptor>,
    pub cleanups: Vec<CleanupDescriptor>,
    pub post_commit_hooks: Vec<PostCommitHookDescriptor>,
    pub transient_runtime: Option<TransientCatalogRuntime>,
}

impl std::fmt::Debug for PreparedCatalogOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedCatalogOp")
            .field("record", &self.record)
            .field("profile", &self.profile)
            .field("has_catalog", &self.catalog.is_some())
            .field("has_dependencies", &self.dependencies.is_some())
            .field("dml_targets", &self.dml_targets)
            .field("staged_artifacts", &self.staged_artifacts)
            .field("runtime_transitions", &self.runtime_transitions)
            .field("cleanups", &self.cleanups)
            .field("post_commit_hooks", &self.post_commit_hooks)
            .field("has_transient_runtime", &self.transient_runtime.is_some())
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct CatalogOpBatch {
    changes: Vec<PreparedCatalogOp>,
}

impl CatalogOpBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, change: PreparedCatalogOp) {
        self.changes.push(change);
    }

    pub fn mark(&self) -> usize {
        self.changes.len()
    }

    pub fn rollback_to_mark(&mut self, mark: usize) -> Vec<PreparedCatalogOp> {
        if mark >= self.changes.len() {
            return Vec::new();
        }
        self.changes.split_off(mark)
    }

    pub fn take_all(&mut self) -> Vec<PreparedCatalogOp> {
        std::mem::take(&mut self.changes)
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }
}
