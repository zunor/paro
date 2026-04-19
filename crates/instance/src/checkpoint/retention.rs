// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::artifact_gc::{ArtifactGc, ArtifactGcReport};
use super::checkpoint_gc::{CheckpointGc, CheckpointGcReport};
use super::manifest_store::ManifestStore;
use super::segments::prune::{SegmentPruneReport, SegmentPruner};
use crate::config::CheckpointConfigOptions;
use crate::storage_manager::StorageManager;
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::checkpoint::{CheckpointManifest, RetentionFloor};
use paro_storage::meta::TabletMetaManager;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct RetentionCoordinator;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionAdvanceReport {
    pub checkpoint_gc: CheckpointGcReport,
    pub segment_prune: SegmentPruneReport,
    pub artifact_gc: ArtifactGcReport,
}

impl RetentionCoordinator {
    pub fn retention_floor(checkpoint_lsn: u64) -> RetentionFloor {
        RetentionFloor {
            checkpoint_lsn,
            manual_keep_from_lsn: None,
            backup_floor_lsn: None,
            replication_floor_lsn: None,
            pitr_floor_lsn: None,
        }
    }

    pub fn advance_retention(
        manifest_store: &ManifestStore,
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        committed_manifest: &CheckpointManifest,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<RetentionAdvanceReport> {
        Ok(RetentionAdvanceReport {
            checkpoint_gc: CheckpointGc::retain_committed(
                manifest_store,
                committed_manifest,
                checkpoint.checkpoint_gc_delete_budget,
            )?,
            segment_prune: SegmentPruner::prune_for_manifest(
                Path::new(&storage.get_wal_path()),
                committed_manifest,
                checkpoint.segment_prune_delete_budget,
            )?,
            artifact_gc: ArtifactGc::sweep_retention_orphans(
                catalog,
                storage,
                storage.get_tablet_meta_manager(),
                checkpoint.artifact_gc_batch_size,
                checkpoint.artifact_gc_delete_budget,
            )?,
        })
    }

    pub fn sweep_startup_orphans(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<ArtifactGcReport> {
        ArtifactGc::sweep_startup_orphans(
            catalog,
            storage,
            tablet_meta_manager,
            checkpoint.artifact_gc_batch_size,
            checkpoint.artifact_gc_delete_budget,
        )
    }
}
