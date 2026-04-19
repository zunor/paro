// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Retention and garbage collection for committed checkpoint bundles.

use super::manifest_store::ManifestStore;
use paro_common::checkpoint::CheckpointManifest;

#[derive(Debug, Default)]
pub struct CheckpointGc;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointGcReport {
    pub deleted_checkpoints: usize,
}

impl CheckpointGc {
    pub fn retain_committed(
        manifest_store: &ManifestStore,
        committed_manifest: &CheckpointManifest,
        delete_budget: usize,
    ) -> anyhow::Result<CheckpointGcReport> {
        if delete_budget == 0 {
            return Ok(CheckpointGcReport::default());
        }

        if committed_manifest
            .retention_floor
            .keeps_history_before_checkpoint_tail()
        {
            return Ok(CheckpointGcReport::default());
        }

        let mut deleted_checkpoints = 0usize;
        for manifest in manifest_store.list_manifests()? {
            if deleted_checkpoints >= delete_budget {
                break;
            }
            if manifest.checkpoint_id == committed_manifest.checkpoint_id {
                continue;
            }
            let manifest_path = manifest_store.manifest_path(manifest.checkpoint_id);
            if manifest_path.exists() {
                std::fs::remove_file(&manifest_path)?;
            }
            let bundle_dir = manifest_store.bundle_dir(manifest.checkpoint_id);
            if bundle_dir.exists() {
                std::fs::remove_dir_all(&bundle_dir)?;
            }
            deleted_checkpoints += 1;
        }

        Ok(CheckpointGcReport {
            deleted_checkpoints,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CheckpointGc;
    use crate::checkpoint::manifest_store::ManifestStore;
    use paro_common::checkpoint::{
        BundleKind, CheckpointDatabaseIdentity, CheckpointFrontier, JournalTailRef,
        RecoverySummary, RetentionFloor, CATALOG_BUNDLE_FORMAT_VERSION,
    };
    use tempfile::tempdir;

    fn test_identity() -> CheckpointDatabaseIdentity {
        CheckpointDatabaseIdentity {
            format_version: 1,
            database_id: 7,
            db_identifier: vec![1, 2, 3, 4],
            created_at_ms: 99,
        }
    }

    fn publish_checkpoint(
        store: &ManifestStore,
        checkpoint_lsn: u64,
        manual_keep_from_lsn: Option<u64>,
    ) -> paro_common::checkpoint::CheckpointManifest {
        let mut staged = store.begin_publish(test_identity()).expect("begin publish");
        store
            .stage_raw_bundle(
                &mut staged,
                "catalog.bin",
                BundleKind::Catalog,
                CATALOG_BUNDLE_FORMAT_VERSION,
                b"catalog",
                None,
            )
            .expect("stage bundle");
        store
            .publish_manifest(
                staged,
                CheckpointFrontier {
                    checkpoint_lsn,
                    checkpoint_commit_id: checkpoint_lsn,
                    checkpoint_maintenance_id: 0,
                },
                RecoverySummary {
                    max_lsn: checkpoint_lsn,
                    max_commit_id: checkpoint_lsn,
                    max_maintenance_id: 0,
                    max_catalog_commit_id: checkpoint_lsn,
                    max_seen_object_id: checkpoint_lsn,
                },
                JournalTailRef {
                    replay_from_segment_id: 1,
                    replay_from_lsn: checkpoint_lsn.saturating_add(1),
                },
                RetentionFloor {
                    checkpoint_lsn,
                    manual_keep_from_lsn,
                    backup_floor_lsn: None,
                    replication_floor_lsn: None,
                    pitr_floor_lsn: None,
                },
            )
            .expect("publish manifest")
    }

    #[test]
    fn checkpoint_gc_keeps_only_latest_without_external_pins() {
        let dir = tempdir().expect("tempdir");
        let store = ManifestStore::open_database_root(dir.path()).expect("open store");
        publish_checkpoint(&store, 10, None);
        let latest = publish_checkpoint(&store, 20, None);

        let report = CheckpointGc::retain_committed(&store, &latest, usize::MAX).expect("gc");
        assert_eq!(report.deleted_checkpoints, 1);
        assert_eq!(store.list_manifests().expect("list manifests").len(), 1);
        assert_eq!(
            store
                .read_current_manifest()
                .expect("read current")
                .expect("manifest")
                .checkpoint_id,
            latest.checkpoint_id
        );
    }

    #[test]
    fn checkpoint_gc_preserves_history_when_retention_is_pinned() {
        let dir = tempdir().expect("tempdir");
        let store = ManifestStore::open_database_root(dir.path()).expect("open store");
        publish_checkpoint(&store, 10, None);
        let latest = publish_checkpoint(&store, 20, Some(5));

        let report = CheckpointGc::retain_committed(&store, &latest, usize::MAX).expect("gc");
        assert_eq!(report.deleted_checkpoints, 0);
        assert_eq!(store.list_manifests().expect("list manifests").len(), 2);
    }

    #[test]
    fn checkpoint_gc_respects_delete_budget() {
        let dir = tempdir().expect("tempdir");
        let store = ManifestStore::open_database_root(dir.path()).expect("open store");
        publish_checkpoint(&store, 10, None);
        publish_checkpoint(&store, 20, None);
        let latest = publish_checkpoint(&store, 30, None);

        let report = CheckpointGc::retain_committed(&store, &latest, 1).expect("gc");
        assert_eq!(report.deleted_checkpoints, 1);
        assert_eq!(store.list_manifests().expect("list manifests").len(), 2);
    }
}
