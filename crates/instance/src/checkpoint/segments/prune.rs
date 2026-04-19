// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prune planning for checkpoint-managed journal segments.

use paro_common::checkpoint::CheckpointManifest;
use paro_journal::segments::SegmentCatalogStore;
use std::fs;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Default)]
pub struct SegmentPruner;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPruneReport {
    pub deleted_segments: usize,
}

impl SegmentPruner {
    pub fn prune_for_manifest(
        wal_path: &Path,
        manifest: &CheckpointManifest,
        delete_budget: usize,
    ) -> anyhow::Result<SegmentPruneReport> {
        if delete_budget == 0 {
            return Ok(SegmentPruneReport::default());
        }

        let retain_from_lsn = manifest.retention_floor.effective_replay_from_lsn();
        let store = SegmentCatalogStore::from_seed_path(wal_path);
        let Some(mut catalog) = store.load().map_err(anyhow::Error::from)? else {
            return Ok(SegmentPruneReport::default());
        };

        let active_segment_id = catalog.active_segment_id;
        let mut candidates = Vec::new();
        for segment in &catalog.segments {
            let can_prune = segment.segment_id != active_segment_id
                && segment
                    .sealed_end_lsn
                    .is_some_and(|sealed_end_lsn| sealed_end_lsn < retain_from_lsn);
            if can_prune {
                candidates.push(segment.segment_id);
            }
        }
        candidates.sort_unstable();
        let deleted_segment_ids: Vec<u64> = candidates.into_iter().take(delete_budget).collect();
        if deleted_segment_ids.is_empty() {
            return Ok(SegmentPruneReport::default());
        }

        catalog
            .segments
            .retain(|segment| !deleted_segment_ids.contains(&segment.segment_id));

        store.save(&catalog).map_err(anyhow::Error::from)?;
        for segment_id in &deleted_segment_ids {
            let segment_path = store.layout().segment_path(*segment_id);
            if segment_path.exists() {
                fs::remove_file(&segment_path)?;
            }
        }
        sync_dir(store.layout().segments_dir())?;

        Ok(SegmentPruneReport {
            deleted_segments: deleted_segment_ids.len(),
        })
    }
}

fn sync_dir(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SegmentPruner;
    use paro_common::checkpoint::{
        CheckpointDatabaseIdentity, CheckpointFrontier, CheckpointManifest, JournalTailRef,
        RecoverySummary, RetentionFloor,
    };
    use paro_journal::segments::{SegmentCatalog, SegmentCatalogEntry, SegmentCatalogStore};
    use tempfile::tempdir;

    fn manifest(checkpoint_lsn: u64) -> CheckpointManifest {
        CheckpointManifest {
            format_version: 1,
            checkpoint_id: 2,
            previous_checkpoint_id: Some(1),
            created_at_micros: 0,
            database_identity: CheckpointDatabaseIdentity {
                format_version: 1,
                database_id: 7,
                db_identifier: vec![1, 2, 3, 4],
                created_at_ms: 99,
            },
            frontier: CheckpointFrontier {
                checkpoint_lsn,
                checkpoint_commit_id: checkpoint_lsn,
                checkpoint_maintenance_id: 0,
            },
            bootstrap: RecoverySummary {
                max_lsn: checkpoint_lsn,
                max_commit_id: checkpoint_lsn,
                max_maintenance_id: 0,
                max_catalog_commit_id: checkpoint_lsn,
                max_seen_object_id: checkpoint_lsn,
            },
            journal: JournalTailRef {
                replay_from_segment_id: 3,
                replay_from_lsn: checkpoint_lsn.saturating_add(1),
            },
            bundle_refs: Vec::new(),
            retention_floor: RetentionFloor {
                checkpoint_lsn,
                manual_keep_from_lsn: None,
                backup_floor_lsn: None,
                replication_floor_lsn: None,
                pitr_floor_lsn: None,
            },
        }
    }

    #[test]
    fn pruner_removes_sealed_segments_before_retention_floor() {
        let dir = tempdir().expect("tempdir");
        let wal_path = dir.path().join("db.wal");
        let store = SegmentCatalogStore::from_seed_path(&wal_path);
        store.layout().ensure_dirs().expect("ensure dirs");
        let catalog = SegmentCatalog {
            format_version: 1,
            active_segment_id: 3,
            next_segment_id: 4,
            segments: vec![
                SegmentCatalogEntry {
                    segment_id: 1,
                    locator: "00000000000000000001.wal".to_string(),
                    start_lsn: 1,
                    sealed_end_lsn: Some(10),
                },
                SegmentCatalogEntry {
                    segment_id: 2,
                    locator: "00000000000000000002.wal".to_string(),
                    start_lsn: 11,
                    sealed_end_lsn: Some(20),
                },
                SegmentCatalogEntry {
                    segment_id: 3,
                    locator: "00000000000000000003.wal".to_string(),
                    start_lsn: 21,
                    sealed_end_lsn: None,
                },
            ],
        };
        store.save(&catalog).expect("save catalog");
        for segment_id in 1..=3 {
            std::fs::write(store.layout().segment_path(segment_id), b"wal").expect("write segment");
        }

        let report =
            SegmentPruner::prune_for_manifest(&wal_path, &manifest(20), usize::MAX).expect("prune");
        assert_eq!(report.deleted_segments, 2);
        assert!(!store.layout().segment_path(1).exists());
        assert!(!store.layout().segment_path(2).exists());
        assert!(store.layout().segment_path(3).exists());
        assert_eq!(
            store.load().expect("load").expect("catalog").segments.len(),
            1
        );
    }

    #[test]
    fn pruner_respects_delete_budget() {
        let dir = tempdir().expect("tempdir");
        let wal_path = dir.path().join("db.wal");
        let store = SegmentCatalogStore::from_seed_path(&wal_path);
        store.layout().ensure_dirs().expect("ensure dirs");
        let catalog = SegmentCatalog {
            format_version: 1,
            active_segment_id: 4,
            next_segment_id: 5,
            segments: vec![
                SegmentCatalogEntry {
                    segment_id: 1,
                    locator: "00000000000000000001.wal".to_string(),
                    start_lsn: 1,
                    sealed_end_lsn: Some(10),
                },
                SegmentCatalogEntry {
                    segment_id: 2,
                    locator: "00000000000000000002.wal".to_string(),
                    start_lsn: 11,
                    sealed_end_lsn: Some(20),
                },
                SegmentCatalogEntry {
                    segment_id: 3,
                    locator: "00000000000000000003.wal".to_string(),
                    start_lsn: 21,
                    sealed_end_lsn: Some(30),
                },
                SegmentCatalogEntry {
                    segment_id: 4,
                    locator: "00000000000000000004.wal".to_string(),
                    start_lsn: 31,
                    sealed_end_lsn: None,
                },
            ],
        };
        store.save(&catalog).expect("save catalog");
        for segment_id in 1..=4 {
            std::fs::write(store.layout().segment_path(segment_id), b"wal").expect("write segment");
        }

        let report = SegmentPruner::prune_for_manifest(&wal_path, &manifest(30), 2).expect("prune");
        assert_eq!(report.deleted_segments, 2);
        assert!(!store.layout().segment_path(1).exists());
        assert!(!store.layout().segment_path(2).exists());
        assert!(store.layout().segment_path(3).exists());
        assert!(store.layout().segment_path(4).exists());
    }
}
