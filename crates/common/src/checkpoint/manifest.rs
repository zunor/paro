// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{RetentionFloor, SnapshotBundleRef};
use serde::{Deserialize, Serialize};

pub const CHECKPOINT_MANIFEST_FORMAT_VERSION: u32 = 1;
pub const CHECKPOINT_CURRENT_POINTER_FORMAT_VERSION: u32 = 1;

/// Stable identity copied into checkpoint manifests so recovery can validate
/// that a manifest belongs to the expected database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointDatabaseIdentity {
    pub format_version: u16,
    pub database_id: u64,
    pub db_identifier: Vec<u8>,
    pub created_at_ms: i64,
}

/// Published-prefix frontier absorbed by one checkpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointFrontier {
    pub checkpoint_lsn: u64,
    pub checkpoint_commit_id: u64,
    pub checkpoint_maintenance_id: u64,
}

/// Runtime watermarks needed to bootstrap recovery without replaying trimmed
/// history.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoverySummary {
    pub max_lsn: u64,
    pub max_commit_id: u64,
    pub max_maintenance_id: u64,
    pub max_catalog_commit_id: u64,
    pub max_seen_object_id: u64,
}

/// Tail replay start reference emitted into a committed checkpoint manifest.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalTailRef {
    pub replay_from_segment_id: u64,
    pub replay_from_lsn: u64,
}

/// Durable manifest published as the authoritative checkpoint pointer target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub format_version: u32,
    pub checkpoint_id: u64,
    pub previous_checkpoint_id: Option<u64>,
    pub created_at_micros: u64,
    pub database_identity: CheckpointDatabaseIdentity,
    pub frontier: CheckpointFrontier,
    pub bootstrap: RecoverySummary,
    pub journal: JournalTailRef,
    pub bundle_refs: Vec<SnapshotBundleRef>,
    pub retention_floor: RetentionFloor,
}

/// Atomically published CURRENT pointer referencing the latest committed
/// checkpoint manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointCurrentPointer {
    pub format_version: u32,
    pub checkpoint_id: u64,
    pub manifest_locator: String,
    pub manifest_checksum_crc32c: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{BundleKind, SnapshotBundleRef};

    #[test]
    fn checkpoint_manifest_json_roundtrip_preserves_frontier_and_bundles() {
        let manifest = CheckpointManifest {
            format_version: CHECKPOINT_MANIFEST_FORMAT_VERSION,
            checkpoint_id: 42,
            previous_checkpoint_id: Some(41),
            created_at_micros: 123_456,
            database_identity: CheckpointDatabaseIdentity {
                format_version: 1,
                database_id: 7,
                db_identifier: vec![1, 2, 3, 4],
                created_at_ms: 99,
            },
            frontier: CheckpointFrontier {
                checkpoint_lsn: 17,
                checkpoint_commit_id: 18,
                checkpoint_maintenance_id: 19,
            },
            bootstrap: RecoverySummary {
                max_lsn: 17,
                max_commit_id: 18,
                max_maintenance_id: 19,
                max_catalog_commit_id: 20,
                max_seen_object_id: 21,
            },
            journal: JournalTailRef {
                replay_from_segment_id: 3,
                replay_from_lsn: 18,
            },
            bundle_refs: vec![SnapshotBundleRef {
                kind: BundleKind::Catalog,
                locator: "catalog.bin".to_string(),
                size_bytes: 128,
                checksum_crc32c: 77,
                format_version: 1,
                base_checkpoint_id: None,
            }],
            retention_floor: RetentionFloor {
                checkpoint_lsn: 17,
                manual_keep_from_lsn: Some(16),
                backup_floor_lsn: None,
                replication_floor_lsn: None,
                pitr_floor_lsn: None,
            },
        };

        let json = serde_json::to_vec(&manifest).expect("serialize manifest");
        let restored: CheckpointManifest =
            serde_json::from_slice(&json).expect("deserialize manifest");
        assert_eq!(restored, manifest);
    }

    #[test]
    fn current_pointer_json_roundtrip_preserves_manifest_locator() {
        let pointer = CheckpointCurrentPointer {
            format_version: CHECKPOINT_CURRENT_POINTER_FORMAT_VERSION,
            checkpoint_id: 42,
            manifest_locator: "manifests/00000042.bin".to_string(),
            manifest_checksum_crc32c: 77,
        };

        let json = serde_json::to_vec(&pointer).expect("serialize current pointer");
        let restored: CheckpointCurrentPointer =
            serde_json::from_slice(&json).expect("deserialize current pointer");
        assert_eq!(restored, pointer);
    }
}
