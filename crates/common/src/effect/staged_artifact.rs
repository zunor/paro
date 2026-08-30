// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::data_op::ArtifactRef;
use crate::ddl::DdlObjectKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StagingArtifactId {
    pub txn_id: u64,
    pub path_components: Vec<String>,
}

impl StagingArtifactId {
    pub fn new(txn_id: u64, path_components: Vec<String>) -> Self {
        Self {
            txn_id,
            path_components,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkLoadUniqueSummary {
    pub key_count: u64,
    pub duplicate_key_count: u64,
    pub checksum_crc32c: u32,
    pub min_key_hash: Option<u64>,
    pub max_key_hash: Option<u64>,
}

impl BulkLoadUniqueSummary {
    #[inline]
    pub const fn empty() -> Self {
        Self {
            key_count: 0,
            duplicate_key_count: 0,
            checksum_crc32c: 0,
            min_key_hash: None,
            max_key_hash: None,
        }
    }

    #[inline]
    pub fn requires_conflict_range(self) -> bool {
        self.key_count > 0 && self.min_key_hash.is_some() && self.max_key_hash.is_some()
    }

    #[inline]
    pub fn key_hash_range(self) -> Option<(u64, u64)> {
        match (self.min_key_hash, self.max_key_hash) {
            (Some(start), Some(end)) if start <= end => Some((start, end)),
            _ => None,
        }
    }

    #[inline]
    pub fn is_conflict_free(self) -> bool {
        self.duplicate_key_count == 0 && (self.key_count == 0 || self.key_hash_range().is_some())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkLoadRowsetArtifact {
    pub table_object: DdlObjectKey,
    pub table_id: u64,
    pub tablet_id: u64,
    pub rowset_id: u64,
    pub staging: StagingArtifactId,
    pub rowset_ref: ArtifactRef,
    pub row_count: u64,
    pub byte_size: u64,
    pub schema_epoch: Option<u64>,
    pub physical_schema_token: Option<u64>,
    pub unique_summary: BulkLoadUniqueSummary,
}

/// Durable identity for a search generation built before transaction commit.
///
/// The descriptor records ownership and cleanup metadata for CDC, diagnostics,
/// transaction abort, and startup orphan sweeping. Installation is deliberately
/// *not* performed by the descriptor handler: the corresponding
/// `PublishSearchGeneration` tablet mutation owns the atomic directory + head
/// transition in both live publication and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchGenerationBuildArtifact {
    pub table_object: DdlObjectKey,
    pub table_id: u64,
    pub tablet_id: u64,
    pub definition_id: u64,
    pub generation_id: u64,
    pub build_snapshot_version: i64,
    pub config_fingerprint: u64,
    /// Private generation directory removed on abort or startup orphan sweep.
    pub staged_ref: ArtifactRef,
    /// Immutable generation-qualified destination used by the tablet mutation.
    pub generation_ref: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StagedArtifactDescriptor {
    PropertyGraphBuild {
        object: DdlObjectKey,
        staging: StagingArtifactId,
        schema_fingerprint: String,
    },
    BulkLoadRowset(BulkLoadRowsetArtifact),
    SearchGenerationBuild(SearchGenerationBuildArtifact),
}
