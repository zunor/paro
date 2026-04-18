// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::cleanup::staging;
use crate::compaction::plan::types::{CompactionJobId, CompactionPlan};
use crate::rowset::{RowsetId, RowsetMeta, RowsetSharedPtr};
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStrategy {
    AtomicRename,
    ManifestSwitch,
}

#[derive(Debug)]
pub struct CompactionWorkspace {
    pub job_id: CompactionJobId,
    pub workspace_dir: PathBuf,
    pub rowset_dir: PathBuf,
    pub rowset_id: RowsetId,
    pub cancel_token: CancellationToken,
    pub publish_strategy: PublishStrategy,
    pub created_at_ms: i64,
}

impl CompactionWorkspace {
    pub fn create(tablet: &Tablet, job_id: CompactionJobId, rowset_id: RowsetId) -> Result<Self> {
        Self::create_with_cancel_token(tablet, job_id, rowset_id, CancellationToken::new())
    }

    pub fn create_with_cancel_token(
        tablet: &Tablet,
        job_id: CompactionJobId,
        rowset_id: RowsetId,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let staging_root = tablet.compaction_staging_dir();
        let final_root = tablet.data_dir().join("rowsets");
        fs::create_dir_all(&staging_root).map_err(|err| {
            paro_error::io_error(format!(
                "create compaction staging root {}: {}",
                staging_root.display(),
                err
            ))
        })?;
        fs::create_dir_all(&final_root).map_err(|err| {
            paro_error::io_error(format!(
                "create canonical rowset root {}: {}",
                final_root.display(),
                err
            ))
        })?;

        ensure_same_filesystem(&staging_root, &final_root)?;

        let workspace_dir = staging_root.join(format!("job_{}", job_id.0));
        let rowset_dir = workspace_dir.join(format!("rowset_{}", rowset_id));
        fs::create_dir_all(&rowset_dir).map_err(|err| {
            paro_error::io_error(format!(
                "create compaction workspace {}: {}",
                rowset_dir.display(),
                err
            ))
        })?;

        Ok(Self {
            job_id,
            workspace_dir,
            rowset_dir,
            rowset_id,
            cancel_token,
            publish_strategy: PublishStrategy::AtomicRename,
            created_at_ms: now_millis(),
        })
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

impl Drop for CompactionWorkspace {
    fn drop(&mut self) {
        staging::enqueue_cleanup(self.workspace_dir.clone());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryIndexType {
    Hnsw,
    FullText,
    Sparse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryIndexBuildState {
    EmbeddedFilesReady,
    RuntimeInstallReady,
}

#[derive(Debug, Clone)]
pub struct SegmentArtifact {
    pub segment_id: u32,
    pub data_path: PathBuf,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SecondaryIndexArtifact {
    pub index_type: SecondaryIndexType,
    pub relative_paths: Vec<PathBuf>,
    pub build_state: SecondaryIndexBuildState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactionBuildStats {
    pub input_rows: u64,
    pub input_size: u64,
    pub output_rows: u64,
    pub output_size: u64,
}

#[derive(Debug)]
pub struct StagedArtifact {
    pub plan: Arc<CompactionPlan>,
    pub workspace: CompactionWorkspace,
    pub rowset_meta: RowsetMeta,
    pub rowset: RowsetSharedPtr,
    pub segment_files: Vec<SegmentArtifact>,
    pub stats: CompactionBuildStats,
    pub secondary_index_outputs: Vec<SecondaryIndexArtifact>,
}

impl StagedArtifact {
    pub fn from_rowset(
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        rowset: RowsetSharedPtr,
    ) -> Result<Self> {
        let rowset_meta = rowset.rowset_meta();
        let mut segment_files = Vec::new();
        for seg_id in 0..rowset_meta.num_segments() {
            let data_path = workspace.rowset_dir.join(format!("{}.dat", seg_id));
            let file_size_bytes = fs::metadata(&data_path)
                .map(|meta| meta.len())
                .unwrap_or_default();
            segment_files.push(SegmentArtifact {
                segment_id: seg_id,
                data_path,
                file_size_bytes,
            });
        }

        Ok(Self {
            stats: CompactionBuildStats {
                input_rows: plan.planned_input_rows(),
                input_size: plan.planned_input_size(),
                output_rows: rowset.num_rows(),
                output_size: rowset.total_disk_size(),
            },
            plan,
            workspace,
            rowset_meta,
            rowset,
            segment_files,
            secondary_index_outputs: Vec::new(),
        })
    }

    pub fn final_rowset_path(&self, tablet: &Tablet) -> PathBuf {
        tablet
            .data_dir()
            .join("rowsets")
            .join(format!("rowset_{}", self.rowset_meta.rowset_id()))
    }
}

#[derive(Debug)]
pub enum CompactionBuildOutput {
    Rowset(StagedArtifact),
    PrimaryKey {
        artifact: StagedArtifact,
        pk_delta: crate::compaction::publish::record::PkPublishDelta,
    },
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn ensure_same_filesystem(left: &Path, right: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let left_dev = fs::metadata(left)
            .map_err(|err| {
                paro_error::io_error(format!(
                    "inspect compaction staging root {}: {}",
                    left.display(),
                    err
                ))
            })?
            .dev();
        let right_dev = fs::metadata(right)
            .map_err(|err| {
                paro_error::io_error(format!(
                    "inspect canonical rowset root {}: {}",
                    right.display(),
                    err
                ))
            })?
            .dev();
        if left_dev != right_dev {
            return Err(paro_error::invalid_input(format!(
                "compaction staging root {} and canonical rowset root {} are on different filesystems",
                left.display(),
                right.display()
            )));
        }
    }

    Ok(())
}
