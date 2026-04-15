// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::workspace::{CompactionBuildOutput, StagedArtifact};
use crate::compaction::publish::record::PkPublishDelta;
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};

pub struct CompactionValidator;

impl CompactionValidator {
    pub fn validate_artifact(_tablet: &Tablet, output: &CompactionBuildOutput) -> Result<()> {
        match output {
            CompactionBuildOutput::Rowset(artifact) => Self::validate_rowset_artifact(artifact),
            CompactionBuildOutput::PrimaryKey { artifact, pk_delta } => {
                Self::validate_rowset_artifact(artifact)?;
                Self::validate_pk_delta(artifact, pk_delta)
            }
        }
    }

    fn validate_rowset_artifact(artifact: &StagedArtifact) -> Result<()> {
        if artifact.rowset.rowset_id() != artifact.plan.output_rowset_id {
            return Err(paro_error::invalid_input(format!(
                "compaction output rowset id {} does not match planned {}",
                artifact.rowset.rowset_id(),
                artifact.plan.output_rowset_id
            )));
        }
        if artifact.rowset.version() != artifact.plan.output_version {
            return Err(paro_error::invalid_input(format!(
                "compaction output version {} does not match planned {}",
                artifact.rowset.version(),
                artifact.plan.output_version
            )));
        }
        if !artifact.workspace.rowset_dir.exists() {
            return Err(paro_error::io_error(format!(
                "missing staged compaction directory {}",
                artifact.workspace.rowset_dir.display()
            )));
        }
        for segment in &artifact.segment_files {
            if !segment.data_path.exists() {
                return Err(paro_error::io_error(format!(
                    "missing staged segment file {}",
                    segment.data_path.display()
                )));
            }
            let size_bytes = std::fs::metadata(&segment.data_path)
                .map_err(|err| {
                    paro_error::io_error(format!(
                        "inspect staged segment file {}: {}",
                        segment.data_path.display(),
                        err
                    ))
                })?
                .len();
            if size_bytes == 0 {
                return Err(paro_error::io_error(format!(
                    "empty staged segment file {}",
                    segment.data_path.display()
                )));
            }
        }
        artifact.rowset.load()?;
        let _ = artifact.rowset.statistics()?;
        Ok(())
    }

    fn validate_pk_delta(artifact: &StagedArtifact, pk_delta: &PkPublishDelta) -> Result<()> {
        let max_segment_id = artifact.rowset.rowset_meta().num_segments();
        for candidate in &pk_delta.upsert_candidates {
            if candidate.output_location.rowset_id != artifact.plan.output_rowset_id {
                return Err(paro_error::invalid_input(format!(
                    "pk publish candidate points at rowset {}, expected {}",
                    candidate.output_location.rowset_id, artifact.plan.output_rowset_id
                )));
            }
            if candidate.output_location.segment_id >= max_segment_id {
                return Err(paro_error::invalid_input(format!(
                    "pk publish candidate segment {} out of range for output rowset {}",
                    candidate.output_location.segment_id, artifact.plan.output_rowset_id
                )));
            }
        }
        for delete_delta in &pk_delta.internal_delete_vectors {
            if delete_delta.segment_id >= max_segment_id {
                return Err(paro_error::invalid_input(format!(
                    "pk publish delete vector segment {} out of range for output rowset {}",
                    delete_delta.segment_id, artifact.plan.output_rowset_id
                )));
            }
        }
        Ok(())
    }
}
