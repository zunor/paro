// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ownership token for a complete pre-commit search generation.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};

use paro_common::ddl::DdlObjectKey;
use paro_common::effect::{
    ArtifactRef, SearchGenerationBuildArtifact, SearchGenerationHeadMeta, StagedArtifactDescriptor,
    StorageCommitOp, TabletApplyOp, TabletMutation,
};
use paro_common::error::{self as paro_error, Result};

use crate::tablet::LayoutMaintenanceLease;

use super::generation::coverage::SearchGenerationCoverage;

const WORKSPACE_TRANSIENT: u8 = 0;
const WORKSPACE_DURABLE_HANDOFF: u8 = 1;
const WORKSPACE_PUBLISHED: u8 = 2;
const WORKSPACE_DISCARDED: u8 = 3;

/// Lifecycle of the private workspace across the durable-append boundary.
///
/// Before append, dropping the owner is an abort and removes the workspace.
/// Once handed to the commit runtime, an ambiguous or failed required-publish
/// result must preserve it for WAL recovery. Only a proven non-durable append
/// failure or a completed tablet mutation may remove it.
#[derive(Debug)]
struct StagedWorkspace {
    root: PathBuf,
    state: AtomicU8,
}

impl StagedWorkspace {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            state: AtomicU8::new(WORKSPACE_TRANSIENT),
        }
    }

    fn prepare_durable_handoff(&self) -> Result<()> {
        match self.state.compare_exchange(
            WORKSPACE_TRANSIENT,
            WORKSPACE_DURABLE_HANDOFF,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(WORKSPACE_DURABLE_HANDOFF) => Ok(()),
            Err(state) => Err(paro_error::internal(format!(
                "staged search workspace cannot enter durable handoff from state {state}"
            ))),
        }
    }

    fn discard_before_durable_append(&self) -> Result<()> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                WORKSPACE_TRANSIENT | WORKSPACE_DURABLE_HANDOFF => {
                    if self
                        .state
                        .compare_exchange(
                            state,
                            WORKSPACE_DISCARDED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return self.remove();
                    }
                }
                WORKSPACE_DISCARDED => return Ok(()),
                WORKSPACE_PUBLISHED => {
                    return Err(paro_error::internal(
                        "cannot discard an already published search workspace as non-durable",
                    ));
                }
                _ => {
                    return Err(paro_error::internal(format!(
                        "staged search workspace has invalid lifecycle state {state}"
                    )));
                }
            }
        }
    }

    fn mark_published(&self) -> Result<()> {
        match self.state.compare_exchange(
            WORKSPACE_DURABLE_HANDOFF,
            WORKSPACE_PUBLISHED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(WORKSPACE_PUBLISHED) => {
                // Publication is already durable at this point. Cleanup is a
                // separate failure domain: startup orphan sweeping can retry
                // it, so it must never turn a committed transaction into an
                // apply failure.
                if let Err(error) = self.remove() {
                    tracing::warn!(
                        path = %self.root.display(),
                        error = %error,
                        "failed to remove published search-generation workspace"
                    );
                }
                Ok(())
            }
            Err(state) => Err(paro_error::internal(format!(
                "staged search workspace cannot publish from state {state}"
            ))),
        }
    }

    fn remove(&self) -> Result<()> {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(paro_error::io_error(format!(
                "remove staged search-generation workspace {}: {}",
                self.root.display(),
                error
            ))),
        }
    }
}

impl Drop for StagedWorkspace {
    fn drop(&mut self) {
        if self.state.load(Ordering::Acquire) == WORKSPACE_DURABLE_HANDOFF {
            // The WAL may already name this workspace. Recovery, rather than a
            // transient Rust owner, now decides when it is orphaned.
            return;
        }
        if let Err(error) = self.remove() {
            tracing::warn!(
                path = %self.root.display(),
                error = %error,
                "failed to remove staged search-generation workspace"
            );
        }
    }
}

/// A staged definition directory plus the exclusive physical-layout lease
/// under which it was built.
///
/// Transient drop removes its private staging root. After durable handoff,
/// drop deliberately preserves the root until required publish completes or
/// recovery proves it orphaned; this prevents an ambiguous live failure from
/// deleting the only source named by WAL.
#[derive(Debug)]
pub struct StagedSearchGeneration {
    workspace: StagedWorkspace,
    staged_ref: ArtifactRef,
    generation_ref: ArtifactRef,
    head: SearchGenerationHeadMeta,
    definition_id: u64,
    generation_id: u64,
    build_snapshot_version: i64,
    config_fingerprint: u64,
    coverage: SearchGenerationCoverage,
    _layout_lease: LayoutMaintenanceLease,
}

pub(crate) struct StagedSearchGenerationInit {
    pub staging_root: PathBuf,
    pub staged_ref: ArtifactRef,
    pub generation_ref: ArtifactRef,
    pub head: SearchGenerationHeadMeta,
    pub definition_id: u64,
    pub generation_id: u64,
    pub build_snapshot_version: i64,
    pub config_fingerprint: u64,
    pub coverage: SearchGenerationCoverage,
    pub layout_lease: LayoutMaintenanceLease,
}

impl StagedSearchGeneration {
    pub(crate) fn new(init: StagedSearchGenerationInit) -> Self {
        Self {
            workspace: StagedWorkspace::new(init.staging_root),
            staged_ref: init.staged_ref,
            generation_ref: init.generation_ref,
            head: init.head,
            definition_id: init.definition_id,
            generation_id: init.generation_id,
            build_snapshot_version: init.build_snapshot_version,
            config_fingerprint: init.config_fingerprint,
            coverage: init.coverage,
            _layout_lease: init.layout_lease,
        }
    }

    pub fn coverage(&self) -> &SearchGenerationCoverage {
        &self.coverage
    }

    pub fn storage_op(&self, tablet_id: u64) -> StorageCommitOp {
        StorageCommitOp::Tablet(TabletApplyOp {
            tablet_id,
            mutations: vec![self.mutation()],
        })
    }

    pub fn mutation(&self) -> TabletMutation {
        TabletMutation::PublishSearchGeneration {
            staged_ref: self.staged_ref.clone(),
            generation_ref: self.generation_ref.clone(),
            head: self.head.clone(),
        }
    }

    /// Transfer cleanup authority to the durable commit lifecycle.
    ///
    /// This must happen immediately before the commit job is submitted. Once
    /// set, dropping the transient owner preserves the workspace unless append
    /// is proven not durable or publication completes.
    pub fn prepare_durable_handoff(&self) -> Result<()> {
        self.workspace.prepare_durable_handoff()
    }

    /// Remove a workspace after the commit runtime proves WAL append failed.
    pub fn discard_before_durable_append(&self) -> Result<()> {
        self.workspace.discard_before_durable_append()
    }

    /// Complete ownership transfer after the tablet mutation installed the
    /// immutable generation and persisted its head.
    pub fn mark_published(&self) -> Result<()> {
        self.workspace.mark_published()
    }

    pub fn durable_descriptor(
        &self,
        table_object: DdlObjectKey,
        table_id: u64,
        tablet_id: u64,
    ) -> StagedArtifactDescriptor {
        StagedArtifactDescriptor::SearchGenerationBuild(SearchGenerationBuildArtifact {
            table_object,
            table_id,
            tablet_id,
            definition_id: self.definition_id,
            generation_id: self.generation_id,
            build_snapshot_version: self.build_snapshot_version,
            config_fingerprint: self.config_fingerprint,
            staged_ref: self.staged_ref.clone(),
            generation_ref: self.generation_ref.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_path(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("paro-staged-search-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn transient_workspace_drop_removes_files() {
        let path = workspace_path("transient-drop");
        drop(StagedWorkspace::new(path.clone()));
        assert!(!path.exists());
    }

    #[test]
    fn durable_handoff_preserves_source_until_publication() {
        let path = workspace_path("durable-handoff");
        let workspace = StagedWorkspace::new(path.clone());
        workspace.prepare_durable_handoff().unwrap();
        drop(workspace);
        assert!(path.exists());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn proven_append_failure_and_publication_both_clean_workspace() {
        let failed_path = workspace_path("append-failure");
        let failed = StagedWorkspace::new(failed_path.clone());
        failed.prepare_durable_handoff().unwrap();
        failed.discard_before_durable_append().unwrap();
        assert!(!failed_path.exists());

        let published_path = workspace_path("published");
        let published = StagedWorkspace::new(published_path.clone());
        published.prepare_durable_handoff().unwrap();
        published.mark_published().unwrap();
        assert!(!published_path.exists());
    }
}
