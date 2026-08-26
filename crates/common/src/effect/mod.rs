// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod apply_descriptor;
mod cleanup;
mod data_op;
mod deferred_task;
mod post_commit_hook;
mod runtime_transition;
mod staged_artifact;
mod txn_catalog_op;

pub use apply_descriptor::ApplyDescriptor;
pub use cleanup::CleanupDescriptor;
pub use data_op::{
    decode_delete_patch_artifact_bytes, encode_delete_patch_artifact_bytes, ArtifactNamespace,
    ArtifactRef, CompactionCumulativePointAction, DeletePatchEncoding, DeletePatchGroup,
    DeletePatchInline, DeletePatchRef, DeletePatchSegment, PreparedDataOp, RetiredRowsetInput,
    RowsetLocator, SearchGenerationHeadMeta, StorageCommitOp, TabletApplyOp, TabletMutation,
    VersionSpan,
};
pub use deferred_task::DeferredTask;
pub use post_commit_hook::{GraphDmlTableDelta, PostCommitHookDescriptor};
pub use runtime_transition::RuntimeTransitionDescriptor;
pub use staged_artifact::{
    BulkLoadRowsetArtifact, BulkLoadUniqueSummary, SearchGenerationBuildArtifact,
    StagedArtifactDescriptor, StagingArtifactId,
};
pub use txn_catalog_op::CatalogTxnOp;
