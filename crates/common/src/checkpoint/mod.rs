// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable checkpoint schema shared across checkpoint writers, recovery, and
//! retention planning.

mod bundle;
mod deferred_progress;
mod manifest;
mod retention;

pub use bundle::{ArtifactRootsBundle, BundleKind, CheckpointArtifactRef, SnapshotBundleRef};
pub use bundle::{
    CheckpointDeleteVectorBundle, CheckpointRowsetBundle, CheckpointTabletBundle,
    CheckpointTabletIdentity, DerivedProgressBundle, DurableTabletFreezeMode,
    GraphManifestProgressEntry, PrimaryIndexProgressEntry, RouteRegistryBundle, RouteRegistryEntry,
    TabletShardBundle, ARTIFACT_ROOTS_BUNDLE_FORMAT_VERSION, CATALOG_BUNDLE_FORMAT_VERSION,
    DERIVED_PROGRESS_BUNDLE_FORMAT_VERSION, ROUTE_REGISTRY_BUNDLE_FORMAT_VERSION,
    TABLET_SHARD_BUNDLE_FORMAT_VERSION,
};
pub use deferred_progress::{
    DeferredTaskKey, DeferredTaskKind, DeferredTaskProgress, DeferredTaskScope,
};
pub use manifest::{
    CheckpointCurrentPointer, CheckpointDatabaseIdentity, CheckpointFrontier, CheckpointManifest,
    JournalTailRef, RecoverySummary, CHECKPOINT_CURRENT_POINTER_FORMAT_VERSION,
    CHECKPOINT_MANIFEST_FORMAT_VERSION,
};
pub use retention::RetentionFloor;
