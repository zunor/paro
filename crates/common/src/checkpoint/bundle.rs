// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

pub const CATALOG_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const ROUTE_REGISTRY_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const TABLET_SHARD_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const DERIVED_PROGRESS_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const ARTIFACT_ROOTS_BUNDLE_FORMAT_VERSION: u32 = 1;

/// Durable reference to one snapshot bundle emitted by a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotBundleRef {
    pub kind: BundleKind,
    /// First version stores a path relative to the checkpoint root.
    pub locator: String,
    pub size_bytes: u64,
    pub checksum_crc32c: u32,
    pub format_version: u32,
    pub base_checkpoint_id: Option<u64>,
}

/// Logical bundle classes written under one checkpoint root.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BundleKind {
    Catalog,
    RouteRegistry,
    TabletShard { shard_id: u32 },
    DerivedProgress,
    ArtifactRoots,
}

/// Durable artifact reference owned by checkpoint retention / artifact GC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointArtifactRef {
    pub namespace: String,
    pub locator: String,
}

/// Roots used for orphan artifact GC without re-parsing full snapshot payloads.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRootsBundle {
    pub roots: Vec<CheckpointArtifactRef>,
}

/// Stable route registry snapshot derived from catalog-visible tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteRegistryBundle {
    pub entries: Vec<RouteRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteRegistryEntry {
    pub schema_name: String,
    pub table_name: String,
    pub table_object_id: u64,
    pub tablet_id: u64,
    pub storage_descriptor: Vec<u8>,
}

/// Durable freeze mode captured while materializing one tablet checkpoint view.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DurableTabletFreezeMode {
    Optimistic,
    MetaLock,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointTabletIdentity {
    pub table_id: u64,
    pub partition_id: u64,
    pub tablet_id: u64,
    pub schema_id: u64,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TabletShardBundle {
    pub shard_id: u32,
    pub tablets: Vec<CheckpointTabletBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointTabletBundle {
    pub identity: CheckpointTabletIdentity,
    pub visible_rowset_count: u32,
    pub visible_version: i64,
    pub max_version: i64,
    pub cumulative_point: i64,
    pub freeze_mode: DurableTabletFreezeMode,
    pub meta_bytes: Vec<u8>,
    pub rowsets: Vec<CheckpointRowsetBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRowsetBundle {
    pub rowset_id: u64,
    pub meta_bytes: Vec<u8>,
    pub delete_vectors: Vec<CheckpointDeleteVectorBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointDeleteVectorBundle {
    pub segment_id: u32,
    pub version: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedProgressBundle {
    pub primary_indexes: Vec<PrimaryIndexProgressEntry>,
    pub graph_manifests: Vec<GraphManifestProgressEntry>,
    pub deferred_tasks: Vec<crate::checkpoint::DeferredTaskProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryIndexProgressEntry {
    pub tablet_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestProgressEntry {
    pub graph_name: String,
    pub locator: String,
    pub payload: Vec<u8>,
}
