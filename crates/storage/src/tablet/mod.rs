// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Tablet Module
//!
//! Tablet management for Paro storage engine.
//!
//! ## Architecture
//!
//! A Tablet is the fundamental storage unit in Paro, managing a collection of Rowsets
//! with version-based MVCC. This design is inspired by the Primary Key Table model.
//!
//! ```text
//! Tablet
//! ├── TabletMeta                      # Tablet metadata (persisted)
//! ├── TabletSchema                    # Schema definition
//! ├── max_committed_version           # Current max committed version
//! ├── cumulative_point                # Cumulative/Base compaction boundary
//! │
//! ├── Rowset v0 (Base)                # Base version after compaction
//! │   ├── Segment 0
//! │   └── Segment 1
//! │
//! ├── Rowset v1 (Cumulative)          # Cumulative versions
//! ├── Rowset v2 (Cumulative)
//! │
//! └── Rowset v3 (Delta)               # Latest delta writes
//! ```
//!
//! ## Version-based MVCC
//!
//! - Each Rowset has a version range `[start_version, end_version]`
//! - Readers see only Rowsets where `rowset.version <= visible_version`
//! - Writers create new Rowsets with `version = max_version + 1`
//! - No locks needed for reads - version isolation guarantees consistency
//!
//! ## Modules
//!
//! - `tablet_runtime`: Core Tablet runtime managing Rowset collection
//! - `tablet_meta`: Tablet metadata for persistence
//! - `tablet_schema`: Schema definition for Tablet columns
//! - `tablet_reader`: Cross-Rowset merge reader

mod layout_maintenance_gate;
mod prepared_txn_registry;
mod primary_index;
mod schema_adapter;
mod shutdown_sweep;
pub mod statistics;
mod tablet_chunk_assembler;
pub mod tablet_meta;
pub mod tablet_reader;
mod tablet_reader_params;
mod tablet_rowid_lookup;
mod tablet_runtime;
pub mod tablet_schema;
pub mod versioned_rowset_catalog;

use std::sync::atomic::{AtomicUsize, Ordering};

const DEFAULT_DELETE_PATCH_INLINE_ROW_REF_THRESHOLD: usize = 256;
static DELETE_PATCH_INLINE_ROW_REF_THRESHOLD: AtomicUsize =
    AtomicUsize::new(DEFAULT_DELETE_PATCH_INLINE_ROW_REF_THRESHOLD);

// Re-export main types
pub use crate::rowset::PhysicalRowRef;
pub use layout_maintenance_gate::{
    LayoutMaintenanceGate, LayoutMaintenanceLease, LayoutMaintenanceSnapshot,
};
pub use schema_adapter::TabletSchemaAdaptationPlan;
pub use statistics::{TabletColumnStatistics, TabletStatistics};
pub use tablet_meta::{SearchGenerationHeadMeta, TabletMeta};
pub use tablet_reader::TabletReader;
pub use tablet_reader_params::{
    ColumnProjection, ColumnValueProjection, TabletReaderBuilder, TabletReaderParams,
};
pub use tablet_rowid_lookup::TabletRowIdReader;
pub use tablet_runtime::{
    CheckpointMaintenanceTicket, CheckpointPublishObserver, CheckpointTabletFreezeMode,
    CheckpointTabletSnapshot, PrimaryIndexUpdate, RetiredGcBarrier, RetiredPendingGcStatus, Tablet,
    TabletId, TabletIdentity, TabletReadGuard, TabletRef, TabletSnapshotMaterialization,
    TabletState, Version, VersionGap,
};
pub(crate) use tablet_runtime::{
    RowsetPublishObserver, SearchGenerationHeadUpdates, SearchGenerationPublishGuard,
    SearchGenerationPublishOutcome, SearchIngestAdmissionLease,
};
pub use tablet_schema::{ColumnId, KeysType, TabletColumn, TabletSchema, TabletSchemaRef};

pub fn set_delete_patch_inline_row_ref_threshold(threshold: usize) {
    DELETE_PATCH_INLINE_ROW_REF_THRESHOLD.store(threshold.max(1), Ordering::Relaxed);
}

pub fn current_delete_patch_inline_row_ref_threshold() -> usize {
    DELETE_PATCH_INLINE_ROW_REF_THRESHOLD
        .load(Ordering::Relaxed)
        .max(1)
}
