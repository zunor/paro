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

mod delete_intent_store;
mod prepared_txn_registry;
mod primary_index;
mod shutdown_sweep;
pub mod statistics;
mod tablet_chunk_assembler;
pub mod tablet_meta;
pub mod tablet_reader;
mod tablet_reader_params;
mod tablet_rowid_lookup;
mod tablet_runtime;
pub mod tablet_schema;
mod wal_replay;

// Re-export main types
pub use statistics::{TabletColumnStatistics, TabletStatistics};
pub use tablet_meta::TabletMeta;
pub use tablet_reader::TabletReader;
pub use tablet_reader_params::{ColumnProjection, TabletReaderBuilder, TabletReaderParams};
pub use tablet_runtime::{
    PhysicalRowRef, PrimaryIndexUpdate, RetiredGcBarrier, RetiredPendingGcStatus, Tablet, TabletId,
    TabletIdentity, TabletReadGuard, TabletRef, TabletState, Version, VersionGap,
};
pub use tablet_schema::{ColumnId, KeysType, TabletColumn, TabletSchema, TabletSchemaRef};
