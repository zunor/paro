// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Primary Key support.
//!
//! Modules:
//! - `comparable_encoding`: Memcmp-comparable primary-key encoding helpers.
//! - `row_id`: Unified 64-bit row identifier.
//! - `rssid`: Tablet-local rssid mapping manager.
//! - `delete_vector`: DeleteVector bitmap per segment.
//! - `primary_index`: In-memory L0 primary index.
//! - `persistent_index`: On-disk persistent primary index.

pub mod comparable_encoding;
pub mod delete_vector;
pub mod immutable_index;
pub mod persistent_index;
pub mod primary_index;
pub mod row_id;
pub mod rssid;

pub use comparable_encoding::ComparableEncoder;
pub use delete_vector::{DeleteVector, DeleteVectorSnapshot};
pub use immutable_index::{
    ImmutableIndexBuildOptions, ImmutableIndexReader, ImmutableIndexStats, ImmutableIndexWriter,
};
pub use persistent_index::{PersistentIndex, PERSISTENT_INDEX_FORMAT_VERSION};
pub use primary_index::{PrimaryIndex, PrimaryKeySerializer};
pub use row_id::{RowID, NULL_ROW_ID};
pub use rssid::{Rssid, RssidManager, RssidMappingEntry};
