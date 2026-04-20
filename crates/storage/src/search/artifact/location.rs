// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;
use serde::{Deserialize, Serialize};

use crate::search::stats::SegmentId;

/// Durable search artifact location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactLocation {
    InlineSegmentBlob {
        rowset_id: RowsetId,
        segment_id: SegmentId,
        column_id: ColumnId,
    },
    SidecarArtifactFile {
        relative_path: PathBuf,
        byte_offset: u64,
        byte_length: u64,
    },
}
