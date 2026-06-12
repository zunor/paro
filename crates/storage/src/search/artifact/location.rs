// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;
use serde::{Deserialize, Serialize};

use crate::search::stats::{SearchDefinitionId, SearchGenerationId, SegmentId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentPagePointer {
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
    pub column_id: ColumnId,
    pub page_offset: u64,
    pub page_len: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactFileId {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub package_index: u32,
}

/// Durable search artifact location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactLocation {
    Inline {
        page: SegmentPagePointer,
    },
    SidecarArtifactFile {
        file_id: ArtifactFileId,
        offset: u64,
        len: u64,
        checksum: u64,
    },
}
