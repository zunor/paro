// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-local read-your-writes overlay reader.
//!
//! The transaction crate only carries opaque participant state. This module
//! resolves the storage participant into typed pending rowsets and delete masks
//! before scan workers enter their hot loops.

use crate::primary_key::DeleteVector;
use crate::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use crate::tablet::TabletRef;
use crate::transaction::write_buffer::{PendingPrimaryKeyEntry, StorageTxnState};
use paro_common::error::Result;
use paro_transaction::{CommandId, TransactionView};
use std::collections::HashMap;
use std::sync::Arc;

pub type OverlayDeleteVectorMap = HashMap<(u64, u32), DeleteVector>;

#[derive(Debug, Clone)]
pub struct TxnOverlayReader {
    tablet_id: u64,
    command_id: CommandId,
    rowsets: Vec<RowsetSharedPtr>,
    delete_vectors: Arc<OverlayDeleteVectorMap>,
    primary_keys: HashMap<Vec<u8>, PendingPrimaryKeyEntry>,
}

impl TxnOverlayReader {
    pub fn for_tablet(tablet: &TabletRef, view: &TransactionView) -> Result<Option<Self>> {
        let Some(write_buffer) = StorageTxnState::write_buffer_from_view(view) else {
            return Ok(None);
        };

        let command_id = view.command_id().min(write_buffer.published_command_id());
        let tablet_id = tablet.tablet_id();
        let snapshot = write_buffer.immutable_overlay_snapshot_for_tablet(tablet_id, command_id)?;

        let mut delete_vectors = OverlayDeleteVectorMap::new();
        for location in snapshot.row_id_deletes {
            delete_vectors
                .entry((location.rowset_id, location.segment_id))
                .or_insert_with(|| DeleteVector::with_version(view.visible_version_i64()))
                .mark_deleted(location.row_offset);
        }

        if snapshot.rowsets.is_empty()
            && delete_vectors.is_empty()
            && snapshot.primary_keys.is_empty()
        {
            return Ok(None);
        }

        Ok(Some(Self {
            tablet_id,
            command_id,
            rowsets: snapshot.rowsets,
            delete_vectors: Arc::new(delete_vectors),
            primary_keys: snapshot.primary_keys,
        }))
    }

    #[inline]
    pub fn tablet_id(&self) -> u64 {
        self.tablet_id
    }

    #[inline]
    pub fn command_id(&self) -> CommandId {
        self.command_id
    }

    #[inline]
    pub fn rowsets(&self) -> &[RowsetSharedPtr] {
        &self.rowsets
    }

    pub fn all_rowsets(&self) -> Vec<RowsetSharedPtr> {
        self.rowsets.clone()
    }

    #[inline]
    pub fn has_delete_vectors(&self) -> bool {
        !self.delete_vectors.is_empty()
    }

    #[inline]
    pub fn delete_vectors(&self) -> Option<Arc<OverlayDeleteVectorMap>> {
        if self.delete_vectors.is_empty() {
            None
        } else {
            Some(Arc::clone(&self.delete_vectors))
        }
    }

    pub(crate) fn primary_key_entry(&self, key: &[u8]) -> Option<PendingPrimaryKeyEntry> {
        self.primary_keys.get(key).copied()
    }

    pub(crate) fn primary_key_entries(
        &self,
    ) -> impl Iterator<Item = (&Vec<u8>, &PendingPrimaryKeyEntry)> {
        self.primary_keys.iter()
    }

    pub fn segments_with_options(
        &self,
        options: SegmentOptions,
    ) -> Result<Vec<(RowsetSharedPtr, SegmentSharedPtr)>> {
        let mut segments = Vec::new();
        for rowset in &self.rowsets {
            for segment in rowset.open_segment_view(options.clone())? {
                segments.push((rowset.clone(), segment));
            }
        }
        Ok(segments)
    }
}
