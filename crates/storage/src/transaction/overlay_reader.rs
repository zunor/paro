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
use crate::transaction::spill::StagedRowsetArtifact;
use crate::transaction::write_buffer::{PendingPrimaryKeyEntry, StorageTxnState};
use paro_common::error::Result;
use paro_transaction::{CommandId, TransactionView};
use std::collections::HashMap;
use std::sync::Arc;

pub type OverlayDeleteVectorMap = HashMap<(u64, u32), DeleteVector>;

#[derive(Debug, Clone)]
pub struct SpilledArtifactReader {
    tablet_id: u64,
    command_id: CommandId,
    artifact_id: u64,
    sequence: u64,
    bytes: u64,
    rowset: RowsetSharedPtr,
}

impl SpilledArtifactReader {
    pub(crate) fn from_rowset(rowset: RowsetSharedPtr, artifact: StagedRowsetArtifact) -> Self {
        Self {
            tablet_id: artifact.tablet_id(),
            command_id: artifact.command_id(),
            artifact_id: artifact.artifact_id(),
            sequence: artifact.sequence(),
            bytes: artifact.bytes(),
            rowset,
        }
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
    pub fn artifact_id(&self) -> u64 {
        self.artifact_id
    }

    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[inline]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    fn segments_with_options(
        &self,
        options: SegmentOptions,
    ) -> Result<Vec<(RowsetSharedPtr, SegmentSharedPtr)>> {
        self.rowset.load_with_options(options)?;
        Ok(self
            .rowset
            .segments()
            .into_iter()
            .map(|segment| (self.rowset.clone(), segment))
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct TxnOverlayReader {
    tablet_id: u64,
    command_id: CommandId,
    rowsets: Vec<RowsetSharedPtr>,
    delete_vectors: Arc<OverlayDeleteVectorMap>,
    spilled_artifacts: Vec<SpilledArtifactReader>,
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

        let spilled_artifacts = snapshot
            .spilled_rowsets
            .into_iter()
            .map(|(rowset, artifact)| SpilledArtifactReader::from_rowset(rowset, artifact))
            .collect::<Vec<_>>();

        if snapshot.rowsets.is_empty()
            && spilled_artifacts.is_empty()
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
            spilled_artifacts,
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
        let mut rowsets = self.rowsets.clone();
        rowsets.extend(
            self.spilled_artifacts
                .iter()
                .map(|artifact| artifact.rowset.clone()),
        );
        rowsets
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

    #[inline]
    pub fn spilled_artifacts(&self) -> &[SpilledArtifactReader] {
        &self.spilled_artifacts
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
            rowset.load_with_options(options.clone())?;
            for segment in rowset.segments() {
                segments.push((rowset.clone(), segment));
            }
        }
        for artifact in &self.spilled_artifacts {
            segments.extend(artifact.segments_with_options(options.clone())?);
        }
        Ok(segments)
    }
}
