// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;

use paro_storage::rowset::SegmentOptions;
use paro_storage::tablet::{ColumnProjection, TabletReaderParams};
use paro_storage::transaction::overlay_reader::TxnOverlayReader;

use crate::physical::specs::RowsetScanSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{RowsetSourceGlobal, RowsetSourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct RowsetSourceExec {
    pub desc: RowsetSourceDesc,
}

#[derive(Debug, Clone)]
pub struct RowsetSourceDesc {
    pub table_index: usize,
    pub table: Arc<paro_catalog::entry::TableCatalogEntry>,
    pub column_ids: Box<[usize]>,
    pub emit_row_id: bool,
    pub returned_types: Box<[paro_common::types::LogicalType]>,
}

impl RowsetSourceDesc {
    pub fn from_plan_spec(spec: &RowsetScanSpec) -> Self {
        Self {
            table_index: spec.table_index,
            table: spec.table.clone(),
            column_ids: spec.column_ids.clone(),
            emit_row_id: spec.emit_row_id,
            returned_types: spec.returned_types.clone(),
        }
    }
}

impl RowsetSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        let table = self
            .desc
            .table
            .get_storage()
            .cloned()
            .ok_or_else(|| paro_error::internal("rowset scan table has no storage handle"))?;
        let storage_snapshot = table.storage_snapshot(
            ctx.query.transaction.read_ts(),
            ctx.query.transaction.read_snapshot().lease(),
        )?;
        let overlay = TxnOverlayReader::for_tablet(&table.tablet(), &ctx.query.transaction)?;
        let mut segments = storage_snapshot
            .segments_with_options(SegmentOptions::default())?
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(overlay) = &overlay {
            let visible_rowsets = segments
                .iter()
                .map(|(rowset, _)| rowset.rowset_id())
                .collect::<HashSet<_>>();
            segments.extend(
                overlay
                    .segments_with_options(SegmentOptions::default())?
                    .into_iter()
                    .filter(|(rowset, _)| !visible_rowsets.contains(&rowset.rowset_id())),
            );
        }
        let overlay_delete_vectors = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors);
        let column_projection = if self.desc.column_ids.is_empty() && !self.desc.emit_row_id {
            ColumnProjection::new((0..table.types().len()).collect())
        } else {
            ColumnProjection::new(self.desc.column_ids.to_vec())
        };

        Ok(SourceGlobal::Rowset(Arc::new(RowsetSourceGlobal {
            table_index: self.desc.table_index,
            table,
            storage_snapshot,
            segments: segments.into_boxed_slice(),
            next_segment: Default::default(),
            column_projection,
            overlay_delete_vectors,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        global.rowset()?;
        Ok(SourceLocal::Rowset(RowsetSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = global.rowset()?;
        let local = local.rowset_mut()?;

        loop {
            ctx.cancel.check()?;
            if local.reader.is_none() {
                let segment_idx = global.next_segment.fetch_add(1, Ordering::AcqRel);
                local.next_morsel = segment_idx;
                let Some((rowset, segment)) = global.segments.get(segment_idx) else {
                    return Ok(SourcePoll::Finished);
                };

                let mut params =
                    TabletReaderParams::with_version(global.storage_snapshot.visible_version())
                        .with_projection(global.column_projection.clone())
                        .with_emit_row_id(self.desc.emit_row_id)
                        .with_segment(segment.segment_id());
                if let Some(delete_vectors) = &global.overlay_delete_vectors {
                    params = params.with_overlay_delete_vectors(Arc::clone(delete_vectors));
                }
                let mut reader = global.table.create_reader_with_allocator(
                    params,
                    ctx.query.allocator(MemoryTag::ColumnData),
                )?;
                reader.prepare_with_pinned_rowsets(vec![rowset.clone()])?;
                local.reader = Some(reader);
            }

            let reader = local.reader.as_mut().expect("rowset reader initialized");
            match reader.get_next_chunk()? {
                Some(mut chunk) => {
                    // The scratch chunk is only an ownership slot here; move
                    // the reader-owned vector array into it without cloning.
                    output.move_from(&mut chunk);
                    return Ok(SourcePoll::Output);
                }
                None => {
                    local.reader = None;
                }
            }
        }
    }
}
