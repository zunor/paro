// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::buffer::{BufferManager, StandardBufferManager};
use crate::codec::vector_decoder;
use crate::index::art::{ARTConflictType, ARTKey, ART};
use crate::index::{BoundIndex, IndexAppendMode, IndexConstraintType};
use crate::rowset::{RowsetSharedPtr, SegmentSharedPtr};
use crate::tablet::{ColumnId, TabletRef};
use paro_common::allocator::{default_allocator, Allocator, ArenaAllocator};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::index_set::IndexSet;

const ART_BACKFILL_BATCH_SIZE: usize = 1024;

fn is_art_backfill_supported(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Date
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
    )
}

#[derive(Debug)]
pub(crate) struct RuntimeIndexes {
    indexes: IndexSet,
    declared_art_indexes: RwLock<HashSet<ColumnId>>,
}

impl RuntimeIndexes {
    pub(crate) fn new() -> Self {
        Self {
            indexes: IndexSet::new(),
            declared_art_indexes: RwLock::new(HashSet::new()),
        }
    }

    pub(crate) fn index_count(&self) -> usize {
        self.indexes.len()
    }

    pub(crate) fn has_index(&self, name: &str) -> bool {
        self.indexes.has_index(name)
    }

    pub(crate) fn get_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.indexes.find_by_name(name)
    }

    pub(crate) fn get_indexes(&self) -> Vec<Arc<dyn BoundIndex>> {
        self.indexes.get_all()
    }

    pub(crate) fn add_index(&self, index: Arc<dyn BoundIndex>) -> Result<()> {
        self.indexes.add_index(index)
    }

    pub(crate) fn remove_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.indexes.remove_index(name)
    }

    pub(crate) fn declare_art_index(&self, tablet: &TabletRef, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_indexes.write() {
            guard.insert(column_id);
        }
        tablet.mark_declared_art_column(column_id);
    }

    pub(crate) fn forget_art_index(&self, tablet: &TabletRef, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_indexes.write() {
            guard.remove(&column_id);
        }
        tablet.unmark_declared_art_column(column_id);
    }

    pub(crate) fn rebuild_art_index(&self, tablet: &TabletRef, column_id: ColumnId) -> Result<()> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible)?;
        let art_columns = [column_id];

        for rowset in rowsets {
            Self::rebuild_art_indexes_for_rowset(&rowset, &art_columns)?;
        }

        Ok(())
    }

    pub(crate) fn declared_art_columns(&self) -> Vec<ColumnId> {
        self.declared_art_indexes
            .read()
            .map(|guard| {
                let mut columns = guard.iter().copied().collect::<Vec<_>>();
                columns.sort_unstable();
                columns
            })
            .unwrap_or_default()
    }

    pub(crate) fn recovery_index_count(&self, tablet: &TabletRef) -> usize {
        let mut art_columns = self
            .declared_art_indexes
            .read()
            .map(|guard| guard.iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();

        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    for meta in segment.column_metas() {
                        let column_id = meta.column_id;
                        if segment.art_index(column_id).is_some() {
                            art_columns.insert(column_id);
                        }
                    }
                }
            }
        }

        self.indexes.len().saturating_add(art_columns.len())
    }

    pub(crate) fn rebuild_art_indexes_for_rowset(
        rowset: &RowsetSharedPtr,
        art_columns: &[ColumnId],
    ) -> Result<()> {
        if art_columns.is_empty() {
            return Ok(());
        }

        rowset.load()?;
        let buffer_manager: Arc<dyn BufferManager> = Arc::new(StandardBufferManager::default());
        for segment in rowset.segments() {
            for &column_id in art_columns {
                Self::rebuild_art_index_for_segment_with_buffer(
                    &segment,
                    column_id,
                    Arc::clone(&buffer_manager),
                )?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn rebuild_art_index_for_segment(
        segment: &SegmentSharedPtr,
        column_id: ColumnId,
    ) -> Result<()> {
        let buffer_manager: Arc<dyn BufferManager> = Arc::new(StandardBufferManager::default());
        Self::rebuild_art_index_for_segment_with_buffer(segment, column_id, buffer_manager)
    }

    fn rebuild_art_index_for_segment_with_buffer(
        segment: &SegmentSharedPtr,
        column_id: ColumnId,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> Result<()> {
        if segment.art_index(column_id).is_some() {
            return Ok(());
        }

        let schema_column = segment
            .schema()
            .column_by_id(column_id)
            .ok_or_else(|| paro_error::column_not_found(format!("column {}", column_id)))?;
        let logical_type = schema_column.logical_type.clone();
        if !is_art_backfill_supported(&logical_type) {
            return Err(paro_error::not_supported(format!(
                "ART runtime backfill does not support column {} type {:?}",
                column_id, logical_type
            )));
        }

        let mut iter = segment.new_column_iterator(column_id)?;
        let vector_allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let arena_allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let mut arena = ArenaAllocator::new(arena_allocator);
        let mut art = ART::new(
            format!("art_segment_{}_col_{}", segment.segment_id(), column_id),
            IndexConstraintType::None,
            column_id,
            logical_type.clone(),
            buffer_manager,
        );
        let mut row_id_base = 0u64;

        loop {
            let (count, batch) = iter.next_batch(ART_BACKFILL_BATCH_SIZE)?;
            if count == 0 {
                break;
            }

            let vector = vector_decoder::decode_column_batch(
                &logical_type,
                &batch,
                count,
                Arc::clone(&vector_allocator),
                None,
            )?;

            for row_idx in 0..count {
                if vector.is_null(row_idx) {
                    continue;
                }
                let key = ARTKey::from_vector_value(&vector, row_idx, &logical_type, &mut arena)?;
                let row_id = row_id_base
                    .checked_add(row_idx as u64)
                    .ok_or_else(|| paro_error::data_corrupted("ART row id overflow"))?;
                match art.insert_key(&mut arena, &key, row_id as i64, IndexAppendMode::Default) {
                    ARTConflictType::NoConflict => {}
                    ARTConflictType::Constraint => {
                        return Err(paro_error::internal(format!(
                            "Duplicate key violation while building runtime ART for column {}",
                            column_id
                        )))
                    }
                    ARTConflictType::Transaction => {
                        return Err(paro_error::serialization_failure(
                            "Transaction conflict while building runtime ART index",
                        ))
                    }
                }
            }

            row_id_base = row_id_base
                .checked_add(count as u64)
                .ok_or_else(|| paro_error::data_corrupted("ART batch row count overflow"))?;
        }

        segment.register_runtime_art_index(column_id, Arc::new(art));
        Ok(())
    }
}
