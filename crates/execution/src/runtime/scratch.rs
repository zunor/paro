// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-task scratch chunks and pending chunk ownership.

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::{MemoryAccountingClass, MemoryOwner};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use crate::memory_runtime::{
    LocalMemoryGrant, OperatorMemoryAccount, OperatorMemoryScope, QueryMemoryPool,
    DEFAULT_LOCAL_INITIAL_GRANT_BYTES,
};

use super::context::RetainedMemorySnapshot;
use super::state::{SinkLocal, SourceLocal, TransformLocal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLayout {
    pub types: Box<[LogicalType]>,
    pub capacity: usize,
    pub kind: ChunkLayoutKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkLayoutKind {
    Materialized,
    View,
}

impl ChunkLayout {
    pub fn new(types: impl Into<Box<[LogicalType]>>, capacity: usize) -> Self {
        Self {
            types: types.into(),
            capacity,
            kind: ChunkLayoutKind::Materialized,
        }
    }

    pub fn view(types: impl Into<Box<[LogicalType]>>, capacity: usize) -> Self {
        Self {
            types: types.into(),
            capacity,
            kind: ChunkLayoutKind::View,
        }
    }

    pub fn create_chunk(&self, allocator: Arc<dyn Allocator>) -> Result<Chunk> {
        match self.kind {
            ChunkLayoutKind::Materialized => {
                Chunk::try_initialize(&self.types, self.capacity, allocator)
            }
            ChunkLayoutKind::View => {
                let mut chunk = Chunk::try_init_empty(&self.types, allocator)?;
                chunk.set_capacity(self.capacity);
                Ok(chunk)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineScratchLayout {
    pub source_output: ChunkLayout,
    pub transform_outputs: Box<[ChunkLayout]>,
    pub max_cardinality: usize,
}

impl PipelineScratchLayout {
    pub fn new(
        source_output: ChunkLayout,
        transform_outputs: Vec<ChunkLayout>,
        max_cardinality: usize,
    ) -> Self {
        Self {
            source_output,
            transform_outputs: transform_outputs.into_boxed_slice(),
            max_cardinality,
        }
    }

    pub fn create_scratch(&self, allocator: Arc<dyn Allocator>) -> Result<PipelineScratch> {
        let source_chunk = self.source_output.create_chunk(allocator.clone())?;
        let transform_chunks = self
            .transform_outputs
            .iter()
            .map(|layout| layout.create_chunk(allocator.clone()))
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        Ok(PipelineScratch {
            source_chunk,
            transform_chunks,
            expression: ExpressionScratchArena::default(),
        })
    }
}

#[derive(Debug)]
pub struct PipelineScratch {
    pub source_chunk: Chunk,
    pub transform_chunks: Box<[Chunk]>,
    pub expression: ExpressionScratchArena,
}

impl PipelineScratch {
    pub fn transform_chunk_mut(&mut self, index: usize) -> Option<&mut Chunk> {
        self.transform_chunks.get_mut(index)
    }
}

#[derive(Debug, Default)]
pub struct ExpressionScratchArena {
    // Selection scratch is the first task-level expression buffer migrated out
    // of per-operator locals. Decoded-vector and scalar arenas follow the same
    // borrow boundary in later streaming phases.
    generation: u64,
    selection: SelectionScratchPool,
    selection_aux: SelectionScratchPool,
}

#[derive(Debug, Default)]
struct SelectionScratchPool {
    slots: Vec<SelectionVector>,
    next_slot: usize,
}

// Keep lookup cost and task-local retained memory bounded. Published views own
// their buffers independently, so replacing a shared cache entry is safe.
const SELECTION_SCRATCH_CACHE_CAPACITY: usize = 4;

impl SelectionScratchPool {
    fn lease(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&mut SelectionVector> {
        let required = capacity.max(1);
        let slot_count = self.slots.len();
        let mut reusable = None;
        let mut resizable = None;

        for offset in 0..slot_count {
            let candidate = (self.next_slot + offset) % slot_count;
            let slot = &self.slots[candidate];
            if !slot.is_uniquely_owned() {
                continue;
            }
            if slot.capacity() >= required {
                reusable = Some(candidate);
                break;
            }
            resizable.get_or_insert(candidate);
        }

        let slot_idx = if let Some(candidate) = reusable {
            candidate
        } else if let Some(candidate) = resizable {
            self.slots[candidate] = SelectionVector::try_with_capacity(required, allocator)?;
            candidate
        } else if slot_count < SELECTION_SCRATCH_CACHE_CAPACITY {
            self.slots
                .push(SelectionVector::try_with_capacity(required, allocator)?);
            slot_count
        } else {
            let candidate = self.next_slot;
            self.slots[candidate] = SelectionVector::try_with_capacity(required, allocator)?;
            candidate
        };
        self.next_slot = (slot_idx + 1) % self.slots.len();
        let slot = &mut self.slots[slot_idx];
        slot.set_len(capacity);
        Ok(slot)
    }
}

impl ExpressionScratchArena {
    #[inline(always)]
    pub fn begin_call(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    #[inline(always)]
    pub fn lease(&mut self) -> ExpressionScratchLease<'_> {
        ExpressionScratchLease { arena: self }
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

pub struct ExpressionScratchLease<'a> {
    arena: &'a mut ExpressionScratchArena,
}

impl ExpressionScratchLease<'_> {
    #[inline]
    pub fn generation(&self) -> u64 {
        self.arena.generation()
    }

    pub fn selection(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&mut SelectionVector> {
        self.arena.selection.lease(capacity, allocator)
    }

    pub fn selection_aux(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&mut SelectionVector> {
        self.arena.selection_aux.lease(capacity, allocator)
    }

    pub fn selection_pair(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<(&mut SelectionVector, &mut SelectionVector)> {
        let primary = self.arena.selection.lease(capacity, allocator.clone())?;
        let auxiliary = self.arena.selection_aux.lease(capacity, allocator)?;
        Ok((primary, auxiliary))
    }
}

#[derive(Debug)]
pub enum PendingChunkState {
    Empty,
    SourceOutput {
        chunk: ChunkLease,
    },
    TransformOutput {
        transform_idx: usize,
        resume: TransformResumeState,
        chunk: ChunkLease,
    },
    SinkInput {
        resume: SinkResumeState,
        chunk: ChunkLease,
    },
    CompletionResult {
        chunk: ChunkLease,
    },
}

impl Default for PendingChunkState {
    fn default() -> Self {
        Self::Empty
    }
}

impl PendingChunkState {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

#[derive(Debug)]
pub struct ChunkLease {
    pub chunk: Chunk,
    pub memory: RetainedMemorySnapshot,
}

impl ChunkLease {
    pub fn take_from_scratch(scratch: &mut Chunk, memory: RetainedMemorySnapshot) -> Result<Self> {
        let allocator = scratch.allocator().clone();
        let mut empty = Chunk::try_new(allocator)?;
        std::mem::swap(scratch, &mut empty);
        Ok(Self {
            chunk: empty,
            memory,
        })
    }

    pub fn restore_into(mut self, scratch: &mut Chunk) {
        scratch.move_from(&mut self.chunk);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformResumeState {
    FromStart,
    OutputMore,
    FlushNext,
    FlushOutputMore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkResumeState {
    FromStart,
    LocalCursor,
    RowOffset(usize),
    AfterTransformOutputMore {
        transform_idx: usize,
    },
    AfterFlushOutput {
        transform_idx: usize,
        output_more: bool,
    },
}

#[derive(Debug)]
pub struct TaskMemoryGrants {
    operator: LocalMemoryGrant,
}

impl TaskMemoryGrants {
    pub fn detached(allocator: Arc<dyn Allocator>) -> Self {
        Self {
            operator: LocalMemoryGrant::detached(
                0,
                MemoryTag::Allocator,
                MemoryAccountingClass::NonRevocable,
                allocator,
            ),
        }
    }

    pub fn new(operator: LocalMemoryGrant) -> Self {
        Self { operator }
    }

    /// Create a task-local grant owned by the query memory hierarchy.
    pub(crate) fn query_accounted(
        query_memory: Arc<QueryMemoryPool>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let account = Arc::new(OperatorMemoryAccount::new(query_memory));
        let owner: Arc<dyn MemoryOwner> = account;
        Ok(Self::new(LocalMemoryGrant::new(
            owner,
            DEFAULT_LOCAL_INITIAL_GRANT_BYTES,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            allocator,
        )?))
    }

    #[inline]
    pub fn call_scope(&self) -> OperatorMemoryScope<'_> {
        OperatorMemoryScope::new(&self.operator)
    }
}

/// State owned by a scheduler-dispatched finish sub-task.
///
/// Finish tasks operate only on pipeline-global state. Keeping their state
/// separate from [`PipelineTaskState`] makes it impossible to accidentally
/// construct source/transform/sink locals or vector scratch for completion
/// work, while retaining the same query-owned memory grant contract.
#[derive(Debug)]
pub(crate) struct FinishTaskState {
    memory: TaskMemoryGrants,
}

impl FinishTaskState {
    pub(crate) fn try_new(
        query_memory: Arc<QueryMemoryPool>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self {
            memory: TaskMemoryGrants::query_accounted(query_memory, allocator)?,
        })
    }

    #[inline]
    pub(crate) fn call_scope(&self) -> OperatorMemoryScope<'_> {
        self.memory.call_scope()
    }
}

#[derive(Debug)]
pub struct PipelineTaskState {
    data: Option<PipelineDataTaskState>,
    pub memory: TaskMemoryGrants,
    pub pending: PendingChunkState,
}

#[derive(Debug)]
pub struct PipelineDataTaskState {
    pub source: SourceLocal,
    pub transforms: Box<[TransformLocal]>,
    pub sink: SinkLocal,
    pub scratch: PipelineScratch,
}

impl PipelineTaskState {
    pub(crate) fn new_data(
        source: SourceLocal,
        transforms: Box<[TransformLocal]>,
        sink: SinkLocal,
        memory: TaskMemoryGrants,
        scratch: PipelineScratch,
    ) -> Self {
        Self {
            data: Some(PipelineDataTaskState {
                source,
                transforms,
                sink,
                scratch,
            }),
            memory,
            pending: PendingChunkState::Empty,
        }
    }

    pub(crate) fn new_finish(
        query_memory: Arc<QueryMemoryPool>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self {
            data: None,
            memory: TaskMemoryGrants::query_accounted(query_memory, allocator)?,
            pending: PendingChunkState::Empty,
        })
    }

    #[inline]
    pub fn data(&self) -> &PipelineDataTaskState {
        self.data
            .as_ref()
            .expect("data pipeline phase requires task-local operator state")
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut PipelineDataTaskState {
        self.data
            .as_mut()
            .expect("data pipeline phase requires task-local operator state")
    }

    pub fn data_and_memory_mut(&mut self) -> (&mut PipelineDataTaskState, &TaskMemoryGrants) {
        let Self { data, memory, .. } = self;
        (
            data.as_mut()
                .expect("data pipeline phase requires task-local operator state"),
            memory,
        )
    }

    pub(crate) fn is_finish_only(&self) -> bool {
        self.data.is_none()
    }
}

// Data-path callers retain the original field-oriented task API. Finish-only states never enter
// those phases, and fail fast if a future caller crosses that role boundary.
impl std::ops::Deref for PipelineTaskState {
    type Target = PipelineDataTaskState;

    fn deref(&self) -> &Self::Target {
        self.data
            .as_ref()
            .expect("finish-only task has no data-path operator state")
    }
}

impl std::ops::DerefMut for PipelineTaskState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
            .as_mut()
            .expect("finish-only task has no data-path operator state")
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;

    use super::*;

    #[test]
    fn scratch_layout_allocates_stable_chunk_slots() {
        let layout = PipelineScratchLayout::new(
            ChunkLayout::new(vec![LogicalType::Integer], 8),
            vec![ChunkLayout::new(vec![LogicalType::Boolean], 8)],
            8,
        );

        let scratch = layout
            .create_scratch(paro_common::test_utils::test_allocator())
            .expect("scratch allocation");

        assert_eq!(scratch.source_chunk.column_count(), 1);
        assert_eq!(scratch.transform_chunks.len(), 1);
        assert_eq!(scratch.transform_chunks[0].column_count(), 1);
        assert_eq!(scratch.transform_chunks[0].types(), &[LogicalType::Boolean]);
    }

    #[test]
    fn view_layout_avoids_materialized_vector_storage() {
        let layout = ChunkLayout::view(vec![LogicalType::Integer], 8);
        let chunk = layout
            .create_chunk(paro_common::test_utils::test_allocator())
            .expect("view scratch allocation");

        assert_eq!(chunk.column_count(), 1);
        assert_eq!(chunk.capacity(), 8);
        assert_eq!(chunk.get_allocation_size(), 0);
    }

    #[test]
    fn selection_scratch_rotates_while_published_view_is_live() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut arena = ExpressionScratchArena::default();
        let (first_id, published) = {
            let mut lease = arena.lease();
            let selection = lease
                .selection(8, allocator.clone())
                .expect("first selection lease");
            (selection.allocation_identity(), selection.clone())
        };

        let second_id = {
            let mut lease = arena.lease();
            lease
                .selection(8, allocator.clone())
                .expect("second selection lease")
                .allocation_identity()
        };
        assert_ne!(second_id, first_id);

        drop(published);
        let reused_id = {
            let mut lease = arena.lease();
            lease
                .selection(8, allocator)
                .expect("reused selection lease")
                .allocation_identity()
        };
        assert_eq!(reused_id, first_id);
    }

    #[test]
    fn selection_scratch_reuses_non_fifo_release() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut arena = ExpressionScratchArena::default();
        let first = {
            let mut lease = arena.lease();
            lease
                .selection(8, allocator.clone())
                .expect("first selection lease")
                .clone()
        };
        let second_id = {
            let mut lease = arena.lease();
            lease
                .selection(8, allocator.clone())
                .expect("second selection lease")
                .allocation_identity()
        };

        let reused_id = {
            let mut lease = arena.lease();
            lease
                .selection(8, allocator)
                .expect("non-FIFO selection lease")
                .allocation_identity()
        };

        assert_eq!(reused_id, second_id);
        assert_eq!(arena.selection.slots.len(), 2);
        drop(first);
    }

    #[test]
    fn selection_scratch_cache_stays_bounded_with_live_views() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut arena = ExpressionScratchArena::default();
        let published_count = SELECTION_SCRATCH_CACHE_CAPACITY * 3;
        let mut published = Vec::with_capacity(published_count);

        for value in 0..published_count {
            let mut lease = arena.lease();
            let selection = lease
                .selection(1, allocator.clone())
                .expect("selection lease");
            selection.set(0, value);
            published.push(selection.clone());
        }

        assert_eq!(
            arena.selection.slots.len(),
            SELECTION_SCRATCH_CACHE_CAPACITY
        );
        for (value, selection) in published.iter().enumerate() {
            assert_eq!(selection.get(0), value);
        }
    }

    #[test]
    fn chunk_lease_moves_scratch_chunk_without_deep_copy() {
        let layout = ChunkLayout::new(vec![LogicalType::Integer], 4);
        let mut scratch = layout
            .create_chunk(paro_common::test_utils::test_allocator())
            .expect("scratch allocation");
        scratch.set_cardinality(2);

        let lease =
            ChunkLease::take_from_scratch(&mut scratch, RetainedMemorySnapshot { bytes: 128 })
                .expect("lease should move chunk out");

        assert_eq!(scratch.column_count(), 0);
        assert_eq!(lease.chunk.column_count(), 1);
        assert_eq!(lease.chunk.size(), 2);
        assert_eq!(lease.memory.bytes, 128);

        lease.restore_into(&mut scratch);
        assert_eq!(scratch.column_count(), 1);
        assert_eq!(scratch.size(), 2);
    }
}
