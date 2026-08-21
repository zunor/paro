// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed materialized breaker source/sink adapter.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use super::{FoundBits, HandleRef, MaterializedHandle, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SinkGlobal, SinkLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct MaterializedSourceExec {
    pub handle: HandleRef<MaterializedHandle>,
}

#[derive(Debug)]
pub struct MaterializedSourceGlobal {
    reader: MaterializedReader,
    next_chunk: AtomicUsize,
}

impl MaterializedSourceGlobal {
    pub(crate) fn new(handle: Arc<MaterializedHandle>) -> Self {
        Self {
            reader: MaterializedReader::new(handle, "materialized source"),
            next_chunk: AtomicUsize::new(0),
        }
    }

    /// Capture the immutable producer snapshot once for this consumer runtime.
    ///
    /// Runtime globals may be created before their producer runs, so snapshot
    /// binding is lazy. Once bound, polling never returns to the resettable
    /// producer handle and remains valid for this execution attempt.
    pub(crate) fn sealed_chunks(&self) -> Result<&Arc<[Chunk]>> {
        self.reader.sealed_chunks()
    }

    #[inline]
    pub(crate) fn found_bits(&self) -> Option<FoundBits> {
        self.reader.found_bits()
    }

    #[inline]
    fn next_chunk_index(&self) -> usize {
        // Snapshot publication is handled by MaterializedReader. This atomic
        // only assigns distinct immutable chunk indices to source tasks.
        self.next_chunk.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct MaterializedSourceLocal {
    // Deliberately keep a task-local Arc lease: this removes even the
    // OnceLock acquire-load from the per-chunk polling path.
    chunks: Option<Arc<[Chunk]>>,
}

impl MaterializedSourceLocal {
    fn sealed_chunks(&mut self, global: &MaterializedSourceGlobal) -> Result<&Arc<[Chunk]>> {
        if self.chunks.is_none() {
            self.chunks = Some(Arc::clone(global.sealed_chunks()?));
        }
        Ok(self
            .chunks
            .as_ref()
            .expect("materialized task snapshot initialized above"))
    }
}

impl MaterializedSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Materialized(Arc::new(
            MaterializedSourceGlobal::new(ctx.handles.get(self.handle)?),
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::Materialized(MaterializedSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let global = global.materialized()?;
        let SourceLocal::Materialized(local) = local else {
            return Err(paro_error::internal(
                "materialized source local state mismatch",
            ));
        };
        let chunks = local.sealed_chunks(global)?;
        let idx = global.next_chunk_index();
        let Some(chunk) = chunks.get(idx) else {
            return Ok(SourcePoll::Finished);
        };
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeSinkExec {
    pub handle: HandleRef<MaterializedHandle>,
}

#[derive(Debug)]
pub struct MaterializeSinkGlobal {
    pub handle: Arc<MaterializedHandle>,
}

#[derive(Debug, Default)]
pub struct MaterializeSinkLocal {
    pub chunks: Vec<Chunk>,
}

impl MaterializeSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::Materialize(Arc::new(MaterializeSinkGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::Materialize(MaterializeSinkLocal::default()))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::Materialize(local) = local else {
            return Err(paro_error::internal(
                "materialize sink local state mismatch",
            ));
        };
        local.chunks.push(input.handoff_referencing_vectors());
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::Materialize(global) = global else {
            return Err(paro_error::internal(
                "materialize sink global state mismatch",
            ));
        };
        let SinkLocal::Materialize(local) = local else {
            return Err(paro_error::internal(
                "materialize sink local state mismatch",
            ));
        };
        global.handle.append_chunks(&mut local.chunks)?;
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
    }

    pub(crate) fn finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::Materialize(global) = global else {
            return Err(paro_error::internal(
                "materialize sink global state mismatch",
            ));
        };
        global.handle.seal()?;
        Ok(FinishPoll::Done)
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
    use crate::runtime::breaker::BreakerHandleMetadata;

    use super::*;

    fn materialized_handle() -> Arc<MaterializedHandle> {
        Arc::new(MaterializedHandle::new(BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::Materialized,
            row_type: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        }))
    }

    fn chunk(value: i32) -> Chunk {
        let allocator = paro_common::test_utils::test_allocator();
        Chunk::from_vectors(
            vec![Vector::try_from_i32(&[value], allocator.clone()).expect("test vector")],
            allocator,
        )
    }

    fn publish(handle: &MaterializedHandle, values: &[i32]) {
        let mut chunks = values.iter().copied().map(chunk).collect::<Vec<_>>();
        handle.append_chunks(&mut chunks).expect("append chunks");
        handle.seal().expect("seal chunks");
    }

    #[test]
    fn materialized_source_globals_own_independent_scan_cursors() {
        let handle = materialized_handle();
        publish(handle.as_ref(), &[10, 20]);
        let first = MaterializedSourceGlobal::new(Arc::clone(&handle));
        let second = MaterializedSourceGlobal::new(handle);

        assert_eq!(first.next_chunk_index(), 0);
        assert_eq!(first.next_chunk_index(), 1);
        assert_eq!(second.next_chunk_index(), 0);
        assert_eq!(first.sealed_chunks().unwrap().len(), 2);
        assert_eq!(second.sealed_chunks().unwrap().len(), 2);
    }

    #[test]
    fn materialized_source_snapshot_is_scoped_to_one_runtime() {
        let handle = materialized_handle();
        publish(handle.as_ref(), &[10]);
        let first_runtime = MaterializedSourceGlobal::new(Arc::clone(&handle));
        let first_snapshot = first_runtime.sealed_chunks().unwrap();
        assert_eq!(first_snapshot[0].column(0).unwrap().get_i32(0), Some(10));

        handle.reset_for_reuse();
        publish(handle.as_ref(), &[20]);
        let second_runtime = MaterializedSourceGlobal::new(handle);

        assert_eq!(
            first_runtime.sealed_chunks().unwrap()[0]
                .column(0)
                .unwrap()
                .get_i32(0),
            Some(10)
        );
        assert_eq!(
            second_runtime.sealed_chunks().unwrap()[0]
                .column(0)
                .unwrap()
                .get_i32(0),
            Some(20)
        );
    }
}
