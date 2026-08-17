// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::{SelectionVector, Vector};
use paro_storage::row::{RowStore, RowStoreSpillWriter};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::{QueryMemoryPool, RetainedChunkVec, RetainedMemoryHandle};
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::group_hash::GroupHashScratch;
use crate::operators::aggregate::payload_spill::AggregatePayloadSpillBuffer;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::runtime::breaker::{PartitionAggregateWindowHandle, UngroupedAggregateRuntimeState};
use crate::runtime::state::{
    DynGlobalState, DynLocalState, DynStateTypeId, UngroupedAggregateSinkLocal,
};

use super::{GlobalPartitionAggregateDetailFormat, PartitionAggregateSnapshot};

#[derive(Debug)]
pub(crate) enum PartitionAggregateLocalBacking {
    Columnar {
        tables: Vec<AggregateHashTable>,
        payloads: RetainedChunkVec,
    },
    External {
        spill: AggregatePayloadSpillBuffer,
    },
    Merged,
}

impl PartitionAggregateLocalBacking {
    pub(crate) fn reclaimable_bytes(&self) -> usize {
        match self {
            Self::Columnar { tables, payloads } => payloads.retained_bytes().saturating_add(
                tables
                    .iter()
                    .map(AggregateHashTable::memory_usage)
                    .sum::<usize>(),
            ),
            Self::External { .. } | Self::Merged => 0,
        }
    }
}

pub(crate) const PARTITION_AGGREGATE_BUILD_GLOBAL: DynStateTypeId =
    DynStateTypeId("partition_aggregate_build_global");
pub(crate) const PARTITION_AGGREGATE_BUILD_LOCAL: DynStateTypeId =
    DynStateTypeId("partition_aggregate_build_local");
pub(crate) const GLOBAL_AGGREGATE_WINDOW_BUILD_LOCAL: DynStateTypeId =
    DynStateTypeId("global_aggregate_window_build_local");

#[derive(Debug)]
pub(crate) struct GlobalAggregateBuildState {
    pub accumulator: UngroupedAggregateRuntimeState,
    pub payloads: Vec<Chunk>,
    pub payload_memory: Vec<Arc<RetainedMemoryHandle>>,
    pub external_payloads: Vec<RowStore>,
}

#[derive(Debug)]
pub(crate) enum GlobalAggregatePayloadBacking {
    Columnar(RetainedChunkVec),
    External(RowStoreSpillWriter<GlobalPartitionAggregateDetailFormat>),
    Merged,
}

#[derive(Debug)]
pub(crate) struct PartitionAggregateBuildGlobal {
    pub handle: Arc<PartitionAggregateWindowHandle>,
    /// Present only for the zero-key execution domain. Keyed windows publish
    /// sink-local hash tables through the runtime handle instead.
    pub global: Option<Mutex<GlobalAggregateBuildState>>,
}

impl DynGlobalState for PartitionAggregateBuildGlobal {
    fn state_type(&self) -> DynStateTypeId {
        PARTITION_AGGREGATE_BUILD_GLOBAL
    }
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }
}

#[derive(Debug)]
pub(crate) struct PartitionAggregateBuildLocal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Option<Chunk>,
    pub group_refs: Box<[usize]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub group_hash_scratch: GroupHashScratch,
    pub addresses: Vector,
    pub new_groups: SelectionVector,
    pub backing: Arc<Mutex<PartitionAggregateLocalBacking>>,
    pub local_reclaimer_name: Option<String>,
    pub query_memory: Option<Arc<QueryMemoryPool>>,
}

impl PartitionAggregateBuildLocal {
    pub(crate) fn unregister_reclaimer(&mut self) {
        if let (Some(memory), Some(name)) =
            (self.query_memory.as_ref(), self.local_reclaimer_name.take())
        {
            memory.unregister_reclaimer_by_name(&name);
        }
    }
}

impl Drop for PartitionAggregateBuildLocal {
    fn drop(&mut self) {
        self.unregister_reclaimer();
        if let PartitionAggregateLocalBacking::Columnar { tables, .. } = &mut *self.backing.lock() {
            for table in tables {
                let _ = table.destroy();
            }
        }
    }
}

impl DynLocalState for PartitionAggregateBuildLocal {
    fn state_type(&self) -> DynStateTypeId {
        PARTITION_AGGREGATE_BUILD_LOCAL
    }
    fn as_any(&self) -> &(dyn Any + Send) {
        self
    }
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send) {
        self
    }
}

/// Task-local state for a complete, unpartitioned aggregate window.
///
/// The accumulator is exactly the scalar-aggregate state used by the ordinary
/// ungrouped aggregate operator. Detail batches retain their existing vectors;
/// no projected payload or row copy is created.
#[derive(Debug)]
pub(crate) struct GlobalAggregateWindowBuildLocal {
    pub accumulator: UngroupedAggregateSinkLocal,
    pub payloads: GlobalAggregatePayloadBacking,
}

impl DynLocalState for GlobalAggregateWindowBuildLocal {
    fn state_type(&self) -> DynStateTypeId {
        GLOBAL_AGGREGATE_WINDOW_BUILD_LOCAL
    }
    fn as_any(&self) -> &(dyn Any + Send) {
        self
    }
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send) {
        self
    }
}

#[derive(Debug)]
pub struct PartitionAggregateEmitGlobal {
    handle: Arc<PartitionAggregateWindowHandle>,
    snapshot: OnceLock<Arc<PartitionAggregateSnapshot>>,
    next_batch: AtomicUsize,
}

impl PartitionAggregateEmitGlobal {
    pub(crate) fn new(handle: Arc<PartitionAggregateWindowHandle>) -> Self {
        Self {
            handle,
            snapshot: OnceLock::new(),
            next_batch: AtomicUsize::new(0),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<&Arc<PartitionAggregateSnapshot>> {
        if self.snapshot.get().is_none() {
            let snapshot = self.handle.snapshot()?;
            let _ = self.snapshot.set(snapshot);
        }
        self.snapshot
            .get()
            .ok_or_else(|| paro_error::internal("partition aggregate snapshot was not initialized"))
    }

    /// Resolve the immutable work domain after the dependency gate opens.
    /// Runtime construction may happen earlier, but scheduler admission must
    /// only call this once the producer's finalize phase has completed.
    pub(crate) fn work_count(&self) -> Result<usize> {
        Ok(self.snapshot()?.work_count())
    }

    #[inline]
    pub(crate) fn claim_batch(&self) -> usize {
        self.next_batch.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct PartitionAggregateEmitLocal {
    pub aggregate_selection: SelectionVector,
    pub external_cursor: Option<paro_storage::row::ReclaimingRowScanCursor>,
    pub global_external_chunk: Option<Chunk>,
}

impl PartitionAggregateEmitLocal {
    pub(crate) fn try_new(allocator: Arc<dyn paro_common::allocator::Allocator>) -> Result<Self> {
        Ok(Self {
            aggregate_selection: SelectionVector::try_with_capacity(0, allocator)?,
            external_cursor: None,
            global_external_chunk: None,
        })
    }
}

pub(crate) fn build_global(
    state: &crate::runtime::state::SinkGlobal,
) -> Result<&PartitionAggregateBuildGlobal> {
    let crate::runtime::state::SinkGlobal::Dyn(state) = state else {
        return Err(paro_error::internal(
            "partition aggregate build global state mismatch",
        ));
    };
    state
        .as_any()
        .downcast_ref::<PartitionAggregateBuildGlobal>()
        .ok_or_else(|| paro_error::internal("partition aggregate build global type mismatch"))
}

pub(crate) fn build_local_mut(
    state: &mut crate::runtime::state::SinkLocal,
) -> Result<&mut PartitionAggregateBuildLocal> {
    let crate::runtime::state::SinkLocal::Dyn(state) = state else {
        return Err(paro_error::internal(
            "partition aggregate build local state mismatch",
        ));
    };
    state
        .as_any_mut()
        .downcast_mut::<PartitionAggregateBuildLocal>()
        .ok_or_else(|| paro_error::internal("partition aggregate build local type mismatch"))
}

pub(crate) fn global_build_local_mut(
    state: &mut crate::runtime::state::SinkLocal,
) -> Result<&mut GlobalAggregateWindowBuildLocal> {
    let crate::runtime::state::SinkLocal::Dyn(state) = state else {
        return Err(paro_error::internal(
            "global aggregate window build local state mismatch",
        ));
    };
    state
        .as_any_mut()
        .downcast_mut::<GlobalAggregateWindowBuildLocal>()
        .ok_or_else(|| paro_error::internal("global aggregate window build local type mismatch"))
}

pub(crate) fn emit_global(
    state: &crate::runtime::state::SourceGlobal,
) -> Result<&PartitionAggregateEmitGlobal> {
    let crate::runtime::state::SourceGlobal::PartitionAggregateWindowEmit(state) = state else {
        return Err(paro_error::internal(
            "partition aggregate emit global state mismatch",
        ));
    };
    Ok(state)
}

pub(crate) fn emit_local_mut(
    state: &mut crate::runtime::state::SourceLocal,
) -> Result<&mut PartitionAggregateEmitLocal> {
    let crate::runtime::state::SourceLocal::PartitionAggregateWindowEmit(state) = state else {
        return Err(paro_error::internal(
            "partition aggregate emit local state mismatch",
        ));
    };
    Ok(state)
}
