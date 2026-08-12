// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::memory::MemoryAccountingContext;
use paro_common::vector::{SelectionVector, Vector};
use paro_storage::row::RowStoreSpillReader;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::QueryMemoryPool;
use crate::operators::aggregate::payload_spill::{
    AggregatePayloadSpillBuffer, AggregateStateSpillBuffer,
};
use crate::runtime::breaker::{AggregateHandle, UngroupedAggregateRuntimeState};

use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::distinct_state::DistinctAggregateState;
use super::group_hash::GroupHashScratch;
use super::group_key_codec::GroupKeyEncoder;
use super::ordered_helpers::OrderedAggregateCollector;
use super::perfect_aggregate_hashtable::{
    FinalizedPerfectAggregateTable, PerfectAggregateHashTable, PerfectAggregateScanScratch,
    PerfectAggregateStateFilter, PerfectHTScanPosition,
};
use super::post_reduction::PostAggregateFilterLocal;
use super::post_reduction::PostAggregateInputRollup;
use super::radix_partitioned_aggregate_hashtable::{AggregateHTScanPosition, AggregateHashTable};
use super::row_format::AggregateGroupFormat;

#[derive(Debug, Default)]
pub struct HashAggregateEmitSourceLocal {
    pub work: Option<HashAggregateEmitWork>,
    pub scan_chunk: Option<Chunk>,
    pub spilled_chunk: Option<Chunk>,
    pub position: AggregateHTScanPosition,
    pub having_executor: Option<ExpressionExecutor>,
    pub having_selection: Option<SelectionVector>,
    pub having_columns: Box<[usize]>,
    pub(crate) post_filter: Option<PostAggregateFilterLocal>,
}

#[derive(Debug)]
pub struct HashAggregateEmitSourceGlobal {
    pub handle: Arc<AggregateHandle>,
    pub work: Mutex<Option<VecDeque<HashAggregateEmitWork>>>,
    pub work_count: AtomicUsize,
}

impl HashAggregateEmitSourceGlobal {
    pub fn claim_work(&self) -> Option<HashAggregateEmitWork> {
        self.work.lock().as_mut()?.pop_front()
    }

    pub fn work_count(&self) -> usize {
        self.work_count.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub enum HashAggregateEmitWork {
    Table {
        grouping_idx: usize,
        table: AggregateHashTable,
    },
    Spilled {
        grouping_idx: usize,
        reader: RowStoreSpillReader<AggregateGroupFormat>,
    },
}

#[derive(Debug, Default)]
pub struct UngroupedAggregateEmitSourceLocal {
    pub state: Option<UngroupedAggregateRuntimeState>,
    pub emitted: bool,
    pub having_executor: Option<ExpressionExecutor>,
    pub having_selection: Option<SelectionVector>,
}

#[derive(Debug, Default)]
pub struct PerfectHashAggregateEmitSourceLocal {
    pub(crate) table: Option<FinalizedPerfectAggregateTable>,
    pub position: PerfectHTScanPosition,
    pub(crate) scan_scratch: Option<PerfectAggregateScanScratch>,
    pub(crate) state_filter: Option<PerfectAggregateStateFilter>,
    pub having_executor: Option<ExpressionExecutor>,
    pub having_selection: Option<SelectionVector>,
    pub(crate) post_filter: Option<PostAggregateFilterLocal>,
}

impl Drop for HashAggregateEmitWork {
    fn drop(&mut self) {
        if let Self::Table { table, .. } = self {
            let _ = table.destroy();
        }
    }
}

impl Drop for UngroupedAggregateEmitSourceLocal {
    fn drop(&mut self) {
        if let Some(state) = self.state.as_mut() {
            let _ = state.destroy();
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateBuildSinkLocal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Option<Chunk>,
    pub group_refs: Box<[usize]>,
    pub(crate) group_key_encoder: GroupKeyEncoder,
    pub(crate) group_hash_scratch: GroupHashScratch,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub addresses: Vector,
    pub new_groups: SelectionVector,
    pub tables: Arc<Mutex<Vec<AggregateHashTable>>>,
    pub(crate) local_build_reclaimer_name: Option<String>,
    pub(crate) local_payload_spill_reclaimer_name: Option<String>,
    pub(crate) local_state_spill_reclaimer_name: Option<String>,
    pub(crate) query_memory: Option<Arc<QueryMemoryPool>>,
    pub(crate) raw_payload_spill_enabled: bool,
    pub(crate) raw_payload_spill_requested: Arc<AtomicBool>,
    pub(crate) payload_spill: Option<AggregatePayloadSpillBuffer>,
    pub(crate) state_spill: Arc<Mutex<Option<AggregateStateSpillBuffer>>>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
    pub(crate) modifier_memory: MemoryAccountingContext,
    pub(crate) distinct: DistinctAggregateState,
}

#[derive(Debug)]
pub struct UngroupedAggregateSinkLocal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub layout: AggregateStateLayout,
    pub aggregate_inputs: Arc<[Vec<usize>]>,
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Chunk,
    pub state_buffer: Vec<u64>,
    pub addresses: Vector,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
    pub arena_allocator: ArenaAllocator,
    pub destroyed: bool,
    pub(crate) modifier_memory: MemoryAccountingContext,
    pub(crate) distinct: DistinctAggregateState,
}

#[derive(Debug)]
pub struct PerfectHashAggregateSinkLocal {
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Option<Chunk>,
    pub group_refs: Box<[usize]>,
    pub addresses: Vector,
    pub new_groups: SelectionVector,
    pub table: Option<PerfectAggregateHashTable>,
    pub(crate) input_rollup: Option<PostAggregateInputRollup>,
}

impl Drop for HashAggregateBuildSinkLocal {
    fn drop(&mut self) {
        self.unregister_local_reclaimers();
        for table in self.tables.lock().iter_mut() {
            let _ = table.destroy();
        }
    }
}

impl HashAggregateBuildSinkLocal {
    pub(crate) fn unregister_local_reclaimers(&mut self) {
        if let Some(memory) = self.query_memory.as_ref() {
            if let Some(name) = self.local_build_reclaimer_name.take() {
                memory.unregister_reclaimer_by_name(&name);
            }
            if let Some(name) = self.local_payload_spill_reclaimer_name.take() {
                memory.unregister_reclaimer_by_name(&name);
            }
            if let Some(name) = self.local_state_spill_reclaimer_name.take() {
                memory.unregister_reclaimer_by_name(&name);
            }
        }
    }

    pub(crate) fn raw_payload_spill_enabled(&self) -> bool {
        self.raw_payload_spill_enabled || self.raw_payload_spill_requested.load(Ordering::Acquire)
    }

    pub(crate) fn enable_raw_payload_spill(&mut self) {
        self.raw_payload_spill_enabled = true;
        self.raw_payload_spill_requested
            .store(true, Ordering::Release);
        self.unregister_local_reclaimers();
    }

    pub(crate) fn activate_raw_payload_spill_if_requested(&mut self) {
        if self.raw_payload_spill_requested.load(Ordering::Acquire) {
            self.enable_raw_payload_spill();
        }
    }
}

impl Drop for PerfectHashAggregateSinkLocal {
    fn drop(&mut self) {
        if let Some(table) = self.table.as_mut() {
            let _ = table.destroy();
        }
    }
}
