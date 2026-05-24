// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::memory::MemoryAccountingContext;
use paro_common::vector::{SelectionVector, Vector};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::runtime::breaker::UngroupedAggregateRuntimeState;

use super::accounted_rows::AccountedValueRowSet;
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::ordered_helpers::OrderedAggregateCollector;
use super::perfect_aggregate_hashtable::{PerfectAggregateHashTable, PerfectHTScanPosition};
use super::radix_partitioned_aggregate_hashtable::{AggregateHTScanPosition, AggregateHashTable};

#[derive(Debug, Default)]
pub struct HashAggregateEmitSourceLocal {
    pub tables: Option<Vec<AggregateHashTable>>,
    pub positions: Vec<AggregateHTScanPosition>,
    pub grouping_idx: usize,
}

#[derive(Debug, Default)]
pub struct UngroupedAggregateEmitSourceLocal {
    pub state: Option<UngroupedAggregateRuntimeState>,
    pub emitted: bool,
}

#[derive(Debug, Default)]
pub struct PerfectHashAggregateEmitSourceLocal {
    pub table: Option<PerfectAggregateHashTable>,
    pub position: PerfectHTScanPosition,
}

impl Drop for HashAggregateEmitSourceLocal {
    fn drop(&mut self) {
        if let Some(tables) = self.tables.as_mut() {
            for table in tables {
                let _ = table.destroy();
            }
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

impl Drop for PerfectHashAggregateEmitSourceLocal {
    fn drop(&mut self) {
        if let Some(table) = self.table.as_mut() {
            let _ = table.destroy();
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateBuildSinkLocal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Option<Chunk>,
    pub group_refs: Box<[usize]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub addresses: Vector,
    pub new_groups: SelectionVector,
    pub tables: Vec<AggregateHashTable>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
    pub(crate) modifier_memory: MemoryAccountingContext,
    pub(crate) distinct_sets: Vec<Option<AccountedValueRowSet>>,
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
    pub(crate) distinct_sets: Vec<Option<AccountedValueRowSet>>,
}

#[derive(Debug)]
pub struct PerfectHashAggregateSinkLocal {
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Option<Chunk>,
    pub group_refs: Box<[usize]>,
    pub addresses: Vector,
    pub new_groups: SelectionVector,
    pub table: Option<PerfectAggregateHashTable>,
}

impl Drop for HashAggregateBuildSinkLocal {
    fn drop(&mut self) {
        for table in &mut self.tables {
            let _ = table.destroy();
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

#[derive(Debug)]
pub struct StreamingAggregateTransformGlobal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub layout: AggregateStateLayout,
    pub aggregate_inputs: Arc<[Vec<usize>]>,
}

pub struct StreamingAggregateTransformLocal {
    pub aggregate_objects: Arc<[AggregateObject]>,
    pub layout: AggregateStateLayout,
    pub aggregate_inputs: Arc<[Vec<usize>]>,
    pub projection_executor: Option<ExpressionExecutor>,
    pub payload_chunk: Chunk,
    /// U64-aligned aggregate states. Aggregate kernels access this buffer
    /// through raw state-address vectors; `destroyed` makes cleanup idempotent
    /// when flush/finalize exits through an error and `Drop` still runs.
    pub state_buffer: Vec<u64>,
    pub arena_allocator: ArenaAllocator,
    pub emitted: bool,
    pub destroyed: bool,
}

impl fmt::Debug for StreamingAggregateTransformLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingAggregateTransformLocal")
            .field("aggregate_count", &self.aggregate_objects.len())
            .field("state_buffer_words", &self.state_buffer.len())
            .field("emitted", &self.emitted)
            .field("destroyed", &self.destroyed)
            .finish()
    }
}
