// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime aggregate breaker handle.
//!
//! Build sinks update task-local aggregate state on the per-chunk path. The
//! handle is touched only during local merge, finish, cleanup, and the emit
//! source's first poll. The emit source takes ownership of finalized state so
//! scan batches do not lock the shared handle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use parking_lot::Mutex;
use paro_common::allocator::ArenaAllocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use crate::operators::aggregate::aggregate_kernel::destroy_states;
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::ordered_helpers::OrderedAggregateCollector;
use crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateHashTable;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct AggregateHandle {
    metadata: BreakerHandleMetadata,
    state: OnceLock<Mutex<Option<AggregateRuntimeState>>>,
    finalized: AtomicBool,
    cleanup: CleanupState,
}

impl AggregateHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            state: OnceLock::new(),
            finalized: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn initialize(&self, state: AggregateRuntimeState) -> Result<()> {
        match self.state.set(Mutex::new(Some(state))) {
            Ok(()) => Ok(()),
            Err(state) => {
                if let Some(mut state) = state.into_inner() {
                    state.destroy()?;
                }
                Ok(())
            }
        }
    }

    pub fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut AggregateRuntimeState) -> Result<R>,
    ) -> Result<R> {
        let state = self.state.get().ok_or_else(|| {
            paro_error::internal("aggregate handle has no initialized runtime state")
        })?;
        let mut guard = state.lock();
        let state = guard.as_mut().ok_or_else(|| {
            paro_error::internal("aggregate handle state was already moved to emit source")
        })?;
        f(state)
    }

    pub fn take_state(&self) -> Result<Option<AggregateRuntimeState>> {
        let state = self.state.get().ok_or_else(|| {
            paro_error::internal("aggregate handle has no initialized runtime state")
        })?;
        Ok(state.lock().take())
    }

    #[inline]
    pub fn mark_finalized(&self) {
        self.finalized.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for AggregateHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        if let Some(state) = self.state.get() {
            if let Some(mut state) = state.lock().take() {
                state.destroy()?;
            }
        }
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug)]
pub enum AggregateRuntimeState {
    Hash(HashAggregateRuntimeState),
    Ungrouped(UngroupedAggregateRuntimeState),
    Perfect(PerfectHashAggregateRuntimeState),
}

impl AggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        match self {
            Self::Hash(state) => state.destroy(),
            Self::Ungrouped(state) => state.destroy(),
            Self::Perfect(state) => state.destroy(),
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateRuntimeState {
    pub tables: Vec<AggregateHashTable>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
}

impl HashAggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        for table in &mut self.tables {
            table.destroy()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PerfectHashAggregateRuntimeState {
    pub table: PerfectAggregateHashTable,
}

impl PerfectHashAggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        self.table.destroy()
    }
}

pub struct UngroupedAggregateRuntimeState {
    pub aggregate_objects: std::sync::Arc<[AggregateObject]>,
    pub layout: AggregateStateLayout,
    pub aggregate_inputs: std::sync::Arc<[Vec<usize>]>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
    pub state_buffer: Vec<u64>,
    pub arena_allocator: ArenaAllocator,
    pub destroyed: bool,
}

impl std::fmt::Debug for UngroupedAggregateRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UngroupedAggregateRuntimeState")
            .field("aggregate_count", &self.aggregate_objects.len())
            .field("ordered_aggregate_count", &self.ordered_collectors.len())
            .field("state_buffer_words", &self.state_buffer.len())
            .field("destroyed", &self.destroyed)
            .finish()
    }
}

impl UngroupedAggregateRuntimeState {
    pub fn base_ptr(&mut self) -> *mut u8 {
        self.state_buffer.as_mut_ptr() as *mut u8
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.destroyed {
            return Ok(());
        }
        let addresses = single_state_addresses(
            self.base_ptr(),
            self.arena_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        destroy_states(&self.aggregate_objects, &mut input_data, &addresses, 1)?;
        self.destroyed = true;
        Ok(())
    }
}

pub fn single_state_addresses(
    base_ptr: *mut u8,
    allocator: std::sync::Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
    let mut addresses = Vector::try_new(paro_common::types::LogicalType::BigInt, 1, allocator)?;
    addresses.set_count(1);
    unsafe {
        *addresses.flat_data_mut::<*mut u8>() = base_ptr;
    }
    Ok(addresses)
}
