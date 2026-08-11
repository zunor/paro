// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Slot-range parallel merge for perfect aggregate tables.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::{Allocator, ArenaAllocator, MemoryTag};
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::vector::VECTOR_SIZE;
use paro_function::aggregate::{
    prepare_direct_state_predicate, AggregateCombineType, AggregateInputData,
    PreparedDirectAggregateStatePredicate,
};

use super::slot_bitmap::SLOT_WORD_BITS;
use super::support::pointer_vector_from_slice;
use super::{
    combine_states, initialize_state_at_address, AggregateObject, AggregateStateLayout,
    PerfectAggregateHashTable, PerfectAggregateStateFilter,
};

const MIN_SLOTS_PER_TASK: usize = 64 * 1024;

#[derive(Debug, Clone, Copy)]
struct MergeSource {
    occupancy: *const u64,
    states: *mut u8,
}

#[derive(Clone, Copy)]
enum UniqueStateOwner {
    Target,
    Source(*mut u8),
}

struct FilteredWordContext<'a> {
    range: &'a Range<usize>,
    word: usize,
    state_offset: usize,
    predicate: &'a PreparedDirectAggregateStatePredicate,
    selected_slots: &'a mut AccountedVec<usize>,
    added_count: &'a mut usize,
}

/// Owns a perfect-table reduction while independent finish tasks combine
/// disjoint slot ranges.
///
/// The target buffers never resize after construction. A slot belongs to
/// exactly one range, so tasks neither read nor write another task's target
/// state. Source states are shared read-only and aggregate combine runs with
/// `PreserveInput`; each task owns its allocator. This makes allocator-backed
/// aggregate states safe without serializing otherwise independent ranges.
pub(crate) struct ParallelPerfectAggregateMerge {
    // Keep the target before task arenas: if finish work fails, target state
    // destructors must run while any memory allocated by combine is still live.
    target: UnsafeCell<Option<PerfectAggregateHashTable>>,
    sources: Box<[PerfectAggregateHashTable]>,
    task_arenas: Mutex<Vec<Option<ArenaAllocator>>>,
    task_preselections: Mutex<Vec<Option<AccountedVec<usize>>>>,
    source_views: Box<[MergeSource]>,
    target_occupancy: *mut u64,
    target_states: *mut u8,
    row_width: usize,
    total_groups: usize,
    state_layout: AggregateStateLayout,
    aggregate_objects: Arc<[AggregateObject]>,
    direct_combine_program: Option<paro_function::aggregate::DirectGroupedAggregateProgram>,
    allocator: Arc<dyn Allocator>,
    memory: MemoryAccountingContext,
    state_filter: Option<PerfectAggregateStateFilter>,
    direct_state_predicate: Option<PreparedDirectAggregateStatePredicate>,
    initial_count: usize,
    added_count: AtomicUsize,
    task_count: usize,
    finished: AtomicBool,
}

// SAFETY: construction freezes all table allocations. `combine_task` assigns
// non-overlapping target slot ranges, reads source buffers without mutation,
// and uses one arena per task. `finish` is called only after every task joins.
unsafe impl Send for ParallelPerfectAggregateMerge {}
unsafe impl Sync for ParallelPerfectAggregateMerge {}

impl std::fmt::Debug for ParallelPerfectAggregateMerge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelPerfectAggregateMerge")
            .field("sources", &self.sources.len())
            .field("total_groups", &self.total_groups)
            .field("task_count", &self.task_count)
            .field("added_count", &self.added_count.load(Ordering::Acquire))
            .field("finished", &self.finished.load(Ordering::Acquire))
            .finish()
    }
}

impl ParallelPerfectAggregateMerge {
    pub(crate) fn try_new(
        mut tables: Vec<PerfectAggregateHashTable>,
        max_tasks: usize,
        state_filter: Option<PerfectAggregateStateFilter>,
        memory: MemoryAccountingContext,
    ) -> Result<Option<Self>> {
        if tables.len() <= 1 {
            return Ok(None);
        }
        let mut target = tables.remove(0);
        for source in &tables {
            target.ensure_compatible(source)?;
        }
        let total_groups = target.total_groups();
        let task_limit = total_groups
            .div_ceil(MIN_SLOTS_PER_TASK)
            .max(1)
            .min(target.occupancy.word_count());
        let task_count = max_tasks.max(1).min(task_limit);
        let target_occupancy = target.occupancy.words_mut_ptr();
        let target_states = target.state_ptr(0);
        let source_views = tables
            .iter()
            .map(|source| MergeSource {
                occupancy: source.occupancy.words_ptr(),
                states: source.state_ptr(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let allocator = target.allocator();
        let initial_count = target.count();
        let row_width = target.row_width;
        let state_layout = target.state_layout.clone();
        let aggregate_objects: Arc<[AggregateObject]> =
            Arc::from(target.aggregate_objects.clone().into_boxed_slice());
        let direct_combine_program = target
            .direct_update_program
            .as_ref()
            .filter(|program| program.supports_direct_combine())
            .cloned();
        // A state filter can share the merge traversal only when every state
        // row combines synchronously in the slot loop. Generic aggregate ABI
        // batches may defer a row past its occupancy word; those tables retain
        // the ordinary post-merge emit filter.
        let direct_state_predicate = match (state_filter.as_ref(), direct_combine_program.as_ref())
        {
            (Some(filter), Some(_)) => {
                let object = aggregate_objects.get(filter.aggregate_index).ok_or_else(|| {
                    paro_error::internal(format!(
                        "perfect aggregate merge state-filter index out of bounds: index={}, aggregates={}",
                        filter.aggregate_index,
                        aggregate_objects.len()
                    ))
                })?;
                prepare_direct_state_predicate(
                    &object.function,
                    filter.comparison,
                    &filter.constant,
                )?
            }
            _ => None,
        };
        let state_filter = direct_state_predicate
            .is_some()
            .then_some(state_filter)
            .flatten();
        Ok(Some(Self {
            target: UnsafeCell::new(Some(target)),
            sources: tables.into_boxed_slice(),
            task_arenas: Mutex::new((0..task_count).map(|_| None).collect()),
            task_preselections: Mutex::new((0..task_count).map(|_| None).collect()),
            source_views,
            target_occupancy,
            target_states,
            row_width,
            total_groups,
            state_layout,
            aggregate_objects,
            direct_combine_program,
            allocator,
            memory,
            state_filter,
            direct_state_predicate,
            initial_count,
            added_count: AtomicUsize::new(0),
            task_count,
            finished: AtomicBool::new(false),
        }))
    }

    pub(crate) fn task_count(&self) -> usize {
        self.task_count
    }

    pub(crate) fn combine_task(&self, task_idx: usize) -> Result<()> {
        if task_idx >= self.task_count {
            return Err(paro_error::internal(format!(
                "perfect aggregate merge task out of bounds: task={task_idx}, tasks={}",
                self.task_count
            )));
        }
        if self.task_arenas.lock()[task_idx].is_some() {
            return Err(paro_error::internal(format!(
                "perfect aggregate merge task executed more than once: task={task_idx}"
            )));
        }

        let range = partition_range(self.total_groups, self.task_count, task_idx);
        let mut arena = ArenaAllocator::new(self.allocator.clone());
        let result = self.combine_range(range, &mut arena);
        // Preserve task-owned allocations even on error: already-combined
        // target states may refer to them and must remain valid until cleanup.
        self.task_arenas.lock()[task_idx] = Some(arena);
        let preselection = result?;
        if let Some(preselection) = preselection {
            self.task_preselections.lock()[task_idx] = Some(preselection);
        }
        Ok(())
    }

    fn combine_range(
        &self,
        range: Range<usize>,
        arena: &mut ArenaAllocator,
    ) -> Result<Option<AccountedVec<usize>>> {
        if self.state_filter.is_some() {
            return self.combine_filtered_range(range, arena);
        }
        let mut source_ptrs = Vec::with_capacity(VECTOR_SIZE);
        let mut target_ptrs = Vec::with_capacity(VECTOR_SIZE);
        let mut added = 0usize;
        for source in &self.source_views {
            let start_word = range.start / SLOT_WORD_BITS;
            let end_word = range.end.div_ceil(SLOT_WORD_BITS);
            for word in start_word..end_word {
                // SAFETY: source views cover every occupancy word and are
                // immutable for the complete merge.
                let mut bits = unsafe { *source.occupancy.add(word) };
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    let slot = word * SLOT_WORD_BITS + bit;
                    if slot < range.start || slot >= range.end {
                        continue;
                    }
                    // SAFETY: this task exclusively owns `range` in the target.
                    let target_occupied = unsafe { self.target_occupancy.add(word) };
                    let mask = 1_u64 << bit;
                    let target_state = unsafe { self.target_states.add(slot * self.row_width) };
                    let source_state = unsafe { source.states.add(slot * self.row_width) };
                    if unsafe { *target_occupied } & mask == 0 {
                        if self
                            .direct_combine_program
                            .as_ref()
                            .is_some_and(|program| program.supports_trivial_state_copy())
                        {
                            // SAFETY: the task owns this target range and
                            // direct admission proves the complete source row
                            // is fixed-width and non-owning. MaybeUninit bytes
                            // permit copying any struct padding.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    source_state.cast::<MaybeUninit<u8>>(),
                                    target_state.cast::<MaybeUninit<u8>>(),
                                    self.row_width,
                                );
                                *target_occupied |= mask;
                            }
                            added += 1;
                            continue;
                        }
                        unsafe {
                            initialize_state_at_address(
                                &self.state_layout,
                                &self.aggregate_objects,
                                target_state,
                            );
                            *target_occupied |= mask;
                        }
                        added += 1;
                    }
                    if let Some(program) = self.direct_combine_program.as_ref() {
                        let combined = unsafe {
                            program.combine_direct_rows(source_state.cast_const(), target_state)
                        };
                        debug_assert!(combined, "direct combine program declined after admission");
                        continue;
                    }
                    source_ptrs.push(source_state);
                    target_ptrs.push(target_state);
                    if source_ptrs.len() == VECTOR_SIZE {
                        self.combine_pointer_batch(&source_ptrs, &target_ptrs, arena)?;
                        source_ptrs.clear();
                        target_ptrs.clear();
                    }
                }
            }
            if !source_ptrs.is_empty() {
                self.combine_pointer_batch(&source_ptrs, &target_ptrs, arena)?;
                source_ptrs.clear();
                target_ptrs.clear();
            }
        }
        self.added_count.fetch_add(added, Ordering::AcqRel);
        Ok(None)
    }

    fn combine_pointer_batch(
        &self,
        source_ptrs: &[*mut u8],
        target_ptrs: &[*mut u8],
        arena: &mut ArenaAllocator,
    ) -> Result<()> {
        let source = pointer_vector_from_slice(source_ptrs, self.allocator.clone())?;
        let target = pointer_vector_from_slice(target_ptrs, self.allocator.clone())?;
        let mut input_data =
            AggregateInputData::new(None, arena, AggregateCombineType::PreserveInput);
        combine_states(
            &self.aggregate_objects,
            &mut input_data,
            &source,
            &target,
            source_ptrs.len(),
        )
    }

    fn combine_filtered_range(
        &self,
        range: Range<usize>,
        _arena: &mut ArenaAllocator,
    ) -> Result<Option<AccountedVec<usize>>> {
        let program = self
            .direct_combine_program
            .as_ref()
            .expect("state-filter merge requires a complete direct combine program");
        let predicate = self
            .direct_state_predicate
            .as_ref()
            .expect("state-filter merge requires a prepared direct predicate");
        let filter = self
            .state_filter
            .as_ref()
            .expect("state-filter merge requires filter identity");
        let state_offset = self.state_layout.state_offset(filter.aggregate_index);
        debug_assert!(program.supports_trivial_state_copy());
        let mut selected_slots = AccountedVec::new_with_accounting(
            self.memory.reserve_grant(0)?,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        let mut added_count = 0usize;
        let start_word = range.start / SLOT_WORD_BITS;
        let end_word = range.end.div_ceil(SLOT_WORD_BITS);
        let mut source_words = vec![0_u64; self.source_views.len()];
        for word in start_word..end_word {
            // SAFETY: this task exclusively owns its complete occupancy words.
            let target_occupancy = unsafe { self.target_occupancy.add(word) };
            let target_bits = unsafe { *target_occupancy };
            let mut seen_bits = target_bits;
            let mut duplicate_bits = 0_u64;
            for (source, source_bits) in self.source_views.iter().zip(&mut source_words) {
                // SAFETY: source occupancy is immutable for the merge lifetime.
                *source_bits = unsafe { *source.occupancy.add(word) };
                duplicate_bits |= seen_bits & *source_bits;
                seen_bits |= *source_bits;
            }

            // Most grouped inputs have one local owner per key. Handle those
            // states owner-at-a-time so the hot loop neither searches every
            // source table nor writes rejected states into the destination.
            {
                let mut context = FilteredWordContext {
                    range: &range,
                    word,
                    state_offset,
                    predicate,
                    selected_slots: &mut selected_slots,
                    added_count: &mut added_count,
                };
                self.filter_unique_word(
                    &mut context,
                    target_bits & !duplicate_bits,
                    UniqueStateOwner::Target,
                )?;
                for (source, source_bits) in self.source_views.iter().zip(&source_words) {
                    self.filter_unique_word(
                        &mut context,
                        *source_bits & !duplicate_bits,
                        UniqueStateOwner::Source(source.states),
                    )?;
                }
            }

            // Shared keys are uncommon for clustered inputs, but require a
            // real reduction before their aggregate state can be filtered.
            while duplicate_bits != 0 {
                let bit = duplicate_bits.trailing_zeros() as usize;
                duplicate_bits &= duplicate_bits - 1;
                let slot = word * SLOT_WORD_BITS + bit;
                if slot < range.start || slot >= range.end {
                    continue;
                }
                let mask = 1_u64 << bit;
                let target_state = unsafe { self.target_states.add(slot * self.row_width) };
                let mut state = (target_bits & mask != 0).then_some(target_state);
                let mut source_count = 0usize;
                let mut only_source = std::ptr::null_mut();
                for (source, source_bits) in self.source_views.iter().zip(&source_words) {
                    if *source_bits & mask == 0 {
                        continue;
                    }
                    let source_state = unsafe { source.states.add(slot * self.row_width) };
                    source_count += 1;
                    if let Some(target) = state {
                        let combined = unsafe {
                            program.combine_direct_rows(source_state.cast_const(), target)
                        };
                        debug_assert!(combined, "direct combine program declined after admission");
                    } else if source_count == 1 {
                        only_source = source_state;
                    } else {
                        // A group shared by multiple source tables needs a
                        // writable target before remaining partials combine.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                only_source.cast::<MaybeUninit<u8>>(),
                                target_state.cast::<MaybeUninit<u8>>(),
                                self.row_width,
                            );
                            *target_occupancy |= mask;
                        }
                        added_count += 1;
                        state = Some(target_state);
                        let combined = unsafe {
                            program.combine_direct_rows(source_state.cast_const(), target_state)
                        };
                        debug_assert!(combined, "direct combine program declined after admission");
                    }
                }
                let (candidate_state, deferred_copy) = match state {
                    Some(state) => (state, std::ptr::null_mut()),
                    None => {
                        debug_assert_eq!(source_count, 1);
                        (only_source, only_source)
                    }
                };
                if !unsafe { predicate.matches(candidate_state.add(state_offset))? } {
                    continue;
                }
                if !deferred_copy.is_null() {
                    // SAFETY: deferred sources are initialized fixed-width,
                    // non-owning states admitted by `supports_trivial_state_copy`.
                    // This task exclusively owns the target occupancy word.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            deferred_copy.cast::<MaybeUninit<u8>>(),
                            target_state.cast::<MaybeUninit<u8>>(),
                            self.row_width,
                        );
                        *target_occupancy |= mask;
                    }
                    added_count += 1;
                }
                selected_slots.try_push(slot)?;
            }
        }
        self.added_count.fetch_add(added_count, Ordering::AcqRel);
        Ok(Some(selected_slots))
    }

    fn filter_unique_word(
        &self,
        context: &mut FilteredWordContext<'_>,
        mut bits: u64,
        owner: UniqueStateOwner,
    ) -> Result<()> {
        let (owner_states, copy_on_match) = match owner {
            UniqueStateOwner::Target => (self.target_states, false),
            UniqueStateOwner::Source(states) => (states, true),
        };
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let slot = context.word * SLOT_WORD_BITS + bit;
            if slot < context.range.start || slot >= context.range.end {
                continue;
            }
            // SAFETY: the owner occupancy bit proves this immutable slot state
            // is initialized, and range partitioning keeps the slot in bounds.
            let owner_state = unsafe { owner_states.add(slot * self.row_width) };
            // SAFETY: `state_offset` comes from the target/source-compatible
            // state layout used to prepare this direct predicate.
            if !unsafe {
                context
                    .predicate
                    .matches(owner_state.add(context.state_offset))?
            } {
                continue;
            }
            if copy_on_match {
                // SAFETY: the canonical slot is in bounds for the target's
                // fixed allocation. Unique ownership proves it is unoccupied.
                let target_state = unsafe { self.target_states.add(slot * self.row_width) };
                // SAFETY: unique ownership proves that the destination slot is
                // empty. Direct admission proves the complete state row is
                // fixed-width and non-owning, so copying padding is valid.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        owner_state.cast::<MaybeUninit<u8>>(),
                        target_state.cast::<MaybeUninit<u8>>(),
                        self.row_width,
                    );
                    *self.target_occupancy.add(context.word) |= 1_u64 << bit;
                }
                *context.added_count += 1;
            }
            context.selected_slots.try_push(slot)?;
        }
        Ok(())
    }

    /// Return the reduced table after all range tasks have completed.
    ///
    /// # Safety contract
    /// The finish driver calls this exactly once after observing completion of
    /// every scheduled task with acquire ordering.
    pub(crate) fn finish(&self) -> Result<PerfectAggregateHashTable> {
        if self.finished.swap(true, Ordering::AcqRel) {
            return Err(paro_error::internal(
                "perfect aggregate parallel merge finished more than once",
            ));
        }
        let mut arenas = self.task_arenas.lock();
        if arenas.iter().any(Option::is_none) {
            return Err(paro_error::internal(
                "perfect aggregate parallel merge finished before every task completed",
            ));
        }
        // SAFETY: the finish driver has joined every range task and the
        // `finished` exchange grants this caller exclusive ownership transfer.
        let mut target = unsafe { (&mut *self.target.get()).take() }.ok_or_else(|| {
            paro_error::internal("perfect aggregate merge target was already taken")
        })?;
        target.count = self
            .initial_count
            .checked_add(self.added_count.load(Ordering::Acquire))
            .ok_or_else(|| paro_error::internal("perfect aggregate merged count overflow"))?;
        for arena in arenas.iter_mut() {
            target
                .aggregate_allocator
                .absorb(arena.as_mut().expect("arenas were validated"))?;
        }
        if let Some(filter) = self.state_filter.as_ref() {
            let mut task_preselections = self.task_preselections.lock();
            if task_preselections.iter().any(Option::is_none) {
                return Err(paro_error::internal(
                    "perfect aggregate merge finished before every state-filter task completed",
                ));
            }
            let selected_count = task_preselections
                .iter()
                .map(|slots| slots.as_ref().expect("preselections were validated").len())
                .sum();
            let mut slots = AccountedVec::new_with_accounting(
                self.memory.reserve_grant(0)?,
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            );
            slots.try_reserve(selected_count)?;
            for task_slots in task_preselections.iter_mut() {
                let task_slots = task_slots.as_mut().expect("preselections were validated");
                slots.try_extend(task_slots.drain())?;
            }
            target.install_preselection(filter.clone(), slots);
        }
        Ok(target)
    }
}

fn partition_range(total: usize, tasks: usize, task: usize) -> Range<usize> {
    let words = total.div_ceil(SLOT_WORD_BITS);
    let start = words * task / tasks * SLOT_WORD_BITS;
    let end = (words * (task + 1) / tasks * SLOT_WORD_BITS).min(total);
    start..end
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_function::aggregate::{prepare_direct_state_predicate, AggregateComparison};
    use paro_planner::expression::AggregateType;

    use super::{partition_range, ParallelPerfectAggregateMerge, PerfectAggregateStateFilter};
    use crate::operators::aggregate::aggregate_object::AggregateObject;
    use crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateHashTable;

    #[test]
    fn slot_ranges_cover_domain_without_overlap() {
        let ranges = (0..4)
            .map(|task| partition_range(256, 4, task))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![0..64, 64..128, 128..192, 192..256]);
    }

    #[test]
    fn parallel_ranges_reduce_sparse_tables_into_one_owner() {
        const SLOTS: usize = 2 * 64 * 1024;
        let object = AggregateObject {
            function: get_count_star_function(),
            bind_info: None,
            child_count: 0,
            payload_size: 8,
            aggr_type: AggregateType::NonDistinct,
            return_type: LogicalType::BigInt,
            filter: None,
            order_bys: Vec::new(),
        };
        let make_table = || {
            PerfectAggregateHashTable::new(
                vec![LogicalType::Integer],
                vec![object.clone()],
                vec![Vec::new()],
                vec![0],
                vec![SLOTS],
                paro_common::test_utils::test_allocator(),
            )
            .unwrap()
        };
        let mut tables = vec![make_table(), make_table(), make_table()];
        for (table_idx, table) in tables.iter_mut().enumerate() {
            for slot in [7, SLOTS / 2 + 9] {
                let state = table.state_ptr(slot);
                table.initialize_state(state);
                unsafe { *state.cast::<i64>() = (table_idx + 1) as i64 };
                table.occupancy.set(slot);
                table.count += 1;
            }
        }

        let merge = Arc::new(
            ParallelPerfectAggregateMerge::try_new(
                tables,
                2,
                None,
                paro_common::memory::MemoryAccountingContext::detached(
                    paro_common::allocator::MemoryTag::HashTable,
                    paro_common::memory::MemoryAccountingClass::Revocable,
                ),
            )
            .unwrap()
            .unwrap(),
        );
        assert_eq!(merge.task_count(), 2);
        let workers = (0..2)
            .map(|task| {
                let merge = Arc::clone(&merge);
                std::thread::spawn(move || merge.combine_task(task))
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }

        let table = merge.finish().unwrap();
        assert_eq!(table.count(), 2);
        for slot in [7, SLOTS / 2 + 9] {
            assert_eq!(unsafe { *table.state_ptr(slot).cast::<i64>() }, 6);
        }
    }

    #[test]
    fn selective_merge_copies_only_qualifying_unique_owners() {
        const SLOTS: usize = 2 * 64 * 1024;
        let input_type = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let (function, targets) = get_sum_function()
            .bind(std::slice::from_ref(&input_type))
            .unwrap();
        assert_eq!(targets.as_slice(), std::slice::from_ref(&input_type));
        let payload_size = function.state_size;
        let object = AggregateObject {
            bind_info: function.bind_data.clone(),
            return_type: function.return_type.clone(),
            function,
            child_count: 1,
            payload_size,
            aggr_type: AggregateType::NonDistinct,
            filter: None,
            order_bys: Vec::new(),
        };
        let make_table = || {
            PerfectAggregateHashTable::new(
                vec![LogicalType::Integer],
                vec![object.clone()],
                vec![vec![0]],
                vec![0],
                vec![SLOTS],
                paro_common::test_utils::test_allocator(),
            )
            .unwrap()
        };
        let update = |table: &mut PerfectAggregateHashTable, keys: &[i32], values: &[i64]| {
            let groups = paro_common::test_utils::test_chunk_from_vectors(vec![
                paro_common::test_utils::test_i32_vector(keys),
            ]);
            let mut values_vector = paro_common::test_utils::test_vector_with_capacity(
                input_type.clone(),
                values.len(),
            );
            for (row, value) in values.iter().enumerate() {
                values_vector.set_value(row, &Value::Decimal(i128::from(*value), 15, 2));
            }
            values_vector.set_count(values.len());
            let payload = Chunk::from_vectors(
                vec![values_vector],
                paro_common::test_utils::test_allocator(),
            );
            assert!(table.try_update_direct_groups(&groups, &payload).unwrap());
        };

        let mut target = make_table();
        // key=1 remains physically owned by the target but is absent from the
        // output preselection. key=3 combines across both tables and passes.
        update(&mut target, &[1, 3], &[20_000, 25_000]);
        let mut source = make_table();
        // key=2 is a rejected source-only group and must never be copied.
        // key=4 is a qualifying source-only group and transfers ownership.
        update(&mut source, &[2, 3, 4], &[25_000, 10_000, 35_000]);

        let filter = PerfectAggregateStateFilter {
            aggregate_index: 0,
            comparison: AggregateComparison::GreaterThan,
            constant: Value::Decimal(30_000, 38, 2),
        };
        let predicate = prepare_direct_state_predicate(
            &source.aggregate_objects[0].function,
            filter.comparison,
            &filter.constant,
        )
        .unwrap()
        .unwrap();
        assert!(!unsafe { predicate.matches(source.state_ptr(3)) }.unwrap());
        assert!(unsafe { predicate.matches(source.state_ptr(5)) }.unwrap());
        let merge = Arc::new(
            ParallelPerfectAggregateMerge::try_new(
                vec![target, source],
                2,
                Some(filter.clone()),
                paro_common::memory::MemoryAccountingContext::detached(
                    paro_common::allocator::MemoryTag::HashTable,
                    paro_common::memory::MemoryAccountingClass::Revocable,
                ),
            )
            .unwrap()
            .unwrap(),
        );
        let workers = (0..merge.task_count())
            .map(|task| {
                let merge = Arc::clone(&merge);
                std::thread::spawn(move || merge.combine_task(task))
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }

        let table = merge.finish().unwrap();
        assert!(!table.occupancy.is_set(3)); // key=2
        assert!(table.occupancy.is_set(5)); // key=4
        let preselection = table.preselection.as_ref().unwrap();
        assert_eq!(preselection.filter, filter);
        let mut selected_slots = preselection.slots.as_slice().to_vec();
        selected_slots.sort_unstable();
        assert_eq!(selected_slots, [4, 5]); // keys 3 and 4
        assert_eq!(table.count(), 3);
    }
}
