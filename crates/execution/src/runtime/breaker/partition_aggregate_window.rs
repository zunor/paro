// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime handle for a sort-free full-partition aggregate window.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::{Allocator, BufferAllocator, BufferManager, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, ErrorClass, Result};
use paro_common::memory::{MemoryAccountingContext, MemoryError, MemoryResult};
use paro_common::vector::{SelectionVector, ValidatedVectorSelection, Vector, VECTOR_SIZE};
use paro_storage::row::{RowSpillWriter, RowStoreSpillWriter};

use crate::memory_runtime::{ReclaimStats, Reclaimer, SpillCost};
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, build_groups_chunk, create_hash_aggregate_tables, group_payload_refs,
    normalized_grouping_sets, update_hash_aggregate_tables,
};
use crate::operators::aggregate::group_hash::GroupHashScratch;
use crate::operators::aggregate::payload_spill::{
    aggregate_spill_radix_bits, AggregatePayloadSpillBuffer, AggregateSpilledPayload,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHTScanPosition, AggregateHashTable,
};
use crate::operators::window::partition_aggregate::{
    FinalizedPartitionIndex, PartitionAggregateLocalOutput, PartitionAggregateOutputFormat,
    PartitionAggregateSnapshot,
};
use crate::physical::specs::PartitionAggregateWindowSpec;
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct PartitionAggregateWindowHandle {
    metadata: BreakerHandleMetadata,
    pending: Mutex<Vec<PartitionAggregateLocalOutput>>,
    /// Immutable, query-scoped publication. The registry owns this snapshot
    /// through query teardown, which is the current explicit lifetime of its
    /// detail and index memory leases.
    snapshot: OnceLock<Arc<PartitionAggregateSnapshot>>,
    sealed: AtomicBool,
    cleanup: CleanupState,
}

#[derive(Debug)]
pub struct PartitionAggregatePendingSpillReclaimer {
    name: String,
    handle: Arc<PartitionAggregateWindowHandle>,
    spec: PartitionAggregateWindowSpec,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    radix_bits: usize,
    memory: MemoryAccountingContext,
}

impl PartitionAggregatePendingSpillReclaimer {
    pub(crate) fn new(
        handle: Arc<PartitionAggregateWindowHandle>,
        spec: PartitionAggregateWindowSpec,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        parallelism: usize,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            name: Self::name_for(&handle),
            handle,
            spec,
            buffer_pool,
            radix_bits: aggregate_spill_radix_bits(parallelism),
            memory,
        }
    }

    pub fn name_for(handle: &PartitionAggregateWindowHandle) -> String {
        format!(
            "partition_aggregate_pending_spill:{}",
            handle.metadata().id.index()
        )
    }
}

impl Reclaimer for PartitionAggregatePendingSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.handle.reclaimable_pending_bytes()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        self.handle
            .reclaim_pending(
                target_bytes,
                &self.spec,
                Arc::clone(&self.buffer_pool),
                self.radix_bits,
                self.memory.clone(),
            )
            .map_err(|error| MemoryError::reclaim_failed(error.to_string()))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

impl PartitionAggregateWindowHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            pending: Mutex::new(Vec::new()),
            snapshot: OnceLock::new(),
            sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub(crate) fn append_local_with(
        &self,
        make_local: impl FnOnce() -> Result<PartitionAggregateLocalOutput>,
    ) -> Result<()> {
        let mut pending = self.pending.lock();
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append to a sealed partition aggregate window",
            ));
        }
        // The caller moves its local backing while this lock excludes the
        // pending reclaimer. Thus every revocable byte is continuously owned
        // by either the still-registered local reclaimer or this pending list.
        pending.push(make_local()?);
        Ok(())
    }

    fn reclaimable_pending_bytes(&self) -> usize {
        if self.is_sealed() {
            return 0;
        }
        self.pending.try_lock().map_or(0, |pending| {
            pending.iter().map(local_reclaimable_bytes).sum()
        })
    }

    fn reclaim_pending(
        &self,
        target_bytes: usize,
        spec: &PartitionAggregateWindowSpec,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        radix_bits: usize,
        memory: MemoryAccountingContext,
    ) -> Result<ReclaimStats> {
        if target_bytes == 0 || self.is_sealed() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(mut pending) = self.pending.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let mut reclaimed = 0usize;
        let mut spilled = 0usize;
        for local in pending.iter_mut() {
            if reclaimed >= target_bytes {
                break;
            }
            let before = local_reclaimable_bytes(local);
            let PartitionAggregateLocalOutput::Columnar { payloads, .. } = local else {
                continue;
            };
            if before == 0 {
                continue;
            }
            let replacement = match spill_payload_chunks(
                spec,
                payloads,
                Arc::clone(&buffer_pool),
                radix_bits,
                memory.clone(),
            ) {
                Ok(replacement) => replacement,
                // Every preceding local was already published atomically as
                // External. Report that progress so the coordinator can retry
                // the still-columnar suffix; only a zero-progress failure is
                // propagated as a failed reclaim attempt.
                Err(_) if reclaimed > 0 => break,
                Err(error) => return Err(error),
            };
            let replacement_bytes = replacement.size_in_bytes();
            let old =
                std::mem::replace(local, PartitionAggregateLocalOutput::External(replacement));
            drop(old);
            reclaimed = reclaimed.saturating_add(before);
            spilled = spilled.saturating_add(replacement_bytes);
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, spilled))
    }

    pub(crate) fn seal(
        &self,
        spec: &PartitionAggregateWindowSpec,
        allocator: Arc<dyn Allocator>,
        table_memory: MemoryAccountingContext,
        index_memory: MemoryAccountingContext,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        parallelism: usize,
        cancel: &paro_context::StatementCancellation,
    ) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        spec.verify()?;
        let pending = std::mem::take(&mut *self.pending.lock());
        if pending
            .iter()
            .any(|local| matches!(local, PartitionAggregateLocalOutput::External(_)))
        {
            let snapshot = seal_external(
                spec,
                pending,
                allocator,
                table_memory,
                index_memory,
                buffer_pool,
                parallelism,
                cancel,
            )?;
            self.snapshot.set(Arc::new(snapshot)).map_err(|_| {
                paro_error::internal("partition aggregate window was published more than once")
            })?;
            self.sealed.store(true, Ordering::Release);
            return Ok(());
        }
        let fallback_pending = (table_memory.accounting_class()
            == paro_common::memory::MemoryAccountingClass::Revocable)
            .then(|| clone_raw_payload_locals(&pending))
            .transpose()?;
        let in_memory = (|| -> Result<PartitionAggregateSnapshot> {
            let mut payloads = Vec::new();
            let mut payload_memory = Vec::new();
            // This first backend publishes one flat table per sink task, then
            // combines those fragments here. It deliberately reuses the aggregate
            // ABI and preserves every finalize/destructor rule. The publication
            // contract is independent of this merge policy: a future high-NDV
            // backend can replace this serial flat merge with the aggregate
            // breaker's parallel radix merge without changing detail replay or the
            // immutable result index.
            let mut table: Option<AggregateHashTable> = None;
            for local in pending {
                cancel.check()?;
                let PartitionAggregateLocalOutput::Columnar {
                    payloads: mut local_payloads,
                    tables: mut local_tables,
                    payload_memory: local_memory,
                } = local
                else {
                    unreachable!("external locals rejected above");
                };
                payloads.append(&mut local_payloads);
                payload_memory.push(local_memory);
                if local_tables.len() != 1 {
                    return Err(paro_error::internal(format!(
                        "partition aggregate local table count mismatch: expected=1, actual={}",
                        local_tables.len()
                    )));
                }
                let mut local_table = local_tables.pop().expect("verified local table count");
                match table.as_mut() {
                    Some(table) => table.combine(&mut local_table)?,
                    None => table = Some(local_table),
                }
            }
            let mut table = match table {
                Some(table) => table,
                None => create_hash_aggregate_tables(
                    &spec.aggregate,
                    allocator.clone(),
                    table_memory.clone(),
                    1,
                )?
                .pop()
                .ok_or_else(|| {
                    paro_error::internal("partition aggregate failed to create its empty table")
                })?,
            };

            // Finalize every group before publishing either the lookup index or
            // detail rows. A failure in any aggregate therefore fails the breaker
            // as a whole and can never be hidden by a later detail-row predicate.
            let result_types = table.scan_output_types();
            let mut position = AggregateHTScanPosition::default();
            let mut result_chunks = Vec::new();
            loop {
                cancel.check()?;
                let mut output =
                    Chunk::try_initialize(&result_types, VECTOR_SIZE, allocator.clone())?;
                if !table.scan(&mut position, &mut output)? {
                    break;
                }
                result_chunks.push(output);
            }
            let index = FinalizedPartitionIndex::try_new(
                spec.aggregate.groups[0].return_type(),
                spec.aggregate_column_count(),
                result_chunks,
                index_memory.clone(),
            )?;
            Ok(PartitionAggregateSnapshot::InMemory {
                payloads: Arc::from(payloads.into_boxed_slice()),
                index,
                _payload_memory: payload_memory.into_boxed_slice(),
            })
        })();
        let snapshot = match in_memory {
            Ok(snapshot) => snapshot,
            Err(error) if error.error_class() == ErrorClass::Resource => {
                let Some(fallback_pending) = fallback_pending else {
                    return Err(error);
                };
                seal_external(
                    spec,
                    fallback_pending,
                    allocator,
                    table_memory,
                    index_memory,
                    buffer_pool,
                    parallelism,
                    cancel,
                )?
            }
            Err(error) => return Err(error),
        };
        self.snapshot.set(Arc::new(snapshot)).map_err(|_| {
            paro_error::internal("partition aggregate window was published more than once")
        })?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<PartitionAggregateSnapshot>> {
        if !self.is_sealed() {
            return Err(paro_error::internal(
                "partition aggregate emit was scheduled before finalization",
            ));
        }
        self.snapshot
            .get()
            .cloned()
            .ok_or_else(|| paro_error::internal("sealed partition aggregate snapshot is missing"))
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

fn local_reclaimable_bytes(local: &PartitionAggregateLocalOutput) -> usize {
    match local {
        PartitionAggregateLocalOutput::Columnar {
            tables,
            payload_memory,
            ..
        } => payload_memory.bytes().saturating_add(
            tables
                .iter()
                .map(AggregateHashTable::memory_usage)
                .sum::<usize>(),
        ),
        PartitionAggregateLocalOutput::External(_) => 0,
    }
}

fn clone_raw_payload_locals(
    pending: &[PartitionAggregateLocalOutput],
) -> Result<Vec<PartitionAggregateLocalOutput>> {
    pending
        .iter()
        .map(|local| match local {
            PartitionAggregateLocalOutput::Columnar {
                payloads,
                payload_memory,
                ..
            } => Ok(PartitionAggregateLocalOutput::Columnar {
                payloads: payloads
                    .iter()
                    .map(Chunk::clone_referencing_vectors)
                    .collect(),
                // External replay reconstructs all aggregate state from raw
                // payload and deliberately carries no partially combined HT.
                tables: Vec::new(),
                payload_memory: Arc::clone(payload_memory),
            }),
            PartitionAggregateLocalOutput::External(_) => Err(paro_error::internal(
                "cannot clone an external partition aggregate local",
            )),
        })
        .collect()
}

fn spill_payload_chunks(
    spec: &PartitionAggregateWindowSpec,
    payloads: &[Chunk],
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    radix_bits: usize,
    memory: MemoryAccountingContext,
) -> Result<crate::operators::aggregate::payload_spill::AggregateSpilledPayload> {
    let inner: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
        buffer_pool.clone() as Arc<dyn BufferManager>,
        MemoryTag::HashTable,
    ));
    let scratch_allocator: Arc<dyn Allocator> = match memory.owner() {
        Some(owner) => Arc::new(paro_common::memory::MemoryOwnerAllocator::new(
            inner,
            owner,
            memory.domain(),
            MemoryTag::HashTable,
            paro_common::memory::MemoryAccountingClass::Spill,
        )),
        None => inner,
    };
    let mut spill = AggregatePayloadSpillBuffer::new(
        buffer_pool,
        spec.aggregate.payload_types.iter().cloned(),
        radix_bits,
        memory,
    )?;
    let mut hash_scratch = GroupHashScratch::try_new(VECTOR_SIZE, scratch_allocator)?;
    let group_refs = group_payload_refs(&spec.aggregate)?;
    for payload in payloads {
        let groups = build_groups_chunk(payload, &group_refs)?;
        let hashes = hash_scratch.hash(&groups)?;
        spill.append_payload(payload, &hashes)?;
    }
    Ok(spill.seal())
}

#[allow(clippy::too_many_arguments)]
fn seal_external(
    spec: &PartitionAggregateWindowSpec,
    pending: Vec<PartitionAggregateLocalOutput>,
    allocator: Arc<dyn Allocator>,
    table_memory: MemoryAccountingContext,
    index_memory: MemoryAccountingContext,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    parallelism: usize,
    cancel: &paro_context::StatementCancellation,
) -> Result<PartitionAggregateSnapshot> {
    let initial_radix_bits = aggregate_spill_radix_bits(parallelism);
    let mut spills = Vec::with_capacity(pending.len());
    for local in pending {
        cancel.check()?;
        match local {
            PartitionAggregateLocalOutput::External(spill) => spills.push(spill),
            PartitionAggregateLocalOutput::Columnar {
                payloads,
                tables,
                payload_memory,
            } => {
                spills.push(spill_payload_chunks(
                    spec,
                    &payloads,
                    Arc::clone(&buffer_pool),
                    initial_radix_bits,
                    table_memory.clone(),
                )?);
                // Keep the retained payload lease live through the complete
                // Columnar -> External copy. Only the published spill owns the
                // rows after this point.
                drop(tables);
                drop(payload_memory);
            }
        }
    }
    let mut radix_bits = initial_radix_bits;
    loop {
        cancel.check()?;
        match seal_external_radix(
            spec,
            &spills,
            allocator.clone(),
            table_memory.clone(),
            index_memory.clone(),
            Arc::clone(&buffer_pool),
            cancel,
        ) {
            Ok(mut snapshot) => {
                snapshot.set_external_spill_stats(
                    spills
                        .iter()
                        .map(AggregateSpilledPayload::size_in_bytes)
                        .sum(),
                    radix_bits
                        .saturating_sub(initial_radix_bits)
                        .saturating_add(1),
                );
                return Ok(snapshot);
            }
            Err(error)
                if error.error_class() == ErrorClass::Resource
                    && radix_bits < paro_storage::row::RadixPartitioning::MAX_RADIX_BITS =>
            {
                radix_bits += 1;
                spills = spills
                    .into_iter()
                    .map(|spill| spill.into_repartitioned(radix_bits))
                    .collect::<Result<Vec<_>>>()?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn seal_external_radix(
    spec: &PartitionAggregateWindowSpec,
    spills: &[AggregateSpilledPayload],
    allocator: Arc<dyn Allocator>,
    table_memory: MemoryAccountingContext,
    index_memory: MemoryAccountingContext,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    cancel: &paro_context::StatementCancellation,
) -> Result<PartitionAggregateSnapshot> {
    let Some(first) = spills.first() else {
        return Ok(PartitionAggregateSnapshot::External {
            outputs: Mutex::new(Vec::new()),
            spilled_bytes: 0,
            repartition_depth: 1,
        });
    };
    let partition_count = first.partition_count();
    if spills
        .iter()
        .any(|spill| spill.partition_count() != partition_count)
    {
        return Err(paro_error::internal(
            "partition aggregate raw payload partition count mismatch",
        ));
    }

    let aggregate_objects = aggregate_objects(&spec.aggregate)?;
    let group_refs = group_payload_refs(&spec.aggregate)?;
    let grouping_sets = normalized_grouping_sets(&spec.aggregate)?
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let format = PartitionAggregateOutputFormat::new(&spec.output_types);
    let mut outputs = Vec::with_capacity(partition_count);
    let mut addresses = Vector::try_new(
        paro_common::types::LogicalType::BigInt,
        VECTOR_SIZE,
        allocator.clone(),
    )?;
    let mut new_groups = SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?;
    let spill_memory = table_memory.with_class(paro_common::memory::MemoryAccountingClass::Spill);

    for partition_idx in 0..partition_count {
        cancel.check()?;
        let mut tables = create_hash_aggregate_tables(
            &spec.aggregate,
            allocator.clone(),
            table_memory.clone(),
            1,
        )?;
        for spill in spills {
            spill.replay_partition_payloads(partition_idx, allocator.clone(), |payload| {
                cancel.check()?;
                let groups = build_groups_chunk(payload, &group_refs)?;
                update_hash_aggregate_tables(
                    &spec.aggregate,
                    &aggregate_objects,
                    payload,
                    &groups,
                    &grouping_sets,
                    &mut tables,
                    &mut addresses,
                    &mut new_groups,
                )
            })?;
        }
        if tables.len() != 1 {
            return Err(paro_error::internal(
                "partition aggregate external replay requires one grouping table",
            ));
        }
        let mut table = tables.pop().expect("verified single grouping table");
        let index =
            finalize_partition_index(spec, &mut table, allocator.clone(), index_memory.clone())?;
        let mut writer = RowStoreSpillWriter::new(
            Arc::clone(&buffer_pool),
            format.clone(),
            paro_storage::buffer::MemoryTag::Window,
            spill_memory.clone(),
        );
        for spill in spills {
            spill.replay_partition_payloads(partition_idx, allocator.clone(), |payload| {
                cancel.check()?;
                append_external_output(
                    spec,
                    payload,
                    &group_refs,
                    &index,
                    &mut writer,
                    allocator.clone(),
                )
            })?;
        }
        table.destroy()?;
        outputs.push(writer.finish()?);
    }
    Ok(PartitionAggregateSnapshot::External {
        outputs: Mutex::new(outputs.into_iter().map(Some).collect()),
        spilled_bytes: 0,
        repartition_depth: 1,
    })
}

fn finalize_partition_index(
    spec: &PartitionAggregateWindowSpec,
    table: &mut AggregateHashTable,
    allocator: Arc<dyn Allocator>,
    memory: MemoryAccountingContext,
) -> Result<FinalizedPartitionIndex> {
    let result_types = table.scan_output_types();
    let mut position = AggregateHTScanPosition::default();
    let mut chunks = Vec::new();
    loop {
        let mut output = Chunk::try_initialize(&result_types, VECTOR_SIZE, allocator.clone())?;
        if !table.scan(&mut position, &mut output)? {
            break;
        }
        chunks.push(output);
    }
    FinalizedPartitionIndex::try_new(
        spec.aggregate.groups[0].return_type(),
        spec.aggregate_column_count(),
        chunks,
        memory,
    )
}

fn append_external_output(
    spec: &PartitionAggregateWindowSpec,
    payload: &Chunk,
    group_refs: &[usize],
    index: &FinalizedPartitionIndex,
    writer: &mut RowStoreSpillWriter<PartitionAggregateOutputFormat>,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let keys = build_groups_chunk(payload, group_refs)?;
    let mut selection =
        SelectionVector::try_with_capacity(payload.size().max(1), allocator.clone())?;
    selection.set_len(payload.size());
    for row in 0..payload.size() {
        selection.try_set(row, index.lookup(&keys, row)?.0 as usize)?;
    }
    let child_count = index
        .aggregate_columns()
        .first()
        .map_or(0, |column| column.len());
    let validated = ValidatedVectorSelection::try_new(selection, child_count)?;
    let mut columns = Vec::with_capacity(spec.output_types.len());
    for &source in &spec.detail_columns {
        columns.push(Arc::clone(payload.column(source).ok_or_else(|| {
            paro_error::internal("partition aggregate external detail column is missing")
        })?));
    }
    for aggregate in index.aggregate_columns() {
        columns.push(Arc::new(Vector::try_dictionary_from_validated(
            Arc::clone(aggregate),
            validated.clone(),
        )?));
    }
    let output = Chunk::try_from_arc_vectors_with_cardinality(columns, payload.size(), allocator)?;
    writer.append_chunk(&output)?;
    Ok(())
}

impl RuntimeCleanup for PartitionAggregateWindowHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&PartitionAggregatePendingSpillReclaimer::name_for(self));
        self.pending.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryReleaseHandle};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::StatementCancellation;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_planner::expression::{AggregateExpression, Expression, ReferenceExpression};
    use tokio_util::sync::CancellationToken;

    use crate::memory_runtime::{QueryMemoryPool, RetainedMemoryHandle};
    use crate::operators::aggregate::group_hash::hash_group_columns;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::physical::specs::{AggregateSpec, GroupKeyEncoding};
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};

    fn reference(index: usize, ty: LogicalType) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, ty))
    }

    fn test_spec() -> PartitionAggregateWindowSpec {
        let aggregate = AggregateSpec {
            grouping_key_count: 1,
            state_output_projection: Box::new([]),
            estimated_input_rows: None,
            projection_exprs: Box::new([
                reference(0, LogicalType::Integer),
                reference(1, LogicalType::Integer),
            ]),
            payload_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
            groups: Box::new([reference(0, LogicalType::Integer)]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            post_reduction: None,
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "count".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        };
        PartitionAggregateWindowSpec {
            input_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
            detail_columns: Box::new([0, 1]),
            aggregate,
            output_names: Box::new(["k".to_string(), "detail".to_string(), "count".to_string()]),
            output_types: Box::new([
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::BigInt,
            ]),
        }
    }

    fn payload(rows: &[(i32, i32)], allocator: Arc<dyn Allocator>) -> Chunk {
        let mut payload = Chunk::try_initialize(
            &[LogicalType::Integer, LogicalType::Integer],
            rows.len(),
            allocator,
        )
        .expect("payload");
        payload.set_cardinality(rows.len());
        for (row, &(key, detail)) in rows.iter().enumerate() {
            payload
                .column_mut(0)
                .expect("key")
                .set_value(row, &Value::Integer(key));
            payload
                .column_mut(1)
                .expect("detail")
                .set_value(row, &Value::Integer(detail));
        }
        payload
    }

    fn handle(spec: &PartitionAggregateWindowSpec) -> PartitionAggregateWindowHandle {
        PartitionAggregateWindowHandle::new(BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::PartitionAggregateWindow,
            row_type: RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec()),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        })
    }

    fn retained_payload_bytes(bytes: usize) -> Arc<RetainedMemoryHandle> {
        Arc::new(RetainedMemoryHandle::new(MemoryReleaseHandle::new(
            None,
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::Window,
            MemoryAccountingClass::Revocable,
            bytes,
        )))
    }

    #[test]
    fn pending_reclaimer_satisfies_target_across_multiple_locals() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(paro_storage::buffer::BufferPool::new(16 * 1024 * 1024));
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let spec = test_spec();
        let handle = handle(&spec);
        for rows in [[(1, 10)], [(2, 20)]] {
            let payload = payload(&rows, allocator.clone());
            handle
                .append_local_with(|| {
                    Ok(PartitionAggregateLocalOutput::Columnar {
                        payloads: vec![payload],
                        tables: Vec::new(),
                        payload_memory: retained_payload_bytes(1),
                    })
                })
                .expect("append pending local");
        }

        let stats = handle
            .reclaim_pending(2, &spec, buffer_pool, aggregate_spill_radix_bits(1), memory)
            .expect("reclaim pending locals");
        assert_eq!(stats.reclaimed_bytes, 2);
        assert!(stats.spilled_bytes > 0);
        assert!(handle
            .pending
            .lock()
            .iter()
            .all(|local| matches!(local, PartitionAggregateLocalOutput::External(_))));
    }

    #[test]
    fn in_memory_seal_resource_failure_replays_retained_payload_externally() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(paro_storage::buffer::BufferPool::new(32 * 1024 * 1024));
        let detached = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let spec = test_spec();
        let aggregate_objects = aggregate_objects(&spec.aggregate).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec.aggregate).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec.aggregate)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>();
        let mut tables =
            create_hash_aggregate_tables(&spec.aggregate, allocator.clone(), detached, 1)
                .expect("table");
        let mut payloads = Vec::new();
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        for start in (0..20_000).step_by(VECTOR_SIZE) {
            let count = (20_000 - start).min(VECTOR_SIZE);
            let rows = (start..start + count)
                .map(|value| (value as i32, value as i32))
                .collect::<Vec<_>>();
            let payload = payload(&rows, allocator.clone());
            let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
            update_hash_aggregate_tables(
                &spec.aggregate,
                &aggregate_objects,
                &payload,
                &groups,
                &grouping_sets,
                &mut tables,
                &mut addresses,
                &mut new_groups,
            )
            .expect("update table");
            payloads.push(payload);
        }

        let handle = handle(&spec);
        handle
            .append_local_with(|| {
                Ok(PartitionAggregateLocalOutput::Columnar {
                    payloads,
                    tables,
                    payload_memory: retained_payload_bytes(0),
                })
            })
            .expect("append local");
        let pool = Arc::new(QueryMemoryPool::new(96 * 1024));
        let owner: Arc<dyn paro_common::memory::MemoryOwner> = pool;
        let table_memory = MemoryAccountingContext::from_owner(
            Arc::clone(&owner),
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let index_memory = MemoryAccountingContext::from_owner(
            owner,
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::Window,
            MemoryAccountingClass::NonRevocable,
        );
        let cancellation = StatementCancellation::new(CancellationToken::new(), None);
        handle
            .seal(
                &spec,
                allocator,
                table_memory,
                index_memory,
                buffer_pool,
                16,
                &cancellation,
            )
            .expect("external fallback");
        let snapshot = handle.snapshot().expect("snapshot");
        assert!(snapshot.is_external());
        assert!(snapshot.work_count() >= 16);
        let output_rows = (0..snapshot.work_count())
            .filter_map(|index| snapshot.take_output(index))
            .map(|store| store.count() as usize)
            .sum::<usize>();
        assert_eq!(output_rows, 20_000);
    }

    #[test]
    fn mixed_columnar_and_external_locals_replay_one_global_radix_domain() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(paro_storage::buffer::BufferPool::new(16 * 1024 * 1024));
        let table_memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let index_memory = MemoryAccountingContext::detached(
            MemoryTag::Window,
            MemoryAccountingClass::NonRevocable,
        );
        let spec = test_spec();
        let aggregate_objects = aggregate_objects(&spec.aggregate).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec.aggregate).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec.aggregate)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>();

        let columnar_payload = payload(&[(1, 10), (1, 20)], allocator.clone());
        let groups = build_groups_chunk(&columnar_payload, &group_refs).expect("groups");
        let mut tables = create_hash_aggregate_tables(
            &spec.aggregate,
            allocator.clone(),
            table_memory.clone(),
            1,
        )
        .expect("tables");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        update_hash_aggregate_tables(
            &spec.aggregate,
            &aggregate_objects,
            &columnar_payload,
            &groups,
            &grouping_sets,
            &mut tables,
            &mut addresses,
            &mut new_groups,
        )
        .expect("update columnar local");
        let payload_memory = retained_payload_bytes(0);

        let external_payload = payload(&[(1, 30), (2, 40)], allocator.clone());
        let external_groups =
            build_groups_chunk(&external_payload, &group_refs).expect("external groups");
        let hashes = hash_group_columns(&external_groups).expect("external hashes");
        let mut spill = AggregatePayloadSpillBuffer::new(
            Arc::clone(&buffer_pool),
            spec.aggregate.payload_types.iter().cloned(),
            aggregate_spill_radix_bits(1),
            table_memory.clone(),
        )
        .expect("spill");
        spill
            .append_payload(&external_payload, &hashes)
            .expect("append external payload");

        let handle = handle(&spec);
        handle
            .append_local_with(|| {
                Ok(PartitionAggregateLocalOutput::Columnar {
                    payloads: vec![columnar_payload],
                    tables,
                    payload_memory,
                })
            })
            .expect("append columnar local");
        handle
            .append_local_with(|| Ok(PartitionAggregateLocalOutput::External(spill.seal())))
            .expect("append external local");
        let cancellation = StatementCancellation::new(CancellationToken::new(), None);
        handle
            .seal(
                &spec,
                allocator.clone(),
                table_memory,
                index_memory,
                buffer_pool,
                1,
                &cancellation,
            )
            .expect("seal mixed locals");

        let snapshot = handle.snapshot().expect("snapshot");
        assert!(snapshot.is_external());
        let mut rows = Vec::new();
        for output_idx in 0..snapshot.work_count() {
            let Some(store) = snapshot.take_output(output_idx) else {
                continue;
            };
            let mut scanner = store.into_reclaimable().into_reclaiming_scanner();
            let mut output =
                Chunk::try_initialize(&spec.output_types, VECTOR_SIZE, allocator.clone())
                    .expect("output");
            while scanner.next_chunk(&mut output).expect("scan output") > 0 {
                rows.extend((0..output.size()).map(|row| {
                    (
                        output.column(0).unwrap().get_i32(row).unwrap(),
                        output.column(1).unwrap().get_i32(row).unwrap(),
                        output.column(2).unwrap().get_i64(row).unwrap(),
                    )
                }));
            }
        }
        rows.sort_unstable_by_key(|row| row.1);
        assert_eq!(rows, vec![(1, 10, 3), (1, 20, 3), (1, 30, 3), (2, 40, 1)]);
    }
}
