// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, ErrorClass, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
use paro_common::runtime_value::Value;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::FunctionExecContext;
use paro_storage::row::{RowSpillWriter, RowStoreSpillWriter};

use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::RetainedChunkVec;
use crate::memory_runtime::{ReclaimStats, Reclaimer, SpillCost};
use crate::operators::aggregate::aggregate_kernel::finalize_states;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, build_groups_chunk, combine_ungrouped_states, create_hash_aggregate_tables,
    create_ungrouped_runtime_state, destroy_ungrouped_local, group_payload_refs,
    normalized_grouping_sets, projected_payload_chunk, query_modifier_memory,
    update_hash_aggregate_tables_with_scratch,
};
use crate::operators::aggregate::group_hash::GroupHashScratch;
use crate::operators::aggregate::payload_spill::{
    aggregate_spill_radix_bits, AggregatePayloadSpillBuffer,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::operators::aggregate::ungrouped::build::{
    consume_ungrouped_sink_local, create_ungrouped_sink_local,
};
use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::properties::MemoryClass;
use crate::physical::specs::{PartitionAggregateDomain, PartitionAggregateWindowSpec};
use crate::runtime::breaker::{
    single_state_addresses, HandleRef, PartitionAggregatePendingSpillReclaimer,
    PartitionAggregateWindowHandle,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{SinkGlobal, SinkLocal};

use super::state::{
    build_global, build_local_mut, global_build_local_mut, GlobalAggregateBuildState,
    GlobalAggregatePayloadBacking, GlobalAggregateWindowBuildLocal, PartitionAggregateBuildGlobal,
    PartitionAggregateBuildLocal, PartitionAggregateLocalBacking,
};
use super::{
    GlobalPartitionAggregateDetailFormat, PartitionAggregateLocalOutput, PartitionAggregateSnapshot,
};

const PARTITION_AGGREGATE_PREEMPTIVE_SPILL_CAP_PER_THREAD: usize =
    paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE * 4;
static NEXT_PARTITION_AGGREGATE_LOCAL_ID: AtomicU64 = AtomicU64::new(1);

fn global_detail_types(
    spec: &PartitionAggregateWindowSpec,
) -> Result<Box<[paro_common::types::LogicalType]>> {
    spec.detail_columns
        .iter()
        .map(|&index| {
            spec.input_types.get(index).cloned().ok_or_else(|| {
                paro_error::internal("global aggregate detail column is out of bounds")
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn global_detail_chunk(spec: &PartitionAggregateWindowSpec, input: &Chunk) -> Result<Chunk> {
    let mut detail = Chunk::from_arc_vectors(
        spec.detail_columns
            .iter()
            .map(|&index| {
                input.column(index).cloned().ok_or_else(|| {
                    paro_error::internal("global aggregate detail column is missing")
                })
            })
            .collect::<Result<Vec<_>>>()?,
        input.allocator().clone(),
    );
    detail.try_set_cardinality(input.size())?;
    Ok(detail)
}

fn global_detail_spill_writer(
    spec: &PartitionAggregateWindowSpec,
    ctx: &crate::runtime::context::QueryRuntimeContext,
) -> Result<RowStoreSpillWriter<GlobalPartitionAggregateDetailFormat>> {
    let types = global_detail_types(spec)?;
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = ctx.memory.clone();
    Ok(RowStoreSpillWriter::new(
        ctx.session.buffer_pool().clone(),
        GlobalPartitionAggregateDetailFormat::new(&types),
        MemoryTag::Window,
        MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::Window,
            MemoryAccountingClass::Spill,
        ),
    ))
}

fn externalize_global_payloads(
    spec: &PartitionAggregateWindowSpec,
    payloads: &RetainedChunkVec,
    current: Option<&Chunk>,
    ctx: &crate::runtime::context::QueryRuntimeContext,
) -> Result<RowStoreSpillWriter<GlobalPartitionAggregateDetailFormat>> {
    let mut writer = global_detail_spill_writer(spec, ctx)?;
    for payload in payloads.iter().chain(current) {
        writer.append_chunk(&global_detail_chunk(spec, payload)?)?;
    }
    Ok(writer)
}

#[derive(Debug)]
struct PartitionAggregateLocalSpillReclaimer {
    name: String,
    backing: Arc<parking_lot::Mutex<PartitionAggregateLocalBacking>>,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    payload_types: Box<[paro_common::types::LogicalType]>,
    group_refs: Box<[usize]>,
    radix_bits: usize,
    memory: MemoryAccountingContext,
}

impl PartitionAggregateLocalSpillReclaimer {
    fn name_for(handle: &PartitionAggregateWindowHandle, local_id: u64) -> String {
        format!(
            "partition_aggregate_local_spill:{}:{local_id}",
            handle.metadata().id.index()
        )
    }

    fn externalize(&self, payloads: &RetainedChunkVec) -> Result<AggregatePayloadSpillBuffer> {
        externalize_payloads(
            payloads,
            None,
            Arc::clone(&self.buffer_pool),
            &self.payload_types,
            &self.group_refs,
            self.radix_bits,
            self.memory.clone(),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn externalize_payloads(
    payloads: &RetainedChunkVec,
    current: Option<&Chunk>,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    payload_types: &[paro_common::types::LogicalType],
    group_refs: &[usize],
    radix_bits: usize,
    memory: MemoryAccountingContext,
) -> Result<AggregatePayloadSpillBuffer> {
    let scratch_allocator = spill_scratch_allocator(&memory, Arc::clone(&buffer_pool));
    let mut spill = AggregatePayloadSpillBuffer::new(
        buffer_pool,
        payload_types.iter().cloned(),
        radix_bits,
        memory,
    )?;
    let mut hash_scratch = GroupHashScratch::try_new(VECTOR_SIZE, scratch_allocator)?;
    for payload in payloads.iter().chain(current) {
        let groups = build_groups_chunk(payload, group_refs)?;
        let hashes = hash_scratch.hash(&groups)?;
        spill.append_payload(payload, &hashes)?;
    }
    Ok(spill)
}

fn spill_scratch_allocator(
    memory: &MemoryAccountingContext,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
) -> Arc<dyn paro_common::allocator::Allocator> {
    let inner: Arc<dyn paro_common::allocator::Allocator> =
        Arc::new(paro_common::allocator::BufferAllocator::new(
            buffer_pool as Arc<dyn paro_common::allocator::BufferManager>,
            MemoryTag::HashTable,
        ));
    match memory.owner() {
        Some(owner) => Arc::new(paro_common::memory::MemoryOwnerAllocator::new(
            inner,
            owner,
            memory.domain(),
            MemoryTag::HashTable,
            MemoryAccountingClass::Spill,
        )),
        None => inner,
    }
}

impl Reclaimer for PartitionAggregateLocalSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.backing
            .try_lock()
            .map_or(0, |backing| backing.reclaimable_bytes())
    }

    fn reclaim_sync(&self, target_bytes: usize) -> paro_common::memory::MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(mut backing) = self.backing.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let PartitionAggregateLocalBacking::Columnar { tables, payloads } = &mut *backing else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let before = payloads.retained_bytes().saturating_add(
            tables
                .iter()
                .map(AggregateHashTable::memory_usage)
                .sum::<usize>(),
        );
        if before == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        // Build the complete replacement first. If any append fails, dropping
        // this unpublished writer removes its temporary blocks and the live
        // columnar state remains untouched.
        let spill = self
            .externalize(payloads)
            .map_err(|error| paro_common::memory::MemoryError::reclaim_failed(error.to_string()))?;
        let spilled_bytes = spill.size_in_bytes();
        let old = std::mem::replace(
            &mut *backing,
            PartitionAggregateLocalBacking::External { spill },
        );
        drop(old);
        Ok(ReclaimStats::new(target_bytes, before, spilled_bytes))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

#[derive(Debug, Clone)]
pub struct PartitionAggregateWindowBuildSinkExec {
    pub handle: HandleRef<PartitionAggregateWindowHandle>,
    pub spec: PartitionAggregateWindowSpec,
}

impl PartitionAggregateWindowBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        self.spec.verify()?;
        if ctx.query.session.limits.force_external && !query_has_temporary_directory(ctx.query) {
            return Err(paro_error::out_of_memory(
                "force_external partition aggregate window requires a temporary directory",
            ));
        }
        let handle = ctx.handles.get(self.handle)?;
        if self.spec.domain == PartitionAggregateDomain::Keyed
            && query_has_temporary_directory(ctx.query)
        {
            ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                PartitionAggregatePendingSpillReclaimer::new(
                    Arc::clone(&handle),
                    self.spec.clone(),
                    ctx.query.session.buffer_pool().clone(),
                    ctx.query.session.number_of_threads(),
                    partition_aggregate_table_memory(ctx.query, true),
                ),
            ));
        }
        let global = if self.spec.domain == PartitionAggregateDomain::Global {
            Some(parking_lot::Mutex::new(GlobalAggregateBuildState {
                accumulator: create_ungrouped_runtime_state(
                    &self.spec.aggregate,
                    ctx.query.allocator(MemoryTag::HashTable),
                    ctx.query.session.buffer_pool().clone(),
                    query_modifier_memory(ctx.query),
                )?,
                payloads: Vec::new(),
                payload_memory: Vec::new(),
                external_payloads: Vec::new(),
            }))
        } else {
            None
        };
        Ok(SinkGlobal::Dyn(Box::new(PartitionAggregateBuildGlobal {
            handle,
            global,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        if self.spec.domain == PartitionAggregateDomain::Global {
            let spillable = query_has_temporary_directory(ctx.query);
            let force_external = ctx.query.session.limits.force_external
                || partition_aggregate_preemptive_spill_enabled(ctx.query);
            let payloads = if spillable && force_external {
                GlobalAggregatePayloadBacking::External(global_detail_spill_writer(
                    &self.spec, ctx.query,
                )?)
            } else {
                GlobalAggregatePayloadBacking::Columnar(RetainedChunkVec::new(
                    MemoryAccountingContext::from_owner(
                        ctx.query.memory.clone(),
                        MemoryDomain::Host,
                        MemoryTag::Window,
                        MemoryAccountingClass::NonRevocable,
                    ),
                ))
            };
            return Ok(SinkLocal::Dyn(Box::new(GlobalAggregateWindowBuildLocal {
                accumulator: create_ungrouped_sink_local(&self.spec.aggregate, ctx)?,
                payloads,
            })));
        }
        let handle = ctx.handles.get(self.handle)?;
        let aggregate_objects = aggregate_objects(&self.spec.aggregate)?;
        let projection_executor = (!self.spec.aggregate.projection_exprs.is_empty()).then(|| {
            ExpressionExecutor::with_expressions_for_session(
                &self.spec.aggregate.projection_exprs,
                ctx.query.session.as_ref(),
            )
        });
        let spillable = query_has_temporary_directory(ctx.query);
        let force_external = ctx.query.session.limits.force_external
            || partition_aggregate_preemptive_spill_enabled(ctx.query);
        let memory = partition_aggregate_table_memory(ctx.query, spillable);
        let radix_bits = aggregate_spill_radix_bits(ctx.query.session.number_of_threads());
        let group_refs = group_payload_refs(&self.spec.aggregate)?.into_boxed_slice();
        let backing = Arc::new(parking_lot::Mutex::new(if force_external && spillable {
            PartitionAggregateLocalBacking::External {
                spill: AggregatePayloadSpillBuffer::new(
                    ctx.query.session.buffer_pool().clone(),
                    self.spec.aggregate.payload_types.iter().cloned(),
                    radix_bits,
                    memory.clone(),
                )?,
            }
        } else {
            PartitionAggregateLocalBacking::Columnar {
                tables: create_hash_aggregate_tables(
                    &self.spec.aggregate,
                    ctx.query.allocator(MemoryTag::HashTable),
                    memory.clone(),
                    1,
                )?,
                payloads: RetainedChunkVec::new(MemoryAccountingContext::from_owner(
                    ctx.query.memory.clone(),
                    MemoryDomain::Host,
                    MemoryTag::Window,
                    if spillable {
                        MemoryAccountingClass::Revocable
                    } else {
                        MemoryAccountingClass::NonRevocable
                    },
                )),
            }
        }));
        let (local_reclaimer_name, query_memory) = if spillable && !force_external {
            let local_id = NEXT_PARTITION_AGGREGATE_LOCAL_ID.fetch_add(1, Ordering::Relaxed);
            let name = PartitionAggregateLocalSpillReclaimer::name_for(&handle, local_id);
            ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                PartitionAggregateLocalSpillReclaimer {
                    name: name.clone(),
                    backing: Arc::clone(&backing),
                    buffer_pool: ctx.query.session.buffer_pool().clone(),
                    payload_types: self.spec.aggregate.payload_types.clone(),
                    group_refs: group_refs.clone(),
                    radix_bits,
                    memory: memory.clone(),
                },
            ));
            (Some(name), Some(Arc::clone(&ctx.query.memory)))
        } else {
            (None, None)
        };
        Ok(SinkLocal::Dyn(Box::new(PartitionAggregateBuildLocal {
            aggregate_objects: Arc::from(aggregate_objects.into_boxed_slice()),
            projection_executor,
            payload_chunk: None,
            group_refs,
            grouping_sets: normalized_grouping_sets(&self.spec.aggregate)?
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            group_hash_scratch: GroupHashScratch::try_new(
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            addresses: Vector::try_new(
                paro_common::types::LogicalType::BigInt,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            new_groups: SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            backing,
            local_reclaimer_name,
            query_memory,
        })))
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
        if self.spec.domain == PartitionAggregateDomain::Global {
            let local = global_build_local_mut(local)?;
            let spillable = query_has_temporary_directory(ctx.query);
            let mut columnar_append = false;
            let transition = match &mut local.payloads {
                GlobalAggregatePayloadBacking::Columnar(payloads) => {
                    if let Err(error) = payloads.push(input.clone_referencing_vectors()) {
                        if !spillable {
                            return Err(error.into());
                        }
                        Some(externalize_global_payloads(
                            &self.spec,
                            payloads,
                            Some(input),
                            ctx.query,
                        )?)
                    } else {
                        columnar_append = true;
                        None
                    }
                }
                GlobalAggregatePayloadBacking::External(writer) => {
                    writer.append_chunk(&global_detail_chunk(&self.spec, input)?)?;
                    None
                }
                GlobalAggregatePayloadBacking::Merged => {
                    return Err(paro_error::internal(
                        "global aggregate window consumed rows after local merge",
                    ));
                }
            };
            if let Some(writer) = transition {
                let old = std::mem::replace(
                    &mut local.payloads,
                    GlobalAggregatePayloadBacking::External(writer),
                );
                drop(old);
            }
            if let Err(error) = consume_ungrouped_sink_local(
                &self.spec.aggregate,
                ctx,
                &mut local.accumulator,
                input,
            ) {
                if columnar_append {
                    let GlobalAggregatePayloadBacking::Columnar(payloads) = &mut local.payloads
                    else {
                        return Err(paro_error::internal(
                            "global aggregate payload backing changed during rollback",
                        ));
                    };
                    let _ = payloads.pop();
                }
                return Err(error);
            }
            return Ok(SinkPoll::NeedMoreInput);
        }
        let local = build_local_mut(local)?;
        let payload: &Chunk = if let Some(executor) = local.projection_executor.as_mut() {
            projected_payload_chunk(
                &self.spec.aggregate,
                executor,
                &mut local.payload_chunk,
                input,
                ctx.query,
            )?
        } else {
            &*input
        };
        let groups = build_groups_chunk(payload, &local.group_refs)?;
        let backing = Arc::clone(&local.backing);
        let mut backing = backing.lock();
        let transition = match &mut *backing {
            PartitionAggregateLocalBacking::Columnar { tables, payloads } => {
                let spillable = query_has_temporary_directory(ctx.query);
                if let Err(error) = payloads.push(payload.clone_referencing_vectors()) {
                    if !spillable {
                        return Err(error.into());
                    }
                    Some(externalize_payloads(
                        payloads,
                        Some(payload),
                        ctx.query.session.buffer_pool().clone(),
                        &self.spec.aggregate.payload_types,
                        &local.group_refs,
                        aggregate_spill_radix_bits(ctx.query.session.number_of_threads()),
                        partition_aggregate_table_memory(ctx.query, true),
                    )?)
                } else if let Err(error) = update_hash_aggregate_tables_with_scratch(
                    &self.spec.aggregate,
                    &local.aggregate_objects,
                    payload,
                    &groups,
                    &local.grouping_sets,
                    tables,
                    &mut local.group_hash_scratch,
                    &mut local.addresses,
                    &mut local.new_groups,
                ) {
                    if error.error_class() != ErrorClass::Resource || !spillable {
                        let _ = payloads.pop();
                        return Err(error);
                    }
                    // A table update can publish a prefix before an allocation
                    // fails. The retained payload is the transaction log:
                    // externalize all rows, then discard the partial table.
                    Some(externalize_payloads(
                        payloads,
                        None,
                        ctx.query.session.buffer_pool().clone(),
                        &self.spec.aggregate.payload_types,
                        &local.group_refs,
                        aggregate_spill_radix_bits(ctx.query.session.number_of_threads()),
                        partition_aggregate_table_memory(ctx.query, true),
                    )?)
                } else {
                    None
                }
            }
            PartitionAggregateLocalBacking::External { spill } => {
                let hashes = local.group_hash_scratch.hash(&groups)?;
                spill.append_payload(payload, &hashes)?;
                None
            }
            PartitionAggregateLocalBacking::Merged => {
                return Err(paro_common::error::internal(
                    "partition aggregate consumed rows after local merge",
                ));
            }
        };
        if let Some(spill) = transition {
            let old = std::mem::replace(
                &mut *backing,
                PartitionAggregateLocalBacking::External { spill },
            );
            drop(old);
        }
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let global = build_global(global)?;
        if self.spec.domain == PartitionAggregateDomain::Global {
            let local = global_build_local_mut(local)?;
            let payloads =
                std::mem::replace(&mut local.payloads, GlobalAggregatePayloadBacking::Merged);
            let aggregate = global.global.as_ref().ok_or_else(|| {
                paro_error::internal("global aggregate window accumulator is missing")
            })?;
            let mut aggregate = aggregate.lock();
            combine_ungrouped_states(&mut aggregate.accumulator, &mut local.accumulator)?;
            destroy_ungrouped_local(&mut local.accumulator)?;
            match payloads {
                GlobalAggregatePayloadBacking::Columnar(mut payloads) => {
                    let (mut payloads, payload_memory) = payloads.drain_chunks_with_handle();
                    aggregate.payloads.append(&mut payloads);
                    aggregate.payload_memory.push(payload_memory);
                }
                GlobalAggregatePayloadBacking::External(writer) => {
                    aggregate.external_payloads.push(writer.finish()?);
                }
                GlobalAggregatePayloadBacking::Merged => {
                    return Err(paro_error::internal(
                        "global aggregate window local was merged more than once",
                    ));
                }
            }
            return Ok(MergePoll::Done);
        }
        let local = build_local_mut(local)?;
        global.handle.append_local_with(|| {
            let backing = std::mem::replace(
                &mut *local.backing.lock(),
                PartitionAggregateLocalBacking::Merged,
            );
            match backing {
                PartitionAggregateLocalBacking::Columnar {
                    mut tables,
                    mut payloads,
                } => {
                    let (payloads, payload_memory) = payloads.drain_chunks_with_handle();
                    Ok(PartitionAggregateLocalOutput::Columnar {
                        payloads,
                        tables: std::mem::take(&mut tables),
                        payload_memory,
                    })
                }
                PartitionAggregateLocalBacking::External { spill } => {
                    Ok(PartitionAggregateLocalOutput::External(spill.seal()))
                }
                PartitionAggregateLocalBacking::Merged => Err(paro_common::error::internal(
                    "partition aggregate local was merged more than once",
                )),
            }
        })?;
        local.unregister_reclaimer();
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
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        if self.spec.domain == PartitionAggregateDomain::Global {
            return Ok(FinishWork::None);
        }
        let handle = Arc::clone(&build_global(global)?.handle);
        let spec = self.spec.clone();
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "partition_aggregate_window_seal",
            MemoryClass::Blocking,
            move |ctx| seal_handle(&handle, &spec, ctx),
        )))
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let global = build_global(global)?;
        let handle = &global.handle;
        if self.spec.domain == PartitionAggregateDomain::Global {
            if !handle.is_sealed() {
                let aggregate = global.global.as_ref().ok_or_else(|| {
                    paro_error::internal("global aggregate window accumulator is missing")
                })?;
                let mut aggregate = aggregate.lock();
                let values = finalize_global_aggregate(
                    &self.spec,
                    &mut aggregate.accumulator,
                    ctx.query.allocator(MemoryTag::Window),
                )?;
                let external_payloads = std::mem::take(&mut aggregate.external_payloads);
                let spilled_bytes = external_payloads
                    .iter()
                    .map(paro_storage::row::RowStore::size_in_bytes)
                    .sum();
                let snapshot = PartitionAggregateSnapshot::Global {
                    payloads: Arc::from(std::mem::take(&mut aggregate.payloads).into_boxed_slice()),
                    external_payloads: parking_lot::Mutex::new(
                        external_payloads.into_iter().map(Some).collect(),
                    ),
                    aggregates: values,
                    _payload_memory: std::mem::take(&mut aggregate.payload_memory)
                        .into_boxed_slice(),
                    spilled_bytes,
                };
                handle.publish_snapshot(snapshot)?;
            }
            record_partition_aggregate_spill(handle, ctx)?;
            return Ok(FinishPoll::Done);
        }
        if !handle.is_sealed() {
            seal_handle(handle, &self.spec, ctx)?;
        }
        ctx.query.memory.unregister_reclaimer_by_name(
            &PartitionAggregatePendingSpillReclaimer::name_for(handle),
        );
        Ok(FinishPoll::Done)
    }
}

fn finalize_global_aggregate(
    spec: &PartitionAggregateWindowSpec,
    state: &mut crate::runtime::breaker::UngroupedAggregateRuntimeState,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Box<[Value]>> {
    let addresses = single_state_addresses(
        state.state_buffer.as_mut_ptr() as *mut u8,
        state.arena_allocator.get_allocator().clone(),
    )?;
    let mut output = Chunk::try_initialize(&spec.aggregate.output_types, 1, allocator)?;
    let mut input_data = AggregateInputData::new(
        None,
        &mut state.arena_allocator,
        AggregateCombineType::PreserveInput,
    );
    finalize_states(
        &state.aggregate_objects,
        &mut input_data,
        &addresses,
        &mut output,
        1,
    )?;
    let values = (0..output.column_count())
        .map(|column| {
            output.get_value(column, 0).ok_or_else(|| {
                paro_error::internal("global aggregate window result column is missing")
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    state.destroy()?;
    Ok(values)
}

fn seal_handle(
    handle: &PartitionAggregateWindowHandle,
    spec: &PartitionAggregateWindowSpec,
    ctx: &mut OperatorFinishContext,
) -> Result<()> {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = ctx.query.memory.clone();
    let index_memory = MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::Window,
        MemoryAccountingClass::NonRevocable,
    );
    handle.seal(
        spec,
        ctx.memory
            .accounted_allocator_for(MemoryTag::Window, MemoryAccountingClass::NonRevocable),
        partition_aggregate_table_memory(ctx.query, query_has_temporary_directory(ctx.query)),
        index_memory,
        ctx.query.session.buffer_pool().clone(),
        ctx.query.session.number_of_threads(),
        ctx.cancel,
    )?;
    record_partition_aggregate_spill(handle, ctx)
}

fn record_partition_aggregate_spill(
    handle: &PartitionAggregateWindowHandle,
    ctx: &mut OperatorFinishContext,
) -> Result<()> {
    if let Some((spilled_bytes, repartition_depth)) = handle.snapshot()?.spill_stats() {
        ctx.profiler.record_runtime(
            ctx.operator.index() as u64,
            ExplainRuntimeStats {
                spilled: Some(true),
                spilled_bytes: (spilled_bytes > 0).then_some(spilled_bytes as u64),
                repartition_depth: Some(repartition_depth as u64),
                ..ExplainRuntimeStats::default()
            },
        );
    }
    Ok(())
}

fn partition_aggregate_table_memory(
    query: &crate::runtime::context::QueryRuntimeContext,
    spillable: bool,
) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        if spillable {
            MemoryAccountingClass::Revocable
        } else {
            MemoryAccountingClass::NonRevocable
        },
    )
}

fn partition_aggregate_preemptive_spill_enabled(
    query: &crate::runtime::context::QueryRuntimeContext,
) -> bool {
    if !query_has_temporary_directory(query) {
        return false;
    }
    let capacity = query.memory.capacity_bytes();
    if capacity >= usize::MAX / 8 {
        return false;
    }
    let threshold = PARTITION_AGGREGATE_PREEMPTIVE_SPILL_CAP_PER_THREAD
        .saturating_mul(query.session.number_of_threads().max(1));
    capacity <= threshold
}
