// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;
use paro_storage::row::RowSpillReader;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::group_key_codec::{decode_group_columns, has_encoded_group_keys};
use crate::operators::aggregate::output_filter::copy_selected_rows;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHTScanPosition, AggregateHashTable,
};
use crate::operators::output::ensure_source_output;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateRuntimeState, HandleRef,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext, QueryRuntimeContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    HashAggregateEmitSourceGlobal, HashAggregateEmitSourceLocal, HashAggregateEmitWork,
    SourceGlobal, SourceLocal,
};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct HashAggregateEmitSourceExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl HashAggregateEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let global = Arc::new(HashAggregateEmitSourceGlobal {
            handle,
            work: parking_lot::Mutex::new(None),
            work_count: AtomicUsize::new(0),
        });
        if global.handle.is_finalized() {
            initialize_work(ctx.query, &global)?;
        }
        Ok(SourceGlobal::HashAggregateEmit(global))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        let mut local = HashAggregateEmitSourceLocal::default();
        if !self.spec.having_filter.is_empty() {
            if self.spec.having_filter.len() != 1 {
                return Err(paro_error::internal(
                    "aggregate HAVING lowering requires one normalized predicate",
                ));
            }
            local.having_executor = Some(ExpressionExecutor::with_expressions_for_session(
                &self.spec.having_filter,
                ctx.query.session.as_ref(),
            ));
            local.having_selection = Some(paro_common::vector::SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query
                    .allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?);
            local.having_columns = (self.spec.grouping_key_count
                ..self.spec.grouping_key_count + self.spec.aggregates.len())
                .collect();
        }
        Ok(SourceLocal::HashAggregateEmit(local))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::HashAggregateEmit(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate emit source global state mismatch",
            ));
        };
        let SourceLocal::HashAggregateEmit(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate emit source local state mismatch",
            ));
        };
        initialize_work(ctx.query, global)?;
        ensure_source_output(output, &self.spec.output_types, VECTOR_SIZE)?;
        loop {
            if local.work.is_none() {
                local.work = global.claim_work();
                local.position = Default::default();
                if local.work.is_none() {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                }
            }
            let Some(work) = local.work.as_mut() else {
                return Err(paro_error::internal(
                    "hash aggregate emit source failed to claim initialized work",
                ));
            };
            match work {
                HashAggregateEmitWork::Table {
                    grouping_idx,
                    table,
                } => {
                    let encoded_groups = has_encoded_group_keys(&self.spec);
                    let produced = if encoded_groups {
                        let scan_types = table.scan_output_types();
                        let scratch = local
                            .scan_chunk
                            .get_or_insert(Chunk::try_new(output.allocator().clone())?);
                        ensure_source_output(scratch, &scan_types, VECTOR_SIZE)?;
                        let produced = scan_table_batch(
                            table,
                            &mut local.position,
                            scratch,
                            local.having_executor.as_mut(),
                            local.having_selection.as_mut(),
                            ctx.query,
                        )?;
                        if produced {
                            decode_aggregate_output(&self.spec, scratch, output)?;
                        }
                        produced
                    } else {
                        scan_table_batch(
                            table,
                            &mut local.position,
                            output,
                            local.having_executor.as_mut(),
                            local.having_selection.as_mut(),
                            ctx.query,
                        )?
                    };
                    if produced {
                        populate_grouping_columns(&self.spec, output, *grouping_idx)?;
                        return Ok(SourcePoll::Output);
                    }
                }
                HashAggregateEmitWork::Spilled {
                    grouping_idx,
                    reader,
                } => {
                    let scratch = local
                        .spilled_chunk
                        .get_or_insert(Chunk::try_new(output.allocator().clone())?);
                    let scanned = reader.read_next(scratch)?;
                    if scanned > 0 {
                        if let (Some(executor), Some(selection)) = (
                            local.having_executor.as_mut(),
                            local.having_selection.as_mut(),
                        ) {
                            let aggregate_types =
                                &self.spec.output_types[self.spec.grouping_key_count
                                    ..self.spec.grouping_key_count + self.spec.aggregates.len()];
                            let mut aggregate_view = Chunk::try_init_empty(
                                aggregate_types,
                                scratch.allocator().clone(),
                            )?;
                            aggregate_view.reference_columns(scratch, &local.having_columns);
                            let selected_count = executor.select_kernel(
                                0,
                                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                                    params: ctx.query.params.as_ref(),
                                    columns: &aggregate_view,
                                })
                                .with_count(scanned),
                                ctx.query,
                                selection,
                            )?;
                            if selected_count == 0 {
                                continue;
                            }
                            if has_encoded_group_keys(&self.spec) {
                                let filtered = local
                                    .scan_chunk
                                    .get_or_insert(Chunk::try_new(output.allocator().clone())?);
                                ensure_source_output(filtered, &scratch.types(), VECTOR_SIZE)?;
                                copy_selected_rows(scratch, filtered, selection, selected_count)?;
                                decode_aggregate_output(&self.spec, filtered, output)?;
                            } else {
                                copy_selected_rows(scratch, output, selection, selected_count)?;
                            }
                        } else if has_encoded_group_keys(&self.spec) {
                            decode_aggregate_output(&self.spec, scratch, output)?;
                        } else {
                            copy_spilled_output_rows(scratch, output)?;
                        }
                        populate_grouping_columns(&self.spec, output, *grouping_idx)?;
                        return Ok(SourcePoll::Output);
                    }
                }
            }
            local.work = None;
        }
    }
}

fn scan_table_batch(
    table: &mut AggregateHashTable,
    position: &mut AggregateHTScanPosition,
    output: &mut Chunk,
    having_executor: Option<&mut ExpressionExecutor>,
    having_selection: Option<&mut SelectionVector>,
    query: &QueryRuntimeContext,
) -> Result<bool> {
    match (having_executor, having_selection) {
        (Some(executor), Some(selection)) => table.scan_with_aggregate_filter(
            position,
            output,
            selection,
            |aggregates, count, selection| {
                executor.select_kernel(
                    0,
                    VectorKernelInput::from_eval_input(ExpressionEvalInput {
                        params: query.params.as_ref(),
                        columns: aggregates,
                    })
                    .with_count(count),
                    query,
                    selection,
                )
            },
        ),
        (None, None) => table.scan(position, output),
        _ => Err(paro_error::internal(
            "aggregate HAVING executor and selection state disagree",
        )),
    }
}

fn decode_aggregate_output(spec: &AggregateSpec, source: &Chunk, output: &mut Chunk) -> Result<()> {
    decode_group_columns(spec, source, output)?;
    let group_count = spec.grouping_key_count;
    if source.column_count() < group_count + spec.aggregates.len() {
        return Err(paro_error::internal(format!(
            "encoded aggregate output is too narrow: groups={group_count}, aggregates={}, columns={}",
            spec.aggregates.len(),
            source.column_count()
        )));
    }
    for aggregate_idx in 0..spec.aggregates.len() {
        let column_idx = group_count + aggregate_idx;
        let source_column = source.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "encoded aggregate source column not found: index={column_idx}"
            ))
        })?;
        let output_column = output.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "decoded aggregate output column not found: index={column_idx}"
            ))
        })?;
        if output_column.logical_type() != source_column.logical_type() {
            return Err(paro_error::internal(format!(
                "decoded aggregate output type mismatch at index {column_idx}: expected={:?}, actual={:?}",
                output_column.logical_type(),
                source_column.logical_type()
            )));
        }
        output.data[column_idx] = Arc::clone(source_column);
    }
    output.try_set_cardinality(source.size())
}

fn initialize_work(
    query: &QueryRuntimeContext,
    global: &HashAggregateEmitSourceGlobal,
) -> Result<()> {
    let mut shared_work = global.work.lock();
    if shared_work.is_some() {
        return Ok(());
    }
    if !global.handle.is_finalized() {
        return Err(paro_error::internal(
            "hash aggregate emit source polled before handle was finalized",
        ));
    }
    query
        .memory
        .unregister_reclaimer_by_name(&AggregateBuildCompactionReclaimer::name_for(&global.handle));
    query
        .memory
        .unregister_reclaimer_by_name(&AggregateFinalizedStateReclaimer::name_for(&global.handle));

    let mut work = std::collections::VecDeque::new();
    if let Some(state) = global.handle.take_state()? {
        let AggregateRuntimeState::Hash(state) = state else {
            return Err(paro_error::internal(
                "aggregate handle does not contain hash aggregate state",
            ));
        };
        if let Some(spilled_outputs) = state.spilled_outputs {
            for (grouping_idx, output) in spilled_outputs.into_iter().enumerate() {
                if let Some(output) = output {
                    work.push_back(HashAggregateEmitWork::Spilled {
                        grouping_idx,
                        reader: output.into_reader(),
                    });
                }
            }
        } else {
            for (grouping_idx, table) in state.tables.into_iter().enumerate() {
                for table in table.into_scan_partitions() {
                    work.push_back(HashAggregateEmitWork::Table {
                        grouping_idx,
                        table,
                    });
                }
            }
        }
    }
    global.work_count.store(work.len(), Ordering::Release);
    *shared_work = Some(work);
    Ok(())
}

fn copy_spilled_output_rows(source: &Chunk, output: &mut Chunk) -> Result<()> {
    let row_count = source.size();
    if output.column_count() < source.column_count() {
        return Err(paro_error::internal(format!(
            "hash aggregate spilled output has more columns than source output: spilled={} output={}",
            source.column_count(),
            output.column_count()
        )));
    }
    output.try_set_cardinality(row_count)?;
    for col_idx in 0..source.column_count() {
        let source_vector = source.column(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing spilled aggregate output source column {col_idx}"
            ))
        })?;
        let target = output.column_mut(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing spilled aggregate output target column {col_idx}"
            ))
        })?;
        target.try_copy_range(0, source_vector.as_ref(), 0, row_count)?;
    }
    Ok(())
}

pub(crate) fn populate_grouping_columns(
    spec: &AggregateSpec,
    chunk: &mut Chunk,
    grouping_idx: usize,
) -> Result<()> {
    if spec.grouping_functions.is_empty() || chunk.is_empty() {
        return Ok(());
    }
    let grouping_set = spec.grouping_sets.get(grouping_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "grouping set index out of bounds while populating GROUPING(): grouping_idx={grouping_idx}"
        ))
    })?;
    let grouping_offset = spec.grouping_key_count + spec.aggregates.len();
    let row_count = chunk.size();
    for (func_idx, grouping_fn) in spec.grouping_functions.iter().enumerate() {
        let output_idx = grouping_offset + func_idx;
        let grouping_col = chunk.column_mut(output_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing GROUPING() output column at index {output_idx}"
            ))
        })?;
        let value = Value::BigInt(grouping_value(grouping_set, grouping_fn));
        for row_idx in 0..row_count {
            grouping_col.set_value(row_idx, &value);
        }
    }
    Ok(())
}

fn grouping_value(grouping_set: &[usize], grouping_function: &[usize]) -> i64 {
    let mut value = 0i64;
    for (arg_idx, &group_idx) in grouping_function.iter().enumerate() {
        if !grouping_set.contains(&group_idx) {
            let bit = (grouping_function.len() - 1 - arg_idx) as i64;
            value |= 1_i64 << bit;
        }
    }
    value
}
