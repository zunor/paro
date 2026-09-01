// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! External grouping-set domains.
//!
//! Every grouping set owns an independent hash domain: absent keys are NULL
//! before hashing, and one domain is replayed to completion before the next.
//! This prevents logically identical groups from being split by columns that
//! the grouping set omits and keeps the live aggregate state bounded.

use super::*;

pub(super) fn append_payload_to_local_spills(
    ctx: &mut OperatorCallContext,
    payload: &Chunk,
    all_groups: &Chunk,
    grouping_sets: &[Box<[usize]>],
    payload_spills: &mut [Option<AggregatePayloadSpillBuffer>],
) -> Result<()> {
    if grouping_sets.len() != payload_spills.len() {
        return Err(paro_error::internal(format!(
            "aggregate payload spill domain mismatch: grouping_sets={} spills={}",
            grouping_sets.len(),
            payload_spills.len()
        )));
    }
    for (grouping_set, payload_spill) in grouping_sets.iter().zip(payload_spills.iter_mut()) {
        let groups =
            build_groups_chunk_for_set(all_groups, grouping_set, all_groups.column_count())?;
        let hashes = hash_group_columns(&groups)?;
        if payload_spill.is_none() {
            *payload_spill = Some(AggregatePayloadSpillBuffer::new(
                ctx.query.session.buffer_pool().clone(),
                payload.types(),
                aggregate_spill_radix_bits(ctx.query.session.number_of_threads()),
                query_hash_table_memory(ctx.query),
            )?);
        }
        payload_spill
            .as_mut()
            .expect("aggregate payload spill initialized above")
            .append_payload(payload, &hashes)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spill_grouping_set_payloads_to_outputs(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    state: &mut HashAggregateRuntimeState,
    spilled_payloads: &[crate::operators::aggregate::payload_spill::AggregateSpilledPayload],
    spilled_states: &[crate::operators::aggregate::payload_spill::AggregateSpilledState],
    mut post_reducer: Option<&mut PostAggregateReducer>,
) -> Result<usize> {
    // Multi-domain aggregates enter raw spill before accepting their first
    // row. There is therefore no partially built table or state stream to
    // reconcile with differently hashed payload domains.
    if !spilled_states.is_empty()
        || state.tables.iter().any(|table| table.count() > 0)
        || !state.pending_radix_merges.is_empty()
    {
        return Err(paro_error::internal(
            "grouping-set external aggregate mixed raw payload with in-memory state",
        ));
    }
    if spilled_payloads
        .iter()
        .any(|payload| payload.grouping_idx() >= grouping_sets.len())
    {
        return Err(paro_error::internal(
            "grouping-set payload references an unknown grouping domain",
        ));
    }

    for table in &mut state.tables {
        table.destroy()?;
    }
    state.tables.clear();

    let mut outputs = (0..grouping_sets.len())
        .map(|_| None)
        .collect::<Vec<Option<AggregateSpilledOutput>>>();
    let mut output_bytes = 0usize;
    let mut addresses = Vector::try_new(
        LogicalType::BigInt,
        VECTOR_SIZE,
        ctx.query.allocator(MemoryTag::HashTable),
    )?;
    let mut new_groups =
        SelectionVector::try_with_capacity(VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;
    let mut group_key_encoder =
        GroupKeyEncoder::try_new(spec, VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;

    for (grouping_idx, grouping_set) in grouping_sets.iter().enumerate() {
        let domain_payloads = spilled_payloads
            .iter()
            .filter(|payload| payload.grouping_idx() == grouping_idx)
            .collect::<Vec<_>>();
        let Some(first_payload) = domain_payloads.first() else {
            continue;
        };
        let partition_count = first_payload.partition_count();
        if domain_payloads
            .iter()
            .any(|payload| payload.partition_count() != partition_count)
        {
            return Err(paro_error::internal(format!(
                "grouping-set payload partition mismatch in domain {grouping_idx}"
            )));
        }

        let mut domain_spec = spec.clone();
        domain_spec.grouping_sets = vec![grouping_set.clone()].into_boxed_slice();
        let domain_grouping_sets = [grouping_set.clone()];
        let mut domain_writers = None;
        for partition_idx in 0..partition_count {
            let mut partition_tables = create_hash_aggregate_tables(
                &domain_spec,
                ctx.query.allocator(MemoryTag::HashTable),
                query_hash_table_memory(ctx.query),
                ctx.query.session.number_of_threads(),
            )?;
            for spilled_payload in &domain_payloads {
                spilled_payload.replay_partition_payloads(
                    partition_idx,
                    ctx.query.allocator(MemoryTag::HashTable),
                    |payload_batch| {
                        let groups = group_key_encoder.encode_payload(payload_batch, group_refs)?;
                        update_hash_aggregate_tables(
                            &domain_spec,
                            aggregate_objects,
                            payload_batch,
                            groups,
                            &domain_grouping_sets,
                            &mut partition_tables,
                            &mut addresses,
                            &mut new_groups,
                        )
                    },
                )?;
            }
            append_partition_tables_to_output_writers(
                &mut domain_writers,
                &mut partition_tables,
                ctx.query.session.buffer_pool().clone(),
                query_hash_table_memory(ctx.query),
                post_reducer.as_deref_mut(),
            )?;
        }
        let mut domain_outputs = finish_output_spill_writers(domain_writers.unwrap_or_default())?;
        if domain_outputs.len() != 1 {
            return Err(paro_error::internal(format!(
                "grouping-set external aggregate produced {} outputs for domain {grouping_idx}",
                domain_outputs.len()
            )));
        }
        let output = domain_outputs.pop().flatten();
        output_bytes = output_bytes.saturating_add(
            output
                .as_ref()
                .map_or(0, AggregateSpilledOutput::size_in_bytes),
        );
        outputs[grouping_idx] = output;
    }
    state.spilled_outputs = Some(outputs);
    Ok(output_bytes)
}
