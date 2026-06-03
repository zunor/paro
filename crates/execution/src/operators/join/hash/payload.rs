// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join build payload projection helpers.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

pub(crate) fn build_payload_chunk_ref<'a>(
    input: &'a Chunk,
    projection: &[usize],
    output_types: &[LogicalType],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    if output_types.is_empty() {
        if slot.as_ref().map_or(true, |payload| {
            payload.column_count() != 0 || payload.capacity() < input.size().max(1)
        }) {
            *slot = Some(Chunk::try_initialize(
                &[],
                input.size().max(1),
                input.allocator().clone(),
            )?);
        }
        let payload = slot
            .as_mut()
            .expect("hash join empty payload chunk was initialized above");
        payload.try_set_cardinality(input.size())?;
        return Ok(payload);
    }

    let projected_len = if projection.is_empty() {
        input.column_count()
    } else {
        projection.len()
    };
    if projected_len != output_types.len() {
        return Err(paro_error::internal(format!(
            "hash join build payload projection has {projected_len} columns but {} types",
            output_types.len()
        )));
    }

    let identity_projection = projection.is_empty()
        || (projection.len() == input.column_count()
            && projection
                .iter()
                .copied()
                .enumerate()
                .all(|(output_idx, input_idx)| output_idx == input_idx));
    if identity_projection {
        return Ok(input);
    }

    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let payload = slot
        .as_mut()
        .expect("hash join projected payload chunk was initialized above");
    payload.data.clear();
    payload.data.reserve(projection.len());
    for &column_idx in projection {
        payload
            .data
            .push(Arc::clone(input.data.get(column_idx).ok_or_else(|| {
                paro_error::internal("hash join build projection out of bounds")
            })?));
    }
    payload.set_capacity(input.size().max(1));
    payload.try_set_cardinality(input.size())?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use super::*;

    fn input_chunk() -> Chunk {
        let allocator = paro_common::test_utils::test_allocator();
        let mut chunk =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::Integer], 2, allocator)
                .expect("chunk");
        chunk.try_set_cardinality(2).unwrap();
        chunk.set_value(0, 0, &Value::Integer(1)).unwrap();
        chunk.set_value(0, 1, &Value::Integer(2)).unwrap();
        chunk.set_value(1, 0, &Value::Integer(10)).unwrap();
        chunk.set_value(1, 1, &Value::Integer(20)).unwrap();
        chunk
    }

    #[test]
    fn build_payload_empty_projection_reuses_input_columns_without_projection_vec() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(
            &input,
            &[],
            &[LogicalType::Integer, LogicalType::Integer],
            &mut slot,
        )
        .expect("payload");

        assert_eq!(payload.size(), 2);
        assert!(Arc::ptr_eq(&payload.data[0], &input.data[0]));
        assert!(Arc::ptr_eq(&payload.data[1], &input.data[1]));
        assert!(slot.is_none());
    }

    #[test]
    fn build_payload_can_be_empty_for_payload_free_joins() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(&input, &[], &[], &mut slot).expect("payload");

        assert_eq!(payload.size(), 2);
        assert_eq!(payload.column_count(), 0);
        assert!(slot.is_some());
    }

    #[test]
    fn build_payload_projects_columns_only_when_projection_is_non_identity() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("payload");

        assert_eq!(payload.size(), 2);
        assert_eq!(payload.column(0).unwrap().get_i32(0), Some(10));
        assert_eq!(payload.column(0).unwrap().get_i32(1), Some(20));
        assert!(Arc::ptr_eq(&payload.data[0], &input.data[1]));
        assert!(slot.is_some());
    }

    #[test]
    fn build_payload_reuses_projected_metadata_slot_across_chunks() {
        let input = input_chunk();
        let mut slot = None;
        build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("warm payload");
        let data_ptr = slot.as_ref().expect("projected payload slot").data.as_ptr();
        let data_capacity = slot
            .as_ref()
            .expect("projected payload slot")
            .data
            .capacity();

        build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("reused payload");

        let payload = slot.as_ref().expect("projected payload slot");
        assert_eq!(payload.data.as_ptr(), data_ptr);
        assert_eq!(payload.data.capacity(), data_capacity);
    }
}
