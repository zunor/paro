// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::sort_key::{OrderModifiers, SortKeyEncoding};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::Expression;
use paro_storage::row::{RowLayout, RowValidityType};

use super::sort_projection_column::SortProjectionColumn;

#[derive(Debug)]
pub struct Sort {
    orders: Vec<OrderByNode>,
    key_column_indices: Vec<usize>,
    sort_key_modifiers: Vec<OrderModifiers>,
    sort_key_encoding: Arc<SortKeyEncoding>,
    key_layout: Arc<RowLayout>,
    payload_layout: Arc<RowLayout>,
    input_projection_map: Vec<usize>,
    output_projection_columns: Vec<SortProjectionColumn>,
    is_index_sort: bool,
}

impl Sort {
    pub fn new(
        orders: Vec<OrderByNode>,
        input_types: Vec<LogicalType>,
        projection_map: Vec<usize>,
        is_index_sort: bool,
    ) -> Result<Self> {
        let projection_map = if projection_map.is_empty() {
            (0..input_types.len()).collect()
        } else {
            projection_map
        };

        let sort_key_modifiers = orders
            .iter()
            .map(|order| OrderModifiers::new(order.ascending, order.nulls_first))
            .collect::<Vec<_>>();

        let mut input_column_to_key = HashMap::new();
        let mut key_column_indices = Vec::with_capacity(orders.len());
        for (key_idx, order) in orders.iter().enumerate() {
            let column_idx = match &order.expression {
                Expression::ColumnRef(col_ref) => col_ref.binding.column_index,
                Expression::Reference(reference) => reference.index,
                other => {
                    return Err(paro_error::internal(format!(
                        "sort key expression was not lowered to a column reference: {other:?}"
                    )));
                }
            };
            input_column_to_key.insert(column_idx, key_idx);
            key_column_indices.push(column_idx);
        }

        let key_types = orders
            .iter()
            .map(|order| order.expression.return_type())
            .collect::<Vec<_>>();
        let sort_key_encoding = Arc::new(SortKeyEncoding::new(
            key_types.clone(),
            sort_key_modifiers.clone(),
        )?);

        let mut payload_types = Vec::new();
        let mut input_projection_map = Vec::new();
        let mut output_projection_columns = Vec::new();
        for (output_col_idx, &input_col_idx) in projection_map.iter().enumerate() {
            if let Some(&key_idx) = input_column_to_key.get(&input_col_idx) {
                output_projection_columns.push(SortProjectionColumn::new(
                    false,
                    key_idx,
                    output_col_idx,
                ));
            } else {
                output_projection_columns.push(SortProjectionColumn::new(
                    true,
                    payload_types.len(),
                    output_col_idx,
                ));
                payload_types.push(input_types[input_col_idx].clone());
                input_projection_map.push(input_col_idx);
            }
        }
        output_projection_columns.sort_by_key(|projection| projection.output_col_idx);

        Ok(Self {
            orders,
            key_column_indices,
            sort_key_modifiers,
            sort_key_encoding,
            key_layout: Arc::new(RowLayout::from_types(
                key_types,
                RowValidityType::CanHaveNullValues,
            )),
            payload_layout: Arc::new(RowLayout::from_types(
                payload_types,
                RowValidityType::CanHaveNullValues,
            )),
            input_projection_map,
            output_projection_columns,
            is_index_sort,
        })
    }

    #[inline]
    pub fn orders(&self) -> &[OrderByNode] {
        &self.orders
    }

    #[inline]
    pub fn key_column_indices(&self) -> &[usize] {
        &self.key_column_indices
    }

    #[inline]
    pub fn sort_key_modifiers(&self) -> &[OrderModifiers] {
        &self.sort_key_modifiers
    }

    #[inline]
    pub fn sort_key_encoding(&self) -> &Arc<SortKeyEncoding> {
        &self.sort_key_encoding
    }

    #[inline]
    pub fn key_layout(&self) -> &Arc<RowLayout> {
        &self.key_layout
    }

    #[inline]
    pub fn payload_layout(&self) -> &Arc<RowLayout> {
        &self.payload_layout
    }

    #[inline]
    pub fn input_projection_map(&self) -> &[usize] {
        &self.input_projection_map
    }

    #[inline]
    pub fn output_projection_columns(&self) -> &[SortProjectionColumn] {
        &self.output_projection_columns
    }

    #[inline]
    pub fn is_index_sort(&self) -> bool {
        self.is_index_sort
    }
}

pub(crate) fn build_key_chunk_into<'a>(
    chunk: &'a Chunk,
    key_column_indices: &[usize],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    let output = projected_metadata_chunk(chunk, key_column_indices.len(), slot)?;
    build_key_chunk_in_place(chunk, key_column_indices, output)?;
    Ok(output)
}

pub(crate) fn build_payload_chunk_into<'a>(
    chunk: &'a Chunk,
    projection_map: &[usize],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    let output = projected_metadata_chunk(chunk, projection_map.len(), slot)?;
    build_payload_chunk_in_place(chunk, projection_map, output)?;
    Ok(output)
}

pub(crate) fn build_key_chunk_in_place(
    chunk: &Chunk,
    key_column_indices: &[usize],
    output: &mut Chunk,
) -> Result<()> {
    output.data.clear();
    output.data.reserve(key_column_indices.len());
    output.set_capacity(chunk.size().max(1));
    for &column_idx in key_column_indices {
        output
            .data
            .push(Arc::clone(chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!("sort key column out of bounds: {column_idx}"))
            })?));
    }
    output.try_set_cardinality(chunk.size())?;
    Ok(())
}

pub(crate) fn build_payload_chunk_in_place(
    chunk: &Chunk,
    projection_map: &[usize],
    output: &mut Chunk,
) -> Result<()> {
    output.data.clear();
    output.data.reserve(projection_map.len());
    output.set_capacity(chunk.size().max(1));
    for &column_idx in projection_map {
        output
            .data
            .push(Arc::clone(chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!("sort payload column out of bounds: {column_idx}"))
            })?));
    }
    output.try_set_cardinality(chunk.size())?;
    Ok(())
}

fn projected_metadata_chunk<'a>(
    input: &'a Chunk,
    column_count: usize,
    slot: &'a mut Option<Chunk>,
) -> Result<&'a mut Chunk> {
    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let output = slot
        .as_mut()
        .expect("sort projected metadata chunk was initialized above");
    output.data.clear();
    output.data.reserve(column_count);
    output.set_capacity(input.size().max(1));
    Ok(output)
}
