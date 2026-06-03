// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::allocator::default_allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::Vector;

use crate::row::codec::scatter_to_positions;
use crate::row::pin::PinSet;
use crate::row::region::RowLocation;
use crate::row::{Ordering, RowAddr, RowStore};
use std::sync::Arc;

/// Physical gather order plus the map back to caller-visible order.
#[derive(Debug, Clone)]
pub struct GatherPlan {
    output_positions: Vec<usize>,
    logical_to_physical: Vec<usize>,
}

impl GatherPlan {
    pub(crate) fn identity(count: usize) -> Self {
        let positions: Vec<usize> = (0..count).collect();
        Self {
            output_positions: positions.clone(),
            logical_to_physical: positions,
        }
    }

    pub(crate) fn from_physical_to_logical(output_positions: Vec<usize>) -> Self {
        let mut logical_to_physical = vec![0; output_positions.len()];
        for (physical_idx, logical_idx) in output_positions.iter().copied().enumerate() {
            logical_to_physical[logical_idx] = physical_idx;
        }
        Self {
            output_positions,
            logical_to_physical,
        }
    }

    #[inline]
    pub fn output_positions(&self) -> &[usize] {
        &self.output_positions
    }

    #[inline]
    pub fn logical_to_physical(&self) -> &[usize] {
        &self.logical_to_physical
    }
}

/// Rows pinned through a sealed [`RowStore`].
#[derive(Debug)]
pub struct PinnedRows<'a> {
    store: &'a RowStore,
    logical_rows: Vec<RowLocation>,
    physical_rows: Vec<RowLocation>,
    plan: GatherPlan,
    _pin_set: PinSet<'a>,
}

impl<'a> PinnedRows<'a> {
    pub(crate) fn new(
        store: &'a RowStore,
        rows: Vec<RowLocation>,
        ordering: Ordering,
        pin_set: PinSet<'a>,
    ) -> Self {
        let logical_rows = rows;
        let (physical_rows, plan) = match ordering {
            Ordering::Sequential => (
                logical_rows.clone(),
                GatherPlan::identity(logical_rows.len()),
            ),
            Ordering::Arbitrary => {
                let mut indexed: Vec<(usize, RowLocation)> =
                    logical_rows.iter().copied().enumerate().collect();
                indexed.sort_by_key(|(_, row)| {
                    (
                        row.region_index,
                        row.addr.block_index(),
                        row.addr.row_within_block(),
                    )
                });
                let output_positions: Vec<usize> = indexed
                    .iter()
                    .map(|(logical_idx, _)| *logical_idx)
                    .collect();
                let physical_rows: Vec<RowLocation> =
                    indexed.into_iter().map(|(_, row)| row).collect();
                (
                    physical_rows,
                    GatherPlan::from_physical_to_logical(output_positions),
                )
            }
        };

        Self {
            store,
            logical_rows,
            physical_rows,
            plan,
            _pin_set: pin_set,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.logical_rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.logical_rows.is_empty()
    }

    #[inline]
    pub fn plan(&self) -> &GatherPlan {
        &self.plan
    }

    #[inline]
    pub fn addrs(&self) -> impl Iterator<Item = RowAddr> + '_ {
        self.logical_rows.iter().map(|row| row.addr)
    }

    pub fn row(&self, index: usize) -> Option<PinnedRow<'_, 'a>> {
        (index < self.len()).then_some(PinnedRow {
            rows: self,
            logical_index: index,
        })
    }

    /// Gather columns into `output`, preserving the caller-visible order.
    pub fn gather_columns(
        &self,
        column_ids: &[usize],
        output: &mut Chunk,
        output_offset: usize,
    ) -> Result<()> {
        let required = output_offset.saturating_add(self.len());
        ensure_output(output, required)?;

        for &column_idx in column_ids {
            self.gather_column_to_positions(column_idx, column_idx, output, |logical_idx| {
                output_offset + logical_idx
            })?;
        }
        Ok(())
    }

    /// Gather columns into arbitrary output positions without changing row order semantics.
    pub fn gather_columns_scattered(
        &self,
        column_ids: &[usize],
        output: &mut Chunk,
        output_positions: &[u32],
    ) -> Result<()> {
        if output_positions.len() != self.len() {
            return Err(paro_error::internal(format!(
                "output position count {} does not match pinned row count {}",
                output_positions.len(),
                self.len()
            )));
        }

        let required = output_positions
            .iter()
            .copied()
            .max()
            .map(|idx| idx as usize + 1)
            .unwrap_or(0);
        ensure_output(output, required)?;

        for &column_idx in column_ids {
            self.gather_column_to_positions(column_idx, column_idx, output, |logical_idx| {
                output_positions[logical_idx] as usize
            })?;
        }
        Ok(())
    }

    /// Gather source columns into arbitrary output columns and positions.
    pub fn gather_columns_projected(
        &self,
        projections: &[(usize, usize)],
        output: &mut Chunk,
        output_positions: &[u32],
    ) -> Result<()> {
        if output_positions.len() != self.len() {
            return Err(paro_error::internal(format!(
                "output position count {} does not match pinned row count {}",
                output_positions.len(),
                self.len()
            )));
        }

        let required = output_positions
            .iter()
            .copied()
            .max()
            .map(|idx| idx as usize + 1)
            .unwrap_or(0);
        ensure_output(output, required)?;

        for &(source_col_idx, output_col_idx) in projections {
            self.gather_column_to_positions(
                source_col_idx,
                output_col_idx,
                output,
                |logical_idx| output_positions[logical_idx] as usize,
            )?;
        }
        Ok(())
    }

    pub(crate) fn read_value(&self, logical_idx: usize, column_idx: usize) -> Result<Value> {
        let row = self.logical_rows.get(logical_idx).ok_or_else(|| {
            paro_error::internal(format!("pinned row index {} out of bounds", logical_idx))
        })?;
        self.store.validate_column(column_idx)?;
        let typ = self.store.layout().types()[column_idx].clone();
        let mut tmp = Vector::try_new(typ, 1, Arc::new(default_allocator()))?;
        self.store
            .region(row.region_index)
            .collection()
            .gather_column(&[row.local_ordinal], column_idx, &mut tmp)?;
        Ok(tmp.get_value(0))
    }

    fn gather_column_to_positions<F>(
        &self,
        column_idx: usize,
        output_col_idx: usize,
        output: &mut Chunk,
        mut output_position: F,
    ) -> Result<()>
    where
        F: FnMut(usize) -> usize,
    {
        self.store.validate_column(column_idx)?;
        if output_col_idx >= output.column_count() {
            return Err(paro_error::internal(format!(
                "output column {} out of range {}",
                output_col_idx,
                output.column_count()
            )));
        }

        let typ = self.store.layout().types()[column_idx].clone();
        let mut group_start = 0usize;
        while group_start < self.physical_rows.len() {
            let region_idx = self.physical_rows[group_start].region_index;
            let mut group_end = group_start + 1;
            while group_end < self.physical_rows.len()
                && self.physical_rows[group_end].region_index == region_idx
            {
                group_end += 1;
            }

            let group_len = group_end - group_start;
            let local_ordinals: Vec<usize> = self.physical_rows[group_start..group_end]
                .iter()
                .map(|row| row.local_ordinal)
                .collect();
            let mut tmp = Vector::try_new(typ.clone(), group_len, output.allocator().clone())?;
            self.store.region(region_idx).collection().gather_column(
                &local_ordinals,
                column_idx,
                &mut tmp,
            )?;

            let output_positions: Vec<usize> = (group_start..group_end)
                .map(|physical_idx| {
                    let logical_idx = self.plan.output_positions[physical_idx];
                    output_position(logical_idx)
                })
                .collect();
            let codec = self
                .store
                .layout()
                .codecs()
                .get(column_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!("missing codec for row column {}", column_idx))
                })?;
            scatter_to_positions(codec, output_col_idx, &tmp, output, &output_positions)?;

            group_start = group_end;
        }

        Ok(())
    }
}

fn ensure_output(output: &mut Chunk, required: usize) -> Result<()> {
    if required > output.capacity() {
        return Err(paro_error::internal(format!(
            "output chunk capacity {} smaller than required {}",
            output.capacity(),
            required
        )));
    }
    if output.size() < required {
        output.try_set_cardinality(required)?;
    }
    Ok(())
}

/// One row borrowed from a [`PinnedRows`] guard.
#[derive(Debug)]
pub struct PinnedRow<'rows, 'store> {
    rows: &'rows PinnedRows<'store>,
    logical_index: usize,
}

impl PinnedRow<'_, '_> {
    #[inline]
    pub fn addr(&self) -> RowAddr {
        self.rows.logical_rows[self.logical_index].addr
    }

    #[inline]
    pub fn ordinal(&self) -> u64 {
        self.rows.logical_rows[self.logical_index].ordinal
    }

    pub fn read_value(&self, column_idx: usize) -> Result<Value> {
        self.rows.read_value(self.logical_index, column_idx)
    }
}
