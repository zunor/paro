// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::{SelectionVector, VectorSelection};

pub(crate) fn copy_selected_rows(
    source: &Chunk,
    output: &mut Chunk,
    selection: &SelectionVector,
    selected_count: usize,
) -> Result<()> {
    if output.column_count() < source.column_count() {
        return Err(paro_error::internal(format!(
            "aggregate output has fewer columns than its source: source={} output={}",
            source.column_count(),
            output.column_count()
        )));
    }

    output.try_set_cardinality(selected_count)?;
    let vector_selection = VectorSelection::from(selection);
    for column_idx in 0..source.column_count() {
        let source_vector = source.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing selected aggregate source column {column_idx}"
            ))
        })?;
        let target_vector = output.column_mut(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing selected aggregate target column {column_idx}"
            ))
        })?;
        target_vector.try_copy_selection(
            0,
            source_vector.as_ref(),
            &vector_selection,
            selected_count,
        )?;
    }
    Ok(())
}
