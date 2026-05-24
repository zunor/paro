// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! EXPLAIN ANALYZE output helpers for typed query execution.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use crate::runtime::{QueryOutputPort, QueryOutputWrite};

pub(super) fn push_explain_lines(
    output: &QueryOutputPort,
    lines: &[String],
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let output_types = [LogicalType::Varchar];
    if lines.is_empty() {
        let mut chunk = Chunk::try_initialize(&output_types, 1, allocator)?;
        chunk.try_set_cardinality(0)?;
        match output.try_push(chunk) {
            QueryOutputWrite::Written => return Ok(()),
            QueryOutputWrite::Blocked(_) => {
                return Err(paro_error::internal(
                    "unbounded EXPLAIN output port unexpectedly blocked",
                ));
            }
        }
    }

    for chunk_lines in lines.chunks(VECTOR_SIZE) {
        let mut chunk =
            Chunk::try_initialize(&output_types, chunk_lines.len().max(1), allocator.clone())?;
        for (row_idx, line) in chunk_lines.iter().enumerate() {
            chunk
                .column_mut(0)
                .ok_or_else(|| paro_error::internal("EXPLAIN output column missing"))?
                .try_set_string(row_idx, line)?;
        }
        chunk.try_set_cardinality(chunk_lines.len())?;
        match output.try_push(chunk) {
            QueryOutputWrite::Written => {}
            QueryOutputWrite::Blocked(_) => {
                return Err(paro_error::internal(
                    "unbounded EXPLAIN output port unexpectedly blocked",
                ));
            }
        }
    }
    Ok(())
}
