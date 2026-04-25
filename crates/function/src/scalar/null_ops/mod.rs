// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! NULL handling functions.
//!
//!
//!
//! ## Functions
//! - `ifnull(a, b)` - Returns `b` if `a` is NULL, otherwise `a`
//! - `nullif(a, b)` - Returns NULL if `a = b`, otherwise `a`
//! - `coalesce(a, b,...)` - Returns first non-NULL argument

mod coalesce;
mod ifnull;
mod nullif;

pub use coalesce::*;
pub use ifnull::*;
pub use nullif::*;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::Vector;

use crate::ScalarFunctionSet;

/// Register all NULL handling functions.
pub fn register_null_functions() -> Vec<ScalarFunctionSet> {
    vec![
        get_ifnull_functions(),
        get_nullif_functions(),
        get_coalesce_functions(),
    ]
}

pub(crate) fn coalesce_rows(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.try_set_count(count)?;

    for row in 0..count {
        let mut copied = false;
        for source in &input.data {
            if source.is_null(row) {
                continue;
            }
            result.try_copy_at(row, source, row)?;
            copied = true;
            break;
        }

        if !copied {
            result.try_set_null(row, true)?;
        }
    }

    Ok(())
}

pub(crate) fn ifnull_rows(input: &Chunk, result: &mut Vector) -> Result<()> {
    if input.data.len() != 2 {
        return Err(paro_error::internal(format!(
            "ifnull expects exactly 2 arguments, got {}",
            input.data.len()
        )));
    }

    let count = input.size();
    result.try_set_count(count)?;
    let left = &input.data[0];
    let right = &input.data[1];

    for row in 0..count {
        let source = if left.is_null(row) { right } else { left };
        if source.is_null(row) {
            result.try_set_null(row, true)?;
        } else {
            result.try_copy_at(row, source, row)?;
        }
    }

    Ok(())
}

pub(crate) fn nullif_rows<F>(input: &Chunk, result: &mut Vector, mut equals: F) -> Result<()>
where
    F: FnMut(&Value, &Value) -> bool,
{
    if input.data.len() != 2 {
        return Err(paro_error::internal(format!(
            "nullif expects exactly 2 arguments, got {}",
            input.data.len()
        )));
    }

    let count = input.size();
    result.try_set_count(count)?;
    let left = &input.data[0];
    let right = &input.data[1];

    for row in 0..count {
        if left.is_null(row) || right.is_null(row) {
            if left.is_null(row) {
                result.try_set_null(row, true)?;
            } else {
                result.try_copy_at(row, left, row)?;
            }
            continue;
        }

        let left_value = left.get_value(row);
        let right_value = right.get_value(row);
        if equals(&left_value, &right_value) {
            result.try_set_null(row, true)?;
        } else {
            result.try_copy_at(row, left, row)?;
        }
    }

    Ok(())
}
