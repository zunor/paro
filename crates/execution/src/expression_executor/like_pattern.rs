// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector selection adapter for the shared compiled SQL `LIKE` matcher.

use paro_common::error::Result;
pub(crate) use paro_common::string_pattern::{sql_like, PreparedLikePattern};
use paro_common::vector::{SelectionVector, Vector};

use super::predicate::map_row;

pub(crate) fn select_prepared_like(
    values: &Vector,
    pattern: &PreparedLikePattern,
    input_sel: Option<&SelectionVector>,
    count: usize,
    output: &mut SelectionVector,
) -> Result<usize> {
    let values = values.try_to_varlen_view(count)?;
    output.set_len(count);
    let mut selected = 0usize;
    for row_idx in 0..count {
        if values.is_valid(row_idx) && pattern.matches(values.get_string_view(row_idx).as_str()) {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    Ok(selected)
}
