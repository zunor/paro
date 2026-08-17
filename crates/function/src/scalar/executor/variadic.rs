// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::{SelectionVector, Vector};

use super::varlen::VarcharResultWriter;

pub fn execute_concat(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let views = input
        .data
        .iter()
        .map(|vector| vector.try_to_varlen_view(count))
        .collect::<Result<Vec<_>>>()?;
    let mut writer = VarcharResultWriter::new(result, count);

    // SAFETY: concat binds every input as VARCHAR.
    for row in 0..count {
        let capacity = views
            .iter()
            .filter(|view| view.is_valid(row))
            .map(|view| unsafe { view.str_unchecked(row) }.len())
            .sum();
        let mut concatenated = String::with_capacity(capacity);
        for view in &views {
            if view.is_valid(row) {
                concatenated.push_str(unsafe { view.str_unchecked(row) });
            }
        }
        writer.write_str(row, &concatenated)?;
    }

    Ok(())
}

pub fn execute_concat_ws(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let separator = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing separator column".to_string()))?
        .try_to_varlen_view(count)?;
    let arguments = input
        .data
        .iter()
        .skip(1)
        .map(|vector| vector.try_to_varlen_view(count))
        .collect::<Result<Vec<_>>>()?;
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !separator.is_valid(row) {
            writer.set_null(row);
            continue;
        }

        // SAFETY: concat_ws binds the separator and arguments as VARCHAR.
        let sep = unsafe { separator.str_unchecked(row) };
        let mut value_count = 0;
        let mut capacity = 0;
        for view in &arguments {
            if view.is_valid(row) {
                value_count += 1;
                capacity += unsafe { view.str_unchecked(row) }.len();
            }
        }
        if value_count > 1 {
            capacity += sep.len() * (value_count - 1);
        }
        let mut concatenated = String::with_capacity(capacity);
        let mut wrote_any = false;

        for view in &arguments {
            if !view.is_valid(row) {
                continue;
            }
            if wrote_any {
                concatenated.push_str(sep);
            }
            concatenated.push_str(unsafe { view.str_unchecked(row) });
            wrote_any = true;
        }

        writer.write_str(row, &concatenated)?;
    }

    Ok(())
}

pub fn apply_coalesce_child(
    result: &mut Vector,
    child: &Vector,
    unresolved: &SelectionVector,
    unresolved_count: usize,
    next_unresolved: &mut SelectionVector,
) -> Result<usize> {
    next_unresolved.set_len(unresolved_count);
    let mut next_count = 0;

    for row in 0..unresolved_count {
        let logical_row = unresolved.get(row);
        if child.is_null(row) {
            next_unresolved.set(next_count, logical_row);
            next_count += 1;
        } else {
            result.try_copy_at(logical_row, child, row)?;
        }
    }

    next_unresolved.set_len(next_count);
    Ok(next_count)
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use super::{apply_coalesce_child, execute_concat, execute_concat_ws};

    #[test]
    fn concat_skips_null_inputs() {
        let first = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "foo"],
            paro_common::test_utils::test_allocator(),
        );
        let mut second = paro_common::test_utils::test_string_vector_with_allocator(
            &[" ", "-"],
            paro_common::test_utils::test_allocator(),
        );
        second.set_null(0, true);
        let third = paro_common::test_utils::test_string_vector_with_allocator(
            &["world", "bar"],
            paro_common::test_utils::test_allocator(),
        );
        let input = Chunk::from_vectors(
            vec![first, second, third],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        execute_concat(&input, &mut result).expect("concat helper should succeed");

        assert_eq!(result.get_string(0), Some("helloworld"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn concat_ws_null_separator_returns_null() {
        let mut separator = paro_common::test_utils::test_string_vector_with_allocator(
            &[", "],
            paro_common::test_utils::test_allocator(),
        );
        separator.set_null(0, true);
        let first = paro_common::test_utils::test_string_vector_with_allocator(
            &["a"],
            paro_common::test_utils::test_allocator(),
        );
        let second = paro_common::test_utils::test_string_vector_with_allocator(
            &["b"],
            paro_common::test_utils::test_allocator(),
        );
        let input = Chunk::from_vectors(
            vec![separator, first, second],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        execute_concat_ws(&input, &mut result).expect("concat_ws helper should succeed");

        assert!(result.is_null(0));
    }

    #[test]
    fn concat_handles_long_unicode_inputs() {
        let long = "数".repeat(256);
        let first = paro_common::test_utils::test_string_vector_with_allocator(
            &[long.as_str()],
            paro_common::test_utils::test_allocator(),
        );
        let second = paro_common::test_utils::test_string_vector_with_allocator(
            &["-paro-"],
            paro_common::test_utils::test_allocator(),
        );
        let third_value = format!("{}终", "据".repeat(256));
        let third = paro_common::test_utils::test_string_vector_with_allocator(
            &[third_value.as_str()],
            paro_common::test_utils::test_allocator(),
        );
        let input = Chunk::from_vectors(
            vec![first, second, third],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        execute_concat(&input, &mut result).expect("concat helper should succeed");

        let expected = format!("{long}-paro-{third_value}");
        assert_eq!(result.get_string(0), Some(expected.as_str()));
    }

    #[test]
    fn apply_coalesce_child_tracks_unresolved_rows() {
        let mut result = paro_common::test_utils::test_string_vector_with_allocator(
            &["", "", ""],
            paro_common::test_utils::test_allocator(),
        );
        for row in 0..3 {
            result.set_null(row, true);
        }

        let mut child = paro_common::test_utils::test_string_vector_with_allocator(
            &["alpha", "beta", "gamma"],
            paro_common::test_utils::test_allocator(),
        );
        child.set_null(1, true);

        let unresolved = paro_common::test_utils::test_selection(vec![2_u32, 0, 1]);
        let mut next_unresolved = paro_common::test_utils::test_selection_with_capacity(3);

        let next_count =
            apply_coalesce_child(&mut result, &child, &unresolved, 3, &mut next_unresolved)
                .expect("coalesce child copy should succeed");

        assert_eq!(next_count, 1);
        assert_eq!(next_unresolved.get(0), 0);
        assert_eq!(result.get_string(2), Some("alpha"));
        assert_eq!(result.get_string(1), Some("gamma"));
        assert!(result.is_null(0));
    }
}
