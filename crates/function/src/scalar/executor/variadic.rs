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
        .map(|vector| vector.to_varlen_view(count))
        .collect::<Vec<_>>();
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        let capacity = views
            .iter()
            .filter(|view| view.is_valid(row))
            .map(|view| view.get_inline_string(row).as_str().len())
            .sum();
        let mut concatenated = String::with_capacity(capacity);
        for view in &views {
            if view.is_valid(row) {
                concatenated.push_str(view.get_inline_string(row).as_str());
            }
        }
        writer.write_str(row, &concatenated);
    }

    Ok(())
}

pub fn execute_concat_ws(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let separator = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing separator column".to_string()))?
        .to_varlen_view(count);
    let arguments = input
        .data
        .iter()
        .skip(1)
        .map(|vector| vector.to_varlen_view(count))
        .collect::<Vec<_>>();
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !separator.is_valid(row) {
            writer.set_null(row);
            continue;
        }

        let separator_value = separator.get_inline_string(row);
        let sep = separator_value.as_str();
        let mut value_count = 0;
        let mut capacity = 0;
        for view in &arguments {
            if view.is_valid(row) {
                value_count += 1;
                capacity += view.get_inline_string(row).as_str().len();
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
            concatenated.push_str(view.get_inline_string(row).as_str());
            wrote_any = true;
        }

        writer.write_str(row, &concatenated);
    }

    Ok(())
}

pub fn apply_coalesce_child(
    result: &mut Vector,
    child: &Vector,
    unresolved: &SelectionVector,
    unresolved_count: usize,
    next_unresolved: &mut SelectionVector,
) -> usize {
    next_unresolved.set_len(unresolved_count);
    let mut next_count = 0;

    for row in 0..unresolved_count {
        let logical_row = unresolved.get(row);
        if child.is_null(row) {
            next_unresolved.set(next_count, logical_row);
            next_count += 1;
        } else {
            result.copy_at(logical_row, child, row);
        }
    }

    next_unresolved.set_len(next_count);
    next_count
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::{SelectionVector, Vector};

    use super::{apply_coalesce_child, execute_concat, execute_concat_ws};

    #[test]
    fn concat_skips_null_inputs() {
        let first = Vector::from_strings(&["hello", "foo"]);
        let mut second = Vector::from_strings(&[" ", "-"]);
        second.set_null(0, true);
        let third = Vector::from_strings(&["world", "bar"]);
        let input = Chunk::from_vectors(vec![first, second, third]);
        let mut result = Vector::new(LogicalType::Varchar);

        execute_concat(&input, &mut result).expect("concat helper should succeed");

        assert_eq!(result.get_string(0), Some("helloworld"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn concat_ws_null_separator_returns_null() {
        let mut separator = Vector::from_strings(&[", "]);
        separator.set_null(0, true);
        let first = Vector::from_strings(&["a"]);
        let second = Vector::from_strings(&["b"]);
        let input = Chunk::from_vectors(vec![separator, first, second]);
        let mut result = Vector::new(LogicalType::Varchar);

        execute_concat_ws(&input, &mut result).expect("concat_ws helper should succeed");

        assert!(result.is_null(0));
    }

    #[test]
    fn concat_handles_long_unicode_inputs() {
        let long = "数".repeat(256);
        let first = Vector::from_strings(&[long.as_str()]);
        let second = Vector::from_strings(&["-paro-"]);
        let third_value = format!("{}终", "据".repeat(256));
        let third = Vector::from_strings(&[third_value.as_str()]);
        let input = Chunk::from_vectors(vec![first, second, third]);
        let mut result = Vector::new(LogicalType::Varchar);

        execute_concat(&input, &mut result).expect("concat helper should succeed");

        let expected = format!("{long}-paro-{third_value}");
        assert_eq!(result.get_string(0), Some(expected.as_str()));
    }

    #[test]
    fn apply_coalesce_child_tracks_unresolved_rows() {
        let mut result = Vector::from_strings(&["", "", ""]);
        for row in 0..3 {
            result.set_null(row, true);
        }

        let mut child = Vector::from_strings(&["alpha", "beta", "gamma"]);
        child.set_null(1, true);

        let unresolved = SelectionVector::from(vec![2_u32, 0, 1]);
        let mut next_unresolved = SelectionVector::with_capacity(3);

        let next_count =
            apply_coalesce_child(&mut result, &child, &unresolved, 3, &mut next_unresolved);

        assert_eq!(next_count, 1);
        assert_eq!(next_unresolved.get(0), 0);
        assert_eq!(result.get_string(2), Some("alpha"));
        assert_eq!(result.get_string(1), Some("gamma"));
        assert!(result.is_null(0));
    }
}
