// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::types::InlineString;
use paro_common::vector::{StringHeap, ValidityMask, Vector};

pub struct VarcharResultWriter<'a> {
    entries: *mut InlineString,
    validity: &'a mut ValidityMask,
    heap: &'a mut StringHeap,
}

impl<'a> VarcharResultWriter<'a> {
    pub fn new(result: &'a mut Vector, count: usize) -> Self {
        let (entries, validity, heap) = result.begin_varlen_write(count);
        Self {
            entries,
            validity,
            heap,
        }
    }

    #[inline]
    pub fn write_str(&mut self, row: usize, value: &str) {
        let entry = self.heap.add_string(value);
        unsafe {
            *self.entries.add(row) = entry;
        }
        self.validity.set_valid(row);
    }

    #[inline]
    pub fn write_bytes(&mut self, row: usize, value: &[u8]) {
        let entry = self.heap.add_blob(value);
        unsafe {
            *self.entries.add(row) = entry;
        }
        self.validity.set_valid(row);
    }

    #[inline]
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            *self.entries.add(row) = InlineString::empty();
        }
        self.validity.set_null(row);
    }
}

pub fn execute_varchar_unary_to_varchar<F>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str, usize, &mut VarcharResultWriter<'_>) -> Result<()>,
{
    let view = input.try_to_varlen_view(count)?;
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        op(view.get_inline_string(row).as_str(), row, &mut writer)?;
    }

    Ok(())
}

pub fn execute_varchar_unary_to_i64<F>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str) -> i64,
{
    let view = input.try_to_varlen_view(count)?;
    result.set_count(count);

    for row in 0..count {
        if !view.is_valid(row) {
            result.set_null(row, true);
            continue;
        }
        result.set_i64(row, op(view.get_inline_string(row).as_str()));
    }

    Ok(())
}

pub fn execute_varchar_binary_to_bool<F>(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str, &str) -> bool,
{
    let left = left.try_to_varlen_view(count)?;
    let right = right.try_to_varlen_view(count)?;
    result.set_count(count);

    for row in 0..count {
        if !left.is_valid(row) || !right.is_valid(row) {
            result.set_null(row, true);
            continue;
        }
        result.set_bool(
            row,
            op(
                left.get_inline_string(row).as_str(),
                right.get_inline_string(row).as_str(),
            ),
        );
    }

    Ok(())
}

pub fn execute_varchar_binary_to_i64<F>(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str, &str) -> i64,
{
    let left = left.try_to_varlen_view(count)?;
    let right = right.try_to_varlen_view(count)?;
    result.set_count(count);

    for row in 0..count {
        if !left.is_valid(row) || !right.is_valid(row) {
            result.set_null(row, true);
            continue;
        }
        result.set_i64(
            row,
            op(
                left.get_inline_string(row).as_str(),
                right.get_inline_string(row).as_str(),
            ),
        );
    }

    Ok(())
}

pub fn execute_varchar_binary_to_varchar<F>(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str, &str, usize, &mut VarcharResultWriter<'_>) -> Result<()>,
{
    let left = left.try_to_varlen_view(count)?;
    let right = right.try_to_varlen_view(count)?;
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !left.is_valid(row) || !right.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        op(
            left.get_inline_string(row).as_str(),
            right.get_inline_string(row).as_str(),
            row,
            &mut writer,
        )?;
    }

    Ok(())
}

pub fn execute_varchar_ternary_to_varchar<F>(
    first: &Vector,
    second: &Vector,
    third: &Vector,
    result: &mut Vector,
    count: usize,
    mut op: F,
) -> Result<()>
where
    F: FnMut(&str, &str, &str, usize, &mut VarcharResultWriter<'_>) -> Result<()>,
{
    let first = first.try_to_varlen_view(count)?;
    let second = second.try_to_varlen_view(count)?;
    let third = third.try_to_varlen_view(count)?;
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !first.is_valid(row) || !second.is_valid(row) || !third.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        op(
            first.get_inline_string(row).as_str(),
            second.get_inline_string(row).as_str(),
            third.get_inline_string(row).as_str(),
            row,
            &mut writer,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;

    use super::{execute_varchar_binary_to_bool, execute_varchar_unary_to_varchar};

    #[test]
    fn writer_handles_dictionary_inputs() {
        let base = std::sync::Arc::new(paro_common::test_utils::test_string_vector_with_allocator(
            &["alpha", "beta", "gamma"],
            paro_common::test_utils::test_allocator(),
        ));
        let input = paro_common::test_utils::test_dictionary(base, vec![2_u32, 0]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        execute_varchar_unary_to_varchar(&input, &mut result, 2, |value, row, writer| {
            writer.write_str(row, value);
            Ok(())
        })
        .expect("varlen unary should succeed");

        assert_eq!(result.get_string(0), Some("gamma"));
        assert_eq!(result.get_string(1), Some("alpha"));
    }

    #[test]
    fn binary_bool_helper_propagates_nulls() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world"],
            paro_common::test_utils::test_allocator(),
        );
        let mut right = paro_common::test_utils::test_string_vector_with_allocator(
            &["he", "or"],
            paro_common::test_utils::test_allocator(),
        );
        right.set_null(1, true);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        execute_varchar_binary_to_bool(&left, &right, &mut result, 2, |value, prefix| {
            value.starts_with(prefix)
        })
        .expect("varlen binary bool should succeed");

        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
    }
}
