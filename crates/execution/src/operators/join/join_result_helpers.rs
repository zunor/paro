// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared join result construction helpers.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

fn project_columns(
    input: &Chunk,
    sel: &SelectionVector,
    projection_map: &[usize],
    result: &mut Chunk,
    result_offset: usize,
) -> Result<()> {
    for (out_idx, &input_idx) in projection_map.iter().enumerate() {
        result.data[result_offset + out_idx] = Arc::new(Vector::try_dictionary(
            Arc::clone(&input.data[input_idx]),
            sel.clone(),
        )?);
    }
    Ok(())
}

pub fn construct_semi_join_result(
    left: &Chunk,
    left_sel: &SelectionVector,
    count: usize,
    left_projection_map: &[usize],
    result: &mut Chunk,
) -> Result<()> {
    project_columns(left, left_sel, left_projection_map, result, 0)?;
    result.try_set_cardinality(count)?;
    Ok(())
}

pub fn construct_anti_join_result(
    left: &Chunk,
    left_sel: &SelectionVector,
    count: usize,
    left_projection_map: &[usize],
    result: &mut Chunk,
) -> Result<()> {
    construct_semi_join_result(left, left_sel, count, left_projection_map, result)
}

pub fn construct_left_outer_result(
    left: &Chunk,
    left_sel: &SelectionVector,
    count: usize,
    left_projection_map: &[usize],
    right_types: &[LogicalType],
    result: &mut Chunk,
) -> Result<()> {
    project_columns(left, left_sel, left_projection_map, result, 0)?;
    let right_offset = left_projection_map.len();
    for (idx, typ) in right_types.iter().enumerate() {
        result.data[right_offset + idx] = Arc::new(Vector::try_constant_null(
            typ.clone(),
            count,
            result.allocator().clone(),
        )?);
    }
    result.try_set_cardinality(count)?;
    Ok(())
}

pub fn construct_mark_join_result(
    left: &Chunk,
    left_projection_map: &[usize],
    markers: &[Option<bool>],
    result: &mut Chunk,
) -> Result<()> {
    let count = markers.len();
    let mut left_sel = SelectionVector::try_with_capacity(count, result.allocator().clone())?;
    left_sel.set_len(count);
    for idx in 0..count {
        left_sel.set(idx, idx);
    }
    project_columns(left, &left_sel, left_projection_map, result, 0)?;

    let marker_offset = left_projection_map.len();
    result.data[marker_offset] = Arc::new(Vector::try_new(
        LogicalType::Boolean,
        count.max(1),
        result.allocator().clone(),
    )?);
    let marker_vec = Arc::make_mut(&mut result.data[marker_offset]);
    marker_vec.set_len(count);
    for (idx, marker) in markers.iter().enumerate() {
        match marker {
            Some(value) => {
                marker_vec.set_bool(idx, *value);
            }
            None => {
                marker_vec.set_bool(idx, false);
                marker_vec.try_set_null(idx, true)?;
            }
        }
    }
    if result.capacity() < count {
        result.set_capacity(count);
    }
    result.try_set_cardinality(count)?;
    Ok(())
}

pub fn construct_right_outer_scan_result(
    build: &Chunk,
    build_sel: &SelectionVector,
    count: usize,
    left_types: &[LogicalType],
    right_projection_map: &[usize],
    result: &mut Chunk,
) -> Result<()> {
    for (idx, typ) in left_types.iter().enumerate() {
        result.data[idx] = Arc::new(Vector::try_constant_null(
            typ.clone(),
            count,
            result.allocator().clone(),
        )?);
    }
    project_columns(
        build,
        build_sel,
        right_projection_map,
        result,
        left_types.len(),
    )?;
    result.try_set_cardinality(count)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        construct_left_outer_result, construct_mark_join_result, construct_right_outer_scan_result,
    };
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use std::sync::Arc;

    #[test]
    fn left_outer_result_projects_left_and_null_fills_right() {
        let left = Chunk::from_arc_vectors(
            vec![
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20],
                    paro_common::test_utils::test_allocator(),
                )),
                Arc::new(paro_common::test_utils::test_string_vector_with_allocator(
                    &["a", "b"],
                    paro_common::test_utils::test_allocator(),
                )),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::BigInt,
            ],
            2,
        );

        let sel = paro_common::test_utils::test_selection(vec![1]);
        construct_left_outer_result(
            &left,
            &sel,
            1,
            &[1],
            &[LogicalType::Boolean, LogicalType::BigInt],
            &mut result,
        )
        .unwrap();

        assert_eq!(result.size(), 1);
        assert_eq!(result.data[0].get_value(0).to_string(), "'b'");
        assert!(result.data[1].is_null(0));
        assert!(result.data[2].is_null(0));
    }

    #[test]
    fn mark_join_result_appends_boolean_marker() {
        let left = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer, LogicalType::Boolean],
            2,
        );

        construct_mark_join_result(&left, &[0], &[Some(true), None], &mut result).unwrap();

        assert_eq!(result.size(), 2);
        assert_eq!(result.data[1].get_value(0).to_string(), "true");
        assert!(result.data[1].is_null(1));
    }

    #[test]
    fn mark_join_result_resizes_marker_for_large_probe_chunk() {
        let values = (0..=paro_common::vector::VECTOR_SIZE as i32).collect::<Vec<_>>();
        let markers = values
            .iter()
            .enumerate()
            .map(|(idx, _)| (idx % 2 == 0).then_some(true))
            .collect::<Vec<_>>();
        let left = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &values,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer, LogicalType::Boolean],
            paro_common::vector::VECTOR_SIZE,
        );

        construct_mark_join_result(&left, &[0], &markers, &mut result).unwrap();

        assert_eq!(result.size(), values.len());
        assert_eq!(result.data[1].get_bool(0), Some(true));
        assert!(result.data[1].is_null(1));
        assert_eq!(
            result.data[1].get_bool(paro_common::vector::VECTOR_SIZE),
            Some(true)
        );
    }

    #[test]
    fn right_outer_scan_result_null_fills_left_side() {
        let build = Chunk::from_arc_vectors(
            vec![
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2],
                    paro_common::test_utils::test_allocator(),
                )),
                Arc::new(paro_common::test_utils::test_string_vector_with_allocator(
                    &["x", "y"],
                    paro_common::test_utils::test_allocator(),
                )),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Boolean,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
            2,
        );

        construct_right_outer_scan_result(
            &build,
            &paro_common::test_utils::test_selection(vec![0]),
            1,
            &[LogicalType::Boolean],
            &[0, 1],
            &mut result,
        )
        .unwrap();

        assert!(result.data[0].is_null(0));
        assert_eq!(result.data[1].get_value(0).to_string(), "1");
        assert_eq!(result.data[2].get_value(0).to_string(), "'x'");
    }
}
