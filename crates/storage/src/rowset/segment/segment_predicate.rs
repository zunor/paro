// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::Segment;
use crate::buffer::Prefetcher;
use crate::codec::vector_decoder;
use crate::index::{
    collect_predicate_columns, IndexEvaluator, Predicate, PredicateResult, PredicateTree,
};
use crate::rowset::column::{ColumnBatch, ColumnIterator};
use crate::tablet::ColumnId;
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct PredicateEvaluator {
    tree: CompiledPredicateTree,
    predicate_columns: Vec<ColumnId>,
    predicate_types: Vec<LogicalType>,
    predicate_iterators: Vec<Option<Box<dyn ColumnIterator + Send + Sync>>>,
    decode_predicate_columns: Vec<bool>,
    allocator: Arc<dyn Allocator>,
}

pub(super) enum PredicateColumnBatch {
    Raw(ColumnBatch),
    Decoded(Vector),
}

impl PredicateColumnBatch {
    #[inline]
    fn is_null(&self, row_idx: usize) -> bool {
        match self {
            Self::Raw(batch) => batch
                .nulls
                .as_ref()
                .is_some_and(|nulls| nulls[row_idx] != 0),
            Self::Decoded(vector) => vector.is_null(row_idx),
        }
    }

    #[inline]
    fn decoded(&self) -> Option<&Vector> {
        match self {
            Self::Raw(_) => None,
            Self::Decoded(vector) => Some(vector),
        }
    }

    #[inline]
    unsafe fn i32_value(&self, row_idx: usize) -> i32 {
        match self {
            Self::Raw(batch) => i32::from_le(unsafe {
                batch
                    .data
                    .as_ptr()
                    .add(row_idx * std::mem::size_of::<i32>())
                    .cast::<i32>()
                    .read_unaligned()
            }),
            Self::Decoded(vector) => unsafe { vector.get_fixed::<i32>(row_idx) },
        }
    }

    #[inline]
    unsafe fn i64_value(&self, row_idx: usize) -> i64 {
        match self {
            Self::Raw(batch) => i64::from_le(unsafe {
                batch
                    .data
                    .as_ptr()
                    .add(row_idx * std::mem::size_of::<i64>())
                    .cast::<i64>()
                    .read_unaligned()
            }),
            Self::Decoded(vector) => unsafe { vector.get_fixed::<i64>(row_idx) },
        }
    }

    #[inline]
    unsafe fn i128_value(&self, row_idx: usize) -> i128 {
        match self {
            Self::Raw(batch) => i128::from_le(unsafe {
                batch
                    .data
                    .as_ptr()
                    .add(row_idx * std::mem::size_of::<i128>())
                    .cast::<i128>()
                    .read_unaligned()
            }),
            Self::Decoded(vector) => unsafe { vector.get_fixed::<i128>(row_idx) },
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl ComparisonOperator {
    #[inline]
    fn matches(self, ordering: Ordering) -> bool {
        match self {
            Self::Equal => ordering.is_eq(),
            Self::NotEqual => !ordering.is_eq(),
            Self::LessThan => ordering.is_lt(),
            Self::LessThanOrEqual => ordering.is_le(),
            Self::GreaterThan => ordering.is_gt(),
            Self::GreaterThanOrEqual => ordering.is_ge(),
        }
    }
}

enum CompiledPredicateTree {
    Leaf(CompiledPredicate),
    And(Vec<CompiledPredicateTree>),
    Or(Vec<CompiledPredicateTree>),
}

enum CompiledPredicate {
    I32Comparisons {
        column_idx: usize,
        comparisons: Vec<(ComparisonOperator, i32)>,
    },
    I64Comparisons {
        column_idx: usize,
        comparisons: Vec<(ComparisonOperator, i64)>,
    },
    I128Comparisons {
        column_idx: usize,
        comparisons: Vec<(ComparisonOperator, i128)>,
    },
    Generic {
        column_idx: usize,
        predicate: Predicate,
    },
}

impl PredicateEvaluator {
    pub(super) fn new(
        segment: &Segment,
        tree: PredicateTree,
        evaluator: &IndexEvaluator,
        prefetcher: Option<Arc<Prefetcher>>,
        explicit_predicate_columns: Option<Vec<ColumnId>>,
    ) -> Result<Option<Self>> {
        if !Self::predicate_tree_requires_row_verification(&tree)
            && !Self::requires_row_level_predicate_eval(evaluator, &tree)
        {
            return Ok(None);
        }

        let predicate_columns =
            explicit_predicate_columns.unwrap_or_else(|| collect_predicate_columns(&tree));

        if predicate_columns.is_empty() {
            return Ok(None);
        }

        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        let mut column_map = HashMap::new();
        let mut iterators = Vec::new();
        let mut types = Vec::new();

        for col_id in predicate_columns {
            if !seen.insert(col_id) {
                continue;
            }

            let idx = columns.len();
            columns.push(col_id);
            column_map.insert(col_id, idx);

            let col = segment
                .schema()
                .column_by_id(col_id)
                .ok_or_else(|| paro_error::invalid_input("Predicate column not found in schema"))?;
            types.push(col.logical_type.clone());

            let iter = if segment.get_column_meta(col_id).is_some() {
                Some(segment.new_column_iterator_with_prefetcher(col_id, prefetcher.clone())?)
            } else {
                None
            };
            iterators.push(iter);
        }

        let tree = Self::compile_tree(&tree, &column_map, &types)?;
        let mut decode_predicate_columns = vec![false; columns.len()];
        Self::mark_decoded_columns(&tree, &mut decode_predicate_columns);
        Ok(Some(Self {
            tree,
            predicate_columns: columns,
            predicate_types: types,
            predicate_iterators: iterators,
            decode_predicate_columns,
            allocator: Arc::new(default_allocator()),
        }))
    }

    pub(super) fn requires_row_level_predicate_eval(
        evaluator: &IndexEvaluator,
        predicate_tree: &PredicateTree,
    ) -> bool {
        match predicate_tree {
            PredicateTree::Leaf(predicate) => {
                let leaf = PredicateTree::Leaf(predicate.clone());
                if predicate.requires_row_level_verification() {
                    return !matches!(
                        evaluator.evaluate(&leaf),
                        PredicateResult::NoneMatch | PredicateResult::AllMatch
                    );
                }
                !matches!(
                    evaluator.evaluate(&leaf),
                    PredicateResult::Bitmap(_)
                        | PredicateResult::NoneMatch
                        | PredicateResult::AllMatch
                )
            }
            PredicateTree::And(children) | PredicateTree::Or(children) => children
                .iter()
                .any(|child| Self::requires_row_level_predicate_eval(evaluator, child)),
        }
    }

    pub(super) fn predicate_tree_requires_row_verification(predicate_tree: &PredicateTree) -> bool {
        match predicate_tree {
            PredicateTree::Leaf(predicate) => predicate.requires_row_level_verification(),
            PredicateTree::And(children) | PredicateTree::Or(children) => children
                .iter()
                .any(Self::predicate_tree_requires_row_verification),
        }
    }

    pub(super) fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        for iter in self.predicate_iterators.iter_mut().flatten() {
            iter.seek_to_ordinal(ordinal)?;
        }
        Ok(())
    }

    pub(super) fn read_next_batch(
        &mut self,
        to_read: usize,
    ) -> Result<(usize, Vec<PredicateColumnBatch>)> {
        if to_read == 0 {
            return Ok((0, Vec::new()));
        }

        let mut rows_read: Option<usize> = None;
        let mut batches_by_col: Vec<Option<PredicateColumnBatch>> =
            Vec::with_capacity(self.predicate_columns.len());

        for (idx, iter_opt) in self.predicate_iterators.iter_mut().enumerate() {
            let ty = &self.predicate_types[idx];
            if let Some(iter) = iter_opt.as_mut() {
                let (count, batch) = iter.next_batch(to_read)?;
                if let Some(expected) = rows_read {
                    if count != expected {
                        return Err(paro_error::data_corrupted(
                            "Predicate column row count mismatch",
                        ));
                    }
                } else {
                    rows_read = Some(count);
                }
                if self.decode_predicate_columns[idx] {
                    batches_by_col.push(Some(PredicateColumnBatch::Decoded(
                        vector_decoder::decode_column_batch(
                            ty,
                            &batch,
                            count,
                            self.allocator.clone(),
                            None,
                        )?,
                    )));
                } else {
                    Self::validate_raw_batch(&batch, ty.physical_size(), count)?;
                    batches_by_col.push(Some(PredicateColumnBatch::Raw(batch)));
                }
            } else {
                batches_by_col.push(None);
            }
        }

        let rows = rows_read.unwrap_or(to_read);
        let mut filled = Vec::with_capacity(batches_by_col.len());
        for (idx, batch) in batches_by_col.into_iter().enumerate() {
            match batch {
                Some(batch) => filled.push(batch),
                None => filled.push(PredicateColumnBatch::Decoded(Vector::try_constant_null(
                    self.predicate_types[idx].clone(),
                    rows,
                    self.allocator.clone(),
                )?)),
            }
        }

        Ok((rows, filled))
    }

    fn validate_raw_batch(batch: &ColumnBatch, width: usize, rows: usize) -> Result<()> {
        let expected = rows
            .checked_mul(width)
            .ok_or_else(|| paro_error::data_corrupted("Predicate batch width overflow"))?;
        if width == 0 || batch.data.len() != expected || batch.storage_dictionary.is_some() {
            return Err(paro_error::data_corrupted(
                "Fixed predicate batch has an invalid physical layout",
            ));
        }
        if batch.nulls.as_ref().is_some_and(|nulls| nulls.len() < rows) {
            return Err(paro_error::data_corrupted(
                "Predicate null map is shorter than the batch",
            ));
        }
        Ok(())
    }

    pub(super) fn evaluate_row(
        &self,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> Result<bool> {
        self.evaluate_tree(&self.tree, batches_by_col, row_idx)
    }

    fn evaluate_tree(
        &self,
        tree: &CompiledPredicateTree,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> Result<bool> {
        match tree {
            CompiledPredicateTree::Leaf(predicate) => {
                self.evaluate_leaf(predicate, batches_by_col, row_idx)
            }
            CompiledPredicateTree::And(children) => {
                for child in children {
                    if !self.evaluate_tree(child, batches_by_col, row_idx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            CompiledPredicateTree::Or(children) => {
                for child in children {
                    if self.evaluate_tree(child, batches_by_col, row_idx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    fn evaluate_leaf(
        &self,
        predicate: &CompiledPredicate,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> Result<bool> {
        match predicate {
            CompiledPredicate::I32Comparisons {
                column_idx,
                comparisons,
            } => Ok(batches_by_col.get(*column_idx).is_some_and(|batch| {
                if batch.is_null(row_idx) {
                    return false;
                }
                // SAFETY: read_next_batch validates fixed-width raw batches, and the
                // segment iterator only evaluates rows returned by that batch.
                let lhs = unsafe { batch.i32_value(row_idx) };
                comparisons
                    .iter()
                    .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
            })),
            CompiledPredicate::I64Comparisons {
                column_idx,
                comparisons,
            } => Ok(batches_by_col.get(*column_idx).is_some_and(|batch| {
                if batch.is_null(row_idx) {
                    return false;
                }
                // SAFETY: see the I32Comparisons branch above.
                let lhs = unsafe { batch.i64_value(row_idx) };
                comparisons
                    .iter()
                    .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
            })),
            CompiledPredicate::I128Comparisons {
                column_idx,
                comparisons,
            } => Ok(batches_by_col.get(*column_idx).is_some_and(|batch| {
                if batch.is_null(row_idx) {
                    return false;
                }
                // SAFETY: see the I32Comparisons branch above.
                let lhs = unsafe { batch.i128_value(row_idx) };
                comparisons
                    .iter()
                    .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
            })),
            CompiledPredicate::Generic {
                column_idx,
                predicate,
            } => self.evaluate_generic_leaf(*column_idx, predicate, batches_by_col, row_idx),
        }
    }

    fn evaluate_generic_leaf(
        &self,
        column_idx: usize,
        predicate: &Predicate,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> Result<bool> {
        let vector = batches_by_col
            .get(column_idx)
            .and_then(PredicateColumnBatch::decoded);
        match predicate {
            Predicate::Eq { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_eq))
            }
            Predicate::NotEq { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(|o| !o.is_eq()))
            }
            Predicate::Lt { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_lt))
            }
            Predicate::Le { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_le))
            }
            Predicate::Gt { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_gt))
            }
            Predicate::Ge { value: rhs, .. } => {
                Ok(Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_ge))
            }
            Predicate::In { values, .. } => {
                if vector.is_none_or(|vector| vector.is_null(row_idx)) {
                    return Ok(false);
                }
                Ok(values.iter().any(|rhs| {
                    Self::compare_value(vector, row_idx, rhs).is_some_and(Ordering::is_eq)
                }))
            }
            Predicate::Range { lower, upper, .. } => {
                let ge = Self::compare_value(vector, row_idx, lower).is_some_and(Ordering::is_ge);
                let le = Self::compare_value(vector, row_idx, upper).is_some_and(Ordering::is_le);
                Ok(ge && le)
            }
            Predicate::IsNull { .. } => Ok(vector.is_none_or(|vector| vector.is_null(row_idx))),
            Predicate::IsNotNull { .. } => {
                Ok(vector.is_some_and(|vector| !vector.is_null(row_idx)))
            }
        }
    }

    fn compare_value(vector: Option<&Vector>, row_idx: usize, rhs: &Value) -> Option<Ordering> {
        let vector = vector?;
        if vector.is_null(row_idx) {
            return None;
        }
        unsafe {
            match (vector.logical_type(), rhs) {
                (LogicalType::Date, Value::Date(rhs)) => {
                    vector.get_fixed::<i32>(row_idx).partial_cmp(rhs)
                }
                (LogicalType::Decimal { precision, .. }, Value::Decimal(rhs, _, _))
                    if *precision <= 18 =>
                {
                    (vector.get_fixed::<i64>(row_idx) as i128).partial_cmp(rhs)
                }
                (LogicalType::Decimal { .. }, Value::Decimal(rhs, _, _)) => {
                    vector.get_fixed::<i128>(row_idx).partial_cmp(rhs)
                }
                (LogicalType::Integer, Value::Integer(rhs)) => {
                    vector.get_fixed::<i32>(row_idx).partial_cmp(rhs)
                }
                (LogicalType::BigInt, Value::BigInt(rhs)) => {
                    vector.get_fixed::<i64>(row_idx).partial_cmp(rhs)
                }
                _ => vector.get_value(row_idx).partial_cmp(rhs),
            }
        }
    }

    fn compile_tree(
        tree: &PredicateTree,
        column_map: &HashMap<ColumnId, usize>,
        column_types: &[LogicalType],
    ) -> Result<CompiledPredicateTree> {
        match tree {
            PredicateTree::Leaf(predicate) => {
                let column_idx = *column_map.get(&predicate.column_id()).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Predicate column {} has no materialized vector",
                        predicate.column_id()
                    ))
                })?;
                let logical_type = column_types.get(column_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Predicate column index {column_idx} has no logical type"
                    ))
                })?;
                Ok(CompiledPredicateTree::Leaf(Self::compile_leaf(
                    column_idx,
                    logical_type,
                    predicate,
                )))
            }
            PredicateTree::And(children) => {
                let mut compiled = Vec::new();
                for child in children {
                    match Self::compile_tree(child, column_map, column_types)? {
                        CompiledPredicateTree::And(grandchildren) => compiled.extend(grandchildren),
                        child => compiled.push(child),
                    }
                }
                Ok(CompiledPredicateTree::And(
                    Self::coalesce_fixed_comparisons(compiled),
                ))
            }
            PredicateTree::Or(children) => Ok(CompiledPredicateTree::Or(
                children
                    .iter()
                    .map(|child| Self::compile_tree(child, column_map, column_types))
                    .collect::<Result<Vec<_>>>()?,
            )),
        }
    }

    fn mark_decoded_columns(tree: &CompiledPredicateTree, decoded: &mut [bool]) {
        match tree {
            CompiledPredicateTree::Leaf(CompiledPredicate::Generic { column_idx, .. }) => {
                decoded[*column_idx] = true;
            }
            CompiledPredicateTree::Leaf(_) => {}
            CompiledPredicateTree::And(children) | CompiledPredicateTree::Or(children) => {
                for child in children {
                    Self::mark_decoded_columns(child, decoded);
                }
            }
        }
    }

    fn compile_leaf(
        column_idx: usize,
        logical_type: &LogicalType,
        predicate: &Predicate,
    ) -> CompiledPredicate {
        let Some((operator, rhs)) = Self::comparison_parts(predicate) else {
            return CompiledPredicate::Generic {
                column_idx,
                predicate: predicate.clone(),
            };
        };
        match (logical_type, rhs) {
            (LogicalType::Date, Value::Date(rhs)) | (LogicalType::Integer, Value::Integer(rhs)) => {
                CompiledPredicate::I32Comparisons {
                    column_idx,
                    comparisons: vec![(operator, *rhs)],
                }
            }
            (LogicalType::BigInt, Value::BigInt(rhs)) => CompiledPredicate::I64Comparisons {
                column_idx,
                comparisons: vec![(operator, *rhs)],
            },
            (LogicalType::Decimal { precision, .. }, Value::Decimal(rhs, _, _))
                if *precision <= 18 && i64::try_from(*rhs).is_ok() =>
            {
                CompiledPredicate::I64Comparisons {
                    column_idx,
                    comparisons: vec![(operator, *rhs as i64)],
                }
            }
            (LogicalType::Decimal { .. }, Value::Decimal(rhs, _, _)) => {
                CompiledPredicate::I128Comparisons {
                    column_idx,
                    comparisons: vec![(operator, *rhs)],
                }
            }
            _ => CompiledPredicate::Generic {
                column_idx,
                predicate: predicate.clone(),
            },
        }
    }

    fn comparison_parts(predicate: &Predicate) -> Option<(ComparisonOperator, &Value)> {
        match predicate {
            Predicate::Eq { value, .. } => Some((ComparisonOperator::Equal, value)),
            Predicate::NotEq { value, .. } => Some((ComparisonOperator::NotEqual, value)),
            Predicate::Lt { value, .. } => Some((ComparisonOperator::LessThan, value)),
            Predicate::Le { value, .. } => Some((ComparisonOperator::LessThanOrEqual, value)),
            Predicate::Gt { value, .. } => Some((ComparisonOperator::GreaterThan, value)),
            Predicate::Ge { value, .. } => Some((ComparisonOperator::GreaterThanOrEqual, value)),
            _ => None,
        }
    }

    fn coalesce_fixed_comparisons(
        predicates: Vec<CompiledPredicateTree>,
    ) -> Vec<CompiledPredicateTree> {
        let mut result: Vec<CompiledPredicateTree> = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            match predicate {
                CompiledPredicateTree::Leaf(CompiledPredicate::I32Comparisons {
                    column_idx,
                    comparisons,
                }) => {
                    let existing = result.iter_mut().find_map(|predicate| match predicate {
                        CompiledPredicateTree::Leaf(CompiledPredicate::I32Comparisons {
                            column_idx: existing_idx,
                            comparisons,
                        }) if *existing_idx == column_idx => Some(comparisons),
                        _ => None,
                    });
                    if let Some(existing) = existing {
                        existing.extend(comparisons);
                    } else {
                        result.push(CompiledPredicateTree::Leaf(
                            CompiledPredicate::I32Comparisons {
                                column_idx,
                                comparisons,
                            },
                        ));
                    }
                }
                CompiledPredicateTree::Leaf(CompiledPredicate::I64Comparisons {
                    column_idx,
                    comparisons,
                }) => {
                    let existing = result.iter_mut().find_map(|predicate| match predicate {
                        CompiledPredicateTree::Leaf(CompiledPredicate::I64Comparisons {
                            column_idx: existing_idx,
                            comparisons,
                        }) if *existing_idx == column_idx => Some(comparisons),
                        _ => None,
                    });
                    if let Some(existing) = existing {
                        existing.extend(comparisons);
                    } else {
                        result.push(CompiledPredicateTree::Leaf(
                            CompiledPredicate::I64Comparisons {
                                column_idx,
                                comparisons,
                            },
                        ));
                    }
                }
                CompiledPredicateTree::Leaf(CompiledPredicate::I128Comparisons {
                    column_idx,
                    comparisons,
                }) => {
                    let existing = result.iter_mut().find_map(|predicate| match predicate {
                        CompiledPredicateTree::Leaf(CompiledPredicate::I128Comparisons {
                            column_idx: existing_idx,
                            comparisons,
                        }) if *existing_idx == column_idx => Some(comparisons),
                        _ => None,
                    });
                    if let Some(existing) = existing {
                        existing.extend(comparisons);
                    } else {
                        result.push(CompiledPredicateTree::Leaf(
                            CompiledPredicate::I128Comparisons {
                                column_idx,
                                comparisons,
                            },
                        ));
                    }
                }
                predicate => result.push(predicate),
            }
        }
        result
    }
}

trait PredicateVerificationExt {
    fn requires_row_level_verification(&self) -> bool;
}

impl PredicateVerificationExt for Predicate {
    fn requires_row_level_verification(&self) -> bool {
        matches!(
            self,
            Predicate::NotEq { .. }
                | Predicate::Lt { .. }
                | Predicate::Le { .. }
                | Predicate::Gt { .. }
                | Predicate::Ge { .. }
                | Predicate::Range { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_comparison_tree(
        conjunction: fn(Vec<PredicateTree>) -> PredicateTree,
    ) -> PredicateTree {
        conjunction(vec![
            PredicateTree::leaf(Predicate::Ge {
                column_id: 7,
                value: Value::Integer(10),
            }),
            PredicateTree::leaf(Predicate::Lt {
                column_id: 7,
                value: Value::Integer(20),
            }),
        ])
    }

    #[test]
    fn compile_tree_coalesces_same_column_comparisons_only_inside_and() {
        let column_map = HashMap::from([(7, 0)]);
        let column_types = [LogicalType::Integer];

        let compiled_and = PredicateEvaluator::compile_tree(
            &integer_comparison_tree(PredicateTree::And),
            &column_map,
            &column_types,
        )
        .unwrap();
        let CompiledPredicateTree::And(and_children) = compiled_and else {
            panic!("expected compiled AND");
        };
        assert_eq!(and_children.len(), 1);
        assert!(matches!(
            &and_children[0],
            CompiledPredicateTree::Leaf(CompiledPredicate::I32Comparisons {
                column_idx: 0,
                comparisons,
            }) if comparisons.len() == 2
        ));

        let compiled_or = PredicateEvaluator::compile_tree(
            &integer_comparison_tree(PredicateTree::Or),
            &column_map,
            &column_types,
        )
        .unwrap();
        let CompiledPredicateTree::Or(or_children) = compiled_or else {
            panic!("expected compiled OR");
        };
        assert_eq!(or_children.len(), 2);
    }

    #[test]
    fn fixed_comparisons_read_raw_column_batches() {
        let column_map = HashMap::from([(7, 0)]);
        let column_types = [LogicalType::Integer];
        let tree = PredicateEvaluator::compile_tree(
            &integer_comparison_tree(PredicateTree::And),
            &column_map,
            &column_types,
        )
        .unwrap();
        let evaluator = PredicateEvaluator {
            tree,
            predicate_columns: vec![7],
            predicate_types: column_types.to_vec(),
            predicate_iterators: std::iter::once(None).collect(),
            decode_predicate_columns: vec![false],
            allocator: Arc::new(default_allocator()),
        };
        let values = [5_i32, 10, 19, 20];
        let data = values
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        let batches = [PredicateColumnBatch::Raw(ColumnBatch::new(
            bytes::Bytes::from(data),
            Some(bytes::Bytes::from_static(&[0, 0, 1, 0])),
        ))];

        assert!(!evaluator.evaluate_row(&batches, 0).unwrap());
        assert!(evaluator.evaluate_row(&batches, 1).unwrap());
        assert!(!evaluator.evaluate_row(&batches, 2).unwrap());
        assert!(!evaluator.evaluate_row(&batches, 3).unwrap());
    }

    #[test]
    fn generic_predicates_require_decoded_column_batches() {
        let column_map = HashMap::from([(7, 0)]);
        let column_types = [LogicalType::Integer];
        let tree = PredicateEvaluator::compile_tree(
            &PredicateTree::And(vec![
                PredicateTree::leaf(Predicate::Ge {
                    column_id: 7,
                    value: Value::Integer(10),
                }),
                PredicateTree::leaf(Predicate::IsNotNull { column_id: 7 }),
            ]),
            &column_map,
            &column_types,
        )
        .unwrap();
        let mut decoded = [false];

        PredicateEvaluator::mark_decoded_columns(&tree, &mut decoded);

        assert_eq!(decoded, [true]);
    }
}
