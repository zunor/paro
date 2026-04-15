// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::Segment;
use crate::buffer::Prefetcher;
use crate::codec::vector_decoder;
use crate::index::{
    collect_predicate_columns, IndexEvaluator, Predicate, PredicateResult, PredicateTree,
};
use crate::rowset::column::ColumnIterator;
use crate::tablet::ColumnId;
use paro_common::allocator::default_allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct PredicateEvaluator {
    tree: PredicateTree,
    predicate_columns: Vec<ColumnId>,
    predicate_types: Vec<LogicalType>,
    predicate_column_map: HashMap<ColumnId, usize>,
    predicate_iterators: Vec<Option<Box<dyn ColumnIterator + Send + Sync>>>,
}

impl PredicateEvaluator {
    pub(super) fn new(
        segment: &Segment,
        tree: PredicateTree,
        evaluator: &IndexEvaluator,
        prefetcher: Option<Arc<Prefetcher>>,
        explicit_predicate_columns: Option<Vec<ColumnId>>,
    ) -> Result<Option<Self>> {
        if !Self::requires_row_level_predicate_eval(evaluator, &tree) {
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

        Ok(Some(Self {
            tree,
            predicate_columns: columns,
            predicate_types: types,
            predicate_column_map: column_map,
            predicate_iterators: iterators,
        }))
    }

    pub(super) fn requires_row_level_predicate_eval(
        evaluator: &IndexEvaluator,
        predicate_tree: &PredicateTree,
    ) -> bool {
        match predicate_tree {
            PredicateTree::Leaf(predicate) => {
                let leaf = PredicateTree::Leaf(predicate.clone());
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

    pub(super) fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        for iter in self.predicate_iterators.iter_mut().flatten() {
            iter.seek_to_ordinal(ordinal)?;
        }
        Ok(())
    }

    pub(super) fn read_next_batch(&mut self, to_read: usize) -> Result<(usize, Vec<Vec<Value>>)> {
        if to_read == 0 {
            return Ok((0, Vec::new()));
        }

        let mut rows_read: Option<usize> = None;
        let mut values_by_col: Vec<Option<Vec<Value>>> =
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
                let values = if batch.storage_dictionary.is_some() {
                    let vector = vector_decoder::decode_column_batch(
                        ty,
                        &batch,
                        count,
                        Arc::new(default_allocator()),
                        None,
                    )?;
                    (0..count)
                        .map(|row_idx| vector.get_value(row_idx))
                        .collect()
                } else {
                    crate::codec::value_decoder::decode_values(
                        ty,
                        &batch.data,
                        batch.nulls.as_deref(),
                        count,
                    )?
                };
                values_by_col.push(Some(values));
            } else {
                values_by_col.push(None);
            }
        }

        let rows = rows_read.unwrap_or(to_read);
        let mut filled = Vec::with_capacity(values_by_col.len());
        for (idx, values) in values_by_col.into_iter().enumerate() {
            match values {
                Some(vals) => filled.push(vals),
                None => filled.push(vec![Value::Null(self.predicate_types[idx].clone()); rows]),
            }
        }

        Ok((rows, filled))
    }

    pub(super) fn evaluate_row(
        &self,
        values_by_col: &[Vec<Value>],
        row_idx: usize,
    ) -> Result<bool> {
        self.evaluate_tree(&self.tree, values_by_col, row_idx)
    }

    fn evaluate_tree(
        &self,
        tree: &PredicateTree,
        values_by_col: &[Vec<Value>],
        row_idx: usize,
    ) -> Result<bool> {
        match tree {
            PredicateTree::Leaf(predicate) => self.evaluate_leaf(predicate, values_by_col, row_idx),
            PredicateTree::And(children) => {
                for child in children {
                    if !self.evaluate_tree(child, values_by_col, row_idx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PredicateTree::Or(children) => {
                for child in children {
                    if self.evaluate_tree(child, values_by_col, row_idx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    fn evaluate_leaf(
        &self,
        predicate: &Predicate,
        values_by_col: &[Vec<Value>],
        row_idx: usize,
    ) -> Result<bool> {
        let value = self.get_row_value(values_by_col, predicate.column_id(), row_idx);
        match predicate {
            Predicate::Eq { value: rhs, .. } => {
                Ok(!Self::is_null_value(value) && value.is_some_and(|v| v == rhs))
            }
            Predicate::NotEq { value: rhs, .. } => {
                Ok(!Self::is_null_value(value) && value.is_some_and(|v| v != rhs))
            }
            Predicate::Lt { value: rhs, .. } => Self::compare_value(value, rhs, |o| o.is_lt()),
            Predicate::Le { value: rhs, .. } => Self::compare_value(value, rhs, |o| o.is_le()),
            Predicate::Gt { value: rhs, .. } => Self::compare_value(value, rhs, |o| o.is_gt()),
            Predicate::Ge { value: rhs, .. } => Self::compare_value(value, rhs, |o| o.is_ge()),
            Predicate::In { values, .. } => {
                if Self::is_null_value(value) {
                    return Ok(false);
                }
                Ok(value.is_some_and(|lhs| values.iter().any(|rhs| rhs == lhs)))
            }
            Predicate::Range { lower, upper, .. } => {
                if Self::is_null_value(value) {
                    return Ok(false);
                }
                let lhs = value.expect("checked above");
                let ge = lhs.partial_cmp(lower).map(|o| o.is_ge()).unwrap_or(false);
                let le = lhs.partial_cmp(upper).map(|o| o.is_le()).unwrap_or(false);
                Ok(ge && le)
            }
            Predicate::IsNull { .. } => Ok(Self::is_null_value(value)),
            Predicate::IsNotNull { .. } => Ok(!Self::is_null_value(value)),
        }
    }

    fn compare_value<F>(value: Option<&Value>, rhs: &Value, cmp: F) -> Result<bool>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        if Self::is_null_value(value) {
            return Ok(false);
        }
        Ok(value
            .expect("checked above")
            .partial_cmp(rhs)
            .map(cmp)
            .unwrap_or(false))
    }

    fn is_null_value(value: Option<&Value>) -> bool {
        matches!(value, None | Some(Value::Null(_)))
    }

    fn get_row_value<'a>(
        &self,
        values_by_col: &'a [Vec<Value>],
        column_id: ColumnId,
        row_idx: usize,
    ) -> Option<&'a Value> {
        let idx = self.predicate_column_map.get(&column_id)?;
        values_by_col.get(*idx)?.get(row_idx)
    }
}
