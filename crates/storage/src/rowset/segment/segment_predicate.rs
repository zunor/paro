// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::Segment;
use crate::buffer::Prefetcher;
use crate::codec::vector_decoder;
use crate::index::{
    collect_predicate_columns, IndexEvaluator, Predicate, PredicateComparison, PredicateResult,
    PredicateTree,
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
    predicate_column_access: Vec<PredicateColumnAccess>,
    allocator: Arc<dyn Allocator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateColumnAccess {
    Unused,
    Raw { width: usize },
    Decoded,
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

    pub(super) fn raw(&self) -> Option<&ColumnBatch> {
        match self {
            Self::Raw(batch) => Some(batch),
            Self::Decoded(_) => None,
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

impl From<PredicateComparison> for ComparisonOperator {
    fn from(value: PredicateComparison) -> Self {
        match value {
            PredicateComparison::Equal => Self::Equal,
            PredicateComparison::NotEqual => Self::NotEqual,
            PredicateComparison::LessThan => Self::LessThan,
            PredicateComparison::LessThanOrEqual => Self::LessThanOrEqual,
            PredicateComparison::GreaterThan => Self::GreaterThan,
            PredicateComparison::GreaterThanOrEqual => Self::GreaterThanOrEqual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedComparisonWidth {
    I32,
    I64,
    I128,
}

impl FixedComparisonWidth {
    fn from_logical_type(logical_type: &LogicalType) -> Option<Self> {
        match logical_type {
            LogicalType::Date | LogicalType::Integer => Some(Self::I32),
            LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => Some(Self::I64),
            LogicalType::Decimal { precision, .. } if *precision <= 18 => Some(Self::I64),
            LogicalType::Decimal { .. } | LogicalType::Interval | LogicalType::Uuid => {
                Some(Self::I128)
            }
            _ => None,
        }
    }

    fn bytes(self) -> usize {
        match self {
            Self::I32 => std::mem::size_of::<i32>(),
            Self::I64 => std::mem::size_of::<i64>(),
            Self::I128 => std::mem::size_of::<i128>(),
        }
    }
}

enum CompiledPredicateTree {
    Leaf(CompiledPredicate),
    And(Vec<CompiledPredicateTree>),
    Or(Vec<CompiledPredicateTree>),
}

enum FixedComparisonValues {
    I32(Vec<(ComparisonOperator, i32)>),
    I64(Vec<(ComparisonOperator, i64)>),
    I128(Vec<(ComparisonOperator, i128)>),
}

impl FixedComparisonValues {
    fn physical_width(&self) -> usize {
        match self {
            Self::I32(_) => std::mem::size_of::<i32>(),
            Self::I64(_) => std::mem::size_of::<i64>(),
            Self::I128(_) => std::mem::size_of::<i128>(),
        }
    }

    fn extend_same_type(&mut self, other: &mut Self) -> bool {
        match (self, other) {
            (Self::I32(existing), Self::I32(incoming)) => existing.append(incoming),
            (Self::I64(existing), Self::I64(incoming)) => existing.append(incoming),
            (Self::I128(existing), Self::I128(incoming)) => existing.append(incoming),
            _ => return false,
        }
        true
    }
}

enum CompiledPredicate {
    FixedComparisons {
        column_idx: usize,
        comparisons: FixedComparisonValues,
    },
    Generic {
        column_idx: usize,
        predicate: Predicate,
    },
    FixedColumnComparison {
        left_column_idx: usize,
        right_column_idx: usize,
        comparison: ComparisonOperator,
        width: FixedComparisonWidth,
    },
}

impl PredicateEvaluator {
    pub(super) fn raw_column_info(&self, column_id: ColumnId) -> Option<(usize, usize)> {
        let column_idx = self
            .predicate_columns
            .iter()
            .position(|candidate| *candidate == column_id)?;
        match self.predicate_column_access[column_idx] {
            PredicateColumnAccess::Raw { width } => Some((column_idx, width)),
            PredicateColumnAccess::Unused | PredicateColumnAccess::Decoded => None,
        }
    }

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
        let mut predicate_column_access = vec![PredicateColumnAccess::Unused; columns.len()];
        Self::mark_column_access(&tree, &mut predicate_column_access)?;
        for (idx, access) in predicate_column_access.iter_mut().enumerate() {
            match access {
                PredicateColumnAccess::Raw { width } if *width != types[idx].physical_size() => {
                    return Err(paro_error::internal(format!(
                        "Compiled predicate width {width} disagrees with {:?} physical width {}",
                        types[idx],
                        types[idx].physical_size(),
                    )));
                }
                PredicateColumnAccess::Unused => *access = PredicateColumnAccess::Decoded,
                PredicateColumnAccess::Raw { .. } | PredicateColumnAccess::Decoded => {}
            }
        }
        Ok(Some(Self {
            tree,
            predicate_columns: columns,
            predicate_types: types,
            predicate_iterators: iterators,
            predicate_column_access,
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
                batches_by_col.push(Some(Self::prepare_column_batch(
                    ty,
                    self.predicate_column_access[idx],
                    batch,
                    count,
                    self.allocator.clone(),
                )?));
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

    fn prepare_column_batch(
        ty: &LogicalType,
        access: PredicateColumnAccess,
        batch: ColumnBatch,
        rows: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<PredicateColumnBatch> {
        if let PredicateColumnAccess::Raw { width } = access {
            if batch.storage_dictionary.is_none() {
                Self::validate_raw_batch(&batch, width, rows)?;
                return Ok(PredicateColumnBatch::Raw(batch));
            }
        }

        Ok(PredicateColumnBatch::Decoded(
            vector_decoder::decode_column_batch(ty, &batch, rows, allocator, None)?,
        ))
    }

    fn validate_raw_batch(batch: &ColumnBatch, width: usize, rows: usize) -> Result<()> {
        let expected = rows
            .checked_mul(width)
            .ok_or_else(|| paro_error::data_corrupted("Predicate batch width overflow"))?;
        if width == 0 || batch.data.len() != expected {
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

    pub(super) fn evaluate_batch(
        &self,
        batches_by_col: &[PredicateColumnBatch],
        rows: usize,
        matches: &mut Vec<usize>,
    ) -> Result<()> {
        matches.clear();
        let CompiledPredicateTree::And(children) = &self.tree else {
            return self.evaluate_batch_generic(batches_by_col, rows, matches);
        };
        let Some((first, remaining)) = children.split_first() else {
            matches.extend(0..rows);
            return Ok(());
        };
        if !children.iter().all(Self::is_fixed_comparison_leaf) {
            return self.evaluate_batch_generic(batches_by_col, rows, matches);
        }

        for row_idx in 0..rows {
            if Self::evaluate_fixed_comparison_leaf(first, batches_by_col, row_idx) {
                matches.push(row_idx);
            }
        }
        for predicate in remaining {
            matches.retain(|&row_idx| {
                Self::evaluate_fixed_comparison_leaf(predicate, batches_by_col, row_idx)
            });
        }
        Ok(())
    }

    fn evaluate_batch_generic(
        &self,
        batches_by_col: &[PredicateColumnBatch],
        rows: usize,
        matches: &mut Vec<usize>,
    ) -> Result<()> {
        for row_idx in 0..rows {
            if self.evaluate_tree(&self.tree, batches_by_col, row_idx)? {
                matches.push(row_idx);
            }
        }
        Ok(())
    }

    fn is_fixed_comparison_leaf(predicate: &CompiledPredicateTree) -> bool {
        matches!(
            predicate,
            CompiledPredicateTree::Leaf(
                CompiledPredicate::FixedComparisons { .. }
                    | CompiledPredicate::FixedColumnComparison { .. }
            )
        )
    }

    #[inline]
    fn evaluate_fixed_comparison_leaf(
        predicate: &CompiledPredicateTree,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> bool {
        let CompiledPredicateTree::Leaf(predicate) = predicate else {
            return false;
        };
        Self::evaluate_fixed_comparison(predicate, batches_by_col, row_idx).unwrap_or(false)
    }

    #[inline]
    fn evaluate_fixed_comparison(
        predicate: &CompiledPredicate,
        batches_by_col: &[PredicateColumnBatch],
        row_idx: usize,
    ) -> Option<bool> {
        match predicate {
            CompiledPredicate::FixedComparisons {
                column_idx,
                comparisons,
            } => Some(batches_by_col.get(*column_idx).is_some_and(|batch| {
                if batch.is_null(row_idx) {
                    return false;
                }
                // SAFETY: construction cross-checks the comparison width
                // with the schema, read_next_batch validates raw layouts,
                // and row_idx is bounded by the batch row count.
                unsafe {
                    match comparisons {
                        FixedComparisonValues::I32(comparisons) => {
                            let lhs = batch.i32_value(row_idx);
                            comparisons
                                .iter()
                                .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
                        }
                        FixedComparisonValues::I64(comparisons) => {
                            let lhs = batch.i64_value(row_idx);
                            comparisons
                                .iter()
                                .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
                        }
                        FixedComparisonValues::I128(comparisons) => {
                            let lhs = batch.i128_value(row_idx);
                            comparisons
                                .iter()
                                .all(|(operator, rhs)| operator.matches(lhs.cmp(rhs)))
                        }
                    }
                }
            })),
            CompiledPredicate::Generic { .. } => None,
            CompiledPredicate::FixedColumnComparison {
                left_column_idx,
                right_column_idx,
                comparison,
                width,
            } => {
                let left = batches_by_col.get(*left_column_idx)?;
                let right = batches_by_col.get(*right_column_idx)?;
                if left.is_null(row_idx) || right.is_null(row_idx) {
                    return Some(false);
                }
                // SAFETY: compile_tree requires identical logical types, the
                // access planner validates both physical widths, and row_idx is
                // bounded by the shared predicate batch row count.
                Some(unsafe {
                    let ordering = match width {
                        FixedComparisonWidth::I32 => {
                            left.i32_value(row_idx).cmp(&right.i32_value(row_idx))
                        }
                        FixedComparisonWidth::I64 => {
                            left.i64_value(row_idx).cmp(&right.i64_value(row_idx))
                        }
                        FixedComparisonWidth::I128 => {
                            left.i128_value(row_idx).cmp(&right.i128_value(row_idx))
                        }
                    };
                    comparison.matches(ordering)
                })
            }
        }
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
            CompiledPredicate::FixedComparisons { .. }
            | CompiledPredicate::FixedColumnComparison { .. } => Ok(
                Self::evaluate_fixed_comparison(predicate, batches_by_col, row_idx)
                    .unwrap_or(false),
            ),
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
            Predicate::ColumnComparison { .. } => Err(paro_error::internal(
                "Column comparison was not compiled to its fixed-width evaluator",
            )),
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
                if let Predicate::ColumnComparison {
                    left_column_id,
                    right_column_id,
                    comparison,
                } = predicate
                {
                    let left_column_idx = *column_map.get(left_column_id).ok_or_else(|| {
                        paro_error::internal(format!(
                            "Predicate column {left_column_id} has no materialized vector"
                        ))
                    })?;
                    let right_column_idx = *column_map.get(right_column_id).ok_or_else(|| {
                        paro_error::internal(format!(
                            "Predicate column {right_column_id} has no materialized vector"
                        ))
                    })?;
                    let left_type = column_types.get(left_column_idx).ok_or_else(|| {
                        paro_error::internal(format!(
                            "Predicate column index {left_column_idx} has no logical type"
                        ))
                    })?;
                    let right_type = column_types.get(right_column_idx).ok_or_else(|| {
                        paro_error::internal(format!(
                            "Predicate column index {right_column_idx} has no logical type"
                        ))
                    })?;
                    if left_type != right_type {
                        return Err(paro_error::internal(format!(
                            "Column comparison types differ: left={left_type:?}, right={right_type:?}"
                        )));
                    }
                    let width =
                        FixedComparisonWidth::from_logical_type(left_type).ok_or_else(|| {
                            paro_error::internal(format!(
                                "Unsupported raw column comparison type: {left_type:?}"
                            ))
                        })?;
                    return Ok(CompiledPredicateTree::Leaf(
                        CompiledPredicate::FixedColumnComparison {
                            left_column_idx,
                            right_column_idx,
                            comparison: (*comparison).into(),
                            width,
                        },
                    ));
                }
                let column_id = predicate.index_column_id().ok_or_else(|| {
                    paro_error::internal("Predicate has no index column or column comparison")
                })?;
                let column_idx = *column_map.get(&column_id).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Predicate column {column_id} has no materialized vector"
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

    fn mark_column_access(
        tree: &CompiledPredicateTree,
        access: &mut [PredicateColumnAccess],
    ) -> Result<()> {
        match tree {
            CompiledPredicateTree::Leaf(CompiledPredicate::Generic { column_idx, .. }) => {
                access[*column_idx] = PredicateColumnAccess::Decoded;
            }
            CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
                column_idx,
                comparisons,
            }) => {
                let width = comparisons.physical_width();
                match access[*column_idx] {
                    PredicateColumnAccess::Unused => {
                        access[*column_idx] = PredicateColumnAccess::Raw { width };
                    }
                    PredicateColumnAccess::Raw {
                        width: existing_width,
                    } if existing_width != width => {
                        return Err(paro_error::internal(format!(
                            "Predicate column {column_idx} compiled with conflicting widths {existing_width} and {width}"
                        )));
                    }
                    PredicateColumnAccess::Raw { .. } | PredicateColumnAccess::Decoded => {}
                }
            }
            CompiledPredicateTree::Leaf(CompiledPredicate::FixedColumnComparison {
                left_column_idx,
                right_column_idx,
                width,
                ..
            }) => {
                for column_idx in [*left_column_idx, *right_column_idx] {
                    match access[column_idx] {
                        PredicateColumnAccess::Unused => {
                            access[column_idx] = PredicateColumnAccess::Raw {
                                width: width.bytes(),
                            };
                        }
                        PredicateColumnAccess::Raw {
                            width: existing_width,
                        } if existing_width != width.bytes() => {
                            return Err(paro_error::internal(format!(
                                "Predicate column {column_idx} compiled with conflicting widths {existing_width} and {}",
                                width.bytes()
                            )));
                        }
                        PredicateColumnAccess::Raw { .. } | PredicateColumnAccess::Decoded => {}
                    }
                }
            }
            CompiledPredicateTree::And(children) | CompiledPredicateTree::Or(children) => {
                for child in children {
                    Self::mark_column_access(child, access)?;
                }
            }
        }
        Ok(())
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
                CompiledPredicate::FixedComparisons {
                    column_idx,
                    comparisons: FixedComparisonValues::I32(vec![(operator, *rhs)]),
                }
            }
            (LogicalType::BigInt, Value::BigInt(rhs)) => CompiledPredicate::FixedComparisons {
                column_idx,
                comparisons: FixedComparisonValues::I64(vec![(operator, *rhs)]),
            },
            (LogicalType::Decimal { precision, .. }, Value::Decimal(rhs, _, _))
                if *precision <= 18 =>
            {
                let Ok(rhs) = i64::try_from(*rhs) else {
                    return CompiledPredicate::Generic {
                        column_idx,
                        predicate: predicate.clone(),
                    };
                };
                CompiledPredicate::FixedComparisons {
                    column_idx,
                    comparisons: FixedComparisonValues::I64(vec![(operator, rhs)]),
                }
            }
            (LogicalType::Decimal { .. }, Value::Decimal(rhs, _, _)) => {
                CompiledPredicate::FixedComparisons {
                    column_idx,
                    comparisons: FixedComparisonValues::I128(vec![(operator, *rhs)]),
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
                CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
                    column_idx,
                    mut comparisons,
                }) => {
                    let existing = result.iter_mut().find_map(|predicate| match predicate {
                        CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
                            column_idx: existing_idx,
                            comparisons,
                        }) if *existing_idx == column_idx => Some(comparisons),
                        _ => None,
                    });
                    if existing.is_none_or(|existing| !existing.extend_same_type(&mut comparisons))
                    {
                        result.push(CompiledPredicateTree::Leaf(
                            CompiledPredicate::FixedComparisons {
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
                | Predicate::ColumnComparison { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::encoding::BinaryPlainPageBuilder;
    use bytes::Bytes;

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
            CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
                column_idx: 0,
                comparisons: FixedComparisonValues::I32(comparisons),
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
            predicate_column_access: vec![PredicateColumnAccess::Raw {
                width: std::mem::size_of::<i32>(),
            }],
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

        let mut matches = Vec::new();
        evaluator.evaluate_batch(&batches, 4, &mut matches).unwrap();
        assert_eq!(matches, [1]);
    }

    #[test]
    fn fixed_column_comparison_reads_both_raw_batches_with_filter_null_semantics() {
        let column_map = HashMap::from([(7, 0), (8, 1)]);
        let column_types = [LogicalType::Date, LogicalType::Date];
        let tree = PredicateEvaluator::compile_tree(
            &PredicateTree::leaf(Predicate::ColumnComparison {
                left_column_id: 7,
                right_column_id: 8,
                comparison: PredicateComparison::LessThan,
            }),
            &column_map,
            &column_types,
        )
        .unwrap();
        let mut access = [PredicateColumnAccess::Unused; 2];
        PredicateEvaluator::mark_column_access(&tree, &mut access).unwrap();
        assert_eq!(
            access,
            [
                PredicateColumnAccess::Raw { width: 4 },
                PredicateColumnAccess::Raw { width: 4 }
            ]
        );
        let evaluator = PredicateEvaluator {
            tree,
            predicate_columns: vec![7, 8],
            predicate_types: column_types.to_vec(),
            predicate_iterators: std::iter::repeat_with(|| None).take(2).collect(),
            predicate_column_access: access.to_vec(),
            allocator: Arc::new(default_allocator()),
        };
        let raw_batch = |values: [i32; 4], nulls: &'static [u8]| {
            PredicateColumnBatch::Raw(ColumnBatch::new(
                Bytes::from(
                    values
                        .into_iter()
                        .flat_map(i32::to_le_bytes)
                        .collect::<Vec<_>>(),
                ),
                Some(Bytes::from_static(nulls)),
            ))
        };
        let batches = [
            raw_batch([1, 3, 5, 7], &[0, 0, 1, 0]),
            raw_batch([2, 2, 9, 8], &[0, 0, 0, 1]),
        ];

        let mut matches = Vec::new();
        evaluator.evaluate_batch(&batches, 4, &mut matches).unwrap();

        assert_eq!(matches, [0]);
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
        let mut access = [PredicateColumnAccess::Unused];

        PredicateEvaluator::mark_column_access(&tree, &mut access).unwrap();

        assert_eq!(access, [PredicateColumnAccess::Decoded]);
    }

    #[test]
    fn narrow_decimal_never_compiles_to_a_wider_physical_reader() {
        let predicate = Predicate::Eq {
            column_id: 7,
            value: Value::Decimal(i128::MAX, 18, 0),
        };

        let compiled = PredicateEvaluator::compile_leaf(
            0,
            &LogicalType::Decimal {
                precision: 18,
                scale: 0,
            },
            &predicate,
        );

        assert!(matches!(compiled, CompiledPredicate::Generic { .. }));
    }

    #[test]
    fn raw_fixed_predicate_falls_back_to_decoding_a_storage_dictionary() {
        let mut dictionary = BinaryPlainPageBuilder::new(1024);
        assert!(dictionary.add_slice(&5_i32.to_le_bytes()));
        assert!(dictionary.add_slice(&11_i32.to_le_bytes()));
        let dictionary = dictionary.finish().unwrap();
        let codes = [0_u32, 1, 0]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let batch = ColumnBatch::with_storage_dictionary(dictionary, Bytes::from(codes), None);

        let prepared = PredicateEvaluator::prepare_column_batch(
            &LogicalType::Integer,
            PredicateColumnAccess::Raw {
                width: std::mem::size_of::<i32>(),
            },
            batch,
            3,
            Arc::new(default_allocator()),
        )
        .unwrap();

        let PredicateColumnBatch::Decoded(vector) = prepared else {
            panic!("storage dictionary must be decoded before fixed predicate evaluation");
        };
        assert_eq!(unsafe { vector.get_fixed::<i32>(0) }, 5);
        assert_eq!(unsafe { vector.get_fixed::<i32>(1) }, 11);
        assert_eq!(unsafe { vector.get_fixed::<i32>(2) }, 5);
    }
}
