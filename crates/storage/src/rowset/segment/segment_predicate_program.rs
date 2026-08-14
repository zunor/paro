// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Staged execution for conjunctive storage predicates.
//!
//! A staged program groups constant predicate leaves by physical column. The
//! first column is read sequentially; later columns may gather only surviving
//! row ids. Every column iterator is advanced to the common batch boundary so
//! the next invocation can choose either access mode independently.

use super::predicate_column::PredicateColumnBatch;
use super::segment_predicate::{CompiledPredicate, CompiledPredicateTree, PredicateEvaluator};
use crate::rowset::column::OrderedRowIds;
use crate::rowset::scan_cost::ScanAccessCostModel;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;

#[derive(Debug, Clone, Default)]
pub(super) struct PredicateStageReadStats {
    #[cfg(test)]
    pub(super) stages: Vec<PredicateStageReadCounter>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PredicateStageReadCounter {
    pub(super) sequential_rows: u64,
    pub(super) gathered_rows: u64,
}

impl PredicateStageReadStats {
    #[cfg(test)]
    fn stage_mut(&mut self, stage_idx: usize) -> &mut PredicateStageReadCounter {
        if self.stages.len() <= stage_idx {
            self.stages
                .resize(stage_idx + 1, PredicateStageReadCounter::default());
        }
        &mut self.stages[stage_idx]
    }

    fn add_sequential(&mut self, stage_idx: usize, rows: usize) {
        #[cfg(test)]
        {
            self.stage_mut(stage_idx).sequential_rows += rows as u64;
        }
        #[cfg(not(test))]
        let _ = (stage_idx, rows);
    }

    fn add_gathered(&mut self, stage_idx: usize, rows: usize) {
        #[cfg(test)]
        {
            self.stage_mut(stage_idx).gathered_rows += rows as u64;
        }
        #[cfg(not(test))]
        let _ = (stage_idx, rows);
    }
}

pub(super) struct CompiledPredicateProgram {
    pub(super) tree: CompiledPredicateTree,
    stages: Option<Box<[PredicateStage]>>,
    stage_children: Box<[usize]>,
}

#[derive(Clone, Copy)]
struct PredicateStage {
    column_idx: usize,
    child_start: usize,
    child_end: usize,
}

#[derive(Default)]
pub(super) struct PredicateStageScratch {
    absolute_rowids: Vec<u32>,
    candidate_rows: Vec<usize>,
}

impl CompiledPredicateProgram {
    pub(super) fn new(tree: CompiledPredicateTree, allow_staged_access: bool) -> Self {
        let compiled = allow_staged_access
            .then(|| Self::compile_stages(&tree))
            .flatten();
        let (stages, stage_children) = compiled.map_or_else(
            || (None, Box::default()),
            |(stages, children)| (Some(stages), children),
        );
        Self {
            tree,
            stages,
            stage_children,
        }
    }

    #[cfg(test)]
    pub(super) fn legacy(tree: CompiledPredicateTree) -> Self {
        Self {
            tree,
            stages: None,
            stage_children: Box::default(),
        }
    }

    pub(super) fn is_staged(&self) -> bool {
        self.stages.is_some()
    }

    fn stages(&self) -> Option<&[PredicateStage]> {
        self.stages.as_deref()
    }

    fn compile_stages(
        tree: &CompiledPredicateTree,
    ) -> Option<(Box<[PredicateStage]>, Box<[usize]>)> {
        let CompiledPredicateTree::And(children) = tree else {
            return None;
        };
        let mut columns = Vec::<(usize, Vec<usize>)>::new();
        for (child_idx, child) in children.iter().enumerate() {
            let CompiledPredicateTree::Leaf(predicate) = child else {
                return None;
            };
            let column_idx = match predicate {
                CompiledPredicate::FixedComparisons { column_idx, .. }
                | CompiledPredicate::VarlenComparisons { column_idx, .. }
                | CompiledPredicate::VarlenMatch { column_idx, .. } => *column_idx,
                CompiledPredicate::Generic { .. }
                | CompiledPredicate::FixedColumnComparison { .. } => return None,
            };
            if let Some((_, children)) = columns
                .iter_mut()
                .find(|(candidate, _)| *candidate == column_idx)
            {
                children.push(child_idx);
            } else {
                columns.push((column_idx, vec![child_idx]));
            }
        }
        if columns.is_empty() {
            return None;
        }
        let child_count = columns.iter().map(|(_, children)| children.len()).sum();
        let mut stages = Vec::with_capacity(columns.len());
        let mut stage_children = Vec::with_capacity(child_count);
        for (column_idx, children) in columns {
            let child_start = stage_children.len();
            stage_children.extend(children);
            stages.push(PredicateStage {
                column_idx,
                child_start,
                child_end: stage_children.len(),
            });
        }
        Some((stages.into_boxed_slice(), stage_children.into_boxed_slice()))
    }
}

impl PredicateEvaluator {
    /// Execute a compiled pure-AND predicate as a sequence of column stages.
    ///
    /// `matches` is returned in ascending, batch-relative ordinal order. The
    /// first stage reads its physical span sequentially. Later stages either
    /// scan the surviving span or gather the exact absolute row ids, then
    /// advance their independent iterator to `batch_end` so access modes can
    /// change safely on the next batch.
    pub(super) fn evaluate_staged_batch(
        &mut self,
        start_ordinal: u64,
        max_rows: usize,
        cost_model: ScanAccessCostModel,
        matches: &mut Vec<usize>,
        stats: &mut PredicateStageReadStats,
    ) -> Result<usize> {
        let mut scratch = std::mem::take(&mut self.stage_scratch);
        let result = self.evaluate_staged_batch_inner(
            start_ordinal,
            max_rows,
            cost_model,
            matches,
            stats,
            &mut scratch,
        );
        self.stage_scratch = scratch;
        result
    }

    fn evaluate_staged_batch_inner(
        &mut self,
        start_ordinal: u64,
        max_rows: usize,
        cost_model: ScanAccessCostModel,
        matches: &mut Vec<usize>,
        stats: &mut PredicateStageReadStats,
        scratch: &mut PredicateStageScratch,
    ) -> Result<usize> {
        let stage_count = self
            .program
            .stages()
            .map(<[PredicateStage]>::len)
            .ok_or_else(|| paro_error::internal("predicate has no staged access program"))?;
        matches.clear();
        let mut batch_rows = max_rows;
        let mut batch_end = start_ordinal;

        for stage_idx in 0..stage_count {
            let stage = {
                *self
                    .program
                    .stages()
                    .expect("stage count was established above")
                    .get(stage_idx)
                    .expect("stage count was established above")
            };

            if stage_idx == 0 {
                let (rows_read, batch) =
                    self.read_stage_sequential(stage.column_idx, start_ordinal, max_rows)?;
                batch_rows = rows_read;
                batch_end = start_ordinal.checked_add(rows_read as u64).ok_or_else(|| {
                    paro_error::data_corrupted("predicate batch ordinal overflow")
                })?;
                stats.add_sequential(stage_idx, rows_read);
                self.filter_stage(stage, &batch, rows_read, matches, true)?;
                self.finish_stage_column(stage.column_idx, batch_end)?;
                continue;
            }

            if matches.is_empty() {
                self.finish_stage_column(stage.column_idx, batch_end)?;
                continue;
            }

            let first = *matches.first().expect("non-empty stage selection");
            let last = *matches.last().expect("non-empty stage selection");
            let span = last
                .checked_sub(first)
                .and_then(|span| span.checked_add(1))
                .ok_or_else(|| paro_error::data_corrupted("predicate selection span overflow"))?;

            if cost_model.sequential_materialization_is_cheaper(matches.len(), span) {
                let stage_start = start_ordinal.checked_add(first as u64).ok_or_else(|| {
                    paro_error::data_corrupted("predicate stage ordinal overflow")
                })?;
                let (rows_read, batch) =
                    self.read_stage_sequential(stage.column_idx, stage_start, span)?;
                stats.add_sequential(stage_idx, rows_read);
                if rows_read == span {
                    for row in matches.iter_mut() {
                        *row -= first;
                    }
                    self.filter_stage(stage, &batch, span, matches, false)?;
                    for row in matches.iter_mut() {
                        *row += first;
                    }
                } else {
                    // `ColumnIterator::next_batch` promises at most `span`
                    // rows, not an exact count. A page-aware implementation may
                    // therefore stop at its current boundary. Re-read only the
                    // surviving candidates through the absolute gather API so
                    // the stage remains correct without materializing or
                    // concatenating the rest of the physical span.
                    let gathered =
                        self.filter_stage_gathered(stage, start_ordinal, matches, scratch)?;
                    stats.add_gathered(stage_idx, gathered);
                }
            } else {
                let gathered =
                    self.filter_stage_gathered(stage, start_ordinal, matches, scratch)?;
                stats.add_gathered(stage_idx, gathered);
            }
            self.finish_stage_column(stage.column_idx, batch_end)?;
        }
        Ok(batch_rows)
    }

    fn read_stage_sequential(
        &mut self,
        column_idx: usize,
        start_ordinal: u64,
        rows: usize,
    ) -> Result<(usize, PredicateColumnBatch)> {
        let logical_type = self
            .predicate_types
            .get(column_idx)
            .cloned()
            .ok_or_else(|| paro_error::internal("predicate stage type is missing"))?;
        let access = *self
            .predicate_column_access
            .get(column_idx)
            .ok_or_else(|| paro_error::internal("predicate stage access is missing"))?;
        let allocator = self.allocator.clone();
        let Some(iterator) = self
            .predicate_iterators
            .get_mut(column_idx)
            .ok_or_else(|| paro_error::internal("predicate stage iterator is missing"))?
            .as_mut()
        else {
            return Ok((
                rows,
                PredicateColumnBatch::Decoded(Vector::try_constant_null(
                    logical_type,
                    rows,
                    allocator,
                )?),
            ));
        };
        if iterator.current_ordinal() != start_ordinal {
            iterator.seek_to_ordinal(start_ordinal)?;
        }
        let (read, batch) = iterator.next_predicate_batch(rows)?;
        if read > rows {
            return Err(paro_error::data_corrupted(format!(
                "predicate stage read {read} rows beyond requested maximum {rows}",
            )));
        }
        if rows != 0 && read == 0 {
            return Err(paro_error::data_corrupted(
                "predicate stage iterator made no forward progress",
            ));
        }
        Ok((
            read,
            PredicateColumnBatch::prepare(&logical_type, access, batch, read, allocator)?,
        ))
    }

    fn filter_stage_gathered(
        &mut self,
        stage: PredicateStage,
        start_ordinal: u64,
        matches: &mut Vec<usize>,
        scratch: &mut PredicateStageScratch,
    ) -> Result<usize> {
        scratch.absolute_rowids.clear();
        scratch.absolute_rowids.reserve(matches.len());
        for row in matches.iter().copied() {
            let absolute = start_ordinal
                .checked_add(row as u64)
                .and_then(|ordinal| u32::try_from(ordinal).ok())
                .ok_or_else(|| {
                    paro_error::data_corrupted("predicate gather row id exceeds the segment domain")
                })?;
            scratch.absolute_rowids.push(absolute);
        }
        let batch = self.read_stage_gather(stage.column_idx, &scratch.absolute_rowids)?;
        std::mem::swap(matches, &mut scratch.candidate_rows);
        let candidate_count = scratch.candidate_rows.len();
        matches.clear();
        self.filter_stage(stage, &batch, candidate_count, matches, true)?;
        for candidate_idx in matches.iter_mut() {
            *candidate_idx = scratch.candidate_rows[*candidate_idx];
        }
        scratch.candidate_rows.clear();
        Ok(candidate_count)
    }

    fn read_stage_gather(
        &mut self,
        column_idx: usize,
        absolute_rowids: &[u32],
    ) -> Result<PredicateColumnBatch> {
        let logical_type = self
            .predicate_types
            .get(column_idx)
            .cloned()
            .ok_or_else(|| paro_error::internal("predicate stage type is missing"))?;
        let access = *self
            .predicate_column_access
            .get(column_idx)
            .ok_or_else(|| paro_error::internal("predicate stage access is missing"))?;
        let allocator = self.allocator.clone();
        let Some(iterator) = self
            .predicate_iterators
            .get_mut(column_idx)
            .ok_or_else(|| paro_error::internal("predicate stage iterator is missing"))?
            .as_mut()
        else {
            return Ok(PredicateColumnBatch::Decoded(Vector::try_constant_null(
                logical_type,
                absolute_rowids.len(),
                allocator,
            )?));
        };
        let ordered = OrderedRowIds::try_new(absolute_rowids)?;
        let batch = iterator.read_by_ordered_rowids(&ordered)?;
        PredicateColumnBatch::prepare(
            &logical_type,
            access,
            batch,
            absolute_rowids.len(),
            allocator,
        )
    }

    fn finish_stage_column(&mut self, column_idx: usize, batch_end: u64) -> Result<()> {
        let iterator = self
            .predicate_iterators
            .get_mut(column_idx)
            .ok_or_else(|| paro_error::internal("predicate stage iterator is missing"))?;
        if let Some(iterator) = iterator.as_mut() {
            if iterator.current_ordinal() != batch_end {
                iterator.seek_to_ordinal(batch_end)?;
            }
        }
        Ok(())
    }

    fn filter_stage(
        &self,
        stage: PredicateStage,
        batch: &PredicateColumnBatch,
        rows: usize,
        selection: &mut Vec<usize>,
        seed: bool,
    ) -> Result<()> {
        let CompiledPredicateTree::And(children) = &self.program.tree else {
            return Err(paro_error::internal(
                "staged predicate program is not a conjunction",
            ));
        };
        let child_indices = self
            .program
            .stage_children
            .get(stage.child_start..stage.child_end)
            .ok_or_else(|| paro_error::internal("staged predicate child range is invalid"))?;
        for (position, child_idx) in child_indices.iter().copied().enumerate() {
            let child = children.get(child_idx).ok_or_else(|| {
                paro_error::internal("staged predicate child index is out of bounds")
            })?;
            let CompiledPredicateTree::Leaf(predicate) = child else {
                return Err(paro_error::internal("staged predicate child is not a leaf"));
            };
            Self::filter_typed_constant(predicate, batch, rows, selection, seed && position == 0)?;
            if selection.is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn filter_typed_constant(
        predicate: &CompiledPredicate,
        batch: &PredicateColumnBatch,
        rows: usize,
        selection: &mut Vec<usize>,
        seed: bool,
    ) -> Result<()> {
        match predicate {
            CompiledPredicate::FixedComparisons { comparisons, .. } => {
                comparisons.filter_batch(batch, rows, selection, seed)
            }
            CompiledPredicate::VarlenComparisons { comparisons, .. } => {
                comparisons.filter_batch(batch, rows, selection, seed)
            }
            CompiledPredicate::VarlenMatch { matcher, .. } => {
                matcher.filter_batch(batch, rows, selection, seed)
            }
            CompiledPredicate::Generic { .. } | CompiledPredicate::FixedColumnComparison { .. } => {
                Err(paro_error::internal(
                    "staged predicate contains a non-constant leaf",
                ))
            }
        }
    }
}
