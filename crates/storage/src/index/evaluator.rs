// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Evaluator
//!
//! Combines multiple indexes to evaluate predicate trees.

use std::collections::HashMap;
use std::sync::Arc;

use crate::index::bound_index::{BoundIndex, IndexPredicateEvaluation, PredicateIndexBinding};
use crate::index::page_layout::PageLayout;
use crate::index::predicate::{Predicate, PredicateTree};
use crate::index::predicate_result::{
    intersect, intersect_with_layout, union, union_with_layout, PredicateResult,
};
use crate::index::ColumnId;

/// Index evaluator that combines multiple indexes.
pub struct IndexEvaluator {
    /// Column -> indexes (sorted by priority)
    indexes: HashMap<ColumnId, Vec<PredicateIndexBinding>>,
    /// Optional page layout for precise PageRanges ↔ Bitmap conversion.
    page_layout: Option<PageLayout>,
    /// Segment-local row domain against which completeness credentials are
    /// checked. Generic/index-unit evaluators intentionally have no domain and
    /// therefore cannot turn a scalar posting answer into an exact proof.
    segment_rows: Option<u64>,
}

impl IndexEvaluator {
    /// Create a new evaluator from a list of indexes.
    pub fn new(indexes: Vec<Arc<dyn BoundIndex>>) -> Self {
        Self::with_layout(indexes, None)
    }

    /// Create a new evaluator with an optional page layout.
    ///
    /// When a `PageLayout` is provided, `PageRanges × Bitmap` intersections
    /// are computed precisely by converting page ranges to row ranges first.
    pub fn with_layout(indexes: Vec<Arc<dyn BoundIndex>>, page_layout: Option<PageLayout>) -> Self {
        Self::with_bindings(
            indexes
                .into_iter()
                .map(PredicateIndexBinding::candidate)
                .collect(),
            page_layout,
            None,
        )
    }

    /// Build an evaluator for one immutable segment. Only bindings carrying a
    /// matching [`SegmentLocalComplete`](crate::index::SegmentLocalComplete)
    /// credential may suppress row verification.
    pub(crate) fn for_segment(
        indexes: Vec<PredicateIndexBinding>,
        page_layout: Option<PageLayout>,
        segment_rows: u64,
    ) -> Self {
        Self::with_bindings(indexes, page_layout, Some(segment_rows))
    }

    fn with_bindings(
        indexes: Vec<PredicateIndexBinding>,
        page_layout: Option<PageLayout>,
        segment_rows: Option<u64>,
    ) -> Self {
        let mut map: HashMap<ColumnId, Vec<PredicateIndexBinding>> = HashMap::new();
        for index in indexes {
            for &column_id in index.index().column_ids() {
                map.entry(column_id).or_default().push(index.clone());
            }
        }

        IndexEvaluator {
            indexes: map,
            page_layout,
            segment_rows,
        }
    }

    /// Evaluate a predicate tree using available indexes.
    pub fn evaluate(&self, predicate_tree: &PredicateTree) -> PredicateResult {
        self.evaluate_with_proof(predicate_tree).candidates
    }

    /// Evaluate candidates and guaranteed-true rows through one tree walk.
    pub fn evaluate_with_proof(&self, predicate_tree: &PredicateTree) -> IndexPredicateEvaluation {
        match predicate_tree {
            PredicateTree::Leaf(predicate) => self.evaluate_single(predicate),
            PredicateTree::And(children) => {
                let mut candidates = PredicateResult::AllMatch;
                let mut guaranteed = PredicateResult::AllMatch;
                let mut exact = true;
                for child in children {
                    let child = self.evaluate_with_proof(child);
                    exact &= child.is_exact();
                    candidates = match &self.page_layout {
                        Some(layout) => {
                            intersect_with_layout(&candidates, &child.candidates, layout)
                        }
                        None => intersect(&candidates, &child.candidates),
                    };
                    if matches!(candidates, PredicateResult::NoneMatch) {
                        // `guaranteed ⊆ candidates` makes the proof empty too;
                        // no remaining child can make an AND row eligible.
                        return IndexPredicateEvaluation::exact(PredicateResult::NoneMatch);
                    }
                    guaranteed = match &self.page_layout {
                        Some(layout) => {
                            intersect_with_layout(&guaranteed, child.guaranteed(), layout)
                        }
                        None => intersect(&guaranteed, child.guaranteed()),
                    };
                }
                if exact {
                    IndexPredicateEvaluation::exact(candidates)
                } else {
                    IndexPredicateEvaluation::new(candidates, guaranteed)
                }
            }
            PredicateTree::Or(children) => {
                let mut candidates = PredicateResult::NoneMatch;
                let mut guaranteed = PredicateResult::NoneMatch;
                let mut exact = true;
                for child in children {
                    let child = self.evaluate_with_proof(child);
                    exact &= child.is_exact();
                    candidates = match &self.page_layout {
                        Some(layout) => union_with_layout(&candidates, &child.candidates, layout),
                        None => union(&candidates, &child.candidates),
                    };
                    guaranteed = match &self.page_layout {
                        Some(layout) => union_with_layout(&guaranteed, child.guaranteed(), layout),
                        None => union(&guaranteed, child.guaranteed()),
                    };
                }
                if exact {
                    IndexPredicateEvaluation::exact(candidates)
                } else {
                    IndexPredicateEvaluation::new(candidates, guaranteed)
                }
            }
        }
    }

    /// Evaluate a single predicate using the best available index.
    ///
    /// Indexes are pre-filtered by column_id (stored in the HashMap) and
    /// ordered by the predicate-aware access-path policy below.
    fn evaluate_single(&self, predicate: &Predicate) -> IndexPredicateEvaluation {
        // Empty membership is an algebraic contradiction, independent of
        // which access paths exist for the column. Prove it here so callers do
        // not reopen/decode a column merely to discover that no row can pass.
        // This is also the canonical representation used when binding an
        // out-of-domain runtime parameter (for example SMALLINT = 100000).
        if matches!(
            predicate,
            Predicate::In { values, .. } if values.is_empty()
        ) || matches!(
            predicate,
            Predicate::FixedIn { values, .. } if values.len() == 0
        ) || matches!(
            predicate,
            Predicate::StringPrefixIn { prefixes, .. } if prefixes.is_empty()
        ) {
            return IndexPredicateEvaluation::exact(PredicateResult::NoneMatch);
        }

        let Some(column_id) = predicate.index_column_id() else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };
        let Some(indexes) = self.indexes.get(&column_id) else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };

        let mut candidates = PredicateResult::Unknown;
        let mut guaranteed = PredicateResult::NoneMatch;
        let mut ordered = indexes.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|binding| index_priority(binding.index().index_type(), predicate));
        for binding in ordered {
            let complete_scalar = self
                .segment_rows
                .is_some_and(|rows| binding.is_complete_for(rows));
            if !matches!(candidates, PredicateResult::Unknown)
                && !complete_scalar
                && !binding.index().provides_predicate_proof()
            {
                continue;
            }
            let result = if complete_scalar {
                let candidates = binding.index().evaluate_predicate(predicate);
                if matches!(candidates, PredicateResult::Unknown) {
                    IndexPredicateEvaluation::candidates_only(candidates)
                } else {
                    IndexPredicateEvaluation::exact(candidates)
                }
            } else {
                binding.index().evaluate_predicate_with_proof(predicate)
            };
            if result.is_exact() {
                // An exact representation has completely answered this leaf.
                // Consulting a second exact index can only reproduce the same
                // set and is particularly expensive when that representation
                // is an ART duplicate-key subtree.
                return result;
            }
            let next_guaranteed = match &self.page_layout {
                Some(layout) => union_with_layout(&guaranteed, result.guaranteed(), layout),
                None => union(&guaranteed, result.guaranteed()),
            };
            if matches!(candidates, PredicateResult::Unknown)
                && !matches!(result.candidates, PredicateResult::Unknown)
            {
                candidates = result.candidates;
            }
            guaranteed = next_guaranteed;
        }
        IndexPredicateEvaluation::new(candidates, guaranteed)
    }
}

fn index_priority(index_type: &str, predicate: &Predicate) -> u8 {
    let ordered = matches!(
        predicate,
        Predicate::Lt { .. }
            | Predicate::Le { .. }
            | Predicate::Gt { .. }
            | Predicate::Ge { .. }
            | Predicate::Range { .. }
    );
    match (ordered, index_type) {
        // ART performs an ordered cursor walk for ranges. Bitmap is preferred
        // for equality/membership where it can return one immutable posting.
        (true, "ART") | (false, "BITMAP") => 0,
        (true, "BITMAP") | (false, "ART") => 1,
        (_, "BLOOM") => 2,
        (_, "ZONEMAP") => 3,
        _ => 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::predicate::{Predicate, PredicateTree};
    use crate::index::predicate_result::PageRange;
    use crate::index::SegmentLocalComplete;
    use crate::index::{IndexConstraintType, IndexStorageInfo};
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use roaring::RoaringBitmap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockIndex {
        name: String,
        index_type: String,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        result: PredicateResult,
        evaluations: AtomicUsize,
    }

    impl MockIndex {
        fn new(index_type: &str, result: PredicateResult) -> Self {
            Self {
                name: format!("mock_{}", index_type),
                index_type: index_type.to_string(),
                column_ids: vec![0],
                logical_types: vec![LogicalType::Integer],
                result,
                evaluations: AtomicUsize::new(0),
            }
        }
    }

    impl crate::index::Index for MockIndex {
        fn column_ids(&self) -> &[ColumnId] {
            &self.column_ids
        }

        fn is_bound(&self) -> bool {
            true
        }

        fn index_type(&self) -> &str {
            &self.index_type
        }

        fn index_name(&self) -> &str {
            &self.name
        }

        fn constraint_type(&self) -> IndexConstraintType {
            IndexConstraintType::None
        }

        fn commit_drop(&mut self) -> Result<()> {
            Ok(())
        }
    }

    impl BoundIndex for MockIndex {
        fn physical_types(&self) -> &[LogicalType] {
            &self.logical_types
        }

        fn logical_types(&self) -> &[LogicalType] {
            &self.logical_types
        }

        fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
            Ok(())
        }

        fn delete(&self, _entries: &Chunk, _row_ids: &Vector) -> Result<usize> {
            Ok(0)
        }

        fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
            Ok(())
        }

        fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
            Ok(true)
        }

        fn vacuum(&self) {}

        fn get_in_memory_size(&self) -> usize {
            0
        }

        fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
            Ok(IndexStorageInfo::default())
        }

        fn evaluate_predicate(&self, _predicate: &Predicate) -> PredicateResult {
            self.evaluations.fetch_add(1, Ordering::Relaxed);
            self.result.clone()
        }
    }

    #[test]
    fn low_cardinality_bitmap_precedes_art() {
        let art = Arc::new(MockIndex::new(
            "ART",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([1])),
        ));
        let bitmap = Arc::new(MockIndex::new(
            "BITMAP",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([2])),
        ));
        let bloom = Arc::new(MockIndex::new(
            "BLOOM",
            PredicateResult::PageRanges(vec![PageRange::new(0, 10)]),
        ));

        let evaluator = IndexEvaluator::new(vec![bloom, bitmap, art]);
        let predicate = Predicate::Eq {
            column_id: 0,
            value: paro_common::runtime_value::Value::Integer(1),
        };
        let result = evaluator.evaluate(&PredicateTree::leaf(predicate));

        match result {
            PredicateResult::Bitmap(bitmap) => {
                assert!(bitmap.contains(2));
                assert!(!bitmap.contains(1));
            }
            _ => panic!("expected posting bitmap"),
        }
    }

    #[test]
    fn ordered_range_prefers_art_over_bitmap() {
        let art = Arc::new(MockIndex::new(
            "ART",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([1])),
        ));
        let bitmap = Arc::new(MockIndex::new(
            "BITMAP",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([2])),
        ));
        let evaluator = IndexEvaluator::new(vec![bitmap.clone(), art.clone()]);
        let result = evaluator.evaluate(&PredicateTree::leaf(Predicate::Range {
            column_id: 0,
            lower: paro_common::runtime_value::Value::Integer(1),
            upper: paro_common::runtime_value::Value::Integer(3),
        }));

        assert!(matches!(result, PredicateResult::Bitmap(ref rows) if rows.contains(1)));
        assert_eq!(art.evaluations.load(Ordering::Relaxed), 1);
        assert_eq!(bitmap.evaluations.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn exactness_requires_matching_segment_completeness_credential() {
        let index = Arc::new(MockIndex::new(
            "BITMAP",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([1])),
        ));
        let predicate = PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: paro_common::runtime_value::Value::Integer(1),
        });

        let candidate_only = IndexEvaluator::new(vec![index.clone()]);
        assert!(!candidate_only.evaluate_with_proof(&predicate).is_exact());

        let completeness = SegmentLocalComplete::prove(4, 4).expect("complete segment");
        let exact = IndexEvaluator::for_segment(
            vec![PredicateIndexBinding::complete_scalar(index, completeness)],
            None,
            4,
        );
        assert!(exact.evaluate_with_proof(&predicate).is_exact());
        assert!(SegmentLocalComplete::prove(3, 4).is_err());
    }

    #[test]
    fn empty_membership_is_exact_without_an_index_or_segment_credential() {
        let evaluator = IndexEvaluator::new(Vec::new());
        let result = evaluator.evaluate_with_proof(&PredicateTree::leaf(Predicate::In {
            column_id: 0,
            values: Vec::new(),
        }));

        assert!(result.is_exact());
        assert!(matches!(result.candidates, PredicateResult::NoneMatch));
        assert!(matches!(result.guaranteed(), PredicateResult::NoneMatch));
    }

    #[test]
    fn test_and_union_logic() {
        let mut left_bitmap = RoaringBitmap::new();
        left_bitmap.insert(1);
        left_bitmap.insert(2);

        let left = Arc::new(MockIndex::new(
            "BITMAP",
            PredicateResult::Bitmap(left_bitmap),
        ));
        let right = Arc::new(MockIndex::new(
            "BLOOM",
            PredicateResult::PageRanges(vec![PageRange::new(0, 2)]),
        ));

        let evaluator = IndexEvaluator::new(vec![left, right]);

        let pred_left = PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: paro_common::runtime_value::Value::Integer(1),
        });
        let pred_right = PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: paro_common::runtime_value::Value::Integer(2),
        });

        let tree = PredicateTree::And(vec![pred_left, pred_right]);
        let result = evaluator.evaluate(&tree);
        assert!(matches!(result, PredicateResult::Bitmap(_)));
    }

    #[test]
    fn and_stops_after_the_candidate_set_becomes_empty() {
        let rejecting = Arc::new(MockIndex::new("ART", PredicateResult::NoneMatch));
        let evaluator = IndexEvaluator::new(vec![rejecting.clone()]);
        let leaf = |value| {
            PredicateTree::leaf(Predicate::Eq {
                column_id: 0,
                value: paro_common::runtime_value::Value::Integer(value),
            })
        };

        let result = evaluator.evaluate_with_proof(&PredicateTree::And(vec![leaf(1), leaf(2)]));

        assert!(matches!(result.candidates, PredicateResult::NoneMatch));
        assert!(matches!(result.guaranteed(), PredicateResult::NoneMatch));
        assert_eq!(rejecting.evaluations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn candidate_only_indexes_stop_after_the_highest_priority_answer() {
        let art = Arc::new(MockIndex::new(
            "ART",
            PredicateResult::Bitmap(RoaringBitmap::from_iter([1])),
        ));
        let bloom = Arc::new(MockIndex::new(
            "BLOOM",
            PredicateResult::PageRanges(vec![PageRange::new(0, 10)]),
        ));
        let evaluator = IndexEvaluator::new(vec![bloom.clone(), art.clone()]);
        let predicate = PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: paro_common::runtime_value::Value::Integer(1),
        });

        let result = evaluator.evaluate_with_proof(&predicate);

        assert!(matches!(result.candidates, PredicateResult::Bitmap(_)));
        assert_eq!(art.evaluations.load(Ordering::Relaxed), 1);
        assert_eq!(bloom.evaluations.load(Ordering::Relaxed), 0);
    }
}
