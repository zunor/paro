// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Evaluator
//!
//! Combines multiple indexes to evaluate predicate trees.

use std::collections::HashMap;
use std::sync::Arc;

use crate::index::bound_index::{BoundIndex, IndexPredicateEvaluation};
use crate::index::page_layout::PageLayout;
use crate::index::predicate::{Predicate, PredicateTree};
use crate::index::predicate_result::{
    intersect, intersect_with_layout, union, union_with_layout, PredicateResult,
};
use crate::index::ColumnId;

/// Index evaluator that combines multiple indexes.
pub struct IndexEvaluator {
    /// Column -> indexes (sorted by priority)
    indexes: HashMap<ColumnId, Vec<Arc<dyn BoundIndex>>>,
    /// Optional page layout for precise PageRanges ↔ Bitmap conversion.
    page_layout: Option<PageLayout>,
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
        let mut map: HashMap<ColumnId, Vec<Arc<dyn BoundIndex>>> = HashMap::new();
        for index in indexes {
            for &column_id in index.column_ids() {
                map.entry(column_id).or_default().push(index.clone());
            }
        }

        for indexes in map.values_mut() {
            indexes.sort_by_key(|idx| index_priority(idx.index_type()));
        }

        IndexEvaluator {
            indexes: map,
            page_layout,
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
                for child in children {
                    let child = self.evaluate_with_proof(child);
                    candidates = match &self.page_layout {
                        Some(layout) => {
                            intersect_with_layout(&candidates, &child.candidates, layout)
                        }
                        None => intersect(&candidates, &child.candidates),
                    };
                    guaranteed = match &self.page_layout {
                        Some(layout) => {
                            intersect_with_layout(&guaranteed, &child.guaranteed, layout)
                        }
                        None => intersect(&guaranteed, &child.guaranteed),
                    };
                }
                IndexPredicateEvaluation::new(candidates, guaranteed)
            }
            PredicateTree::Or(children) => {
                let mut candidates = PredicateResult::NoneMatch;
                let mut guaranteed = PredicateResult::NoneMatch;
                for child in children {
                    let child = self.evaluate_with_proof(child);
                    candidates = match &self.page_layout {
                        Some(layout) => union_with_layout(&candidates, &child.candidates, layout),
                        None => union(&candidates, &child.candidates),
                    };
                    guaranteed = match &self.page_layout {
                        Some(layout) => union_with_layout(&guaranteed, &child.guaranteed, layout),
                        None => union(&guaranteed, &child.guaranteed),
                    };
                }
                IndexPredicateEvaluation::new(candidates, guaranteed)
            }
        }
    }

    /// Evaluate a single predicate using the best available index.
    ///
    /// Indexes are pre-filtered by column_id (stored in the HashMap)
    /// and sorted by priority (ART > Bitmap > Bloom > ZoneMap).
    fn evaluate_single(&self, predicate: &Predicate) -> IndexPredicateEvaluation {
        let Some(column_id) = predicate.index_column_id() else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };
        let Some(indexes) = self.indexes.get(&column_id) else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };

        let mut candidates = PredicateResult::Unknown;
        let mut guaranteed = PredicateResult::NoneMatch;
        for index in indexes {
            let result = index.evaluate_predicate_with_proof(predicate);
            if matches!(candidates, PredicateResult::Unknown)
                && !matches!(result.candidates, PredicateResult::Unknown)
            {
                candidates = result.candidates;
            }
            guaranteed = match &self.page_layout {
                Some(layout) => union_with_layout(&guaranteed, &result.guaranteed, layout),
                None => union(&guaranteed, &result.guaranteed),
            };
        }
        IndexPredicateEvaluation::new(candidates, guaranteed)
    }
}

fn index_priority(index_type: &str) -> u8 {
    match index_type {
        "ART" => 0,
        "BITMAP" => 1,
        "BLOOM" => 2,
        "ZONEMAP" => 3,
        _ => 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::predicate::{Predicate, PredicateTree};
    use crate::index::predicate_result::PageRange;
    use crate::index::{IndexConstraintType, IndexStorageInfo};
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use roaring::RoaringBitmap;

    struct MockIndex {
        name: String,
        index_type: String,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        result: PredicateResult,
    }

    impl MockIndex {
        fn new(index_type: &str, result: PredicateResult) -> Self {
            Self {
                name: format!("mock_{}", index_type),
                index_type: index_type.to_string(),
                column_ids: vec![0],
                logical_types: vec![LogicalType::Integer],
                result,
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
            self.result.clone()
        }
    }

    #[test]
    fn test_priority_order() {
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
                assert!(bitmap.contains(1));
                assert!(!bitmap.contains(2));
            }
            _ => panic!("expected bitmap from ART"),
        }
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
}
