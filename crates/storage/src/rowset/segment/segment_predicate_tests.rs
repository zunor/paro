// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::rowset::column::OrderedRowIds;
use crate::rowset::encoding::BinaryPlainPageBuilder;
use crate::rowset::scan_cost::ScanAccessCostModel;
use bytes::Bytes;
use paro_common::test_utils::test_nullable_bool_vector;

use super::super::segment_predicate_program::PredicateStageReadStats;

struct ShortBatchIterator {
    value: i32,
    current: u64,
    rows: u64,
    max_batch_rows: usize,
}

impl ShortBatchIterator {
    fn batch(&self, rows: usize) -> ColumnBatch {
        ColumnBatch::new(
            Bytes::from(
                std::iter::repeat_n(self.value, rows)
                    .flat_map(i32::to_le_bytes)
                    .collect::<Vec<_>>(),
            ),
            None,
        )
    }
}

impl ColumnIterator for ShortBatchIterator {
    fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        self.current = ordinal;
        Ok(())
    }

    fn next_batch(&mut self, rows: usize) -> Result<(usize, ColumnBatch)> {
        let rows = rows
            .min(self.max_batch_rows)
            .min((self.rows - self.current) as usize);
        self.current += rows as u64;
        Ok((rows, self.batch(rows)))
    }

    fn read_by_rowids(&mut self, rowids: &[u64]) -> Result<ColumnBatch> {
        Ok(self.batch(rowids.len()))
    }

    fn read_by_ordered_rowids(&mut self, rowids: &OrderedRowIds<'_>) -> Result<ColumnBatch> {
        Ok(self.batch(rowids.len()))
    }

    fn current_ordinal(&self) -> u64 {
        self.current
    }

    fn num_rows(&self) -> u64 {
        self.rows
    }
}

#[test]
fn staged_later_short_read_falls_back_to_gather_and_realigns_iterators() {
    let equality = |column_idx| {
        CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
            column_idx,
            comparisons: FixedComparisonValues::I32(FixedConjunction::new(
                ComparisonOperator::Equal,
                7,
            )),
        })
    };
    let mut evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::new(
            CompiledPredicateTree::And(vec![equality(0), equality(1)]),
            true,
        ),
        predicate_columns: vec![0, 1],
        predicate_types: vec![LogicalType::Integer, LogicalType::Integer],
        predicate_iterators: vec![
            Some(Box::new(ShortBatchIterator {
                value: 7,
                current: 0,
                rows: 8,
                max_batch_rows: 3,
            })),
            Some(Box::new(ShortBatchIterator {
                value: 7,
                current: 0,
                rows: 8,
                max_batch_rows: 1,
            })),
        ],
        predicate_column_access: vec![
            PredicateColumnAccess::Typed { raw_width: Some(4) },
            PredicateColumnAccess::Typed { raw_width: Some(4) },
        ],
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
    };
    let mut matches = Vec::new();
    let mut stats = PredicateStageReadStats::default();

    let rows = evaluator
        .evaluate_staged_batch(
            0,
            8,
            ScanAccessCostModel::default(),
            &mut matches,
            &mut stats,
        )
        .unwrap();

    assert_eq!(rows, 3);
    assert_eq!(matches, [0, 1, 2]);
    assert_eq!(stats.stages[0].sequential_rows, 3);
    assert_eq!(stats.stages[1].sequential_rows, 1);
    assert_eq!(stats.stages[1].gathered_rows, 3);
    assert!(evaluator
        .predicate_iterators
        .iter()
        .flatten()
        .all(|iterator| iterator.current_ordinal() == 3));
}

fn integer_comparison_tree(conjunction: fn(Vec<PredicateTree>) -> PredicateTree) -> PredicateTree {
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
        }) if comparisons.lower.is_some() && comparisons.upper.is_some()
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
fn equal_priority_fixed_ranges_read_narrower_column_first() {
    let column_map = HashMap::from([(6, 0), (10, 1)]);
    let column_types = [
        LogicalType::Decimal {
            precision: 15,
            scale: 2,
        },
        LogicalType::Date,
    ];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::And(vec![
            PredicateTree::leaf(Predicate::Range {
                column_id: 6,
                lower: Value::Decimal(5, 15, 2),
                upper: Value::Decimal(7, 15, 2),
            }),
            PredicateTree::leaf(Predicate::Range {
                column_id: 10,
                lower: Value::Date(0),
                upper: Value::Date(364),
            }),
        ]),
        &column_map,
        &column_types,
    )
    .unwrap();

    let CompiledPredicateTree::And(children) = tree else {
        panic!("expected compiled AND");
    };
    assert!(matches!(
        children.first(),
        Some(CompiledPredicateTree::Leaf(
            CompiledPredicate::FixedComparisons { column_idx: 1, .. }
        ))
    ));
    assert!(matches!(
        children.get(1),
        Some(CompiledPredicateTree::Leaf(
            CompiledPredicate::FixedComparisons { column_idx: 0, .. }
        ))
    ));
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
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::once(None).collect(),
        predicate_column_access: vec![PredicateColumnAccess::Typed {
            raw_width: Some(std::mem::size_of::<i32>()),
        }],
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
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
fn fixed_range_reads_raw_column_batches() {
    let column_map = HashMap::from([(7, 0)]);
    let column_types = [LogicalType::Integer];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::leaf(Predicate::Range {
            column_id: 7,
            lower: Value::Integer(10),
            upper: Value::Integer(20),
        }),
        &column_map,
        &column_types,
    )
    .unwrap();
    assert!(matches!(
        &tree,
        CompiledPredicateTree::Leaf(CompiledPredicate::FixedComparisons {
            comparisons: FixedComparisonValues::I32(comparisons),
            ..
        }) if comparisons.lower.is_some() && comparisons.upper.is_some()
    ));
    let mut access = [PredicateColumnAccess::Unused];
    PredicateEvaluator::mark_column_access(&tree, &mut access).unwrap();
    assert_eq!(
        access,
        [PredicateColumnAccess::Typed {
            raw_width: Some(std::mem::size_of::<i32>())
        }]
    );
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::once(None).collect(),
        predicate_column_access: access.to_vec(),
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
    };
    let data = [9_i32, 10, 20, 21]
        .into_iter()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let batches = [PredicateColumnBatch::Raw(ColumnBatch::new(
        Bytes::from(data),
        Some(Bytes::from_static(&[0, 0, 0, 0])),
    ))];

    let mut matches = Vec::new();
    evaluator.evaluate_batch(&batches, 4, &mut matches).unwrap();

    assert_eq!(matches, [1, 2]);
}

#[test]
fn i32_range_seed_preserves_open_bounds_and_domain_edges() {
    let evaluate = |predicates: Vec<PredicateTree>| {
        let column_map = HashMap::from([(7, 0)]);
        let tree = PredicateEvaluator::compile_tree(
            &PredicateTree::And(predicates),
            &column_map,
            &[LogicalType::Integer],
        )
        .unwrap();
        let evaluator = PredicateEvaluator {
            program: CompiledPredicateProgram::legacy(tree),
            predicate_columns: vec![7],
            predicate_types: vec![LogicalType::Integer],
            predicate_iterators: std::iter::once(None).collect(),
            predicate_column_access: vec![PredicateColumnAccess::Typed {
                raw_width: Some(std::mem::size_of::<i32>()),
            }],
            allocator: Arc::new(default_allocator()),
            stage_scratch: PredicateStageScratch::default(),
        };
        // The four-lane groups cover mixed, all-accepted, all-rejected, and
        // mixed masks in sequence. This also verifies that SIMD compaction
        // carries its output cursor across groups without gaps or reordering.
        let values = [
            i32::MIN,
            2,
            0,
            3,
            2,
            3,
            4,
            2,
            i32::MIN,
            0,
            1,
            i32::MAX,
            5,
            3,
            1,
            i32::MAX,
        ];
        let batch = PredicateColumnBatch::Raw(ColumnBatch::new(
            Bytes::from(
                values
                    .into_iter()
                    .flat_map(i32::to_le_bytes)
                    .collect::<Vec<_>>(),
            ),
            None,
        ));
        let mut matches = Vec::new();
        evaluator
            .evaluate_batch(&[batch], values.len(), &mut matches)
            .unwrap();
        matches
    };

    assert_eq!(
        evaluate(vec![
            PredicateTree::leaf(Predicate::Ge {
                column_id: 7,
                value: Value::Integer(2),
            }),
            PredicateTree::leaf(Predicate::Lt {
                column_id: 7,
                value: Value::Integer(5),
            }),
        ]),
        [1, 3, 4, 5, 6, 7, 13]
    );
    assert_eq!(
        evaluate(vec![PredicateTree::leaf(Predicate::Gt {
            column_id: 7,
            value: Value::Integer(3),
        })]),
        [6, 11, 12, 15]
    );
    assert_eq!(
        evaluate(vec![PredicateTree::leaf(Predicate::Lt {
            column_id: 7,
            value: Value::Integer(1),
        })]),
        [0, 2, 8, 9]
    );
    assert!(evaluate(vec![PredicateTree::leaf(Predicate::Gt {
        column_id: 7,
        value: Value::Integer(i32::MAX),
    })])
    .is_empty());
}

#[test]
fn i64_range_seed_preserves_open_bounds_and_domain_edges() {
    let evaluate = |predicates: Vec<PredicateTree>| {
        let column_map = HashMap::from([(7, 0)]);
        let tree = PredicateEvaluator::compile_tree(
            &PredicateTree::And(predicates),
            &column_map,
            &[LogicalType::BigInt],
        )
        .unwrap();
        let evaluator = PredicateEvaluator {
            program: CompiledPredicateProgram::legacy(tree),
            predicate_columns: vec![7],
            predicate_types: vec![LogicalType::BigInt],
            predicate_iterators: std::iter::once(None).collect(),
            predicate_column_access: vec![PredicateColumnAccess::Typed {
                raw_width: Some(std::mem::size_of::<i64>()),
            }],
            allocator: Arc::new(default_allocator()),
            stage_scratch: PredicateStageScratch::default(),
        };
        let values = [i64::MIN, 1, 2, 3, 4, 5, i64::MAX];
        let batch = PredicateColumnBatch::Raw(ColumnBatch::new(
            Bytes::from(
                values
                    .into_iter()
                    .flat_map(i64::to_le_bytes)
                    .collect::<Vec<_>>(),
            ),
            None,
        ));
        let mut matches = Vec::new();
        evaluator
            .evaluate_batch(&[batch], values.len(), &mut matches)
            .unwrap();
        matches
    };

    assert_eq!(
        evaluate(vec![
            PredicateTree::leaf(Predicate::Gt {
                column_id: 7,
                value: Value::BigInt(1),
            }),
            PredicateTree::leaf(Predicate::Le {
                column_id: 7,
                value: Value::BigInt(4),
            }),
        ]),
        [2, 3, 4]
    );
    assert_eq!(
        evaluate(vec![PredicateTree::leaf(Predicate::Ge {
            column_id: 7,
            value: Value::BigInt(4),
        })]),
        [4, 5, 6]
    );
    assert_eq!(
        evaluate(vec![PredicateTree::leaf(Predicate::Lt {
            column_id: 7,
            value: Value::BigInt(3),
        })]),
        [0, 1, 2]
    );
    assert!(evaluate(vec![PredicateTree::leaf(Predicate::Gt {
        column_id: 7,
        value: Value::BigInt(i64::MAX),
    })])
    .is_empty());
}

#[test]
fn fixed_in_set_coalesces_with_ranges_and_reads_raw_batches() {
    let column_map = HashMap::from([(7, 0)]);
    let column_types = [LogicalType::Integer];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::And(vec![
            PredicateTree::leaf(Predicate::In {
                column_id: 7,
                values: vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)],
            }),
            PredicateTree::leaf(Predicate::Gt {
                column_id: 7,
                value: Value::Integer(10),
            }),
        ]),
        &column_map,
        &column_types,
    )
    .unwrap();
    let CompiledPredicateTree::And(children) = tree else {
        panic!("expected compiled AND");
    };
    assert_eq!(children.len(), 1);
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(CompiledPredicateTree::And(children)),
        predicate_columns: vec![7],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::once(None).collect(),
        predicate_column_access: vec![PredicateColumnAccess::Typed {
            raw_width: Some(std::mem::size_of::<i32>()),
        }],
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
    };
    let values = [5_i32, 10, 20, 30, 40];
    let data = values
        .into_iter()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let batches = [PredicateColumnBatch::Raw(ColumnBatch::new(
        Bytes::from(data),
        Some(Bytes::from_static(&[0, 0, 0, 1, 0])),
    ))];

    let mut matches = Vec::new();
    evaluator.evaluate_batch(&batches, 5, &mut matches).unwrap();
    assert_eq!(matches, [2]);
}

#[test]
fn mixed_conjunction_filters_typed_columns_before_residuals() {
    let column_map = HashMap::from([(7, 0), (8, 1)]);
    let column_types = [LogicalType::Integer, LogicalType::Boolean];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::And(vec![
            PredicateTree::leaf(Predicate::IsNotNull { column_id: 8 }),
            PredicateTree::leaf(Predicate::In {
                column_id: 7,
                values: vec![Value::Integer(2), Value::Integer(4)],
            }),
        ]),
        &column_map,
        &column_types,
    )
    .unwrap();
    let CompiledPredicateTree::And(children) = &tree else {
        panic!("expected compiled AND");
    };
    assert!(matches!(
        children.first(),
        Some(CompiledPredicateTree::Leaf(
            CompiledPredicate::FixedComparisons { column_idx: 0, .. }
        ))
    ));

    let mut access = [PredicateColumnAccess::Unused; 2];
    PredicateEvaluator::mark_column_access(&tree, &mut access).unwrap();
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7, 8],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::repeat_with(|| None).take(2).collect(),
        predicate_column_access: access.to_vec(),
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
    };
    let raw = [1_i32, 2, 3, 4]
        .into_iter()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let batches = [
        PredicateColumnBatch::Raw(ColumnBatch::new(Bytes::from(raw), None)),
        PredicateColumnBatch::Decoded(test_nullable_bool_vector(&[
            Some(true),
            Some(false),
            None,
            None,
        ])),
    ];
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
            PredicateColumnAccess::Typed { raw_width: Some(4) },
            PredicateColumnAccess::Typed { raw_width: Some(4) }
        ]
    );
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7, 8],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::repeat_with(|| None).take(2).collect(),
        predicate_column_access: access.to_vec(),
        allocator: Arc::new(default_allocator()),
        stage_scratch: PredicateStageScratch::default(),
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
fn varchar_comparison_filters_raw_varlen_bytes_without_materializing_a_vector() {
    let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
    let column_map = HashMap::from([(7, 0)]);
    let column_types = [LogicalType::Varchar];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::leaf(Predicate::NotEq {
            column_id: 7,
            value: Value::Varchar("Brand#45".to_string()),
        }),
        &column_map,
        &column_types,
    )
    .unwrap();
    assert!(matches!(
        &tree,
        CompiledPredicateTree::Leaf(CompiledPredicate::VarlenComparisons { .. })
    ));
    let mut data = Vec::new();
    for value in ["Brand#12", "Brand#45", "Brand#46", "Brand#99"] {
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value.as_bytes());
    }
    let batch = PredicateColumnBatch::prepare(
        &LogicalType::Varchar,
        PredicateColumnAccess::Typed { raw_width: None },
        ColumnBatch::new(Bytes::from(data), Some(Bytes::from_static(&[0, 0, 0, 1]))),
        4,
        allocator.clone(),
    )
    .unwrap();
    assert!(matches!(batch, PredicateColumnBatch::RawVarlen(_)));
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::once(None).collect(),
        predicate_column_access: vec![PredicateColumnAccess::Typed { raw_width: None }],
        allocator,
        stage_scratch: PredicateStageScratch::default(),
    };
    let batches = [batch];
    let mut matches = Vec::new();

    evaluator.evaluate_batch(&batches, 4, &mut matches).unwrap();

    assert_eq!(matches, [0, 2]);
}

#[test]
fn prefix_membership_filters_borrowed_binary_plain_page() {
    let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
    let column_map = HashMap::from([(7, 0)]);
    let column_types = [LogicalType::Varchar];
    let tree = PredicateEvaluator::compile_tree(
        &PredicateTree::leaf(Predicate::StringPrefixIn {
            column_id: 7,
            prefixes: vec!["13".to_string(), "31".to_string()],
        }),
        &column_map,
        &column_types,
    )
    .unwrap();
    let mut builder = BinaryPlainPageBuilder::new(1024);
    for value in ["12-aaa", "13-bbb", "31-ccc", "3"] {
        assert!(builder.add_slice(value.as_bytes()));
    }
    let page = builder.finish().unwrap();
    let mut decoder = crate::rowset::encoding::BinaryPlainPageDecoder::new(page);
    decoder.init().unwrap();
    let page_slice = decoder.next_encoded_batch(4).unwrap();
    let batch = ColumnBatch::with_storage_binary_plain(page_slice, None);
    let batch = PredicateColumnBatch::prepare(
        &LogicalType::Varchar,
        PredicateColumnAccess::Typed { raw_width: None },
        batch,
        4,
        allocator.clone(),
    )
    .unwrap();
    assert!(matches!(batch, PredicateColumnBatch::RawVarlen(_)));
    let evaluator = PredicateEvaluator {
        program: CompiledPredicateProgram::legacy(tree),
        predicate_columns: vec![7],
        predicate_types: column_types.to_vec(),
        predicate_iterators: std::iter::once(None).collect(),
        predicate_column_access: vec![PredicateColumnAccess::Typed { raw_width: None }],
        allocator,
        stage_scratch: PredicateStageScratch::default(),
    };
    let mut matches = Vec::new();

    evaluator.evaluate_batch(&[batch], 4, &mut matches).unwrap();

    assert_eq!(matches, [1, 2]);
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
fn fixed_predicate_preserves_a_validated_storage_dictionary() {
    let mut dictionary = BinaryPlainPageBuilder::new(1024);
    assert!(dictionary.add_slice(&5_i32.to_le_bytes()));
    assert!(dictionary.add_slice(&11_i32.to_le_bytes()));
    let dictionary = dictionary.finish().unwrap();
    let codes = [0_u32, 1, 0]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect::<Vec<_>>();
    let batch = ColumnBatch::with_storage_dictionary(dictionary, Bytes::from(codes), None);

    let prepared = PredicateColumnBatch::prepare(
        &LogicalType::Integer,
        PredicateColumnAccess::Typed {
            raw_width: Some(std::mem::size_of::<i32>()),
        },
        batch,
        3,
        Arc::new(default_allocator()),
    )
    .unwrap();

    let PredicateColumnBatch::StorageDictionary(_) = &prepared else {
        panic!("typed predicate must retain the storage dictionary");
    };
    let comparisons =
        FixedComparisonValues::I32(FixedConjunction::new(ComparisonOperator::Equal, 11));
    let mut selection = Vec::new();
    comparisons
        .filter_batch(&prepared, 3, &mut selection, true)
        .unwrap();
    assert_eq!(selection, [1]);
}
