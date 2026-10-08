// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::runtime_value::Value;
use paro_function::scalar::ScalarFunction;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression, FunctionExpression};
use paro_planner::logical::operator::ColumnBinding;
use paro_storage::search::FullTextQueryStats;

#[test]
fn search_window_uses_filter_binding_contract_not_projection_encoding() {
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::logical::operator::ProjectionMap;
    let column = |index| {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(7, index), LogicalType::Integer).into(),
        )
    };
    let get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new_without_table(
        7,
        vec!["id".into(), "predicate".into(), "score_input".into()],
        vec![LogicalType::Integer; 3],
    ))));
    let mut lower = Filter::new(get, vec![column(1)]);
    lower.projection_map = ProjectionMap::new(vec![2, 0]);
    let mut upper = Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(lower)),
        vec![column(2)],
    );
    upper.projection_map = ProjectionMap::new(vec![1, 0]);
    let projection = Projection::new(
        8,
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(upper)),
        vec![column(0), column(2)],
    );
    let mut topn = TopN::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection)),
        vec![OrderByNode {
            expression: Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(8, 1), LogicalType::Integer).into(),
            ),
            ascending: true,
            nulls_first: false,
        }],
        2,
        0,
    );
    let pattern = extract_topn_pattern(&topn).expect("named scan bindings survive both maps");
    assert_eq!(pattern.filters.len(), 2);
    assert_eq!(pattern.get.table_index, 7);
    let LogicalOperator::Projection(projection) = &mut topn.child.operator else {
        unreachable!()
    };
    // The lower predicate can read this column, but the final projection
    // cannot: it has been removed from that occurrence's output.
    projection.expressions[0] = column(1);
    assert!(extract_topn_pattern(&topn).is_none());
}

#[test]
fn search_window_declines_shifted_positional_and_foreign_bindings() {
    use paro_planner::expression::ReferenceExpression;
    use paro_planner::logical::operator::ProjectionMap;
    let get = Get::new_without_table(
        7,
        vec!["a".into(), "b".into()],
        vec![LogicalType::Integer; 2],
    );
    let mut filter = Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        vec![],
    );
    let mut projection = Projection::new(
        8,
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        vec![Expression::Reference(
            ReferenceExpression::new(0, LogicalType::Integer).into(),
        )],
    );
    filter.projection_map = ProjectionMap::new(vec![0, 1]);
    assert!(scan_filter_path_preserves_bindings(
        &projection,
        &[&filter],
        &get
    ));
    filter.projection_map = ProjectionMap::new(vec![1, 0]);
    assert!(!scan_filter_path_preserves_bindings(
        &projection,
        &[&filter],
        &get
    ));
    for (table, column, depth, ty) in [
        (9, 0, 0, LogicalType::Integer),
        (7, 2, 0, LogicalType::Integer),
        (7, 0, 1, LogicalType::Integer),
        (7, 0, 0, LogicalType::Varchar),
    ] {
        let mut reference = ColumnRefExpression::new(ColumnBinding::new(table, column), ty);
        reference.depth = depth;
        projection.expressions[0] = Expression::ColumnRef(reference.into());
        assert!(!scan_filter_path_preserves_bindings(
            &projection,
            &[&filter],
            &get
        ));
    }
}

fn noop_scalar(
    _input: &paro_common::chunk::Chunk,
    _state: &dyn paro_function::scalar::ExpressionState,
    _result: &mut paro_common::vector::Vector,
) -> paro_common::error::Result<()> {
    Ok(())
}

fn scalar_function(
    name: &str,
    arguments: Vec<LogicalType>,
    return_type: LogicalType,
) -> ScalarFunction {
    ScalarFunction::new(name.to_string(), arguments, return_type, noop_scalar)
}

#[test]
fn borrowed_provider_window_preserves_holes_and_reader_errors() {
    let context = OptimizationContext::new(
        paro_context::TestStatementContextBuilder::minimal().build(),
        paro_planner::binder::context::BindContext::new(),
    );
    let stats = NodeStats::default();
    let operator = LogicalOperator::Filter(Filter {
        child: 7usize,
        expressions: Vec::new(),
        projection_map: paro_planner::logical::operator::ProjectionMap::all(),
    });
    let root = || SearchNodeRef {
        id: PlanNodeId(1),
        stats: &stats,
        operator: &operator,
    };
    let mut reads = 0;
    let result = AccessPlanner::new()
        .physical_candidate_for_window(
            root(),
            1,
            |child| {
                assert_eq!(*child, 7);
                reads += 1;
                Ok(None)
            },
            &context,
        )
        .unwrap();
    assert!(result.is_none());
    assert_eq!(
        reads, 1,
        "a group hole must not trigger representative expansion"
    );
    let error = AccessPlanner::new()
        .physical_candidate_for_window(
            root(),
            1,
            |_| Err(paro_error::internal("reader failure sentinel")),
            &context,
        )
        .unwrap_err();
    assert!(error.to_string().contains("reader failure sentinel"));
}

#[test]
fn adaptive_decision_is_used_for_close_costs() {
    let decision = select_search_decision(
        SearchCandidate {
            intent: SearchIntent::Hnsw(HnswIntent {
                column_id: 1,
                query: DenseVectorQuery::Literal(vec![1.0, 2.0]),
                distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
                options: Default::default(),
            }),
            token: paro_storage::search::CapabilityToken {
                definition_id: 1,
                generation_id: 2,
                root_version: 1,
                capability_state: paro_storage::search::SearchCapabilityState::Queryable,
            },
            kind: paro_storage::search::SearchIndexKind::Hnsw,
            estimated_cost: Some(PlannedSearchCostEstimate::new(95.0)),
            exact_filter_materialization: None,
        },
        build_sequential_capability(7, 100),
    );
    assert!(matches!(decision, Some(SearchDecision::Adaptive { .. })));
}

#[test]
fn fulltext_match_extracts_query_terms() {
    let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
    let expr = Expression::Function(
        FunctionExpression::new(
            scalar_function(
                "fulltext_match",
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Boolean,
            ),
            vec![
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Varchar).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("hello world".to_string()),
                        LogicalType::Varchar,
                    )
                    .into(),
                ),
            ],
            LogicalType::Boolean,
        )
        .into(),
    );

    let intent = extract_fulltext_match_intent(&expr, &get).unwrap().unwrap();
    assert_eq!(intent.column_id, 0);
    assert_eq!(intent.score_mode, FullTextScoreMode::CorpusBm25V1);
    assert_eq!(intent.query, "hello world");
    assert_eq!(intent.query_stats.term_count, 2);
    assert_eq!(intent.query_stats.effective_query_terms(), 2);
}

#[test]
fn internal_fulltext_match_accepts_a_folded_tsquery_constant() {
    let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
    let vector = Expression::Function(
        FunctionExpression::new(
            scalar_function(
                "to_tsvector",
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsVector,
            ),
            vec![
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("simple".to_string()),
                        LogicalType::Varchar,
                    )
                    .into(),
                ),
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Varchar).into(),
                ),
            ],
            LogicalType::TsVector,
        )
        .into(),
    );
    let expression = Expression::Function(
        FunctionExpression::new(
            scalar_function(
                "fulltext_match_internal",
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Boolean,
            ),
            vec![
                vector,
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("vector & database".to_string()),
                        LogicalType::TsQuery,
                    )
                    .into(),
                ),
            ],
            LogicalType::Boolean,
        )
        .into(),
    );

    let intent = extract_fulltext_match_intent(&expression, &get)
        .unwrap()
        .unwrap();
    assert_eq!(intent.query, "vector & database");
    assert_eq!(intent.query_kind, FullTextQueryKind::SerializedTsQuery);
    assert_eq!(intent.config, "simple");
    assert_eq!(intent.query_stats.effective_query_terms(), 2);
}

#[test]
fn document_score_requires_the_exact_document_binding_and_argument_order() {
    let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
    let column = |table| {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Varchar).into(),
        )
    };
    let query = || {
        Expression::Constant(
            ConstantExpression::new(Value::Varchar("needle".to_string()), LogicalType::Varchar)
                .into(),
        )
    };
    let score = |args| {
        Expression::Function(
            FunctionExpression::new(
                scalar_function(
                    "bm25",
                    vec![LogicalType::Varchar, LogicalType::Varchar],
                    LogicalType::Float,
                ),
                args,
                LogicalType::Float,
            )
            .into(),
        )
    };
    let valid = score(vec![column(1), query()]);
    assert_eq!(
        extract_fulltext_score_intent(&valid, &get)
            .unwrap()
            .unwrap()
            .score_mode,
        FullTextScoreMode::DocumentRankV1
    );
    assert!(
        extract_fulltext_score_intent(&score(vec![column(2), query()]), &get)
            .unwrap()
            .is_none()
    );
    assert!(
        extract_fulltext_score_intent(&score(vec![query(), column(1)]), &get)
            .unwrap()
            .is_none()
    );
}

#[test]
fn derived_scan_output_declines_stored_search_projection() {
    let mut get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
    get.append_matched_utf8_prefix(0, 2, LogicalType::Varchar);
    assert!(projection_spec(&get, false).is_none());
}

#[test]
fn fulltext_filter_scan_preserves_absorbed_filter_projection() {
    let intent = FullTextIntent {
        column_id: 1,
        query: "graph".to_string(),
        query_kind: FullTextQueryKind::Legacy,
        query_stats: FullTextQueryStats::new(1),
        config: "simple".to_string(),
        score_mode: FullTextScoreMode::CorpusBm25V1,
    };
    let scan = FullTextFilterScan {
        get: Get::new_without_table(
            7,
            vec!["id".to_string(), "body".to_string(), "category".to_string()],
            vec![
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ],
        ),
        projection_map: vec![2, 0].into(),
        request: NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::Filter,
            predicate: None,
            projections: ProjectionSpec {
                columns: vec![2, 0],
                include_score: false,
            },
            intents: vec![SearchIntent::FullText(intent.clone())],
            fusion: None,
        },
        match_expression: Expression::Constant(
            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
        ),
        other_predicates: Vec::new(),
        residual_predicates: Vec::new(),
        decision: SearchDecision::IndexScan {
            candidate: SearchCandidate {
                intent: SearchIntent::FullText(intent),
                token: paro_storage::search::CapabilityToken {
                    definition_id: 1,
                    generation_id: 1,
                    root_version: 1,
                    capability_state: paro_storage::search::SearchCapabilityState::Queryable,
                },
                kind: paro_storage::search::SearchIndexKind::FullText,
                estimated_cost: Some(PlannedSearchCostEstimate::new(1.0)),
                exact_filter_materialization: None,
            },
            confidence: Confidence::High,
        },
    };
    let operator = LogicalOperator::FullTextFilterScan(Box::new(scan));

    assert_eq!(operator.output_names(), ["category", "id"]);
    assert_eq!(
        operator.types(),
        [LogicalType::Varchar, LogicalType::Integer]
    );
    assert_eq!(
        operator.get_column_bindings(),
        [ColumnBinding::new(7, 2), ColumnBinding::new(7, 0)]
    );
}
