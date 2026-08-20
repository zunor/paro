// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::{Join, LogicalOperator};
use paro_planner::plan::{
    CardinalityEstimate, CardinalityProvenance, LogicalPlan, NodeStats, PlanNodeId,
};
use paro_planner::planner::Planner;

use super::dimension_deferral;
use crate::expression::traversal::visit_expression;
use crate::subquery::partition_aggregate_tests::setup_session;

#[test]
fn unique_dimension_payload_is_attached_after_partial_aggregation() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT nation, sum(amount) \
         FROM ( \
             SELECT n_name AS nation, s_acctbal AS amount \
             FROM supplier JOIN nation ON s_nationkey = n_nationkey \
         ) AS profit \
         GROUP BY nation",
    )
    .expect("parse dimension aggregate")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(statement)
        .expect("plan dimension aggregate");
    let planned = planner.take_plan().expect("logical dimension aggregate");
    let planned = annotate_cardinalities(planned);

    let (rewritten, changed) = dimension_deferral::optimize_plan(
        planned,
        &planner.binder.bind_context,
        &crate::cost_model::CostModel::default(),
    )
    .expect("rewrite dimension aggregate");
    assert!(changed);
    let mut aggregates = 0usize;
    let mut nation_gets = 0usize;
    let mut nation_table_indices = std::collections::HashSet::new();
    let mut payload_free_dimension_filters = 0usize;
    let mut final_join_stats = None;
    let mut filtering_join_bindings = Vec::new();
    let mut final_join_bindings = Vec::new();
    rewritten
        .try_visit_pre_order(|plan| {
            match &plan.operator {
                LogicalOperator::Aggregate(_) => aggregates += 1,
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "nation") =>
                {
                    nation_gets += 1;
                    nation_table_indices.insert(get.table_index);
                }
                LogicalOperator::Join(Join::Comparison(join))
                    if join.right_projection_map.is_none() =>
                {
                    payload_free_dimension_filters += 1;
                    for condition in &join.conditions {
                        collect_bindings(&condition.left, &mut filtering_join_bindings);
                        collect_bindings(&condition.right, &mut filtering_join_bindings);
                    }
                }
                LogicalOperator::Join(Join::Comparison(join)) => {
                    final_join_stats = Some((plan.id, plan.stats.clone()));
                    for condition in &join.conditions {
                        collect_bindings(&condition.left, &mut final_join_bindings);
                        collect_bindings(&condition.right, &mut final_join_bindings);
                    }
                }
                _ => {}
            }
            Ok(())
        })
        .expect("inspect rewritten plan");

    assert_eq!(aggregates, 2, "{rewritten:#?}");
    assert_eq!(nation_gets, 2, "{rewritten:#?}");
    assert_eq!(nation_table_indices.len(), 2, "{rewritten:#?}");
    assert_eq!(payload_free_dimension_filters, 1, "{rewritten:#?}");
    let (final_join_id, final_stats) =
        final_join_stats.expect("rewritten plan has a final dimension join");
    assert_ne!(final_join_id, PlanNodeId::SYNTHETIC);
    assert_eq!(final_stats, NodeStats::default());
    let filtering_dimension_indices = filtering_join_bindings
        .iter()
        .map(|binding| binding.table_index)
        .filter(|index| nation_table_indices.contains(index))
        .collect::<std::collections::HashSet<_>>();
    let final_dimension_indices = final_join_bindings
        .iter()
        .map(|binding| binding.table_index)
        .filter(|index| nation_table_indices.contains(index))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(filtering_dimension_indices.len(), 1, "{rewritten:#?}");
    assert_eq!(final_dimension_indices.len(), 1, "{rewritten:#?}");
    assert_ne!(filtering_dimension_indices, final_dimension_indices);
}

#[test]
fn non_unique_dimension_key_is_not_deferred() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT customer, sum(amount) \
         FROM ( \
             SELECT c_name AS customer, s_acctbal AS amount \
             FROM supplier JOIN customer ON s_nationkey = c_nationkey \
         ) AS balances \
         GROUP BY customer",
    )
    .expect("parse non-unique dimension aggregate")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(statement)
        .expect("plan non-unique dimension aggregate");
    let planned = annotate_cardinalities(
        planner
            .take_plan()
            .expect("logical non-unique dimension aggregate"),
    );

    let (rewritten, changed) = dimension_deferral::optimize_plan(
        planned,
        &planner.binder.bind_context,
        &crate::cost_model::CostModel::default(),
    )
    .expect("inspect non-unique dimension aggregate");
    assert!(!changed);
    let mut aggregates = 0usize;
    let mut customer_gets = 0usize;
    rewritten
        .try_visit_pre_order(|plan| {
            match &plan.operator {
                LogicalOperator::Aggregate(_) => aggregates += 1,
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "customer") =>
                {
                    customer_gets += 1;
                }
                _ => {}
            }
            Ok(())
        })
        .expect("inspect unchanged aggregate");

    assert_eq!(aggregates, 1, "{rewritten:#?}");
    assert_eq!(customer_gets, 1, "{rewritten:#?}");
}

#[test]
fn selective_fact_subtree_does_not_use_unfiltered_leaf_cardinality() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT nation, sum(amount) \
         FROM ( \
             SELECT n_name AS nation, s_acctbal AS amount \
             FROM supplier JOIN nation ON s_nationkey = n_nationkey \
         ) AS profit \
         GROUP BY nation",
    )
    .expect("parse selective dimension aggregate")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(statement)
        .expect("plan selective dimension aggregate");
    let planned = annotate_cardinalities_with_join_rows(
        planner
            .take_plan()
            .expect("logical selective dimension aggregate"),
        1_000,
    )
    .try_map_post_order(|mut plan| {
        if matches!(&plan.operator,
            LogicalOperator::Get(get)
                if get.table.as_ref().is_some_and(|table| table.base.base.name == "supplier"))
        {
            plan.stats.estimated_cardinality = Some(CardinalityEstimate {
                min: 0,
                expected: 1_000,
                max: 10_000,
            });
        }
        Ok(plan)
    })
    .expect("annotate selective fact source");

    let (_, changed) = dimension_deferral::optimize_plan(
        planned,
        &planner.binder.bind_context,
        &crate::cost_model::CostModel::default(),
    )
    .expect("inspect selective dimension aggregate");
    assert!(!changed);
}

fn collect_bindings(
    expression: &paro_planner::expression::Expression,
    bindings: &mut Vec<paro_planner::operator::ColumnBinding>,
) {
    visit_expression(expression, &mut |expression| {
        if let paro_planner::expression::Expression::ColumnRef(column) = expression {
            bindings.push(column.binding);
        }
    });
}

fn annotate_cardinalities(plan: LogicalPlan) -> LogicalPlan {
    annotate_cardinalities_with_join_rows(plan, 10_000)
}

fn annotate_cardinalities_with_join_rows(plan: LogicalPlan, join_rows: u64) -> LogicalPlan {
    plan.try_map_post_order(|mut plan| {
        let expected = match &plan.operator {
            LogicalOperator::Get(get) => match get
                .table
                .as_ref()
                .map(|table| table.base.base.name.as_str())
            {
                Some("supplier") => 10_000,
                Some("nation") => 25,
                _ => 1_000,
            },
            LogicalOperator::Aggregate(_) => 25,
            LogicalOperator::Join(_) => join_rows,
            _ => 10_000,
        };
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(expected));
        if matches!(plan.operator, LogicalOperator::Join(_)) {
            plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
        }
        Ok(plan)
    })
    .expect("annotate cardinalities")
}
