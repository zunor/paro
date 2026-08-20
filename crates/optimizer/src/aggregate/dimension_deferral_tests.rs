// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::{Join, LogicalOperator};
use paro_planner::plan::{CardinalityEstimate, LogicalPlan};
use paro_planner::planner::Planner;

use super::dimension_deferral;
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

    let rewritten = dimension_deferral::optimize_plan(planned, &planner.binder.bind_context)
        .expect("rewrite dimension aggregate");
    let mut aggregates = 0usize;
    let mut nation_gets = 0usize;
    let mut payload_free_dimension_filters = 0usize;
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
                }
                LogicalOperator::Join(Join::Comparison(join))
                    if join.right_projection_map.is_none() =>
                {
                    payload_free_dimension_filters += 1;
                }
                _ => {}
            }
            Ok(())
        })
        .expect("inspect rewritten plan");

    assert_eq!(aggregates, 2, "{rewritten:#?}");
    assert_eq!(nation_gets, 2, "{rewritten:#?}");
    assert_eq!(payload_free_dimension_filters, 1, "{rewritten:#?}");
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

    let rewritten = dimension_deferral::optimize_plan(planned, &planner.binder.bind_context)
        .expect("inspect non-unique dimension aggregate");
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

fn annotate_cardinalities(plan: LogicalPlan) -> LogicalPlan {
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
            _ => 10_000,
        };
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(expected));
        Ok(plan)
    })
    .expect("annotate cardinalities")
}
