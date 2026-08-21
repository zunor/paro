// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::LogicalOperator;
use paro_planner::planner::Planner;

use super::input_materialization;
use crate::subquery::partition_aggregate_tests::setup_session;

#[test]
fn total_narrow_aggregate_input_is_materialized_below_fact_joins() {
    let (rewritten, changed) = optimize_sql(
        "SELECT n_name, \
                sum(l_extendedprice * (1 - l_discount) \
                    - ps_supplycost * l_quantity) \
         FROM lineitem \
         JOIN partsupp \
           ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
         JOIN supplier ON s_suppkey = l_suppkey \
         JOIN nation ON n_nationkey = s_nationkey \
         GROUP BY n_name",
    );
    assert!(changed, "{rewritten:#?}");

    let materialized = materialized_projection_count(&rewritten);
    assert_eq!(materialized, 1, "{rewritten:#?}");
}

#[test]
fn fallible_aggregate_input_keeps_its_original_row_domain() {
    let (rewritten, changed) = optimize_sql(
        "SELECT n_name, \
                sum(l_extendedprice / l_discount \
                    - ps_supplycost * l_quantity) \
         FROM lineitem \
         JOIN partsupp \
           ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
         JOIN supplier ON s_suppkey = l_suppkey \
         JOIN nation ON n_nationkey = s_nationkey \
         GROUP BY n_name",
    );

    assert!(!changed, "{rewritten:#?}");
    assert_eq!(materialized_projection_count(&rewritten), 0);
}

#[test]
fn source_binding_used_outside_candidate_declines_width_proof() {
    let (rewritten, changed) = optimize_sql(
        "SELECT n_name, l_quantity, \
                sum(l_extendedprice * (1 - l_discount) \
                    - ps_supplycost * l_quantity) \
         FROM lineitem \
         JOIN partsupp \
           ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
         JOIN supplier ON s_suppkey = l_suppkey \
         JOIN nation ON n_nationkey = s_nationkey \
         GROUP BY n_name, l_quantity",
    );

    assert!(!changed, "{rewritten:#?}");
    assert_eq!(materialized_projection_count(&rewritten), 0);
}

fn optimize_sql(sql: &str) -> (paro_planner::plan::LogicalPlan, bool) {
    let session = setup_session();
    let statement = paro_parser::parse_one(sql)
        .expect("parse aggregate input materialization")
        .stmt;
    let mut planner = Planner::new(session);
    planner
        .create_plan(statement)
        .expect("plan aggregate input materialization");
    input_materialization::optimize_plan(
        planner
            .take_plan()
            .expect("logical aggregate input materialization"),
        &planner.binder.bind_context,
    )
    .expect("optimize aggregate input materialization")
}

fn materialized_projection_count(plan: &paro_planner::plan::LogicalPlan) -> usize {
    let mut count = 0;
    plan.try_visit_pre_order(|plan| {
        if matches!(&plan.operator,
            LogicalOperator::Projection(projection)
                if projection.visible_names.last().is_some_and(
                    |name| name == "__paro_materialized_aggregate_input"))
        {
            count += 1;
        }
        Ok(())
    })
    .expect("inspect aggregate input materialization");
    count
}
