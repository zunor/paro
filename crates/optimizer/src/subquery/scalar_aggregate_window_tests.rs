// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::LogicalOperator;
use paro_planner::planner::Planner;
use std::collections::HashMap;
use std::sync::Arc;

use super::partition_aggregate_tests::setup_session;
use crate::optimizer::Optimizer;

#[test]
fn subset_filtered_scalar_aggregate_reuses_detail_scan() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT c.c_custkey \
         FROM customer AS c \
         WHERE c.c_acctbal < 100000 \
           AND c.c_acctbal > ( \
               SELECT avg(s.c_acctbal) \
               FROM customer AS s \
               WHERE s.c_acctbal < 100000 AND 0 < s.c_acctbal)",
    )
    .expect("parse scalar aggregate sharing shape")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(statement)
        .expect("plan scalar aggregate sharing shape");
    let plan = planner.take_plan().expect("logical plan");
    let mut optimizer = Optimizer::new(planner.binder.clone(), session);
    let optimized = optimizer.optimize(plan).expect("optimize scalar aggregate");

    let mut customer_gets = 0usize;
    let mut global_aggregate_windows = 0usize;
    optimized
        .try_visit_pre_order(|plan| {
            match &plan.operator {
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "customer") =>
                {
                    customer_gets += 1;
                }
                LogicalOperator::Window(window)
                    if window.expressions.iter().any(|expression| {
                        expression.partitions.is_empty()
                            && expression.aggregate_invocation().is_some()
                    }) =>
                {
                    global_aggregate_windows += 1;
                }
                _ => {}
            }
            Ok(())
        })
        .expect("inspect optimized plan");

    assert_eq!(customer_gets, 1, "{optimized:#?}");
    assert_eq!(global_aggregate_windows, 1, "{optimized:#?}");
}

#[test]
fn matched_prefix_has_an_independent_binding_from_the_stored_value() {
    for sql in [
        "SELECT substring(c_phone FROM 1 FOR 2) \
         FROM customer \
         WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31') \
           AND c_phone > '13'",
        "SELECT substring(c_phone FROM 1 FOR 2), c_phone \
         FROM customer \
         WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31')",
    ] {
        let session = setup_session();
        let statement = paro_parser::parse_one(sql)
            .expect("parse rejected prefix proof shape")
            .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan rejected prefix proof shape");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let optimized = optimizer.optimize(plan).expect("optimize prefix query");

        optimized
            .try_visit_pre_order(|plan| {
                if let LogicalOperator::Get(get) = &plan.operator {
                    let matched = get
                        .column_projections
                        .iter()
                        .position(|projection| {
                            matches!(
                                projection,
                                paro_planner::operator::GetColumnProjection::MatchedUtf8Prefix {
                                    byte_width: 2
                                }
                            )
                        })
                        .expect("independent matched-prefix output");
                    let source_id = get.column_ids[matched];
                    assert!(get
                        .column_ids
                        .iter()
                        .enumerate()
                        .any(|(index, &column_id)| {
                            column_id == source_id
                                && index != matched
                                && matches!(
                                    get.column_projections[index],
                                    paro_planner::operator::GetColumnProjection::Stored
                                )
                        }));
                }
                Ok(())
            })
            .expect("inspect prefix query");
    }
}

#[test]
fn matched_prefix_requires_a_direct_pushdown_witness() {
    for (sql, pushdown_enabled) in [
        (
            "SELECT substring(c_phone FROM 1 FOR 2) \
             FROM customer \
             WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31') \
                OR c_acctbal > 0",
            true,
        ),
        (
            "SELECT substring(c_phone FROM 1 FOR 2) \
             FROM customer \
             WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31')",
            false,
        ),
    ] {
        let mut session = setup_session();
        if !pushdown_enabled {
            Arc::get_mut(&mut session).expect("fresh session").settings =
                Arc::new(paro_context::EffectiveSettings::new(HashMap::from([(
                    "rowset_scan_pushdown".to_string(),
                    paro_common::runtime_value::Value::Boolean(false),
                )])));
        }
        let statement = paro_parser::parse_one(sql)
            .expect("parse rejected prefix proof shape")
            .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan rejected prefix proof shape");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let optimized = optimizer.optimize(plan).expect("optimize prefix query");

        optimized
            .try_visit_pre_order(|plan| {
                if let LogicalOperator::Get(get) = &plan.operator {
                    assert!(get.column_projections.iter().all(|projection| matches!(
                        projection,
                        paro_planner::operator::GetColumnProjection::Stored
                    )));
                }
                Ok(())
            })
            .expect("inspect prefix query");
    }
}

#[test]
fn tpch_q22_uses_one_customer_scan() {
    std::thread::Builder::new()
        .name("tpch-q22-scalar-window-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(tpch_q22_uses_one_customer_scan_inner)
        .expect("spawn Q22 optimizer test")
        .join()
        .expect("Q22 optimizer test");
}

fn tpch_q22_uses_one_customer_scan_inner() {
    let session = setup_session();
    let statement = paro_parser::parse_one(include_str!(
        "../../../../benchmark/workloads/tpch/sql/q22.sql"
    ))
    .expect("parse Q22")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner.create_plan(statement).expect("plan Q22");
    let plan = planner.take_plan().expect("logical Q22 plan");
    let mut optimizer = Optimizer::new(planner.binder.clone(), session);
    let optimized = optimizer.optimize(plan).expect("optimize Q22");

    let mut customer_gets = 0usize;
    let mut global_aggregate_windows = 0usize;
    let mut matched_prefix_outputs = 0usize;
    let mut row_fetches = 0usize;
    optimized
        .try_visit_pre_order(|plan| {
            match &plan.operator {
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "customer") =>
                {
                    customer_gets += 1;
                    matched_prefix_outputs += get
                        .column_projections
                        .iter()
                        .filter(|projection| {
                            matches!(
                                projection,
                                paro_planner::operator::GetColumnProjection::MatchedUtf8Prefix {
                                    byte_width: 2
                                }
                            )
                        })
                        .count();
                }
                LogicalOperator::Window(window)
                    if window.expressions.iter().any(|expression| {
                        expression.partitions.is_empty()
                            && expression.aggregate_invocation().is_some()
                    }) =>
                {
                    global_aggregate_windows += 1;
                }
                LogicalOperator::RowFetch(_) => row_fetches += 1,
                _ => {}
            }
            Ok(())
        })
        .expect("inspect Q22 plan");
    assert_eq!(customer_gets, 1, "{optimized:#?}");
    assert_eq!(global_aggregate_windows, 1, "{optimized:#?}");
    assert_eq!(matched_prefix_outputs, 1, "{optimized:#?}");
    assert_eq!(row_fetches, 0, "{optimized:#?}");
}
