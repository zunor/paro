// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::region::limits::RegionLimits;
use paro_planner::binder::Planner;

#[test]
fn pipeline_real_entry_has_one_resource_contract_and_no_memo() {
    for sql in [
        "SELECT 1",
        "SELECT c_nationkey, sum(c_acctbal) FROM customer GROUP BY c_nationkey ORDER BY c_nationkey LIMIT 3",
        "SELECT DISTINCT c_nationkey FROM customer",
        "SELECT c.c_custkey FROM customer c JOIN nation n ON c.c_nationkey=n.n_nationkey WHERE n.n_nationkey=1",
        "SELECT c.c_custkey, n.n_name FROM customer c LEFT JOIN nation n ON c.c_nationkey=n.n_nationkey",
        "WITH x AS (SELECT c_nationkey FROM customer) SELECT a.c_nationkey FROM x a JOIN x b ON a.c_nationkey=b.c_nationkey",
        "SELECT c_custkey FROM customer WHERE EXISTS (SELECT 1 FROM nation WHERE n_nationkey=c_nationkey)",
        "SELECT c_nationkey FROM customer UNION ALL SELECT n_nationkey FROM nation",
    ] {
        let mut session = crate::tests::catalog::setup_session();
        Arc::get_mut(&mut session).unwrap().limits.use_temporary_directory = true;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(paro_parser::parse_one(sql).unwrap().stmt).unwrap();
        let plan = planner.take_plan().unwrap();
        let mut optimizer = Optimizer::new(planner.binder.clone(), session).with_limits(RegionLimits {
             ..Default::default()
        });
        let result = optimizer.optimize(plan).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let OptimizedStatement::Physical(artifact) = result else { panic!("not a physical query") };
        artifact.verify().unwrap();
        let receipt = optimizer.compile_receipt().unwrap();
        assert_eq!(receipt.expected_class, Observed(artifact.grant.id.0));




    }
}

#[test]
fn bounded_region_fallback_keeps_an_executable_plan_and_reports_its_limit() {
    let mut session = crate::tests::catalog::setup_session();
    Arc::get_mut(&mut session)
        .unwrap()
        .limits
        .use_temporary_directory = true;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(
            paro_parser::parse_one(
                "SELECT s_acctbal,n_name FROM supplier JOIN nation ON s_nationkey=n_nationkey",
            )
            .unwrap()
            .stmt,
        )
        .unwrap();
    let mut optimizer = Optimizer::new(planner.binder.clone(), session).with_limits(RegionLimits {
        connected_pairs: 0,
        ..Default::default()
    });
    let OptimizedStatement::Physical(artifact) =
        optimizer.optimize(planner.take_plan().unwrap()).unwrap()
    else {
        panic!("pipeline must retain the legal original when its region budget is exhausted")
    };
    artifact.verify().unwrap();
    let receipt = optimizer.compile_receipt().unwrap();
    assert_eq!(
        receipt.planning_status,
        Observed(PlanningStatus::PlannedWithFallback)
    );
    assert_eq!(receipt.budget_limited, Observed(true));
}

#[test]
fn normalization_eliminates_only_an_unobserved_unique_outer_lookup() {
    use paro_planner::physical::PhysicalNodeKind;
    for (sql, expected_joins) in [
        ("SELECT c.c_custkey FROM customer c LEFT JOIN nation n ON c.c_nationkey=n.n_nationkey", 0),
        ("SELECT c.c_custkey, n.n_name FROM customer c LEFT JOIN nation n ON c.c_nationkey=n.n_nationkey", 1),
        ("SELECT c.c_custkey FROM customer c LEFT JOIN nation n ON c.c_nationkey=n.n_regionkey", 1),
        ("SELECT c.c_custkey FROM customer c INNER JOIN nation n ON c.c_nationkey=n.n_nationkey", 1),
    ] {
        let mut session = crate::tests::catalog::setup_session();
        Arc::get_mut(&mut session).unwrap().limits.use_temporary_directory = true;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(paro_parser::parse_one(sql).unwrap().stmt).unwrap();
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let OptimizedStatement::Physical(artifact) = optimizer.optimize(planner.take_plan().unwrap()).unwrap() else {
            panic!("expected physical query");
        };
        artifact.verify().unwrap();
        let joins = artifact.plan.nodes.iter().filter(|node| matches!(node.kind,
            PhysicalNodeKind::HashJoin(_) | PhysicalNodeKind::NestedLoopJoin(_)
        )).count();
        assert_eq!(joins, expected_joins, "{sql}");
    }
}
