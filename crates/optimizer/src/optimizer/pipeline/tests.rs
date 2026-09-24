// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_planner::planner::Planner;

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
        let mut session = crate::subquery::partition_aggregate_tests::setup_session();
        Arc::get_mut(&mut session).unwrap().limits.use_temporary_directory = true;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(paro_parser::parse_one(sql).unwrap().stmt).unwrap();
        let plan = planner.take_plan().unwrap();
        let mut optimizer = Optimizer::new(planner.binder.clone(), session).with_budget(SearchBudget {
            search_policy: Some(paro_context::OptimizerSearchPolicy::Pipeline), ..Default::default()
        });
        let result = optimizer.optimize(plan).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let OptimizedStatement::Physical(portfolio) = result else { panic!("not a physical query") };
        portfolio.verify().unwrap();
        assert_eq!(portfolio.variants.len(), 1, "{sql}");
        assert_eq!(portfolio.grant_classes.len(), 1, "{sql}");
        let receipt = optimizer.compile_receipt().unwrap();
        assert_eq!(receipt.groups, Observed(0));
        assert_eq!(receipt.logical_expressions, Observed(0));
        assert_eq!(receipt.physical_expressions, Observed(0));
        assert_eq!(receipt.search_complete, Observed(false));
    }
}
