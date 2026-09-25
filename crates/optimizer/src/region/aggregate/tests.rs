// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::optimizer::Optimizer;
use paro_planner::binder::Planner;

#[test]
fn filter_comparisons_connect_region_and_become_exact_cut_conditions() {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ComparisonExpression, ComparisonType, ConjunctionExpression};
    use paro_planner::logical::operator::{ExpressionGet, Filter, JoinComparisonType};
    let column = |table, index| {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, index), LogicalType::Integer).into(),
        )
    };
    let input = |table| {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table,
            vec![],
            vec!["key".into(), "value".into()],
            vec![LogicalType::Integer; 2],
        )))
    };
    let equality = |l, r| JoinCondition::new(l, r, JoinComparisonType::Equal);
    let ab = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            input(1),
            input(2),
            vec![equality(column(1, 0), column(2, 0))],
        ),
    )));
    let abc = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            ab,
            input(3),
            vec![equality(column(1, 1), column(3, 0))],
        ),
    )));
    let expressions = [
        Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, column(2, 1), column(3, 1)).into(),
        ),
        Expression::Comparison(
            ComparisonExpression::new(ComparisonType::LessThan, column(1, 1), column(2, 1)).into(),
        ),
    ];
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        abc,
        vec![Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::And, expressions.to_vec()).into(),
        )],
    )));
    let region = Region::recognize_joins(&plan).unwrap();
    assert_eq!(region.conditions.len(), 4);
    assert!(region.residuals.is_empty());
    // B-C is now a real edge, not a residual waiting for A-B-C. Joining
    // A with B-C owns two equality keys and the range residual exactly once.
    assert_eq!(region.cut(2, 4).len(), 1);
    let cut = region.cut(1, 6);
    assert_eq!(cut.len(), 3);
    assert_eq!(
        cut.iter()
            .filter(|c| c.comparison == JoinComparisonType::Equal)
            .count(),
        2
    );
    assert_eq!(
        region.cut(6, 1).last().unwrap().comparison,
        JoinComparisonType::GreaterThan
    );

    let LogicalOperator::Filter(mut filter) = plan.into_parts().2 else {
        unreachable!()
    };
    filter.expressions = vec![Expression::Conjunction(
        ConjunctionExpression::new(ConjunctionType::Or, expressions.to_vec()).into(),
    )];
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter));
    let region = Region::recognize_joins(&plan).unwrap();
    assert_eq!(region.conditions.len(), 2);
    assert_eq!(
        region.residuals.len(),
        1,
        "OR must remain one predicate, not two hash keys"
    );
}

fn exercise(sql: &str, budget: usize) -> Work {
    exercise_domain(sql, budget, false)
}

fn exercise_domain(sql: &str, budget: usize, ordinary: bool) -> Work {
    exercise_with_statistics(sql, budget, ordinary, false)
}

fn exercise_with_statistics(sql: &str, budget: usize, ordinary: bool, known: bool) -> Work {
    let session = crate::tests::catalog::setup_session();
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(paro_parser::parse_one(sql).unwrap().stmt)
        .unwrap();
    let query = planner.take_plan().unwrap();
    let (plan, mut context, calibration) = Optimizer::new(planner.binder.clone(), session)
        .prepare_region_for_test(query)
        .unwrap();
    if known {
        for statistic in context.column_stats_mut().values_mut() {
            let value = paro_storage::statistics::ColumnStatistics::with_estimated_distinct(
                statistic.statistics().clone(),
                Some(10),
            );
            *statistic = Arc::new(value);
        }
    }
    let grant = ResourceGrantClass {
        id: crate::physical::ResourceGrantClassId(0),
        hard_memory_bytes: 2 * 1024 * 1024 * 1024,
        spill_policy: crate::physical::SpillPolicy::Allowed,
        max_parallel_tasks: 4,
    };
    let mut work = Work::default();
    plan.try_visit_pre_order(|plan| {
        let optimize = if ordinary { optimize_joins } else { optimize };
        if let Some(selected) = optimize(plan, &context, grant, &calibration, budget, &mut work)? {
            if known {
                let oracle_region = if ordinary {
                    Region::recognize_joins(plan)
                } else {
                    Region::recognize(plan)
                };
                if let Some(region) = oracle_region.filter(|r| r.leaves.len() <= 4) {
                    let oracle = optimize_region_with(
                        region,
                        &context,
                        grant,
                        &calibration,
                        budget,
                        &mut Work::default(),
                        |enumeration| {
                            let full = enumeration.states.len() as Mask - 1;
                            for size in 2..=enumeration.region.leaves.len() {
                                for mask in 1..=full {
                                    if mask.count_ones() as usize != size {
                                        continue;
                                    }
                                    let mut left = (mask - 1) & mask;
                                    while left != 0 {
                                        let right = mask ^ left;
                                        if left < right {
                                            enumeration.price_pair(left, right).unwrap();
                                        }
                                        left = (left - 1) & mask;
                                    }
                                }
                            }
                            crate::region::join::enumerator::EnumerationOutcome::Complete
                        },
                    )?
                    .expect("independent subset oracle has a plan");
                    let actual = selected.cost.score.range.expected;
                    let expected = oracle.cost.score.range.expected;
                    assert!(
                        (actual - expected).abs() <= expected.abs().max(1.0) * 1e-10,
                        "connected traversal {actual} != exhaustive subset oracle {expected}"
                    );
                }
            }
            assert_eq!(
                selected.plan.get_column_bindings(),
                plan.get_column_bindings()
            );
            assert_eq!(selected.plan.types(), plan.types());
            let mut joins = 0;
            selected.plan.try_visit_pre_order(|node| {
                assert!(
                    !matches!(node.operator, LogicalOperator::SubplanRef(_)),
                    "no pricing boundary may escape reconstruction"
                );
                assert!(
                    !matches!(node.operator, LogicalOperator::Join(Join::Cross(_))),
                    "connected test regions must not retain an avoidable Cartesian product"
                );
                if let LogicalOperator::Join(Join::Comparison(join)) = &node.operator {
                    joins += 1;
                    let left = join.left.get_column_bindings();
                    let right = join.right.get_column_bindings();
                    for condition in &join.conditions {
                        assert!(columns(&condition.left)
                            .unwrap()
                            .iter()
                            .all(|c| left.contains(c)));
                        assert!(columns(&condition.right)
                            .unwrap()
                            .iter()
                            .all(|c| right.contains(c)));
                    }
                }
                Ok(())
            })?;
            assert!(joins > 0);
        }
        Ok(())
    })
    .unwrap();
    work
}

const QUERY: &str = "SELECT n_name, r_name, sum(s_acctbal) FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey GROUP BY n_name,r_name";

#[test]
fn borrowed_cut_response_equals_full_settlement_and_discards_losing_outputs() {
    // Complete every retained response with the independent full native
    // settlement path. complete_cut asserts exact cost/implementation
    // agreement, not just matching the final SQL layout.
    let work = exercise_with_statistics(
        "SELECT a.s_acctbal FROM supplier a JOIN supplier b ON a.s_nationkey=b.s_nationkey JOIN supplier c ON b.s_nationkey=c.s_nationkey JOIN supplier d ON c.s_nationkey=d.s_nationkey",
        10000, true, true,
    );
    assert!(work.borrowed_cuts > 0);
    assert!(work.completed_outputs < work.transitions);
}

#[test]
fn borrowed_join_pricing_retains_aggregate_grain_contract() {
    let work = exercise_with_statistics(QUERY, 10000, false, true);
    assert!(work.borrowed_cuts > 0);
    assert!(work.partial_states > 0);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn ordinary_regions_use_physical_response_and_preserve_output_bindings() {
    let work = exercise_domain("SELECT r_name,n_name,s_acctbal FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey WHERE s_acctbal > n_nationkey", 10000, true);
    assert!(work.regions > 0);
    assert!(work.transitions > 0);
    assert_eq!(work.partial_states, 0);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn mixed_equality_and_range_conditions_share_the_connected_traversal() {
    let work = exercise_domain(
        "SELECT a.s_acctbal FROM supplier a JOIN supplier b ON a.s_nationkey=b.s_nationkey AND a.s_acctbal < b.s_acctbal JOIN nation ON b.s_nationkey=n_nationkey",
        10000, true,
    );
    assert!(work.regions > 0);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn ordinary_region_is_not_limited_to_eight_relations() {
    let mut query = "SELECT a0.s_acctbal FROM supplier a0".to_string();
    for i in 1..9 {
        query.push_str(&format!(
            " JOIN supplier a{i} ON a{}.s_nationkey=a{i}.s_nationkey",
            i - 1
        ));
    }
    let work = exercise_with_statistics(&query, 65536, true, true);
    assert!(work.regions > 0);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn connected_region_opens_cartesian_input_before_committing_physical_choices() {
    // The third relation connects the first two. Treating their syntactic
    // CROSS PRODUCT as an atomic leaf locks in an arbitrarily large
    // intermediate, even though the complete region is connected.
    let work = exercise_domain("SELECT s_acctbal,r_name,n_name FROM supplier CROSS JOIN region JOIN nation ON s_nationkey=n_nationkey AND r_regionkey=n_regionkey", 10000, true);
    assert!(work.regions > 0);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn genuinely_disconnected_region_keeps_the_explicit_fallback_domain() {
    let work = exercise_domain(
        "SELECT s_acctbal,r_name FROM supplier CROSS JOIN region",
        10000,
        true,
    );
    assert_eq!(work.regions, 0);
}

#[test]
fn joint_states_explore_multiple_aggregation_cuts_without_tree_copies() {
    let work = exercise(QUERY, 10000);
    assert_eq!(work.regions, 1);
    assert!(
        work.partial_states >= 2,
        "must consider more than a single dimension-deferral tree"
    );
    assert!(work.transitions > work.partial_states);
    assert_eq!(work.budget_fallbacks, 0);
}

#[test]
fn joint_budget_retains_original_instead_of_claiming_infeasibility() {
    let work = exercise(QUERY, 0);
    assert_eq!(work.regions, 0);
    assert_eq!(work.transitions, 0);
    assert_eq!(work.budget_fallbacks, 1);
}

#[test]
fn unsupported_aggregate_laws_do_not_enter_joint_search() {
    let work = exercise("SELECT n_name,r_name,count(DISTINCT s_acctbal) FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey GROUP BY n_name,r_name", 10000);
    assert_eq!(work.regions, 0);
    assert_eq!(work.transitions, 0);
}
