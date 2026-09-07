// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::{ColumnBinding, Join, LogicalOperator};
use paro_planner::plan::{
    CardinalityEstimate, CardinalityProvenance, OwnedLogicalPlan, NodeStats, PlanNodeId,
};
use paro_planner::planner::Planner;

use super::dimension_deferral;
use crate::expression::traversal::visit_expression;
use crate::subquery::partition_aggregate_tests::setup_session;

#[test]
fn memo_rule_does_not_rewrite_descendants_of_a_root_projection() {
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
    let mut planner = Planner::new(session);
    planner
        .create_plan(statement)
        .expect("plan dimension aggregate");
    let planned = planner.take_plan().expect("logical dimension aggregate");

    let (_, changed) = dimension_deferral::optimize_plan(planned, &planner.binder.bind_context)
        .expect("apply root-local rule");
    assert!(!changed, "Memo, rather than a rule firing, owns traversal");
}

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

    let (rewritten, changed) = rewrite_root_aggregate(planned, &planner.binder.bind_context)
        .expect("rewrite dimension aggregate");
    assert!(changed);
    let mut aggregates = 0usize;
    let mut nation_gets = 0usize;
    let mut nation_table_indices = std::collections::HashSet::new();
    let mut final_join_stats = None;
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
    assert_eq!(nation_gets, 1, "{rewritten:#?}");
    assert_eq!(nation_table_indices.len(), 1, "{rewritten:#?}");
    let (final_join_id, final_stats) =
        final_join_stats.expect("rewritten plan has a final dimension join");
    assert_ne!(final_join_id, PlanNodeId::SYNTHETIC);
    assert_eq!(final_stats, NodeStats::default());
    let final_dimension_indices = final_join_bindings
        .iter()
        .map(|binding| binding.table_index)
        .filter(|index| nation_table_indices.contains(index))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(final_dimension_indices.len(), 1, "{rewritten:#?}");
}

#[test]
fn settled_partial_key_prevents_repeated_dimension_deferral() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT n_name, sum(s_acctbal) FROM supplier JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name",
    ).unwrap().stmt;
    let mut planner = Planner::new(session);
    planner.create_plan(statement).unwrap();
    let (plan, changed) =
        rewrite_root_aggregate(planner.take_plan().unwrap(), &planner.binder.bind_context).unwrap();
    assert!(changed);
    let plan = crate::statistics::unique_keys::refresh_unique_keys(plan).unwrap();
    let (_, changed) = rewrite_root_aggregate(plan, &planner.binder.bind_context).unwrap();
    assert!(
        !changed,
        "a proven partial key must not grow another aggregate/merge layer"
    );
}

#[test]
fn duplicate_dimension_keys_retain_join_multiplicity_through_final_merge() {
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

    let (rewritten, changed) = rewrite_root_aggregate(planned, &planner.binder.bind_context)
        .expect("rewrite non-unique dimension aggregate");
    assert!(changed);
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
        .expect("inspect rewritten aggregate");

    assert_eq!(aggregates, 2, "{rewritten:#?}");
    assert_eq!(customer_gets, 1, "{rewritten:#?}");
}

#[test]
fn multiway_region_isolates_the_widest_grouping_dimension() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT c_name, c_address, n_name, sum(s_acctbal) \
         FROM supplier \
         JOIN customer ON s_nationkey = c_nationkey \
         JOIN nation ON s_nationkey = n_nationkey \
         GROUP BY c_name, c_address, n_name",
    )
    .expect("parse multiway dimension aggregate")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner
        .create_plan(statement)
        .expect("plan multiway dimension aggregate");
    let planned = annotate_cardinalities(
        planner
            .take_plan()
            .expect("logical multiway dimension aggregate"),
    );

    let (rewritten, changed) = rewrite_root_aggregate(planned, &planner.binder.bind_context)
        .expect("rewrite multiway dimension aggregate");
    assert!(changed, "{rewritten:#?}");
    let mut attached_dimension = None;
    rewritten
        .try_visit_pre_order(|plan| {
            if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
                if attached_dimension.is_none() {
                    if let LogicalOperator::Get(get) = &join.left.operator {
                        attached_dimension =
                            get.table.as_ref().map(|table| table.base.base.name.clone());
                    }
                }
            }
            Ok(())
        })
        .expect("inspect multiway rewrite");
    assert_eq!(attached_dimension.as_deref(), Some("customer"));
    assert_join_conditions_follow_child_orientation(&rewritten);
}

fn assert_join_conditions_follow_child_orientation(plan: &OwnedLogicalPlan) {
    plan.try_visit_pre_order(|plan| {
        if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
            let left = join
                .left
                .get_column_bindings()
                .into_iter()
                .collect::<std::collections::HashSet<_>>();
            let right = join
                .right
                .get_column_bindings()
                .into_iter()
                .collect::<std::collections::HashSet<_>>();
            for condition in &join.conditions {
                assert_expression_bindings_belong_to(&condition.left, &left);
                assert_expression_bindings_belong_to(&condition.right, &right);
            }
        }
        Ok(())
    })
    .expect("inspect join condition orientation");
}

fn assert_expression_bindings_belong_to(
    expression: &paro_planner::expression::Expression,
    expected: &std::collections::HashSet<ColumnBinding>,
) {
    visit_expression(expression, &mut |expression| {
        if let paro_planner::expression::Expression::ColumnRef(column) = expression {
            assert!(
                column.depth != 0 || expected.contains(&column.binding),
                "binding {:?} is outside the expected child scope {expected:?}",
                column.binding
            );
        }
    });
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

fn rewrite_root_aggregate(
    mut plan: OwnedLogicalPlan,
    bind_context: &paro_planner::binder::context::BindContext,
) -> paro_common::error::Result<(OwnedLogicalPlan, bool)> {
    let LogicalOperator::Projection(projection) = &mut plan.operator else {
        return dimension_deferral::optimize_plan(plan, bind_context);
    };
    let child = std::mem::replace(
        &mut projection.child,
        Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    let (child, changed) = dimension_deferral::optimize_plan(*child, bind_context)?;
    projection.child = Box::new(child);
    Ok((plan, changed))
}

fn annotate_cardinalities(plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
    annotate_cardinalities_with_join_rows(plan, 10_000)
}

fn annotate_cardinalities_with_join_rows(plan: OwnedLogicalPlan, join_rows: u64) -> OwnedLogicalPlan {
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
