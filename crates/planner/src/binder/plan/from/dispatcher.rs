// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundFromItem;
use crate::binder::Binder;
use crate::operator::LogicalOperator;
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_table_ref(&mut self, table_ref: BoundFromItem) -> Result<LogicalOperator> {
        match table_ref {
            BoundFromItem::BaseTable(base_ref) => self.plan_base_table_ref(base_ref),
            BoundFromItem::Join(join_ref) => self.plan_join_ref(join_ref),
            BoundFromItem::Subquery(sub_ref) => self.plan_subquery_ref(sub_ref),
            BoundFromItem::TableFunction(tf_ref) => self.plan_table_function_ref(tf_ref),
            BoundFromItem::CTE(cte_ref) => self.plan_cte_ref(cte_ref),
            BoundFromItem::GraphTable(graph_ref) => self.plan_graph_table_ref(graph_ref),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder;
    use crate::operator::{ComparisonJoin, Join};
    use paro_parser::parse_one;

    fn contains_dependent_join(plan: &LogicalOperator) -> bool {
        match plan {
            LogicalOperator::DependentJoin(_) => true,
            _ => plan
                .children()
                .iter()
                .any(|child| contains_dependent_join(&child.operator)),
        }
    }

    fn find_first_comparison_join(plan: &LogicalOperator) -> Option<&ComparisonJoin> {
        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => Some(join),
            _ => plan
                .children()
                .iter()
                .find_map(|child| find_first_comparison_join(&child.operator)),
        }
    }

    #[test]
    fn planner_flattens_inner_lateral_join_into_delim_ready_comparison_join() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t JOIN LATERAL (SELECT t.x AS y) s ON true")
                .expect("parse")
                .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Inner);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_cross_lateral_join_into_inner_comparison_join() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t CROSS JOIN LATERAL (SELECT t.x AS y) s")
                .expect("parse")
                .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Inner);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_left_lateral_join_with_on_true() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * FROM (SELECT 1 AS x) t LEFT JOIN LATERAL (SELECT t.x AS y) s ON true",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Left);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_subquery_inside_inner_join_on_condition() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * \
             FROM (SELECT 1 AS x) t \
             JOIN (SELECT 1 AS y) s \
               ON s.y = t.x \
              AND EXISTS (SELECT 1 WHERE t.x = 1)",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));
    }

    #[test]
    fn planner_flattens_nested_outer_correlated_subquery_inside_lateral_rhs() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * \
             FROM (VALUES (10), (20)) AS o(grp) \
             CROSS JOIN LATERAL ( \
               SELECT EXISTS( \
                 SELECT 1 \
                 WHERE EXISTS( \
                   SELECT 1 \
                   FROM (VALUES (10, 4), (20, 5)) AS d(grp, score) \
                   WHERE d.grp = o.grp \
                 ) \
               ) AS has_match \
             ) AS s",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));
        let plan_debug = format!("{:?}", bound.plan);
        assert!(!plan_debug.contains("Subquery("));
    }
}
