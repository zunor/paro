// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::bind::from::join_utils::{
    collect_table_bindings, convert_join_type, create_join_operator, extract_join_condition,
    get_expression_side, split_conjunction,
};
use crate::binder::ir::BoundJoin;
use crate::binder::plan::subquery::{flatten_dependent_join, RecursiveSubqueryPlanner};
use crate::binder::Binder;
use crate::operator::{
    CrossProduct, DependentJoin, Filter, Join, JoinSide, JoinType, LogicalOperator,
};
use paro_common::error::{self as paro_error, Result};

impl Binder {
    pub(crate) fn plan_join_ref(&mut self, join_ref: BoundJoin) -> Result<LogicalOperator> {
        let mut left_child = self.plan_table_ref(*join_ref.left)?;
        let mut right_child = if join_ref.lateral && !join_ref.correlated_columns.is_empty() {
            self.with_delayed_subquery_planning_disabled(|binder| {
                binder.plan_table_ref(*join_ref.right)
            })?
        } else {
            self.plan_table_ref(*join_ref.right)?
        };
        let join_type = convert_join_type(join_ref.join_type);

        if join_ref.lateral && !join_ref.correlated_columns.is_empty() {
            let dependent_join = DependentJoin::lateral(
                self.wrap_plan(left_child),
                self.wrap_plan(right_child),
                join_ref.correlated_columns,
                join_type,
                join_ref.condition,
            );

            let mut flattened = flatten_dependent_join(self, dependent_join)?;
            RecursiveSubqueryPlanner::new(self).plan_root(&mut flattened)?;
            return Ok(flattened);
        }

        if join_ref.condition.is_none() {
            let cross = CrossProduct::new(self.wrap_plan(left_child), self.wrap_plan(right_child));
            return Ok(LogicalOperator::Join(Join::Cross(cross)));
        }

        let condition = join_ref.condition.expect("checked above");
        let left_bindings = collect_table_bindings(&left_child);
        let right_bindings = collect_table_bindings(&right_child);

        let mut conditions = Vec::new();
        let mut arbitrary_expressions = Vec::new();
        for expr in split_conjunction(condition) {
            extract_join_condition(
                expr,
                &left_bindings,
                &right_bindings,
                &mut conditions,
                &mut arbitrary_expressions,
            );
        }

        // Side-local predicates on the null-supplying side can be evaluated before an
        // outer/semi/anti join without changing which preserved rows are emitted. Doing
        // this before join construction retains equi-conditions for the hash join instead
        // of degrading the whole ON clause to an arbitrary nested-loop predicate.
        let mut left_filters = Vec::new();
        let mut right_filters = Vec::new();
        arbitrary_expressions.retain(|expression| {
            let side = get_expression_side(expression, &left_bindings, &right_bindings);
            match (join_type, side) {
                (
                    JoinType::Left | JoinType::Single | JoinType::Semi | JoinType::Anti,
                    JoinSide::Right,
                ) => {
                    right_filters.push(expression.clone());
                    false
                }
                (JoinType::Right | JoinType::RightSemi | JoinType::RightAnti, JoinSide::Left) => {
                    left_filters.push(expression.clone());
                    false
                }
                _ => true,
            }
        });
        if !left_filters.is_empty() {
            left_child =
                LogicalOperator::Filter(Filter::new(self.wrap_plan(left_child), left_filters));
        }
        if !right_filters.is_empty() {
            right_child =
                LogicalOperator::Filter(Filter::new(self.wrap_plan(right_child), right_filters));
        }

        let has_subquery_in_on = arbitrary_expressions.iter().any(Self::contains_subquery);
        if !has_subquery_in_on {
            return create_join_operator(
                &self.bind_context,
                join_type,
                left_child,
                right_child,
                conditions,
                arbitrary_expressions,
            );
        }

        if join_type != JoinType::Inner {
            return Err(paro_error::not_implemented(
                "Subqueries in non-inner JOIN ON conditions are outside this refactor's supported scope",
            ));
        }

        let mut root = create_join_operator(
            &self.bind_context,
            join_type,
            left_child,
            right_child,
            conditions,
            Vec::new(),
        )?;
        for expr in &mut arbitrary_expressions {
            self.plan_subqueries(expr, &mut root)?;
        }

        if arbitrary_expressions.is_empty() {
            Ok(root)
        } else {
            Ok(LogicalOperator::Filter(Filter::new(
                self.wrap_plan(root),
                arbitrary_expressions,
            )))
        }
    }
}
