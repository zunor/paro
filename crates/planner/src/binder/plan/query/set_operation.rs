// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plans `UNION`/`INTERSECT`/`EXCEPT` (with `ALL` where applicable), inserting casts when column types differ.

use crate::binder::ir::{BoundQuery, BoundSetOperation, SetOperationType};
use crate::binder::plan::subquery::RecursiveSubqueryPlanner;
use crate::binder::Binder;
use crate::expression::{CastExpression, ColumnRefExpression, Expression};
use crate::operator::{
    LogicalOperator, MaterializedCTE, Projection, RecursiveCTE, SetOpType, SetOperation,
};
use paro_common::error::Result;
use paro_common::types::LogicalType;

impl Binder {
    pub(crate) fn plan_query(&mut self, node: BoundQuery) -> Result<LogicalOperator> {
        let mut root = match node {
            BoundQuery::With(n) => {
                let mut root = self.plan_query(*n.child)?;
                for state in n.ctes.iter().rev() {
                    if state.ref_count()? == 0 {
                        continue;
                    }
                    let bound_cte = state.bound.get().ok_or_else(|| {
                        paro_common::error::internal(format!(
                            "Referenced CTE '{}' was not bound",
                            state.info.name
                        ))
                    })?;
                    let cte_query = self.plan_cte_definition(bound_cte)?;
                    root = LogicalOperator::MaterializedCTE(
                        MaterializedCTE::new(
                            bound_cte.cte_index,
                            bound_cte.name.clone(),
                            bound_cte.names.clone(),
                            bound_cte.types.clone(),
                            bound_cte.materialized,
                            self.wrap_plan(cte_query),
                            self.wrap_plan(root),
                        )
                        .with_ref_count(state.ref_count()?),
                    );
                }
                Ok(root)
            }
            BoundQuery::Modifiers(n) => self.plan_query_modifiers(*n),
            BoundQuery::Select(n) => self.plan_select(*n),
            BoundQuery::Values(n) => self.plan_values(n),
            BoundQuery::SetOperation(n) => self.plan_set_operation(*n),
        }?;

        if self.delayed_subquery_planning_enabled() {
            RecursiveSubqueryPlanner::new(self).plan_root(&mut root)?;
        }

        Ok(root)
    }

    pub(crate) fn plan_set_operation(
        &mut self,
        node: BoundSetOperation,
    ) -> Result<LogicalOperator> {
        let left_plan = self.plan_query(*node.left)?;
        let right_plan = self.plan_query(*node.right)?;

        let left_types = left_plan.types();
        let right_types = right_plan.types();

        let left_plan = self.cast_to_types(left_plan, &left_types, &node.types)?;
        let right_plan = self.cast_to_types(right_plan, &right_types, &node.types)?;

        let (setop_type, setop_all) = match node.setop_type {
            SetOperationType::Union => (SetOpType::Union, false),
            SetOperationType::UnionAll => (SetOpType::Union, true),
            SetOperationType::Intersect => (SetOpType::Intersect, false),
            SetOperationType::IntersectAll => (SetOpType::Intersect, true),
            SetOperationType::Except => (SetOpType::Except, false),
            SetOperationType::ExceptAll => (SetOpType::Except, true),
        };

        let setop = SetOperation::new(
            node.table_index,
            self.wrap_plan(left_plan),
            self.wrap_plan(right_plan),
            setop_type,
            setop_all,
            node.types,
        );

        Ok(LogicalOperator::SetOperation(setop))
    }

    fn plan_cte_definition(
        &mut self,
        bound_cte: &crate::binder::ir::CTE,
    ) -> Result<LogicalOperator> {
        if let Some(recursive) = &bound_cte.recursive {
            let anchor = self.plan_query(recursive.anchor.clone())?;
            let recursive_plan = self.plan_query(recursive.recursive.clone())?;

            let anchor_types = anchor.types();
            let recursive_types = recursive_plan.types();
            let anchor = self.cast_to_types(anchor, &anchor_types, &bound_cte.types)?;
            let recursive_plan =
                self.cast_to_types(recursive_plan, &recursive_types, &bound_cte.types)?;

            return Ok(LogicalOperator::RecursiveCTE(RecursiveCTE {
                cte_index: bound_cte.cte_index,
                cte_name: bound_cte.name.clone(),
                column_names: bound_cte.names.clone(),
                column_types: bound_cte.types.clone(),
                union_all: recursive.union_all,
                anchor: Box::new(self.wrap_plan(anchor)),
                recursive: Box::new(self.wrap_plan(recursive_plan)),
            }));
        }

        self.plan_query(bound_cte.query.clone())
    }

    fn cast_to_types(
        &mut self,
        op: LogicalOperator,
        source_types: &[LogicalType],
        target_types: &[LogicalType],
    ) -> Result<LogicalOperator> {
        debug_assert_eq!(source_types.len(), target_types.len());

        if source_types == target_types {
            return Ok(op);
        }

        let child_bindings = op.get_column_bindings();
        let projection_index = self.bind_context.generate_table_index();

        let mut select_list = Vec::with_capacity(target_types.len());
        for (i, (source_type, target_type)) in
            source_types.iter().zip(target_types.iter()).enumerate()
        {
            let binding = &child_bindings[i];
            let col_ref =
                Expression::ColumnRef(ColumnRefExpression::new(*binding, source_type.clone()));

            if source_type != target_type {
                let cast_expr = CastExpression::add_explicit_cast(
                    col_ref,
                    target_type.clone(),
                    &self.cast_functions,
                    false,
                )?;
                select_list.push(cast_expr);
            } else {
                select_list.push(col_ref);
            }
        }

        let projection = Projection::new(projection_index, self.wrap_plan(op), select_list);
        Ok(LogicalOperator::Projection(projection))
    }
}
