// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::deep_copy_plan;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;

use std::ops::ControlFlow;

pub struct CTEInlining<'a> {
    bind_context: &'a BindContext,
}

impl<'a> CTEInlining<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.rewrite_plan(plan)
    }

    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan
            .try_map_children(|child| Ok(self.rewrite_plan(child)))
            .expect("CTE inlining child rewrite cannot fail");
        plan.map_operator(|operator| self.try_inline(operator))
    }

    fn try_inline(&mut self, op: LogicalOperator) -> LogicalOperator {
        let LogicalOperator::MaterializedCTE(mut cte) = op else {
            return op;
        };

        let ref_count = count_cte_references(&cte.child.operator, cte.cte_index);
        if ref_count == 0 {
            return cte.child.operator;
        }

        if cte.materialized == CTEMaterialize::Materialized {
            return LogicalOperator::MaterializedCTE(cte);
        }

        if ref_count == 1 {
            let mut definition = Some(*cte.cte_query);
            inline_single_reference(&mut cte.child.operator, cte.cte_index, &mut definition);
            return cte.child.operator;
        }

        if cte.materialized == CTEMaterialize::NotMaterialized
            || (cte.materialized == CTEMaterialize::Default
                && contains_limit(&cte.child.operator)
                && !ends_in_aggregate_or_distinct(&cte.cte_query.operator))
        {
            let definition = cte.cte_query.as_ref();
            inline_copied_references(
                self.bind_context,
                &mut cte.child.operator,
                cte.cte_index,
                definition,
            );
            return cte.child.operator;
        }

        LogicalOperator::MaterializedCTE(cte)
    }
}

fn count_cte_references(op: &LogicalOperator, cte_index: usize) -> usize {
    let self_count = match op {
        LogicalOperator::CTERef(cte_ref) if cte_ref.cte_index == cte_index => 1,
        _ => 0,
    };
    self_count
        + op.children()
            .into_iter()
            .map(|child| count_cte_references(&child.operator, cte_index))
            .sum::<usize>()
}

fn contains_limit(op: &LogicalOperator) -> bool {
    if matches!(op, LogicalOperator::Limit(_) | LogicalOperator::TopN(_)) {
        return true;
    }
    op.children()
        .into_iter()
        .any(|c| contains_limit(&c.operator))
}

fn ends_in_aggregate_or_distinct(op: &LogicalOperator) -> bool {
    if matches!(
        op,
        LogicalOperator::Aggregate(_) | LogicalOperator::Distinct(_) | LogicalOperator::Window(_)
    ) {
        return true;
    }
    let children = op.children();
    if children.len() != 1 {
        return false;
    }
    ends_in_aggregate_or_distinct(&children[0].operator)
}

fn projection_for_cte_ref(table_index: usize, definition: LogicalPlan) -> LogicalOperator {
    let bindings = definition.get_column_bindings();
    let types = definition.types();
    let expressions = bindings
        .into_iter()
        .zip(types)
        .map(|(binding, ty)| Expression::ColumnRef(ColumnRefExpression::new(binding, ty)))
        .collect();
    LogicalOperator::Projection(Projection::new(table_index, definition, expressions))
}

fn inline_single_reference(
    op: &mut LogicalOperator,
    cte_index: usize,
    definition: &mut Option<LogicalPlan>,
) -> bool {
    if let LogicalOperator::CTERef(cte_ref) = op {
        if cte_ref.cte_index == cte_index {
            let replacement = projection_for_cte_ref(
                cte_ref.table_index,
                definition
                    .take()
                    .expect("single-reference CTE inlining must have a definition"),
            );
            *op = replacement;
            return true;
        }
    }

    let mut replaced = false;
    let _ = op.visit_children_mut(|child| {
        if inline_single_reference(&mut child.operator, cte_index, definition) {
            replaced = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    replaced
}

fn inline_copied_references(
    bind_context: &BindContext,
    op: &mut LogicalOperator,
    cte_index: usize,
    definition: &LogicalPlan,
) -> usize {
    if let LogicalOperator::CTERef(cte_ref) = op {
        if cte_ref.cte_index == cte_index {
            let copied = deep_copy_plan(definition, bind_context.shared().as_ref());
            *op = projection_for_cte_ref(cte_ref.table_index, copied);
            return 1;
        }
    }

    let mut replaced = 0usize;
    let _ = op.visit_children_mut(|child| {
        replaced +=
            inline_copied_references(bind_context, &mut child.operator, cte_index, definition);
        ControlFlow::Continue(())
    });
    replaced
}

#[cfg(test)]
mod tests {
    use super::CTEInlining;
    use crate::verify::verify_logical_plan;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::CTEMaterialize;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression, Expression};
    use paro_planner::operator::{
        CTERef, CrossProduct, ExpressionGet, Filter, Join, LogicalOperator, MaterializedCTE,
        Projection,
    };
    use paro_planner::plan::LogicalPlan;

    fn values(ctx: &BindContext, table_index: usize, vals: &[i32]) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vals.iter()
                    .map(|v| {
                        vec![Expression::Constant(ConstantExpression {
                            value: paro_common::runtime_value::Value::Integer(*v),
                            return_type: LogicalType::Integer,
                        })]
                    })
                    .collect(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn cte_ref(ctx: &BindContext, cte_index: usize, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::CTERef(CTERef::new(
                cte_index,
                table_index,
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    #[test]
    fn inline_single_reference_removes_materialized_cte() {
        let bind_context = BindContext::new();
        let plan = LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            10,
            "nums".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            values(&bind_context, 1, &[1, 2, 3]),
            LogicalPlan::new(
                &bind_context,
                LogicalOperator::Filter(Filter::new(
                    cte_ref(&bind_context, 10, 2),
                    vec![Expression::ColumnRef(ColumnRefExpression::new(
                        paro_planner::operator::ColumnBinding::new(2, 0),
                        LogicalType::Integer,
                    ))],
                )),
            ),
        ));

        let optimized = CTEInlining::new(&bind_context).optimize_plan(LogicalPlan::synthetic(plan));
        verify_logical_plan(&bind_context, &optimized).expect("plan should verify after inlining");
        assert!(!matches!(
            optimized.operator,
            LogicalOperator::MaterializedCTE(_)
        ));
    }

    #[test]
    fn not_materialized_multi_ref_uses_deep_copy() {
        let bind_context = BindContext::new();
        let plan = LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            10,
            "nums".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::NotMaterialized,
            LogicalPlan::new(
                &bind_context,
                LogicalOperator::Projection(Projection::new(
                    3,
                    values(&bind_context, 1, &[1, 2, 3]),
                    vec![Expression::ColumnRef(ColumnRefExpression::new(
                        paro_planner::operator::ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    ))],
                )),
            ),
            LogicalPlan::new(
                &bind_context,
                LogicalOperator::Join(Join::Cross(CrossProduct {
                    left: Box::new(cte_ref(&bind_context, 10, 4)),
                    right: Box::new(cte_ref(&bind_context, 10, 5)),
                })),
            ),
        ));

        let optimized = CTEInlining::new(&bind_context).optimize_plan(LogicalPlan::synthetic(plan));
        verify_logical_plan(&bind_context, &optimized)
            .expect("plan should verify after multi-inline");
        assert!(!matches!(
            optimized.operator,
            LogicalOperator::MaterializedCTE(_)
        ));
    }
}
