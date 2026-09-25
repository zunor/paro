// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::deep_copy_plan_preserving_statistics;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::logical::operator::{LogicalOperator, Projection};
use paro_planner::logical::plan::OwnedLogicalPlan;

use std::ops::ControlFlow;

pub struct CTEInlining<'a> {
    bind_context: &'a BindContext,
}

impl<'a> CTEInlining<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        self.optimize_plan_with_change(plan).0
    }

    pub fn optimize_plan_with_change(
        &mut self,
        plan: OwnedLogicalPlan,
    ) -> (OwnedLogicalPlan, bool) {
        self.rewrite_plan(plan)
    }

    fn rewrite_plan(&mut self, plan: OwnedLogicalPlan) -> (OwnedLogicalPlan, bool) {
        plan.try_fold_post_order(|plan, child_changes| {
            let child_changed = child_changes.into_iter().any(|changed| changed);
            let (id, stats, operator) = plan.into_parts();
            let (operator, local_changed) = self.try_inline(operator);
            Ok((
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator,
                },
                child_changed || local_changed,
            ))
        })
        .expect("CTE inlining traversal cannot fail")
    }

    fn try_inline(&mut self, op: LogicalOperator) -> (LogicalOperator, bool) {
        let LogicalOperator::MaterializedCTE(mut cte) = op else {
            return (op, false);
        };

        let ref_count = count_cte_references(&cte.child.operator, cte.cte_index);
        if ref_count == 0 {
            return ((*cte.child).into_operator(), true);
        }

        if cte.materialized == CTEMaterialize::Materialized {
            return (LogicalOperator::MaterializedCTE(cte), false);
        }

        if ref_count == 1 {
            let mut definition = Some(*cte.cte_query);
            inline_single_reference(&mut cte.child.operator, cte.cte_index, &mut definition);
            return ((*cte.child).into_operator(), true);
        }

        if cte.materialized == CTEMaterialize::NotMaterialized {
            let definition = cte.cte_query.as_ref();
            inline_copied_references(
                self.bind_context,
                &mut cte.child.operator,
                cte.cte_index,
                definition,
            );
            return ((*cte.child).into_operator(), true);
        }

        (LogicalOperator::MaterializedCTE(cte), false)
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

fn projection_for_cte_ref(
    table_index: usize,
    relation_alias: String,
    column_names: Vec<String>,
    mut definition: OwnedLogicalPlan,
) -> LogicalOperator {
    // A direct VALUES definition remains the physical producer after filter
    // pushdown crosses this reference boundary. Attach the CTE reference's
    // identity to that producer so it cannot fall back to generated colN
    // names when the wrapper projection moves above the filter.
    if let LogicalOperator::ExpressionGet(values) = &mut definition.operator {
        values.names.clone_from(&column_names);
        values.relation_alias = Some(relation_alias.clone());
    }
    let bindings = definition.get_column_bindings();
    let types = definition.types();
    let expressions = bindings
        .into_iter()
        .zip(types)
        .map(|(binding, ty)| Expression::ColumnRef(ColumnRefExpression::new(binding, ty).into()))
        .collect();
    LogicalOperator::Projection(
        Projection::new(table_index, definition, expressions)
            .with_visible_names(column_names)
            .with_visible_qualifier(relation_alias),
    )
}

fn inline_single_reference(
    op: &mut LogicalOperator,
    cte_index: usize,
    definition: &mut Option<OwnedLogicalPlan>,
) -> bool {
    if let LogicalOperator::CTERef(cte_ref) = op {
        if cte_ref.cte_index == cte_index {
            let replacement = projection_for_cte_ref(
                cte_ref.table_index,
                cte_ref.relation_alias.clone(),
                cte_ref.column_names.clone(),
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
    definition: &OwnedLogicalPlan,
) -> usize {
    if let LogicalOperator::CTERef(cte_ref) = op {
        if cte_ref.cte_index == cte_index {
            let copied =
                deep_copy_plan_preserving_statistics(definition, bind_context.shared().as_ref());
            *op = projection_for_cte_ref(
                cte_ref.table_index,
                cte_ref.relation_alias.clone(),
                cte_ref.column_names.clone(),
                copied,
            );
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
    use paro_planner::logical::operator::{
        CTERef, CrossProduct, ExpressionGet, Filter, Join, LogicalOperator, MaterializedCTE,
        Projection,
    };
    use paro_planner::logical::plan::OwnedLogicalPlan;

    fn values(ctx: &BindContext, table_index: usize, vals: &[i32]) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vals.iter()
                    .map(|v| {
                        vec![Expression::Constant(
                            ConstantExpression {
                                value: paro_common::runtime_value::Value::Integer(*v),
                                return_type: LogicalType::Integer,
                            }
                            .into(),
                        )]
                    })
                    .collect(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn cte_ref(ctx: &BindContext, cte_index: usize, table_index: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            ctx,
            LogicalOperator::CTERef(CTERef::new(
                cte_index,
                table_index,
                "cte".to_string(),
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
            OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::Filter(Filter::new(
                    cte_ref(&bind_context, 10, 2),
                    vec![Expression::ColumnRef(
                        ColumnRefExpression::new(
                            paro_planner::logical::operator::ColumnBinding::new(2, 0),
                            LogicalType::Integer,
                        )
                        .into(),
                    )],
                )),
            ),
        ));

        let optimized =
            CTEInlining::new(&bind_context).optimize_plan(OwnedLogicalPlan::synthetic(plan));
        verify_logical_plan(&bind_context, &optimized).expect("plan should verify after inlining");
        assert_eq!(optimized.operator.output_names(), ["v"]);
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
            OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::Projection(Projection::new(
                    3,
                    values(&bind_context, 1, &[1, 2, 3]),
                    vec![Expression::ColumnRef(
                        ColumnRefExpression::new(
                            paro_planner::logical::operator::ColumnBinding::new(1, 0),
                            LogicalType::Integer,
                        )
                        .into(),
                    )],
                )),
            ),
            OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::Join(Join::Cross(CrossProduct {
                    left: Box::new(cte_ref(&bind_context, 10, 4)),
                    right: Box::new(cte_ref(&bind_context, 10, 5)),
                    build_side_constraint: Default::default(),
                })),
            ),
        ));

        let optimized =
            CTEInlining::new(&bind_context).optimize_plan(OwnedLogicalPlan::synthetic(plan));
        verify_logical_plan(&bind_context, &optimized)
            .expect("plan should verify after multi-inline");
        assert!(!matches!(
            optimized.operator,
            LogicalOperator::MaterializedCTE(_)
        ));
    }

    #[test]
    fn canonical_policy_inlines_single_reference_and_preserves_multi_ref_sharing() {
        let bind_context = BindContext::new();
        let single = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                9,
                "single".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                values(&bind_context, 0, &[1, 2, 3]),
                cte_ref(&bind_context, 9, 3),
            )),
        );
        let single = CTEInlining::new(&bind_context).optimize_plan(single);
        assert!(!matches!(
            single.operator,
            LogicalOperator::MaterializedCTE(_)
        ));

        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                10,
                "nums".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                values(&bind_context, 1, &[1, 2, 3]),
                OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: Box::new(cte_ref(&bind_context, 10, 4)),
                        right: Box::new(cte_ref(&bind_context, 10, 5)),
                        build_side_constraint: Default::default(),
                    })),
                ),
            )),
        );

        let optimized = CTEInlining::new(&bind_context).optimize_plan(plan);
        verify_logical_plan(&bind_context, &optimized).expect("shared plan should remain valid");
        assert!(matches!(
            optimized.operator,
            LogicalOperator::MaterializedCTE(_)
        ));
    }

    #[test]
    fn cte_root_choice_preserves_an_independent_nested_owner() {
        let bind_context = BindContext::new();
        let nested = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                20,
                "nested".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                cte_ref(&bind_context, 10, 2),
                OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: Box::new(cte_ref(&bind_context, 20, 3)),
                        right: Box::new(cte_ref(&bind_context, 20, 4)),
                        build_side_constraint: Default::default(),
                    })),
                ),
            )),
        );
        let outer = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                10,
                "outer".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                values(&bind_context, 1, &[1, 2, 3]),
                nested,
            )),
        );

        let (optimized, changed) = CTEInlining::new(&bind_context).optimize_plan_with_change(outer);

        assert!(changed);
        let LogicalOperator::MaterializedCTE(nested) = &optimized.operator else {
            panic!("nested owner must retain its materialization boundary")
        };
        assert_eq!(nested.cte_index, 20);
        assert!(!matches!(
            nested.cte_query.operator,
            LogicalOperator::CTERef(_)
        ));
        verify_logical_plan(&bind_context, &optimized)
            .expect("local CTE alternative should verify");
    }
}
