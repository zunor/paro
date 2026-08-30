// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::ops::ControlFlow;

use paro_planner::expression::{ConjunctionExpression, ConjunctionType, Expression};
use paro_planner::operator::Filter as PlannerFilter;
use paro_planner::operator::{ColumnBinding, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::expression::binding_replacer::{ColumnBindingReplacer, ReplacementBinding};
use crate::filter::pushdown::FilterPushdown;

#[derive(Debug, Clone)]
struct FilteredCTERef {
    old_bindings: Vec<ColumnBinding>,
    filters: Vec<Expression>,
}

#[derive(Debug, Clone)]
struct MaterializedCTEInfo {
    all_refs_are_filtered: bool,
    filtered_refs: Vec<FilteredCTERef>,
}

impl Default for MaterializedCTEInfo {
    fn default() -> Self {
        Self {
            all_refs_are_filtered: true,
            filtered_refs: Vec::new(),
        }
    }
}

pub struct CTEFilterPusher;

impl CTEFilterPusher {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize_plan(&mut self, mut plan: LogicalPlan) -> LogicalPlan {
        let mut infos = HashMap::new();
        self.find_candidates(&plan.operator, &mut infos);
        self.push_filters(&mut plan.operator, &infos);
        plan
    }

    fn find_candidates(
        &self,
        op: &LogicalOperator,
        infos: &mut HashMap<usize, MaterializedCTEInfo>,
    ) {
        match op {
            LogicalOperator::MaterializedCTE(cte) => {
                infos.entry(cte.cte_index).or_default();
                self.find_candidates(&cte.cte_query.operator, infos);
                self.find_candidates(&cte.child.operator, infos);
            }
            LogicalOperator::Filter(filter) => {
                if let LogicalOperator::CTERef(ref cte_ref) = filter.child.operator {
                    infos
                        .entry(cte_ref.cte_index)
                        .or_default()
                        .filtered_refs
                        .push(FilteredCTERef {
                            old_bindings: filter.child.get_column_bindings(),
                            filters: filter.expressions.clone(),
                        });
                    return;
                }
                self.find_candidates(&filter.child.operator, infos);
            }
            LogicalOperator::CTERef(cte_ref) => {
                infos
                    .entry(cte_ref.cte_index)
                    .or_default()
                    .all_refs_are_filtered = false;
            }
            _ => {
                for child in op.children() {
                    self.find_candidates(&child.operator, infos);
                }
            }
        }
    }

    fn push_filters(&self, op: &mut LogicalOperator, infos: &HashMap<usize, MaterializedCTEInfo>) {
        match op {
            LogicalOperator::MaterializedCTE(cte) => {
                self.push_filters(&mut cte.cte_query.operator, infos);
                self.push_filters(&mut cte.child.operator, infos);

                let Some(info) = infos.get(&cte.cte_index) else {
                    return;
                };
                if !info.all_refs_are_filtered || info.filtered_refs.is_empty() {
                    return;
                }

                let new_bindings = cte.cte_query.get_column_bindings();
                let Some(or_expr) = build_or_filter(info, &new_bindings) else {
                    return;
                };

                let id = cte.cte_query.id;
                let stats = cte.cte_query.stats.clone();
                let cte_query_plan = std::mem::replace(
                    &mut *cte.cte_query,
                    LogicalPlan::synthetic(LogicalOperator::DummyScan),
                );
                let pushed_plan = FilterPushdown::new().rewrite_plan(LogicalPlan::synthetic(
                    LogicalOperator::Filter(PlannerFilter::new(cte_query_plan, vec![or_expr])),
                ));
                *cte.cte_query = LogicalPlan {
                    id,
                    stats,
                    operator: pushed_plan.operator,
                };
            }
            _ => {
                let _ = op.visit_children_mut(|child| {
                    self.push_filters(&mut child.operator, infos);
                    ControlFlow::Continue(())
                });
            }
        }
    }
}

fn build_or_filter(
    info: &MaterializedCTEInfo,
    new_bindings: &[ColumnBinding],
) -> Option<Expression> {
    let mut refs = Vec::new();

    for filtered_ref in &info.filtered_refs {
        if filtered_ref
            .filters
            .iter()
            .any(|filter| filter.evaluation_properties().is_reorder_fence())
        {
            return None;
        }
        if filtered_ref.old_bindings.len() != new_bindings.len() {
            continue;
        }

        let mut replacer = ColumnBindingReplacer::new();
        for (old_binding, new_binding) in filtered_ref.old_bindings.iter().zip(new_bindings.iter())
        {
            replacer
                .replacement_bindings
                .push(ReplacementBinding::new(*old_binding, *new_binding));
        }

        let mut rewritten_filters = filtered_ref.filters.clone();
        for filter in &mut rewritten_filters {
            replacer.visit_expression(filter);
        }

        let and_expr = if rewritten_filters.len() == 1 {
            rewritten_filters.pop().unwrap()
        } else {
            Expression::Conjunction(ConjunctionExpression::new(
                ConjunctionType::And,
                rewritten_filters,
            ))
        };
        refs.push(and_expr);
    }

    match refs.len() {
        0 => None,
        1 => refs.pop(),
        _ => Some(Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            refs,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_or_filter, CTEFilterPusher, FilteredCTERef, MaterializedCTEInfo};
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::CTEMaterialize;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        FunctionExpression,
    };
    use paro_planner::operator::{CTERef, ExpressionGet, Filter, LogicalOperator, MaterializedCTE};
    use paro_planner::plan::LogicalPlan;

    fn values(ctx: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![
                    vec![Expression::Constant(ConstantExpression {
                        value: paro_common::runtime_value::Value::Integer(1),
                        return_type: LogicalType::Integer,
                    })],
                    vec![Expression::Constant(ConstantExpression {
                        value: paro_common::runtime_value::Value::Integer(2),
                        return_type: LogicalType::Integer,
                    })],
                ],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    #[test]
    fn pushes_or_filter_into_materialized_cte_when_all_refs_are_filtered() {
        let cte_ref = |table_index| {
            LogicalOperator::CTERef(CTERef::new(
                10,
                table_index,
                "nums".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            ))
        };

        let bind_context = BindContext::new();
        let plan = LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            10,
            "nums".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            values(&bind_context, 1),
            LogicalPlan::new(
                &bind_context,
                LogicalOperator::Filter(Filter::new(
                    LogicalPlan::new(&bind_context, cte_ref(2)),
                    vec![Expression::Comparison(ComparisonExpression::new(
                        ComparisonType::GreaterThan,
                        Expression::ColumnRef(ColumnRefExpression::new(
                            paro_planner::operator::ColumnBinding::new(2, 0),
                            LogicalType::Integer,
                        )),
                        Expression::Constant(ConstantExpression {
                            value: paro_common::runtime_value::Value::Integer(1),
                            return_type: LogicalType::Integer,
                        }),
                    ))],
                )),
            ),
        ));

        let optimized = CTEFilterPusher::new().optimize_plan(LogicalPlan::synthetic(plan));
        match optimized.operator {
            LogicalOperator::MaterializedCTE(cte) => {
                assert!(!matches!(
                    cte.cte_query.operator,
                    LogicalOperator::ExpressionGet(_)
                ));
            }
            other => panic!("expected materialized cte, got {other:?}"),
        }
    }

    #[test]
    fn does_not_copy_volatile_filters_into_cte_producer() {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        let random = || {
            Expression::Function(FunctionExpression::new(
                function.clone(),
                vec![],
                LogicalType::Double,
            ))
        };
        let info = MaterializedCTEInfo {
            all_refs_are_filtered: true,
            filtered_refs: vec![FilteredCTERef {
                old_bindings: vec![],
                filters: vec![Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    random(),
                    random(),
                ))],
            }],
        };

        assert!(build_or_filter(&info, &[]).is_none());
    }
}
