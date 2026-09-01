// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use paro_planner::binder::ir::CTEMaterialize;
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

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_plan_with_change(plan).0
    }

    pub fn optimize_plan_with_change(&mut self, mut plan: LogicalPlan) -> (LogicalPlan, bool) {
        let mut infos = HashMap::new();
        self.find_candidates(&plan.operator, &mut infos);
        let changed = self.push_filters(&mut plan.operator, &infos);
        (plan, changed)
    }

    /// Build the shared-materialization alternative for the root DEFAULT CTE.
    ///
    /// Memo invokes this rule for one equivalence group at a time. Restricting
    /// the mutation to that group's root prevents nested CTE choices from
    /// being multiplied into the ancestor expression, and changing DEFAULT
    /// to MATERIALIZED makes the transformation structurally idempotent.
    pub(crate) fn optimize_default_root_with_change(
        &mut self,
        mut plan: LogicalPlan,
    ) -> (LogicalPlan, bool) {
        let is_default_root = matches!(
            &plan.operator,
            LogicalOperator::MaterializedCTE(cte)
                if cte.materialized == CTEMaterialize::Default
        );
        if !is_default_root {
            return (plan, false);
        }
        let mut infos = HashMap::new();
        self.find_candidates(&plan.operator, &mut infos);
        let changed = match &mut plan.operator {
            LogicalOperator::MaterializedCTE(cte) => self.push_current_cte(cte, &infos),
            _ => false,
        };
        (plan, changed)
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

    fn push_filters(
        &self,
        op: &mut LogicalOperator,
        infos: &HashMap<usize, MaterializedCTEInfo>,
    ) -> bool {
        match op {
            LogicalOperator::MaterializedCTE(cte) => {
                let mut changed = self.push_filters(&mut cte.cte_query.operator, infos);
                changed |= self.push_filters(&mut cte.child.operator, infos);
                changed | self.push_current_cte(cte, infos)
            }
            _ => {
                let mut changed = false;
                let _ = op.visit_children_mut(|child| {
                    changed |= self.push_filters(&mut child.operator, infos);
                    ControlFlow::Continue(())
                });
                changed
            }
        }
    }

    fn push_current_cte(
        &self,
        cte: &mut paro_planner::operator::MaterializedCTE,
        infos: &HashMap<usize, MaterializedCTEInfo>,
    ) -> bool {
        // Producer-side filtering belongs exclusively to the shared
        // materialization alternative. Committing a DEFAULT CTE to this
        // branch prevents a later inlining transformation from combining the
        // copied producer predicate with the original consumer predicate.
        // NOT MATERIALIZED is a semantic contract and must never enter this
        // branch.
        if cte.materialized == CTEMaterialize::NotMaterialized {
            return false;
        }
        let Some(info) = infos.get(&cte.cte_index) else {
            return false;
        };
        if !info.all_refs_are_filtered || info.filtered_refs.is_empty() {
            return false;
        }

        let new_bindings = cte.cte_query.get_column_bindings();
        let Some(or_expr) = build_or_filter(info, &new_bindings) else {
            return false;
        };
        if cte.materialized == CTEMaterialize::Default {
            cte.materialized = CTEMaterialize::Materialized;
        }

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
        true
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
        let old_bindings = filtered_ref
            .old_bindings
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut referenced = Vec::new();
        for filter in &filtered_ref.filters {
            crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                filter,
                &mut referenced,
            );
        }
        if referenced
            .iter()
            .any(|binding| !old_bindings.contains(binding))
        {
            return None;
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
        let new_bindings = new_bindings.iter().copied().collect::<HashSet<_>>();
        let mut rewritten_references = Vec::new();
        for filter in &rewritten_filters {
            crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                filter,
                &mut rewritten_references,
            );
        }
        if rewritten_references
            .iter()
            .any(|binding| !new_bindings.contains(binding))
        {
            return None;
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

        let (optimized, changed) =
            CTEFilterPusher::new().optimize_default_root_with_change(LogicalPlan::synthetic(plan));
        assert!(changed);
        let (optimized, changed_again) =
            CTEFilterPusher::new().optimize_default_root_with_change(optimized);
        assert!(!changed_again);
        match optimized.operator {
            LogicalOperator::MaterializedCTE(cte) => {
                assert_eq!(cte.materialized, CTEMaterialize::Materialized);
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
