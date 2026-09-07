// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::ComparisonType;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType, Expression};
use paro_planner::operator::Filter as PlannerFilter;
use paro_planner::operator::{ColumnBinding, LogicalOperator};
use paro_planner::plan::OwnedLogicalPlan;
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

    pub fn optimize_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        self.optimize_plan_with_change(plan).0
    }

    pub fn optimize_plan_with_change(&mut self, mut plan: OwnedLogicalPlan) -> (OwnedLogicalPlan, bool) {
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
        mut plan: OwnedLogicalPlan,
    ) -> (OwnedLogicalPlan, bool) {
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
        let mut producer_filters = vec![or_expr];
        if let Some(domain) = build_common_equality_domain(info, &new_bindings) {
            producer_filters.push(domain);
        }
        if cte.materialized == CTEMaterialize::Default {
            cte.materialized = CTEMaterialize::Materialized;
        }

        let id = cte.cte_query.id;
        let stats = cte.cte_query.stats.clone();
        let cte_query_plan = std::mem::replace(
            &mut *cte.cte_query,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let pushed_plan = FilterPushdown::new().rewrite_plan(OwnedLogicalPlan::synthetic(
            LogicalOperator::Filter(PlannerFilter::new(cte_query_plan, producer_filters)),
        ));
        *cte.cte_query = OwnedLogicalPlan {
            id,
            stats,
            operator: pushed_plan.into_operator(),
        };
        true
    }
}

/// Derive a producer-side necessary condition from equality domains present
/// on every consumer. For `OR(ref_1, ..., ref_n)`, weakening each disjunct to
/// its common-key equalities yields a safe superset filter while allowing the
/// condition to cross aggregates and set-operation projections.
fn build_common_equality_domain(
    info: &MaterializedCTEInfo,
    new_bindings: &[ColumnBinding],
) -> Option<Expression> {
    let mut per_ref = Vec::with_capacity(info.filtered_refs.len());
    for reference in &info.filtered_refs {
        let ordinal_by_binding = reference
            .old_bindings
            .iter()
            .copied()
            .enumerate()
            .map(|(ordinal, binding)| (binding, ordinal))
            .collect::<HashMap<_, _>>();
        let mut equalities = HashMap::new();
        for filter in &reference.filters {
            let Expression::Comparison(comparison) = filter else {
                continue;
            };
            if comparison.comparison_type != ComparisonType::Equal {
                continue;
            }
            let binding = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(_))
                | (Expression::Constant(_), Expression::ColumnRef(column))
                    if column.depth == 0 =>
                {
                    column.binding
                }
                _ => continue,
            };
            let Some(ordinal) = ordinal_by_binding.get(&binding).copied() else {
                continue;
            };
            equalities.entry(ordinal).or_insert_with(|| filter.clone());
        }
        per_ref.push((reference, equalities));
    }
    let first = per_ref.first()?;
    let mut common_ordinals = first
        .1
        .keys()
        .copied()
        .filter(|ordinal| per_ref.iter().all(|(_, map)| map.contains_key(ordinal)))
        .collect::<Vec<_>>();
    common_ordinals.sort_unstable();
    if common_ordinals.is_empty() {
        return None;
    }

    let mut disjuncts = Vec::with_capacity(per_ref.len());
    for (reference, equalities) in per_ref {
        let mut replacer = ColumnBindingReplacer::new();
        for (old_binding, new_binding) in reference.old_bindings.iter().zip(new_bindings.iter()) {
            replacer
                .replacement_bindings
                .push(ReplacementBinding::new(*old_binding, *new_binding));
        }
        let mut conjuncts = Vec::with_capacity(common_ordinals.len());
        for ordinal in &common_ordinals {
            let mut equality = equalities
                .get(ordinal)
                .expect("common consumer domain vanished")
                .clone();
            replacer.visit_expression(&mut equality);
            conjuncts.push(equality);
        }
        disjuncts.push(conjunction(ConjunctionType::And, conjuncts));
    }
    Some(conjunction(ConjunctionType::Or, disjuncts))
}

fn conjunction(kind: ConjunctionType, mut expressions: Vec<Expression>) -> Expression {
    if expressions.len() == 1 {
        expressions.pop().expect("one expression")
    } else {
        Expression::Conjunction(ConjunctionExpression::new(kind, expressions))
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
    use super::{
        build_common_equality_domain, build_or_filter, CTEFilterPusher, FilteredCTERef,
        MaterializedCTEInfo,
    };
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::CTEMaterialize;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionType,
        ConstantExpression, Expression, FunctionExpression,
    };
    use paro_planner::operator::{CTERef, ExpressionGet, Filter, LogicalOperator, MaterializedCTE};
    use paro_planner::plan::OwnedLogicalPlan;

    fn values(ctx: &BindContext, table_index: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
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

    fn integer_equality(table_index: usize, column_index: usize, value: i32) -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(
                paro_planner::operator::ColumnBinding::new(table_index, column_index),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression {
                value: paro_common::runtime_value::Value::Integer(value),
                return_type: LogicalType::Integer,
            }),
        ))
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
            OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::Filter(Filter::new(
                    OwnedLogicalPlan::new(&bind_context, cte_ref(2)),
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
            CTEFilterPusher::new().optimize_default_root_with_change(OwnedLogicalPlan::synthetic(plan));
        assert!(changed);
        let (optimized, changed_again) =
            CTEFilterPusher::new().optimize_default_root_with_change(optimized);
        assert!(!changed_again);
        match &optimized.operator {
            LogicalOperator::MaterializedCTE(cte) => {
                assert_eq!(cte.materialized, CTEMaterialize::Materialized);
                assert!(!matches!(
                    &cte.cte_query.operator,
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

    #[test]
    fn common_equality_domain_keeps_only_ordinals_constrained_by_every_consumer() {
        let info = MaterializedCTEInfo {
            all_refs_are_filtered: true,
            filtered_refs: vec![
                FilteredCTERef {
                    old_bindings: vec![
                        paro_planner::operator::ColumnBinding::new(2, 0),
                        paro_planner::operator::ColumnBinding::new(2, 1),
                    ],
                    filters: vec![integer_equality(2, 0, 2001), integer_equality(2, 1, 1)],
                },
                FilteredCTERef {
                    old_bindings: vec![
                        paro_planner::operator::ColumnBinding::new(3, 0),
                        paro_planner::operator::ColumnBinding::new(3, 1),
                    ],
                    filters: vec![integer_equality(3, 0, 2002)],
                },
            ],
        };
        let producer_bindings = [
            paro_planner::operator::ColumnBinding::new(10, 0),
            paro_planner::operator::ColumnBinding::new(10, 1),
        ];

        let domain = build_common_equality_domain(&info, &producer_bindings)
            .expect("the first ordinal is constrained by every consumer");
        let Expression::Conjunction(disjunction) = domain else {
            panic!("two consumers must produce a disjunction");
        };
        assert_eq!(disjunction.conjunction_type, ConjunctionType::Or);
        assert_eq!(disjunction.children.len(), 2);
        for (predicate, expected_value) in disjunction.children.iter().zip([2001, 2002]) {
            let Expression::Comparison(comparison) = predicate else {
                panic!("one shared ordinal must produce one equality per consumer");
            };
            assert_eq!(comparison.comparison_type, ComparisonType::Equal);
            let Expression::ColumnRef(column) = comparison.left.as_ref() else {
                panic!("normalized equality must retain its column on the left");
            };
            assert_eq!(column.binding, producer_bindings[0]);
            let Expression::Constant(constant) = comparison.right.as_ref() else {
                panic!("normalized equality must retain its literal on the right");
            };
            assert_eq!(
                constant.value,
                paro_common::runtime_value::Value::Integer(expected_value)
            );
        }
    }
}
