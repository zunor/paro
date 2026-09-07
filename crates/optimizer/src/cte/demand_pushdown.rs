// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Push the union of shared-CTE consumer key domains into its producer.
//!
//! A materialized relation referenced only through inner equality joins does
//! not need rows outside the union of those join-key domains. The rewrite
//! duplicates only immutable demand sides, unions their projected keys, and
//! inserts a semi join at the narrowest producer subtree that owns the grouped
//! key. It is an optional Memo alternative: the ordinary materialization stays
//! available when duplicated demand work costs more than the rows it removes.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::deep_copy_operator;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::Expression;
use paro_planner::operator::{
    ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator, MaterializedCTE,
};
use paro_planner::plan::OwnedLogicalPlan;

#[path = "demand_pushdown/producer.rs"]
mod producer;

use producer::{build_demand_relation, push_group_demand};

#[cfg(test)]
#[path = "demand_pushdown/tests.rs"]
mod tests;

#[derive(Debug)]
struct JoinedDemand {
    key_ordinals: Vec<usize>,
    key_types: Vec<LogicalType>,
    plan: OwnedLogicalPlan,
    expressions: Vec<Expression>,
}

#[derive(Debug)]
struct MaterializedDemandInfo {
    all_refs_are_join_constrained: bool,
    joined_refs: Vec<JoinedDemand>,
}

impl Default for MaterializedDemandInfo {
    fn default() -> Self {
        Self {
            all_refs_are_join_constrained: true,
            joined_refs: Vec::new(),
        }
    }
}

pub struct CTEDemandPusher<'a> {
    bind_context: &'a BindContext,
}

impl<'a> CTEDemandPusher<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    /// Build a demand-restricted shared-materialization alternative for one
    /// DEFAULT CTE Memo group. Restricting the operation to DEFAULT also makes
    /// it structurally idempotent: the output is explicitly MATERIALIZED and
    /// cannot fire this rule again.
    pub(crate) fn optimize_default_root_with_change(
        &self,
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
            LogicalOperator::MaterializedCTE(cte) => self.push_current_cte(cte, &mut infos),
            _ => false,
        };
        (plan, changed)
    }

    fn find_candidates(
        &self,
        operator: &LogicalOperator,
        infos: &mut HashMap<usize, MaterializedDemandInfo>,
    ) {
        match operator {
            LogicalOperator::MaterializedCTE(cte) => {
                infos.entry(cte.cte_index).or_default();
                self.find_candidates(&cte.cte_query.operator, infos);
                self.find_candidates(&cte.child.operator, infos);
            }
            LogicalOperator::Join(Join::Comparison(join)) => {
                if let Some((cte_index, cte_is_left)) = direct_cte_side(join) {
                    if let Some(demand) = self.copy_join_demand(operator) {
                        infos.entry(cte_index).or_default().joined_refs.push(demand);
                        let other = if cte_is_left {
                            &join.right.operator
                        } else {
                            &join.left.operator
                        };
                        self.find_candidates(other, infos);
                        return;
                    }
                }
                for child in operator.children() {
                    self.find_candidates(&child.operator, infos);
                }
            }
            LogicalOperator::CTERef(reference) => {
                infos
                    .entry(reference.cte_index)
                    .or_default()
                    .all_refs_are_join_constrained = false;
            }
            _ => {
                for child in operator.children() {
                    self.find_candidates(&child.operator, infos);
                }
            }
        }
    }

    fn copy_join_demand(&self, operator: &LogicalOperator) -> Option<JoinedDemand> {
        let LogicalOperator::Join(Join::Comparison(source)) = operator else {
            return None;
        };
        if source.join_type != JoinType::Inner {
            return None;
        }

        // Copy the complete join once so the demand subtree and its condition
        // expressions share the same fresh binding map. The copied CTE leaf is
        // discarded after its output ordinals have been recovered.
        let copied = deep_copy_operator(operator, self.bind_context.shared().as_ref());
        let LogicalOperator::Join(Join::Comparison(mut join)) = copied else {
            return None;
        };
        let (_, cte_is_left) = direct_cte_side(&join)?;
        let cte_ref = match if cte_is_left {
            &join.left.operator
        } else {
            &join.right.operator
        } {
            LogicalOperator::CTERef(reference) => reference,
            _ => return None,
        };
        let cte_table_index = cte_ref.table_index;
        let cte_column_types = cte_ref.column_types.clone();
        let other_bindings = if cte_is_left {
            join.right.get_column_bindings()
        } else {
            join.left.get_column_bindings()
        }
        .into_iter()
        .collect::<HashSet<_>>();

        let mut keys = Vec::new();
        for condition in &join.conditions {
            if condition.comparison != JoinComparisonType::Equal {
                continue;
            }
            let (cte_expression, demand_expression) = if cte_is_left {
                (&condition.left, &condition.right)
            } else {
                (&condition.right, &condition.left)
            };
            let Expression::ColumnRef(cte_column) = cte_expression else {
                continue;
            };
            if cte_column.depth != 0 || cte_column.binding.table_index != cte_table_index {
                continue;
            }
            let mut demand_bindings = Vec::new();
            crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                demand_expression,
                &mut demand_bindings,
            );
            if demand_bindings.is_empty()
                || demand_bindings
                    .iter()
                    .any(|binding| !other_bindings.contains(binding))
                || demand_expression.evaluation_properties().is_reorder_fence()
            {
                continue;
            }
            let ordinal = cte_column.binding.column_index;
            let key_type = cte_column_types.get(ordinal)?.clone();
            if demand_expression.return_type() != key_type {
                continue;
            }
            keys.push((ordinal, key_type, demand_expression.clone()));
        }
        keys.sort_by_key(|(ordinal, _, _)| *ordinal);
        keys.dedup_by_key(|(ordinal, _, _)| *ordinal);
        if keys.is_empty() {
            return None;
        }

        let dummy = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan));
        let mut plan = if cte_is_left {
            *std::mem::replace(&mut join.right, dummy)
        } else {
            *std::mem::replace(&mut join.left, dummy)
        };
        if !replay_safe_demand(&mut plan.operator) {
            return None;
        }

        let mut key_ordinals = Vec::with_capacity(keys.len());
        let mut key_types = Vec::with_capacity(keys.len());
        let mut expressions = Vec::with_capacity(keys.len());
        for (ordinal, key_type, expression) in keys {
            key_ordinals.push(ordinal);
            key_types.push(key_type);
            expressions.push(expression);
        }
        Some(JoinedDemand {
            key_ordinals,
            key_types,
            plan,
            expressions,
        })
    }

    fn push_current_cte(
        &self,
        cte: &mut MaterializedCTE,
        infos: &mut HashMap<usize, MaterializedDemandInfo>,
    ) -> bool {
        let Some(info) = infos.remove(&cte.cte_index) else {
            return false;
        };
        if cte.materialized != CTEMaterialize::Default
            || !info.all_refs_are_join_constrained
            || info.joined_refs.is_empty()
        {
            return false;
        }
        let Some((key_ordinals, demand)) = build_demand_relation(info, self.bind_context) else {
            return false;
        };
        if !push_group_demand(
            cte.cte_query.as_mut(),
            &key_ordinals,
            demand,
            self.bind_context,
        ) {
            return false;
        }
        cte.materialized = CTEMaterialize::Materialized;
        true
    }
}

fn direct_cte_side(join: &ComparisonJoin) -> Option<(usize, bool)> {
    let left = match &join.left.operator {
        LogicalOperator::CTERef(reference) => Some(reference.cte_index),
        _ => None,
    };
    let right = match &join.right.operator {
        LogicalOperator::CTERef(reference) => Some(reference.cte_index),
        _ => None,
    };
    match (left, right) {
        (Some(cte_index), None) => Some((cte_index, true)),
        (None, Some(cte_index)) => Some((cte_index, false)),
        (Some(_), Some(_)) | (None, None) => None,
    }
}

/// Demand subplans are duplicated, so only immutable, error-free relational
/// shapes are admitted. The allowlist deliberately starts narrow; adding an
/// operator requires an explicit replayability proof.
fn replay_safe_demand(operator: &mut LogicalOperator) -> bool {
    let shape_is_safe = match operator {
        LogicalOperator::Get(get) => get.table.is_some(),
        LogicalOperator::ExpressionGet(_) | LogicalOperator::EmptyResult(_) => true,
        LogicalOperator::Filter(_) | LogicalOperator::Projection(_) => true,
        _ => false,
    };
    if !shape_is_safe {
        return false;
    }
    let mut expressions_are_safe = true;
    paro_planner::visitor::enumerate_expressions(operator, |expression| {
        expressions_are_safe &= !expression.evaluation_properties().is_reorder_fence();
    });
    if !expressions_are_safe {
        return false;
    }
    let mut children_are_safe = true;
    let _ = operator.visit_children_mut(|child| {
        children_are_safe &= replay_safe_demand(&mut child.operator);
        ControlFlow::Continue(())
    });
    children_are_safe
}
