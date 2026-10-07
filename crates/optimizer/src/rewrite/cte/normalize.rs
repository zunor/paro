// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pre-search CTE necessary-domain normalization. Consumer residuals remain
//! unchanged; no source-rule identity is used as a quality certificate.
use super::predicate_domain::{derive_producer_predicates, FilteredCTERef};
use paro_planner::expression::Expression;
use paro_planner::logical::operator::{ColumnBinding, Filter, LogicalOperator};
use paro_planner::logical::plan::OwnedLogicalPlan;
use std::collections::HashSet;

#[cfg(test)]
mod tests;

/// Push a producer-side necessary condition through the transparent part of
/// the producer before the ordinary optional rules run.  This is deliberately
/// a thin adapter over the planner's shared transfer contract: the normalizer
/// does not decide which predicates are safe to move and does not duplicate
/// Aggregate/Projection/UNION/Join semantics here.
fn push_producer_domain(
    plan: OwnedLogicalPlan,
    predicates: Vec<Expression>,
) -> paro_common::error::Result<OwnedLogicalPlan> {
    use paro_common::error as paro_error;
    use paro_planner::logical::plan::arena::LogicalPlanNode;

    enum Work {
        Visit(OwnedLogicalPlan, Vec<Expression>),
        Assemble(LogicalPlanNode<()>, usize, Vec<Expression>),
    }
    fn restrict(plan: OwnedLogicalPlan, predicates: Vec<Expression>) -> OwnedLogicalPlan {
        if predicates.is_empty() {
            plan
        } else {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(plan, predicates)))
        }
    }
    let mut work = vec![Work::Visit(plan, predicates)];
    let mut complete = Vec::new();
    while let Some(next) = work.pop() {
        match next {
            Work::Visit(plan, mut predicates) => {
                if let LogicalOperator::Filter(filter) = &plan.operator {
                    predicates.retain(|predicate| {
                        !filter
                            .expressions
                            .iter()
                            .any(|existing| existing.equals(predicate))
                    });
                }
                if predicates.is_empty() {
                    complete.push(Box::new(plan));
                    continue;
                }
                let layouts = plan
                    .children()
                    .iter()
                    .map(|child| child.output_layout())
                    .collect::<Vec<_>>();
                let refs = layouts.iter().collect::<Vec<_>>();
                let Some(routed) = crate::rewrite::predicate::column_transfer::transfer_predicates(
                    &plan.operator,
                    &refs,
                    &predicates,
                ) else {
                    complete.push(Box::new(restrict(plan, predicates)));
                    continue;
                };
                let (shell, children) = LogicalPlanNode::detach(plan);
                if children.len() != routed.child_predicates.len() {
                    return Err(paro_error::internal("domain transfer child arity mismatch"));
                }
                work.push(Work::Assemble(
                    shell,
                    children.len(),
                    routed.remaining.into_vec(),
                ));
                for (child, predicates) in children.into_iter().zip(routed.child_predicates).rev() {
                    work.push(Work::Visit(*child, predicates.into_vec()));
                }
            }
            Work::Assemble(shell, count, remaining) => {
                let start = complete
                    .len()
                    .checked_sub(count)
                    .ok_or_else(|| paro_error::internal("domain transfer missing child"))?;
                let children = complete.split_off(start);
                complete.push(Box::new(restrict(shell.assemble(children)?, remaining)));
            }
        }
    }
    if complete.len() != 1 {
        return Err(paro_error::internal("domain transfer has no unique root"));
    }
    complete
        .pop()
        .map(|plan| *plan)
        .ok_or_else(|| paro_error::internal("domain transfer missing root"))
}

/// Normalize each materialized producer only after accounting for every consumer
/// occurrence, including references in nested producer definitions. No rule
/// provenance is produced: quality must inspect the selected executable DAG.
pub(crate) fn normalize(plan: OwnedLogicalPlan) -> paro_common::error::Result<OwnedLogicalPlan> {
    plan.try_map_post_order(|mut plan| {
        if let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator {
            let mut references = Vec::new();
            let mut invalid = false;
            collect_filtered_cte_refs(
                &cte.child,
                cte.cte_index,
                &cte.output_columns,
                &mut references,
                &mut invalid,
            );
            if !invalid {
                let bindings = cte
                    .output_columns
                    .iter()
                    .map(|column| column.binding)
                    .collect::<Vec<_>>();
                if let Some(predicates) = derive_producer_predicates(references, &bindings) {
                    let producer = std::mem::replace(
                        &mut cte.cte_query,
                        Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
                    );
                    cte.cte_query = Box::new(push_producer_domain(*producer, predicates)?);
                }
            }
        }
        Ok(plan)
    })
}

/// A bare or unsupported consumer makes the producer restriction unavailable.
/// Walk nested producer definitions too: lexical nesting does not imply that
/// they cannot read the enclosing CTE.
fn collect_filtered_cte_refs(
    plan: &OwnedLogicalPlan,
    cte_index: usize,
    output_columns: &[paro_planner::logical::operator::cte::CteOutputColumn],
    references: &mut Vec<FilteredCTERef>,
    invalid_reference: &mut bool,
) {
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        if let LogicalOperator::Filter(filter) = &node.operator {
            if let LogicalOperator::CTERef(reference) = &filter.child.operator {
                if reference.cte_index == cte_index {
                    match filtered_cte_ref(reference, filter, output_columns) {
                        Some(reference) => references.push(reference),
                        None => {
                            *invalid_reference = true;
                            return;
                        }
                    }
                    continue;
                }
            }
        }
        if matches!(&node.operator, LogicalOperator::CTERef(reference) if reference.cte_index == cte_index)
        {
            *invalid_reference = true;
            return;
        }
        pending.extend(node.children());
    }
}

pub(crate) fn filtered_cte_ref<Child>(
    reference: &paro_planner::logical::operator::CTERef,
    filter: &Filter<Child>,
    output_columns: &[paro_planner::logical::operator::cte::CteOutputColumn],
) -> Option<FilteredCTERef> {
    if reference.definition_columns.len() != output_columns.len() || filter.expressions.is_empty() {
        return None;
    }
    let mut old_bindings = Vec::with_capacity(output_columns.len());
    for output in output_columns {
        let mut matches = reference
            .definition_columns
            .iter()
            .zip(
                reference
                    .column_types
                    .iter()
                    .enumerate()
                    .map(|(index, _)| ColumnBinding::new(reference.table_index, index)),
            )
            .filter(|(definition, _)| **definition == output.definition)
            .map(|(_, binding)| binding);
        let binding = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        old_bindings.push(binding);
    }
    let allowed = old_bindings.iter().copied().collect::<HashSet<_>>();
    for expression in &filter.expressions {
        if expression.evaluation_properties().is_reorder_fence() {
            return None;
        }
        let mut valid = true;
        crate::rewrite::expr::traversal::visit_expression(expression, &mut |node| match node {
            Expression::ColumnRef(column) => {
                valid &= column.depth == 0 && allowed.contains(&column.binding);
            }
            // Positional references and subqueries do not carry enough
            // producer-column identity for this pass to prove a safe
            // cross-domain rewrite.  Existing native domain transfer remains
            // the owner of those cases.
            Expression::Reference(_) | Expression::Subquery(_) => valid = false,
            _ => {}
        });
        if !valid {
            return None;
        }
    }
    Some(FilteredCTERef {
        old_bindings,
        filters: filter.expressions.clone(),
    })
}
