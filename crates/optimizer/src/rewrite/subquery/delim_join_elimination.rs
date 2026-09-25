// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Eliminate redundant joins against `DelimGet`.

use std::collections::HashSet;

use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonType, ConjunctionType, Expression, ExpressionIterator,
    OperatorExpression, OperatorType,
};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, DelimGet, Filter, Join, JoinBuildSideConstraint,
    JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
};
use paro_planner::plan::OwnedLogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::rewrite::expr::binding_replacer::{ColumnBindingReplacer, ReplacementBinding};

/// Remove redundant joins against `DelimGet`.
pub struct DelimJoinElimination {
    projected_existence: bool,
}

struct ExistenceDecorrelation {
    conditions: Vec<JoinCondition>,
    local_filters: Vec<Expression>,
}

impl DelimJoinElimination {
    /// Canonical delimiter cleanup shared by every logical candidate.
    pub fn canonical() -> Self {
        Self {
            projected_existence: false,
        }
    }

    /// The broader existence-region alternative may consume a two-valued
    /// marker directly. Keeping this out of canonicalization preserves the
    /// original delimiter shape for specialized correlated-region proofs.
    pub fn projected_existence() -> Self {
        Self {
            projected_existence: true,
        }
    }

    pub fn optimize_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        plan.try_map_post_order(|plan| {
            Ok(plan.map_operator(|operator| self.optimize_operator(operator)))
        })
        .expect("delimiter-join elimination traversal cannot fail")
    }

    fn optimize_operator(&mut self, plan: LogicalOperator) -> LogicalOperator {
        let LogicalOperator::Join(Join::Comparison(mut join)) = plan else {
            return plan;
        };
        if join.duplicate_eliminated_columns.is_empty() {
            return LogicalOperator::Join(Join::Comparison(join));
        }

        if let Some(rewrite) = self.plan_existence_decorrelation(&join) {
            return LogicalOperator::Join(Join::Comparison(Self::decorrelate_existence(
                join,
                rewrite,
                self.projected_existence,
            )));
        }

        let mut replacements = Vec::new();
        {
            let delim_side = if join.delim_flipped {
                &mut join.left
            } else {
                &mut join.right
            };
            while let Some(mut current) =
                Self::remove_first_redundant_join(&mut delim_side.operator)
            {
                replacements.append(&mut current);
            }
        }

        if replacements.is_empty() {
            return LogicalOperator::Join(Join::Comparison(join));
        }

        let mut op = LogicalOperator::Join(Join::Comparison(join));
        let mut replacer = ColumnBindingReplacer::new();
        replacer.replacement_bindings = replacements;
        replacer.visit_operator(&mut op);

        let mut join = match op {
            LogicalOperator::Join(Join::Comparison(join)) => join,
            other => return other,
        };
        let delim_side = if join.delim_flipped {
            &join.left
        } else {
            &join.right
        };
        if !Self::contains_delim_get(delim_side.as_ref()) {
            join.duplicate_eliminated_columns.clear();
            join.delim_flipped = false;
        }
        LogicalOperator::Join(Join::Comparison(join))
    }

    /// Collapse the canonical correlated-EXISTS shape into one comparison
    /// join. The delimiter cross product only supplies the current outer
    /// correlation tuple; the same tuple is already available directly from
    /// the preserved side of a two-valued MARK, SEMI, or ANTI join.
    ///
    /// ```text
    /// outer existence-join projection(filter(base CROSS delim))
    ///   -> outer existence-join base
    /// ```
    ///
    /// Correlation predicates are rebound from delim columns to their outer
    /// expressions. This removes both delimiter materialization and the second
    /// hash table while preserving arbitrary side-local work below `base`.
    fn plan_existence_decorrelation(
        &self,
        join: &ComparisonJoin,
    ) -> Option<ExistenceDecorrelation> {
        let consumed_existence = matches!(join.join_type, JoinType::Semi | JoinType::Anti)
            && join.mark_index.is_none()
            && join.mark_semantics == paro_planner::operator::MarkJoinSemantics::NotMark;
        let two_valued_marker = self.projected_existence
            && join.join_type == JoinType::Mark
            && join.mark_index.is_some()
            && join.mark_semantics == paro_planner::operator::MarkJoinSemantics::TwoValued;
        if !(consumed_existence || two_valued_marker)
            || join.delim_flipped
            || join.anti_join_mode != paro_planner::operator::AntiJoinMode::Regular
            || join.conditions.len() != join.duplicate_eliminated_columns.len()
            || join
                .conditions
                .iter()
                .any(|condition| condition.comparison != JoinComparisonType::NotDistinctFrom)
        {
            return None;
        }

        let Some(region) = passive_projection_child(join.right.as_ref()) else {
            return None;
        };
        // Canonical predicate placement may consume the last Filter into the
        // comparison join. Decorrelation depends on predicate ownership, not
        // on the continued presence of an empty wrapper.
        let (region, filters) = match &region.operator {
            LogicalOperator::Filter(filter) => {
                (filter.child.as_ref(), filter.expressions.as_slice())
            }
            _ => (region, &[][..]),
        };
        let (delim, base_bindings, correlated_join_conditions) = match &region.operator {
            LogicalOperator::Join(Join::Cross(_)) if self.projected_existence => {
                let (delim, base_bindings) = cross_delim_region(region)?;
                (delim, base_bindings, None)
            }
            LogicalOperator::Join(Join::Cross(cross)) => {
                match (&cross.left.operator, &cross.right.operator) {
                    (LogicalOperator::DelimGet(delim), _) => (
                        delim,
                        cross.right.get_column_bindings().into_iter().collect(),
                        None,
                    ),
                    (_, LogicalOperator::DelimGet(delim)) => (
                        delim,
                        cross.left.get_column_bindings().into_iter().collect(),
                        None,
                    ),
                    _ => return None,
                }
            }
            LogicalOperator::Join(Join::Comparison(correlated_join))
                if correlated_join.join_type == JoinType::Inner
                    && correlated_join.duplicate_eliminated_columns.is_empty() =>
            {
                match (
                    &correlated_join.left.operator,
                    &correlated_join.right.operator,
                ) {
                    (LogicalOperator::DelimGet(delim), _) => {
                        let base_bindings = correlated_join
                            .right
                            .get_column_bindings()
                            .into_iter()
                            .collect();
                        (
                            delim,
                            base_bindings,
                            Some(correlated_join.conditions.as_slice()),
                        )
                    }
                    (_, LogicalOperator::DelimGet(delim)) => {
                        let base_bindings = correlated_join
                            .left
                            .get_column_bindings()
                            .into_iter()
                            .collect();
                        (
                            delim,
                            base_bindings,
                            Some(correlated_join.conditions.as_slice()),
                        )
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        if delim.chunk_types.len() != join.duplicate_eliminated_columns.len() {
            return None;
        }
        if !outer_conditions_bind_exact_delim_columns(join, delim.table_index) {
            return None;
        }
        let mut conditions = Vec::new();
        for condition in correlated_join_conditions.into_iter().flatten() {
            if !condition
                .left
                .evaluation_properties()
                .can_share_evaluation()
                || !condition
                    .right
                    .evaluation_properties()
                    .can_share_evaluation()
            {
                return None;
            }
            let condition = correlated_condition_from_parts(
                condition.left.clone(),
                condition.right.clone(),
                join_to_expression_comparison(condition.comparison),
                delim.table_index,
                &join.duplicate_eliminated_columns,
            )?;
            if !expression_references_only_bindings(&condition.right, &base_bindings) {
                return None;
            }
            conditions.push(condition);
        }

        let mut local_filters = Vec::new();
        for expression in filters.iter().flat_map(conjunction_terms) {
            if !expression.evaluation_properties().can_share_evaluation() {
                return None;
            }
            if let Some(condition) = correlated_join_condition(
                expression.clone(),
                delim.table_index,
                &join.duplicate_eliminated_columns,
            ) {
                if !expression_references_only_bindings(&condition.right, &base_bindings) {
                    return None;
                }
                conditions.push(condition);
            } else if !expression_references_table(expression, delim.table_index)
                && expression_references_only_bindings(expression, &base_bindings)
            {
                local_filters.push(expression.clone());
            } else {
                return None;
            }
        }
        Some(ExistenceDecorrelation {
            conditions,
            local_filters,
        })
    }

    fn decorrelate_existence(
        mut join: ComparisonJoin,
        rewrite: ExistenceDecorrelation,
        projected_existence: bool,
    ) -> ComparisonJoin {
        let right = *std::mem::replace(
            &mut join.right,
            Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        let base = match take_existence_base(right, projected_existence) {
            Ok(base) => base,
            Err(right) => {
                // The rewrite planner and extractor deliberately have separate
                // ownership concerns. If their accepted shapes ever diverge,
                // decline without panicking or damaging the original plan.
                join.right = right;
                return join;
            }
        };
        let base = if rewrite.local_filters.is_empty() {
            base
        } else {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                base,
                rewrite.local_filters,
            )))
        };

        let mut direct = ComparisonJoin::new(join.join_type, *join.left, base, rewrite.conditions);
        direct.anti_join_mode = join.anti_join_mode;
        direct.left_projection_map = join.left_projection_map;
        if join.join_type == JoinType::Mark {
            direct.mark_index = join.mark_index;
            direct.mark_semantics = join.mark_semantics;
            direct.right_projection_map = join.right_projection_map;
        }
        direct
    }

    fn remove_first_redundant_join(node: &mut LogicalOperator) -> Option<Vec<ReplacementBinding>> {
        match node {
            LogicalOperator::Join(Join::Comparison(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.right.operator)
                {
                    return Some(replacements);
                }
                Self::try_remove_join_with_delim_get(node)
            }
            LogicalOperator::Join(Join::Any(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut join.right.operator)
            }
            LogicalOperator::Join(Join::Cross(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut join.right.operator)
            }
            LogicalOperator::Filter(filter) => {
                Self::remove_first_redundant_join(&mut filter.child.operator)
            }
            LogicalOperator::Projection(proj) => {
                Self::remove_first_redundant_join(&mut proj.child.operator)
            }
            LogicalOperator::RowFetch(fetch) => {
                Self::remove_first_redundant_join(&mut fetch.child.operator)
            }
            LogicalOperator::ExternalProject(project) => {
                Self::remove_first_redundant_join(&mut project.child.operator)
            }
            LogicalOperator::ExternalTable(table) => table
                .child
                .as_mut()
                .and_then(|child| Self::remove_first_redundant_join(&mut child.operator)),
            LogicalOperator::Limit(limit) => {
                Self::remove_first_redundant_join(&mut limit.child.operator)
            }
            LogicalOperator::Order(order) => {
                Self::remove_first_redundant_join(&mut order.child.operator)
            }
            LogicalOperator::TopN(topn) => {
                Self::remove_first_redundant_join(&mut topn.child.operator)
            }
            LogicalOperator::Aggregate(agg) => {
                Self::remove_first_redundant_join(&mut agg.child.operator)
            }
            LogicalOperator::Distinct(distinct) => {
                Self::remove_first_redundant_join(&mut distinct.child.operator)
            }
            LogicalOperator::Window(window) => {
                Self::remove_first_redundant_join(&mut window.child.operator)
            }
            LogicalOperator::Explain(explain) => {
                Self::remove_first_redundant_join(&mut explain.child.operator)
            }
            LogicalOperator::SetOperation(setop) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut setop.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut setop.right.operator)
            }
            LogicalOperator::MaterializedCTE(cte) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut cte.cte_query.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut cte.child.operator)
            }
            LogicalOperator::RecursiveCTE(cte) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut cte.anchor.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut cte.recursive.operator)
            }
            LogicalOperator::Delete(delete) => {
                Self::remove_first_redundant_join(&mut delete.child.operator)
            }
            LogicalOperator::Update(update) => {
                Self::remove_first_redundant_join(&mut update.child.operator)
            }
            LogicalOperator::Insert(insert) => {
                Self::remove_first_redundant_join(&mut insert.child.operator)
            }
            LogicalOperator::CopyTo(copy) => {
                Self::remove_first_redundant_join(&mut copy.child.operator)
            }
            LogicalOperator::EmptyResult(empty) => {
                Self::remove_first_redundant_join(&mut empty.child.operator)
            }
            LogicalOperator::GraphExpand(expand) => {
                Self::remove_first_redundant_join(&mut expand.child.operator)
            }
            LogicalOperator::Get(_)
            | LogicalOperator::BoundReference(_)
            | LogicalOperator::ExpressionGet(_)
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::DummyScan => None,
        }
    }

    fn try_remove_join_with_delim_get(
        node: &mut LogicalOperator,
    ) -> Option<Vec<ReplacementBinding>> {
        let LogicalOperator::Join(Join::Comparison(join)) = node else {
            return None;
        };
        if !matches!(join.join_type, JoinType::Inner | JoinType::Semi) {
            return None;
        }

        let delim_idx = if Self::operator_is_delim_get(&join.left.operator) {
            0
        } else if Self::operator_is_delim_get(&join.right.operator) {
            1
        } else {
            return None;
        };

        let (delim_table_index, delim_types, mut filter_expressions) = if delim_idx == 0 {
            Self::extract_delim_metadata(join.left.as_ref())?
        } else {
            Self::extract_delim_metadata(join.right.as_ref())?
        };

        if join.conditions.len() != delim_types.len() {
            return None;
        }

        let mut replacements = Vec::with_capacity(join.conditions.len());
        let mut covered_columns = HashSet::with_capacity(delim_types.len());
        for cond in &join.conditions {
            // Only equality proves substitution. A range comparison (or
            // DISTINCT FROM) cannot identify a delimiter value with its mate.
            if !matches!(
                cond.comparison,
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
            ) {
                return None;
            }
            let (delim_expr, other_expr) = if delim_idx == 0 {
                (&cond.left, &cond.right)
            } else {
                (&cond.right, &cond.left)
            };

            let Expression::ColumnRef(delim_colref) = delim_expr else {
                return None;
            };
            let Expression::ColumnRef(other_colref) = other_expr else {
                return None;
            };
            if delim_colref.depth != 0
                || other_colref.depth != 0
                || delim_colref.binding.table_index != delim_table_index
                || delim_colref.binding.column_index >= delim_types.len()
                || !covered_columns.insert(delim_colref.binding.column_index)
                || delim_colref.return_type != other_colref.return_type
            {
                return None;
            }

            replacements.push(ReplacementBinding::new(
                delim_colref.binding,
                other_colref.binding,
            ));

            if cond.comparison == JoinComparisonType::Equal {
                filter_expressions.push(Expression::Operator(
                    OperatorExpression::new_unary(
                        OperatorType::IsNotNull,
                        Expression::ColumnRef(
                            ColumnRefExpression::new(
                                other_colref.binding,
                                other_colref.return_type.clone(),
                            )
                            .into(),
                        ),
                        LogicalType::Boolean,
                    )
                    .into(),
                ));
            }
        }

        let replacement_plan = if delim_idx == 0 {
            std::mem::replace(
                &mut *join.right,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            )
        } else {
            std::mem::replace(
                &mut *join.left,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            )
        };

        *node = if filter_expressions.is_empty() {
            replacement_plan.into_operator()
        } else {
            LogicalOperator::Filter(Filter::new(replacement_plan, filter_expressions))
        };
        Some(replacements)
    }

    fn operator_is_delim_get(op: &LogicalOperator) -> bool {
        matches!(op, LogicalOperator::DelimGet(_))
            || matches!(op, LogicalOperator::Filter(filter) if matches!(&filter.child.operator, LogicalOperator::DelimGet(_)))
    }

    fn extract_delim_metadata(
        plan: &OwnedLogicalPlan,
    ) -> Option<(usize, Vec<LogicalType>, Vec<Expression>)> {
        match &plan.operator {
            LogicalOperator::DelimGet(delim_get) => Some((
                delim_get.table_index,
                delim_get.chunk_types.clone(),
                Vec::new(),
            )),
            LogicalOperator::Filter(filter) => {
                let LogicalOperator::DelimGet(delim_get) = &filter.child.operator else {
                    return None;
                };
                Some((
                    delim_get.table_index,
                    delim_get.chunk_types.clone(),
                    filter.expressions.clone(),
                ))
            }
            _ => None,
        }
    }

    fn contains_delim_get(plan: &OwnedLogicalPlan) -> bool {
        let mut pending = vec![plan];
        while let Some(plan) = pending.pop() {
            if Self::operator_is_delim_get(&plan.operator) {
                return true;
            }
            pending.extend(plan.children());
        }
        false
    }
}

/// Prove that an unconstrained cross-product region contains exactly one
/// delimiter leaf and return the complete binding domain of every other leaf.
/// The region may contain any number of side-local relations; treating only a
/// direct `base CROSS delim` as canonical makes decorrelation depend on parser
/// join associativity.
fn cross_delim_region(plan: &OwnedLogicalPlan) -> Option<(&DelimGet, HashSet<ColumnBinding>)> {
    let mut pending = vec![plan];
    let mut delim = None;
    let mut base_bindings = HashSet::new();
    let mut base_leaves = 0usize;
    while let Some(plan) = pending.pop() {
        match &plan.operator {
            LogicalOperator::Join(Join::Cross(cross)) => {
                if cross.build_side_constraint != JoinBuildSideConstraint::Either {
                    return None;
                }
                pending.push(cross.right.as_ref());
                pending.push(cross.left.as_ref());
            }
            LogicalOperator::DelimGet(candidate) => {
                if delim.replace(candidate).is_some() {
                    return None;
                }
            }
            _ => {
                if DelimJoinElimination::contains_delim_get(plan) {
                    return None;
                }
                base_leaves += 1;
                base_bindings.extend(plan.get_column_bindings());
            }
        }
    }
    if base_leaves == 0 {
        return None;
    }
    Some((delim?, base_bindings))
}

fn conjunction_terms(expression: &Expression) -> Vec<&Expression> {
    match expression {
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::And =>
        {
            conjunction
                .children
                .iter()
                .flat_map(conjunction_terms)
                .collect()
        }
        _ => vec![expression],
    }
}

fn take_existence_base(
    plan: OwnedLogicalPlan,
    projected_existence: bool,
) -> Result<OwnedLogicalPlan, Box<OwnedLogicalPlan>> {
    let (id, stats, operator) = plan.into_parts();
    match operator {
        LogicalOperator::Projection(mut projection) => {
            match take_existence_base(*projection.child, projected_existence) {
                Ok(base) => Ok(base),
                Err(child) => {
                    projection.child = child;
                    Err(Box::new(OwnedLogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Projection(projection),
                    }))
                }
            }
        }
        LogicalOperator::Filter(mut filter) => {
            match take_existence_join_base(*filter.child, projected_existence) {
                Ok(base) => Ok(base),
                Err(child) => {
                    filter.child = child;
                    Err(Box::new(OwnedLogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Filter(filter),
                    }))
                }
            }
        }
        operator @ LogicalOperator::Join(_) => take_existence_join_base(
            OwnedLogicalPlan {
                id,
                stats,
                operator,
            },
            projected_existence,
        ),
        operator => Err(Box::new(OwnedLogicalPlan {
            id,
            stats,
            operator,
        })),
    }
}

fn take_existence_join_base(
    plan: OwnedLogicalPlan,
    projected_existence: bool,
) -> Result<OwnedLogicalPlan, Box<OwnedLogicalPlan>> {
    let (id, stats, operator) = plan.into_parts();
    let LogicalOperator::Join(join) = operator else {
        return Err(Box::new(OwnedLogicalPlan {
            id,
            stats,
            operator,
        }));
    };
    match join {
        Join::Cross(cross)
            if (!projected_existence
                || cross.build_side_constraint == JoinBuildSideConstraint::Either)
                && matches!(cross.left.operator, LogicalOperator::DelimGet(_)) =>
        {
            Ok(*cross.right)
        }
        Join::Cross(cross)
            if (!projected_existence
                || cross.build_side_constraint == JoinBuildSideConstraint::Either)
                && matches!(cross.right.operator, LogicalOperator::DelimGet(_)) =>
        {
            Ok(*cross.left)
        }
        Join::Cross(mut cross)
            if projected_existence
                && cross.build_side_constraint == JoinBuildSideConstraint::Either =>
        {
            let left_has_delim = DelimJoinElimination::contains_delim_get(cross.left.as_ref());
            let right_has_delim = DelimJoinElimination::contains_delim_get(cross.right.as_ref());
            if left_has_delim == right_has_delim {
                return Err(Box::new(OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Join(Join::Cross(cross)),
                }));
            }
            if left_has_delim {
                match take_existence_join_base(*cross.left, true) {
                    Ok(base) => cross.left = Box::new(base),
                    Err(left) => {
                        cross.left = left;
                        return Err(Box::new(OwnedLogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Cross(cross)),
                        }));
                    }
                }
            } else {
                match take_existence_join_base(*cross.right, true) {
                    Ok(base) => cross.right = Box::new(base),
                    Err(right) => {
                        cross.right = right;
                        return Err(Box::new(OwnedLogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Cross(cross)),
                        }));
                    }
                }
            }
            Ok(OwnedLogicalPlan {
                id,
                stats,
                operator: LogicalOperator::Join(Join::Cross(cross)),
            })
        }
        Join::Comparison(join) if matches!(join.left.operator, LogicalOperator::DelimGet(_)) => {
            Ok(*join.right)
        }
        Join::Comparison(join) if matches!(join.right.operator, LogicalOperator::DelimGet(_)) => {
            Ok(*join.left)
        }
        join => Err(Box::new(OwnedLogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Join(join),
        })),
    }
}

fn passive_projection_child(mut plan: &OwnedLogicalPlan) -> Option<&OwnedLogicalPlan> {
    while let LogicalOperator::Projection(projection) = &plan.operator {
        if !projection
            .expressions
            .iter()
            .all(Expression::is_passive_value)
        {
            return None;
        }
        plan = projection.child.as_ref();
    }
    Some(plan)
}

/// Verify that the delimiter join's outer equality conditions are the exact
/// identity map installed by dependent-join flattening. Merely checking their
/// count and comparison kind is insufficient: a business predicate with the
/// same arity must never authorize removal of the delimiter.
fn outer_conditions_bind_exact_delim_columns(
    join: &ComparisonJoin,
    delim_table_index: usize,
) -> bool {
    let mut seen = vec![false; join.duplicate_eliminated_columns.len()];
    for condition in &join.conditions {
        let left_delim = resolve_projected_delim_column(
            join.right.as_ref(),
            condition.left.clone(),
            delim_table_index,
        );
        let right_delim = resolve_projected_delim_column(
            join.right.as_ref(),
            condition.right.clone(),
            delim_table_index,
        );
        let (delim_column, outer_expression) = match (left_delim, right_delim) {
            (Some(column), None) => (column, &condition.right),
            (None, Some(column)) => (column, &condition.left),
            _ => return false,
        };
        let Some(expected_outer) = join.duplicate_eliminated_columns.get(delim_column) else {
            return false;
        };
        if seen[delim_column] || !outer_expression.equals(expected_outer) {
            return false;
        }
        seen[delim_column] = true;
    }
    seen.into_iter().all(|matched| matched)
}

fn resolve_projected_delim_column(
    mut plan: &OwnedLogicalPlan,
    mut expression: Expression,
    delim_table_index: usize,
) -> Option<usize> {
    while let LogicalOperator::Projection(projection) = &plan.operator {
        let Expression::ColumnRef(column) = &expression else {
            return None;
        };
        if column.depth != 0 || column.binding.table_index != projection.table_index {
            return None;
        }
        expression = projection
            .expressions
            .get(column.binding.column_index)?
            .clone();
        plan = projection.child.as_ref();
    }
    let Expression::ColumnRef(column) = expression else {
        return None;
    };
    (column.depth == 0 && column.binding.table_index == delim_table_index)
        .then_some(column.binding.column_index)
}

fn expression_references_only_table(expression: &Expression, table_index: usize) -> bool {
    let mut found = false;
    let mut foreign = false;
    fn visit(expression: &Expression, table_index: usize, found: &mut bool, foreign: &mut bool) {
        if let Expression::ColumnRef(column) = expression {
            if column.depth != 0 || column.binding.table_index != table_index {
                *foreign = true;
            } else {
                *found = true;
            }
            return;
        }
        ExpressionIterator::enumerate_children(expression, |child| {
            visit(child, table_index, found, foreign)
        });
    }
    visit(expression, table_index, &mut found, &mut foreign);
    found && !foreign
}

fn expression_references_table(expression: &Expression, table_index: usize) -> bool {
    if matches!(
        expression,
        Expression::ColumnRef(column)
            if column.depth == 0 && column.binding.table_index == table_index
    ) {
        return true;
    }
    let mut found = false;
    ExpressionIterator::enumerate_children(expression, |child| {
        if !found {
            found = expression_references_table(child, table_index);
        }
    });
    found
}

fn expression_references_only_bindings(
    expression: &Expression,
    bindings: &HashSet<paro_planner::operator::ColumnBinding>,
) -> bool {
    match expression {
        Expression::ColumnRef(column) => column.depth == 0 && bindings.contains(&column.binding),
        _ => {
            let mut valid = true;
            ExpressionIterator::enumerate_children(expression, |child| {
                valid &= expression_references_only_bindings(child, bindings);
            });
            valid
        }
    }
}

fn correlated_join_condition(
    expression: Expression,
    delim_table_index: usize,
    outer_columns: &[Expression],
) -> Option<JoinCondition> {
    let Expression::Comparison(comparison) = expression else {
        return None;
    };
    let comparison = comparison.into_inner();
    correlated_condition_from_parts(
        *comparison.left,
        *comparison.right,
        comparison.comparison_type,
        delim_table_index,
        outer_columns,
    )
}

fn correlated_condition_from_parts(
    left: Expression,
    right: Expression,
    comparison: ComparisonType,
    delim_table_index: usize,
    outer_columns: &[Expression],
) -> Option<JoinCondition> {
    let left_is_delim = expression_references_only_table(&left, delim_table_index);
    let right_is_delim = expression_references_only_table(&right, delim_table_index);
    if left_is_delim == right_is_delim {
        return None;
    }

    let (delim_expression, base_expression, comparison_type) = if left_is_delim {
        (left, right, comparison)
    } else {
        (right, left, flip_comparison(comparison))
    };
    if expression_references_table(&base_expression, delim_table_index) {
        return None;
    }
    let outer_expression = delim_expression.replace_column_ref(&|column| {
        if column.depth != 0 || column.binding.table_index != delim_table_index {
            return None;
        }
        outer_columns.get(column.binding.column_index).cloned()
    });
    if expression_references_table(&outer_expression, delim_table_index) {
        return None;
    }

    Some(JoinCondition::new(
        outer_expression,
        base_expression,
        join_comparison_type(comparison_type),
    ))
}

fn flip_comparison(comparison: ComparisonType) -> ComparisonType {
    match comparison {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

fn join_comparison_type(comparison: ComparisonType) -> JoinComparisonType {
    match comparison {
        ComparisonType::Equal => JoinComparisonType::Equal,
        ComparisonType::NotEqual => JoinComparisonType::NotEqual,
        ComparisonType::LessThan => JoinComparisonType::LessThan,
        ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
        ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
        ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
        ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
    }
}

fn join_to_expression_comparison(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

#[cfg(test)]
mod tests {
    use super::DelimJoinElimination;
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_function::scalar::{ExpressionState, FunctionStability, ScalarFunction};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        FunctionExpression,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, CrossProduct, DelimGet, ExpressionGet, Filter, Join,
        JoinComparisonType, JoinCondition, JoinType, LogicalOperator, MarkJoinSemantics,
        Projection,
    };
    use paro_planner::plan::OwnedLogicalPlan;

    fn noop_scalar_execute(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    fn volatile_call() -> Expression {
        let function = ScalarFunction::new(
            "volatile_delim_test".to_string(),
            vec![],
            LogicalType::Integer,
            noop_scalar_execute,
        )
        .with_stability(FunctionStability::Volatile);
        Expression::Function(FunctionExpression::new(function, vec![], LogicalType::Integer).into())
    }

    fn expression_get(table_index: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table_index, 0), LogicalType::Integer)
                    .into(),
            )]],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )))
    }

    fn column(table_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table_index, 0), LogicalType::Integer)
                .into(),
        )
    }

    fn comparison(
        comparison_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comparison_type, left, right).into())
    }

    fn correlated_existence_join(project_delim_column: bool) -> ComparisonJoin {
        let outer = expression_get(0);
        let base = expression_get(1);
        let delim = OwnedLogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        )));
        let cross = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
            CrossProduct::new(base, delim),
        )));
        let correlated = comparison(ComparisonType::Equal, column(1), column(99));
        let local = comparison(
            ComparisonType::GreaterThan,
            column(1),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
            ),
        );
        let filtered = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            cross,
            vec![correlated, local],
        )));
        let projected = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            2,
            filtered,
            vec![if project_delim_column {
                column(99)
            } else {
                column(1)
            }],
        )));
        let mut join = ComparisonJoin::new(
            JoinType::Semi,
            outer,
            projected,
            vec![JoinCondition::new(
                column(0),
                column(2),
                JoinComparisonType::NotDistinctFrom,
            )],
        );
        join.duplicate_eliminated_columns = vec![column(0)];
        join
    }

    #[test]
    fn decorrelates_existence_and_preserves_base_local_filters() {
        let result = DelimJoinElimination::canonical().optimize_plan(OwnedLogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(correlated_existence_join(true))),
        ));
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("expected direct existence join");
        };
        assert!(join.duplicate_eliminated_columns.is_empty());
        assert_eq!(join.conditions.len(), 1);
        let LogicalOperator::Filter(filter) = &join.right.operator else {
            panic!("base-local predicate must remain on the direct build side");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn decorrelates_two_valued_mark_over_a_multi_relation_cross_region() {
        let outer = expression_get(0);
        let fact = expression_get(1);
        let dimension = expression_get(2);
        let delim = OwnedLogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        )));
        let fact_and_delim = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
            CrossProduct::new(fact, delim),
        )));
        let region = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
            CrossProduct::new(fact_and_delim, dimension),
        )));
        let correlated = comparison(ComparisonType::Equal, column(1), column(99));
        let side_local = comparison(ComparisonType::Equal, column(1), column(2));
        let filtered = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            region,
            vec![correlated, side_local],
        )));
        let projected = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            3,
            filtered,
            vec![column(99)],
        )));
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            outer,
            projected,
            vec![JoinCondition::new(
                column(0),
                column(3),
                JoinComparisonType::NotDistinctFrom,
            )],
        );
        join.mark_index = Some(4);
        join.mark_semantics = MarkJoinSemantics::TwoValued;
        join.duplicate_eliminated_columns = vec![column(0)];

        let result = DelimJoinElimination::projected_existence().optimize_plan(
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join))),
        );
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("expected direct two-valued mark join");
        };
        assert_eq!(join.join_type, JoinType::Mark);
        assert_eq!(join.mark_index, Some(4));
        assert_eq!(join.mark_semantics, MarkJoinSemantics::TwoValued);
        assert!(join.duplicate_eliminated_columns.is_empty());
        assert_eq!(join.conditions.len(), 1);
        let LogicalOperator::Filter(filter) = &join.right.operator else {
            panic!("side-local predicate must remain over the independent cross region");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::Join(Join::Cross(_))
        ));
        assert!(!DelimJoinElimination::contains_delim_get(
            join.right.as_ref()
        ));
    }

    #[test]
    fn does_not_decorrelate_when_outer_join_does_not_bind_delim_output() {
        let result = DelimJoinElimination::canonical().optimize_plan(OwnedLogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(correlated_existence_join(false))),
        ));
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("expected delimiter join to remain");
        };
        assert!(!join.duplicate_eliminated_columns.is_empty());
        assert!(matches!(
            join.right.operator,
            LogicalOperator::Projection(_)
        ));
    }

    #[test]
    fn does_not_duplicate_volatile_correlated_predicates() {
        let mut join = correlated_existence_join(true);
        let LogicalOperator::Projection(projection) = &mut join.right.operator else {
            panic!("expected projected correlated input");
        };
        let LogicalOperator::Filter(filter) = &mut projection.child.operator else {
            panic!("expected correlated filter");
        };
        filter.expressions[0] = comparison(ComparisonType::Equal, volatile_call(), column(99));

        let result = DelimJoinElimination::canonical().optimize_plan(OwnedLogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(join)),
        ));
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("expected delimiter join to remain");
        };
        assert!(!join.duplicate_eliminated_columns.is_empty());
        assert!(matches!(
            join.right.operator,
            LogicalOperator::Projection(_)
        ));
    }

    #[test]
    fn delimiter_substitution_requires_equalities_covering_each_column() {
        for kind in [
            JoinComparisonType::LessThan,
            JoinComparisonType::GreaterThanOrEqual,
            JoinComparisonType::NotEqual,
            JoinComparisonType::DistinctFrom,
        ] {
            let mut node = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                expression_get(1),
                OwnedLogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
                    99,
                    vec![LogicalType::Integer],
                ))),
                vec![JoinCondition::new(column(1), column(99), kind)],
            )));
            assert!(DelimJoinElimination::try_remove_join_with_delim_get(&mut node).is_none());
            assert!(matches!(node, LogicalOperator::Join(_)));
        }
        let mut node = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            expression_get(1),
            OwnedLogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
                99,
                vec![LogicalType::Integer, LogicalType::Integer],
            ))),
            vec![JoinCondition::new(column(1), column(99), JoinComparisonType::Equal); 2],
        )));
        assert!(DelimJoinElimination::try_remove_join_with_delim_get(&mut node).is_none());
    }

    #[test]
    fn removes_redundant_inner_join_with_delim_get() {
        let outer = expression_get(0);
        let base = expression_get(1);
        let delim_get = OwnedLogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        )));

        let redundant = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                base,
                delim_get,
                vec![JoinCondition::new(
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(99, 0), LogicalType::Integer)
                            .into(),
                    ),
                    JoinComparisonType::Equal,
                )],
            ),
        )));

        let mut root_join = ComparisonJoin::new(
            JoinType::Inner,
            outer,
            redundant,
            vec![JoinCondition::new(
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
                ),
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer).into(),
                ),
                JoinComparisonType::Equal,
            )],
        );
        root_join.duplicate_eliminated_columns = vec![Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
        )];

        let result = DelimJoinElimination::canonical().optimize_plan(OwnedLogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(root_join)),
        ));

        match &result.operator {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(join.duplicate_eliminated_columns.is_empty());
                assert!(matches!(
                    join.right.operator,
                    LogicalOperator::Filter(_) | LogicalOperator::ExpressionGet(_)
                ));
                assert!(!matches!(join.right.operator, LogicalOperator::Join(_)));
            }
            _ => panic!("expected comparison join"),
        }
    }
}
