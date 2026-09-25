// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::context::OptimizationContext;
use crate::rewrite::aggregate::deduplicate::CommonAggregateOptimizer;
use crate::rewrite::aggregate::singleton_groups;
use crate::rewrite::column::lifetime::ColumnLifetimeAnalyzer;
use crate::rewrite::column::remove_unused::RemoveUnusedColumns;
use crate::rewrite::cte::inlining::CTEInlining;
use crate::rewrite::cte::iteration::normalize_iteration_ownership;
use crate::rewrite::expr::in_clause::InClauseRewriter;
use crate::rewrite::external::lowering::ExternalRoutineLoweringPass;
use crate::rewrite::graph::match_decompose::GraphMatchDecompose;
use crate::rewrite::graph::predicate_pushdown::GraphPredicatePushdown;
use crate::rewrite::join::mixed_predicates::JoinPredicateNormalizer;
use crate::rewrite::predicate::pushdown::FilterPushdown;
use crate::rewrite::subquery::delim_join_elimination::DelimJoinElimination;
use crate::rewrite::subquery::empty_result::EmptyResultPullup;
use crate::verify::verify_logical_plan;
use paro_common::error::Result;
use paro_planner::{binder::Binder, logical::plan::OwnedLogicalPlan};
use std::{collections::HashMap, sync::Arc};

/// The only owner of ordered normalization and the annotation barriers between
/// committed rewrites. This does not retain alternative query trees.
pub(crate) struct Normalization<'a> {
    pub ctx: &'a OptimizationContext,
    pub binder: &'a Binder,
}

impl Normalization<'_> {
    /// Only canonical, mandatory semantic work belongs here. Cost alternatives
    /// are owned by the implementation registry after this boundary.
    pub(crate) fn normalize(&self, mut plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }

        // GraphMatch is still a planner surface node today, while the executor
        // consumes canonical graph scan/expand semantics. The decomposition is
        // mandatory lowering; start/access alternatives belong in graph region
        // implementations and are not selected here.
        plan = GraphMatchDecompose::new().optimize_plan(plan);
        plan = GraphPredicatePushdown::new().optimize_plan(plan);

        let mut scalar_construction = crate::rewrite::expr::CanonicalScalars::default();
        scalar_construction.normalize_plan(&mut plan);

        CommonAggregateOptimizer::new().optimize(&mut plan);
        plan = DelimJoinElimination::canonical().optimize_plan(plan);
        plan = EmptyResultPullup::new().optimize_plan(plan);
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        plan = InClauseRewriter::new().rewrite(plan)?;
        // Predicate ownership is part of canonical relational semantics, not
        // a cost alternative. In particular, side-local predicates above a
        // delim/MARK boundary must reach that side before join normalization;
        // otherwise comma joins remain executable cross products with a late
        // filter and can create unbounded intermediates.
        plan = FilterPushdown::new().rewrite_plan(plan);
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        plan = ExternalRoutineLoweringPass::lower(plan, &self.ctx.bind_context)?.plan;
        plan = CTEInlining::new(&self.ctx.bind_context)
            .single_reference_defaults()
            .optimize_plan(plan);
        plan = crate::rewrite::cte::normalize::normalize(plan)?;
        // Mandatory substitution creates fresh filter/projection/set
        // boundaries. Canonicalize predicate placement before regional choices.
        plan = crate::rewrite::predicate::canonical::predicates(plan, &mut scalar_construction);
        plan = crate::rewrite::predicate::canonical::finish(plan)?;
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }
        Ok(plan)
    }

    fn estimate_query_candidate(
        &self,
        plan: OwnedLogicalPlan,
    ) -> Result<crate::estimate::AnnotatedRelation> {
        let mut context = self.ctx.fork_for_candidate(Arc::new(HashMap::new()));
        let plan = crate::estimate::annotate(plan, &mut context)?;

        Ok(crate::estimate::AnnotatedRelation {
            plan,
            column_stats: context.column_stats,
        })
    }

    fn settle_schema_candidate(
        &self,
        mut plan: OwnedLogicalPlan,
    ) -> Result<crate::estimate::AnnotatedRelation> {
        // Optional region rewrites and CTE substitution can expose a new
        // Filter(CrossProduct) boundary after the initial semantic pass. Keep
        // the Query-IR boundary canonical so equality edges always reach join
        // enumeration and physical implementation selection.
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        // Dependent-join flattening can preserve strict equality as a
        // null-safe comparison plus an explicit null rejection. Recover the
        // canonical equality before costing and physical artifact selection.
        plan = crate::rewrite::join::null_rejected_equality::optimize_plan(plan).0;
        // Query-IR output contracts are demand driven. Until every planner
        // operator natively exposes ColumnIds, derive the same canonical
        // demand projection once at the Query IR boundary; this is not an
        // optional cost rewrite and never removes an observable evaluation.
        RemoveUnusedColumns::optimize(&mut plan, self.binder, self.ctx.session.as_ref(), true);

        self.estimate_query_candidate(plan)
    }

    fn finalize_query_candidate(
        &self,
        mut candidate: crate::estimate::AnnotatedRelation,
    ) -> Result<crate::estimate::AnnotatedRelation> {
        candidate.plan = normalize_iteration_ownership(candidate.plan)?;
        candidate.plan = singleton_groups::optimize_plan(candidate.plan)?;
        candidate.plan = ColumnLifetimeAnalyzer::new(true).optimize(candidate.plan)?;

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &candidate.plan)?;
        }
        Ok(candidate)
    }

    pub(crate) fn settle_query_candidate(
        &self,
        plan: OwnedLogicalPlan,
    ) -> Result<crate::estimate::AnnotatedRelation> {
        let candidate = self.settle_schema_candidate(plan)?;
        self.finalize_query_candidate(candidate)
    }

    pub(crate) fn settle_relational_baseline(
        &self,
        plan: OwnedLogicalPlan,
    ) -> Result<crate::estimate::AnnotatedRelation> {
        // Direct two-valued existence decorrelation and sibling-marker folding
        // erase only delimiter carriers and multiplicity that no SQL result can
        // observe. They therefore define the baseline relational form rather
        // than a cost choice. Specialized correlated candidates are forked
        // before this boundary and retain the carrier required by their proofs.
        let plan = DelimJoinElimination::projected_existence().optimize_plan(plan);
        // Marker observability is encoded by executable projection maps. Settle
        // once before recognizing a disjunction, then rebuild statistics and
        // layouts exactly once if either normalization changes the tree.
        let candidate = self.settle_query_candidate(plan)?;
        let (plan, disjunction_changed) =
            crate::rewrite::subquery::existence_disjunction::optimize_plan(
                candidate.plan,
                &self.ctx.bind_context,
            )?;
        let (plan, reduction_changed) =
            crate::rewrite::subquery::existence_reduction::optimize_plan(plan)?;
        if !disjunction_changed && !reduction_changed {
            return Ok(crate::estimate::AnnotatedRelation {
                plan,
                column_stats: candidate.column_stats,
            });
        }
        self.settle_query_candidate(plan)
    }
}
