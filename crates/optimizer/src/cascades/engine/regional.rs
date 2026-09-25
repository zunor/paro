// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Finite relational programs over the shared alternative catalog.
//!
//! A pass visits a snapshot of reachable expressions, once. Its products can
//! feed subsequent passes, never recursively re-enter their own producer. No
//! physical task, quality subscriber or restoration cursor exists during this
//! stage. After the program, the existing physical solver costs the closed
//! catalog. Exploration retains the input and context-sensitive alternatives;
//! normalization retires pre-normal representations, not semantic proof IDs.

use super::*;

/// Normalization chooses a representation; exploration retains competing
/// implementations of a region. Keeping these distinct prevents a pass
/// pipeline from accidentally building the old rewrite powerset.
#[derive(Clone, Copy)]
pub(crate) enum RegionalPass {
    Normalize(RuleId),
    Explore(RuleId),
}

impl CascadesEngine {
    pub(crate) fn prepare_regional_candidates(
        &mut self,
        root: GroupId,
        passes: &[RegionalPass],
    ) -> Result<()> {
        if self.memo.physical_expr_count() != 0 {
            return Err(paro_error::internal(
                "regional candidate preparation must precede physical implementation",
            ));
        }
        let _partition = crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::Agenda);
        let _phase = crate::diagnostics::work::phase(crate::diagnostics::work::Phase::Optional);
        self.memo.record_regional_scope();
        self.memo.control().begin_optional();
        self.memo.seal_optional_group_budget();
        for &pass in passes {
            let (rule, normalize) = match pass {
                RegionalPass::Normalize(rule) => (rule, true),
                RegionalPass::Explore(rule) => (rule, false),
            };
            if !self.memo.control().checkpoint()? {
                break;
            }
            if !self.memo.budget().transformation_enabled(rule)
                || self.registry.transformation(rule).is_none()
            {
                continue;
            }
            // The immutable pass frontier is the termination argument. A rule
            // producing an equivalent shape cannot create a fixed-point loop.
            let frontier = self.regional_frontier(root)?;
            for (group, expression) in frontier {
                if !self.memo.control().checkpoint()? {
                    break;
                }
                let group = self.memo.canonical_group(group);
                let implementation = self.registry.transformation(rule).unwrap();
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                let logical = self
                    .memo
                    .logical_expr(expression)
                    .ok_or_else(|| paro_error::internal("regional expression disappeared"))?;
                if !implementation.root_operator_tag_may_match(logical.operator_tag)
                    || !implementation.root_dispatch(logical, &context)?.matches
                {
                    continue;
                }
                let started = Instant::now();
                let allocated = paro_common::allocator::thread_allocated_bytes();
                let binding_set = implementation.bindings(expression, &context)?;
                let elapsed = started.elapsed();
                *self.rule_elapsed.entry(rule).or_default() += elapsed;
                *self.rule_allocated_bytes.entry(rule).or_default() +=
                    paro_common::allocator::allocated_bytes_since(allocated);
                self.transformation_bindings += binding_set.bindings.len() as u64;
                if self.collect_compile_rule_work {
                    let work = self.rule_binding_work.entry(rule).or_default();
                    work.calls += 1;
                    work.elapsed += elapsed;
                }
                if self.collect_rule_work_profile {
                    let profile = self.rule_work_profile.entry(rule).or_default();
                    profile.discovered += 1;
                    profile.matched += binding_set.bindings.len() as u64;
                }
                let version = transformation_dependency_fingerprint(&binding_set.reads);
                if let PatternEnumerationCompletion::BudgetLimited { .. } = binding_set.completion {
                    self.memo
                        .group_ledger_mut(group)
                        .unwrap()
                        .record_budget_limited(binding_set.work_dimension, version);
                }
                if !admit_transformation_work(
                    &mut self.memo,
                    group,
                    expression,
                    rule,
                    version,
                    binding_set.work_units,
                    binding_set.work_dimension,
                )? {
                    *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                    continue;
                }
                let before = self.memo.group(group).unwrap().logical_exprs().len();
                for binding in &binding_set.bindings {
                    if !self.memo.control().checkpoint()? {
                        break;
                    }
                    self.apply_regional_binding(rule, group, expression, binding, version)?;
                }
                // All successful publications already carry checked semantic
                // equivalence. Retire the pre-normal form only after committed
                // replacements exist, outside the append-only transaction.
                if normalize {
                    if let Some(&replacement) =
                        self.memo.group(group).unwrap().logical_exprs().get(before)
                    {
                        self.memo
                            .retire_pre_normal_form(group, expression, replacement)?;
                    }
                }
            }
        }
        // Fact publication already invalidates semantic reader caches. There
        // are no physical subscribers to wake up in the preparation phase.
        self.memo.take_changed_cte_readers();
        Ok(())
    }

    /// Children before parents, with shared groups visited once. Do not walk
    /// unreachable staging products or scan all historical catalog entries.
    fn regional_frontier(&self, root: GroupId) -> Result<Vec<(GroupId, LogicalExprId)>> {
        let mut visited = BTreeSet::new();
        let mut pending = vec![(root, false)];
        let mut output = Vec::new();
        while let Some((group, expanded)) = pending.pop() {
            let group = self.memo.canonical_group(group);
            let entry = self
                .memo
                .group(group)
                .ok_or_else(|| paro_error::internal("regional frontier has an unknown group"))?;
            if expanded {
                output.extend(
                    entry
                        .logical_exprs()
                        .iter()
                        .map(|&expression| (group, expression)),
                );
            } else if visited.insert(group) {
                pending.push((group, true));
                for &expression in entry.logical_exprs().iter().rev() {
                    let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                        paro_error::internal("regional frontier has an unknown expression")
                    })?;
                    pending.extend(
                        logical
                            .key
                            .children
                            .iter()
                            .rev()
                            .map(|&child| (child, false)),
                    );
                }
            }
        }
        Ok(output)
    }

    fn apply_regional_binding(
        &mut self,
        rule: RuleId,
        group: GroupId,
        expression: LogicalExprId,
        binding: &PatternBinding,
        version: Fingerprint,
    ) -> Result<()> {
        let implementation = self.registry.transformation(rule).unwrap();
        let read = RuleContext {
            memo: &self.memo,
            group,
        };
        if implementation.preflight_binding(binding, &read)? == TransformationPreflight::NoOutput {
            return Ok(());
        }
        let class = implementation.budget_class();
        let version = transformation_binding_fingerprint(version, binding.fingerprint);
        let event = transformation_event(group, expression, rule, version);
        if self
            .memo
            .group_ledger_mut(group)
            .unwrap()
            .admit_optional(class.fire_dimension(), event)
            == BudgetDecision::Exhausted
        {
            *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
            return Ok(());
        }
        let bound = implementation.output_bound(
            binding,
            &RuleContext {
                memo: &self.memo,
                group,
            },
        );
        let mut reservations = Vec::new();
        for ordinal in 0..bound {
            let event = transformation_output_event(group, expression, rule, version, ordinal);
            if self
                .memo
                .group_ledger_mut(group)
                .unwrap()
                .admit_optional(class.output_dimension(), event)
                == BudgetDecision::Exhausted
            {
                release_transformation_output_reservations(
                    &mut self.memo,
                    group,
                    &reservations,
                    class.output_dimension(),
                )?;
                *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                return Ok(());
            }
            reservations.push(event);
        }
        self.memo.mark_rule_applied(expression, rule)?;
        *self.rule_attempts.entry(rule).or_default() += 1;
        let _rule = crate::diagnostics::work::rule(rule.0);
        let started = Instant::now();
        let allocated = paro_common::allocator::thread_allocated_bytes();
        let mut context = TransformContext::new(&mut self.memo, group);
        let output = implementation.apply_binding(binding, &mut context);
        *self.rule_elapsed.entry(rule).or_default() += started.elapsed();
        *self.rule_allocated_bytes.entry(rule).or_default() +=
            paro_common::allocator::allocated_bytes_since(allocated);
        if self.collect_rule_work_profile {
            let profile = self.rule_work_profile.entry(rule).or_default();
            match &output {
                Ok(outputs) if !outputs.is_empty() => {
                    profile.applicable += 1;
                    profile.constructed += outputs.len() as u64;
                }
                _ => profile.rejected += 1,
            }
        }
        let result = output.and_then(|outputs| {
            if outputs.len() > reservations.len() {
                return Err(paro_error::internal(
                    "regional producer exceeded its output bound",
                ));
            }
            let mut inserted = 0;
            for output in outputs {
                validate_transformation_proof(rule, expression, &output.proof)?;
                let target = context.memo().canonical_group(output.target_group);
                if target != context.memo().canonical_group(group) {
                    return Err(paro_error::internal(
                        "regional output changed its equivalence boundary",
                    ));
                }
                let duplicate = match output.operator_encoding.as_deref() {
                    Some(encoding) => context
                        .memo()
                        .logical_expr_for_structural_key(target, &output.key, encoding)
                        .is_some(),
                    None => context
                        .memo()
                        .logical_expr_for_key(target, &output.key)
                        .is_some(),
                };
                if !duplicate {
                    let before = context.memo().group(target).unwrap().logical_exprs().len();
                    context
                        .memo_mut()
                        .insert_logical_with_facts(output.into_memo_insertion())?;
                    inserted +=
                        context.memo().group(target).unwrap().logical_exprs().len() - before;
                }
            }
            Ok(inserted)
        });
        let inserted = match result {
            Ok(inserted) if inserted > 0 => {
                // A cancelled transaction cannot leak staged sidecar payloads.
                match context.memo().control().checkpoint() {
                    Ok(true) => {
                        context.commit()?;
                        inserted
                    }
                    stopped => {
                        context.rollback()?;
                        release_transformation_output_reservations(
                            &mut self.memo,
                            group,
                            &reservations,
                            class.output_dimension(),
                        )?;
                        stopped?;
                        return Ok(());
                    }
                }
            }
            Ok(_) => {
                context.rollback()?;
                0
            }
            Err(error) => {
                context.rollback()?;
                release_transformation_output_reservations(
                    &mut self.memo,
                    group,
                    &reservations,
                    class.output_dimension(),
                )?;
                // Fail closed on a broken producer contract. Do not turn an
                // implementation defect into a silently accepted baseline.
                return Err(error);
            }
        };
        release_transformation_output_reservations(
            &mut self.memo,
            group,
            &reservations[inserted..],
            class.output_dimension(),
        )?;
        *self.effective_rule_insertions.entry(rule).or_default() += inserted as u64;
        if self.collect_rule_work_profile {
            self.rule_work_profile.entry(rule).or_default().published += inserted as u64;
        }
        Ok(())
    }
}
