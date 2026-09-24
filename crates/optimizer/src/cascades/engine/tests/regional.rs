// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn regional_program_costs_closed_catalog_without_logical_reactivation() {
    let (mut regional, group, goal) = engine(8);
    regional
        .prepare_regional_candidates(group, &[RegionalPass::Explore(RuleId(5))])
        .unwrap();
    assert_eq!(regional.memo.physical_expr_count(), 0);
    assert_eq!(regional.memo.logical_expr_count(), 2);
    assert_eq!(regional.rule_attempts[&RuleId(5)], 1);
    let winner = regional.optimize(group, goal, SearchMode::Direct).unwrap();
    assert_eq!(regional.memo.logical_expr_count(), 2);
    assert_eq!(regional.rule_attempts[&RuleId(5)], 1);
    let (mut exhaustive, root, goal) = engine(8);
    let oracle = exhaustive.optimize(root, goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.physical_fingerprint, oracle.physical_fingerprint);
    assert_eq!(winner.cost, oracle.cost);
    assert_eq!(
        regional.search_stop().reason,
        SearchStopReason::SearchIncomplete
    );
    assert!(!regional.search_stop().budget_limited);
    assert!(regional
        .prepare_regional_candidates(group, &[RegionalPass::Explore(RuleId(5))])
        .is_err());
}

#[test]
fn regional_budget_rejection_retains_executable_input() {
    let (mut engine, root, goal) = engine(0);
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Explore(RuleId(5))])
        .unwrap();
    assert_eq!(engine.memo.logical_expr_count(), 1);
    let winner = engine.optimize(root, goal, SearchMode::Direct).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert_eq!(engine.search_stop().reason, SearchStopReason::BudgetLimited);
}

#[test]
fn regional_failed_producer_rolls_back_before_propagating_error() {
    let (mut engine, root, _) = engine(8);
    engine
        .registry
        .register_transformation(FailAfterMemoWrite)
        .unwrap();
    let groups = engine.memo.group_count();
    let logical = engine.memo.logical_expr_count();
    let error = engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(6))])
        .unwrap_err();
    assert!(error.to_string().contains("injected optional-rule failure"));
    assert_eq!(engine.memo.group_count(), groups);
    assert_eq!(engine.memo.logical_expr_count(), logical);
    assert_eq!(engine.memo.physical_expr_count(), 0);
}

#[test]
fn regional_cancellation_rolls_back_instead_of_becoming_a_plan() {
    let (mut engine, root, _) = engine(8);
    engine
        .registry
        .register_transformation(StopAfterMemoWrite { cancel: true })
        .unwrap();
    let groups = engine.memo.group_count();
    let logical = engine.memo.logical_expr_count();
    let error = engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(906))])
        .unwrap_err();
    assert!(error.is_query_canceled());
    assert_eq!(engine.memo.group_count(), groups);
    assert_eq!(engine.memo.logical_expr_count(), logical);
    assert_eq!(engine.memo.physical_expr_count(), 0);
}

struct Successor;

impl TransformationRule for Successor {
    fn id(&self) -> RuleId {
        RuleId(91)
    }
    fn matches_root(&self, _: &LogicalExpr) -> bool {
        true
    }
    fn matches(&self, _: &LogicalExpr, _: &RuleContext<'_>) -> bool {
        true
    }
    fn apply(
        &self,
        source: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let mut output = AddEquivalent.apply(source, ctx)?.into_vec();
        output[0].key.operator =
            Fingerprint(ctx.memo().logical_expr(source).unwrap().key.operator.0 + 1);
        output[0].proof = EquivalenceProof::Transformation {
            rule: self.id(),
            source,
            premise: Fingerprint(0),
        };
        Ok(output.into_boxed_slice())
    }
}

#[test]
fn regional_pass_has_finite_input_even_for_self_matching_producer() {
    let (mut engine, root, _) = engine(64);
    engine.registry.register_transformation(Successor).unwrap();
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Explore(RuleId(91))])
        .unwrap();
    assert_eq!(engine.memo.logical_expr_count(), 2);
    assert_eq!(engine.rule_attempts[&RuleId(91)], 1);
}

#[test]
fn normalization_keeps_proof_source_but_not_an_active_cost_alternative() {
    let (mut engine, root, goal) = engine(8);
    let source = engine.memo.group(root).unwrap().logical_exprs()[0];
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(5))])
        .unwrap();
    assert_eq!(engine.memo.group(root).unwrap().logical_exprs().len(), 1);
    assert!(engine.memo.logical_expr(source).is_some());
    assert!(!engine
        .memo
        .group(root)
        .unwrap()
        .logical_exprs()
        .contains(&source));
    crate::cascades::verifier::MemoVerifier::verify(&engine.memo, None).unwrap();
    let winner = engine.optimize(root, goal, SearchMode::Direct).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(11));
    assert_eq!(engine.memo.physical_expr_count(), 1);
}

#[test]
fn regional_program_respects_explicit_rule_exclusion() {
    let mut budget = crate::cascades::budget::SearchBudget::default();
    budget.disabled_transformation_rules.insert(RuleId(5));
    let (mut engine, root, _) = engine_with_budget(budget);
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(5))])
        .unwrap();
    assert_eq!(engine.memo.logical_expr_count(), 1);
    assert!(engine.rule_attempts.is_empty());
}

#[test]
fn retired_source_survives_a_later_failed_transaction() {
    let (mut engine, root, _) = engine(8);
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(5))])
        .unwrap();
    let active = engine.memo.group(root).unwrap().logical_exprs().to_vec();
    let count = engine.memo.logical_expr_count();
    let mut context = TransformContext::new(&mut engine.memo, root);
    let mut outputs = Successor.apply(active[0], &mut context).unwrap().into_vec();
    context
        .memo_mut()
        .insert_logical_with_facts(outputs.remove(0).into_memo_insertion())
        .unwrap();
    context.rollback().unwrap();
    assert_eq!(engine.memo.logical_expr_count(), count);
    assert_eq!(engine.memo.group(root).unwrap().logical_exprs(), active);
    crate::cascades::verifier::MemoVerifier::verify(&engine.memo, None).unwrap();
}

#[test]
fn normalization_does_not_retire_the_base_of_a_recursive_alternative() {
    let (mut engine, root, _) = engine(8);
    let source = engine.memo.group(root).unwrap().logical_exprs()[0];
    let mut context = TransformContext::new(&mut engine.memo, root);
    let mut outputs = AddEquivalent
        .apply(source, &mut context)
        .unwrap()
        .into_vec();
    let mut output = outputs.remove(0);
    output.key.children = vec![root].into_boxed_slice();
    context
        .memo_mut()
        .insert_logical_with_facts(output.into_memo_insertion())
        .unwrap();
    context.commit().unwrap();
    let replacement = engine.memo.group(root).unwrap().logical_exprs()[1];
    engine
        .memo
        .retire_pre_normal_form(root, source, replacement)
        .unwrap();
    assert_eq!(
        engine.memo.group(root).unwrap().logical_exprs(),
        &[source, replacement]
    );
}

#[test]
fn expired_regional_preparation_leaves_a_usable_baseline() {
    let (mut engine, root, goal) = engine(8);
    engine.memo.control().expire();
    engine
        .prepare_regional_candidates(root, &[RegionalPass::Normalize(RuleId(5))])
        .unwrap();
    assert_eq!(engine.memo.logical_expr_count(), 1);
    let _pricing = engine.memo.control().incumbent_phase();
    let winner = engine.optimize(root, goal, SearchMode::Regional).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert!(engine.memo.control().deadline_reached());
}
