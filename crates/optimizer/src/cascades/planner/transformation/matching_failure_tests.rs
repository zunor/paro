// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Wire from matching.rs with:
//! #[cfg(test)]
//! #[path = "matching_failure_tests.rs"]
//! mod failure_tests;

use super::*;
use paro_common::types::LogicalType;
use paro_planner::expression::ColumnRefExpression;
use paro_planner::operator::{Aggregate, Filter, Get, Projection};

struct FailureDag {
    memo: Memo,
    state: Arc<RwLock<PlannerTransformState>>,
    root: GroupId,
    expression: LogicalExprId,
    filters: Vec<GroupId>,
    leaf: GroupId,
    get: LogicalExprId,
}

impl FailureDag {
    fn new(depth: usize, limit: u32) -> Self {
        assert!(depth >= 2);
        let get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(0, vec!["x".into()], vec![LogicalType::Integer]),
        )));
        // Identity Projection is semantically equivalent to its Get, but is
        // deliberately outside AggregateNonNullInput's admitted path grammar.
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            0,
            get,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
            )],
        )));
        for _ in 0..depth {
            plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(plan, vec![])));
        }
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(
            Aggregate::new(1, 2, 3, plan, vec![], vec![], vec![], vec![]),
        )));
        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = limit;
        let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
        let mut fixture = Self {
            expression: input.memo.group(input.root).unwrap().logical_exprs()[0],
            root: input.root,
            memo: input.memo,
            state: input.planner_state,
            filters: Vec::new(),
            leaf: GroupId::new(0),
            get: LogicalExprId(0),
        };
        let mut group = fixture
            .memo
            .logical_expr(fixture.expression)
            .unwrap()
            .key
            .children[0];
        for _ in 0..depth {
            fixture.filters.push(group);
            let expression = fixture.memo.group(group).unwrap().logical_exprs()[0];
            group = fixture.memo.logical_expr(expression).unwrap().key.children[0];
        }
        fixture.leaf = group;
        let projection = fixture.memo.group(group).unwrap().logical_exprs()[0];
        let get_group = fixture.memo.logical_expr(projection).unwrap().key.children[0];
        fixture.get = fixture.memo.group(get_group).unwrap().logical_exprs()[0];

        // F(F(X)) == F(X) for identity filters. Each level can visit either
        // the next or the next-but-one suffix; both share the same failed DAG.
        for index in 0..depth - 1 {
            let target = fixture.filters[index];
            let child = fixture
                .filters
                .get(index + 2)
                .copied()
                .unwrap_or(fixture.leaf);
            fixture.add_filter_edge(target, child);
        }
        fixture
    }

    fn add_filter_edge(&mut self, target: GroupId, child: GroupId) {
        let source = self.memo.group(target).unwrap().logical_exprs()[0];
        let source = self.memo.logical_expr(source).unwrap();
        let mut key = source.key.clone();
        let payload = source.payload;
        key.children = Box::new([child]);
        self.memo
            .insert_logical(
                target,
                key,
                payload,
                EquivalenceProof::Normalization {
                    rule: RuleId(90_301),
                },
            )
            .unwrap();
    }

    fn publish_get(&mut self) {
        let get = self.memo.logical_expr(self.get).unwrap();
        let key = get.key.clone();
        let payload = get.payload;
        self.memo
            .insert_logical(
                self.leaf,
                key,
                payload,
                EquivalenceProof::Normalization {
                    rule: RuleId(90_302),
                },
            )
            .unwrap();
    }

    fn match_paths(&self) -> PatternBindingSet {
        scoped_pattern_bindings(
            PlannerTransformation::AggregateNonNullInput,
            self.root,
            self.expression,
            &self.memo,
            &self.state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
    }

    fn assert_get_paths(&self, result: &PatternBindingSet) {
        assert!(
            !result.bindings.is_empty(),
            "a real Get alternative must remain discoverable"
        );
        for binding in &result.bindings {
            let mut node = &binding.root;
            loop {
                let PatternOperand::Expression {
                    expression,
                    children,
                    ..
                } = node
                else {
                    panic!("a cycle cut or failed suffix must not masquerade as a Get path");
                };
                if children.is_empty() {
                    let payload = self.memo.logical_expr(*expression).unwrap().payload;
                    assert!(matches!(
                        self.state.read().unwrap().payloads.logical[payload.index()]
                            .semantic_template
                            .operator,
                        LogicalOperator::Get(_)
                    ));
                    break;
                }
                assert_eq!(children.len(), 1);
                node = &children[0];
            }
        }
    }
}

#[test]
fn nonnull_shared_failed_suffixes_complete_with_linear_charged_work() {
    for depth in [8, 16, 24] {
        let limit = 16 * (depth as u32 + 2) + 64;
        let fixture = FailureDag::new(depth, limit);
        let result = fixture.match_paths();
        assert!(result.bindings.is_empty());
        assert_eq!(
            result.completion,
            PatternEnumerationCompletion::Complete,
            "shared negative suffixes must finish within a linear budget at depth {depth}"
        );
        // There are 2*depth-1 distinct eligible Filter expressions. Charging
        // only the first observation of each group hides failed path work.
        assert!(
            result.work_units >= 2 * depth - 1,
            "failed expression attempts must be charged: depth={depth}, work={}",
            result.work_units
        );
        assert!(result.work_units <= limit as usize);
        for group in fixture.filters.iter().chain(std::iter::once(&fixture.leaf)) {
            assert!(
                result
                    .reads
                    .iter()
                    .any(|read| read.group == *group && read.logical_frontier_revision.is_some()),
                "negative suffix reuse must preserve frontier subscriptions for {group:?}"
            );
        }
    }
}

#[test]
fn nonnull_failed_dag_budget_exhaustion_is_not_complete() {
    let fixture = FailureDag::new(8, 4);
    let result = fixture.match_paths();
    assert!(result.bindings.is_empty());
    assert!(matches!(
        result.completion,
        PatternEnumerationCompletion::BudgetLimited { .. }
    ));
    assert!(result.work_units <= 4);
}

#[test]
fn nonnull_negative_invocation_does_not_hide_new_get_or_changed_facts() {
    let mut fixture = FailureDag::new(4, 4096);
    let before = fixture.match_paths();
    assert!(before.bindings.is_empty());
    assert_eq!(before.completion, PatternEnumerationCompletion::Complete);
    let old_read = *before
        .reads
        .iter()
        .find(|read| read.group == fixture.leaf)
        .unwrap();
    fixture.memo.group_mut(fixture.leaf).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(90_303),
        CardinalityRecipeKind::Statistics,
        1,
        4,
        9,
    );
    assert!(!old_read.is_current(&fixture.memo).unwrap());
    let changed = fixture.match_paths();
    assert!(changed.bindings.is_empty());
    assert_eq!(changed.completion, PatternEnumerationCompletion::Complete);
    assert!(
        changed
            .reads
            .iter()
            .all(|read| read.is_current(&fixture.memo).unwrap())
    );
    let changed_leaf = *changed
        .reads
        .iter()
        .find(|read| read.group == fixture.leaf)
        .unwrap();
    fixture.publish_get();
    assert!(!changed_leaf.is_current(&fixture.memo).unwrap());
    let after = fixture.match_paths();
    assert_eq!(after.completion, PatternEnumerationCompletion::Complete);
    fixture.assert_get_paths(&after);
    let repeated = fixture.match_paths();
    assert_eq!(repeated.bindings, after.bindings);
    assert_eq!(repeated.completion, PatternEnumerationCompletion::Complete);
}

#[test]
fn nonnull_cycle_terminates_and_does_not_hide_a_finite_get_path() {
    let mut fixture = FailureDag::new(3, 4096);
    let bottom = *fixture.filters.last().unwrap();
    fixture.add_filter_edge(bottom, bottom);
    let failed = fixture.match_paths();
    assert_eq!(failed.completion, PatternEnumerationCompletion::Complete);
    assert!(failed.work_units <= 4096);
    assert_eq!(
        fixture.match_paths().bindings,
        failed.bindings,
        "recursion-cut handling must remain stable across invocations"
    );
    fixture.publish_get();
    let valid = fixture.match_paths();
    assert_eq!(valid.completion, PatternEnumerationCompletion::Complete);
    // Existing cycle semantics may retain a Group hole. Do not count that
    // opaque cut as the newly published finite Get witness.
    assert!(
        valid.bindings.iter().any(|binding| {
            let mut node = &binding.root;
            loop {
                let PatternOperand::Expression {
                    expression,
                    children,
                    ..
                } = node
                else {
                    return false;
                };
                if children.is_empty() {
                    let payload = fixture.memo.logical_expr(*expression).unwrap().payload;
                    return matches!(
                        fixture.state.read().unwrap().payloads.logical[payload.index()]
                            .semantic_template
                            .operator,
                        LogicalOperator::Get(_)
                    );
                }
                assert_eq!(children.len(), 1);
                node = &children[0];
            }
        }),
        "a previous recursion cut must not suppress a finite Get path"
    );
}
