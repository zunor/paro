// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! A finite rewrite system with an oracle independent of Memo bookkeeping.
//! Semantic tags are intentionally separate from expression IDs/fingerprints.

use super::*;

fn identity(tag: u32, salt: u32) -> Fingerprint {
    Fingerprint((u128::from(tag) ^ u128::from(salt)).rotate_left(salt % 127))
}

struct Rewrite {
    id: RuleId,
    from: u32,
    salt: u32,
}

impl TransformationRule for Rewrite {
    fn id(&self) -> RuleId {
        self.id
    }

    fn matches_root(&self, expr: &crate::cascades::memo::LogicalExpr) -> bool {
        expr.payload.0 == self.from
    }

    fn matches(&self, expr: &crate::cascades::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn output_bound(&self, _: &PatternBinding, _: &RuleContext<'_>) -> usize {
        if self.from == 50 {
            9
        } else {
            1
        }
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let source = ctx.memo().logical_expr(expr).unwrap();
        let children = source.key.children.clone();
        let tags = if self.from == 50 {
            let values = children
                .iter()
                .map(|child| {
                    ctx.memo()
                        .group(*child)
                        .unwrap()
                        .logical_exprs()
                        .iter()
                        .map(|expression| ctx.memo().logical_expr(*expression).unwrap().payload.0)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            values[0]
                .iter()
                .flat_map(|a| values[1].iter().map(move |b| 100 + 3 * a + b))
                .collect::<Vec<_>>()
        } else {
            vec![self.from + 1]
        };
        Ok(tags
            .into_iter()
            .map(|tag| EquivalentExpression {
                target_group: ctx.group(),
                key: LogicalExprKey {
                    operator: identity(tag, self.salt),
                    scalars: Box::new([]),
                    children: children.clone(),
                },
                payload: LogicalPayloadId(tag),
                operator_encoding: None,
                logical_properties: LogicalProperties::default(),
                cardinality: GroupCardinality::default(),
                proof: EquivalenceProof::Transformation {
                    rule: self.id,
                    source: expr,
                    premise: identity(tag, self.salt),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice())
    }
}

struct Implementation;

struct NoBinding;

impl TransformationRule for NoBinding {
    fn id(&self) -> RuleId {
        RuleId(800)
    }
    fn matches_root(&self, _: &crate::cascades::memo::LogicalExpr) -> bool {
        true
    }
    fn matches(&self, _: &crate::cascades::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        false
    }
    fn apply(
        &self,
        _: LogicalExprId,
        _: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        panic!("a no-match must never reach apply")
    }
}

impl PhysicalImplementation for Implementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(500)
    }

    fn matches(
        &self,
        _: &crate::cascades::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        true
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let tag = logical.payload.0;
        let work = if tag < 3 {
            0.0
        } else if tag == 50 {
            50.0
        } else {
            let a = i64::from((tag - 100) / 3);
            let b = i64::from((tag - 100) % 3);
            (7 * (a - 2).pow(2) + 5 * (b - 1).pow(2) + 1) as f64
        };
        Ok(Box::new([PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: logical.key.children.clone(),
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(tag),
            provided: provided(),
            child_goals: logical
                .key
                .children
                .iter()
                .map(|child| (*child, goal))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            local_cost: cost(work),
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]))
    }
}

fn search(
    salt: u32,
    reverse_rules: bool,
    reverse_groups: bool,
    reverse_seeds: bool,
    limited: bool,
    output_limited: bool,
) -> (
    BTreeSet<u32>,
    f64,
    Box<[crate::cascades::budget::SearchObligation]>,
) {
    let mut budget = crate::cascades::budget::SearchBudget::default();
    if limited {
        budget.max_rule_firings_per_group = 0;
    }
    if output_limited {
        budget.max_optional_logical_exprs_per_group = 0;
    }
    let mut memo = Memo::new(budget);
    let mut groups = (0..3)
        .map(|_| {
            memo.create_group(
                schema(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        })
        .collect::<Vec<_>>();
    if reverse_groups {
        groups.reverse();
    }
    for &group in &groups[..2] {
        // Both insertion orders describe the same initial frontier.
        for (ordinal, tag) in (if reverse_seeds { [1, 0] } else { [0, 1] })
            .into_iter()
            .enumerate()
        {
            memo.insert_logical(
                group,
                LogicalExprKey {
                    operator: identity(tag, salt),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(tag),
                if ordinal == 0 {
                    EquivalenceProof::Initial
                } else {
                    EquivalenceProof::Normalization { rule: RuleId(900) }
                },
            )
            .unwrap();
        }
    }
    let root = groups[2];
    memo.insert_logical(
        root,
        LogicalExprKey {
            operator: identity(50, salt),
            scalars: Box::new([]),
            children: groups[..2].into(),
        },
        LogicalPayloadId(50),
        EquivalenceProof::Initial,
    )
    .unwrap();
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut registry = ImplementationRegistry::default();
    for (ordinal, from) in [0, 1, 50].into_iter().enumerate() {
        let id = if reverse_rules {
            102 - ordinal
        } else {
            100 + ordinal
        };
        registry
            .register_transformation(Rewrite {
                id: RuleId(id as u32),
                from,
                salt,
            })
            .unwrap();
    }
    registry.register_implementation(Implementation).unwrap();
    registry.register_transformation(NoBinding).unwrap();
    let mut engine = CascadesEngine::new(memo, registry);
    engine.observe_compile_rule_work();
    let winner = engine.optimize(root, goal, SearchMode::Memo).unwrap();
    if !limited {
        assert!(!engine.rule_binding_work().is_empty());
        assert!(engine
            .rule_binding_work()
            .get(&RuleId(800))
            .is_some_and(|work| work.calls > 0));
    }
    assert!(!engine.rule_attempts().contains_key(&RuleId(800)));
    if limited || output_limited {
        assert!(engine.rule_attempts().is_empty());
    }
    if output_limited {
        assert!(engine
            .rule_binding_work()
            .values()
            .any(|work| work.calls > 0));
        assert!(engine
            .rule_budget_exhaustions()
            .values()
            .any(|count| *count > 0));
    }
    let closure = engine
        .memo()
        .group(root)
        .unwrap()
        .logical_exprs()
        .iter()
        .map(|expression| engine.memo().logical_expr(*expression).unwrap().payload.0)
        .collect();
    (
        closure,
        winner.cost.score.range.expected,
        engine.memo().search_obligations(),
    )
}

#[test]
fn complete_closure_and_optimum_ignore_schedule_ids_and_fingerprints() {
    // The entire mathematical domain, not the frontier produced by the engine.
    let expected: BTreeSet<_> = std::iter::once(50)
        .chain((0..3).flat_map(|a| (0..3).map(move |b| 100 + 3 * a + b)))
        .collect();
    let oracle = (0_i64..3)
        .flat_map(|a| (0_i64..3).map(move |b| 7 * (a - 2).pow(2) + 5 * (b - 1).pow(2) + 1))
        .min()
        .unwrap() as f64;
    for salt in [0, 97, 126] {
        for reverse_rules in [false, true] {
            for reverse_groups in [false, true] {
                for reverse_seeds in [false, true] {
                    let (closure, cost, obligations) = search(
                        salt,
                        reverse_rules,
                        reverse_groups,
                        reverse_seeds,
                        false,
                        false,
                    );
                    assert_eq!(closure, expected);
                    assert_eq!(cost, oracle);
                    assert!(obligations.is_empty(), "{obligations:?}");
                }
            }
        }
    }
}

#[test]
fn limited_closure_keeps_baseline_and_names_unexplored_candidate_class() {
    let (closure, work, obligations) = search(97, true, true, true, true, false);
    assert_eq!(closure, BTreeSet::from([50]));
    assert_eq!(work, 50.0);
    assert!(!obligations.is_empty());
    assert!(obligations.iter().all(|obligation| obligation.reason
        == crate::cascades::budget::SearchIncompleteReason::Budget(
            BudgetDimension::RuleFirePerGroup
        )
        && obligation.group.is_some()));
}

#[test]
fn binding_before_output_budget_rejection_remains_observed() {
    let (closure, _, obligations) = search(0, false, false, false, false, true);
    assert_eq!(closure, BTreeSet::from([50]));
    assert!(!obligations.is_empty());
}
