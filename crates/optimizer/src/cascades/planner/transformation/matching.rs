// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free structural preconditions for planner transformations.

use super::*;

/// Enumerate exact logical alternatives consumed by a planner transformation.
/// Every visited group revision is returned even when later rule recognition
/// declines, which gives no-match bindings an incremental wake-up edge.
pub(super) fn pattern_bindings(
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    budget: &SearchBudget,
    cancellation: Option<&paro_context::StatementCancellation>,
) -> Result<PatternBindingSet> {
    struct Enumerator<'a> {
        memo: &'a Memo,
        limit: usize,
        work_units: usize,
        reads: BTreeMap<GroupId, PatternRead>,
        limited: bool,
        cancellation: Option<&'a paro_context::StatementCancellation>,
    }

    impl Enumerator<'_> {
        fn operand_work(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => {
                    1usize.saturating_add(children.iter().map(Self::operand_work).sum::<usize>())
                }
            }
        }

        fn admit_work(&mut self, units: usize) -> Result<bool> {
            if let Some(cancellation) = self.cancellation {
                cancellation.check()?;
            }
            if self.work_units.saturating_add(units) > self.limit {
                self.limited = true;
                return Ok(false);
            }
            self.work_units += units;
            Ok(true)
        }

        fn observe(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if self.reads.contains_key(&group) {
                return Ok(true);
            }
            if !self.admit_work(1)? {
                return Ok(false);
            }
            self.reads
                .insert(group, PatternRead::from_group(self.memo, group)?);
            Ok(true)
        }

        fn group(
            &mut self,
            group: GroupId,
            active: &mut BTreeSet<GroupId>,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            let group = self.memo.canonical_group(group);
            let group_ref = self.memo.group(group).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown Memo group")
            })?;
            if !self.observe(group)? {
                return Ok(Vec::new());
            }
            if !active.insert(group) {
                if !self.admit_work(1)? {
                    return Ok(Vec::new());
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.group-hole.v1");
                fingerprint.write_u64(group.0 as u64);
                return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
            }
            let mut expressions = group_ref.logical_exprs().to_vec();
            expressions.sort_by_key(|expression| {
                self.memo
                    .logical_expr(*expression)
                    .map(|expression| expression.key.stable_fingerprint())
                    .unwrap_or_default()
            });
            let mut result = Vec::new();
            for expression in expressions {
                for candidate in self.expression(group, expression, active)? {
                    if result.len() == self.limit {
                        self.limited = true;
                        break;
                    }
                    result.push(candidate);
                }
                if self.limited && result.len() == self.limit {
                    break;
                }
            }
            active.remove(&group);
            result.sort_by_key(|(_, fingerprint)| *fingerprint);
            result.dedup_by_key(|(_, fingerprint)| *fingerprint);
            Ok(result)
        }

        fn expression(
            &mut self,
            group: GroupId,
            expression: LogicalExprId,
            active: &mut BTreeSet<GroupId>,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown logical expression")
            })?;
            let mut combinations: Vec<Vec<(PatternOperand, Fingerprint)>> = vec![Vec::new()];
            for child in logical.key.children.iter().copied() {
                let alternatives = self.group(child, active)?;
                let mut next = Vec::new();
                'outer: for prefix in combinations {
                    for alternative in &alternatives {
                        if next.len() == self.limit {
                            self.limited = true;
                            break 'outer;
                        }
                        let cloned_work = prefix
                            .iter()
                            .map(|(operand, _)| Self::operand_work(operand))
                            .sum::<usize>()
                            .saturating_add(Self::operand_work(&alternative.0));
                        if !self.admit_work(cloned_work)? {
                            break 'outer;
                        }
                        let mut candidate = prefix.clone();
                        candidate.push(alternative.clone());
                        next.push(candidate);
                    }
                }
                combinations = next;
            }
            if logical.key.children.is_empty() {
                combinations = vec![Vec::new()];
            }
            let mut operands = Vec::with_capacity(combinations.len());
            for children in combinations {
                let cloned_children = children
                    .iter()
                    .map(|(operand, _)| Self::operand_work(operand))
                    .sum::<usize>();
                if !self.admit_work(cloned_children.saturating_add(1))? {
                    break;
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.binding.v1");
                // The operator fingerprint already contains semantic
                // scalar fingerprints. Child group ids are allocation
                // artifacts and are replaced by the exact bound child
                // fingerprints below.
                fingerprint.write_fingerprint(logical.key.operator);
                for (_, child_fingerprint) in &children {
                    fingerprint.write_fingerprint(*child_fingerprint);
                }
                operands.push((
                    PatternOperand::Expression {
                        group,
                        expression,
                        children: children
                            .iter()
                            .map(|(operand, _)| operand.clone())
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    },
                    fingerprint.finish(),
                ));
            }
            Ok(operands)
        }
    }

    let configured_limit =
        usize::try_from(budget.max_rule_work_units_per_group).unwrap_or(usize::MAX);
    let already_consumed = memo
        .group(memo.canonical_group(root_group))
        .map_or(0, |group| {
            group.ledger.consumed(BudgetDimension::RuleWorkPerGroup)
        });
    let limit = configured_limit.saturating_sub(already_consumed);
    let mut enumerator = Enumerator {
        memo,
        limit,
        work_units: 0,
        reads: BTreeMap::new(),
        limited: false,
        cancellation,
    };
    let root_group = memo.canonical_group(root_group);
    // The semantic adapter consumes the root group's facts and cardinality,
    // even though root dispatch starts from an already selected expression.
    // Observe it through the same path as every descendant.
    if !enumerator.observe(root_group)? {
        return Ok(PatternBindingSet {
            bindings: Box::new([]),
            reads: Box::new([]),
            work_units: enumerator.work_units,
            completion: PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings: 0,
                omitted_at_least: 1,
            },
        });
    }
    let candidates = enumerator.expression(
        root_group,
        root_expression,
        &mut BTreeSet::from([root_group]),
    )?;
    let mut bindings = candidates
        .into_iter()
        .map(|(root, fingerprint)| PatternBinding { root, fingerprint })
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.fingerprint);
    bindings.dedup_by_key(|binding| binding.fingerprint);
    let enumerated_bindings = bindings.len();
    Ok(PatternBindingSet {
        bindings: bindings.into_boxed_slice(),
        reads: enumerator
            .reads
            .into_values()
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        work_units: enumerator.work_units,
        completion: if enumerator.limited {
            PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings,
                omitted_at_least: 1,
            }
        } else {
            PatternEnumerationCompletion::Complete
        },
    })
}

pub(super) fn matches_transformation_root(
    transformation: PlannerTransformation,
    expr: &crate::cascades::memo::LogicalExpr,
    state: &PlannerTransformState,
) -> bool {
    let Some(metadata) = state.metadata.get(&expr.payload) else {
        return false;
    };
    transformation_root_operator_matches(
        transformation,
        metadata.operator_type,
        state.rowset_scan_pushdown,
    )
}

/// Root dispatch is a function of the immutable operator shell and session
/// capabilities only. Equivalence provenance is deliberately absent: a rule
/// proof is audit evidence, never a semantic guard against newly added child
/// bindings of an expression produced by that same rule.
fn transformation_root_operator_matches(
    transformation: PlannerTransformation,
    operator: LogicalOperatorType,
    rowset_scan_pushdown: bool,
) -> bool {
    use LogicalOperatorType as Op;

    match transformation {
        PlannerTransformation::ExpensivePredicatePlacement => operator == Op::Filter,
        PlannerTransformation::CtePartitionedMaterialization
        | PlannerTransformation::CteInline
        | PlannerTransformation::CteDemandPushdown
        | PlannerTransformation::CteFilterPushdown => operator == Op::MaterializedCTE,
        PlannerTransformation::JoinRegionEnumeration => operator == Op::ComparisonJoin,
        PlannerTransformation::AggregatePostReduction => {
            matches!(operator, Op::MaterializedCTE | Op::Projection | Op::Filter)
        }
        // This transformation currently recognizes a consumed mark below a
        // transparent wrapper. Until that matcher is expressed with native
        // group holes, every known root remains a possible carrier.
        PlannerTransformation::MarkJoinToSemi => true,
        PlannerTransformation::JoinElimination => matches!(
            operator,
            Op::Projection | Op::Filter | Op::Aggregate | Op::Limit | Op::Order | Op::TopN
        ),
        PlannerTransformation::AggregateJoinPreaggregation
        | PlannerTransformation::AggregateJoinSubsumption
        | PlannerTransformation::AggregateNonNullInput
        | PlannerTransformation::AggregateDimensionDeferral
        | PlannerTransformation::AggregateInputMaterialization => operator == Op::Aggregate,
        PlannerTransformation::TopNIntroduction | PlannerTransformation::LimitPushdown => {
            operator == Op::Limit
        }
        PlannerTransformation::LatePayloadFetch => {
            rowset_scan_pushdown && matches!(operator, Op::Projection | Op::Aggregate | Op::TopN)
        }
        PlannerTransformation::ScalarAggregateWindow => {
            matches!(operator, Op::ComparisonJoin | Op::Projection | Op::Filter)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding_fingerprints(
        reverse: bool,
        id_shift: usize,
        proof_rule: RuleId,
    ) -> (Vec<Fingerprint>, Box<[PatternRead]>) {
        let mut memo = Memo::new(SearchBudget::default());
        let schema = GroupSchema::new(std::iter::empty()).unwrap();
        for _ in 0..id_shift {
            memo.create_group(
                schema.clone(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            );
        }
        let child = memo.create_group(
            schema.clone(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let alternatives = if reverse { [20_u128, 10] } else { [10, 20] };
        for (index, operator) in alternatives.into_iter().enumerate() {
            memo.insert_logical(
                child,
                LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId::new(operator as usize),
                if index == 0 {
                    EquivalenceProof::Initial
                } else {
                    EquivalenceProof::Normalization { rule: proof_rule }
                },
            )
            .unwrap();
        }
        let root = memo.create_group(
            schema,
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let root_expression = memo
            .insert_logical(
                root,
                LogicalExprKey {
                    operator: Fingerprint(30),
                    scalars: Box::new([]),
                    children: Box::new([child]),
                },
                LogicalPayloadId::new(30),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let bindings = pattern_bindings(root, root_expression, &memo, memo.budget(), None).unwrap();
        (
            bindings
                .bindings
                .iter()
                .map(|binding| binding.fingerprint)
                .collect(),
            bindings.reads,
        )
    }

    #[test]
    fn binding_closure_is_independent_of_expression_insertion_order() {
        let (forward, reads) = binding_fingerprints(false, 0, RuleId(99));
        let (reverse, _) = binding_fingerprints(true, 0, RuleId(99));
        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), 2);
        assert_eq!(reads.len(), 2);
    }

    #[test]
    fn binding_closure_is_independent_of_ids_and_proof_fingerprints() {
        let (baseline, baseline_reads) = binding_fingerprints(false, 0, RuleId(99));
        let (renamed, renamed_reads) = binding_fingerprints(false, 3, RuleId(9_999));
        assert_eq!(baseline, renamed);
        assert_eq!(baseline_reads.len(), renamed_reads.len());
        assert_ne!(baseline_reads[0].group, renamed_reads[0].group);
        assert_eq!(
            baseline_reads[0].logical_frontier_revision,
            renamed_reads[0].logical_frontier_revision
        );
    }

    #[test]
    fn pattern_reads_track_facts_and_statistics_without_frontier_churn() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(
            GroupSchema::new(std::iter::empty()).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let initial = PatternRead::from_group(&memo, group).unwrap();
        memo.group_mut(group)
            .unwrap()
            .logical_properties
            .maximum_cardinality = Some(7);
        let with_fact = PatternRead::from_group(&memo, group).unwrap();
        assert_eq!(
            initial.logical_frontier_revision,
            with_fact.logical_frontier_revision
        );
        assert_ne!(
            initial.logical_fact_fingerprint,
            with_fact.logical_fact_fingerprint
        );

        memo.group_mut(group).unwrap().cardinality =
            GroupCardinality::new(Fingerprint(17), CardinalityRecipeKind::Statistics, 1, 4, 9);
        let with_statistics = PatternRead::from_group(&memo, group).unwrap();
        assert_eq!(
            with_fact.logical_frontier_revision,
            with_statistics.logical_frontier_revision
        );
        assert_eq!(
            with_fact.logical_fact_fingerprint,
            with_statistics.logical_fact_fingerprint
        );
        assert_ne!(
            with_fact.statistics_snapshot_fingerprint,
            with_statistics.statistics_snapshot_fingerprint
        );
    }

    #[test]
    fn root_dispatch_has_no_equivalence_provenance_input() {
        assert!(transformation_root_operator_matches(
            PlannerTransformation::JoinRegionEnumeration,
            LogicalOperatorType::ComparisonJoin,
            true,
        ));
        assert!(transformation_root_operator_matches(
            PlannerTransformation::AggregateDimensionDeferral,
            LogicalOperatorType::Aggregate,
            true,
        ));
        assert!(!transformation_root_operator_matches(
            PlannerTransformation::JoinRegionEnumeration,
            LogicalOperatorType::Projection,
            true,
        ));
    }

    fn add_expression(
        memo: &mut Memo,
        ordinal: u128,
        children: Vec<GroupId>,
    ) -> (GroupId, LogicalExprId) {
        let group = memo.create_group(
            GroupSchema::new(std::iter::empty()).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let expression = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(ordinal),
                    scalars: Box::new([]),
                    children: children.into_boxed_slice(),
                },
                LogicalPayloadId::new(ordinal as usize),
                EquivalenceProof::Initial,
            )
            .unwrap();
        (group, expression)
    }

    #[test]
    fn read_set_includes_consumed_root_statistics() {
        let mut memo = Memo::new(SearchBudget::default());
        let (root, expression) = add_expression(&mut memo, 9_871, vec![]);
        let bindings = pattern_bindings(root, expression, &memo, memo.budget(), None).unwrap();
        assert!(bindings.reads.iter().any(|read| read.group == root));

        memo.group_mut(root).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(9_872),
            CardinalityRecipeKind::Statistics,
            1,
            4,
            9,
        );
        assert!(bindings
            .reads
            .iter()
            .any(|read| { *read != PatternRead::from_group(&memo, read.group).unwrap() }));
    }

    #[test]
    fn work_budget_bounds_shared_dag_operand_construction() {
        fn size(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => {
                    1 + children.iter().map(size).sum::<usize>()
                }
            }
        }

        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 8;
        let mut memo = Memo::new(budget);
        let (mut root, mut expression) = add_expression(&mut memo, 1, vec![]);
        for ordinal in 2..=9 {
            (root, expression) = add_expression(&mut memo, ordinal, vec![root, root]);
        }
        let bindings = pattern_bindings(root, expression, &memo, memo.budget(), None).unwrap();
        let expanded_nodes = bindings
            .bindings
            .iter()
            .map(|binding| size(&binding.root))
            .sum::<usize>();
        assert!(bindings.work_units <= 8);
        assert!(
            expanded_nodes <= 8
                || matches!(
                    bindings.completion,
                    PatternEnumerationCompletion::BudgetLimited { .. }
                )
        );
    }
}
