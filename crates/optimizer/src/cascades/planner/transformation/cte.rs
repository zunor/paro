// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! CTE occurrence requirements in the Memo binding domain.
//!
//! A producer is a GroupRef, not a selected expression. Each reference has an
//! independent occurrence path and a positional rebinding contract. Inlining
//! changes ownership while retaining that exact producer group.

use super::*;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{ColumnRefExpression, ComparisonType};
use paro_planner::operator::{CTERef, MaterializedCTE, Projection};
use paro_planner::plan::arena::LogicalPlanArena;

#[derive(Debug, Clone)]
pub(super) struct CteOccurrenceRequirement {
    pub(super) path: Box<[usize]>,
    pub(super) table_index: usize,
    pub(super) null_extended: bool,
    pub(super) output: PlannerBindingLayout,
    /// Local predicates evaluated before this occurrence can be null-extended.
    /// None denotes an unconstrained consumer, not an empty domain.
    pub(super) predicates: Option<Box<[Expression]>>,
    pub(super) keys: Option<CteKeyDemand>,
}

#[derive(Debug, Clone)]
pub(super) struct CteKeyDemand {
    pub(super) group: GroupId,
    pub(super) layout: PlannerBindingLayout,
    pub(super) ordinals: Box<[usize]>,
    pub(super) expressions: Box<[Expression]>,
}

/// Idempotence evidence is attached to an equivalent producer group, never
/// encoded by mutating the SQL materialization policy. Retaining the input
/// group makes subsequent producer alternatives available to every strategy.
#[derive(Debug, Clone)]
pub(in crate::cascades::planner) struct CteDomainProof {
    pub(in crate::cascades::planner) cte_index: usize,
    pub(in crate::cascades::planner) predicates: Box<[Expression]>,
    pub(super) keys: Box<[CteKeyDemand]>,
    /// Canonical domain identity used as a hash-bucket key.  It excludes
    /// Memo group ids (which can change during union/merge) and is therefore
    /// safe across canonicalization revisions; exact structural comparison
    /// remains the collision and group-identity check.
    pub(super) fingerprint: Fingerprint,
}

impl CteDomainProof {
    fn new(
        cte_index: usize,
        predicates: impl IntoIterator<Item = Expression>,
        keys: impl IntoIterator<Item = CteKeyDemand>,
    ) -> Self {
        let predicates = predicates.into_iter().collect::<Vec<_>>();
        let keys = keys.into_iter().collect::<Vec<_>>();
        let fingerprint = domain_fingerprint(cte_index, &predicates, &keys);
        Self {
            cte_index,
            predicates: predicates.into_boxed_slice(),
            keys: keys.into_boxed_slice(),
            fingerprint,
        }
    }

    fn same_domain_by(&self, other: &Self, canonical: impl Fn(GroupId) -> GroupId) -> bool {
        if self.cte_index != other.cte_index || self.fingerprint != other.fingerprint {
            return false;
        }
        let same_key = |left: &CteKeyDemand, right: &CteKeyDemand| {
            canonical(left.group) == canonical(right.group)
                && left.layout == right.layout
                && left.ordinals == right.ordinals
                && left.expressions.len() == right.expressions.len()
                && left
                    .expressions
                    .iter()
                    .zip(&right.expressions)
                    .all(|(left, right)| left.equals(right))
        };
        predicate_domains_equal(&self.predicates, &other.predicates)
            // Each occurrence contributes one arm of a UNION key domain.
            // Association, order and duplicate arms do not change membership.
            && self.keys.iter().all(|left| other.keys.iter().any(|right| same_key(left, right)))
            && other.keys.iter().all(|right| self.keys.iter().any(|left| same_key(left, right)))
    }
}

fn domain_fingerprint(
    cte_index: usize,
    predicates: &[Expression],
    keys: &[CteKeyDemand],
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.cte-domain.v1");
    builder.write_u64(cte_index as u64);

    let mut predicate_fingerprints = predicates
        .iter()
        .map(super::super::expression_fingerprint)
        .collect::<Vec<_>>();
    predicate_fingerprints.sort_unstable();
    predicate_fingerprints.dedup();
    builder.write_u64(predicate_fingerprints.len() as u64);
    for fingerprint in predicate_fingerprints {
        builder.write_fingerprint(fingerprint);
    }

    let mut key_fingerprints = keys
        .iter()
        .map(|key| {
            let mut key_builder = StableFingerprintBuilder::default();
            key_builder.write_u64(key.layout.bindings().len() as u64);
            for binding in key.layout.bindings() {
                key_builder.write_u64(binding.table_index as u64);
                key_builder.write_u64(binding.column_index as u64);
            }
            key_builder.write_u64(key.layout.types().len() as u64);
            for logical_type in key.layout.types() {
                key_builder.write_fingerprint(super::super::logical_type_fingerprint(logical_type));
            }
            key_builder.write_u64(key.ordinals.len() as u64);
            for ordinal in &key.ordinals {
                key_builder.write_u64(*ordinal as u64);
            }
            let expressions = key
                .expressions
                .iter()
                .map(super::super::expression_fingerprint)
                .collect::<Vec<_>>();
            key_builder.write_u64(expressions.len() as u64);
            for fingerprint in expressions {
                key_builder.write_fingerprint(fingerprint);
            }
            key_builder.finish()
        })
        .collect::<Vec<_>>();
    key_fingerprints.sort_unstable();
    key_fingerprints.dedup();
    builder.write_u64(key_fingerprints.len() as u64);
    for fingerprint in key_fingerprints {
        builder.write_fingerprint(fingerprint);
    }
    builder.finish()
}

fn binding_domain_fingerprint(definition: usize, domains: &[CteDomainProof]) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.cte-binding.v1");
    builder.write_u64(definition as u64);
    let mut fingerprints = domains
        .iter()
        .map(|domain| domain.fingerprint)
        .collect::<Vec<_>>();
    fingerprints.sort_unstable();
    fingerprints.dedup();
    builder.write_u64(fingerprints.len() as u64);
    for fingerprint in fingerprints {
        builder.write_fingerprint(fingerprint);
    }
    builder.finish()
}

/// Consumer join enumeration can change the order/association of a necessary
/// boolean domain. That is not a new restriction. Compare the AND/OR sets
/// structurally, retaining exact leaf equality (never hash-only equality).
/// Only total, reorderable predicates are admitted to CTE domains upstream.
fn predicate_domains_equal(left: &[Expression], right: &[Expression]) -> bool {
    fn equal(left: &Expression, right: &Expression) -> bool {
        match (left, right) {
            (Expression::Conjunction(left), Expression::Conjunction(right))
                if left.conjunction_type == right.conjunction_type =>
            {
                fn flatten(
                    expressions: &[Expression],
                    kind: paro_planner::expression::ConjunctionType,
                ) -> Vec<&Expression> {
                    let mut pending = expressions.iter().collect::<Vec<_>>();
                    let mut leaves = Vec::new();
                    while let Some(expression) = pending.pop() {
                        if let Expression::Conjunction(conjunction) = expression {
                            if conjunction.conjunction_type == kind {
                                pending.extend(conjunction.children.iter());
                                continue;
                            }
                        }
                        leaves.push(expression);
                    }
                    leaves
                }
                fn dedup<'a>(expressions: Vec<&'a Expression>) -> Vec<&'a Expression> {
                    let mut unique: Vec<&Expression> = Vec::with_capacity(expressions.len());
                    for expression in expressions {
                        if !unique.iter().any(|candidate| expression.equals(candidate)) {
                            unique.push(expression);
                        }
                    }
                    unique
                }
                let a = dedup(flatten(&left.children, left.conjunction_type));
                let b = dedup(flatten(&right.children, right.conjunction_type));
                a.len() == b.len()
                    && a.iter().all(|a| b.iter().any(|b| a.equals(b)))
                    && b.iter().all(|b| a.iter().any(|a| b.equals(a)))
            }
            _ => left.equals(right),
        }
    }
    left.iter().all(|a| right.iter().any(|b| equal(a, b)))
        && right.iter().all(|b| left.iter().any(|a| equal(a, b)))
}

#[derive(Debug, Clone)]
pub(in crate::cascades::planner) struct CteRestriction {
    pub(in crate::cascades::planner) producer: GroupId,
    pub(in crate::cascades::planner) input: GroupId,
    pub(in crate::cascades::planner) proof: CteDomainProof,
}

/// Closed lexical binding of a CTE domain. Equivalent owners need not have
/// equivalent unfiltered scans. A distinct symbol prevents fact sharing
/// between scans bound to different producer/domain environments.
#[derive(Debug, Clone)]
pub(in crate::cascades::planner) struct NativeCteBinding {
    symbol: usize,
    definition: usize,
    input: GroupId,
    /// Independent restrictions are intersected, not concatenated into one
    /// key UNION. The conjunction is commutative and idempotent.
    domains: Box<[CteDomainProof]>,
    fingerprint: Fingerprint,
}

/// Query-local alpha names for a partition strategy. Replaying a requirement
/// must not manufacture fresh groups by allocating new CTE labels each time.
type OccurrencePartition = Box<[(Box<[usize]>, usize)]>;
pub(in crate::cascades::planner) type PartitionLabels =
    BTreeMap<(usize, OccurrencePartition), Box<[usize]>>;

fn occurrence_equalities(occurrence: &CteOccurrenceRequirement) -> Vec<(usize, Expression)> {
    occurrence
        .predicates
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|expression| {
            let Expression::Comparison(comparison) = expression else {
                return None;
            };
            if comparison.comparison_type != ComparisonType::Equal {
                return None;
            }
            let (column, value) = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(value))
                | (Expression::Constant(value), Expression::ColumnRef(column)) => (column, value),
                _ => return None,
            };
            (column.depth == 0 && column.binding.table_index == occurrence.table_index).then(|| {
                (
                    column.binding.column_index,
                    Expression::Constant(value.clone()),
                )
            })
        })
        .collect()
}

fn input_is_null_extended<Child>(operator: &LogicalOperator<Child>, ordinal: usize) -> bool {
    use paro_planner::operator::dependent_join::DependentJoinKind;
    let join_type = match operator {
        LogicalOperator::Join(join) => join.join_type(),
        LogicalOperator::DependentJoin(join) => match &join.kind {
            DependentJoinKind::Scalar { .. } => JoinType::Single,
            DependentJoinKind::Lateral { join_type, .. } => *join_type,
            DependentJoinKind::Mark { .. } => return false,
        },
        _ => return false,
    };
    match join_type {
        JoinType::Left | JoinType::Single => ordinal == 1,
        JoinType::Right => ordinal == 0,
        JoinType::Outer => true,
        _ => false,
    }
}

fn key_demand(
    operator: &LogicalOperator<()>,
    children: &[PatternOperand],
    ordinal: usize,
    cte_index: usize,
    layouts: &[PlannerBindingLayout],
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<Option<CteKeyDemand>> {
    let LogicalOperator::Join(Join::Comparison(join)) = operator else {
        return Ok(None);
    };
    if join.join_type != JoinType::Inner || children.len() != 2 {
        return Ok(None);
    }
    let PatternOperand::Expression { expression, .. } = &children[ordinal] else {
        return Ok(None);
    };
    let logical = memo
        .logical_expr(*expression)
        .ok_or_else(|| paro_error::internal("CTE key witness lost its reference"))?;
    let LogicalOperator::CTERef(reference) = &state.payloads.logical[logical.payload.index()]
        .semantic_template
        .operator
    else {
        return Ok(None);
    };
    if reference.cte_index != cte_index {
        return Ok(None);
    }
    let other = &children[1 - ordinal];
    let PatternOperand::Expression { group, .. } = other else {
        return Ok(None);
    };
    // The matcher binds the replay proof, not a representative implementation.
    // Its exact root group is retained by the evaluation occurrence below.
    let mut pending = vec![other];
    while let Some(operand) = pending.pop() {
        let PatternOperand::Expression {
            expression,
            children,
            ..
        } = operand
        else {
            return Ok(None);
        };
        let logical = memo
            .logical_expr(*expression)
            .ok_or_else(|| paro_error::internal("CTE replay witness disappeared"))?;
        let mut operator = state.payloads.logical[logical.payload.index()]
            .semantic_template
            .operator
            .clone();
        let safe = match &operator {
            LogicalOperator::Get(get) => {
                get.table.is_some()
                    && get
                        .runtime_filter_expressions
                        .iter()
                        .all(|expression| !expression.evaluation_properties().is_reorder_fence())
            }
            LogicalOperator::ExpressionGet(_)
            | LogicalOperator::EmptyResult(_)
            | LogicalOperator::Filter(_)
            | LogicalOperator::Projection(_) => true,
            _ => false,
        };
        if !safe {
            return Ok(None);
        }
        let mut fenced = false;
        paro_planner::visitor::enumerate_expressions(&mut operator, |expression| {
            fenced |= expression.evaluation_properties().is_reorder_fence()
        });
        if fenced {
            return Ok(None);
        }
        pending.extend(children);
    }
    let layout = layouts
        .get(1 - ordinal)
        .ok_or_else(|| paro_error::internal("CTE demand lost its input layout"))?
        .clone();
    let available = layout.bindings().iter().copied().collect::<BTreeSet<_>>();
    let mut keys = Vec::new();
    for condition in &join.conditions {
        if condition.comparison != JoinComparisonType::Equal {
            continue;
        }
        let (cte_expression, demand_expression) = if ordinal == 0 {
            (&condition.left, &condition.right)
        } else {
            (&condition.right, &condition.left)
        };
        let Expression::ColumnRef(column) = cte_expression else {
            continue;
        };
        if column.depth != 0
            || column.binding.table_index != reference.table_index
            || demand_expression.evaluation_properties().is_reorder_fence()
        {
            continue;
        }
        let mut referenced = Vec::new();
        crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
            demand_expression,
            &mut referenced,
        );
        if referenced.is_empty()
            || referenced
                .iter()
                .any(|binding| !available.contains(binding))
            || column.return_type != demand_expression.return_type()
        {
            continue;
        }
        keys.push((column.binding.column_index, demand_expression.clone()));
    }
    keys.sort_by_key(|(ordinal, _)| *ordinal);
    keys.dedup_by_key(|(ordinal, _)| *ordinal);
    if keys.is_empty() {
        return Ok(None);
    }
    let (ordinals, expressions): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
    Ok(Some(CteKeyDemand {
        group: memo.canonical_group(*group),
        layout,
        ordinals: ordinals.into_boxed_slice(),
        expressions: expressions.into_boxed_slice(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::operator::{ComparisonJoin, JoinCondition, MaterializedCTE};

    fn reference(cte: usize, table: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            cte,
            table,
            "shared".into(),
            vec!["key".into()],
            vec![LogicalType::Integer],
        )))
    }

    fn equality(table: usize, value: i32) -> Expression {
        Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer)
                        .into(),
                ),
                Expression::Constant(
                    paro_planner::expression::ConstantExpression {
                        value: paro_common::runtime_value::Value::Integer(value),
                        return_type: LogicalType::Integer,
                    }
                    .into(),
                ),
            )
            .into(),
        )
    }

    fn owner(consumer: OwnedLogicalPlan) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "shared".into(),
            vec!["key".into()],
            vec![LogicalType::Integer],
            CTEMaterialize::Default,
            super::super::super::tests::test_base_get(0, 7, "source", 10),
            consumer,
        )))
    }

    fn two_discriminator_owner() -> OwnedLogicalPlan {
        use paro_planner::operator::{CrossProduct, ExpressionGet, Filter};
        let branch = |table, a, b| {
            let mut second = equality(table, b);
            let Expression::Comparison(compare) = &mut second else {
                unreachable!()
            };
            let Expression::ColumnRef(column) = compare.left.as_mut() else {
                unreachable!()
            };
            column.binding.column_index = 1;
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
                    9,
                    table,
                    "shared".into(),
                    vec!["a".into(), "b".into()],
                    vec![LogicalType::Integer; 2],
                ))),
                vec![equality(table, a), second],
            )))
        };
        let cross = |left, right| {
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
                left, right,
            ))))
        };
        OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "shared".into(),
            vec!["a".into(), "b".into()],
            vec![LogicalType::Integer; 2],
            CTEMaterialize::Default,
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["a".into(), "b".into()],
                vec![LogicalType::Integer; 2],
            ))),
            cross(
                cross(branch(10, 1, 10), branch(11, 1, 20)),
                cross(branch(12, 2, 10), branch(13, 2, 20)),
            ),
        )))
    }

    #[test]
    fn partition_strategies_enumerate_both_dimensions_and_reuse_alpha_names() {
        let (input, requirement, instantiated, _) = bind_requirement(
            two_discriminator_owner(),
            PlannerTransformation::CtePartitionedMaterialization,
        );
        let mut arena = paro_planner::plan::arena::LogicalPlanArena::default();
        let root = arena.import(instantiated.plan).unwrap();
        let mut labels = PartitionLabels::new();
        for _ in 0..2 {
            let candidates = requirement
                .partitions(
                    arena.export(root).unwrap(),
                    &mut instantiated.group_holes.clone(),
                    &input.bind_context,
                    &mut labels,
                    &mut arena,
                )
                .unwrap();
            assert_eq!(
                candidates.len(),
                2,
                "incomparable channel/year strategies must both reach costing"
            );
            assert_eq!(
                labels.len(),
                2,
                "replaying the requirement must reuse its strategy names"
            );
        }
    }

    #[test]
    fn engine_admits_every_partition_discriminator_from_one_binding() {
        let session = crate::subquery::partition_aggregate_tests::setup_session();
        let binder = Binder::new(session.clone());
        for _ in 0..20 {
            binder.bind_context.generate_table_index();
        }
        let mut context =
            crate::context::OptimizationContext::new(session, binder.bind_context.clone());
        let plan = StatisticsGathering::new()
            .gather(two_discriminator_owner(), &mut context)
            .unwrap();
        let mut budget = SearchBudget::default();
        for rule in PlannerTransformation::ALL {
            if !matches!(rule, PlannerTransformation::CtePartitionedMaterialization) {
                budget.disable_transformation(rule.id());
            }
        }
        let result = MemoBuilder::build_with_search(
            vec![LogicalAlternative {
                plan,
                source: AlternativeOrigin::Baseline,
                column_stats: context.column_stats.clone(),
            }],
            &binder,
            budget,
            &context,
        )
        .unwrap()
        .optimize(&[ResourceGrantClass {
            id: super::super::super::super::ids::ResourceGrantClassId(0),
            hard_memory_bytes: u64::MAX,
            spill_policy: crate::physical::SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        }])
        .unwrap();
        assert!(
            result
                .rule_insertions
                .get(&CTE_PARTITIONED_MATERIALIZATION_RULE)
                .is_some_and(|count| *count >= 2),
            "a two-output binding must not be rolled back by a one-output reservation: {:?}",
            result.rule_insertions
        );
        assert!(
            result.search_summary.is_complete(),
            "{:?}",
            result.search_summary
        );
    }

    fn bind_requirement(
        plan: OwnedLogicalPlan,
        rule: PlannerTransformation,
    ) -> (
        OptimizationInput,
        CteRequirement,
        semantic_plan::InstantiatedPlanWithGroupHoles,
        boundary::BoundarySnapshot,
    ) {
        let bind = BindContext::new();
        for _ in 0..20 {
            bind.generate_table_index();
        }
        let mut input = MemoBuilder::build(plan, bind, SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let bindings = matching::scoped_pattern_bindings(
            rule,
            input.root,
            root,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
        assert_eq!(bindings.bindings.len(), 1);
        let binding = &bindings.bindings[0];
        let requirement = CteRequirement::from_binding(binding, &input.memo, &state).unwrap();
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let plan = semantic_plan::instantiate_bound_plan_with_group_holes(
            context.memo(),
            &state,
            &binding.root,
            Some(&facts),
        )
        .unwrap()
        .unwrap();
        drop(state);
        (input, requirement, plan, facts)
    }

    #[test]
    fn partition_identity_is_the_consumer_edge_not_its_reused_alias() {
        use paro_planner::operator::{Filter, SetOpType, SetOperation};
        let filtered = |value| {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                reference(9, 1),
                vec![equality(1, value)],
            )))
        };
        let consumer =
            OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
                12,
                filtered(1),
                filtered(2),
                SetOpType::Union,
                true,
                vec![LogicalType::Integer],
            )));
        let (input, requirement, mut plan, _) = bind_requirement(
            owner(consumer),
            PlannerTransformation::CtePartitionedMaterialization,
        );
        assert_eq!(requirement.occurrences.len(), 2);
        assert_eq!(
            requirement.occurrences[0].table_index,
            requirement.occurrences[1].table_index
        );
        let result = requirement
            .partition(plan.plan, &mut plan.group_holes, &input.bind_context)
            .unwrap()
            .unwrap();
        let mut references = Vec::new();
        result
            .try_visit_pre_order(|plan| {
                if let LogicalOperator::CTERef(reference) = &plan.operator {
                    references.push(reference.cte_index);
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(references.len(), 2);
        assert_ne!(references[0], references[1]);
    }

    #[test]
    fn partition_preserves_opaque_producer_and_null_extended_occurrences() {
        let filtered = |table, value| {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                paro_planner::operator::Filter::new(
                    reference(9, table),
                    vec![equality(table, value)],
                ),
            ))
        };
        let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(JoinType::Left, filtered(1, 1), filtered(2, 2), vec![]),
        )));
        let (input, requirement, plan, _) = bind_requirement(
            owner(consumers),
            PlannerTransformation::CtePartitionedMaterialization,
        );
        assert!(requirement.occurrences[1].null_extended);
        let layout = plan.plan.output_layout();
        let mut holes = plan.group_holes;
        let result = requirement
            .partition(plan.plan, &mut holes, &input.bind_context)
            .unwrap()
            .unwrap();
        assert_eq!(result.output_layout(), layout);
        assert_eq!(holes.len(), 2);
        assert!(holes.values().all(|group| *group == requirement.producer));
        let mut owners = 0;
        result
            .try_visit_pre_order(|node| {
                if let LogicalOperator::MaterializedCTE(cte) = &node.operator {
                    owners += 1;
                    assert_eq!(cte.materialized, CTEMaterialize::Default);
                    assert!(matches!(cte.cte_query.operator, LogicalOperator::Filter(_)));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(owners, 2);
    }

    #[test]
    fn unrestricted_occurrence_prevents_producer_restriction() {
        let filtered = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
            paro_planner::operator::Filter::new(reference(9, 1), vec![equality(1, 1)]),
        ));
        let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(JoinType::Inner, filtered, reference(9, 2), vec![]),
        )));
        let (input, requirement, plan, _) =
            bind_requirement(owner(consumers), PlannerTransformation::CteFilterPushdown);
        let state = input.planner_state.read().unwrap();
        assert!(requirement
            .restrict_predicate_domain(plan.plan, &input.memo, &state)
            .unwrap()
            .is_none());
    }

    #[test]
    fn predicate_restriction_keeps_policy_and_inline_choice() {
        let consumer = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
            paro_planner::operator::Filter::new(reference(9, 1), vec![equality(1, 1)]),
        ));
        let (input, requirement, plan, _) =
            bind_requirement(owner(consumer), PlannerTransformation::CteFilterPushdown);
        let state = input.planner_state.read().unwrap();
        let (restricted, _) = requirement
            .restrict_predicate_domain(plan.plan, &input.memo, &state)
            .unwrap()
            .unwrap();
        let LogicalOperator::MaterializedCTE(cte) = &restricted.operator else {
            panic!("lost owner")
        };
        assert_eq!(cte.materialized, CTEMaterialize::Default);
        assert!(matches!(cte.cte_query.operator, LogicalOperator::Filter(_)));
        assert_eq!(
            plan.group_holes.values().copied().collect::<Vec<_>>(),
            vec![requirement.producer]
        );
    }

    #[test]
    fn domain_binding_is_stable_and_separates_consumer_fact_environments() {
        let consumer = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
            paro_planner::operator::Filter::new(reference(9, 1), vec![equality(1, 1)]),
        ));
        let (input, requirement, plan, _) =
            bind_requirement(owner(consumer), PlannerTransformation::CteFilterPushdown);
        let mut state = input.planner_state.write().unwrap();
        let (restricted, proof) = requirement
            .restrict_predicate_domain(plan.plan, &input.memo, &state)
            .unwrap()
            .unwrap();
        let original_layout = restricted.output_layout();
        let mut arena = paro_planner::plan::arena::LogicalPlanArena::default();
        let root = arena.import(restricted).unwrap();
        let mut bind_domain = |proof: &CteDomainProof| {
            let result = requirement
                .close_domain(arena.export(root).unwrap(), proof, &input.memo, &mut state)
                .unwrap();
            assert_eq!(result.output_layout(), original_layout);
            let LogicalOperator::MaterializedCTE(cte) = &result.operator else {
                panic!("lost owner")
            };
            let symbol = cte.cte_index;
            assert_ne!(symbol, 9);
            result
                .try_visit_pre_order(|node| {
                    if let LogicalOperator::CTERef(reference) = &node.operator {
                        assert_eq!(reference.cte_index, symbol);
                        assert_eq!(reference.table_index, 1);
                    }
                    Ok(())
                })
                .unwrap();
            symbol
        };
        let first = bind_domain(&proof);
        assert_eq!(
            bind_domain(&proof),
            first,
            "replay must not allocate a fresh group domain"
        );
        let mut different = proof.clone();
        different.predicates = vec![equality(0, 2)].into_boxed_slice();
        assert_ne!(
            bind_domain(&different),
            first,
            "restricted scans are not equivalent across domains"
        );
        assert_eq!(bind_domain(&proof), first);
    }

    #[test]
    fn splitting_a_shared_producer_requires_replay_evidence() {
        let (input, requirement, mut plan, _) =
            bind_requirement(owner(reference(9, 1)), PlannerTransformation::CteInline);
        let LogicalOperator::MaterializedCTE(cte) = &mut plan.plan.operator else {
            panic!("lost owner")
        };
        let LogicalOperator::BoundReference(producer) = &mut cte.cte_query.operator else {
            panic!("lost group hole")
        };
        let mut values = producer.facts.values().clone();
        values.can_replay = false;
        producer.facts = Arc::new(
            paro_planner::operator::bound_reference::BoundRelationFacts::new(
                values,
                producer.types().to_vec(),
            ),
        );
        let original_holes = plan.group_holes.clone();
        assert!(requirement
            .inline(plan.plan, &mut plan.group_holes, &input.bind_context)
            .unwrap()
            .is_none());
        assert_eq!(plan.group_holes, original_holes);
    }

    #[test]
    fn key_domain_retains_replay_source_group_without_copying_it() {
        let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                reference(9, 1),
                super::super::super::tests::test_base_get(2, 8, "domain", 10),
                vec![JoinCondition::new(
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(2, 0), LogicalType::Integer)
                            .into(),
                    ),
                    JoinComparisonType::Equal,
                )],
            ),
        )));
        let (input, requirement, plan, facts) =
            bind_requirement(owner(consumers), PlannerTransformation::CteDemandPushdown);
        let state = input.planner_state.read().unwrap();
        let key = requirement.occurrences[0]
            .keys
            .as_ref()
            .expect("proved key demand");
        let key_group = key.group;
        let original = plan.plan.output_layout();
        let mut holes = plan.group_holes;
        let (restricted, _) = requirement
            .restrict_key_domain(plan.plan, &mut holes, &input.memo, &state, &facts)
            .unwrap()
            .unwrap();
        assert_eq!(restricted.output_layout(), original);
        assert!(holes.values().any(|group| *group == requirement.producer));
        assert!(holes.values().any(|group| *group == key_group));
        let LogicalOperator::MaterializedCTE(cte) = &restricted.operator else {
            panic!("lost owner")
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &cte.cte_query.operator else {
            panic!("lost key domain")
        };
        assert_eq!(join.join_type, JoinType::Semi);
        assert!(matches!(
            join.left.operator,
            LogicalOperator::BoundReference(_)
        ));
    }

    #[test]
    fn restriction_proof_composition_is_idempotent_and_rollback_isolated() {
        let (input, mut requirement, _, _) =
            bind_requirement(owner(reference(9, 1)), PlannerTransformation::CteInline);
        let mut state = input.planner_state.write().unwrap();
        let first = CteDomainProof::new(9, [equality(0, 1)], std::iter::empty());
        let second = CteDomainProof::new(9, [equality(0, 2)], std::iter::empty());
        let initial = requirement.producer;
        let intermediate = input.root;
        let final_group = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap()
            .key
            .children[1];
        let savepoint = state.savepoint();
        state.cte_restrictions.push(CteRestriction {
            producer: intermediate,
            input: initial,
            proof: first.clone(),
        });
        state.cte_restrictions.push(CteRestriction {
            producer: final_group,
            input: intermediate,
            proof: second.clone(),
        });
        requirement.producer = final_group;
        assert!(requirement.domain_is_proved(&first, &input.memo, &state));
        assert!(requirement.domain_is_proved(&second, &input.memo, &state));
        state.rollback_to(savepoint).unwrap();
        assert!(!requirement.domain_is_proved(&first, &input.memo, &state));
    }

    #[test]
    fn reordered_consumer_domains_do_not_create_another_producer_restriction() {
        use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
        let conjunction = |kind, children| {
            Expression::Conjunction(ConjunctionExpression::new(kind, children).into())
        };
        let a = equality(0, 2001);
        let b = equality(0, 2002);
        let c = equality(0, 2003);
        let first = vec![conjunction(
            ConjunctionType::Or,
            vec![
                a.clone(),
                conjunction(ConjunctionType::Or, vec![b.clone(), c.clone()]),
            ],
        )];
        let reordered = vec![conjunction(
            ConjunctionType::Or,
            vec![c, a.clone(), b.clone(), a.clone()],
        )];
        assert!(predicate_domains_equal(&first, &reordered));
        assert_eq!(
            CteDomainProof::new(9, first.clone(), std::iter::empty()).fingerprint,
            CteDomainProof::new(9, reordered.clone(), std::iter::empty()).fingerprint,
            "domain fingerprints must follow the same flattening/idempotence contract as proof equality"
        );
        assert!(!predicate_domains_equal(
            &first,
            &[conjunction(ConjunctionType::And, vec![a, b])]
        ));
    }

    #[test]
    fn composed_domain_identity_is_commutative_and_idempotent() {
        let (input, requirement, _, _) =
            bind_requirement(owner(reference(9, 1)), PlannerTransformation::CteInline);
        let mut state = input.planner_state.write().unwrap();
        let proof = |value| {
            CteDomainProof::new(
                requirement.definition,
                [equality(0, value)],
                std::iter::empty(),
            )
        };
        let a = proof(1);
        let b = proof(2);
        let mut after_a = requirement.clone();
        after_a.cte_index = requirement.domain_symbol(&a, &input.memo, &mut state);
        let mut after_b = requirement.clone();
        after_b.cte_index = requirement.domain_symbol(&b, &input.memo, &mut state);
        assert_ne!(after_a.cte_index, after_b.cte_index);
        let ab = after_a.domain_symbol(&b, &input.memo, &mut state);
        let ba = after_b.domain_symbol(&a, &input.memo, &mut state);
        assert_eq!(ab, ba);
        after_a.cte_index = ab;
        assert_eq!(after_a.domain_symbol(&a, &input.memo, &mut state), ab);
        assert_eq!(after_a.domain_symbol(&b, &input.memo, &mut state), ab);
        assert_eq!(state.cte_bindings.len(), 3);
    }

    #[test]
    fn native_inline_retains_one_producer_group_for_distinct_occurrences() {
        let bind = BindContext::new();
        let producer = super::super::super::tests::test_base_get(0, 7, "source", 10);
        let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Left,
                reference(9, 1),
                reference(9, 2),
                vec![JoinCondition::new(
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(2, 0), LogicalType::Integer)
                            .into(),
                    ),
                    JoinComparisonType::Equal,
                )],
            ),
        )));
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                9,
                "shared".into(),
                vec!["key".into()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                producer,
                consumers,
            )));
        let mut input = MemoBuilder::build(plan, bind, SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let bindings = matching::scoped_pattern_bindings(
            PlannerTransformation::CteInline,
            input.root,
            root,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
        assert_eq!(bindings.bindings.len(), 1);
        let binding = &bindings.bindings[0];
        let requirement = CteRequirement::from_binding(binding, &input.memo, &state).unwrap();
        assert_eq!(requirement.occurrences.len(), 2);
        assert!(!requirement.occurrences[0].null_extended);
        assert!(requirement.occurrences[1].null_extended);
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let instantiated = semantic_plan::instantiate_bound_plan_with_group_holes(
            context.memo(),
            &state,
            &binding.root,
            Some(&facts),
        )
        .unwrap()
        .unwrap();
        let original_layout = instantiated.plan.output_layout();
        let mut holes = instantiated.group_holes;
        let inlined = requirement
            .inline(instantiated.plan, &mut holes, &state.bind_context)
            .unwrap()
            .unwrap();
        assert_eq!(inlined.output_layout(), original_layout);
        assert_eq!(holes.len(), 2);
        assert!(holes.values().all(|group| *group == requirement.producer));
        assert!(inlined
            .children()
            .iter()
            .all(|child| matches!(child.operator, LogicalOperator::Projection(_))));
    }

    #[test]
    fn production_inline_uses_native_shell_without_owned_settlement() {
        let input_plan = owner(OwnedLogicalPlan::synthetic(LogicalOperator::Join(
            Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                reference(9, 1),
                reference(9, 2),
                vec![],
            )),
        )));
        let mut input = MemoBuilder::build(
            input_plan,
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let planner_state = input.planner_state.clone();
        planner_state.write().unwrap().session = Some(
            paro_context::TestStatementContextBuilder::minimal().build(),
        );
        let binding = {
            let state = planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let bindings = matching::scoped_pattern_bindings(
                PlannerTransformation::CteInline,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap();
            assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
            assert_eq!(bindings.bindings.len(), 1);
            bindings.bindings[0].clone()
        };
        let before = planner_state.read().unwrap().staging_arena.len();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::CteInline,
            planner_state: planner_state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            planner_state.read().unwrap().staging_arena.len(),
            before,
            "native CTE inline must not settle through the owned arena"
        );
        let state = planner_state.read().unwrap();
        assert!(matches!(
            state.payloads.logical[outputs[0].payload.index()]
                .semantic_template
                .operator,
            LogicalOperator::Join(_)
        ));
    }

    #[test]
    fn native_partition_shell_preserves_each_discriminator_and_layout() {
        let mut input = MemoBuilder::build(
            two_discriminator_owner(),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let (binding, requirement, facts) = {
            let state = input.planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let bindings = matching::scoped_pattern_bindings(
                PlannerTransformation::CtePartitionedMaterialization,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap();
            assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
            assert_eq!(bindings.bindings.len(), 1);
            let binding = bindings.bindings[0].clone();
            let requirement = CteRequirement::from_binding(&binding, &input.memo, &state).unwrap();
            let facts = {
                let mut context = TransformContext::new(&mut input.memo, input.root);
                boundary::BoundarySnapshot::read(
                    &mut context,
                    &state,
                    &binding.root,
                    BudgetDimension::RuleWorkPerGroup,
                )
                .unwrap()
                .unwrap()
            };
            (binding, requirement, facts)
        };
        let (shell, layouts) = {
            let state = input.planner_state.read().unwrap();
            NativeShell::from_pattern_with_layouts(
                &input.memo,
                &state,
                &binding.root,
                &facts,
            )
            .unwrap()
            .unwrap()
        };
        let original_layout = shell.root_layout().unwrap();
        let mut state = input.planner_state.write().unwrap();
        let alternatives = requirement
            .native_partitions(shell, &layouts, &mut state)
            .unwrap();
        assert_eq!(alternatives.len(), 2);
        assert_eq!(state.cte_partition_labels.len(), 2);
        for alternative in alternatives {
            assert_eq!(alternative.root_layout().unwrap(), original_layout);
            let wrappers = alternative
                .nodes
                .iter()
                .filter_map(|node| match &node.operator {
                    LogicalOperator::MaterializedCTE(cte) => Some(cte.cte_index),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(wrappers.len(), 2);
            assert!(wrappers.iter().all(|symbol| *symbol != 9));
            assert!(alternative.nodes.iter().all(|node| {
                !matches!(&node.operator, LogicalOperator::CTERef(reference) if reference.cte_index == 9)
            }));
        }
    }

    #[test]
    fn production_partition_uses_native_shell_for_all_discriminators() {
        let mut input = MemoBuilder::build(
            two_discriminator_owner(),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let planner_state = input.planner_state.clone();
        planner_state.write().unwrap().session = Some(
            paro_context::TestStatementContextBuilder::minimal().build(),
        );
        let binding = {
            let state = planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let bindings = matching::scoped_pattern_bindings(
                PlannerTransformation::CtePartitionedMaterialization,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap();
            assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
            assert_eq!(bindings.bindings.len(), 1);
            bindings.bindings[0].clone()
        };
        let before_arena = planner_state.read().unwrap().staging_arena.len();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::CtePartitionedMaterialization,
            planner_state: planner_state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(planner_state.read().unwrap().staging_arena.len(), before_arena);
        assert_eq!(planner_state.read().unwrap().cte_partition_labels.len(), 2);
    }

    #[test]
    fn production_filter_domain_settles_native_facts_and_records_proof() {
        let mut input = MemoBuilder::build(
            owner(OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                paro_planner::operator::Filter::new(reference(9, 1), vec![equality(1, 7)]),
            ))),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let planner_state = input.planner_state.clone();
        planner_state.write().unwrap().session = Some(
            paro_context::TestStatementContextBuilder::minimal().build(),
        );
        let binding = {
            let state = planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let bindings = matching::scoped_pattern_bindings(
                PlannerTransformation::CteFilterPushdown,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap();
            assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
            assert_eq!(bindings.bindings.len(), 1);
            bindings.bindings[0].clone()
        };
        let before = planner_state.read().unwrap().staging_arena.len();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::CteFilterPushdown,
            planner_state: planner_state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        // Native construction no longer skips lexical producer settlement.
        // Its bounded node batch uses the same arena and node-fact cache,
        // without reconstructing any opaque Memo descendant as owned IR.
        assert!(planner_state.read().unwrap().settlement_cache.misses > 0);
        assert_eq!(planner_state.read().unwrap().cte_restrictions.len(), 1);
        assert!(matches!(
            planner_state.read().unwrap().payloads.logical[outputs[0].payload.index()]
                .semantic_template
                .operator,
            LogicalOperator::MaterializedCTE(_)
        ));
        context.rollback().unwrap();
        let state = planner_state.read().unwrap();
        assert_eq!(state.staging_arena.len(), before);
        assert!(state.cte_bindings.is_empty());
        assert!(state.cte_restrictions.is_empty());
    }

    #[test]
    fn production_key_domain_uses_native_shell_and_records_proof() {
        let consumers = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                reference(9, 1),
                super::super::super::tests::test_base_get(2, 8, "domain", 10),
                vec![JoinCondition::new(
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(2, 0), LogicalType::Integer)
                            .into(),
                    ),
                    JoinComparisonType::Equal,
                )],
            ),
        )));
        let mut input = MemoBuilder::build(
            owner(consumers),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let planner_state = input.planner_state.clone();
        planner_state.write().unwrap().session = Some(
            paro_context::TestStatementContextBuilder::minimal().build(),
        );
        let binding = {
            let state = planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let bindings = matching::scoped_pattern_bindings(
                PlannerTransformation::CteDemandPushdown,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap();
            assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
            assert_eq!(bindings.bindings.len(), 1);
            bindings.bindings[0].clone()
        };
        let before = planner_state.read().unwrap().staging_arena.len();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::CteDemandPushdown,
            planner_state: planner_state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(planner_state.read().unwrap().staging_arena.len(), before);
        assert_eq!(planner_state.read().unwrap().cte_restrictions.len(), 1);
        assert!(matches!(
            planner_state.read().unwrap().payloads.logical[outputs[0].payload.index()]
                .semantic_template
                .operator,
            LogicalOperator::MaterializedCTE(_)
        ));
    }
}

#[derive(Debug, Clone)]
pub(super) struct CteRequirement {
    pub(super) owner: GroupId,
    pub(super) producer: GroupId,
    pub(super) base_producer: GroupId,
    pub(super) cte_index: usize,
    pub(super) definition: usize,
    pub(super) policy: CTEMaterialize,
    pub(super) sharing_owner: Option<Fingerprint>,
    pub(super) occurrences: Box<[CteOccurrenceRequirement]>,
}

impl CteRequirement {
    /// Inline a CTE directly in the native shell when the binding contains a
    /// replayable producer group.  The consumer shell is already a complete
    /// Memo-shaped DAG; exporting it to an owned plan merely to replace each
    /// CTERef and then importing it again is the bridge this path is intended
    /// to remove.
    ///
    /// This adapter is deliberately conservative.  It only accepts an
    /// opaque producer `MemoGroup`, validates every occurrence by its exact
    /// binding path, and reconstructs the producer projection from the
    /// MaterializedCTE output-column contract.  Unsupported shapes return
    /// `None`, allowing the semantic peer to preserve completeness.
    pub(super) fn native_inline(
        &self,
        mut shell: NativeShell,
        layouts: &[paro_planner::operator::LogicalOutputLayout],
        state: &PlannerTransformState,
    ) -> Result<Option<NativeShell>> {
        if self.policy == CTEMaterialize::Materialized {
            return Ok(None);
        }
        let root = shell.root;
        let LogicalOperator::MaterializedCTE(owner) = shell
            .nodes
            .get(root)
            .ok_or_else(|| paro_error::internal("native CTE inline lost its owner node"))?
            .operator
            .clone()
        else {
            return Err(paro_error::internal(
                "native CTE inline lost its owner shell",
            ));
        };
        if owner.cte_index != self.cte_index || self.occurrences.is_empty() {
            return Ok(None);
        }
        let NativeChild::MemoGroup {
            group: producer_group,
            reference: producer_reference,
            ..
        } = owner.cte_query.clone()
        else {
            // A producer expression rather than a group edge would require
            // importing/owning its descendants.  Keep the semantic peer for
            // that case until a separate native producer contract exists.
            return Ok(None);
        };
        if state
            .session
            .as_ref()
            .is_some_and(|session| session.cancellation.is_cancelled())
        {
            return Ok(None);
        }
        if !producer_reference.facts.can_replay
            || producer_reference.facts.contains_control_region
            || producer_group != self.producer
        {
            return Ok(None);
        }
        if producer_reference.bindings.len() != producer_reference.types().len() {
            return Err(paro_error::internal(
                "native CTE inline producer has an invalid typed contract",
            ));
        }
        let producer_edge = owner.cte_query.clone();
        let producer_layout = match &producer_edge {
            NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => layout,
            NativeChild::Node(index) => layouts.get(*index).ok_or_else(|| {
                paro_error::internal("native CTE inline producer layout is missing")
            })?,
        };
        if producer_layout.bindings() != producer_reference.bindings.as_slice()
            || producer_layout.types() != producer_reference.types()
        {
            return Err(paro_error::internal(
                "native CTE inline producer layout disagrees with its facts",
            ));
        }

        let consumer_root = match owner.child {
            NativeChild::Node(index) => index,
            NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => return Ok(None),
        };
        let original_layout = layouts.get(root).cloned().ok_or_else(|| {
            paro_error::internal("native CTE inline owner layout is missing")
        })?;
        let mut nodes = shell.nodes.into_vec();
        let mut seen = 0usize;
        for occurrence in &self.occurrences {
            let Some(index) = native_path_node(&nodes, consumer_root, &occurrence.path) else {
                return Ok(None);
            };
            let reference = match nodes.get(index).map(|node| &node.operator) {
                Some(LogicalOperator::CTERef(reference)) => reference.clone(),
                _ => return Ok(None),
            };
            if reference.cte_index != self.cte_index
                || reference.table_index != occurrence.table_index
                || reference.column_types.as_slice() != occurrence.output.types()
            {
                return Ok(None);
            }
            let Some(expressions) = cte_projection_expressions(
                &owner,
                producer_layout,
                &reference,
            )?
            else {
                return Ok(None);
            };
            let child = clone_native_memo_group(&producer_edge, state)?;
            let projection = Projection {
                table_index: reference.table_index,
                expressions,
                visible_names: reference.column_names.clone(),
                visible_count: reference.column_names.len(),
                visible_qualifier: Some(reference.relation_alias.clone()),
                child,
                returned_types: reference.column_types.clone(),
            };
            let node = nodes.get_mut(index).ok_or_else(|| {
                paro_error::internal("native CTE inline occurrence node disappeared")
            })?;
            node.operator = LogicalOperator::Projection(projection);
            node.source_proofs = Box::new([]);
            seen += 1;
        }
        if seen != self.occurrences.len() {
            return Err(paro_error::internal(
                "native CTE inline did not consume its complete occurrence requirement",
            ));
        }

        // The owner wrapper is intentionally removed from the reachable
        // shell.  The consumer is the transformed root; compaction drops the
        // now-unreachable owner and producer edge while retaining every
        // opaque Memo group referenced by the new projections.
        shell = compact_native_shell(NativeShell {
            nodes: nodes.into_boxed_slice(),
            root: consumer_root,
        })?;
        if shell.root_layout()? != original_layout {
            return Err(paro_error::internal(
                "native CTE inline changed the owner output layout",
            ));
        }
        if native_shell_contains_control_boundary(&shell) {
            return Ok(None);
        }
        Ok(Some(shell))
    }

    /// Push a common consumer predicate into the producer while retaining a
    /// distinct lexical domain symbol.  The proof is returned to the caller
    /// so publication can journal the exact restricted producer/input pair;
    /// it is not inferred from the presence of the Filter node.
    pub(super) fn native_filter_domain(
        &self,
        shell: NativeShell,
        layouts: &[paro_planner::operator::LogicalOutputLayout],
        memo: &Memo,
        state: &mut PlannerTransformState,
    ) -> Result<Option<(NativeShell, CteDomainProof)>> {
        let root = shell.root;
        let LogicalOperator::MaterializedCTE(owner) = shell
            .nodes
            .get(root)
            .ok_or_else(|| paro_error::internal("native CTE filter lost its owner node"))?
            .operator
            .clone()
        else {
            return Err(paro_error::internal(
                "native CTE filter lost its owner shell",
            ));
        };
        if owner.cte_index != self.cte_index {
            return Ok(None);
        }
        let NativeChild::MemoGroup {
            group: producer_group,
            reference: producer_reference,
            ..
        } = owner.cte_query.clone()
        else {
            return Ok(None);
        };
        if producer_group != self.producer
            || !producer_reference.facts.can_replay
            || producer_reference.facts.contains_control_region
        {
            return Ok(None);
        }
        let Some(references) = self
            .occurrences
            .iter()
            .map(|occurrence| {
                Some(crate::cte::predicate_domain::FilteredCTERef {
                    old_bindings: occurrence.output.bindings().to_vec(),
                    filters: occurrence.predicates.as_ref()?.to_vec(),
                })
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(predicates) = crate::cte::predicate_domain::derive_producer_predicates(
            references,
            &producer_reference.bindings,
        )
        else {
            return Ok(None);
        };
        let proof = CteDomainProof::new(self.definition, predicates.clone(), std::iter::empty());
        if self.domain_is_proved(&proof, memo, state) {
            return Ok(None);
        }
        let symbol = self.domain_symbol(&proof, memo, state);
        let NativeChild::Node(consumer_root) = owner.child else {
            return Ok(None);
        };
        let original_layout = layouts.get(root).cloned().ok_or_else(|| {
            paro_error::internal("native CTE filter owner layout is missing")
        })?;
        let producer_edge = owner.cte_query.clone();
        let mut nodes = shell.nodes.into_vec();
        for occurrence in &self.occurrences {
            let Some(index) = native_path_node(&nodes, consumer_root, &occurrence.path) else {
                return Ok(None);
            };
            let LogicalOperator::CTERef(reference) = &mut nodes[index].operator else {
                return Ok(None);
            };
            if reference.cte_index != self.cte_index
                || reference.table_index != occurrence.table_index
            {
                return Ok(None);
            }
            reference.cte_index = symbol;
        }
        let producer = clone_native_memo_group(&producer_edge, state)?;
        let filter_index = nodes.len();
        let producer_stats = native_shell_child_stats(&nodes, &producer);
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: producer_stats,
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                expressions: predicates,
                child: producer,
                projection_map: paro_planner::operator::ProjectionMap::all(),
            }),
            source_proofs: Box::new([]),
        });
        let wrapper_index = nodes.len();
        let consumer = NativeChild::Node(consumer_root);
        let consumer_stats = native_shell_child_stats(&nodes, &consumer);
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: consumer_stats,
            operator: LogicalOperator::MaterializedCTE(MaterializedCTE {
                cte_index: symbol,
                cte_name: owner.cte_name,
                column_names: owner.column_names,
                column_types: owner.column_types,
                output_columns: owner.output_columns,
                materialized: owner.materialized,
                ref_count: owner.ref_count,
                cte_query: NativeChild::Node(filter_index),
                child: consumer,
            }),
            source_proofs: Box::new([]),
        });
        let (result, result_layout) = compact_native_shell_with_layout(NativeShell {
            nodes: nodes.into_boxed_slice(),
            root: wrapper_index,
        })?;
        if result_layout != original_layout {
            return Err(paro_error::internal(
                "native CTE filter changed the owner output layout",
            ));
        }
        if native_cte_shell_contains_control_boundary(&result) {
            return Ok(None);
        }
        Ok(Some((result, proof)))
    }

    /// Restrict a producer to the union of consumer join keys.  The key
    /// domain is built from direct Memo group edges and native Projection /
    /// UNION / Semi-Join nodes, matching the owned implementation's logical
    /// contract without creating an owned subtree or a second arena.
    pub(super) fn native_key_domain(
        &self,
        shell: NativeShell,
        layouts: &[paro_planner::operator::LogicalOutputLayout],
        memo: &Memo,
        state: &mut PlannerTransformState,
        facts: &boundary::BoundarySnapshot,
    ) -> Result<Option<(NativeShell, CteDomainProof)>> {
        use paro_planner::operator::{BoundReference, ComparisonJoin, JoinCondition, SetOperation};

        let root = shell.root;
        let LogicalOperator::MaterializedCTE(owner) = shell
            .nodes
            .get(root)
            .ok_or_else(|| paro_error::internal("native CTE demand lost its owner node"))?
            .operator
            .clone()
        else {
            return Err(paro_error::internal(
                "native CTE demand lost its owner shell",
            ));
        };
        if owner.cte_index != self.cte_index {
            return Ok(None);
        }
        let NativeChild::MemoGroup {
            group: producer_group,
            reference: producer_reference,
            layout: producer_layout,
            ..
        } = owner.cte_query.clone()
        else {
            return Ok(None);
        };
        if producer_group != self.producer
            || !producer_reference.facts.can_replay
            || producer_reference.facts.contains_control_region
        {
            return Ok(None);
        }
        let Some(keys) = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.keys.clone())
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(first) = keys.first() else {
            return Ok(None);
        };
        let ordinals = first.ordinals.clone();
        if keys.iter().any(|key| key.ordinals != ordinals) {
            return Ok(None);
        }
        let proof = CteDomainProof::new(self.definition, std::iter::empty(), keys.clone());
        if self.domain_is_proved(&proof, memo, state) {
            return Ok(None);
        }
        let NativeChild::Node(consumer_root) = owner.child.clone() else {
            return Ok(None);
        };
        let original_layout = layouts.get(root).cloned().ok_or_else(|| {
            paro_error::internal("native CTE demand owner layout is missing")
        })?;
        let mut nodes = shell.nodes.to_vec();
        let mut domain: Option<NativeChild> = None;
        let mut domain_layout = None;
        for key in keys {
            let group = memo.canonical_group(key.group);
            let transport = facts.transport(memo, state, group, &key.layout)?;
            let id = state.bind_context.next_plan_id();
            let reference_id = paro_planner::operator::BoundReferenceId::group_hole(id.0);
            let reference = BoundReference::new(
                reference_id,
                key.layout.bindings().to_vec(),
                key.layout.types().to_vec(),
            )
            .with_facts(transport)?;
            let stats = NodeStats {
                estimated_cardinality: facts.cardinality(memo, group),
                unique_keys: reference.facts.unique_keys.clone(),
                ..Default::default()
            };
            let input = NativeChild::MemoGroup {
                group,
                id,
                stats: stats.clone(),
                layout: key.layout.as_ref().clone(),
                names: (0..key.layout.bindings().len())
                    .map(|index| format!("__cte_domain_{index}"))
                    .collect::<Vec<_>>()
                    .into(),
                reference,
            };
            let expressions = key.expressions.to_vec();
            // The operator's table index is the binding namespace of the
            // projection output. Generate it once so the layout and payload
            // use the same identity.
            let table_index = state.bind_context.generate_table_index();
            let projection_layout = paro_planner::operator::LogicalOutputLayout::new(
                expressions.iter().map(Expression::return_type).collect(),
                (0..expressions.len())
                    .map(|ordinal| ColumnBinding::new(table_index, ordinal))
                    .collect(),
            );
            let projection_index = nodes.len();
            nodes.push(NativeNode {
                id: state.bind_context.next_plan_id(),
                stats,
                operator: LogicalOperator::Projection(Projection {
                    table_index,
                    expressions,
                    visible_names: (0..key.ordinals.len())
                        .map(|ordinal| format!("__cte_key_{ordinal}"))
                        .collect(),
                    visible_count: 0,
                    visible_qualifier: None,
                    child: input,
                    returned_types: projection_layout.types().to_vec(),
                }),
                source_proofs: Box::new([]),
            });
            let projected = NativeChild::Node(projection_index);
            if let Some(previous) = domain {
                let types = projection_layout.types().to_vec();
                let table_index = state.bind_context.generate_table_index();
                let union_index = nodes.len();
                nodes.push(NativeNode {
                    id: state.bind_context.next_plan_id(),
                    stats: native_shell_child_stats(&nodes, &previous),
                    operator: LogicalOperator::SetOperation(SetOperation {
                        table_index,
                        column_count: types.len(),
                        left: previous,
                        right: projected,
                        setop_type: SetOpType::Union,
                        setop_all: true,
                        allow_out_of_order: true,
                        types: types.clone(),
                    }),
                    source_proofs: Box::new([]),
                });
                domain = Some(NativeChild::Node(union_index));
                domain_layout = Some(paro_planner::operator::LogicalOutputLayout::new(
                    types,
                    (0..key.ordinals.len())
                        .map(|ordinal| ColumnBinding::new(table_index, ordinal))
                        .collect(),
                ));
            } else {
                domain = Some(projected);
                domain_layout = Some(projection_layout);
            }
        }
        let Some(domain) = domain else {
            return Ok(None);
        };
        let Some(domain_layout) = domain_layout else {
            return Ok(None);
        };
        let conditions = ordinals
            .iter()
            .enumerate()
            .map(|(domain_ordinal, producer_ordinal)| {
                let binding = *producer_layout
                    .bindings()
                    .get(*producer_ordinal)
                    .ok_or_else(|| paro_error::internal("native CTE key width changed"))?;
                let ty = producer_layout
                    .types()
                    .get(*producer_ordinal)
                    .cloned()
                    .ok_or_else(|| paro_error::internal("native CTE key type disappeared"))?;
                let domain_binding = *domain_layout
                    .bindings()
                    .get(domain_ordinal)
                    .ok_or_else(|| paro_error::internal("native CTE domain width changed"))?;
                if domain_layout.types().get(domain_ordinal) != Some(&ty) {
                    return Err(paro_error::internal("native CTE key type changed"));
                }
                Ok(JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(binding, ty.clone()).into()),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(domain_binding, ty).into(),
                    ),
                    JoinComparisonType::Equal,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let producer = clone_native_memo_group(&owner.cte_query, state)?;
        let join_index = nodes.len();
        let (left_projection_map, right_projection_map) =
            paro_planner::operator::default_join_projections(JoinType::Semi);
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: native_shell_child_stats(&nodes, &producer),
            operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin {
                join_type: JoinType::Semi,
                anti_join_mode: paro_planner::operator::AntiJoinMode::Regular,
                left: producer,
                right: domain,
                conditions,
                mark_index: None,
                mark_semantics: paro_planner::operator::MarkJoinSemantics::for_join_type(
                    JoinType::Semi,
                ),
                duplicate_eliminated_columns: Vec::new(),
                delim_flipped: false,
                build_side_constraint: Default::default(),
                left_projection_map,
                right_projection_map,
            })),
            source_proofs: Box::new([]),
        });
        let symbol = self.domain_symbol(&proof, memo, state);
        for occurrence in &self.occurrences {
            let Some(index) = native_path_node(&nodes, consumer_root, &occurrence.path) else {
                return Ok(None);
            };
            let LogicalOperator::CTERef(reference) = &mut nodes[index].operator else {
                return Ok(None);
            };
            if reference.cte_index != self.cte_index
                || reference.table_index != occurrence.table_index
            {
                return Ok(None);
            }
            reference.cte_index = symbol;
        }
        let consumer = NativeChild::Node(consumer_root);
        let wrapper_index = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: native_shell_child_stats(&nodes, &consumer),
            operator: LogicalOperator::MaterializedCTE(MaterializedCTE {
                cte_index: symbol,
                cte_name: owner.cte_name,
                column_names: owner.column_names,
                column_types: owner.column_types,
                output_columns: owner.output_columns,
                materialized: owner.materialized,
                ref_count: owner.ref_count,
                cte_query: NativeChild::Node(join_index),
                child: consumer,
            }),
            source_proofs: Box::new([]),
        });
        let (result, result_layout) = compact_native_shell_with_layout(NativeShell {
            nodes: nodes.into_boxed_slice(),
            root: wrapper_index,
        })?;
        if result_layout != original_layout {
            return Err(paro_error::internal(
                "native CTE demand changed the owner output layout",
            ));
        }
        if result
            .nodes
            .iter()
            .any(|node| matches!(node.operator, LogicalOperator::RecursiveCTE(_)))
        {
            return Ok(None);
        }
        Ok(Some((result, proof)))
    }

    /// Produce partitioned materialization alternatives without first
    /// exporting the owner/consumer DAG to the owned-plan arena.  Each
    /// alternative keeps the same opaque producer group and changes only the
    /// exact CTERef paths belonging to one discriminator.  The labels are
    /// session identities, not a frontier ordinal, so replaying a binding
    /// reuses the same symbols.
    pub(super) fn native_partitions(
        &self,
        shell: NativeShell,
        layouts: &[paro_planner::operator::LogicalOutputLayout],
        state: &mut PlannerTransformState,
    ) -> Result<Vec<NativeShell>> {
        if self.policy != CTEMaterialize::Default || self.occurrences.len() < 2 {
            return Ok(Vec::new());
        }
        let root = shell.root;
        let LogicalOperator::MaterializedCTE(owner) = shell
            .nodes
            .get(root)
            .ok_or_else(|| paro_error::internal("native CTE partition lost its owner node"))?
            .operator
            .clone()
        else {
            return Err(paro_error::internal(
                "native CTE partition lost its owner shell",
            ));
        };
        if owner.cte_index != self.cte_index {
            return Ok(Vec::new());
        }
        let NativeChild::MemoGroup {
            group: producer_group,
            reference: producer_reference,
            ..
        } = owner.cte_query.clone()
        else {
            return Ok(Vec::new());
        };
        if producer_group != self.producer
            || !producer_reference.facts.can_replay
            || producer_reference.facts.contains_control_region
        {
            return Ok(Vec::new());
        }
        let NativeChild::Node(consumer_root) = owner.child else {
            return Ok(Vec::new());
        };
        let original_layout = layouts.get(root).cloned().ok_or_else(|| {
            paro_error::internal("native CTE partition owner layout is missing")
        })?;
        let candidates = self
            .occurrences
            .iter()
            .map(occurrence_equalities)
            .collect::<Vec<_>>();
        let Some(first) = candidates.first() else {
            return Ok(Vec::new());
        };
        let ordinals = first
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .filter(|ordinal| {
                candidates
                    .iter()
                    .all(|values| values.iter().any(|(candidate, _)| candidate == ordinal))
            })
            .collect::<BTreeSet<_>>();
        let producer_bindings = producer_reference.bindings.clone();
        let producer_edge = owner.cte_query.clone();
        let mut results = Vec::new();
        let mut partitions_seen = BTreeSet::new();
        for ordinal in ordinals {
            let mut values = Vec::<Expression>::new();
            let mut partitions = Vec::<Vec<&CteOccurrenceRequirement>>::new();
            for (occurrence, values_for_occurrence) in self.occurrences.iter().zip(&candidates) {
                let Some((_, value)) = values_for_occurrence
                    .iter()
                    .find(|(candidate, _)| *candidate == ordinal)
                else {
                    return Ok(results);
                };
                let partition = if let Some(partition) = values
                    .iter()
                    .position(|candidate| candidate.equals(value))
                {
                    partition
                } else {
                    values.push(value.clone());
                    partitions.push(Vec::new());
                    partitions.len() - 1
                };
                partitions[partition].push(occurrence);
            }
            if partitions.len() < 2 {
                continue;
            }
            let mut assignment = partitions
                .iter()
                .enumerate()
                .flat_map(|(partition, occurrences)| {
                    occurrences
                        .iter()
                        .map(move |occurrence| (occurrence.path.clone(), partition))
                })
                .collect::<Vec<_>>();
            assignment.sort_unstable();
            let assignment = assignment.into_boxed_slice();
            if !partitions_seen.insert(assignment.clone()) {
                continue;
            }
            let indices = state
                .cte_partition_labels
                .entry((self.cte_index, assignment))
                .or_insert_with(|| {
                    partitions
                        .iter()
                        .map(|_| state.bind_context.generate_table_index())
                        .collect()
                })
                .clone();
            let mut domains = Vec::with_capacity(partitions.len());
            for occurrences in &partitions {
                let Some(predicates) = crate::cte::predicate_domain::derive_producer_predicates(
                    occurrences
                        .iter()
                        .map(|occurrence| crate::cte::predicate_domain::FilteredCTERef {
                            old_bindings: occurrence.output.bindings().to_vec(),
                            filters: occurrence
                                .predicates
                                .as_deref()
                                .unwrap_or_default()
                                .to_vec(),
                        })
                        .collect(),
                    &producer_bindings,
                ) else {
                    domains.clear();
                    break;
                };
                domains.push(predicates);
            }
            if domains.len() != partitions.len() {
                continue;
            }

            let mut nodes = shell.nodes.to_vec();
            let mut consumer = NativeChild::Node(consumer_root);
            for (partition, predicates) in domains.into_iter().enumerate().rev() {
                for occurrence in &partitions[partition] {
                    let Some(index) = native_path_node(&nodes, consumer_root, &occurrence.path)
                    else {
                        return Err(paro_error::internal(
                            "native CTE partition occurrence path disappeared",
                        ));
                    };
                    let LogicalOperator::CTERef(reference) = &mut nodes[index].operator else {
                        return Err(paro_error::internal(
                            "native CTE partition occurrence changed operator",
                        ));
                    };
                    if reference.cte_index != self.cte_index
                        || reference.table_index != occurrence.table_index
                    {
                        return Err(paro_error::internal(
                            "native CTE partition occurrence changed binding",
                        ));
                    }
                    reference.cte_index = indices[partition];
                }
                let producer = clone_native_memo_group(&producer_edge, state)?;
                let filter_index = nodes.len();
                let producer_stats = native_shell_child_stats(&nodes, &producer);
                nodes.push(NativeNode {
                    id: state.bind_context.next_plan_id(),
                    stats: producer_stats,
                    operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                        expressions: predicates,
                        child: producer,
                        projection_map: paro_planner::operator::ProjectionMap::all(),
                    }),
                    source_proofs: Box::new([]),
                });
                let wrapper_index = nodes.len();
                let consumer_stats = native_shell_child_stats(&nodes, &consumer);
                nodes.push(NativeNode {
                    id: state.bind_context.next_plan_id(),
                    stats: consumer_stats,
                    operator: LogicalOperator::MaterializedCTE(MaterializedCTE {
                        cte_index: indices[partition],
                        cte_name: format!("{}$domain{}", owner.cte_name, partition + 1),
                        column_names: owner.column_names.clone(),
                        column_types: owner.column_types.clone(),
                        output_columns: owner.output_columns.clone(),
                        materialized: owner.materialized,
                        ref_count: partitions[partition].len(),
                        cte_query: NativeChild::Node(filter_index),
                        child: consumer,
                    }),
                    source_proofs: Box::new([]),
                });
                consumer = NativeChild::Node(wrapper_index);
            }
            self.validate_native_occurrence_null_extension(&nodes, consumer_root, &indices)?;
            let NativeChild::Node(result_root) = consumer else {
                unreachable!("native CTE partition always creates a wrapper")
            };
            let (result, result_layout) = compact_native_shell_with_layout(NativeShell {
                nodes: nodes.into_boxed_slice(),
                root: result_root,
            })?;
            if result_layout != original_layout {
                return Err(paro_error::internal(
                    "native CTE partition changed the owner output layout",
                ));
            }
            if native_cte_shell_contains_control_boundary(&result) {
                continue;
            }
            results.push(result);
        }
        Ok(results)
    }

    fn validate_native_occurrence_null_extension(
        &self,
        nodes: &[NativeNode],
        root: usize,
        ctes: &[usize],
    ) -> Result<()> {
        let mut pending = vec![(root, false, Vec::new())];
        let mut seen = 0usize;
        while let Some((index, null_extended, path)) = pending.pop() {
            let node = nodes.get(index).ok_or_else(|| {
                paro_error::internal("native CTE partition references an unknown consumer")
            })?;
            if let LogicalOperator::CTERef(reference) = &node.operator {
                if ctes.contains(&reference.cte_index) {
                    let occurrence = self
                        .occurrences
                        .iter()
                        .find(|occurrence| occurrence.path.as_ref() == path)
                        .ok_or_else(|| {
                            paro_error::internal("native CTE partition introduced an occurrence")
                        })?;
                    if occurrence.null_extended != null_extended
                        || occurrence.table_index != reference.table_index
                    {
                        return Err(paro_error::internal(
                            "native CTE partition changed null-extension ownership",
                        ));
                    }
                    seen += 1;
                }
            }
            let mut children = Vec::new();
            node.operator
                .visit_child_links(&mut |child| children.push(child.clone()));
            for (ordinal, child) in children.into_iter().enumerate() {
                let NativeChild::Node(child) = child else {
                    continue;
                };
                let extended = input_is_null_extended(&node.operator, ordinal);
                let mut child_path = path.clone();
                child_path.push(ordinal);
                pending.push((child, null_extended || extended, child_path));
            }
        }
        if seen != self.occurrences.len() {
            return Err(paro_error::internal(
                "native CTE partition lost occurrence coverage",
            ));
        }
        Ok(())
    }

    fn domain_symbol(
        &self,
        proof: &CteDomainProof,
        memo: &Memo,
        state: &mut PlannerTransformState,
    ) -> usize {
        let prior = state
            .cte_bindings
            .iter()
            .find(|binding| binding.symbol == self.cte_index);
        let input = prior.map_or(self.producer, |binding| binding.input);
        let mut domains = prior.map_or_else(Vec::new, |binding| binding.domains.to_vec());
        let equal = |left: &CteDomainProof, right: &CteDomainProof| {
            left.same_domain_by(right, |group| memo.canonical_group(group))
        };
        if !domains.iter().any(|existing| equal(existing, proof)) {
            domains.push(proof.clone());
        }
        let fingerprint = binding_domain_fingerprint(self.definition, &domains);
        let bucket = (self.definition, fingerprint);
        if let Some(indices) = state.cte_binding_index.get(&bucket) {
            if let Some(binding) = indices
                .iter()
                .filter_map(|index| state.cte_bindings.get(*index))
                .find(|binding: &&NativeCteBinding| {
                    binding.fingerprint == fingerprint
                        && memo.canonical_group(binding.input) == memo.canonical_group(input)
                        && domains
                            .iter()
                            .all(|domain| binding.domains.iter().any(|other| equal(domain, other)))
                        && binding
                            .domains
                            .iter()
                            .all(|domain| domains.iter().any(|other| equal(domain, other)))
                })
            {
                return binding.symbol;
            }
        }
        let symbol = state.bind_context.generate_table_index();
        let index = state.cte_bindings.len();
        state.cte_bindings.push(NativeCteBinding {
            symbol,
            definition: self.definition,
            input,
            domains: domains.into_boxed_slice(),
            fingerprint,
        });
        state
            .cte_binding_index
            .entry(bucket)
            .or_default()
            .push(index);
        symbol
    }

    pub(super) fn close_domain(
        &self,
        mut plan: OwnedLogicalPlan,
        proof: &CteDomainProof,
        memo: &Memo,
        state: &mut PlannerTransformState,
    ) -> Result<OwnedLogicalPlan> {
        let symbol = self.domain_symbol(proof, memo, state);
        let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator else {
            return Err(paro_error::internal("CTE domain binding lost its owner"));
        };
        if cte.cte_index != self.cte_index {
            return Err(paro_error::internal(
                "CTE domain binding changed before publication",
            ));
        }
        cte.cte_index = symbol;
        plan = plan.try_map_post_order(|mut node| {
            if let LogicalOperator::CTERef(reference) = &mut node.operator {
                if reference.cte_index == self.cte_index {
                    reference.cte_index = symbol;
                }
            }
            Ok(node)
        })?;
        let LogicalOperator::MaterializedCTE(cte) = &plan.operator else {
            unreachable!()
        };
        self.validate_occurrence_null_extension(&cte.child, &[symbol])?;
        Ok(plan)
    }

    fn domain_is_proved(
        &self,
        proof: &CteDomainProof,
        memo: &Memo,
        state: &PlannerTransformState,
    ) -> bool {
        let mut pending = vec![self.producer];
        let mut seen = BTreeSet::new();
        while let Some(producer) = pending.pop() {
            let producer = memo.canonical_group(producer);
            if !seen.insert(producer) {
                continue;
            }
            for restriction in state
                .cte_restrictions
                .iter()
                .filter(|restriction| memo.canonical_group(restriction.producer) == producer)
            {
                if restriction
                    .proof
                    .same_domain_by(proof, |group| memo.canonical_group(group))
                {
                    return true;
                }
                pending.push(restriction.input);
            }
        }
        false
    }
    /// Partition occurrences, not a representative UNION tree. Every domain
    /// references the same exact producer group, so pruning and aggregate
    /// deferral below it remain independently composable. No consumer column
    /// is replaced by a constant: outer-join null extension stays untouched.
    pub(super) fn partitions(
        &self,
        plan: OwnedLogicalPlan,
        holes: &mut BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        bind: &BindContext,
        labels: &mut PartitionLabels,
        arena: &mut LogicalPlanArena,
    ) -> Result<Vec<OwnedLogicalPlan>> {
        let candidates = self
            .occurrences
            .iter()
            .map(occurrence_equalities)
            .collect::<Vec<_>>();
        let Some(first) = candidates.first() else {
            return Ok(Vec::new());
        };
        let ordinals = first
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .filter(|ordinal| {
                candidates
                    .iter()
                    .all(|values| values.iter().any(|(candidate, _)| candidate == ordinal))
            })
            .collect::<BTreeSet<_>>();
        debug!(target: targets::OPTIMIZER, owner = self.owner.index(), occurrences = self.occurrences.len(), ?ordinals, "native CTE partition domain coverage");
        // Use the planner-session arena for the one shared source snapshot.
        // Candidate plans are exported at the owned-IR rule boundary, then
        // the temporary suffix is rolled back. This keeps arena identity and
        // generation ownership session-scoped without retaining dead CTE
        // partition roots after a declined transformation.
        let checkpoint = arena.checkpoint();
        let result = (|| {
            let root = arena.import(plan)?;
            let available = holes.clone();
            let mut published_holes = available.clone();
            let mut partitions_seen = BTreeSet::new();
            let mut plans = Vec::new();
            for ordinal in ordinals {
                let mut candidate_holes = available.clone();
                if let Some(candidate) = self.partition_by_ordinal(
                    arena.export(root)?,
                    &mut candidate_holes,
                    bind,
                    ordinal,
                    labels,
                    &mut partitions_seen,
                )? {
                    published_holes.extend(candidate_holes);
                    plans.push(candidate);
                }
            }
            let producer_hole = if plans.is_empty() {
                None
            } else if let LogicalOperator::MaterializedCTE(cte) = &arena.get(root)?.operator {
                if let LogicalOperator::BoundReference(reference) =
                    &arena.get(cte.cte_query)?.operator
                {
                    Some(reference.reference_id)
                } else {
                    None
                }
            } else {
                None
            };
            Ok::<_, paro_common::error::ParoError>((plans, published_holes, producer_hole))
        })();
        arena.rollback_to(checkpoint)?;
        let (plans, published_holes, producer_hole) = result?;
        *holes = published_holes;
        if let Some(reference_id) = producer_hole {
            holes.remove(&reference_id);
        }
        Ok(plans)
    }

    #[cfg(test)]
    fn partition(
        &self,
        plan: OwnedLogicalPlan,
        holes: &mut BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        bind: &BindContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        let mut arena = LogicalPlanArena::default();
        Ok(self
            .partitions(plan, holes, bind, &mut PartitionLabels::new(), &mut arena)?
            .into_iter()
            .next())
    }

    fn partition_by_ordinal(
        &self,
        plan: OwnedLogicalPlan,
        holes: &mut BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        bind: &BindContext,
        ordinal: usize,
        labels: &mut PartitionLabels,
        partitions_seen: &mut BTreeSet<OccurrencePartition>,
    ) -> Result<Option<OwnedLogicalPlan>> {
        if self.policy != CTEMaterialize::Default || self.occurrences.len() < 2 {
            return Ok(None);
        }
        let candidates = self
            .occurrences
            .iter()
            .map(occurrence_equalities)
            .collect::<Vec<_>>();
        let mut values = Vec::<Expression>::new();
        let mut partitions = Vec::<Vec<&CteOccurrenceRequirement>>::new();
        for (occurrence, candidates) in self.occurrences.iter().zip(&candidates) {
            let value = &candidates
                .iter()
                .find(|(candidate, _)| *candidate == ordinal)
                .unwrap()
                .1;
            let partition = if let Some(partition) =
                values.iter().position(|candidate| candidate.equals(value))
            {
                partition
            } else {
                values.push(value.clone());
                partitions.push(Vec::new());
                partitions.len() - 1
            };
            partitions[partition].push(occurrence);
        }
        if partitions.len() < 2 {
            return Ok(None);
        }
        let mut assignment = partitions
            .iter()
            .enumerate()
            .flat_map(|(partition, occurrences)| {
                occurrences
                    .iter()
                    .map(move |occurrence| (occurrence.path.clone(), partition))
            })
            .collect::<Vec<_>>();
        assignment.sort_unstable();
        let assignment = assignment.into_boxed_slice();
        if !partitions_seen.insert(assignment.clone()) {
            return Ok(None);
        }
        let LogicalOperator::MaterializedCTE(cte) = plan.into_operator() else {
            return Err(paro_error::internal("CTE partition lost its owner"));
        };
        let LogicalOperator::BoundReference(producer) = &cte.cte_query.operator else {
            return Err(paro_error::internal(
                "CTE partition requires an opaque producer",
            ));
        };
        if !producer.facts.can_replay {
            debug!(target: targets::OPTIMIZER, producer = self.producer.index(), "CTE partition has no producer replay proof");
            return Ok(None);
        }
        if holes.get(&producer.reference_id) != Some(&self.producer) {
            return Err(paro_error::internal(
                "CTE partition lost its producer identity",
            ));
        }
        let mut domains = Vec::new();
        for occurrences in &partitions {
            let Some(predicates) = crate::cte::predicate_domain::derive_producer_predicates(
                occurrences
                    .iter()
                    .map(|occurrence| crate::cte::predicate_domain::FilteredCTERef {
                        old_bindings: occurrence.output.bindings().to_vec(),
                        filters: occurrence
                            .predicates
                            .as_deref()
                            .unwrap_or_default()
                            .to_vec(),
                    })
                    .collect(),
                &producer.bindings,
            ) else {
                return Ok(None);
            };
            domains.push(predicates);
        }
        let indices = labels
            .entry((self.cte_index, assignment))
            .or_insert_with(|| {
                partitions
                    .iter()
                    .map(|_| bind.generate_table_index())
                    .collect()
            })
            .clone();
        let mut consumer = *cte.child;
        // Column aliases are not occurrence identities: an equivalent child
        // group can occur on two UNION edges with the same output bindings.
        // Apply each demand to its exact edge path, including negative/outer
        // contexts, instead of selecting the first matching table alias.
        for (partition, occurrences) in partitions.iter().enumerate() {
            for occurrence in occurrences {
                let mut node = &mut consumer;
                for ordinal in occurrence.path.iter().copied() {
                    let mut child_index = 0;
                    let mut selected = None;
                    let _ = node.visit_children_mut(|child| {
                        if child_index == ordinal {
                            selected = Some(child);
                            return std::ops::ControlFlow::Break(());
                        }
                        child_index += 1;
                        std::ops::ControlFlow::Continue(())
                    });
                    node = selected
                        .ok_or_else(|| paro_error::internal("CTE occurrence path disappeared"))?;
                }
                let LogicalOperator::CTERef(reference) = &mut node.operator else {
                    return Err(paro_error::internal("CTE occurrence path changed operator"));
                };
                if reference.cte_index != self.cte_index
                    || reference.table_index != occurrence.table_index
                {
                    return Err(paro_error::internal(
                        "CTE occurrence path changed its binding",
                    ));
                }
                reference.cte_index = indices[partition];
            }
        }
        self.validate_occurrence_null_extension(&consumer, &indices)?;
        holes.remove(&producer.reference_id);
        for (partition, predicates) in domains.into_iter().enumerate().rev() {
            let mut reference = producer.clone();
            reference.reference_id =
                paro_planner::operator::BoundReferenceId::group_hole(bind.next_plan_id().0);
            holes.insert(reference.reference_id, self.producer);
            let input = OwnedLogicalPlan::new(bind, LogicalOperator::BoundReference(reference));
            let restricted = OwnedLogicalPlan::new(
                bind,
                LogicalOperator::Filter(paro_planner::operator::Filter::new(input, predicates)),
            );
            consumer = OwnedLogicalPlan::new(
                bind,
                LogicalOperator::MaterializedCTE(
                    MaterializedCTE::new(
                        indices[partition],
                        format!("{}$domain{}", cte.cte_name, partition + 1),
                        cte.column_names.clone(),
                        cte.column_types.clone(),
                        self.policy,
                        restricted,
                        consumer,
                    )
                    .with_ref_count(partitions[partition].len()),
                ),
            );
        }
        Ok(Some(consumer))
    }

    fn validate_occurrence_null_extension(
        &self,
        consumer: &OwnedLogicalPlan,
        ctes: &[usize],
    ) -> Result<()> {
        let mut pending = vec![(consumer, false, Vec::new())];
        let mut seen = 0;
        while let Some((plan, null_extended, path)) = pending.pop() {
            if let LogicalOperator::CTERef(reference) = &plan.operator {
                if ctes.contains(&reference.cte_index) {
                    let occurrence = self
                        .occurrences
                        .iter()
                        .find(|occurrence| occurrence.path.as_ref() == path)
                        .ok_or_else(|| {
                            paro_error::internal("partition introduced a CTE occurrence")
                        })?;
                    if occurrence.null_extended != null_extended
                        || occurrence.table_index != reference.table_index
                    {
                        return Err(paro_error::internal(
                            "partition changed CTE null-extension ownership",
                        ));
                    }
                    seen += 1;
                }
            }
            for (ordinal, child) in plan.children().into_iter().enumerate() {
                let extended = input_is_null_extended(&plan.operator, ordinal);
                let mut child_path = path.clone();
                child_path.push(ordinal);
                pending.push((child, null_extended || extended, child_path));
            }
        }
        if seen != self.occurrences.len() {
            return Err(paro_error::internal(
                "partition lost CTE occurrence coverage",
            ));
        }
        Ok(())
    }
    pub(super) fn from_binding(
        binding: &PatternBinding,
        memo: &Memo,
        state: &PlannerTransformState,
    ) -> Result<Self> {
        let PatternOperand::Expression {
            group,
            expression,
            children,
        } = &binding.root
        else {
            return Err(paro_error::internal("CTE requirement needs an owner shell"));
        };
        let logical = memo
            .logical_expr(*expression)
            .ok_or_else(|| paro_error::internal("CTE owner disappeared"))?;
        let payload = &state.payloads.logical[logical.payload.index()];
        let LogicalOperator::MaterializedCTE(cte) = &payload.semantic_template.operator else {
            return Err(paro_error::internal("CTE requirement has a non-CTE owner"));
        };
        if children.len() != 2 || logical.key.children.len() != 2 {
            return Err(paro_error::internal(
                "CTE owner requires producer and consumer",
            ));
        }
        let mut occurrences = Vec::new();
        let mut pending = vec![(&children[1], Vec::new(), false, None, None)];
        while let Some((operand, path, null_extended, predicates, keys)) = pending.pop() {
            let PatternOperand::Expression {
                expression,
                children,
                ..
            } = operand
            else {
                continue;
            };
            let logical = memo
                .logical_expr(*expression)
                .ok_or_else(|| paro_error::internal("CTE consumer disappeared"))?;
            let operator = &state.payloads.logical[logical.payload.index()]
                .semantic_template
                .operator;
            if let LogicalOperator::CTERef(reference) = operator {
                if reference.cte_index == cte.cte_index {
                    occurrences.push(CteOccurrenceRequirement {
                        path: path.into_boxed_slice(),
                        table_index: reference.table_index,
                        null_extended,
                        output: Arc::new(paro_planner::operator::LogicalOutputLayout::new(
                            reference.column_types.clone(),
                            (0..reference.column_types.len())
                                .map(|ordinal| ColumnBinding::new(reference.table_index, ordinal))
                                .collect(),
                        )),
                        predicates,
                        keys,
                    });
                }
                continue;
            }
            for (ordinal, child) in children.iter().enumerate().rev() {
                let mut child_path = path.clone();
                child_path.push(ordinal);
                let extends_nulls = input_is_null_extended(operator, ordinal);
                let predicates = match operator {
                    LogicalOperator::Filter(filter) => {
                        Some(filter.expressions.clone().into_boxed_slice())
                    }
                    _ => None,
                };
                let keys = key_demand(
                    operator,
                    children,
                    ordinal,
                    cte.cte_index,
                    &state.metadata[&logical.payload].child_layouts,
                    memo,
                    state,
                )?;
                pending.push((
                    child,
                    child_path,
                    null_extended || extends_nulls,
                    predicates,
                    keys,
                ));
            }
        }
        occurrences.sort_by(|left, right| left.path.cmp(&right.path));
        let producer = memo.canonical_group(logical.key.children[0]);
        let mut base_producer = producer;
        let mut ancestry = BTreeSet::new();
        while ancestry.insert(base_producer) {
            let Some(restriction) = state.cte_restrictions.iter().find(|restriction| {
                memo.canonical_group(restriction.producer) == base_producer
                    && memo.canonical_group(restriction.input) != base_producer
            }) else {
                break;
            };
            base_producer = memo.canonical_group(restriction.input);
        }
        Ok(Self {
            owner: memo.canonical_group(*group),
            producer,
            base_producer,
            cte_index: cte.cte_index,
            definition: state
                .cte_bindings
                .iter()
                .find(|binding| binding.symbol == cte.cte_index)
                .map_or(cte.cte_index, |binding| binding.definition),
            policy: cte.materialized,
            sharing_owner: state
                .metadata
                .get(&logical.payload)
                .and_then(|metadata| metadata.required_region_facet),
            occurrences: occurrences.into_boxed_slice(),
        })
    }

    pub(super) fn restrict_predicate_domain(
        &self,
        mut plan: OwnedLogicalPlan,
        memo: &Memo,
        state: &PlannerTransformState,
    ) -> Result<Option<(OwnedLogicalPlan, CteDomainProof)>> {
        use crate::cte::predicate_domain::{derive_producer_predicates, FilteredCTERef};
        let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator else {
            return Err(paro_error::internal("CTE requirement lost its owner"));
        };
        let Some(references) = self
            .occurrences
            .iter()
            .map(|occurrence| {
                Some(FilteredCTERef {
                    old_bindings: occurrence.output.bindings().to_vec(),
                    filters: occurrence.predicates.as_ref()?.to_vec(),
                })
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(predicates) =
            derive_producer_predicates(references, &cte.cte_query.get_column_bindings())
        else {
            return Ok(None);
        };
        let proof = CteDomainProof::new(self.definition, predicates.clone(), std::iter::empty());
        if self.domain_is_proved(&proof, memo, state) {
            return Ok(None);
        }
        let (shell, mut inputs) = paro_planner::plan::arena::LogicalPlanNode::detach(plan);
        let consumer = inputs
            .pop()
            .ok_or_else(|| paro_error::internal("CTE consumer is missing"))?;
        let producer = inputs
            .pop()
            .ok_or_else(|| paro_error::internal("CTE producer is missing"))?;
        let filtered = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
            paro_planner::operator::Filter::new(*producer, predicates),
        ));
        Ok(Some((
            shell.assemble([Box::new(filtered), consumer])?,
            proof,
        )))
    }

    pub(super) fn restrict_key_domain(
        &self,
        plan: OwnedLogicalPlan,
        holes: &mut BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        memo: &Memo,
        state: &PlannerTransformState,
        facts: &boundary::BoundarySnapshot,
    ) -> Result<Option<(OwnedLogicalPlan, CteDomainProof)>> {
        use paro_planner::operator::{BoundReference, ComparisonJoin, JoinCondition, SetOperation};
        let Some(keys) = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.keys.clone())
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(first) = keys.first() else {
            return Ok(None);
        };
        let ordinals = first.ordinals.clone();
        if keys.iter().any(|key| key.ordinals != ordinals) {
            return Ok(None);
        }
        let proof = CteDomainProof::new(self.definition, std::iter::empty(), keys.clone());
        if self.domain_is_proved(&proof, memo, state) {
            return Ok(None);
        }
        let bind = &state.bind_context;
        let mut domain: Option<OwnedLogicalPlan> = None;
        for key in keys {
            let reference_id =
                paro_planner::operator::BoundReferenceId::group_hole(bind.next_plan_id().0);
            let transport = facts.transport(memo, state, key.group, &key.layout)?;
            let reference = BoundReference::new(
                reference_id,
                key.layout.bindings().to_vec(),
                key.layout.types().to_vec(),
            )
            .with_facts(transport)?;
            holes.insert(reference_id, key.group);
            let input = OwnedLogicalPlan::new(bind, LogicalOperator::BoundReference(reference));
            let project = OwnedLogicalPlan::new(
                bind,
                LogicalOperator::Projection(
                    Projection::new(bind.generate_table_index(), input, key.expressions.to_vec())
                        .with_internal_outputs(),
                ),
            );
            domain = Some(if let Some(previous) = domain {
                let types = project.types();
                OwnedLogicalPlan::new(
                    bind,
                    LogicalOperator::SetOperation(SetOperation::union(
                        bind.generate_table_index(),
                        previous,
                        project,
                        true,
                        types,
                    )),
                )
            } else {
                project
            });
        }
        let domain = domain.expect("nonempty key requirements");
        let (shell, mut inputs) = paro_planner::plan::arena::LogicalPlanNode::detach(plan);
        let consumer = inputs
            .pop()
            .ok_or_else(|| paro_error::internal("CTE consumer is missing"))?;
        let producer = inputs
            .pop()
            .ok_or_else(|| paro_error::internal("CTE producer is missing"))?;
        let output = producer.output_layout();
        let domain_layout = domain.output_layout();
        let conditions = ordinals
            .iter()
            .enumerate()
            .map(|(domain_ordinal, producer_ordinal)| {
                let binding = *output
                    .bindings()
                    .get(*producer_ordinal)
                    .ok_or_else(|| paro_error::internal("CTE key demand changed producer width"))?;
                let ty = output.types()[*producer_ordinal].clone();
                if ty != domain_layout.types()[domain_ordinal] {
                    return Err(paro_error::internal("CTE key domain changed type"));
                }
                Ok(JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(binding, ty.clone()).into()),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(domain_layout.bindings()[domain_ordinal], ty)
                            .into(),
                    ),
                    JoinComparisonType::Equal,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let restricted = OwnedLogicalPlan::new(
            bind,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                *producer,
                domain,
                conditions,
            ))),
        );
        Ok(Some((
            shell.assemble([Box::new(restricted), consumer])?,
            proof,
        )))
    }

    pub(super) fn inline(
        &self,
        plan: OwnedLogicalPlan,
        holes: &mut BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        bind_context: &BindContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        if self.policy == CTEMaterialize::Materialized {
            return Ok(None);
        }
        let LogicalOperator::MaterializedCTE(cte) = plan.into_operator() else {
            return Err(paro_error::internal("CTE inline lost its owner shell"));
        };
        let LogicalOperator::BoundReference(producer) = &cte.cte_query.operator else {
            return Err(paro_error::internal(
                "native CTE inline requires an opaque producer",
            ));
        };
        if !producer.facts.can_replay {
            return Ok(None);
        }
        if holes.get(&producer.reference_id) != Some(&self.producer) {
            return Err(paro_error::internal(
                "CTE inline changed its exact producer GroupRef",
            ));
        }
        let mut seen = 0usize;
        let (plan, ()) = (*cte.child).try_fold_post_order(|plan, _: Vec<()>| {
            let LogicalOperator::CTERef(reference) = &plan.operator else {
                return Ok((plan, ()));
            };
            if reference.cte_index != self.cte_index {
                return Ok((plan, ()));
            }
            let reference: CTERef = reference.clone();
            if !self.occurrences.iter().any(|occurrence| {
                occurrence.table_index == reference.table_index
                    && occurrence.output.types() == reference.column_types.as_slice()
            }) {
                return Err(paro_error::internal(
                    "CTE inline reached an unproved consumer occurrence",
                ));
            }
            let input = paro_planner::operator::BoundReference::new(
                paro_planner::operator::BoundReferenceId::group_hole(bind_context.next_plan_id().0),
                producer.bindings.clone(),
                producer.types().to_vec(),
            )
            .with_facts(producer.facts.clone())?;
            holes.insert(input.reference_id, self.producer);
            let mut input_plan =
                OwnedLogicalPlan::new(bind_context, LogicalOperator::BoundReference(input));
            input_plan.stats = cte.cte_query.stats.clone();
            let expressions = input_plan
                .get_column_bindings()
                .into_iter()
                .zip(input_plan.types())
                .map(|(binding, ty)| {
                    Expression::ColumnRef(ColumnRefExpression::new(binding, ty).into())
                })
                .collect();
            let projection = Projection::new(reference.table_index, input_plan, expressions)
                .with_visible_names(reference.column_names)
                .with_visible_qualifier(reference.relation_alias);
            seen += 1;
            Ok((
                OwnedLogicalPlan::new(bind_context, LogicalOperator::Projection(projection)),
                (),
            ))
        })?;
        if seen != self.occurrences.len() {
            return Err(paro_error::internal(
                "CTE inline did not consume its complete occurrence requirement",
            ));
        }
        holes.remove(&producer.reference_id);
        Ok(Some(plan))
    }
}

fn native_path_node(
    nodes: &[NativeNode],
    root: usize,
    path: &[usize],
) -> Option<usize> {
    let mut current = root;
    for ordinal in path {
        let node = nodes.get(current)?;
        let mut children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| children.push(child.clone()));
        current = match children.get(*ordinal)? {
            NativeChild::Node(index) => *index,
            NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => return None,
        };
    }
    Some(current)
}

/// A materialized CTE is the intentional result of the partition rewrite,
/// so the generic native-shell control check (which rejects every CTE
/// wrapper) is too strict here.  Keep its safety property for recursive
/// ownership and for opaque inputs whose boundary facts explicitly contain a
/// control region.
fn native_cte_shell_contains_control_boundary(shell: &NativeShell) -> bool {
    shell.nodes.iter().any(|node| {
        let mut contains = matches!(&node.operator, LogicalOperator::RecursiveCTE(_));
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::MemoGroup { reference, .. } | NativeChild::Group { reference, .. } => {
                contains |= reference.facts.contains_control_region;
            }
            NativeChild::Node(_) => {}
        });
        contains
    })
}

fn clone_native_memo_group(
    child: &NativeChild,
    state: &PlannerTransformState,
) -> Result<NativeChild> {
    let NativeChild::MemoGroup {
        group,
        stats,
        layout,
        names,
        reference,
        ..
    } = child
    else {
        return Err(paro_error::internal(
            "native CTE rewrite requires a Memo group producer edge",
        ));
    };
    let id = state.bind_context.next_plan_id();
    let mut reference = reference.clone();
    reference.reference_id =
        paro_planner::operator::BoundReferenceId::group_hole(id.0);
    Ok(NativeChild::MemoGroup {
        group: *group,
        id,
        stats: stats.clone(),
        layout: layout.clone(),
        names: names.clone(),
        reference,
    })
}

/// Build the positional projection required to replace one CTERef.  The
/// definition-column mapping is authoritative: a positional shortcut would
/// silently change semantics for a producer whose output was reordered or
/// pruned before it reached the materialization boundary.
fn cte_projection_expressions(
    owner: &MaterializedCTE<NativeChild>,
    producer_layout: &paro_planner::operator::LogicalOutputLayout,
    reference: &CTERef,
) -> Result<Option<Vec<Expression>>> {
    if reference.definition_columns.len() != reference.column_types.len()
        || reference.column_names.len() != reference.column_types.len()
    {
        return Ok(None);
    }
    let mut expressions = Vec::with_capacity(reference.definition_columns.len());
    for (ordinal, definition) in reference.definition_columns.iter().enumerate() {
        let Some(output) = owner
            .output_columns
            .iter()
            .find(|output| output.definition == *definition)
        else {
            return Ok(None);
        };
        let Some(source_ordinal) = producer_layout
            .bindings()
            .iter()
            .position(|binding| *binding == output.binding)
        else {
            return Ok(None);
        };
        let Some(source_type) = producer_layout.types().get(source_ordinal) else {
            return Ok(None);
        };
        let Some(returned_type) = reference.column_types.get(ordinal) else {
            return Ok(None);
        };
        if source_type != returned_type {
            return Ok(None);
        }
        expressions.push(Expression::ColumnRef(
            ColumnRefExpression::new(output.binding, source_type.clone()).into(),
        ));
    }
    Ok(Some(expressions))
}

#[cfg(test)]
#[path = "cte_fact_tests.rs"]
mod cte_fact_tests;
