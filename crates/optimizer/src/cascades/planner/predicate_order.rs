// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Filter scheduling over native scalar operands.
//!
//! Logical identity canonicalizes pure, total segments. A physical candidate
//! retains its exact scheduling decision, independently of later statistics.
//! Extraction replays the semantic proof before exporting any executable AST.
//! This policy does not claim a cost rebate: Filter's conservative structural
//! work is still order-independent until kernel evaluation work is modeled.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PredicateOrder {
    ordinals: Box<[usize]>,
}

/// Canonical is a completed decision, not an interruption fallback. A missing
/// result means no candidate may be published for this attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PredicateSchedule {
    Canonical,
    Ordered(PredicateOrder),
}

pub(super) fn implementation_schedule(
    logical: &crate::cascades::memo::LogicalExpr,
    metadata: &PlannerOperatorMetadata,
    state: &PlannerTransformState,
    memo: &Memo,
) -> Result<Option<CachedFilterSchedule>> {
    let key = FilterScheduleKey {
        epoch: memo.cost_epoch(),
        expression: logical.id,
    };
    if let Some(schedule) = state.payloads.filter_schedule(key) {
        return Ok(Some(schedule));
    }
    let Some(schedule) = select(logical, state, memo)? else {
        return Ok(None);
    };
    Ok(Some(state.payloads.publish_filter_schedule(
        key,
        logical.payload,
        CachedFilterSchedule {
            payload: metadata.baseline_payload,
            fingerprint: metadata.operator_fingerprint,
        },
        schedule,
    )))
}

impl PredicateOrder {
    pub(super) fn verify(&self, roots: &[ScalarExprId], arena: &ScalarArena) -> Result<()> {
        if self.ordinals.len() != roots.len() {
            return Err(paro_error::internal(
                "physical predicate order changed operand arity",
            ));
        }
        let mut seen = vec![false; roots.len()];
        let mut start = 0;
        for end in 0..=roots.len() {
            let fence = if end == roots.len() {
                false
            } else {
                arena
                    .get(roots[end])
                    .ok_or_else(|| {
                        paro_error::internal("physical predicate order lost a native operand")
                    })?
                    .properties
                    .is_evaluation_fence()
            };
            if fence || end == roots.len() {
                for &source in &self.ordinals[start..end] {
                    if !(start..end).contains(&source) || seen[source] {
                        return Err(paro_error::internal(
                            "physical predicate order duplicates an operand or crosses an evaluation fence",
                        ));
                    }
                    seen[source] = true;
                }
                if fence && self.ordinals[end] != end {
                    return Err(paro_error::internal(
                        "physical predicate order moved an evaluation fence",
                    ));
                }
                start = end + 1;
            }
        }
        Ok(())
    }

    pub(super) fn ordered_roots(
        &self,
        roots: &[ScalarExprId],
        arena: &ScalarArena,
    ) -> Result<Box<[ScalarExprId]>> {
        self.verify(roots, arena)?;
        Ok(self
            .ordinals
            .iter()
            .map(|&ordinal| roots[ordinal])
            .collect())
    }

    pub(super) fn fingerprint(&self, operator: Fingerprint) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_fingerprint(operator);
        fingerprint.write_bytes(b"physical-filter-order");
        fingerprint.write_u64(self.ordinals.len() as u64);
        for &ordinal in &self.ordinals {
            fingerprint.write_u64(ordinal as u64);
        }
        fingerprint.finish()
    }
}

/// Physical search runs after logical exploration in a frozen fact epoch.
/// Read that epoch's input facts, not the staging payload's historical stats.
/// Each referenced local column is resolved once; CTE references use Memo's
/// explicit definition-column mapping, never producer output ordinals.
pub(super) fn select(
    logical: &crate::cascades::memo::LogicalExpr,
    state: &PlannerTransformState,
    memo: &Memo,
) -> Result<Option<PredicateSchedule>> {
    if !memo.control().checkpoint()? {
        return Ok(None);
    }
    if logical.key.scalars.len() < 2 {
        return Ok(Some(PredicateSchedule::Canonical));
    }
    let [input] = logical.key.children.as_ref() else {
        return Err(paro_error::internal(
            "physical filter has no unique input group",
        ));
    };
    let mut evidence = BTreeMap::new();
    for root in &logical.key.scalars {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        let node = state
            .scalars
            .get(*root)
            .ok_or_else(|| paro_error::internal("physical filter lost its native operand"))?;
        for column in node.properties.local_columns() {
            if let std::collections::btree_map::Entry::Vacant(entry) = evidence.entry(column) {
                if !memo.control().checkpoint()? {
                    return Ok(None);
                }
                entry.insert((
                    memo.column_domain(*input, column)
                        .and_then(|domain| domain.expected()),
                    memo.column_value_domain(*input, column)?,
                ));
            }
        }
    }
    permutation(
        &logical.key.scalars,
        state,
        |column| {
            evidence.get(&column).map(|(point, values)| {
                crate::cost_model::ColumnPredicateEvidence {
                    point: *point,
                    values: values.as_ref().map(|values| values.statistics()),
                    distribution: values.as_ref().and_then(|values| values.distribution()),
                }
            })
        },
        || memo.control().checkpoint(),
    )
}

pub(super) fn permutation<'a>(
    roots: &[ScalarExprId],
    state: &'a PlannerTransformState,
    statistics: impl Fn(ColumnId) -> Option<crate::cost_model::ColumnPredicateEvidence<'a>>,
    mut checkpoint: impl FnMut() -> Result<bool>,
) -> Result<Option<PredicateSchedule>> {
    if !checkpoint()? {
        return Ok(None);
    }
    if roots.len() < 2 {
        return Ok(Some(PredicateSchedule::Canonical));
    }
    let mut ordering = Vec::with_capacity(roots.len());
    let mut segment = Vec::<(usize, f64)>::new();
    let flush = |segment: &mut Vec<(usize, f64)>, ordering: &mut Vec<usize>| {
        segment.sort_by(|left, right| left.1.total_cmp(&right.1));
        ordering.extend(segment.drain(..).map(|(ordinal, _)| ordinal));
    };
    for (ordinal, &root) in roots.iter().enumerate() {
        if !checkpoint()? {
            return Ok(None);
        }
        let node = state
            .scalars
            .get(root)
            .ok_or_else(|| paro_error::internal("predicate ordering lost a native operand"))?;
        if node.properties.is_evaluation_fence() {
            flush(&mut segment, &mut ordering);
            ordering.push(ordinal);
        } else {
            let Some(selectivity) = state.cost_model.estimate_native_selectivity(
                root,
                &state.scalars,
                &state.binding_ids,
                &statistics,
                &mut checkpoint,
            )?
            else {
                return Ok(None);
            };
            segment.push((ordinal, selectivity));
        }
    }
    flush(&mut segment, &mut ordering);
    if ordering
        .iter()
        .enumerate()
        .all(|(before, after)| before == *after)
    {
        return Ok(Some(PredicateSchedule::Canonical));
    }
    let order = PredicateOrder {
        ordinals: ordering.into_boxed_slice(),
    };
    order.verify(roots, &state.scalars)?;
    Ok(Some(PredicateSchedule::Ordered(order)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression,
        FunctionExpression,
    };
    use paro_planner::operator::{Filter, Get};

    fn two_column_filter() -> OptimizationInput {
        let predicates = (0..2)
            .map(|ordinal| {
                Expression::Comparison(
                    ComparisonExpression::new(
                        ComparisonType::Equal,
                        Expression::ColumnRef(
                            ColumnRefExpression::new(
                                ColumnBinding::new(0, ordinal),
                                LogicalType::Integer,
                            )
                            .into(),
                        ),
                        Expression::Constant(
                            ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
                        ),
                    )
                    .into(),
                )
            })
            .collect();
        let mut source =
            OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new_without_table(
                0,
                vec!["a".into(), "b".into()],
                vec![LogicalType::Integer; 2],
            ))));
        source.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1000));
        MemoBuilder::build(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(source, predicates))),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap()
    }

    #[test]
    fn logical_filter_identity_ignores_only_pure_total_permutations() {
        let input = two_column_filter();
        let root = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let mut state = input.planner_state.write().unwrap();
        let mut operator = state.payloads.logical[root.payload.index()]
            .semantic_template
            .operator
            .clone();
        let LogicalOperator::Filter(filter) = &mut operator else {
            panic!("filter")
        };
        filter.expressions.reverse();
        let child_columns = input
            .memo
            .group(root.key.children[0])
            .unwrap()
            .schema
            .columns()
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let PlannerTransformState {
            binding_ids,
            columns,
            scalars,
            ..
        } = &mut *state;
        let reversed = intern_operator_scalars(
            &operator,
            &[],
            &[child_columns],
            binding_ids,
            columns,
            scalars,
        )
        .unwrap();
        assert_eq!(reversed, root.key.scalars);
        assert_eq!(
            query_operator_identity(&operator, &reversed, scalars)
                .unwrap()
                .0,
            root.key.operator
        );
    }

    #[test]
    fn predicate_order_verifier_agrees_with_an_exhaustive_fence_oracle() {
        use crate::cascades::scalar::{
            ScalarKind, ScalarLiteral, ScalarLocalProperties, ScalarSpec,
        };
        let mut arena = ScalarArena::default();
        let pure = arena
            .intern(ScalarSpec {
                kind: ScalarKind::Constant {
                    value: ScalarLiteral::new(Value::Boolean(true), LogicalType::Boolean),
                },
                logical_type: LogicalType::Boolean,
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
            .unwrap();
        let fence = arena
            .intern(ScalarSpec {
                kind: ScalarKind::Constant {
                    value: ScalarLiteral::new(Value::Boolean(true), LogicalType::Boolean),
                },
                logical_type: LogicalType::Boolean,
                children: Box::new([]),
                local_properties: ScalarLocalProperties {
                    may_error: true,
                    ..Default::default()
                },
            })
            .unwrap();
        let roots = [pure, pure, fence, pure, pure, pure];
        // Independently enumerate all 6! schedules. A schedule is legal iff
        // it is a bijection and every occurrence stays in its original block.
        fn enumerate(
            prefix: &mut Vec<usize>,
            remaining: &mut Vec<usize>,
            visit: &mut impl FnMut(&[usize]),
        ) {
            if remaining.is_empty() {
                visit(prefix);
                return;
            }
            for index in 0..remaining.len() {
                let value = remaining.remove(index);
                prefix.push(value);
                enumerate(prefix, remaining, visit);
                prefix.pop();
                remaining.insert(index, value);
            }
        }
        let block = [0, 0, 1, 2, 2, 2];
        let mut count = 0;
        let mut accepted = 0;
        enumerate(&mut Vec::new(), &mut (0..6).collect(), &mut |order| {
            count += 1;
            let expected = order
                .iter()
                .enumerate()
                .all(|(target, source)| block[target] == block[*source]);
            let proof = PredicateOrder {
                ordinals: order.into(),
            };
            assert_eq!(proof.verify(&roots, &arena).is_ok(), expected, "{order:?}");
            accepted += usize::from(expected);
        });
        assert_eq!((count, accepted), (720, 12));
        for invalid in [
            vec![0, 1],
            vec![0, 0, 2, 3, 4, 5],
            vec![0, 1, 2, 3, 4, 6],
            vec![0, 1, 2, 3, 4, usize::MAX],
        ] {
            assert!(
                PredicateOrder {
                    ordinals: invalid.into_boxed_slice()
                }
                .verify(&roots, &arena)
                .is_err()
            );
        }
    }

    #[test]
    fn selected_physical_filter_replays_its_order_after_facts_change_and_rejects_tampering() {
        let mut input = two_column_filter();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let logical = input.memo.logical_expr(expression).unwrap();
        let roots = logical.key.scalars.clone();
        let child = logical.key.children[0];
        let columns = {
            let state = input.planner_state.read().unwrap();
            roots
                .iter()
                .map(|root| {
                    state
                        .scalars
                        .get(*root)
                        .unwrap()
                        .properties
                        .local_columns()
                        .next()
                        .unwrap()
                })
                .collect::<Vec<_>>()
        };
        for (column, point) in columns.iter().zip([2, 100]) {
            input
                .memo
                .group_mut(child)
                .unwrap()
                .logical_properties
                .column_domains
                .insert(*column, GroupColumnDomain::new(Some(point), None).unwrap());
        }
        let classes = super::super::tests::test_grant_classes();
        let mut registry = ImplementationRegistry::default();
        implementation::register_implementations(
            &mut registry,
            input.planner_state.clone(),
            Arc::new(classes.iter().map(|class| (class.id, *class)).collect()),
            input.calibration.clone(),
            false,
        )
        .unwrap();
        let mut engine = CascadesEngine::new(input.memo, registry);
        let winners = engine
            .optimize_for_grants(
                input.root,
                input.root_goal,
                AdmissibleGrantSetId(0),
                classes,
                SearchMode::Memo,
            )
            .unwrap();
        let winner = &winners.winners[0];
        let physical = engine
            .memo()
            .physical_expr(winner.winner.expression)
            .unwrap();
        let payload = physical.payload;
        {
            let state = input.planner_state.read().unwrap();
            let PlannerPhysicalTemplate::OrderedFilter { order, .. } =
                &state.payloads.get_physical(payload).unwrap().template
            else {
                panic!("ordered physical filter")
            };
            assert_eq!(&*order.ordinals, &[1, 0]);
        }
        // Reverse the estimator's preference after the physical decision.
        // Extraction must replay the frozen schedule, not consult new stats.
        for (column, point) in columns.iter().zip([100, 2]) {
            engine
                .memo_mut()
                .group_mut(child)
                .unwrap()
                .logical_properties
                .column_domains
                .insert(*column, GroupColumnDomain::new(Some(point), None).unwrap());
        }
        let state = input.planner_state.read().unwrap();
        let extracted = extract_planner_tree(
            engine.memo(),
            &state,
            &input.bind_context,
            input.root,
            winner.goal,
            winner.winner.candidate,
            SearchMode::Memo,
        )
        .unwrap();
        let presented = enforce_result_presentation(
            extracted,
            &input.presentation,
            &input.bind_context,
            input.calibration.as_ref(),
            winner.winner.physical_fingerprint,
            winner.winner.cost,
        )
        .unwrap();
        let expected_binding = state.binding_ids.relation_binding(columns[1]).unwrap();
        let mut found = false;
        presented
            .plan
            .try_visit_pre_order(|plan| {
                if let LogicalOperator::Filter(filter) = &plan.operator {
                    let Expression::Comparison(comparison) = &filter.expressions[0] else {
                        panic!("comparison")
                    };
                    let slot = [comparison.left.as_ref(), comparison.right.as_ref()]
                        .into_iter()
                        .find_map(|expression| {
                            if let Expression::Reference(slot) = expression {
                                Some(slot)
                            } else {
                                None
                            }
                        })
                        .expect("selected comparison slot");
                    assert_eq!(
                        filter.child.layout().bindings()[slot.index],
                        expected_binding
                    );
                    found = true;
                }
                Ok(())
            })
            .unwrap();
        assert!(found);
        drop(state);
        {
            let state = input.planner_state.read().unwrap();
            state.payloads.tamper_physical(payload, |template| {
                let PlannerPhysicalTemplate::OrderedFilter { order, .. } = template else {
                    panic!("ordered filter")
                };
                order.ordinals[0] = order.ordinals[1];
            });
        }
        let state = input.planner_state.read().unwrap();
        assert!(
            extract_planner_tree(
                engine.memo(),
                &state,
                &input.bind_context,
                input.root,
                winner.goal,
                winner.winner.candidate,
                SearchMode::Memo
            )
            .is_err()
        );
    }

    #[test]
    fn physical_order_interning_rolls_back_without_stale_payloads() {
        let input = two_column_filter();
        let logical = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let mut state = input.planner_state.write().unwrap();
        let checkpoint = state.savepoint();
        let before = state.payloads.physical_count();
        let order = PredicateOrder {
            ordinals: Box::new([1, 0]),
        };
        let metadata = &state.metadata[&logical.payload];
        let key = FilterScheduleKey {
            epoch: input.memo.cost_epoch(),
            expression: logical.id,
        };
        let canonical = CachedFilterSchedule {
            payload: metadata.baseline_payload,
            fingerprint: metadata.operator_fingerprint,
        };
        let first = state.payloads.publish_filter_schedule(
            key,
            logical.payload,
            canonical,
            PredicateSchedule::Ordered(order.clone()),
        );
        for _ in 0..100 {
            assert_eq!(state.payloads.filter_schedule(key), Some(first));
        }
        assert_eq!(state.payloads.physical_count(), before + 1);
        state.rollback_to(checkpoint).unwrap();
        assert_eq!(state.payloads.physical_count(), before);
        assert!(state.payloads.filter_schedule(key).is_none());
        let reinserted = state.payloads.publish_filter_schedule(
            key,
            logical.payload,
            canonical,
            PredicateSchedule::Ordered(order),
        );
        assert!(state.payloads.get_physical(reinserted.payload).is_some());
        assert_eq!(state.payloads.physical_count(), before + 1);
        assert_eq!(state.payloads.schedule_counters()[2].1, 1);
        // A normal unsuccessful rule attempt has no physical delta. It must
        // not scan, retire or reinsert the persistent schedule cache.
        for _ in 0..100 {
            let checkpoint = state.savepoint();
            state.rollback_to(checkpoint).unwrap();
        }
        assert_eq!(state.payloads.filter_schedule(key), Some(reinserted));
        assert_eq!(state.payloads.schedule_counters()[2].1, 1);
    }

    #[test]
    fn expired_scheduling_publishes_no_partial_order_and_keeps_the_baseline() {
        let input = two_column_filter();
        let logical = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let state = input.planner_state.read().unwrap();
        let count = state.payloads.physical_count();
        input.memo.control().begin_optional();
        input.memo.control().expire();
        assert!(
            implementation_schedule(
                logical,
                &state.metadata[&logical.payload],
                &state,
                &input.memo
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(state.payloads.physical_count(), count);
        assert_eq!(state.payloads.schedule_counters()[1].1, 0);
        assert!(
            state
                .payloads
                .get_physical(state.metadata[&logical.payload].baseline_payload)
                .is_some()
        );
    }

    #[test]
    fn schedule_is_built_once_per_frozen_epoch_not_once_per_goal() {
        let mut input = two_column_filter();
        let logical = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap()
            .clone();
        let state = input.planner_state.read().unwrap();
        let columns: Vec<_> = logical
            .key
            .scalars
            .iter()
            .map(|root| {
                state
                    .scalars
                    .get(*root)
                    .unwrap()
                    .properties
                    .local_columns()
                    .next()
                    .unwrap()
            })
            .collect();
        let child = logical.key.children[0];
        for (column, point) in columns.iter().zip([2, 100]) {
            input
                .memo
                .group_mut(child)
                .unwrap()
                .logical_properties
                .column_domains
                .insert(*column, GroupColumnDomain::new(Some(point), None).unwrap());
        }
        let metadata = &state.metadata[&logical.payload];
        let selected = implementation_schedule(&logical, metadata, &state, &input.memo)
            .unwrap()
            .unwrap();
        assert_ne!(selected.payload, metadata.baseline_payload);
        for _ in 0..100 {
            assert_eq!(
                implementation_schedule(&logical, metadata, &state, &input.memo).unwrap(),
                Some(selected)
            );
        }
        assert_eq!(state.payloads.schedule_counters()[1].1, 1);
        assert_eq!(state.payloads.schedule_counters()[0].1, 100);
        let previous = input.memo.cost_epoch();
        input.memo.clear_cost_frontiers().unwrap();
        assert_ne!(previous, input.memo.cost_epoch());
        for (column, point) in columns.iter().zip([100, 2]) {
            input
                .memo
                .group_mut(child)
                .unwrap()
                .logical_properties
                .column_domains
                .insert(*column, GroupColumnDomain::new(Some(point), None).unwrap());
        }
        let next = implementation_schedule(&logical, metadata, &state, &input.memo)
            .unwrap()
            .unwrap();
        assert_eq!(next.payload, metadata.baseline_payload);
        assert_eq!(state.payloads.schedule_counters()[1].1, 2);
        assert!(
            state.payloads.get_physical(selected.payload).is_some(),
            "old exact candidates retain their frozen payload"
        );
    }

    #[test]
    fn physical_implementation_can_complete_while_a_logical_reader_is_held() {
        let input = two_column_filter();
        let state = input.planner_state.clone();
        let reader = state.read().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let classes = super::super::tests::test_grant_classes();
            let mut registry = ImplementationRegistry::default();
            implementation::register_implementations(
                &mut registry,
                input.planner_state.clone(),
                Arc::new(classes.iter().map(|class| (class.id, *class)).collect()),
                input.calibration.clone(),
                false,
            )
            .unwrap();
            let goal = OptimizationGoal {
                grant: GrantGoalKey::Class(classes[0].id),
                ..input.root_goal
            };
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let candidates = registry
                .implementation(PLANNER_BASELINE_IMPLEMENTATION)
                .unwrap()
                .candidates(
                    expression,
                    goal,
                    &ImplementationContext {
                        memo: &input.memo,
                        group: input.root,
                    },
                )
                .unwrap();
            send.send(candidates.len()).unwrap();
        });
        let completed = receive.recv_timeout(std::time::Duration::from_secs(5));
        // Release even on regression so the worker cannot strand the suite.
        drop(reader);
        worker.join().unwrap();
        assert!(completed.unwrap() > 0);
    }

    #[test]
    fn every_interrupted_scheduling_prefix_is_distinct_from_canonical_completion() {
        let input = two_column_filter();
        let logical = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let state = input.planner_state.read().unwrap();
        let mut calls = 0;
        let completed = permutation(
            &logical.key.scalars,
            &state,
            |_| None,
            || {
                calls += 1;
                Ok(true)
            },
        )
        .unwrap();
        assert!(completed.is_some());
        for prefix in 0..calls {
            let mut consumed = 0;
            let interrupted = permutation(
                &logical.key.scalars,
                &state,
                |_| None,
                || {
                    let admitted = consumed < prefix;
                    consumed += 1;
                    Ok(admitted)
                },
            )
            .unwrap();
            assert_eq!(
                interrupted, None,
                "interruption {prefix}/{calls} must not masquerade as canonical"
            );
        }
    }

    #[test]
    fn native_order_retains_fences_stable_ties_and_original_scalar_identities() {
        let column = Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
        );
        let constant = Expression::Constant(
            ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
        );
        let range = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::LessThan, column.clone(), constant.clone())
                .into(),
        );
        let equality = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, column, constant).into(),
        );
        let random = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .unwrap();
        let fence = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::LessThan,
                Expression::Function(
                    FunctionExpression::new(random, vec![], LogicalType::Double).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Double(0.5), LogicalType::Double).into(),
                ),
            )
            .into(),
        );
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                    Get::new_without_table(0, vec!["k".into()], vec![LogicalType::Integer]),
                ))),
                vec![
                    range.clone(),
                    equality.clone(),
                    fence,
                    range,
                    equality.clone(),
                    equality,
                ],
            )));
        let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let root = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let state = input.planner_state.read().unwrap();
        let before = (
            state.staging_arena.len(),
            state.scalars.len(),
            state.columns.len(),
        );
        // Exercise the scheduling policy independently of logical canonical
        // order (which intentionally no longer preserves insertion order).
        let mut roots = root.key.scalars.to_vec();
        let equality = roots[..2]
            .iter()
            .copied()
            .find(|root| {
                matches!(
                    state.scalars.get(*root).unwrap().kind,
                    crate::cascades::scalar::ScalarKind::Comparison(
                        crate::cascades::scalar::ComparisonOp::Equal
                    )
                )
            })
            .unwrap();
        let range = roots[..2]
            .iter()
            .copied()
            .find(|root| *root != equality)
            .unwrap();
        roots = vec![range, equality, roots[2], range, equality, equality];
        let order = permutation(
            &roots,
            &state,
            |_| None,
            || input.memo.control().checkpoint(),
        )
        .unwrap()
        .unwrap();
        let PredicateSchedule::Ordered(order) = order else {
            panic!("ordered")
        };
        assert_eq!(&*order.ordinals, &[1, 0, 2, 4, 5, 3]);
        let ordered = order.ordered_roots(&roots, &state.scalars).unwrap();
        assert_eq!(
            permutation(
                &ordered,
                &state,
                |_| None,
                || input.memo.control().checkpoint()
            )
            .unwrap(),
            Some(PredicateSchedule::Canonical)
        );
        assert_eq!(
            before,
            (
                state.staging_arena.len(),
                state.scalars.len(),
                state.columns.len()
            )
        );
    }
}
