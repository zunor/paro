// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native node batches enter the existing local settlement contract without
//! reconstructing their opaque Memo descendants as an owned plan.

use super::*;

pub(in crate::cascades::planner::transformation) struct SettledNative {
    pub(in crate::cascades::planner::transformation) expression: SettledExpression,
    pub(in crate::cascades::planner::transformation) holes:
        BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
    pub(in crate::cascades::planner::transformation) proofs:
        HashMap<PlanNodeId, Box<[EquivalenceProof]>>,
}

impl SettlementCache {
    pub(in crate::cascades::planner::transformation) fn settle_native_in(
        &mut self,
        shell: NativeShell,
        environment: &PlannerRuleEnvironment,
        arena: &mut LogicalPlanArena,
        identity: &mut PlannerResidentIdentity<'_>,
    ) -> Result<Option<SettledNative>> {
        let _b3 = crate::work_partition::enter_b3(crate::work_partition::Bucket::Settlement);
        let _site = crate::work_partition::cache_site(crate::work_partition::CacheSite::Native);
        let checkpoint = arena.checkpoint();
        let result = self.settle_native_impl(shell, environment, arena, identity);
        if !matches!(result, Ok(Some(_))) {
            let _b3 = crate::work_partition::enter_b3(crate::work_partition::Bucket::Rollback);
            arena.rollback_to(checkpoint)?;
            self.discard_stale_recipes(arena);
        }
        result
    }

    fn settle_native_impl(
        &mut self,
        shell: NativeShell,
        environment: &PlannerRuleEnvironment,
        arena: &mut LogicalPlanArena,
        identity: &mut PlannerResidentIdentity<'_>,
    ) -> Result<Option<SettledNative>> {
        let root = shell.root;
        let mut indices = Vec::with_capacity(shell.nodes.len());
        let mut holes = BTreeMap::new();
        let mut guard = GroupHoleTransportGuard {
            templates: BTreeMap::new(),
        };
        let mut source_proofs = BTreeMap::new();
        for node in shell.nodes {
            environment.session.cancellation.check()?;
            if !environment.control.checkpoint()? {
                return Ok(None);
            }
            let operator = node.operator.try_map_child_links(&mut |child| {
                let (group, id, stats, layout, reference) = match child {
                    NativeChild::Node(index) => {
                        return indices.get(index).copied().ok_or_else(|| {
                            paro_error::internal("native settlement requires post-order node edges")
                        });
                    }
                    NativeChild::MemoGroup {
                        group,
                        id,
                        stats,
                        layout,
                        reference,
                        ..
                    } => (group, id, stats, layout, reference),
                    NativeChild::Group { .. } => {
                        return Err(paro_error::internal(
                            "native settlement requires an explicit Memo operand identity",
                        ));
                    }
                };
                if reference.bindings != layout.bindings() || reference.types() != layout.types() {
                    return Err(paro_error::internal(
                        "native settlement has inconsistent boundary columns",
                    ));
                }
                if holes.insert(reference.reference_id, group).is_some() {
                    return Err(paro_error::internal(
                        "native settlement duplicated a Memo operand occurrence",
                    ));
                }
                guard.templates.insert(
                    reference.reference_id,
                    GroupHoleTransportTemplate {
                        bindings: reference.bindings.clone(),
                        types: reference.types().to_vec(),
                        facts: reference.facts.clone(),
                    },
                );
                arena.append(LogicalPlanNode {
                    id,
                    stats,
                    operator: LogicalOperator::BoundReference(reference),
                })
            })?;
            let index = arena.append(LogicalPlanNode {
                id: node.id,
                stats: node.stats,
                operator,
            })?;
            if !node.source_proofs.is_empty() {
                source_proofs.insert(index, node.source_proofs);
            }
            indices.push(index);
        }
        let root = *indices
            .get(root)
            .ok_or_else(|| paro_error::internal("native settlement lost its root"))?;
        let mut proofs = HashMap::<PlanNodeId, Box<[EquivalenceProof]>>::new();
        let Some(expression) =
            self.settle_root_in(root, environment, arena, identity, |source, output, arena| {
                if let Some(inherited) = source_proofs.get(&source) {
                    if matches!(arena.get(output)?.operator, LogicalOperator::BoundReference(_)) {
                        // Staging consumes an opaque operand without creating
                        // a logical expression on which to attach new proof
                        // lineage. Do not silently discard an adopted proof.
                        return Err(paro_error::internal(
                            "native settlement cannot erase an adopted proof into an opaque operand",
                        ));
                    }
                    // Proofs follow this exact settled occurrence, not a reused
                    // recipe id or another consumer's column namespace.
                    let id = arena.get(output)?.id;
                    let entry = proofs.entry(id).or_default();
                    let mut joined = entry.to_vec();
                    for proof in inherited.iter() {
                        if !joined.contains(proof) {
                            joined.push(proof.clone());
                        }
                    }
                    *entry = joined.into_boxed_slice();
                }
                Ok(())
            })?
        else {
            return Ok(None);
        };
        guard.validate_arena(&arena.plan(expression.plan)?)?;
        Ok(Some(SettledNative {
            expression,
            holes,
            proofs,
        }))
    }

    #[cfg(test)]
    pub(super) fn settle_native_test_in(
        &mut self,
        shell: NativeShell,
        environment: &PlannerRuleEnvironment,
        arena: &mut LogicalPlanArena,
    ) -> Result<Option<SettledNative>> {
        self.with_test_identity(|cache, identity| {
            cache.settle_native_in(shell, environment, arena, identity)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_planner::expression::ConstantExpression;
    use paro_planner::operator::ExpressionGet;

    fn environment() -> PlannerRuleEnvironment {
        PlannerRuleEnvironment {
            control: Arc::new(crate::cascades::control::SearchControl::new(None)),
            bind_context: BindContext::new(),
            session: paro_context::TestStatementContextBuilder::minimal().build(),
            cost_model: Default::default(),
            budget: SearchBudget::default(),
            verify_enabled: false,
        }
    }

    fn values(environment: &PlannerRuleEnvironment, rows: usize) -> NativeShell {
        NativeShell {
            root: 0,
            nodes: vec![NativeNode {
                id: environment.bind_context.next_plan_id(),
                stats: NodeStats::default(),
                operator: LogicalOperator::ExpressionGet(ExpressionGet::new(
                    0,
                    (0..rows)
                        .map(|row| {
                            vec![Expression::Constant(
                                ConstantExpression::new(
                                    Value::Integer(row as i32),
                                    LogicalType::Integer,
                                )
                                .into(),
                            )]
                        })
                        .collect(),
                    vec!["key".into()],
                    vec![LogicalType::Integer],
                )),
                source_proofs: Box::new([]),
            }]
            .into_boxed_slice(),
        }
    }

    #[test]
    fn native_local_reuse_preserves_occurrences_and_rederives_changed_input() {
        let environment = environment();
        let mut arena = LogicalPlanArena::default();
        let mut cache = SettlementCache::default();
        let empty = arena.checkpoint();
        let first = cache
            .settle_native_test_in(values(&environment, 4), &environment, &mut arena)
            .unwrap()
            .unwrap();
        let first_node = arena.get(first.expression.plan).unwrap().clone();
        let misses = cache.misses;
        let next = cache
            .settle_native_test_in(values(&environment, 4), &environment, &mut arena)
            .unwrap()
            .unwrap();
        let next_node = arena.get(next.expression.plan).unwrap();
        assert_eq!(cache.misses, misses);
        assert_eq!(cache.hits, 1);
        assert_ne!(first_node.id, next_node.id);
        assert_eq!(first_node.stats, next_node.stats);
        let changed = cache
            .settle_native_test_in(values(&environment, 9), &environment, &mut arena)
            .unwrap()
            .unwrap();
        assert_eq!(
            arena
                .get(changed.expression.plan)
                .unwrap()
                .stats
                .estimated_cardinality
                .unwrap()
                .expected,
            9
        );
        assert!(cache.misses > misses);
        arena.rollback_to(empty).unwrap();
        cache.discard_stale_recipes(&arena);
        assert!(cache.locals.is_empty());
        let rebuilt = cache
            .settle_native_test_in(values(&environment, 4), &environment, &mut arena)
            .unwrap()
            .unwrap();
        assert!(!arena.owns(first.expression.plan));
        assert_eq!(
            arena.get(rebuilt.expression.plan).unwrap().stats,
            first_node.stats
        );
    }

    #[test]
    fn native_interruption_leaves_no_partial_arena_or_cache() {
        let mut environment = environment();
        environment.control = Arc::new(crate::cascades::control::SearchControl::new(Some(
            std::time::Duration::ZERO,
        )));
        environment.control.begin_optional();
        let mut arena = LogicalPlanArena::default();
        let mut cache = SettlementCache::default();
        assert!(cache
            .settle_native_test_in(values(&environment, 4), &environment, &mut arena)
            .unwrap()
            .is_none());
        assert!(arena.is_empty());
        assert!(cache.locals.is_empty());
        environment.control = Arc::new(crate::cascades::control::SearchControl::new(None));
        let mut invalid = values(&environment, 4).nodes.into_vec();
        invalid.push(NativeNode {
            id: environment.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Filter(Filter {
                child: NativeChild::Node(2), expressions: vec![],
                projection_map: paro_planner::operator::ProjectionMap::all(),
            }),
            source_proofs: Box::new([]),
        });
        assert!(cache
            .settle_native_test_in(
                NativeShell {
                    nodes: invalid.into_boxed_slice(),
                    root: 1
                },
                &environment,
                &mut arena
            )
            .is_err());
        assert!(arena.is_empty());
        assert!(cache.locals.is_empty());
    }
}
