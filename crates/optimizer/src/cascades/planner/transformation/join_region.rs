// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact identities for associative join-graph enumeration problems.

use super::*;

/// Internal join order is not an input to the graph enumerator. Atomic
/// boundaries, predicates and consumed facts are. Retain exact bytes rather
/// than using a digest as evidence that two enumeration problems are equal.
#[cfg(test)]
pub(super) fn identity(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<Option<Box<[u8]>>> {
    identity_with_facts(binding, memo, state, None)
}

pub(super) fn identity_with_facts(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: Option<&boundary::BoundarySnapshot>,
) -> Result<Option<Box<[u8]>>> {
    struct Graph {
        atoms: Vec<Box<[u8]>>,
        predicates: Vec<u64>,
        joins: usize,
        inputs: BTreeSet<GroupId>,
    }
    fn atom(
        operand: &PatternOperand,
        memo: &Memo,
        output: &mut StableFingerprintBuilder,
    ) -> Result<()> {
        match operand {
            PatternOperand::Group(group) => {
                output.write_u64(0);
                output.write_u64(memo.canonical_group(*group).0 as u64);
            }
            PatternOperand::Expression {
                group,
                expression,
                children,
            } => {
                let logical = memo
                    .logical_expr(*expression)
                    .ok_or_else(|| paro_error::internal("join atom lost its expression"))?;
                let encoding = logical.operator_encoding.as_deref().ok_or_else(|| {
                    paro_error::internal("join atom has no exact operator encoding")
                })?;
                output.write_u64(1);
                output.write_u64(memo.canonical_group(*group).0 as u64);
                output.write_u64(encoding.len() as u64);
                output.write_bytes(encoding);
                output.write_u64(logical.key.scalars.len() as u64);
                for scalar in &logical.key.scalars {
                    output.write_u64(scalar.0 as u64);
                }
                output.write_u64(children.len() as u64);
                for child in children {
                    atom(child, memo, output)?;
                }
            }
        }
        Ok(())
    }
    fn visit(
        operand: &PatternOperand,
        memo: &Memo,
        state: &PlannerTransformState,
        graph: &mut Graph,
    ) -> Result<()> {
        if let PatternOperand::Expression {
            expression,
            children,
            ..
        } = operand
        {
            let logical = memo
                .logical_expr(*expression)
                .ok_or_else(|| paro_error::internal("join binding lost its expression"))?;
            let operator = &state
                .payloads
                .logical
                .get(logical.payload.index())
                .ok_or_else(|| paro_error::internal("join binding lost its operator"))?
                .semantic_template
                .operator;
            match operator {
                LogicalOperator::Join(join @ Join::Comparison(comparison))
                    if comparison.join_type == JoinType::Inner && crate::join_order::relation_manager::RelationManager::join_is_reorderable(join)
                        && logical.key.scalars.len() == comparison.conditions.len() => {
                    graph.predicates.extend(logical.key.scalars.iter().map(|scalar| scalar.0 as u64));
                    graph.joins += 1;
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                LogicalOperator::Join(join @ Join::Cross(_)) if crate::join_order::relation_manager::RelationManager::join_is_reorderable(join) => {
                    graph.joins += 1;
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                LogicalOperator::Filter(filter) if filter.expressions.iter().all(|expression| !expression.evaluation_properties().is_reorder_fence()) => {
                    graph.predicates.extend(logical.key.scalars.iter().map(|scalar| scalar.0 as u64));
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                _ => {}
            }
        }
        let mut encoded = StableFingerprintBuilder::recording();
        atom(operand, memo, &mut encoded)?;
        graph.atoms.push(encoded.finish_recording().1);
        let owner = match operand {
            PatternOperand::Group(group) | PatternOperand::Expression { group, .. } => *group,
        };
        graph.inputs.insert(memo.canonical_group(owner));
        let mut inputs = vec![operand];
        while let Some(input) = inputs.pop() {
            match input {
                PatternOperand::Group(group) => {
                    graph.inputs.insert(memo.canonical_group(*group));
                }
                PatternOperand::Expression { children, .. } => inputs.extend(children.iter()),
            }
        }
        Ok(())
    }
    let mut graph = Graph {
        atoms: Vec::new(),
        predicates: Vec::new(),
        joins: 0,
        inputs: BTreeSet::new(),
    };
    visit(binding, memo, state, &mut graph)?;
    if graph.joins == 0 {
        return Ok(None);
    }
    graph.atoms.sort();
    graph.predicates.sort();
    let mut encoder = StableFingerprintBuilder::recording();
    encoder.write_bytes(b"paro.memo.join-graph-input.v1");
    encoder.write_u64(graph.atoms.len() as u64);
    for atom in &graph.atoms {
        encoder.write_u64(atom.len() as u64);
        encoder.write_bytes(atom);
    }
    encoder.write_u64(graph.predicates.len() as u64);
    for predicate in &graph.predicates {
        encoder.write_u64(*predicate);
    }
    // JoinGraph computes internal cardinalities from its atomic inputs. The
    // old binary tree's intermediate estimates are not enumeration inputs.
    let root = match binding {
        PatternOperand::Group(group) | PatternOperand::Expression { group, .. } => *group,
    };
    graph.inputs.insert(memo.canonical_group(root));
    for group in graph.inputs {
        let read = PatternRead::facts_from_group(memo, group)?;
        encoder.write_u64(group.0 as u64);
        if let Some(facts) = facts {
            // The reader already resolved inherited and producer evidence.
            // Recipe ids and input-list growth are invalidation cursors, not
            // inputs to enumeration. Equal fact values retain the same graph
            // problem even after its evidence DAG gains another derivation.
            facts.encode_group(group, &mut encoder)?;
        } else {
            encoder.write_fingerprint(read.logical_fact_fingerprint);
            encoder.write_fingerprint(read.statistics_snapshot_fingerprint);
        }
    }
    Ok(Some(encoder.finish_recording().1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ComparisonJoin, Get, JoinCondition};

    fn scan(table: usize) -> LogicalPlan {
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Get(Get::new_without_table(
            table,
            vec!["k".into()],
            vec![LogicalType::BigInt],
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
        plan
    }
    fn join(left: LogicalPlan, right: LogicalPlan, a: usize, b: usize) -> LogicalPlan {
        let column = |table| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table, 0),
                LogicalType::BigInt,
            ))
        };
        LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                left,
                right,
                vec![JoinCondition::new(
                    column(a),
                    column(b),
                    JoinComparisonType::Equal,
                )],
            ),
        )))
    }

    #[test]
    fn associative_graph_identity_preserves_predicates_and_input_statistics() {
        let left = join(join(scan(0), scan(1), 0, 1), scan(2), 1, 2);
        let right = join(scan(0), join(scan(1), scan(2), 2, 1), 1, 0);
        let mut input = MemoBuilder::build_alternatives(
            vec![
                LogicalAlternative {
                    plan: left,
                    source: AlternativeOrigin::Baseline,
                    column_stats: Arc::new(HashMap::new()),
                },
                LogicalAlternative {
                    plan: right,
                    source: AlternativeOrigin::Specialized {
                        rule: JOIN_REGION_ENUMERATION_RULE,
                    },
                    column_stats: Arc::new(HashMap::new()),
                },
            ],
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let state = input.planner_state.read().unwrap();
        let mut scans = BTreeMap::new();
        let leaves = input
            .memo
            .groups()
            .filter_map(|group| {
                let logical = input.memo.logical_expr(group.logical_exprs()[0])?;
                let LogicalOperator::Get(get) = &state.payloads.logical[logical.payload.index()]
                    .semantic_template
                    .operator
                else {
                    return None;
                };
                Some((get.table_index, group.id))
            })
            .collect::<Vec<_>>();
        for (table, group) in leaves {
            if let Some(previous) = scans.insert(table, group) {
                input.memo.merge_groups(previous, group).unwrap();
            }
        }
        let expressions = input
            .memo
            .group(input.root)
            .unwrap()
            .logical_exprs()
            .to_vec();
        assert_eq!(expressions.len(), 2);
        let bindings = expressions
            .iter()
            .map(|expression| {
                matching::scoped_pattern_bindings(
                    PlannerTransformation::JoinRegionEnumeration,
                    input.root,
                    *expression,
                    &input.memo,
                    &state,
                    None,
                    BudgetDimension::RuleWorkPerGroup,
                )
                .unwrap()
                .bindings[0]
                    .clone()
            })
            .collect::<Vec<_>>();
        let first = identity(&bindings[0].root, &input.memo, &state)
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            identity(&bindings[1].root, &input.memo, &state)
                .unwrap()
                .unwrap()
        );
        let leaf = input
            .memo
            .groups()
            .find(|group| {
                group.logical_exprs().iter().any(|expr| {
                    let logical = input.memo.logical_expr(*expr).unwrap();
                    matches!(
                        state.payloads.logical[logical.payload.index()]
                            .semantic_template
                            .operator,
                        LogicalOperator::Get(_)
                    )
                })
            })
            .unwrap()
            .id;
        fn native_identity(
            memo: &mut Memo,
            state: &PlannerTransformState,
            root: GroupId,
            binding: &PatternOperand,
        ) -> Box<[u8]> {
            let mut ctx = TransformContext::new(memo, root);
            let facts = boundary::BoundarySnapshot::read(
                &mut ctx,
                state,
                binding,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .unwrap();
            identity_with_facts(binding, ctx.memo(), state, Some(&facts))
                .unwrap()
                .unwrap()
        }
        let native = native_identity(&mut input.memo, &state, input.root, &bindings[0].root);
        input.memo.group_mut(leaf).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(776),
            CardinalityRecipeKind::Statistics,
            100,
            100,
            100,
        );
        assert_eq!(
            native,
            native_identity(&mut input.memo, &state, input.root, &bindings[0].root),
            "changing a recipe without changing its value is not a new graph problem"
        );
        input.memo.group_mut(leaf).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(777),
            CardinalityRecipeKind::Statistics,
            0,
            80,
            100,
        );
        assert_ne!(
            first,
            identity(&bindings[0].root, &input.memo, &state)
                .unwrap()
                .unwrap()
        );
        assert_ne!(
            native,
            native_identity(&mut input.memo, &state, input.root, &bindings[0].root)
        );
    }
}
