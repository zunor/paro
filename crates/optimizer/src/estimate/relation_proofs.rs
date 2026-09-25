// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Operator-local semantic key/domain algebra. Estimates never enter this
//! module. CTE publication uses definition identities, not compacted ordinals.

use paro_planner::expression::Expression;
use paro_planner::logical::operator::cte::{CteColumnId, CteOutputColumn};
use paro_planner::logical::operator::{LogicalOperator, LogicalOutputLayout, SetOpType};
use paro_planner::logical::plan::finite_domain::{DomainValue, FiniteDomains, MAX_DOMAIN_VALUES};
use paro_planner::logical::plan::{LogicalPlanPostOrderFolder, OwnedLogicalPlan};
use paro_planner::logical::plan::{NodeStats, UniqueKey, UniqueKeyColumn, UniqueKeyProvenance};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) struct Input<'a> {
    pub keys: &'a [UniqueKey],
    pub domains: &'a FiniteDomains,
}

#[derive(Default)]
pub(crate) struct RelationProofs {
    ctes: HashMap<usize, (Vec<CteOutputColumn>, Vec<UniqueKey>, FiniteDomains)>,
}

impl RelationProofs {
    pub fn publish(&mut self, index: usize, columns: &[CteOutputColumn], stats: &NodeStats) {
        self.ctes.insert(
            index,
            (
                columns.to_vec(),
                stats.unique_keys.clone(),
                stats.finite_domains.clone(),
            ),
        );
    }

    pub fn derive<Child>(
        &self,
        operator: &LogicalOperator<Child>,
        output: &LogicalOutputLayout,
        layouts: &[LogicalOutputLayout],
        inputs: &[Input<'_>],
        stats: &mut NodeStats,
    ) {
        let child_keys = inputs.iter().map(|i| i.keys).collect::<Vec<_>>();
        stats.unique_keys = super::unique_keys::derive_unique_keys_from_facts(
            operator,
            output,
            &layouts.iter().collect::<Vec<_>>(),
            &child_keys,
        );
        let domains = |index: usize| inputs.get(index).map(|i| i.domains);
        let expression_domain = |expression: &Expression, index: usize| match expression {
            Expression::Constant(c) => {
                DomainValue::from_value(&c.value).map(|v| BTreeSet::from([v]))
            }
            Expression::ColumnRef(c) if c.depth == 0 => domains(index)?.get(&c.binding).cloned(),
            _ => None,
        };
        let mut result = FiniteDomains::new();
        match operator {
            LogicalOperator::BoundReference(r) => result.clone_from(&r.facts.finite_domains),
            LogicalOperator::Projection(p) => {
                for (binding, expression) in output.bindings().iter().zip(&p.expressions) {
                    if let Some(domain) = expression_domain(expression, 0) {
                        result.insert(*binding, domain);
                    }
                }
            }
            LogicalOperator::Aggregate(a) if a.has_plain_grouping_domain() => {
                for (binding, expression) in output.bindings().iter().zip(&a.groups) {
                    if let Some(domain) = expression_domain(expression, 0) {
                        result.insert(*binding, domain);
                    }
                }
            }
            LogicalOperator::Filter(f) => {
                result = domains(0).cloned().unwrap_or_default();
                for expression in &f.expressions {
                    refine(expression, &mut result);
                }
            }
            LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::Window(_)
            | LogicalOperator::RowFetch(_)
            | LogicalOperator::ExternalProject(_) => {
                result = domains(0).cloned().unwrap_or_default()
            }
            LogicalOperator::MaterializedCTE(_) => result = domains(1).cloned().unwrap_or_default(),
            LogicalOperator::Join(paro_planner::logical::operator::Join::Comparison(j))
                if j.join_type == paro_planner::logical::operator::JoinType::Inner =>
            {
                for input in inputs {
                    result.extend(input.domains.clone());
                }
            }
            LogicalOperator::SetOperation(s) if s.setop_type == SetOpType::Union => {
                if let (Some(left), Some(right), Some(ll), Some(rl)) =
                    (domains(0), domains(1), layouts.first(), layouts.get(1))
                {
                    let mut separators = Vec::new();
                    for (ordinal, binding) in output.bindings().iter().enumerate() {
                        if output.types()[ordinal].collation().is_some() {
                            continue;
                        }
                        if let Some((l, r)) = ll
                            .bindings()
                            .get(ordinal)
                            .and_then(|b| left.get(b))
                            .zip(rl.bindings().get(ordinal).and_then(|b| right.get(b)))
                        {
                            if l.is_disjoint(r) {
                                separators.push(ordinal);
                            }
                            let values = l.union(r).cloned().collect::<BTreeSet<_>>();
                            if values.len() <= MAX_DOMAIN_VALUES {
                                result.insert(*binding, values);
                            }
                        }
                    }
                    // A branch discriminator alone is not a key. Both branch
                    // keys must be covered in the resulting tuple as well.
                    for separator in separators {
                        for left_key in inputs[0].keys {
                            for right_key in inputs[1].keys {
                                if !left_key
                                    .columns
                                    .iter()
                                    .all(|c| ll.bindings().get(c.output_index) == Some(&c.binding))
                                    || !right_key.columns.iter().all(|c| {
                                        rl.bindings().get(c.output_index) == Some(&c.binding)
                                    })
                                {
                                    continue;
                                }
                                let indices = left_key
                                    .columns
                                    .iter()
                                    .chain(right_key.columns.iter())
                                    .map(|c| c.output_index)
                                    .chain([separator])
                                    .collect::<BTreeSet<_>>();
                                if indices.iter().all(|i| *i < output.len()) {
                                    stats.unique_keys.push(UniqueKey::new(
                                        indices.into_iter().map(|i| UniqueKeyColumn {
                                            output_index: i,
                                            binding: output.bindings()[i],
                                        }),
                                        UniqueKeyProvenance::Structural,
                                        left_key.null_semantics.max(right_key.null_semantics),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            LogicalOperator::CTERef(r) => {
                if let Some((columns, keys, source_domains)) = self.ctes.get(&r.cte_index) {
                    let source = columns
                        .iter()
                        .map(|c| (c.binding, c.definition))
                        .collect::<BTreeMap<_, _>>();
                    let target = r
                        .definition_columns
                        .iter()
                        .copied()
                        .zip(output.bindings().iter().copied())
                        .collect::<BTreeMap<CteColumnId, _>>();
                    let mapping = source
                        .into_iter()
                        .filter_map(|(binding, definition)| {
                            target.get(&definition).map(|to| (binding, *to))
                        })
                        .collect::<BTreeMap<_, _>>();
                    stats.unique_keys = keys
                        .iter()
                        .filter_map(|key| {
                            let columns = key
                                .columns
                                .iter()
                                .map(|c| {
                                    let binding = *mapping.get(&c.binding)?;
                                    Some(UniqueKeyColumn {
                                        output_index: output
                                            .bindings()
                                            .iter()
                                            .position(|b| *b == binding)?,
                                        binding,
                                    })
                                })
                                .collect::<Option<Vec<_>>>()?;
                            Some(UniqueKey::new(
                                columns,
                                UniqueKeyProvenance::Structural,
                                key.null_semantics,
                            ))
                        })
                        .collect();
                    result = source_domains
                        .iter()
                        .filter_map(|(b, d)| mapping.get(b).map(|to| (*to, d.clone())))
                        .collect();
                }
            }
            _ => {}
        }
        result.retain(|b, _| {
            output
                .bindings()
                .iter()
                .position(|c| c == b)
                .is_some_and(|i| output.types()[i].collation().is_none())
        });
        // Removing a non-NULL constant key component is valid under either
        // NULL equality contract. Never infer dependencies between other
        // grouping columns, and never use estimated NDV=1 as a constant.
        for key in &mut stats.unique_keys {
            key.columns = key
                .columns
                .iter()
                .filter(|c| {
                    !result
                        .get(&c.binding)
                        .is_some_and(|d| d.len() == 1 && !d.contains(&DomainValue::Null))
                })
                .copied()
                .collect();
        }
        super::unique_keys::normalize_unique_keys(&mut stats.unique_keys);
        stats.finite_domains = result;
    }
}

fn refine(expression: &Expression, domains: &mut FiniteDomains) {
    if let Expression::Conjunction(c) = expression {
        if c.conjunction_type == paro_planner::expression::ConjunctionType::And {
            for child in &c.children {
                refine(child, domains);
            }
            return;
        }
    }
    let Some((binding, values)) = super::annotate::relation::finite_equality_domain(expression)
    else {
        return;
    };
    let Some(values) = values
        .iter()
        .map(DomainValue::from_value)
        .collect::<Option<BTreeSet<_>>>()
    else {
        return;
    };
    if values.len() > MAX_DOMAIN_VALUES {
        return;
    }
    domains
        .entry(binding)
        .and_modify(|old| *old = old.intersection(&values).cloned().collect())
        .or_insert(values);
}

impl LogicalPlanPostOrderFolder<LogicalOutputLayout> for RelationProofs {
    fn child_completed(
        &mut self,
        parent: &paro_planner::logical::plan::arena::LogicalPlanNode<()>,
        completed: &[Box<OwnedLogicalPlan>],
        _: &[LogicalOutputLayout],
        _: &[Box<OwnedLogicalPlan>],
    ) -> paro_common::error::Result<()> {
        if let (LogicalOperator::MaterializedCTE(cte), [producer]) = (&parent.operator, completed) {
            self.publish(cte.cte_index, &cte.output_columns, &producer.stats);
        }
        Ok(())
    }
    fn fold(
        &mut self,
        mut plan: OwnedLogicalPlan,
        layouts: Vec<LogicalOutputLayout>,
    ) -> paro_common::error::Result<(OwnedLogicalPlan, LogicalOutputLayout)> {
        let output = plan.operator.output_layout_from_children(&layouts);
        let inputs = plan
            .operator
            .children()
            .into_iter()
            .map(|c| Input {
                keys: &c.stats.unique_keys,
                domains: &c.stats.finite_domains,
            })
            .collect::<Vec<_>>();
        self.derive(&plan.operator, &output, &layouts, &inputs, &mut plan.stats);
        Ok((plan, output))
    }
}
pub(crate) fn refresh(plan: OwnedLogicalPlan) -> paro_common::error::Result<OwnedLogicalPlan> {
    plan.try_fold_post_order_with(&mut RelationProofs::default())
        .map(|(plan, _)| plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::logical::operator::{CTERef, ColumnBinding, SetOperation};
    use paro_planner::logical::plan::UniqueKeyNullSemantics;

    fn layout(table: usize) -> LogicalOutputLayout {
        LogicalOutputLayout::new(
            vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
            (0..3).map(|i| ColumnBinding::new(table, i)).collect(),
        )
    }
    fn key(layout: &LogicalOutputLayout) -> UniqueKey {
        UniqueKey::new(
            (0..2).map(|i| UniqueKeyColumn {
                output_index: i,
                binding: layout.bindings()[i],
            }),
            UniqueKeyProvenance::Structural,
            UniqueKeyNullSemantics::NullsEqual,
        )
    }
    fn union(tag: &str) -> NodeStats {
        union_with_collation(tag, false)
    }

    fn union_with_collation(tag: &str, collated: bool) -> NodeStats {
        let l = layout(1);
        let r = layout(2);
        let output = if collated {
            LogicalOutputLayout::new(
                vec![
                    LogicalType::Integer,
                    LogicalType::Integer,
                    LogicalType::varchar_collation("NOCASE"),
                ],
                layout(3).bindings().to_vec(),
            )
        } else {
            layout(3)
        };
        let ld = FiniteDomains::from([(
            l.bindings()[2],
            BTreeSet::from([DomainValue::String("s".into())]),
        )]);
        let rd = FiniteDomains::from([(
            r.bindings()[2],
            BTreeSet::from([DomainValue::String(tag.into())]),
        )]);
        let lk = [key(&l)];
        let rk = [key(&r)];
        let operator = LogicalOperator::SetOperation(SetOperation {
            table_index: 3,
            column_count: 3,
            left: (),
            right: (),
            setop_type: SetOpType::Union,
            setop_all: true,
            allow_out_of_order: true,
            types: output.types().to_vec(),
        });
        let mut stats = NodeStats::default();
        RelationProofs::default().derive(
            &operator,
            &output,
            &[l, r],
            &[
                Input {
                    keys: &lk,
                    domains: &ld,
                },
                Input {
                    keys: &rk,
                    domains: &rd,
                },
            ],
            &mut stats,
        );
        stats
    }

    #[test]
    fn disjoint_union_requires_complete_branch_keys() {
        let result = union("c");
        assert_eq!(result.unique_keys.len(), 1);
        assert_eq!(result.unique_keys[0].columns.len(), 3);
        // Knowing the first attribute does not determine the second one.
        assert!(!result.unique_keys.iter().any(|k| k.columns.len() == 1));
        assert!(
            union("s").unique_keys.is_empty(),
            "overlapping branch tags cannot prove uniqueness"
        );
        assert!(
            union_with_collation("S", true).unique_keys.is_empty(),
            "byte-distinct tags do not prove disjointness under a collation"
        );
    }

    #[test]
    fn only_proven_non_null_constants_reduce_composite_keys() {
        let output = layout(1);
        let keys = [key(&output)];
        for (value, expected_width) in [(DomainValue::Integer(2001), 1), (DomainValue::Null, 2)] {
            let domains = FiniteDomains::from([(output.bindings()[1], BTreeSet::from([value]))]);
            let operator = LogicalOperator::Filter(paro_planner::logical::operator::Filter {
                expressions: vec![],
                child: (),
                projection_map: paro_planner::logical::operator::ProjectionMap::all(),
            });
            let mut stats = NodeStats::default();
            RelationProofs::default().derive(
                &operator,
                &output,
                std::slice::from_ref(&output),
                &[Input {
                    keys: &keys,
                    domains: &domains,
                }],
                &mut stats,
            );
            assert_eq!(stats.unique_keys[0].columns.len(), expected_width);
        }
    }

    #[test]
    fn cte_keys_follow_definition_identity_not_reader_positions() {
        let producer = union("c");
        let source = layout(3);
        let output = layout(9);
        let mut proofs = RelationProofs::default();
        let columns = source
            .bindings()
            .iter()
            .enumerate()
            .map(|(i, b)| CteOutputColumn {
                definition: CteColumnId(i),
                binding: *b,
            })
            .collect::<Vec<_>>();
        proofs.publish(7, &columns, &producer);
        let mut reference = CTERef::new(7, 9, "r".into(), vec![], output.types().to_vec());
        reference.definition_columns = vec![CteColumnId(2), CteColumnId(0), CteColumnId(1)];
        let mut stats = NodeStats::default();
        proofs.derive(
            &LogicalOperator::<()>::CTERef(reference.clone()),
            &output,
            &[],
            &[],
            &mut stats,
        );
        assert_eq!(stats.unique_keys.len(), 1);
        assert_eq!(
            stats
                .finite_domains
                .get(&output.bindings()[0])
                .unwrap()
                .len(),
            2
        );
        reference.definition_columns.pop();
        proofs.derive(
            &LogicalOperator::<()>::CTERef(reference),
            &output,
            &[],
            &[],
            &mut stats,
        );
        assert!(
            stats.unique_keys.is_empty(),
            "pruning a key column loses the proof"
        );
    }
}
