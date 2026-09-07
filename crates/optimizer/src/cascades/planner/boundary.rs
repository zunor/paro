// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fact-backed Memo boundaries. No logical tree or representative is built.

use super::*;
use paro_planner::operator::bound_reference::{BoundRelationFacts, BoundSourceColumn};
use paro_planner::plan::{UniqueKey, UniqueKeyColumn, UniqueKeyProvenance};

#[cfg(test)]
mod tests;

#[derive(Debug, Default, PartialEq, Eq)]
struct GroupFacts {
    relational: bool,
    unique_keys: BTreeSet<Box<[ColumnId]>>,
    cardinality: Option<CardinalityEnvelope>,
    lineage: BTreeMap<ColumnId, Option<Vec<BoundSourceColumn>>>,
    control: bool,
}

pub(super) struct BoundarySnapshot {
    groups: BTreeMap<GroupId, Arc<GroupFacts>>,
}

#[derive(Debug, Default)]
pub(super) struct BoundaryFactCache {
    entries: BTreeMap<(GroupId, bool), CachedFacts>,
}

#[derive(Debug)]
struct CachedFacts {
    read: PatternRead,
    inputs: Vec<(GroupId, Option<Arc<GroupFacts>>)>,
    facts: Arc<GroupFacts>,
}

impl CachedFacts {
    fn matches(&self, read: PatternRead, inputs: &[(GroupId, Option<Arc<GroupFacts>>)]) -> bool {
        self.read == read
            && self.inputs.len() == inputs.len()
            && self
                .inputs
                .iter()
                .zip(inputs)
                .all(|((left, a), (right, b))| {
                    left == right
                        && match (a, b) {
                            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                            (None, None) => true,
                            _ => false,
                        }
                })
    }
}

impl BoundarySnapshot {
    /// Encode resolved boundary evidence, not internal binary join shape or
    /// the revisions of unrelated alternatives. A changed inherited input
    /// invalidates graph reuse exactly when its consumed facts change.
    pub(super) fn encode_group(
        &self,
        group: GroupId,
        encoder: &mut StableFingerprintBuilder,
    ) -> Result<()> {
        let facts = self
            .groups
            .get(&group)
            .ok_or_else(|| paro_error::internal("graph identity read unobserved facts"))?;
        encoder.write_u64(facts.cardinality.is_some() as u64);
        if let Some(range) = facts.cardinality {
            for value in [
                range.lower,
                range.expected_lower,
                range.expected_upper,
                range.upper,
            ] {
                encoder.write_u64(value);
            }
        }
        encoder.write_u64(facts.control as u64);
        encoder.write_u64(facts.unique_keys.len() as u64);
        for key in &facts.unique_keys {
            encoder.write_u64(key.len() as u64);
            for column in key {
                encoder.write_u64(column.0 as u64);
            }
        }
        encoder.write_u64(facts.lineage.len() as u64);
        for (column, sources) in &facts.lineage {
            encoder.write_u64(column.0 as u64);
            encoder.write_u64(sources.is_some() as u64);
            if let Some(sources) = sources {
                encoder.write_u64(sources.len() as u64);
                for source in sources {
                    for value in [source.source, source.occurrence, source.column] {
                        encoder.write_u64(value as u64);
                    }
                    encoder.write_u64(source.rows.is_some() as u64);
                    if let Some(rows) = source.rows {
                        for value in [rows.min, rows.expected, rows.max] {
                            encoder.write_u64(value);
                        }
                    }
                    encoder.write_u64(source.distinct.is_some() as u64);
                    encoder.write_u64(source.distinct.unwrap_or(0));
                    encoder.write_u64(source.unique as u64);
                }
            }
        }
        Ok(())
    }
    /// Traverse the evidence DAG once, admitting every group, shell, edge and
    /// output column before allocating its result. Cycles cannot establish
    /// source coverage; a missing child witness is conservatively unknown.
    pub(super) fn read(
        ctx: &mut TransformContext<'_>,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        dimension: BudgetDimension,
    ) -> Result<Option<Self>> {
        let mut pending = Vec::new();
        let mut operands = vec![binding];
        while let Some(operand) = operands.pop() {
            if !ctx.admit_fact_work(dimension, 1)? {
                return Ok(None);
            }
            let (group, relational) = match operand {
                PatternOperand::Group(group) => (*group, true),
                PatternOperand::Expression {
                    group, children, ..
                } => {
                    operands.extend(children.iter());
                    (*group, false)
                }
            };
            pending.push((ctx.memo().canonical_group(group), relational, false));
        }
        let mut active = BTreeSet::new();
        let mut result = Self {
            groups: BTreeMap::new(),
        };
        while let Some((group, relational, finish)) = pending.pop() {
            if let Some(session) = &state.session {
                session.cancellation.check()?;
            }
            if result
                .groups
                .get(&group)
                .is_some_and(|facts| facts.relational || !relational)
            {
                continue;
            }
            if finish {
                let input_count = ctx
                    .memo()
                    .cardinality_dependencies(group)
                    .count()
                    .saturating_add(if relational {
                        ctx.memo()
                            .group(group)
                            .unwrap()
                            .logical_exprs()
                            .iter()
                            .map(|expression| {
                                ctx.memo()
                                    .logical_expr(*expression)
                                    .unwrap()
                                    .key
                                    .children
                                    .len()
                            })
                            .sum::<usize>()
                    } else {
                        0
                    });
                if !ctx.admit_fact_work(dimension, 1 + input_count)? {
                    return Ok(None);
                }
                let mut input_ids = BTreeSet::new();
                if relational {
                    for expression in ctx.memo().group(group).unwrap().logical_exprs() {
                        input_ids.extend(
                            ctx.memo()
                                .logical_expr(*expression)
                                .unwrap()
                                .key
                                .children
                                .iter()
                                .map(|group| ctx.memo().canonical_group(*group)),
                        );
                    }
                }
                input_ids.extend(
                    ctx.memo()
                        .cardinality_dependencies(group)
                        .map(|(group, _)| ctx.memo().canonical_group(group)),
                );
                let inputs = input_ids
                    .into_iter()
                    .map(|id| (id, result.groups.get(&id).cloned()))
                    .collect::<Vec<_>>();
                let read = if relational {
                    PatternRead::from_group(ctx.memo(), group)?
                } else {
                    PatternRead::facts_from_group(ctx.memo(), group)?
                };
                let mut cache = state
                    .boundary_cache
                    .lock()
                    .expect("boundary fact cache poisoned");
                if let Some(cached) = cache
                    .entries
                    .get(&(group, relational))
                    .filter(|cached| cached.matches(read, &inputs))
                {
                    result.groups.insert(group, cached.facts.clone());
                    active.remove(&(group, relational));
                    continue;
                }
                let proof_units = if relational {
                    ctx.memo()
                        .group(group)
                        .unwrap()
                        .logical_exprs()
                        .iter()
                        .map(|expression| {
                            let logical = ctx.memo().logical_expr(*expression).unwrap();
                            logical
                                .key
                                .children
                                .iter()
                                .filter_map(|child| {
                                    result.groups.get(&ctx.memo().canonical_group(*child))
                                })
                                .map(|facts| {
                                    facts
                                        .lineage
                                        .values()
                                        .flatten()
                                        .map(Vec::len)
                                        .sum::<usize>()
                                        .saturating_add(
                                            facts
                                                .unique_keys
                                                .iter()
                                                .map(|key| key.len())
                                                .sum::<usize>(),
                                        )
                                })
                                .sum::<usize>()
                                .saturating_add(
                                    state.metadata[&logical.payload]
                                        .child_layouts
                                        .iter()
                                        .map(|layout| layout.bindings.len())
                                        .sum::<usize>(),
                                )
                        })
                        .sum()
                } else {
                    0
                };
                if !ctx.admit_fact_work(dimension, proof_units)? {
                    return Ok(None);
                }
                let derived = result.derive(ctx.memo(), state, group, relational)?;
                let facts = cache
                    .entries
                    .get(&(group, relational))
                    .filter(|cached| cached.facts.as_ref() == &derived)
                    .map(|cached| cached.facts.clone())
                    .unwrap_or_else(|| Arc::new(derived));
                cache.entries.insert(
                    (group, relational),
                    CachedFacts {
                        read,
                        inputs,
                        facts: facts.clone(),
                    },
                );
                result.groups.insert(group, facts);
                active.remove(&(group, relational));
                continue;
            }
            if active.contains(&(group, relational)) {
                continue;
            }
            let width = ctx
                .memo()
                .group(group)
                .ok_or_else(|| paro_error::internal("fact reader lost group"))?
                .schema
                .columns()
                .len();
            if !ctx.admit_fact_work(dimension, 1 + width)? {
                return Ok(None);
            }
            ctx.record_fact_read(if relational {
                PatternRead::from_group(ctx.memo(), group)?
            } else {
                PatternRead::facts_from_group(ctx.memo(), group)?
            });
            active.insert((group, relational));
            pending.push((group, relational, true));
            // Copy only admitted ids. Neither the matcher nor this reader may
            // hide a recursive cardinality walk in a one-unit observation.
            let expressions = if relational {
                ctx.memo().group(group).unwrap().logical_exprs().len()
            } else {
                0
            };
            for ordinal in 0..expressions {
                if !ctx.admit_fact_work(dimension, 1)? {
                    return Ok(None);
                }
                let expression = ctx.memo().group(group).unwrap().logical_exprs()[ordinal];
                let arity = ctx
                    .memo()
                    .logical_expr(expression)
                    .unwrap()
                    .key
                    .children
                    .len();
                if !ctx.admit_fact_work(dimension, arity)? {
                    return Ok(None);
                }
                pending.extend(
                    ctx.memo()
                        .logical_expr(expression)
                        .unwrap()
                        .key
                        .children
                        .iter()
                        .map(|child| (ctx.memo().canonical_group(*child), true, false)),
                );
            }
            let dependencies = ctx.memo().cardinality_dependencies(group).count();
            if !ctx.admit_fact_work(dimension, dependencies)? {
                return Ok(None);
            }
            pending.extend(
                ctx.memo()
                    .cardinality_dependencies(group)
                    .map(|(input, _)| (ctx.memo().canonical_group(input), false, false)),
            );
        }
        Ok(Some(result))
    }

    pub(super) fn cardinality(&self, memo: &Memo, group: GroupId) -> Option<CardinalityEstimate> {
        let range = self.groups.get(&memo.canonical_group(group))?.cardinality?;
        Some(CardinalityEstimate {
            min: range.lower,
            expected: range
                .expected_lower
                .saturating_add((range.expected_upper - range.expected_lower) / 2),
            max: range.upper,
        })
    }

    pub(super) fn transport(
        &self,
        memo: &Memo,
        state: &PlannerTransformState,
        group: GroupId,
        layout: &PlannerBindingLayout,
    ) -> Result<Arc<BoundRelationFacts>> {
        let group = memo.canonical_group(group);
        let facts = self
            .groups
            .get(&group)
            .ok_or_else(|| paro_error::internal("unobserved Memo boundary"))?;
        let columns = layout
            .bindings
            .iter()
            .zip(&layout.types)
            .map(|(binding, ty)| {
                state
                    .binding_ids
                    .get(binding.table_index, binding.column_index, ty)
                    .copied()
                    .ok_or_else(|| paro_error::internal("boundary layout has an unknown column"))
            })
            .collect::<Result<Vec<_>>>()?;
        let ordinals = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (*column, index))
            .collect::<BTreeMap<_, _>>();
        let unique_keys = facts
            .unique_keys
            .iter()
            .filter_map(|key| {
                let columns = key
                    .iter()
                    .map(|column| {
                        let output_index = *ordinals.get(column)?;
                        Some(UniqueKeyColumn {
                            output_index,
                            binding: layout.bindings[output_index],
                        })
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(UniqueKey::new(columns, UniqueKeyProvenance::Structural))
            })
            .collect();
        Ok(Arc::new(BoundRelationFacts {
            unique_keys,
            source_lineage: columns
                .iter()
                .map(|column| facts.lineage.get(column).cloned().flatten())
                .collect(),
            contains_control_region: facts.control,
        }))
    }

    fn derive(
        &self,
        memo: &Memo,
        state: &PlannerTransformState,
        id: GroupId,
        relational: bool,
    ) -> Result<GroupFacts> {
        let group = memo
            .group(id)
            .ok_or_else(|| paro_error::internal("fact derivation lost group"))?;
        let mut inherited = None;
        let mut producer = None;
        for (input, is_producer) in memo.cardinality_dependencies(id) {
            if let Some(range) = self
                .groups
                .get(&memo.canonical_group(input))
                .and_then(|facts| facts.cardinality)
            {
                let target = if is_producer {
                    &mut producer
                } else {
                    &mut inherited
                };
                *target = Some(
                    target.map_or(range, |previous: CardinalityEnvelope| previous.hull(range)),
                );
            }
        }
        let mut cardinality = producer.or(memo.local_cardinality_envelope(id));
        if let Some(range) = inherited {
            cardinality = Some(cardinality.map_or(range, |previous| previous.hull(range)));
        }
        cardinality =
            cardinality.map(|range| range.clamp(group.logical_properties.maximum_cardinality));
        if !relational {
            return Ok(GroupFacts {
                cardinality,
                control: true,
                ..GroupFacts::default()
            });
        }
        let mut common: Option<BTreeMap<ColumnId, Option<Vec<BoundSourceColumn>>>> = None;
        let mut unique_keys = group.logical_properties.unique_keys.clone();
        let mut control = false;
        for expression in group.logical_exprs() {
            let logical = memo
                .logical_expr(*expression)
                .ok_or_else(|| paro_error::internal("fact recipe lost expression"))?;
            let metadata = state
                .metadata
                .get(&logical.payload)
                .ok_or_else(|| paro_error::internal("fact recipe has no metadata"))?;
            let operator = &state.payloads.logical[logical.payload.index()]
                .semantic_template
                .operator;
            let child_layouts = metadata
                .child_layouts
                .iter()
                .map(|layout| {
                    paro_planner::operator::LogicalOutputLayout::new(
                        layout.types.to_vec(),
                        layout.bindings.to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            let child_keys = logical
                .key
                .children
                .iter()
                .zip(&metadata.child_layouts)
                .map(|(group, layout)| {
                    if self.groups.contains_key(&memo.canonical_group(*group)) {
                        self.transport(memo, state, *group, layout)
                            .map(|facts| facts.unique_keys.clone())
                    } else {
                        Ok(Vec::new())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let layout = operator.output_layout_from_children(&child_layouts);
            let local_keys = crate::statistics::unique_keys::derive_unique_keys_from_facts(
                operator,
                &layout,
                &child_layouts,
                &child_keys.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            );
            for key in local_keys {
                let key = key
                    .columns
                    .iter()
                    .map(|column| {
                        let ty = layout.types().get(column.output_index)?;
                        let id = *state.binding_ids.get(
                            column.binding.table_index,
                            column.binding.column_index,
                            ty,
                        )?;
                        metadata.output_columns.contains(&id).then_some(id)
                    })
                    .collect::<Option<BTreeSet<_>>>();
                if let Some(key) = key {
                    unique_keys.insert(key.into_iter().collect());
                }
            }
            let child = |index: usize| {
                logical
                    .key
                    .children
                    .get(index)
                    .and_then(|group| self.groups.get(&memo.canonical_group(*group)))
            };
            control |= matches!(
                operator,
                LogicalOperator::MaterializedCTE(_) | LogicalOperator::RecursiveCTE(_)
            ) || matches!(operator, LogicalOperator::Join(Join::Comparison(join)) if !join.duplicate_eliminated_columns.is_empty())
                || (0..logical.key.children.len())
                    .any(|index| child(index).is_none_or(|facts| facts.control));
            let column_at = |index: usize, ordinal: usize| -> Option<ColumnId> {
                let layout = metadata.child_layouts.get(index)?;
                let binding = layout.bindings.get(ordinal)?;
                state
                    .binding_ids
                    .get(
                        binding.table_index,
                        binding.column_index,
                        layout.types.get(ordinal)?,
                    )
                    .copied()
            };
            let expression_column = |expression: &Expression, index: usize| -> Option<ColumnId> {
                match expression {
                    Expression::ColumnRef(column) if column.depth == 0 => state
                        .binding_ids
                        .get(
                            column.binding.table_index,
                            column.binding.column_index,
                            &column.return_type,
                        )
                        .copied(),
                    Expression::Reference(reference) => column_at(index, reference.index),
                    _ => None,
                }
            };
            let lineage = |index: usize, column: ColumnId| {
                child(index)
                    .and_then(|facts| facts.lineage.get(&column))
                    .cloned()
                    .flatten()
            };
            let mut local = BTreeMap::new();
            for (ordinal, column) in metadata.output_columns.iter().copied().enumerate() {
                let source = |get: &paro_planner::operator::Get, source_index: usize| {
                    (get.table.is_some() && get.stored_column(source_index).is_some()).then(|| {
                        vec![BoundSourceColumn {
                            source: get.table_index,
                            occurrence: id.index(),
                            column: ordinal,
                            rows: cardinality.map(|range| CardinalityEstimate {
                                min: range.lower,
                                expected: range.expected_lower.saturating_add(
                                    (range.expected_upper - range.expected_lower) / 2,
                                ),
                                max: range.upper,
                            }),
                            distinct: group
                                .logical_properties
                                .column_domains
                                .get(&column)
                                .and_then(|domain| domain.expected()),
                            unique: unique_keys.iter().any(|key| key.as_ref() == [column]),
                        }]
                    })
                };
                let sources = match operator {
                    LogicalOperator::Get(get) => source(get, ordinal),
                    LogicalOperator::SearchScan(search) => search
                        .projections
                        .get(ordinal)
                        .and_then(|expression| match expression {
                            Expression::Reference(reference) => Some(reference.index),
                            Expression::ColumnRef(column)
                                if column.depth == 0
                                    && column.binding.table_index == search.get.table_index =>
                            {
                                Some(column.binding.column_index)
                            }
                            _ => None,
                        })
                        .and_then(|index| source(&search.get, index)),
                    LogicalOperator::FullTextFilterScan(search) => search
                        .get
                        .returned_types
                        .iter()
                        .enumerate()
                        .find(|(index, ty)| {
                            state.binding_ids.get(search.get.table_index, *index, ty)
                                == Some(&column)
                        })
                        .and_then(|(index, _)| source(&search.get, index)),
                    LogicalOperator::Filter(_) => lineage(0, column),
                    LogicalOperator::Projection(projection) => projection
                        .expressions
                        .get(ordinal)
                        .and_then(|expression| expression_column(expression, 0))
                        .and_then(|column| lineage(0, column)),
                    LogicalOperator::SetOperation(setop)
                        if setop.setop_type == paro_planner::operator::SetOpType::Union
                            && setop.setop_all =>
                    {
                        column_at(0, ordinal)
                            .and_then(|column| lineage(0, column))
                            .zip(column_at(1, ordinal).and_then(|column| lineage(1, column)))
                            .and_then(|(mut left, right)| {
                                // A source-work identity cannot represent two bag
                                // occurrences, even when their Memo group is shared.
                                if left
                                    .iter()
                                    .any(|a| right.iter().any(|b| a.source == b.source))
                                {
                                    return None;
                                }
                                left.extend(right);
                                Some(left)
                            })
                    }
                    LogicalOperator::Join(Join::Comparison(join))
                        if join.duplicate_eliminated_columns.is_empty() && !join.delim_flipped =>
                    {
                        if join.join_type.preserves_left_values() {
                            lineage(0, column)
                        } else {
                            None
                        }
                        .or_else(|| {
                            if join.join_type.preserves_right_values() {
                                lineage(1, column)
                            } else {
                                None
                            }
                        })
                    }
                    _ => None,
                };
                local.insert(column, sources);
            }
            if let Some(common) = &mut common {
                for (column, sources) in common.iter_mut() {
                    // A witness from just one alternative is insufficient.
                    // Different paths or incomplete coverage remain unknown.
                    if local.get(column) != Some(sources) {
                        *sources = None;
                    }
                }
            } else {
                common = Some(local);
            }
        }
        Ok(GroupFacts {
            relational,
            unique_keys,
            cardinality,
            lineage: common.unwrap_or_default(),
            control: control || group.logical_exprs().is_empty(),
        })
    }
}
