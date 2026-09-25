// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// The same source-work contract can be read from an existing planner tree or
/// an immutable native boundary. The native view never creates a tree or
/// reconstructs an input implementation.
#[derive(Clone, Copy)]
pub(crate) enum RuntimeFilterInput<'a> {
    Owned(&'a OwnedLogicalPlan),
    Boundary {
        layout: &'a paro_planner::logical::operator::LogicalOutputLayout,
        facts: &'a paro_planner::logical::operator::subplan_ref::BoundRelationFacts,
    },
}

impl<'a> RuntimeFilterInput<'a> {
    pub(super) fn bindings(self) -> std::borrow::Cow<'a, [ColumnBinding]> {
        match self {
            Self::Owned(plan) => plan.get_column_bindings().into(),
            Self::Boundary { layout, .. } => layout.bindings().into(),
        }
    }

    fn lineages(self, output_index: usize) -> Option<RuntimeFilterProbeLineage<'a>> {
        match self {
            Self::Owned(plan) => runtime_filter_probe_lineages(plan, output_index),
            Self::Boundary { layout, facts } => {
                if layout.types() != facts.types() {
                    return None;
                }
                let columns = facts.source_lineage.get(output_index)?.as_ref()?;
                Some(RuntimeFilterProbeLineage {
                    sources: columns
                        .iter()
                        .map(|column| RuntimeFilterProbeSource {
                            plan: None,
                            output_index: column.column,
                            boundary: Some(column),
                        })
                        .collect(),
                })
            }
        }
    }

    pub(super) fn probe_multiplicity(
        self,
        expressions: &[&Expression],
    ) -> RuntimeFilterProbeMultiplicity {
        match self {
            Self::Owned(plan) => {
                infer_runtime_filter_probe_multiplicity(plan, expressions.iter().copied())
            }
            Self::Boundary { layout, facts } => {
                if layout.types() == facts.types()
                    && crate::estimate::unique_keys::expressions_cover_unique_key_from_facts(
                        layout,
                        &facts.unique_keys,
                        expressions,
                    )
                {
                    RuntimeFilterProbeMultiplicity::DeclaredUnique
                } else {
                    RuntimeFilterProbeMultiplicity::Unknown
                }
            }
        }
    }
}

pub(crate) fn supports_runtime_filter_auxiliary(
    join: &paro_planner::logical::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    supports_runtime_filter_input(
        join,
        RuntimeFilterInput::Owned(&join.left),
        rowset_scan_pushdown,
    )
}

pub(super) fn supports_runtime_filter_input<Child>(
    join: &paro_planner::logical::operator::join::ComparisonJoin<Child>,
    probe: RuntimeFilterInput<'_>,
    rowset_scan_pushdown: bool,
) -> bool {
    if !rowset_scan_pushdown
        || !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
        )
    {
        return false;
    }

    let probe_bindings = probe.bindings();
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        if !crate::physical::RuntimeFilterResourceContract::for_keys(
            &[condition.right.return_type()],
            1,
        )
        .is_ok_and(|contract| {
            contract.capability != crate::physical::RuntimeFilterCapability::Disabled
        }) {
            return false;
        }
        let output_index = match &condition.left {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        };
        output_index.is_some_and(|index| {
            probe
                .lineages(index)
                .is_some_and(|lineage| !lineage.sources.is_empty())
        })
    })
}

pub(crate) fn supports_build_left_runtime_filter_auxiliary(
    join: &paro_planner::logical::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    supports_build_left_runtime_filter_input(
        join,
        RuntimeFilterInput::Owned(&join.right),
        rowset_scan_pushdown,
    )
}

pub(super) fn supports_build_left_runtime_filter_input<Child>(
    join: &paro_planner::logical::operator::join::ComparisonJoin<Child>,
    probe: RuntimeFilterInput<'_>,
    rowset_scan_pushdown: bool,
) -> bool {
    if !rowset_scan_pushdown
        || join.anti_join_mode != AntiJoinMode::Regular
        || !matches!(
            join.join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::RightSemi
        )
    {
        return false;
    }

    let probe_bindings = probe.bindings();
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        if !crate::physical::RuntimeFilterResourceContract::for_keys(
            &[condition.left.return_type()],
            1,
        )
        .is_ok_and(|contract| {
            contract.capability != crate::physical::RuntimeFilterCapability::Disabled
        }) {
            return false;
        }
        let output_index = match &condition.right {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        };
        output_index
            .and_then(|index| probe.lineages(index))
            .is_some_and(|lineage| !lineage.sources.is_empty())
    })
}

struct RuntimeFilterProbeLineage<'a> {
    sources: Vec<RuntimeFilterProbeSource<'a>>,
}

#[derive(Clone, Copy)]
struct RuntimeFilterProbeSource<'a> {
    plan: Option<&'a OwnedLogicalPlan>,
    output_index: usize,
    boundary: Option<&'a paro_planner::logical::operator::subplan_ref::BoundSourceColumn>,
}

/// Freeze the existing source-lineage contract at a local relation boundary.
/// Regional costing must expose precisely the RF capability that committed
/// tree selection sees; an opaque child must not silently disable that choice.
pub(crate) fn planner_source_lineage(
    plan: &OwnedLogicalPlan,
) -> Vec<Option<Vec<paro_planner::logical::operator::subplan_ref::BoundSourceColumn>>> {
    use paro_planner::logical::operator::subplan_ref::BoundSourceColumn;
    (0..plan.types().len())
        .map(|ordinal| {
            runtime_filter_probe_lineages(plan, ordinal)?
                .sources
                .into_iter()
                .map(|source| {
                    if let Some(boundary) = source.boundary {
                        return Some(boundary.clone());
                    }
                    let plan = source.plan?;
                    let binding = *plan.get_column_bindings().get(source.output_index)?;
                    let ty = plan.types().get(source.output_index)?.clone();
                    let expression = Expression::ColumnRef(
                        paro_planner::expression::ColumnRefExpression::new(binding, ty).into(),
                    );
                    let multiplicity = infer_runtime_filter_probe_multiplicity(plan, [&expression]);
                    Some(BoundSourceColumn {
                        source: runtime_filter_source_id(source)?.0,
                        occurrence: plan.id.0 as usize,
                        column: source.output_index,
                        rows: plan.stats.estimated_cardinality,
                        distinct: match multiplicity {
                            RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys } => {
                                Some(keys)
                            }
                            _ => None,
                        },
                        unique: matches!(
                            multiplicity,
                            RuntimeFilterProbeMultiplicity::DeclaredUnique
                        ),
                    })
                })
                .collect()
        })
        .collect()
}

fn runtime_filter_source_id(source: RuntimeFilterProbeSource<'_>) -> Option<WorkSourceId> {
    if let Some(boundary) = source.boundary {
        return Some(WorkSourceId(boundary.source));
    }
    let get = match &source.plan?.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::SearchScan(search) => &search.get,
        LogicalOperator::FullTextFilterScan(search) => &search.get,
        _ => return None,
    };
    Some(WorkSourceId(get.table_index))
}

pub(super) fn runtime_filter_input_source_facts<'a>(
    input: RuntimeFilterInput<'_>,
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> Option<Box<[PlannerRuntimeFilterSource]>> {
    let bindings = input.bindings();
    let mut expected_sources = None;
    let mut source_keys = BTreeMap::<
        WorkSourceId,
        (
            RuntimeFilterProbeSource<'_>,
            BTreeMap<usize, RuntimeFilterProbeSource<'_>>,
        ),
    >::new();
    let mut saw_expression = false;
    for expression in expressions {
        saw_expression = true;
        let output_index = match expression {
            Expression::ColumnRef(column) if column.depth == 0 => bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        }?;
        let lineage = input.lineages(output_index)?;
        let mut current_sources = lineage
            .sources
            .iter()
            .copied()
            .map(runtime_filter_source_id)
            .collect::<Option<Vec<_>>>()?;
        current_sources.sort_unstable();
        current_sources.dedup();
        if current_sources.is_empty()
            || expected_sources
                .as_ref()
                .is_some_and(|expected| expected != &current_sources)
        {
            return None;
        }
        expected_sources.get_or_insert(current_sources);
        for source in lineage.sources {
            let source_id = runtime_filter_source_id(source)?;
            let entry = source_keys
                .entry(source_id)
                .or_insert_with(|| (source, BTreeMap::new()));
            // One binding identity names one physical rowset occurrence. If a
            // future lineage maps it to two plan nodes, decline instead of
            // merging unrelated statistics under one source-work identity.
            let same_occurrence = match (entry.0.boundary, source.boundary) {
                (Some(left), Some(right)) => left.occurrence == right.occurrence,
                (None, None) => std::ptr::eq(entry.0.plan?, source.plan?),
                _ => false,
            };
            if !same_occurrence {
                return None;
            }
            entry.1.insert(source.output_index, source);
        }
    }
    if !saw_expression {
        return None;
    }
    source_keys
        .into_iter()
        .map(|(source, (first, keys))| {
            if let Some(boundary) = first.boundary {
                let multiplicity = if keys
                    .values()
                    .any(|key| key.boundary.is_some_and(|key| key.unique))
                {
                    RuntimeFilterProbeMultiplicity::DeclaredUnique
                } else if keys.len() == 1 {
                    boundary
                        .distinct
                        .map_or(RuntimeFilterProbeMultiplicity::Unknown, |keys| {
                            RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys }
                        })
                } else {
                    RuntimeFilterProbeMultiplicity::Unknown
                };
                return Some(PlannerRuntimeFilterSource {
                    source,
                    rows: boundary.rows?,
                    multiplicity,
                });
            }
            let plan = first.plan?;
            let types = plan.types();
            let bindings = plan.get_column_bindings();
            let key_expressions = keys
                .into_keys()
                .map(|index| {
                    Some(Expression::ColumnRef(
                        paro_planner::expression::ColumnRefExpression::new(
                            *bindings.get(index)?,
                            types.get(index)?.clone(),
                        )
                        .into(),
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let rows = plan.stats.estimated_cardinality?;
            let multiplicity = infer_runtime_filter_probe_multiplicity(
                plan,
                key_expressions.iter().collect::<Vec<_>>(),
            );
            Some(PlannerRuntimeFilterSource {
                source,
                rows,
                multiplicity,
            })
        })
        .collect::<Option<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn runtime_filter_probe_lineages(
    plan: &OwnedLogicalPlan,
    output_index: usize,
) -> Option<RuntimeFilterProbeLineage<'_>> {
    match &plan.operator {
        LogicalOperator::SubplanRef(reference) => {
            let columns = reference.facts.source_lineage.get(output_index)?.as_ref()?;
            Some(RuntimeFilterProbeLineage {
                sources: columns
                    .iter()
                    .map(|column| RuntimeFilterProbeSource {
                        plan: Some(plan),
                        output_index: column.column,
                        boundary: Some(column),
                    })
                    .collect(),
            })
        }
        LogicalOperator::Get(get)
            if get.table.is_some() && get.stored_column(output_index).is_some() =>
        {
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::SearchScan(search) if search.get.table.is_some() => {
            let source_index = match search.projections.get(output_index)? {
                Expression::Reference(reference) => reference.index,
                Expression::ColumnRef(column) if column.depth == 0 => {
                    (0..search.get.returned_types.len()).find(|index| {
                        ColumnBinding::new(search.get.table_index, *index) == column.binding
                    })?
                }
                _ => return None,
            };
            search.get.stored_column(source_index)?;
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::FullTextFilterScan(search) if search.get.table.is_some() => {
            let source_index = *search
                .projection_map
                .to_indices(search.get.returned_types.len())
                .get(output_index)?;
            search.get.stored_column(source_index)?;
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::Filter(filter) => {
            let child_index = filter
                .projection_map
                .to_indices(filter.child.types().len())
                .get(output_index)
                .copied()?;
            runtime_filter_probe_lineages(&filter.child, child_index)
        }
        LogicalOperator::Projection(projection)
            if !matches!(projection.child.operator, LogicalOperator::RowFetch(_)) =>
        {
            let child_bindings = projection.child.get_column_bindings();
            let child_index = match projection.expressions.get(output_index)? {
                Expression::ColumnRef(column) if column.depth == 0 => child_bindings
                    .iter()
                    .position(|binding| *binding == column.binding),
                Expression::Reference(reference) => Some(reference.index),
                _ => None,
            }?;
            runtime_filter_probe_lineages(&projection.child, child_index)
        }
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == paro_planner::logical::operator::SetOpType::Union
                && setop.setop_all =>
        {
            if output_index >= setop.column_count {
                return None;
            }
            let mut left = runtime_filter_probe_lineages(&setop.left, output_index)?;
            let right = runtime_filter_probe_lineages(&setop.right, output_index)?;
            left.sources.extend(right.sources);
            Some(left)
        }
        LogicalOperator::Join(Join::Comparison(inner))
            if inner.duplicate_eliminated_columns.is_empty() && !inner.delim_flipped =>
        {
            let left_projection = inner
                .left_projection_map
                .to_indices(inner.left.types().len());
            if let Some(&child_index) = left_projection.get(output_index) {
                if !inner.join_type.preserves_left_values() {
                    return None;
                }
                return runtime_filter_probe_lineages(&inner.left, child_index);
            }
            if !inner.join_type.preserves_right_values() {
                // A NULL-extended value no longer has exact source lineage.
                // Pushing a predicate into its stored origin could change
                // which preserved rows are considered matched.
                return None;
            }
            let right_output = output_index.checked_sub(left_projection.len())?;
            let right_projection = inner
                .right_projection_map
                .to_indices(inner.right.types().len());
            runtime_filter_probe_lineages(&inner.right, *right_projection.get(right_output)?)
        }
        // A CTE reference is not a rowset consumer. Crossing it requires one
        // AuxiliaryPlanRegion jointly owned by the CTE producer, every
        // reference, and the runtime-filter build.
        _ => None,
    }
}

pub(super) fn runtime_filter_input_source_rows<'a>(
    input: RuntimeFilterInput<'_>,
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> Option<paro_planner::logical::plan::CardinalityEstimate> {
    let probe_bindings = input.bindings();
    expressions.into_iter().find_map(|expression| {
        let output_index = match expression {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        }?;
        let lineage = input.lineages(output_index)?;
        lineage.sources.into_iter().try_fold(
            paro_planner::logical::plan::CardinalityEstimate::exact(0),
            |sum, source| {
                let rows = source.boundary.map_or(
                    source
                        .plan
                        .and_then(|plan| plan.stats.estimated_cardinality),
                    |column| column.rows,
                )?;
                Some(paro_planner::logical::plan::CardinalityEstimate {
                    min: sum.min.saturating_add(rows.min),
                    expected: sum.expected.saturating_add(rows.expected),
                    max: sum.max.saturating_add(rows.max),
                })
            },
        )
    })
}
