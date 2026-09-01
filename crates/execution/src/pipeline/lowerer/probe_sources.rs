// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Probe-source materialization and scan-filter attachment.

use super::*;

impl PipelineLowerer<'_> {
    pub(crate) fn attach_owned_hash_join_runtime_filters(
        &self,
        scan_node: PhysicalPlanNodeId,
        rowset: &mut RowsetSourceSpec,
    ) -> Result<()> {
        for edge in self.plan.edges.iter().filter(|edge| {
            edge.consumer == scan_node
                && matches!(
                    edge.kind,
                    crate::physical::PhysicalEdgeKind::RuntimeFilter(_)
                )
        }) {
            let crate::physical::PhysicalEdgeKind::RuntimeFilter(artifact) = edge.kind else {
                unreachable!("filtered runtime-filter edge changed kind")
            };
            let handle = *self.runtime_filter_handles.get(&artifact).ok_or_else(|| {
                paro_error::internal(
                    "runtime-filter consumer was lowered before its build handle was registered",
                )
            })?;
            let owner = *self.runtime_filter_owners.get(&artifact).ok_or_else(|| {
                paro_error::internal("runtime-filter artifact has no registered physical owner")
            })?;
            let owner = self.plan.node(owner);
            let PhysicalNodeKind::HashJoin(spec) = &owner.kind else {
                return Err(paro_error::internal(
                    "runtime-filter artifact owner is not a physical hash join",
                ));
            };
            let [probe, build] = self.plan.child_ids(&owner.children) else {
                return Err(paro_error::internal(
                    "runtime-filter hash join owner has invalid children",
                ));
            };
            if *build != edge.producer
                || spec
                    .runtime_filter
                    .is_none_or(|filter| filter.artifact != artifact)
            {
                return Err(paro_error::internal(
                    "runtime-filter edge disagrees with its registered physical owner",
                ));
            }
            let mut installed = 0usize;
            for (build_key_index, condition) in spec.key_conditions.iter().enumerate() {
                if condition.comparison != JoinComparisonType::Equal {
                    continue;
                }
                let Expression::Reference(reference) = &condition.left else {
                    continue;
                };
                let Some(source_index) =
                    trace_probe_reference_to_rowset(self.plan, *probe, reference.index, scan_node)
                else {
                    continue;
                };
                let Some(probe_column_id) = rowset.scan.column_projection.column_id(source_index)
                else {
                    continue;
                };
                let Ok(probe_column_id) = u32::try_from(probe_column_id) else {
                    continue;
                };
                rowset.add_dynamic_runtime_filter(RowsetDynamicRuntimeFilterSpec {
                    handle,
                    artifact,
                    build_key_index,
                    probe_column_id,
                });
                installed += 1;
            }
            if installed == 0 {
                return Err(paro_error::internal(
                    "runtime-filter edge could not resolve a probe column at its rowset consumer",
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn attach_hash_join_runtime_filters(
        &self,
        mut source: SourceSpec,
        transforms: &[TransformSpec],
        handle: BreakerHandleId,
        spec: &HashJoinSpec,
    ) -> SourceSpec {
        let Some(runtime_filter) = spec.runtime_filter else {
            return source;
        };
        if !can_push_hash_join_runtime_filter(spec.join_type) {
            return source;
        }
        let SourceSpec::Rowset(rowset) = &mut source else {
            return source;
        };
        for (build_key_index, condition) in spec.key_conditions.iter().enumerate() {
            if condition.comparison != JoinComparisonType::Equal {
                continue;
            }
            let Expression::Reference(reference) = &condition.left else {
                continue;
            };
            let Some(source_index) = trace_probe_reference_to_source(reference.index, transforms)
            else {
                continue;
            };
            let Some(probe_column_id) = rowset.scan.column_projection.column_id(source_index)
            else {
                continue;
            };
            let Ok(probe_column_id) = u32::try_from(probe_column_id) else {
                continue;
            };
            rowset.add_dynamic_runtime_filter(RowsetDynamicRuntimeFilterSpec {
                handle,
                artifact: runtime_filter.artifact,
                build_key_index,
                probe_column_id,
            });
        }
        source
    }

    pub(crate) fn attach_nlj_scalar_runtime_filter(
        &self,
        mut source: SourceSpec,
        transforms: &[TransformSpec],
        handle: BreakerHandleId,
        spec: &NestedLoopJoinSpec,
        exact_single_row: bool,
    ) -> (SourceSpec, bool) {
        if spec.join_type != JoinType::Inner {
            return (source, false);
        }
        let [condition] = spec.conditions.as_ref() else {
            return (source, false);
        };
        if !matches!(
            condition.comparison,
            JoinComparisonType::Equal
                | JoinComparisonType::LessThan
                | JoinComparisonType::LessThanOrEqual
                | JoinComparisonType::GreaterThan
                | JoinComparisonType::GreaterThanOrEqual
        ) {
            return (source, false);
        }
        let Some((reference_index, probe_type)) = exact_monotonic_probe_reference(&condition.left)
        else {
            return (source, false);
        };
        let Expression::Reference(build_reference) = &condition.right else {
            return (source, false);
        };
        if spec.right_output_types.get(build_reference.index) != Some(&build_reference.return_type)
        {
            return (source, false);
        }
        let Some(source_index) = trace_probe_reference_to_source(reference_index, transforms)
        else {
            return (source, false);
        };
        let SourceSpec::Rowset(rowset) = &mut source else {
            return (source, false);
        };
        let Some(probe_column_id) = rowset.scan.column_projection.column_id(source_index) else {
            return (source, false);
        };
        let Ok(probe_column_id) = u32::try_from(probe_column_id) else {
            return (source, false);
        };
        let semantic_exact = exact_single_row
            && exact_decimal_scalar_filter(&probe_type, &build_reference.return_type);
        rowset.add_dynamic_scalar_filter(RowsetDynamicScalarFilterSpec {
            handle,
            build_column_index: build_reference.index,
            probe_column_id,
            probe_type,
            comparison: condition.comparison,
            semantics: if semantic_exact {
                ScalarFilterSemantics::ExactSingleRow
            } else {
                ScalarFilterSemantics::Conservative
            },
        });
        (source, semantic_exact)
    }

    pub(crate) fn collect_probe_roles_source_fallback(
        &mut self,
        root: PhysicalPlanNodeId,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<CollectedProbeChain> {
        let output = self.plan.node(root).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            output.clone(),
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            root,
            SinkSpec::Materialize(MaterializeSinkSpec { handle }),
            SinkSharing::Exclusive,
            output,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        let source = SourceSpec::Materialized(MaterializedSourceSpec { handle });
        Ok(CollectedProbeChain {
            source,
            transforms: Vec::new(),
            pending_builds: vec![PendingProbeDependency {
                producer,
                handle,
                kind: DependencyKind::MaterializeBeforeRead,
            }],
            pending_replays: Vec::new(),
        })
    }
}

fn trace_probe_reference_to_rowset(
    plan: &PhysicalPlan,
    node: PhysicalPlanNodeId,
    output_index: usize,
    target: PhysicalPlanNodeId,
) -> Option<usize> {
    paro_optimizer::physical::lineage::trace_rowset_lineage(plan, node, output_index)
        .into_iter()
        .find_map(|(scan, source_index)| (scan == target).then_some(source_index))
}

/// Return the source reference under an exact, monotonic representation cast.
/// Runtime scalar bounds may cross such a cast because outward rounding on the
/// original type cannot remove a true match. Narrowing, TRY_CAST, and all
/// non-decimal conversions remain execution-only predicates.
fn exact_monotonic_probe_reference(expression: &Expression) -> Option<(usize, LogicalType)> {
    match expression {
        Expression::Reference(reference) => Some((reference.index, reference.return_type.clone())),
        Expression::Cast(cast) if !cast.try_cast => {
            let Expression::Reference(reference) = cast.child.as_ref() else {
                return None;
            };
            exact_decimal_widening(&reference.return_type, &cast.target_type)
                .then(|| (reference.index, reference.return_type.clone()))
        }
        _ => None,
    }
}

fn exact_decimal_widening(source: &LogicalType, target: &LogicalType) -> bool {
    let (
        LogicalType::Decimal {
            precision: source_precision,
            scale: source_scale,
        },
        LogicalType::Decimal {
            precision: target_precision,
            scale: target_scale,
        },
    ) = (source, target)
    else {
        return false;
    };
    let Some(source_integer_digits) = source_precision.checked_sub(*source_scale) else {
        return false;
    };
    let Some(target_integer_digits) = target_precision.checked_sub(*target_scale) else {
        return false;
    };
    *target_scale >= *source_scale && target_integer_digits >= source_integer_digits
}

fn exact_decimal_scalar_filter(probe: &LogicalType, build: &LogicalType) -> bool {
    matches!(
        (probe, build),
        (
            LogicalType::Decimal {
                scale: probe_scale,
                ..
            },
            LogicalType::Decimal {
                scale: build_scale,
                ..
            }
        ) if build_scale >= probe_scale
    )
}

#[cfg(test)]
fn can_push_hash_join_runtime_filter(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
    )
}

/// Trace a downstream join-key reference back to the rowset source.
///
/// A chained inner/semi hash probe emits its projected left columns before
/// any build payload, and a passthrough projection preserves the referenced
/// source column exactly. Other transforms are deliberate barriers: crossing
/// one would require its own expression-lineage proof and could move a dynamic
/// predicate across a limit or volatile expression.
fn trace_probe_reference_to_source(
    mut reference_index: usize,
    transforms: &[TransformSpec],
) -> Option<usize> {
    for transform in transforms.iter().rev() {
        match transform {
            TransformSpec::HashJoinProbe(probe) => {
                if !matches!(probe.join_type, JoinType::Inner | JoinType::Semi) {
                    return None;
                }
                reference_index = *probe.left_projection.get(reference_index)?;
            }
            TransformSpec::Project(project) => {
                let Expression::Reference(reference) = project.expressions.get(reference_index)?
                else {
                    return None;
                };
                reference_index = reference.index;
            }
            _ => return None,
        }
    }
    Some(reference_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_filter_lineage_crosses_only_exact_decimal_widening() {
        assert!(exact_decimal_widening(
            &LogicalType::Decimal {
                precision: 15,
                scale: 2,
            },
            &LogicalType::Decimal {
                precision: 19,
                scale: 6,
            },
        ));
        assert!(!exact_decimal_widening(
            &LogicalType::Decimal {
                precision: 15,
                scale: 2,
            },
            &LogicalType::Decimal {
                precision: 14,
                scale: 2,
            },
        ));
        assert!(!exact_decimal_widening(
            &LogicalType::Decimal {
                precision: 15,
                scale: 2,
            },
            &LogicalType::Decimal {
                precision: 15,
                scale: 1,
            },
        ));
    }
}
