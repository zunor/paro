// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Probe-source materialization and scan-filter attachment.

use super::*;

impl PipelineLowerer<'_> {
    pub(crate) fn attach_hash_join_runtime_filters(
        &self,
        mut source: SourceSpec,
        transforms: &[TransformSpec],
        handle: BreakerHandleId,
        spec: &HashJoinSpec,
    ) -> SourceSpec {
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
    ) -> Result<(SourceSpec, Vec<TransformSpec>, Vec<PendingProbeDependency>)> {
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
        Ok((
            source,
            Vec::new(),
            vec![PendingProbeDependency {
                producer,
                handle,
                kind: DependencyKind::MaterializeBeforeRead,
            }],
        ))
    }
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

fn can_push_hash_join_runtime_filter(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
    )
}

/// Trace a downstream join-key reference back to the rowset source.
///
/// A chained inner/semi hash probe emits its projected left columns before
/// any build payload, so a reference inside `left_projection` has exact
/// lineage to the preceding transform. Other transforms are deliberate
/// barriers: crossing one would require its own expression-lineage proof and
/// could move a dynamic predicate across a limit or volatile expression.
fn trace_probe_reference_to_source(
    mut reference_index: usize,
    transforms: &[TransformSpec],
) -> Option<usize> {
    for transform in transforms.iter().rev() {
        let TransformSpec::HashJoinProbe(probe) = transform else {
            return None;
        };
        if !matches!(probe.join_type, JoinType::Inner | JoinType::Semi) {
            return None;
        }
        reference_index = *probe.left_projection.get(reference_index)?;
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
