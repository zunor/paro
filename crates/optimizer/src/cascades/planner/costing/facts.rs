// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Group-resolved cardinality and row-width inputs for physical costing.

use super::*;

pub(in crate::cascades::planner) fn expression_cost_facts(
    memo: &Memo,
    group: GroupId,
    children: &[GroupId],
    template: &PlannerCostFacts,
) -> Result<ResolvedPlannerCostFacts> {
    let output = memo
        .group(group)
        .ok_or_else(|| paro_error::internal("costing references an unknown output group"))?;
    let output_rows = group_cardinality_work_range(memo.cardinality_envelope(group))?;
    let output_rows_hard_upper = output.logical_properties.maximum_cardinality;
    let child_rows = children
        .iter()
        .map(|child| {
            memo.group(*child)
                .ok_or_else(|| paro_error::internal("costing references an unknown child group"))?;
            group_cardinality_work_range(memo.cardinality_envelope(*child))
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    let child_rows_hard_upper = children
        .iter()
        .map(|child| {
            memo.group(*child)
                .and_then(|group| group.logical_properties.maximum_cardinality)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(ResolvedPlannerCostFacts {
        output_rows,
        child_rows,
        output_rows_hard_upper,
        child_rows_hard_upper,
        child_row_widths: template.child_row_widths.clone(),
        child_materialization_risk_rows: template.child_materialization_risk_rows.clone(),
        output_row_width: template.output_row_width,
        hash_key_width: template.hash_key_width,
        scan_access_width: template.scan_access_width,
        scan_physical_rows: template.scan_physical_rows,
        scan_work_source: template.scan_work_source,
        perfect_hash: template.perfect_hash,
        topn_capacity: template.topn_capacity,
        runtime_filter_probe_multiplicity: template.runtime_filter_probe_multiplicity,
        runtime_filter_build_left_probe_multiplicity: template
            .runtime_filter_build_left_probe_multiplicity,
        runtime_filter_probe_source_rows: template
            .runtime_filter_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_build_left_probe_source_rows: template
            .runtime_filter_build_left_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_probe_sources: template
            .runtime_filter_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_left_probe_sources: template
            .runtime_filter_build_left_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_distinct_expected: children
            .get(1)
            .zip(template.runtime_filter_build_domain_column)
            .and_then(|(group, column)| {
                memo.column_domain(*group, column)
                    .and_then(|domain| domain.expected())
            })
            .or(template.runtime_filter_build_distinct_expected),
        runtime_filter_build_domain_identity: children
            .get(1)
            .zip(template.runtime_filter_build_key)
            .and_then(|(group, key)| {
                runtime_filter_domain_identity(memo, *group, key, JoinKeySide::Right)
            }),
        runtime_filter_build_left_domain_identity: children
            .first()
            .zip(template.runtime_filter_build_left_key)
            .and_then(|(group, key)| {
                runtime_filter_domain_identity(memo, *group, key, JoinKeySide::Left)
            }),
        runtime_filter_build_left_distinct_expected: children
            .first()
            .zip(template.runtime_filter_build_left_domain_column)
            .and_then(|(group, column)| {
                memo.column_domain(*group, column)
                    .and_then(|domain| domain.expected())
            })
            .or(template.runtime_filter_build_left_distinct_expected),
        runtime_filter_key_types: template.runtime_filter_key_types.clone(),
    })
}

/// Return the identity of a logical runtime-filter build domain.
///
/// A physical implementation is intentionally absent: all implementations
/// of the same canonical Memo group and complete key may reuse the semantic
/// proof.  The group identity prevents two nested, same-shaped relations from
/// being deduplicated merely because they have the same operator fingerprint;
/// fact/statistics snapshots make a proof stale when the relation's evidence
/// changes.  Group ids are Memo-local semantic identities, not physical
/// occurrence ids, so this remains stable across physical alternatives.
fn runtime_filter_domain_identity(
    memo: &Memo,
    group: GroupId,
    key: Fingerprint,
    side: JoinKeySide,
) -> Option<Fingerprint> {
    let group = memo.canonical_group(group);
    let group_ref = memo.group(group)?;
    let mut identity = StableFingerprintBuilder::default();
    identity.write_bytes(b"paro.runtime-filter-build-domain.v3");
    identity.write_u64(group.0 as u64);
    identity.write_fingerprint(key);
    identity.write_u64(match side {
        JoinKeySide::Left => 0,
        JoinKeySide::Right => 1,
    });
    identity.write_fingerprint(group_ref.logical_fact_fingerprint());
    identity.write_fingerprint(memo.local_statistics_fingerprint(group));
    Some(identity.finish())
}

fn group_cardinality_work_range(cardinality: Option<CardinalityEnvelope>) -> Result<CompactRange> {
    match cardinality {
        Some(range) => CompactRange::new(
            range.lower as f64,
            range
                .expected_lower
                .saturating_add(range.expected_upper.saturating_sub(range.expected_lower) / 2)
                as f64,
            range.upper as f64,
        ),
        None => CompactRange::new(0.0, 1.0, 4.0),
    }
}

#[cfg(test)]
mod key_identity_tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ReferenceExpression;

    #[test]
    fn compound_keys_preserve_tuple_order_and_input_layout() {
        let key = |index| {
            Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer).into())
        };
        let a = key(0);
        let b = key(1);
        let bindings = [ColumnBinding::new(7, 0), ColumnBinding::new(7, 1)];
        let both = join_key_identity(&[&a, &b], &bindings).unwrap();
        assert_eq!(Some(both), join_key_identity(&[&a, &b], &bindings));
        assert_ne!(Some(both), join_key_identity(&[&b, &a], &bindings));
        assert_ne!(Some(both), join_key_identity(&[&a, &b, &a], &bindings));
        assert_ne!(Some(both), join_key_identity(&[&a], &bindings));
        assert_ne!(
            Some(both),
            join_key_identity(&[&a, &b], &[bindings[1], bindings[0]])
        );
        assert_eq!(join_key_identity(&[], &bindings), None);
    }
}
