// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Local implementation costing and executable memory floors.
//!
//! This kernel evaluates estimates without owning candidate plans.
//! Both planning strategies supply one immutable set of resolved input facts.

use crate::cost::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, OP_HASH_KEY_BYTE_BLOCK,
    OP_RUNTIME_FILTER_APPLY_ROW, OP_RUNTIME_FILTER_BUILD_ROW, OP_TUPLE_BYTE_BLOCK,
};
use crate::cost::source::WorkSourceId;
use crate::physical::PhysicalImplementationFlavor;
use paro_common::error::{self as paro_error, Result};
use paro_common::task_supply::useful_pipeline_tasks;
use paro_common::types::LogicalType;
use paro_planner::logical::operator::LogicalOperatorType;
use paro_planner::physical::cost::{CompactRange, PhysicalCost, ResourceDimension, ScoreSummary};
use paro_planner::physical::OpClassId;

#[cfg(test)]
mod tests {
    use super::{
        sort_work, topn_work, CompactRange, ResolvedRuntimeFilterSource, RuntimeFilterBuild,
        RuntimeFilterBuildKey, RuntimeFilterExactness, RuntimeFilterProbeKeyDomain,
        RuntimeFilterSourceKey,
    };
    use crate::cost::source::WorkSourceId;

    /// Survivors of a one-key filter whose build domain is `domain`.
    fn survivors(
        probe: CompactRange,
        domain: CompactRange,
        probe_domain: RuntimeFilterProbeKeyDomain,
        exactness: RuntimeFilterExactness,
    ) -> CompactRange {
        let source = ResolvedRuntimeFilterSource {
            source: WorkSourceId(0),
            keys: Box::new([RuntimeFilterSourceKey {
                key: 0,
                column: 0,
                domain: probe_domain,
            }]),
            rows: probe,
        };
        RuntimeFilterBuild {
            keys: Box::new([RuntimeFilterBuildKey { domain, exactness }]),
            sources: std::slice::from_ref(&source),
        }
        .source_survivors(&source)
        .unwrap()
    }

    #[test]
    fn bounded_topn_prices_heap_work_instead_of_a_full_sort() {
        let rows = CompactRange::new(100_000.0, 100_000.0, 100_000.0).unwrap();
        let sort = sort_work(rows).unwrap();
        let topn = topn_work(rows, 100).unwrap();

        assert!(topn.expected < sort.expected);
        assert!(topn.upper < sort.upper);
    }

    #[test]
    fn runtime_filter_fallback_keeps_the_complete_probe_as_risk_upper() {
        let probe = CompactRange::new(100_000.0, 100_000.0, 100_000.0).unwrap();
        let build = CompactRange::new(10_000.0, 12_000.0, 15_000.0).unwrap();

        let unique = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::DeclaredUnique,
            RuntimeFilterExactness::Expected,
        );
        let unconstrained = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::Unknown,
            RuntimeFilterExactness::Coarse,
        );
        let small_exact_domain = CompactRange::new(50.0, 100.0, 150.0).unwrap();
        let exact_unconstrained = survivors(
            probe,
            small_exact_domain,
            RuntimeFilterProbeKeyDomain::Unknown,
            RuntimeFilterExactness::Expected,
        );
        let coarse_unconstrained = survivors(
            probe,
            small_exact_domain,
            RuntimeFilterProbeKeyDomain::Unknown,
            RuntimeFilterExactness::Coarse,
        );

        assert_eq!(unique.upper, 100_000.0);
        assert_eq!(unconstrained.upper, 100_000.0);
        assert_eq!(exact_unconstrained.upper, 100_000.0);
        assert!(unique.expected < unconstrained.expected);
        assert_eq!(exact_unconstrained.expected, coarse_unconstrained.expected);
    }

    #[test]
    fn guaranteed_exact_filter_bounds_a_declared_unique_probe() {
        let probe = CompactRange::new(0.0, 100_000.0, 120_000.0).unwrap();
        let build = CompactRange::new(0.0, 100.0, 5_000.0).unwrap();
        let exact = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::DeclaredUnique,
            RuntimeFilterExactness::Guaranteed {
                build_keys_upper: 2_000,
            },
        );
        let estimated_distinct = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 95_000 },
            RuntimeFilterExactness::Guaranteed {
                build_keys_upper: 2_000,
            },
        );

        assert_eq!(exact.upper, 2_000.0);
        assert_eq!(estimated_distinct.upper, probe.upper);
    }

    #[test]
    fn runtime_filter_selectivity_uses_probe_ndv_instead_of_probe_rows() {
        let probe = CompactRange::point(12_000.0).unwrap();
        let build = CompactRange::point(4.0).unwrap();

        let repeated = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 6 },
            RuntimeFilterExactness::Expected,
        );
        let unknown = survivors(
            probe,
            build,
            RuntimeFilterProbeKeyDomain::Unknown,
            RuntimeFilterExactness::Expected,
        );

        // Four build keys cover four of the six probe keys.
        assert_eq!(repeated.expected, 8_000.0);
        assert_eq!(unknown.expected, probe.expected);
    }

    #[test]
    fn composite_filters_keep_rows_passing_every_key() {
        let source = ResolvedRuntimeFilterSource {
            source: WorkSourceId(0),
            keys: Box::new([
                RuntimeFilterSourceKey {
                    key: 0,
                    column: 3,
                    domain: RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 6 },
                },
                RuntimeFilterSourceKey {
                    key: 1,
                    column: 1,
                    domain: RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 100_000 },
                },
            ]),
            rows: CompactRange::point(2_880_000.0).unwrap(),
        };
        let key = |domain: f64, exactness| RuntimeFilterBuildKey {
            domain: CompactRange::new(0.0, domain, 56.0).unwrap(),
            exactness,
        };
        let build = |second| RuntimeFilterBuild {
            keys: Box::new([key(3.0, RuntimeFilterExactness::Expected), second]),
            sources: std::slice::from_ref(&source),
        };
        // Three of six stores and 56 of 100k customers, independently.
        let both = build(key(56.0, RuntimeFilterExactness::Expected))
            .source_survivors(&source)
            .unwrap();
        assert!((both.expected - 2_880_000.0 * 0.5 * 56.0 / 100_000.0).abs() < 1e-6);
        assert_eq!(both.upper, 2_880_000.0);
        // A key that degrades to a range claims no reduction of its own.
        let coarse = build(key(56.0, RuntimeFilterExactness::Coarse))
            .source_survivors(&source)
            .unwrap();
        assert_eq!(coarse.expected, 1_440_000.0);
    }
}

/// Fit one local implementation to its actual resource envelope.
pub(crate) fn fit_local_cost(
    mut cost: PhysicalCost,
    spillable: bool,
    class: paro_planner::physical::ResourceGrantClass,
    force_spill: bool,
) -> Result<Option<PhysicalCost>> {
    if cost.minimum_memory_bytes > class.hard_memory_bytes {
        return Ok(None);
    }
    if spillable && class.spill_policy == paro_planner::physical::SpillPolicy::Forbidden {
        // In a no-spill class the entire retained state is required for
        // forward progress. It is neither revocable nor a soft target, even
        // when the same implementation can be adaptive in another class.
        if force_spill
            || cost.peak_memory_upper == u64::MAX
            || cost.peak_memory_upper > class.hard_memory_bytes
        {
            return Ok(None);
        }
        cost.minimum_memory_bytes = cost.minimum_memory_bytes.max(cost.peak_memory_upper);
        cost.non_revocable_memory_upper =
            cost.non_revocable_memory_upper.max(cost.peak_memory_upper);
        cost.revocable_memory_target = 0;
        cost.validate()?;
        return Ok(Some(cost));
    }
    if cost.peak_memory_upper == u64::MAX {
        if spillable && class.spill_policy == paro_planner::physical::SpillPolicy::Allowed {
            // These implementations allocate all retained state through the
            // query pool. The allocator bounds resident memory and the spill
            // protocol proves forward progress. Spill volume stays UNKNOWN.
            cost.peak_memory_upper = class.hard_memory_bytes;
            cost.revocable_memory_target = cost
                .revocable_memory_target
                .min(class.hard_memory_bytes - cost.minimum_memory_bytes);
            cost.validate()?;
            return Ok(Some(cost));
        }
        if cost.memory_completion.is_runtime_capped() {
            // The query allocator is the resident-memory proof for this
            // explicitly best-effort implementation.  This does not promote
            // it to a forward-progress guarantee: artifact selection keeps
            // preferring any fully bounded or spillable alternative.
            cost.apply_runtime_cap(class.hard_memory_bytes, cost.minimum_memory_bytes)?;
            return Ok(Some(cost));
        }
        return Ok(None);
    }
    if force_spill && spillable {
        if class.spill_policy == paro_planner::physical::SpillPolicy::Forbidden {
            return Ok(None);
        } else {
            let spilled = cost.revocable_memory_target.max(1);
            cost.peak_memory_upper = cost.peak_memory_upper.min(class.hard_memory_bytes);
            cost.revocable_memory_target = cost
                .revocable_memory_target
                .min(cost.peak_memory_upper - cost.minimum_memory_bytes);
            add_spill_cost(&mut cost, spilled)?;
            cost.validate()?;
            return Ok(Some(cost));
        }
    }
    if cost.peak_memory_upper <= class.hard_memory_bytes {
        return Ok(Some(cost));
    }
    if spillable && class.spill_policy == paro_planner::physical::SpillPolicy::Allowed {
        let spilled = cost
            .preferred_memory_bytes()
            .saturating_sub(class.hard_memory_bytes);
        cost.peak_memory_upper = class.hard_memory_bytes;
        cost.revocable_memory_target = cost
            .revocable_memory_target
            .min(class.hard_memory_bytes - cost.minimum_memory_bytes);
        if spilled > 0 {
            add_spill_cost(&mut cost, spilled)?;
        }
        cost.validate()?;
        return Ok(Some(cost));
    }
    if cost.memory_completion.is_runtime_capped() {
        cost.apply_runtime_cap(class.hard_memory_bytes, cost.minimum_memory_bytes)?;
        return Ok(Some(cost));
    }
    Ok(None)
}

pub(crate) struct LocalCostModel<'a> {
    pub operator_type: LogicalOperatorType,
    pub local_cost: PhysicalCost,
    pub baseline: PhysicalImplementationFlavor,
    pub resource_sensitive: bool,
    pub spillable: bool,
    pub perfect_hash: Option<paro_planner::physical::PerfectHashResourceContract>,
    pub runtime_filter_key_types: &'a [LogicalType],
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedPlannerCostFacts {
    pub(crate) output_rows: CompactRange,
    pub(crate) child_rows: Box<[CompactRange]>,
    pub(crate) output_rows_hard_upper: Option<u64>,
    pub(crate) child_rows_hard_upper: Box<[Option<u64>]>,
    pub(crate) child_row_widths: Box<[u64]>,
    pub(crate) child_materialization_risk_rows: Box<[u64]>,
    pub(crate) output_row_width: u64,
    pub(crate) hash_key_width: Option<u64>,
    pub(crate) scan_access_width: Option<u64>,
    pub(crate) predicate_width: Option<u64>,
    pub(crate) scan_work_source: Option<WorkSourceId>,
    pub(crate) perfect_hash: Option<crate::physical::PerfectHashResourceContract>,
    pub(crate) topn_capacity: Option<u64>,
    pub(crate) runtime_filter_probe_sources: Box<[ResolvedRuntimeFilterSource]>,
    pub(crate) runtime_filter_build_left_probe_sources: Box<[ResolvedRuntimeFilterSource]>,
    /// Snapshot distinct estimate of each build key, by equality condition.
    pub(crate) runtime_filter_build_key_distinct: Box<[Option<u64>]>,
    pub(crate) runtime_filter_build_left_key_distinct: Box<[Option<u64>]>,
    pub(crate) runtime_filter_key_types: Box<[LogicalType]>,
}

/// Domain of one runtime-filter key column at its traced source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum RuntimeFilterProbeKeyDomain {
    #[default]
    Unknown,
    /// Snapshot HLL evidence for the probe-key domain. This is an expected
    /// distribution input only; it is neither a schema invariant nor a
    /// correctness or memory proof.
    EstimatedDistinct { keys: u64 },
    /// Catalog uniqueness survives plan reuse and may tighten the risk range.
    DeclaredUnique,
}

/// One equality key of a runtime filter as it reaches a traced source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeFilterSourceKey {
    /// Position of the join's equality condition, which is also the
    /// filter's key position.
    pub(crate) key: usize,
    /// Source output column holding the key.
    pub(crate) column: usize,
    pub(crate) domain: RuntimeFilterProbeKeyDomain,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedRuntimeFilterSource {
    pub(crate) source: WorkSourceId,
    /// The filter keys this source holds, by ascending column.
    pub(crate) keys: Box<[RuntimeFilterSourceKey]>,
    pub(crate) rows: CompactRange,
}

/// The build side of one runtime filter. Execution keeps one membership set
/// per equality key and a probe row must pass every key's set, so each key
/// has its own domain and exactness.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeFilterBuild<'a> {
    pub(crate) keys: Box<[RuntimeFilterBuildKey]>,
    pub(crate) sources: &'a [ResolvedRuntimeFilterSource],
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RuntimeFilterBuildKey {
    pub(crate) domain: CompactRange,
    pub(crate) exactness: RuntimeFilterExactness,
}

/// The runtime filter `flavor` builds, if any.
pub(crate) fn runtime_filter_build(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    max_concurrent_tasks: u16,
) -> Result<Option<RuntimeFilterBuild<'_>>> {
    use PhysicalImplementationFlavor as F;
    let (index, distinct, sources) = match flavor {
        F::HashJoinRuntimeFilter => (
            1,
            &facts.runtime_filter_build_key_distinct,
            &facts.runtime_filter_probe_sources,
        ),
        F::HashJoinBuildLeftRuntimeFilter => (
            0,
            &facts.runtime_filter_build_left_key_distinct,
            &facts.runtime_filter_build_left_probe_sources,
        ),
        _ => return Ok(None),
    };
    let rows = facts
        .child_rows
        .get(index)
        .copied()
        .unwrap_or(CompactRange::ZERO);
    let hard_upper = facts.child_rows_hard_upper.get(index).copied().flatten();
    let resource = crate::physical::RuntimeFilterResourceContract::for_keys(
        &facts.runtime_filter_key_types,
        max_concurrent_tasks,
    )?;
    let keys = (0..facts.runtime_filter_key_types.len())
        .map(|key| {
            let domain = runtime_filter_build_domain(distinct.get(key).copied().flatten(), rows)?;
            Ok(RuntimeFilterBuildKey {
                domain,
                exactness: runtime_filter_exactness(&resource, key, hard_upper, domain.expected),
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    Ok(Some(RuntimeFilterBuild { keys, sources }))
}

impl RuntimeFilterBuild<'_> {
    /// Membership lookups this filter performs: every key's set is probed by
    /// every source row.
    pub(crate) fn lookups(&self) -> Result<CompactRange> {
        self.sources
            .iter()
            .try_fold(CompactRange::ZERO, |sum, source| {
                sum.checked_add(scaled_work(source.rows, source.keys.len() as f64)?)
            })
    }

    /// Fraction of `rows` source rows that pass `key`'s membership set.
    pub(crate) fn key_retained(&self, rows: f64, key: &RuntimeFilterSourceKey) -> f64 {
        let Some(build) = self.keys.get(key.key) else {
            return 1.0;
        };
        if !build.exactness.expected_exact() || rows <= 0.0 {
            return 1.0;
        }
        let probe_domain = match key.domain {
            RuntimeFilterProbeKeyDomain::Unknown => return 1.0,
            RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys } => keys as f64,
            RuntimeFilterProbeKeyDomain::DeclaredUnique => rows,
        };
        (build.domain.expected / probe_domain.min(rows).max(1.0)).clamp(0.0, 1.0)
    }

    /// Rows of `source` that pass every key's membership set. Under key
    /// containment a key keeps the share of the source's key domain its build
    /// domain covers, as reduction cardinality estimates do; keys on
    /// different columns are independent. Skew and stale snapshot NDVs are
    /// risk, carried by the complete-source upper rather than by discounting
    /// the expectation. A row count is not a key-domain estimate, and a
    /// coarse range claims no reduction.
    pub(crate) fn source_survivors(
        &self,
        source: &ResolvedRuntimeFilterSource,
    ) -> Result<CompactRange> {
        let rows = source.rows;
        if rows.expected <= 0.0 {
            return CompactRange::new(0.0, 0.0, rows.upper.max(0.0));
        }
        let (mut retained, mut upper) = (1.0, rows.upper);
        for key in source.keys.iter() {
            retained *= self.key_retained(rows.expected, key);
            // An exact set of a unique column admits one row per build key.
            // Expected exactness may still degrade to a range under skew, and
            // estimated uniqueness is not a proof.
            if let (
                Some(RuntimeFilterBuildKey {
                    exactness: RuntimeFilterExactness::Guaranteed { build_keys_upper },
                    ..
                }),
                RuntimeFilterProbeKeyDomain::DeclaredUnique,
            ) = (self.keys.get(key.key), key.domain)
            {
                upper = upper.min(*build_keys_upper as f64);
            }
        }
        let expected = rows.expected * retained;
        CompactRange::new(0.0, expected, upper.max(expected))
    }

    /// Rows of a stream derived from the traced sources that survive this
    /// filter. Each source keeps its own estimate, and the fraction kept by
    /// all sources applies to every row derived from them. Source-work
    /// retention uses the same per-source estimates, so a join and its
    /// sources cannot disagree about one filter. Without traced sources
    /// nothing is kept out.
    pub(crate) fn survivors(&self, rows: CompactRange) -> Result<CompactRange> {
        let (mut total, mut kept) = (CompactRange::ZERO, CompactRange::ZERO);
        for source in self.sources {
            total = total.checked_add(source.rows)?;
            kept = kept.checked_add(self.source_survivors(source)?)?;
        }
        let fraction = |kept: f64, total: f64| {
            if total > 0.0 {
                (kept / total).clamp(0.0, 1.0)
            } else {
                1.0
            }
        };
        let expected = fraction(kept.expected, total.expected);
        let upper = fraction(kept.upper, total.upper).max(expected);
        CompactRange::new(
            rows.lower * expected,
            rows.expected * expected,
            rows.upper * upper,
        )
    }
}

pub(crate) fn flavor_spillable(
    metadata: &LocalCostModel<'_>,
    flavor: PhysicalImplementationFlavor,
) -> bool {
    match flavor {
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
        | PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PartitionAggregateWindow
        | PhysicalImplementationFlavor::Window => metadata.spillable,
        PhysicalImplementationFlavor::AdaptiveSort => metadata.spillable,
        PhysicalImplementationFlavor::CrossProductExternal => metadata.spillable,
        PhysicalImplementationFlavor::Structural => metadata.spillable,
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::HeapTopN
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => false,
    }
}

const OP_NESTED_LOOP_PAIR: OpClassId = OpClassId(3);
const OP_SORT_COMPARE: OpClassId = OpClassId(4);
pub(crate) const OP_RANGE_JOIN_ROW: OpClassId = OpClassId(5);
pub(crate) const OP_IE_JOIN_ROW: OpClassId = OpClassId(6);
const OP_HASH_AGGREGATE_ROW: OpClassId = OpClassId(7);
const OP_HASH_AGGREGATE_GROUP: OpClassId = OpClassId(8);
const OP_PERFECT_AGGREGATE_ROW: OpClassId = OpClassId(9);
const OP_PERFECT_AGGREGATE_SLOT: OpClassId = OpClassId(10);
// 11 and 12 are the stable runtime-filter classes in `calibration`; keep the
// planner-local namespace disjoint so calibration cannot silently price an
// unrelated operator with runtime-filter coefficients.
const OP_WINDOW_ROW: OpClassId = OpClassId(13);
const OP_PARTITION_AGGREGATE_WINDOW_ROW: OpClassId = OpClassId(14);
const OP_SINGLETON_AGGREGATE_PROJECT_ROW: OpClassId = OpClassId(15);

pub(crate) fn multiply_work(left: CompactRange, right: CompactRange) -> Result<CompactRange> {
    CompactRange::new(
        left.lower * right.lower,
        left.expected * right.expected,
        left.upper * right.upper,
    )
}

pub(crate) fn sort_work(rows: CompactRange) -> Result<CompactRange> {
    let comparisons = |value: f64| {
        if value <= 1.0 {
            value
        } else {
            value * value.log2()
        }
    };
    CompactRange::new(
        comparisons(rows.lower),
        comparisons(rows.expected),
        comparisons(rows.upper),
    )
}

fn topn_work(rows: CompactRange, capacity: u64) -> Result<CompactRange> {
    let comparisons_per_row = (capacity.max(2) as f64).log2();
    scaled_work(rows, comparisons_per_row)
}

pub(crate) fn implementation_cost(
    metadata: &LocalCostModel<'_>,
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
) -> Result<PhysicalCost> {
    let mut work = LocalOperatorWork::default();
    let peak_memory_upper;
    match flavor {
        PhysicalImplementationFlavor::Structural => {
            return refreshed_structural_cost(metadata, facts, max_concurrent_tasks);
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {}
        _ => add_tuple_byte_work(&mut work, facts)?,
    }
    match flavor {
        PhysicalImplementationFlavor::Structural => {
            unreachable!()
        }
        PhysicalImplementationFlavor::AdaptiveSort => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SORT_COMPARE, sort_work(input)?)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(
                    facts
                        .child_row_widths
                        .first()
                        .copied()
                        .unwrap_or(facts.output_row_width)
                        .max(1),
                );
        }
        PhysicalImplementationFlavor::HeapTopN => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            let capacity = facts.topn_capacity.ok_or_else(|| {
                paro_error::internal("heap TopN candidate lost its bounded capacity")
            })?;
            work.add(OP_SORT_COMPARE, topn_work(input, capacity)?)?;
            peak_memory_upper = capacity.saturating_mul(
                facts
                    .child_row_widths
                    .first()
                    .copied()
                    .unwrap_or(facts.output_row_width)
                    .max(1),
            );
        }
        PhysicalImplementationFlavor::HashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_HASH_AGGREGATE_ROW, input)?;
            add_hash_key_byte_work(&mut work, input, facts.hash_key_width)?;
            work.add(OP_HASH_AGGREGATE_GROUP, facts.output_rows)?;
            peak_memory_upper = facts
                .output_rows_hard_upper
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.saturating_add(16));
        }
        PhysicalImplementationFlavor::PerfectHashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            let resource = facts.perfect_hash.ok_or_else(|| {
                paro_error::internal("perfect-hash candidate lost its proven key domain")
            })?;
            work.add(OP_PERFECT_AGGREGATE_ROW, input)?;
            work.add(
                OP_PERFECT_AGGREGATE_SLOT,
                CompactRange::point(resource.slots as f64)?,
            )?;
            peak_memory_upper = resource.memory.preferred_memory_bytes().unwrap_or(u64::MAX);
        }
        PhysicalImplementationFlavor::SingletonAggregateProjection => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SINGLETON_AGGREGATE_PROJECT_ROW, input)?;
            peak_memory_upper = 0;
        }
        PhysicalImplementationFlavor::Window => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SORT_COMPARE, sort_work(input)?)?;
            work.add(OP_WINDOW_ROW, input)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::PartitionAggregateWindow => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_PARTITION_AGGREGATE_WINDOW_ROW, input)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let shape = hash_join_shape(facts, flavor, max_concurrent_tasks)?;
            if let Some(filter) = shape.runtime_filter {
                // Every key keeps its own membership set of the build rows.
                work.add(
                    OP_RUNTIME_FILTER_BUILD_ROW,
                    scaled_work(shape.build, filter.keys as f64)?,
                )?;
                // A non-local runtime filter runs at the traced rowset source,
                // before any intervening joins. Price every source-row lookup;
                // charging only the already-reduced logical child makes a
                // second sideways filter appear almost free and can select a
                // physically slower plan. Source lanes replace this by the
                // lookups the scan's staged evaluation actually performs.
                work.add(OP_RUNTIME_FILTER_APPLY_ROW, filter.lookups)?;
            }
            crate::cost::join::add_hash_join_work(&mut work, hash_join_points(facts, &shape, 1.0))?;
            peak_memory_upper = shape.build_hard_upper.unwrap_or(u64::MAX).saturating_mul(
                crate::cost::join::hash_build_width(
                    facts.child_row_widths[shape.build_index] as f64,
                    facts.hash_key_width.unwrap_or(8) as f64,
                ) as u64,
            );
        }
        PhysicalImplementationFlavor::NestedLoopJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(OP_NESTED_LOOP_PAIR, multiply_work(left, right)?)?;
            work.add(OP_RANGE_JOIN_ROW, facts.output_rows)?;
            peak_memory_upper = if flavor == PhysicalImplementationFlavor::CrossProductExternal {
                0
            } else {
                facts
                    .child_rows_hard_upper
                    .get(1)
                    .copied()
                    .flatten()
                    .unwrap_or(u64::MAX)
                    .saturating_mul(
                        facts
                            .child_row_widths
                            .get(1)
                            .copied()
                            .unwrap_or(facts.output_row_width)
                            .max(1),
                    )
            };
        }
        PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::CrossProductExternal => {
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            // Materialization writes the build once and the probe reads it for
            // every left row. The tuple-byte base charge already includes the
            // logical output; external execution adds a second build pass for
            // serialization and replay.
            if flavor == PhysicalImplementationFlavor::CrossProductExternal {
                let width = facts
                    .child_row_widths
                    .get(1)
                    .copied()
                    .unwrap_or(facts.output_row_width);
                work.add(
                    OP_TUPLE_BYTE_BLOCK,
                    scaled_work(right, width as f64 / 16.0)?,
                )?;
            }
            peak_memory_upper = facts
                .child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(
                    facts
                        .child_row_widths
                        .get(1)
                        .copied()
                        .unwrap_or(facts.output_row_width)
                        .max(1),
                );
        }
        PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(
                OP_SORT_COMPARE,
                sort_work(left)?.checked_add(sort_work(right)?)?,
            )?;
            work.add(
                if flavor == PhysicalImplementationFlavor::SortRangeJoin {
                    OP_RANGE_JOIN_ROW
                } else {
                    OP_IE_JOIN_ROW
                },
                left.checked_add(right)?.checked_add(facts.output_rows)?,
            )?;
            let left_hard = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX);
            let right_hard = facts
                .child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX);
            peak_memory_upper = left_hard
                .saturating_add(right_hard)
                .saturating_mul(facts.output_row_width.max(32));
        }
    }
    // Preserve implementation work in a serial-normalized form. Local selection
    // engine assigns it to scheduler-visible phases only after concrete child
    // winners expose their pipeline task supply.
    let mut cost = calibration.fold(&work)?;
    let retained_memory_target = expected_retained_memory_target(facts, flavor, peak_memory_upper);
    apply_execution_memory_contract(
        metadata,
        flavor,
        peak_memory_upper,
        retained_memory_target,
        max_concurrent_tasks,
        &mut cost,
    )?;
    if flavor == PhysicalImplementationFlavor::CrossProductExternal {
        let right = facts
            .child_rows
            .get(1)
            .copied()
            .unwrap_or(CompactRange::ZERO);
        let width = facts
            .child_row_widths
            .get(1)
            .copied()
            .unwrap_or(facts.output_row_width)
            .max(1);
        add_spill_cost(
            &mut cost,
            (right.expected * width as f64).min(u64::MAX as f64) as u64,
        )?;
    }
    cost.validate()?;
    Ok(cost)
}

/// Work of evaluating a runtime filter on `rows` source rows: exactly the
/// apply part of the owning join's `implementation_cost` for those rows.
pub(crate) fn runtime_filter_apply_cost(
    rows: CompactRange,
    calibration: &MachineCalibrationBundle,
) -> Result<PhysicalCost> {
    let mut work = LocalOperatorWork::default();
    work.add(OP_RUNTIME_FILTER_APPLY_ROW, rows)?;
    Ok(calibration.fold(&work)?.work_only())
}

/// Inputs of one hash-join flavor after its own runtime filter, if any.
struct HashJoinShape {
    build_index: usize,
    build: CompactRange,
    build_hard_upper: Option<u64>,
    /// Rows entering the probe. Runtime filtering is evaluated at the traced
    /// source, but rows rejected there never enter this join operator.
    probe: CompactRange,
    /// This flavor's runtime filter, if any.
    runtime_filter: Option<HashJoinRuntimeFilter>,
}

#[derive(Clone, Copy)]
struct HashJoinRuntimeFilter {
    keys: usize,
    /// Membership lookups at the traced sources, or at the probe without one.
    lookups: CompactRange,
}

fn hash_join_shape(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    max_concurrent_tasks: u16,
) -> Result<HashJoinShape> {
    use PhysicalImplementationFlavor as F;
    if facts.child_row_widths.len() != 2 {
        return Err(paro_error::internal(
            "hash-join work requires two typed input widths",
        ));
    }
    let left = facts
        .child_rows
        .first()
        .copied()
        .unwrap_or(CompactRange::ZERO);
    let right = facts
        .child_rows
        .get(1)
        .copied()
        .unwrap_or(CompactRange::ZERO);
    let build_left = matches!(
        flavor,
        F::HashJoinBuildLeft | F::HashJoinBuildLeftRuntimeFilter
    );
    let (build, probe) = if build_left {
        (left, right)
    } else {
        (right, left)
    };
    let build_index = usize::from(!build_left);
    let build_materialization_risk = facts
        .child_materialization_risk_rows
        .get(build_index)
        .copied()
        .unwrap_or(0) as f64;
    let build_work = CompactRange::new(
        build.lower,
        build.expected,
        build.upper.max(build_materialization_risk),
    )?;
    let build_hard_upper = facts
        .child_rows_hard_upper
        .get(build_index)
        .copied()
        .flatten();
    let filter = runtime_filter_build(facts, flavor, max_concurrent_tasks)?;
    let Some(filter) = filter else {
        return Ok(HashJoinShape {
            build_index,
            build: build_work,
            build_hard_upper,
            probe,
            runtime_filter: None,
        });
    };
    let keys = filter.keys.len().max(1);
    Ok(HashJoinShape {
        build_index,
        build: build_work,
        build_hard_upper,
        probe: filter.survivors(probe)?,
        runtime_filter: Some(HashJoinRuntimeFilter {
            keys,
            lookups: if filter.sources.is_empty() {
                scaled_work(probe, keys as f64)?
            } else {
                filter.lookups()?
            },
        }),
    })
}

fn hash_join_points(
    facts: &ResolvedPlannerCostFacts,
    shape: &HashJoinShape,
    build_scale: f64,
) -> [crate::cost::join::HashJoinWork; 3] {
    // Local tuple movement and probing use only the surviving probe rows;
    // source-work composition prices the full lookup stream separately, or
    // wide probes are charged twice.
    let point = |build_rows: f64, probe_rows, output_rows| crate::cost::join::HashJoinWork {
        build_rows: build_rows * build_scale,
        probe_rows,
        output_rows,
        build_width: facts.child_row_widths[shape.build_index] as f64,
        probe_width: facts.child_row_widths[1 - shape.build_index] as f64,
        output_width: facts.output_row_width as f64,
        key_width: facts.hash_key_width.unwrap_or(8) as f64,
    };
    [
        point(
            shape.build.lower,
            shape.probe.lower,
            facts.output_rows.lower,
        ),
        point(
            shape.build.expected,
            shape.probe.expected,
            facts.output_rows.expected,
        ),
        point(
            shape.build.upper,
            shape.probe.upper,
            facts.output_rows.upper,
        ),
    ]
}

/// Work of a hash join's streaming side: probing and emitting its output.
/// Hash-join units are linear in build, probe and output rows, so this is an
/// exact part of `implementation_cost` for the same flavor, excluding the
/// build and any runtime-filter build/apply work. `None` for other flavors.
pub(crate) fn hash_join_probe_stream_cost(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
) -> Result<Option<PhysicalCost>> {
    use PhysicalImplementationFlavor as F;
    if !matches!(
        flavor,
        F::HashJoin
            | F::HashJoinBuildLeft
            | F::HashJoinRuntimeFilter
            | F::HashJoinBuildLeftRuntimeFilter
    ) {
        return Ok(None);
    }
    let shape = hash_join_shape(facts, flavor, max_concurrent_tasks)?;
    let mut work = LocalOperatorWork::default();
    crate::cost::join::add_hash_join_work(&mut work, hash_join_points(facts, &shape, 0.0))?;
    Ok(Some(calibration.fold(&work)?.work_only()))
}

pub(crate) fn useful_output_tasks(
    facts: &ResolvedPlannerCostFacts,
    max_concurrent_tasks: u16,
) -> u16 {
    u16::try_from(useful_pipeline_tasks(
        estimated_bytes(facts.output_rows.expected, facts.output_row_width),
        usize::from(max_concurrent_tasks.max(1)),
    ))
    .unwrap_or(max_concurrent_tasks.max(1))
    .max(1)
}

fn apply_execution_memory_contract(
    metadata: &LocalCostModel<'_>,
    flavor: PhysicalImplementationFlavor,
    retained_memory_upper: u64,
    retained_memory_target: u64,
    max_concurrent_tasks: u16,
    cost: &mut PhysicalCost,
) -> Result<()> {
    use crate::physical::resources::{
        ExecutionMemoryContract, BLOCKING_FIXED_SCRATCH_BYTES, BLOCKING_PER_TASK_SCRATCH_BYTES,
        SPILL_BUFFER_MINIMUM_BYTES,
    };
    use crate::physical::MemoryCompletion;

    let stateful = retained_memory_upper > 0
        || matches!(
            flavor,
            PhysicalImplementationFlavor::HashAggregate
                | PhysicalImplementationFlavor::PerfectHashAggregate
                | PhysicalImplementationFlavor::HashJoin
                | PhysicalImplementationFlavor::HashJoinBuildLeft
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
                | PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | PhysicalImplementationFlavor::NestedLoopJoin
                | PhysicalImplementationFlavor::SortRangeJoin
                | PhysicalImplementationFlavor::ClassicIeJoin
                | PhysicalImplementationFlavor::CrossProductInMemory
                | PhysicalImplementationFlavor::CrossProductExternal
                | PhysicalImplementationFlavor::AdaptiveSort
                | PhysicalImplementationFlavor::HeapTopN
                | PhysicalImplementationFlavor::Window
                | PhysicalImplementationFlavor::PartitionAggregateWindow
        );
    if !stateful {
        return Ok(());
    }

    let spillable = flavor_spillable(metadata, flavor);
    let contract = if flavor == PhysicalImplementationFlavor::PerfectHashAggregate {
        metadata
            .perfect_hash
            .ok_or_else(|| {
                paro_error::internal("perfect-hash memory contract disappeared during costing")
            })?
            .memory
    } else if spillable {
        let fixed_non_revocable_bytes = if matches!(
            flavor,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        ) {
            crate::physical::RuntimeFilterResourceContract::for_keys(
                metadata.runtime_filter_key_types,
                max_concurrent_tasks,
            )?
            .peak_memory_bytes
        } else {
            0
        };
        let base = ExecutionMemoryContract {
            fixed_non_revocable_bytes,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: BLOCKING_PER_TASK_SCRATCH_BYTES,
            max_concurrent_tasks,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: SPILL_BUFFER_MINIMUM_BYTES,
        };
        let minimum = base.minimum_memory_bytes()?;
        let contract = ExecutionMemoryContract {
            // The hard upper may deliberately remain unknown (`u64::MAX`).
            // A preferred target is not a correctness proof and must stay a
            // representable addition to the executable floor; admission will
            // cap it to the granted working set while spill preserves
            // progress at the floor.
            revocable_target_bytes: retained_memory_target
                .min(retained_memory_upper)
                .min(u64::MAX - minimum),
            ..base
        };
        let peak_memory_upper = if retained_memory_upper == u64::MAX {
            u64::MAX
        } else {
            minimum.saturating_add(retained_memory_upper)
        };
        publish_execution_memory_contract(
            cost,
            contract,
            contract.fixed_non_revocable_bytes,
            peak_memory_upper,
            MemoryCompletion::Guaranteed,
        )?;
        return Ok(());
    } else if retained_memory_upper == u64::MAX {
        // A missing semantic row bound cannot prove how much state this
        // non-spillable operator will eventually retain.  It also must not
        // erase the only semantically valid implementation.  Publish the
        // executable scratch floor and preserve the unknown retained-state
        // upper explicitly; grant admission will cap resident memory while
        // keeping the weaker completion contract visible to plan selection.
        let contract = ExecutionMemoryContract {
            fixed_non_revocable_bytes: 0,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: 0,
            max_concurrent_tasks: 0,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: 0,
        };
        publish_execution_memory_contract(
            cost,
            contract,
            u64::MAX,
            u64::MAX,
            MemoryCompletion::runtime_capped_unbounded(),
        )?;
        return Ok(());
    } else {
        ExecutionMemoryContract {
            fixed_non_revocable_bytes: retained_memory_upper,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: 0,
            max_concurrent_tasks: 0,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: 0,
        }
    };
    publish_execution_memory_contract(
        cost,
        contract,
        contract.fixed_non_revocable_bytes,
        contract.preferred_memory_bytes()?,
        MemoryCompletion::Guaranteed,
    )
}

fn publish_execution_memory_contract(
    cost: &mut PhysicalCost,
    contract: crate::physical::resources::ExecutionMemoryContract,
    non_revocable_memory_upper: u64,
    peak_memory_upper: u64,
    memory_completion: crate::physical::MemoryCompletion,
) -> Result<()> {
    contract.validate()?;
    cost.non_revocable_memory_upper = non_revocable_memory_upper;
    cost.minimum_memory_bytes = contract.minimum_memory_bytes()?;
    cost.revocable_memory_target = contract.revocable_target_bytes;
    cost.peak_memory_upper = peak_memory_upper;
    cost.memory_completion = memory_completion;
    cost.validate()
}

fn expected_retained_memory_target(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    retained_memory_upper: u64,
) -> u64 {
    let child = |index: usize| {
        facts
            .child_rows
            .get(index)
            .copied()
            .unwrap_or(CompactRange::ZERO)
            .expected
    };
    let child_width = |index: usize| {
        facts
            .child_row_widths
            .get(index)
            .copied()
            .unwrap_or(facts.output_row_width)
            .max(1)
    };
    let expected = match flavor {
        PhysicalImplementationFlavor::AdaptiveSort => estimated_bytes(child(0), child_width(0)),
        PhysicalImplementationFlavor::HeapTopN => facts
            .topn_capacity
            .unwrap_or(0)
            .saturating_mul(child_width(0)),
        PhysicalImplementationFlavor::HashAggregate => estimated_bytes(
            facts.output_rows.expected,
            facts.output_row_width.saturating_add(16),
        ),
        PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::PartitionAggregateWindow => {
            estimated_bytes(child(0), facts.output_row_width.max(32))
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let build_index = usize::from(!matches!(
                flavor,
                PhysicalImplementationFlavor::HashJoinBuildLeft
                    | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
            ));
            estimated_bytes(
                child(build_index),
                crate::cost::join::hash_build_width(
                    child_width(build_index) as f64,
                    facts.hash_key_width.unwrap_or(8) as f64,
                ) as u64,
            )
        }
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory => {
            estimated_bytes(child(1), child_width(1))
        }
        PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => estimated_bytes(
            child(0).max(0.0) + child(1).max(0.0),
            facts.output_row_width.max(32),
        ),
        PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::CrossProductExternal
        | PhysicalImplementationFlavor::Structural => retained_memory_upper,
    };
    expected.min(retained_memory_upper)
}

pub(crate) fn estimated_bytes(rows: f64, width: u64) -> u64 {
    if rows <= 0.0 {
        0
    } else if rows >= u64::MAX as f64 / width as f64 {
        u64::MAX
    } else {
        (rows.ceil() as u64).saturating_mul(width)
    }
}

fn refreshed_structural_cost(
    metadata: &LocalCostModel<'_>,
    facts: &ResolvedPlannerCostFacts,
    max_concurrent_tasks: u16,
) -> Result<PhysicalCost> {
    if matches!(
        metadata.operator_type,
        LogicalOperatorType::SearchScan
            | LogicalOperatorType::FullTextFilterScan
            | LogicalOperatorType::ExternalProject
            | LogicalOperatorType::ExternalTable
    ) {
        return Ok(metadata.local_cost);
    }
    if metadata.operator_type == LogicalOperatorType::Get {
        // Search-provider replacements reuse the logical Get implementation
        // metadata with provider-specific facts. Only an actual base-table
        // scan owns an access-width frontier; provider costs remain the
        // immutable contract recorded by that implementation.
        let cost = facts.scan_access_width.map_or_else(
            || Ok(metadata.local_cost),
            |access_width| base_table_scan_cost(facts.output_rows, access_width),
        )?;
        return Ok(cost);
    }
    let width_factor = (facts.output_row_width as f64 / 32.0).max(1.0);
    let child_count = facts.child_rows.len() as f64;
    // A materialized CTE is a real breaker: its first child is written into
    // retained storage before the consumer (the second child) can run.  The
    // child candidates account for producing and consuming tuples, but not
    // for this write.  Pricing only the wrapper's final output made a
    // single-reference materialization appear cheaper than its inline peer
    // whenever the consumer was selective.
    let materialization_write = if metadata.operator_type == LogicalOperatorType::MaterializedCTE {
        let producer = facts
            .child_rows
            .first()
            .copied()
            .unwrap_or(CompactRange::ZERO);
        let producer_width = facts
            .child_row_widths
            .first()
            .copied()
            .unwrap_or(facts.output_row_width);
        scaled_work(producer, (producer_width as f64 / 32.0).max(1.0))?
    } else {
        CompactRange::ZERO
    };
    let expected = facts.output_rows.expected.max(1.0) * width_factor
        + child_count
        + materialization_write.expected;
    let upper = facts.output_rows.upper.max(facts.output_rows.expected) * width_factor
        + child_count
        + materialization_write.upper;
    let range = CompactRange::new(1.0_f64.min(expected), expected, upper.max(expected))?;
    let mut cost = PhysicalCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
        work_latency: range,
        critical_path: range,
        ..PhysicalCost::ZERO
    };
    if metadata.resource_sensitive {
        let retained_child = match metadata.operator_type {
            LogicalOperatorType::CrossProduct => Some(1),
            LogicalOperatorType::MaterializedCTE => Some(0),
            _ => None,
        };
        let (rows, width) = retained_child.map_or_else(
            || {
                (
                    facts.output_rows_hard_upper.unwrap_or(u64::MAX),
                    facts.output_row_width,
                )
            },
            |child| {
                (
                    facts
                        .child_rows_hard_upper
                        .get(child)
                        .copied()
                        .flatten()
                        .unwrap_or(u64::MAX),
                    facts
                        .child_row_widths
                        .get(child)
                        .copied()
                        .unwrap_or(facts.output_row_width),
                )
            },
        );
        cost.peak_memory_upper = rows.saturating_mul(width);
        let resident_expected = retained_child.map_or(facts.output_rows.expected, |child| {
            facts
                .child_rows
                .get(child)
                .copied()
                .unwrap_or(CompactRange::ZERO)
                .expected
        });
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected * width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
        apply_execution_memory_contract(
            metadata,
            metadata.baseline,
            cost.peak_memory_upper,
            estimated_bytes(resident_expected, width).min(cost.peak_memory_upper),
            max_concurrent_tasks,
            &mut cost,
        )?;
    }
    if let Some(evaluation) = filter_evaluation_cost(facts)? {
        cost = cost.sequential(evaluation)?;
    }
    cost.validate()?;
    Ok(cost)
}

/// Work a filter spends evaluating its predicates: every input row reads the
/// referenced columns. Structural tuple movement prices only the survivors;
/// without this a selective string predicate looks free, and so does
/// skipping it. `None` for operators that are not filters.
pub(crate) fn filter_evaluation_cost(
    facts: &ResolvedPlannerCostFacts,
) -> Result<Option<PhysicalCost>> {
    let Some(width) = facts.predicate_width else {
        return Ok(None);
    };
    let input = facts
        .child_rows
        .first()
        .copied()
        .unwrap_or(CompactRange::ZERO);
    divisible_work_cost(scaled_work(input, width as f64 / 32.0)?).map(Some)
}

pub(crate) fn add_tuple_byte_work(
    work: &mut LocalOperatorWork,
    facts: &ResolvedPlannerCostFacts,
) -> Result<()> {
    add_tuple_byte_work_for_children(work, facts, &facts.child_rows)
}

pub(crate) fn add_tuple_byte_work_for_children(
    work: &mut LocalOperatorWork,
    facts: &ResolvedPlannerCostFacts,
    child_rows: &[CompactRange],
) -> Result<()> {
    const BYTE_BLOCK: f64 = 32.0;

    if child_rows.len() != facts.child_row_widths.len() {
        return Err(paro_error::internal(
            "tuple-byte costing has no width for one or more child streams",
        ));
    }

    let mut blocks = scaled_work(
        facts.output_rows,
        facts.output_row_width as f64 / BYTE_BLOCK,
    )?;
    for (rows, width) in child_rows.iter().zip(facts.child_row_widths.iter()) {
        blocks = blocks.checked_add(scaled_work(*rows, *width as f64 / BYTE_BLOCK)?)?;
    }
    work.add(OP_TUPLE_BYTE_BLOCK, blocks)
}

pub(crate) fn add_hash_key_byte_work(
    work: &mut LocalOperatorWork,
    rows: CompactRange,
    key_width: Option<u64>,
) -> Result<()> {
    const INTEGRAL_KEY_BASELINE_BYTES: u64 = 8;
    const BYTE_BLOCK: f64 = 32.0;

    let excess_bytes = key_width
        .unwrap_or(INTEGRAL_KEY_BASELINE_BYTES)
        .saturating_sub(INTEGRAL_KEY_BASELINE_BYTES);
    if excess_bytes != 0 {
        work.add(
            OP_HASH_KEY_BYTE_BLOCK,
            scaled_work(rows, excess_bytes as f64 / BYTE_BLOCK)?,
        )?;
    }
    Ok(())
}

pub(crate) fn scaled_work(range: CompactRange, factor: f64) -> Result<CompactRange> {
    CompactRange::new(
        range.lower * factor,
        range.expected * factor,
        range.upper * factor,
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RuntimeFilterExactness {
    Coarse,
    Expected,
    Guaranteed { build_keys_upper: u64 },
}

impl RuntimeFilterExactness {
    fn expected_exact(self) -> bool {
        !matches!(self, Self::Coarse)
    }
}

pub(crate) fn runtime_filter_exactness(
    resource: &crate::physical::RuntimeFilterResourceContract,
    key: usize,
    build_rows_hard_upper: Option<u64>,
    build_distinct_expected: f64,
) -> RuntimeFilterExactness {
    if resource.guarantees_exact_key(key, build_rows_hard_upper) {
        RuntimeFilterExactness::Guaranteed {
            build_keys_upper: build_rows_hard_upper
                .expect("an exact-membership guarantee requires a hard build bound"),
        }
    } else if resource.expects_exact_key(key, build_distinct_expected) {
        RuntimeFilterExactness::Expected
    } else {
        RuntimeFilterExactness::Coarse
    }
}

pub(crate) fn runtime_filter_build_domain(
    build_distinct_expected: Option<u64>,
    build_rows: CompactRange,
) -> Result<CompactRange> {
    let expected = build_distinct_expected
        .map(|distinct| distinct as f64)
        .unwrap_or(build_rows.expected)
        .min(build_rows.expected);
    // Snapshot NDV is an expected-cost input only. The full build cardinality
    // remains the upper domain so stale statistics cannot fabricate a proof.
    CompactRange::new(0.0, expected, build_rows.upper)
}

pub(crate) fn base_table_scan_cost(rows: CompactRange, access_width: u64) -> Result<PhysicalCost> {
    // A scan pays one fixed cursor/vector unit per row plus actual storage
    // source bytes. Do not floor the byte component: doing so makes a virtual
    // rowid indistinguishable from another stored fixed-width column.
    divisible_work_cost(scaled_work(rows, 1.0 + access_width as f64 / 32.0)?)
}

/// Serial CPU work of one pipeline lane, without memory or ownership.
pub(crate) fn divisible_work_cost(range: CompactRange) -> Result<PhysicalCost> {
    let mut cost = PhysicalCost {
        score: ScoreSummary {
            range,
            risk_adjusted: range.expected + (range.upper - range.expected) * 0.5,
        },
        work_latency: range,
        critical_path: range,
        ..PhysicalCost::ZERO
    };
    cost.resources_expected[ResourceDimension::Cpu as usize] = range.expected;
    cost.resources_risk_upper[ResourceDimension::Cpu as usize] = range.upper;
    cost.validate()?;
    Ok(cost)
}

fn add_spill_cost(cost: &mut PhysicalCost, spilled: u64) -> Result<()> {
    cost.spill_bytes_expected = cost.spill_bytes_expected.saturating_add(spilled);
    let io_work = (spilled as f64 / 4096.0).max(1.0);
    let spill_range = CompactRange::new(io_work, io_work * 2.0, io_work * 6.0)?;
    cost.score.range = cost.score.range.checked_add(spill_range)?;
    cost.score.risk_adjusted += io_work * 3.0;
    cost.critical_path = cost.critical_path.checked_add(spill_range)?;
    cost.resources_expected[ResourceDimension::SequentialIo as usize] += io_work * 2.0;
    cost.resources_risk_upper[ResourceDimension::SequentialIo as usize] += io_work * 6.0;
    cost.validate()?;
    Ok(())
}
