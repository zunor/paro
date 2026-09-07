// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner payloads, implementation metadata, and transformation savepoints.

use super::*;
use paro_common::types::LogicalType;

/// Persistent ownership scope for one logical subtree.
///
/// Memo construction creates one small node per operator and materializes a
/// set only for operators that actually own a planning-region facet. This
/// avoids copying every descendant group into every ancestor on ordinary
/// plans while preserving exact set semantics at the region boundary.
#[derive(Debug, Clone)]
pub(super) struct PlannerRegionScope(Arc<PlannerRegionScopeNode>);

#[derive(Debug)]
struct PlannerRegionScopeNode {
    group: GroupId,
    children: Box<[PlannerRegionScope]>,
}

impl PlannerRegionScope {
    pub(super) fn new(
        group: GroupId,
        children: impl IntoIterator<Item = PlannerRegionScope>,
    ) -> Self {
        Self(Arc::new(PlannerRegionScopeNode {
            group,
            children: children.into_iter().collect(),
        }))
    }

    /// Materialize at most `ceiling + 1` unique groups. The overflow witness
    /// rejects an oversized facet without walking the remainder of its tree.
    pub(super) fn materialize_bounded(&self, ceiling: usize) -> (BTreeSet<GroupId>, bool) {
        let mut groups = BTreeSet::new();
        let mut pending = vec![self.clone()];
        while let Some(scope) = pending.pop() {
            if groups.insert(scope.0.group) && groups.len() > ceiling {
                return (groups, true);
            }
            pending.extend(scope.0.children.iter().cloned());
        }
        (groups, false)
    }
}

#[derive(Debug)]
pub(super) struct PlannerLogicalPayload {
    /// Binding-based operator semantics. Positional projection maps and input
    /// slots are derived only after winner selection.
    pub(super) semantic_template: LogicalPlan,
    /// Exact canonical encoding of this operator shell. The Memo hashes this
    /// value for lookup but compares the bytes before declaring equivalence.
    pub(super) operator_encoding: Box<[u8]>,
    pub(super) column_stats: SharedColumnStatistics,
}

#[derive(Debug)]
pub(super) enum PlannerPhysicalTemplate {
    Logical(LogicalPayloadId),
    Executable(Box<LogicalPlan>),
}

#[derive(Debug)]
pub(super) struct PlannerPhysicalPayload {
    pub(super) template: PlannerPhysicalTemplate,
}

#[derive(Debug, Default)]
pub(super) struct PlannerPayloadArena {
    pub(super) logical: Vec<PlannerLogicalPayload>,
    pub(super) physical: Vec<PlannerPhysicalPayload>,
}

impl PlannerPayloadArena {
    pub(super) fn push_logical(
        &mut self,
        payload: PlannerLogicalPayload,
    ) -> (LogicalPayloadId, PhysicalPayloadId) {
        let id = LogicalPayloadId::new(self.logical.len());
        self.logical.push(payload);
        let physical = self.push_physical(PlannerPhysicalTemplate::Logical(id));
        (id, physical)
    }

    pub(super) fn push_physical(&mut self, template: PlannerPhysicalTemplate) -> PhysicalPayloadId {
        let id = PhysicalPayloadId::new(self.physical.len());
        self.physical.push(PlannerPhysicalPayload { template });
        id
    }

    pub(super) fn get_physical(&self, id: PhysicalPayloadId) -> Option<&PlannerPhysicalPayload> {
        self.physical.get(id.index())
    }
}

pub(super) struct PlannerTransformState {
    pub(super) columns: ColumnCatalog,
    pub(super) scalars: ScalarArena,
    pub(super) binding_ids: BindingCatalog,
    pub(super) payloads: PlannerPayloadArena,
    pub(super) metadata: BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>,
    pub(super) expression_groups: BTreeMap<LogicalExprKey, Vec<(GroupId, LogicalExprId)>>,
    pub(super) expression_group_insertions: Vec<(LogicalExprKey, (GroupId, LogicalExprId))>,
    pub(super) metadata_runtime_filter_changes: Vec<MetadataRuntimeFilterChange>,
    pub(super) enumerated_join_regions: BTreeSet<(GroupId, Box<[u8]>)>,
    pub(super) join_region_insertions: Vec<(GroupId, Box<[u8]>)>,
    pub(super) binder: Option<Binder>,
    pub(super) bind_context: BindContext,
    pub(super) session: Option<Arc<paro_context::StatementContext>>,
    pub(super) cost_model: crate::cost_model::CostModel,
    pub(super) verify_enabled: bool,
    pub(super) rowset_scan_pushdown: bool,
    pub(super) scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

pub(super) struct PlannerTransformSavepoint {
    column_count: usize,
    scalar_count: usize,
    binding_checkpoint: usize,
    logical_payload_count: usize,
    physical_payload_count: usize,
    expression_group_insertion_count: usize,
    metadata_runtime_filter_change_count: usize,
    join_region_insertion_count: usize,
}

impl PlannerTransformState {
    pub(super) fn savepoint(&self) -> PlannerTransformSavepoint {
        PlannerTransformSavepoint {
            column_count: self.columns.len(),
            scalar_count: self.scalars.len(),
            binding_checkpoint: self.binding_ids.checkpoint(),
            logical_payload_count: self.payloads.logical.len(),
            physical_payload_count: self.payloads.physical.len(),
            expression_group_insertion_count: self.expression_group_insertions.len(),
            metadata_runtime_filter_change_count: self.metadata_runtime_filter_changes.len(),
            join_region_insertion_count: self.join_region_insertions.len(),
        }
    }

    pub(super) fn rollback_to(&mut self, savepoint: PlannerTransformSavepoint) -> Result<()> {
        while self.join_region_insertions.len() > savepoint.join_region_insertion_count {
            let key = self
                .join_region_insertions
                .pop()
                .expect("join region journal length checked");
            self.enumerated_join_regions.remove(&key);
        }
        self.columns.truncate(savepoint.column_count)?;
        self.scalars.truncate(savepoint.scalar_count)?;
        self.binding_ids.rollback_to(savepoint.binding_checkpoint)?;
        self.payloads
            .logical
            .truncate(savepoint.logical_payload_count);
        self.payloads
            .physical
            .truncate(savepoint.physical_payload_count);
        self.metadata
            .retain(|payload, _| payload.index() < savepoint.logical_payload_count);

        if savepoint.metadata_runtime_filter_change_count
            > self.metadata_runtime_filter_changes.len()
        {
            return Err(paro_error::internal(
                "planner metadata rollback exceeds its mutation journal",
            ));
        }
        while self.metadata_runtime_filter_changes.len()
            > savepoint.metadata_runtime_filter_change_count
        {
            let change = self
                .metadata_runtime_filter_changes
                .pop()
                .expect("journal length was checked");
            if let Some(metadata) = self.metadata.get_mut(&change.payload) {
                metadata.runtime_filter_region_facet = change.previous_facet;
                metadata.implementations.hash_join_runtime_filter = change.previous_implementation;
                metadata.implementations.hash_join_build_left_runtime_filter =
                    change.previous_build_left_implementation;
            }
        }

        if savepoint.expression_group_insertion_count > self.expression_group_insertions.len() {
            return Err(paro_error::internal(
                "planner expression-group rollback exceeds its insertion journal",
            ));
        }
        while self.expression_group_insertions.len() > savepoint.expression_group_insertion_count {
            let (key, pair) = self
                .expression_group_insertions
                .pop()
                .expect("journal length was checked");
            let remove_key = {
                let candidates = self.expression_groups.get_mut(&key).ok_or_else(|| {
                    paro_error::internal(
                        "planner expression-group journal references a missing key",
                    )
                })?;
                if candidates.pop() != Some(pair) {
                    return Err(paro_error::internal(
                        "planner expression-group journal disagrees with its index",
                    ));
                }
                candidates.is_empty()
            };
            if remove_key {
                self.expression_groups.remove(&key);
            }
        }
        Ok(())
    }

    pub(super) fn record_expression_group(
        &mut self,
        key: LogicalExprKey,
        group: GroupId,
        logical: LogicalExprId,
    ) {
        self.expression_groups
            .entry(key.clone())
            .or_default()
            .push((group, logical));
        self.expression_group_insertions
            .push((key, (group, logical)));
    }

    pub(super) fn disable_runtime_filter(&mut self, payload: LogicalPayloadId) -> Result<()> {
        let metadata = self.metadata.get_mut(&payload).ok_or_else(|| {
            paro_error::internal("runtime-filter mutation references unknown planner metadata")
        })?;
        self.metadata_runtime_filter_changes
            .push(MetadataRuntimeFilterChange {
                payload,
                previous_facet: metadata.runtime_filter_region_facet,
                previous_implementation: metadata.implementations.hash_join_runtime_filter,
                previous_build_left_implementation: metadata
                    .implementations
                    .hash_join_build_left_runtime_filter,
            });
        metadata.runtime_filter_region_facet = None;
        metadata.implementations.hash_join_runtime_filter = false;
        metadata.implementations.hash_join_build_left_runtime_filter = false;
        Ok(())
    }
}

pub(super) struct MetadataRuntimeFilterChange {
    payload: LogicalPayloadId,
    previous_facet: Option<Fingerprint>,
    previous_implementation: bool,
    previous_build_left_implementation: bool,
}

impl std::fmt::Debug for PlannerTransformState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlannerTransformState")
            .field("payloads", &self.payloads.logical.len())
            .field("metadata", &self.metadata.len())
            .field("expression_groups", &self.expression_groups.len())
            .field("has_binder", &self.binder.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(super) struct PlannerOperatorMetadata {
    pub(super) origin_rule: Option<RuleId>,
    pub(super) operator_type: LogicalOperatorType,
    pub(super) operator_fingerprint: Fingerprint,
    pub(super) provided: ProvidedProperties,
    pub(super) local_cost: SearchCost,
    pub(super) implementations: PlannerImplementationSet,
    pub(super) grant_dependency: GrantDependencyDescriptor,
    pub(super) spillable: bool,
    pub(super) cost_facts: PlannerCostFacts,
    pub(super) output_columns: Box<[ColumnId]>,
    /// Binding layouts consumed by this exact operator shell. Native group
    /// holes use these layouts directly; they never materialize a descendant
    /// expression merely to recover planner-era column identities.
    pub(super) child_layouts: Box<[PlannerBindingLayout]>,
    pub(super) child_required: Box<[PropertySetId]>,
    pub(super) child_row_goals: Box<[PlannerChildRowGoal]>,
    pub(super) search: Option<PlannerSearchImplementationMetadata>,
    /// Context required to implement this expression at its owning group.
    pub(super) input_context: OptimizationContextId,
    /// Context inherited by ordinary children. Required region owners extend
    /// this context; alternatives that discharge the owner keep it unchanged.
    pub(super) child_context: OptimizationContextId,
    pub(super) required_region_facet: Option<Fingerprint>,
    pub(super) runtime_filter_region_facet: Option<Fingerprint>,
    pub(super) structural_retained_children: u64,
    pub(super) baseline_payload: PhysicalPayloadId,
}

#[derive(Debug, Clone)]
pub(super) struct PlannerBindingLayout {
    pub(super) bindings: Box<[ColumnBinding]>,
    pub(super) types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlannerChildRowGoal {
    All,
    Parent,
}

#[derive(Debug, Clone)]
pub(super) struct PlannerSearchImplementationMetadata {
    pub(super) payload: PhysicalPayloadId,
    pub(super) payload_fingerprint: Fingerprint,
    pub(super) provided: ProvidedProperties,
    pub(super) local_cost: SearchCost,
    pub(super) cost_facts: PlannerCostFacts,
}

#[derive(Debug, Clone)]
pub(super) struct PlannerCostFacts {
    pub(super) child_row_widths: Box<[u64]>,
    /// Expression-local cardinality risk for materializing each child. Unlike
    /// `child_rows_hard_upper`, this is statistical evidence used for ranking
    /// only; it never proves capacity or query correctness.
    pub(super) child_materialization_risk_rows: Box<[u64]>,
    pub(super) output_row_width: u64,
    /// Bytes participating in one hash key. Row-oriented hash work is
    /// calibrated for one integral key; wider/composite keys pay separately.
    pub(super) hash_key_width: Option<u64>,
    /// Bytes physically read from base-table column sources for each scan
    /// row. `None` identifies a non-scan structural operator.
    pub(super) scan_access_width: Option<u64>,
    /// Snapshot physical rows presented by a base-table source before
    /// predicates. This is task-supply evidence only: it affects duration
    /// ranking, never cardinality or a semantic upper bound.
    pub(super) scan_physical_rows: Option<u64>,
    pub(super) scan_work_source: Option<WorkSourceId>,
    pub(super) perfect_hash: Option<crate::physical::PerfectHashResourceContract>,
    pub(super) topn_capacity: Option<u64>,
    pub(super) runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(super) runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(super) runtime_filter_probe_source_rows: Option<paro_planner::plan::CardinalityEstimate>,
    pub(super) runtime_filter_build_left_probe_source_rows:
        Option<paro_planner::plan::CardinalityEstimate>,
    pub(super) runtime_filter_probe_sources: Box<[PlannerRuntimeFilterSource]>,
    pub(super) runtime_filter_build_left_probe_sources: Box<[PlannerRuntimeFilterSource]>,
    /// Snapshot estimate of the distinct build-key domain. This ranks
    /// runtime-filter benefit; it never proves capacity or correctness.
    pub(super) runtime_filter_build_distinct_expected: Option<u64>,
    /// Stable output identity used to resolve the current build domain from
    /// the right child group at cost-composition time.
    pub(super) runtime_filter_build_domain_column: Option<ColumnId>,
    /// Snapshot estimate for the logical-left key domain when a physical
    /// implementation inverts build and probe.
    pub(super) runtime_filter_build_left_distinct_expected: Option<u64>,
    pub(super) runtime_filter_build_left_domain_column: Option<ColumnId>,
    pub(super) runtime_filter_key_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub(super) struct ResolvedPlannerCostFacts {
    pub(super) output_rows: CompactRange,
    pub(super) child_rows: Box<[CompactRange]>,
    pub(super) output_rows_hard_upper: Option<u64>,
    pub(super) child_rows_hard_upper: Box<[Option<u64>]>,
    pub(super) child_row_widths: Box<[u64]>,
    pub(super) child_materialization_risk_rows: Box<[u64]>,
    pub(super) output_row_width: u64,
    pub(super) hash_key_width: Option<u64>,
    pub(super) scan_access_width: Option<u64>,
    pub(super) scan_physical_rows: Option<u64>,
    pub(super) scan_work_source: Option<WorkSourceId>,
    pub(super) perfect_hash: Option<crate::physical::PerfectHashResourceContract>,
    pub(super) topn_capacity: Option<u64>,
    pub(super) runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(super) runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(super) runtime_filter_probe_source_rows: Option<CompactRange>,
    pub(super) runtime_filter_build_left_probe_source_rows: Option<CompactRange>,
    pub(super) runtime_filter_probe_sources: Box<[ResolvedRuntimeFilterSource]>,
    pub(super) runtime_filter_build_left_probe_sources: Box<[ResolvedRuntimeFilterSource]>,
    pub(super) runtime_filter_build_distinct_expected: Option<u64>,
    pub(super) runtime_filter_build_left_distinct_expected: Option<u64>,
    pub(super) runtime_filter_key_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum RuntimeFilterProbeMultiplicity {
    #[default]
    Unknown,
    /// Snapshot HLL evidence for the probe-key domain. This is an expected
    /// distribution input only; it is neither a schema invariant nor a
    /// correctness or memory proof.
    EstimatedDistinct { keys: u64 },
    /// Catalog uniqueness survives plan reuse and may tighten the risk range.
    DeclaredUnique,
}

/// One physical rowset lane reached by a runtime-filter key lineage. Source
/// cardinality and key multiplicity stay attached to the lane: summing them
/// first loses a declared-unique proof when another lineage is non-unique.
#[derive(Debug, Clone)]
pub(super) struct PlannerRuntimeFilterSource {
    pub(super) source: WorkSourceId,
    pub(super) rows: paro_planner::plan::CardinalityEstimate,
    pub(super) multiplicity: RuntimeFilterProbeMultiplicity,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedRuntimeFilterSource {
    pub(super) source: WorkSourceId,
    pub(super) rows: CompactRange,
    pub(super) multiplicity: RuntimeFilterProbeMultiplicity,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannerImplementationSet {
    pub(super) baseline: PhysicalImplementationFlavor,
    pub(super) perfect_hash_aggregate: bool,
    pub(super) sort_range_join: bool,
    pub(super) classic_ie_join: bool,
    pub(super) hash_join_build_left: bool,
    pub(super) hash_join_build_left_runtime_filter: bool,
    pub(super) hash_join_runtime_filter: bool,
    pub(super) partition_aggregate_window: bool,
    pub(super) singleton_aggregate_projection: bool,
    pub(super) external_cross_product: bool,
}

impl PlannerImplementationSet {
    pub(super) const STRUCTURAL: Self = Self {
        baseline: PhysicalImplementationFlavor::Structural,
        perfect_hash_aggregate: false,
        sort_range_join: false,
        classic_ie_join: false,
        hash_join_build_left: false,
        hash_join_build_left_runtime_filter: false,
        hash_join_runtime_filter: false,
        partition_aggregate_window: false,
        singleton_aggregate_projection: false,
        external_cross_product: false,
    };

    pub(super) fn supports(self, flavor: PhysicalImplementationFlavor) -> bool {
        match flavor {
            PhysicalImplementationFlavor::PerfectHashAggregate => self.perfect_hash_aggregate,
            PhysicalImplementationFlavor::SortRangeJoin => self.sort_range_join,
            PhysicalImplementationFlavor::ClassicIeJoin => self.classic_ie_join,
            PhysicalImplementationFlavor::HashJoinBuildLeft => self.hash_join_build_left,
            PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter => {
                self.hash_join_build_left_runtime_filter
            }
            PhysicalImplementationFlavor::HashJoinRuntimeFilter => self.hash_join_runtime_filter,
            PhysicalImplementationFlavor::PartitionAggregateWindow => {
                self.partition_aggregate_window
            }
            PhysicalImplementationFlavor::SingletonAggregateProjection => {
                self.singleton_aggregate_projection
            }
            PhysicalImplementationFlavor::CrossProductExternal => self.external_cross_product,
            _ => false,
        }
    }
}
