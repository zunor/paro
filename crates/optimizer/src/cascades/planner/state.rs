// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner payloads, implementation metadata, and transformation savepoints.

use super::*;

#[derive(Debug)]
pub(super) struct PlannerLogicalPayload {
    pub(super) skeleton: LogicalPlan,
    pub(super) output_estimate: Option<paro_planner::plan::CardinalityEstimate>,
    pub(super) column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
}

#[derive(Debug, Default)]
pub(super) struct PlannerPayloadArena {
    pub(super) logical: Vec<PlannerLogicalPayload>,
}

impl PlannerPayloadArena {
    pub(super) fn push(&mut self, payload: PlannerLogicalPayload) -> LogicalPayloadId {
        let id = LogicalPayloadId::new(self.logical.len());
        self.logical.push(payload);
        id
    }

    pub(super) fn get_physical(&self, id: PhysicalPayloadId) -> Option<&PlannerLogicalPayload> {
        self.logical.get(id.index())
    }
}

pub(super) struct PlannerTransformState {
    pub(super) columns: ColumnCatalog,
    pub(super) scalars: ScalarArena,
    pub(super) binding_ids: BTreeMap<(usize, usize, Fingerprint), ColumnId>,
    pub(super) payloads: PlannerPayloadArena,
    pub(super) metadata: BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>,
    pub(super) expression_groups: BTreeMap<LogicalExprKey, Vec<(GroupId, LogicalExprId)>>,
    pub(super) binder: Option<Binder>,
    pub(super) bind_context: BindContext,
    pub(super) session: Option<Arc<paro_context::StatementContext>>,
    pub(super) cost_model: crate::cost_model::CostModel,
    pub(super) verify_enabled: bool,
    pub(super) rowset_scan_pushdown: bool,
    pub(super) scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

pub(super) struct PlannerTransformSavepoint {
    columns: ColumnCatalog,
    scalars: ScalarArena,
    binding_ids: BTreeMap<(usize, usize, Fingerprint), ColumnId>,
    payload_count: usize,
    metadata: BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>,
    expression_groups: BTreeMap<LogicalExprKey, Vec<(GroupId, LogicalExprId)>>,
}

impl PlannerTransformState {
    pub(super) fn savepoint(&self) -> PlannerTransformSavepoint {
        PlannerTransformSavepoint {
            columns: self.columns.clone(),
            scalars: self.scalars.clone(),
            binding_ids: self.binding_ids.clone(),
            payload_count: self.payloads.logical.len(),
            metadata: self.metadata.clone(),
            expression_groups: self.expression_groups.clone(),
        }
    }

    pub(super) fn rollback_to(&mut self, savepoint: PlannerTransformSavepoint) {
        self.columns = savepoint.columns;
        self.scalars = savepoint.scalars;
        self.binding_ids = savepoint.binding_ids;
        self.payloads.logical.truncate(savepoint.payload_count);
        self.metadata = savepoint.metadata;
        self.expression_groups = savepoint.expression_groups;
    }
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
    pub(super) operator_type: LogicalOperatorType,
    pub(super) operator_fingerprint: Fingerprint,
    pub(super) provided: ProvidedProperties,
    pub(super) local_cost: SearchCost,
    pub(super) implementations: PlannerImplementationSet,
    pub(super) grant_dependency: GrantDependencyDescriptor,
    pub(super) spillable: bool,
    pub(super) cost_facts: PlannerCostFacts,
    pub(super) output_columns: Box<[ColumnId]>,
    pub(super) search: Option<PlannerSearchImplementationMetadata>,
    pub(super) required_region_facet: Option<Fingerprint>,
    pub(super) runtime_filter_region_facet: Option<Fingerprint>,
    pub(super) structural_retained_children: u64,
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
    pub(super) output_rows: CompactRange,
    pub(super) child_rows: Box<[CompactRange]>,
    pub(super) output_rows_hard_upper: Option<u64>,
    pub(super) child_rows_hard_upper: Box<[Option<u64>]>,
    pub(super) child_row_widths: Box<[u64]>,
    pub(super) output_row_width: u64,
    pub(super) perfect_hash_slots: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannerImplementationSet {
    pub(super) baseline: PhysicalImplementationFlavor,
    pub(super) perfect_hash_aggregate: bool,
    pub(super) sort_range_join: bool,
    pub(super) classic_ie_join: bool,
    pub(super) hash_join_runtime_filter: bool,
    pub(super) partition_aggregate_window: bool,
    pub(super) singleton_aggregate_projection: bool,
}

impl PlannerImplementationSet {
    pub(super) const STRUCTURAL: Self = Self {
        baseline: PhysicalImplementationFlavor::Structural,
        perfect_hash_aggregate: false,
        sort_range_join: false,
        classic_ie_join: false,
        hash_join_runtime_filter: false,
        partition_aggregate_window: false,
        singleton_aggregate_projection: false,
    };

    pub(super) fn supports(self, flavor: PhysicalImplementationFlavor) -> bool {
        match flavor {
            PhysicalImplementationFlavor::PerfectHashAggregate => self.perfect_hash_aggregate,
            PhysicalImplementationFlavor::SortRangeJoin => self.sort_range_join,
            PhysicalImplementationFlavor::ClassicIeJoin => self.classic_ie_join,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter => self.hash_join_runtime_filter,
            PhysicalImplementationFlavor::PartitionAggregateWindow => {
                self.partition_aggregate_window
            }
            PhysicalImplementationFlavor::SingletonAggregateProjection => {
                self.singleton_aggregate_projection
            }
            _ => false,
        }
    }
}
