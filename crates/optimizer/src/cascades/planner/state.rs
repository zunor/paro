// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner payloads, implementation metadata, and transformation savepoints.

use super::*;

#[derive(Debug)]
pub(super) struct PlannerLogicalPayload {
    /// Positional planner ABI retained only as an extraction recipe. Memo
    /// identity comes from semantic operator/scalar keys and group contracts.
    pub(super) extraction_template: LogicalPlan,
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
    pub(super) binding_ids: BindingCatalog,
    pub(super) payloads: PlannerPayloadArena,
    pub(super) metadata: BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>,
    pub(super) expression_groups: BTreeMap<LogicalExprKey, Vec<(GroupId, LogicalExprId)>>,
    pub(super) expression_group_insertions: Vec<(LogicalExprKey, (GroupId, LogicalExprId))>,
    pub(super) metadata_runtime_filter_changes: Vec<MetadataRuntimeFilterChange>,
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
    payload_count: usize,
    expression_group_insertion_count: usize,
    metadata_runtime_filter_change_count: usize,
}

impl PlannerTransformState {
    pub(super) fn savepoint(&self) -> PlannerTransformSavepoint {
        PlannerTransformSavepoint {
            column_count: self.columns.len(),
            scalar_count: self.scalars.len(),
            binding_checkpoint: self.binding_ids.checkpoint(),
            payload_count: self.payloads.logical.len(),
            expression_group_insertion_count: self.expression_group_insertions.len(),
            metadata_runtime_filter_change_count: self.metadata_runtime_filter_changes.len(),
        }
    }

    pub(super) fn rollback_to(&mut self, savepoint: PlannerTransformSavepoint) -> Result<()> {
        self.columns.truncate(savepoint.column_count)?;
        self.scalars.truncate(savepoint.scalar_count)?;
        self.binding_ids.rollback_to(savepoint.binding_checkpoint)?;
        self.payloads.logical.truncate(savepoint.payload_count);
        self.metadata
            .retain(|payload, _| payload.index() < savepoint.payload_count);

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
            });
        metadata.runtime_filter_region_facet = None;
        metadata.implementations.hash_join_runtime_filter = false;
        Ok(())
    }
}

pub(super) struct MetadataRuntimeFilterChange {
    payload: LogicalPayloadId,
    previous_facet: Option<Fingerprint>,
    previous_implementation: bool,
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
