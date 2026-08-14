// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline-level physical property accumulation.

use crate::physical::properties::{
    ExecutionCapabilities, MemoryClass, MemoryRequirement, MorselCapability, MorselPartitioning,
    OrderingProperty, OrderingSpec, Parallelism, PartitioningProperty, PhysicalPropertySolver,
    PipelineProperties, Placement, PrefetchPolicy, PropertyRepair, PropertyRepairKind,
    ProvidedProperties,
};

use super::graph::{SinkSpec, SourceSpec, TransformSpec};

#[derive(Debug, Clone)]
pub struct PipelinePropertyBuild {
    /// Pipeline properties after every requested repair has been applied.
    pub properties: PipelineProperties,
    /// Lowering-time repair transforms that still need to be inserted into the
    /// pipeline shape.
    pub repair: PropertyRepair,
}

#[derive(Debug, Clone)]
pub struct PipelinePropertyAccumulator {
    current: ProvidedProperties,
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
    placement: Placement,
}

impl PipelinePropertyAccumulator {
    pub fn start_from_source(source: &SourceSpec) -> Self {
        let properties = source_properties(source);
        Self {
            current: properties.provided,
            capabilities: properties.capabilities,
            memory: properties.memory,
            placement: properties.placement,
        }
    }

    pub fn apply_transform(&mut self, transform: &TransformSpec) {
        match transform {
            TransformSpec::Filter(_)
            | TransformSpec::Project(_)
            | TransformSpec::HashJoinProbe(_)
            | TransformSpec::NestedLoopJoinProbe(_)
            | TransformSpec::SortRangeJoinProbe(_)
            | TransformSpec::CrossProductProbe(_)
            | TransformSpec::ExternalProject(_)
            | TransformSpec::GraphExpand(_)
            | TransformSpec::RowFetch(_)
            | TransformSpec::GraphProject(_) => {}
            TransformSpec::Limit(_)
            | TransformSpec::StreamingTopN(_)
            | TransformSpec::StreamingWindow(_)
            | TransformSpec::GraphShortestPath(_) => {
                self.capabilities.parallelism =
                    self.capabilities.parallelism.merge(Parallelism::single());
                self.placement = self.placement.merge(Placement::SingleTask);
                self.current.partitioning = PartitioningProperty::None;
                if matches!(transform, TransformSpec::StreamingTopN(_)) {
                    self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                }
            }
            TransformSpec::PropertyRepair(spec) => self.apply_repair(&spec.kind),
        }
    }

    pub fn close_with_sink(mut self, sink: &SinkSpec) -> PipelinePropertyBuild {
        match sink {
            SinkSpec::PerfectHashAggregate(spec) => {
                if let Some(plan) = spec.spec.perfect_hash.as_ref() {
                    self.capabilities.parallelism = self
                        .capabilities
                        .parallelism
                        .merge(Parallelism::bounded(plan.max_local_tables));
                }
            }
            SinkSpec::TopNBuild(_) => {
                // TopN retains its heap, normalized keys, and payload until
                // the emit boundary. It currently has neither a reclaimer nor
                // an external path, so it is blocking but not spillable.
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
            }
            SinkSpec::PartitionAggregateWindowBuild(_) => {
                // Local and merged payload/state can be atomically moved to
                // raw radix partitions. Finalization then works one bounded
                // partition at a time and publishes reclaiming output stores.
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                self.memory.revocable = true;
                self.memory.spillable = true;
                self.capabilities.supports_spill = true;
            }
            _ => {}
        }
        let required = sink.required_properties();
        let repair =
            PhysicalPropertySolver::reconcile(&required, &self.current, &self.capabilities);
        for kind in &repair.repairs {
            self.apply_repair(kind);
        }

        PipelinePropertyBuild {
            properties: PipelineProperties {
                placement: self.placement,
                required,
                provided: self.current,
                capabilities: self.capabilities,
                memory: self.memory,
                tuning: Default::default(),
            },
            repair,
        }
    }

    fn apply_repair(&mut self, repair: &PropertyRepairKind) {
        match repair {
            PropertyRepairKind::Sort(spec) => {
                self.current.ordering = OrderingProperty::Fixed(spec.clone());
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                self.memory.spillable = true;
            }
            PropertyRepairKind::BatchIndexAdapter => {
                self.current.partitioning = PartitioningProperty::BatchIndex;
            }
            PropertyRepairKind::SingleTaskFallback => {
                self.capabilities.parallelism = Parallelism::single();
                self.placement = self.placement.merge(Placement::SingleTask);
            }
            PropertyRepairKind::MaterializationAdapter => {
                self.current.ordering = OrderingProperty::Preserved;
                self.current.partitioning = PartitioningProperty::None;
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
            }
        }
    }
}

struct SourceProperties {
    provided: ProvidedProperties,
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
    placement: Placement,
}

fn source_properties(source: &SourceSpec) -> SourceProperties {
    let mut properties = SourceProperties {
        provided: ProvidedProperties::default(),
        capabilities: ExecutionCapabilities::default(),
        memory: MemoryRequirement::default(),
        placement: Placement::Local,
    };

    match source {
        SourceSpec::Rowset(_) => {
            properties.provided.partitioning =
                PartitioningProperty::Morsel(MorselPartitioning::rowset_segments());
            properties.provided.ordering = OrderingProperty::Any;
            properties.capabilities.morsel = MorselCapability::Source;
            properties.capabilities.parallelism = Parallelism::unbounded();
            properties.capabilities.supports_late_materialization = true;
            properties.placement = Placement::Partitioned(MorselPartitioning::rowset_segments());
        }
        SourceSpec::Values(_)
        | SourceSpec::Dummy(_)
        | SourceSpec::Empty(_)
        | SourceSpec::Chunk(_)
        | SourceSpec::Expression(_)
        | SourceSpec::TableFunction(_)
        | SourceSpec::VectorSearch(_)
        | SourceSpec::SparseVectorSearch(_)
        | SourceSpec::FullTextSearch(_)
        | SourceSpec::AdaptiveSearch(_)
        | SourceSpec::GraphScan(_) => {
            properties.provided.ordering = OrderingProperty::Preserved;
            properties.capabilities.parallelism = if matches!(source, SourceSpec::GraphScan(_)) {
                Parallelism::unbounded()
            } else {
                Parallelism::single()
            };
            properties.placement = if matches!(source, SourceSpec::GraphScan(_)) {
                Placement::Local
            } else {
                Placement::SingleTask
            };
        }
        SourceSpec::Materialized(_) => {
            properties.provided.ordering = OrderingProperty::Preserved;
            properties.capabilities.parallelism = Parallelism::unbounded();
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::NljUnmatched(_)
        | SourceSpec::ClassicIeJoin(_)
        | SourceSpec::HashJoinSpillReplay(_)
        | SourceSpec::HashJoinUnmatched(_)
        | SourceSpec::HashAggregateEmit(_)
        | SourceSpec::UngroupedAggregateEmit(_)
        | SourceSpec::PerfectHashAggregateEmit(_)
        | SourceSpec::CteScan(_)
        | SourceSpec::DelimScan(_)
        | SourceSpec::RecursiveTableScan(_)
        | SourceSpec::ExternalTable(_) => {
            properties.provided.ordering = OrderingProperty::Preserved;
            properties.capabilities.parallelism = Parallelism::unbounded();
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::PartitionAggregateWindowEmit(_) => {
            // Retained batches are claimed dynamically by parallel workers;
            // the detail rows are complete but have no physical order.
            properties.provided.ordering = OrderingProperty::Unordered;
            properties.capabilities.parallelism = Parallelism::unbounded();
            properties.capabilities.preserves_order = false;
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::WindowEmit(_) => {
            properties.provided.ordering = OrderingProperty::Preserved;
            properties.capabilities.parallelism = Parallelism::single();
            properties.placement = Placement::SingleTask;
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::SetOperationEmit(_) => {
            properties.provided.ordering = OrderingProperty::Preserved;
            properties.capabilities.parallelism = Parallelism::single();
            properties.placement = Placement::SingleTask;
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::SortEmit(spec) => {
            properties.provided.ordering = OrderingProperty::Fixed(spec.ordering.clone());
            properties.capabilities.parallelism = Parallelism::single();
            properties.placement = Placement::SingleTask;
            properties.memory.class = MemoryClass::Blocking;
        }
        SourceSpec::TopNEmit(spec) => {
            properties.provided.ordering =
                OrderingProperty::Fixed(ordering_spec_from_topn(&spec.spec.orders));
            properties.capabilities.parallelism = Parallelism::single();
            properties.placement = Placement::SingleTask;
            properties.memory.class = MemoryClass::Blocking;
        }
    }

    properties
}

fn ordering_spec_from_topn(orders: &[paro_planner::binder::ir::OrderByNode]) -> OrderingSpec {
    let columns = orders
        .iter()
        .filter_map(|order| {
            let column = match &order.expression {
                paro_planner::expression::Expression::Reference(reference) => reference.index,
                paro_planner::expression::Expression::ColumnRef(column_ref) => {
                    column_ref.binding.column_index
                }
                _ => return None,
            };
            Some(crate::physical::properties::OrderingColumn {
                column,
                direction: if order.ascending {
                    crate::physical::properties::OrderingDirection::Asc
                } else {
                    crate::physical::properties::OrderingDirection::Desc
                },
                nulls: if order.nulls_first {
                    crate::physical::properties::NullOrdering::First
                } else {
                    crate::physical::properties::NullOrdering::Last
                },
            })
        })
        .collect();
    OrderingSpec::new(columns)
}

pub fn repair_transform(kind: PropertyRepairKind) -> TransformSpec {
    TransformSpec::PropertyRepair(super::graph::PropertyRepairSpec { kind })
}

pub fn rowset_prefetch_policy(distance: usize) -> PrefetchPolicy {
    PrefetchPolicy::RowsetSegments { distance }
}
