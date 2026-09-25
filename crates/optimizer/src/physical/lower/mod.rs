// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Build an immutable physical plan from committed implementation contracts.
//! The committed physical constructor assigns slots and
//! validates the chosen implementation; it must not choose an algorithm again.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_planner::expression::{
    ColumnRefExpression, ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
    ExpressionIterator, ReferenceExpression, WindowExpression, WindowFrameBound, WindowInvocation,
};
use paro_planner::logical::operator::join::{
    AntiJoinMode, ComparisonJoin, CrossProduct, Join, JoinComparisonType, JoinCondition, JoinType,
    MarkJoinSemantics,
};
use paro_planner::logical::operator::{
    Aggregate as LogicalAggregate, CTERef as LogicalCteRef, CopyTo as LogicalCopyTo,
    CreateIndex as LogicalCreateIndex, Delete as LogicalDelete, DelimGet as LogicalDelimGet,
    Distinct as LogicalDistinct, EmptyResult as LogicalEmptyResult, Explain as LogicalExplain,
    ExplainFormat, ExplainMode, ExpressionGet, Filter as LogicalFilter,
    FullTextFilterScan as LogicalFullTextFilterScan, Get, GraphExpand as LogicalGraphExpand,
    GraphScan as LogicalGraphScan, Insert as LogicalInsert, Limit as LogicalLimit,
    LogicalExternalProject, LogicalExternalTable, LogicalOperator,
    MaterializedCTE as LogicalMaterializedCte, Order as LogicalOrder,
    Projection as LogicalProjection, RecursiveCTE as LogicalRecursiveCte,
    RowFetch as LogicalRowFetch, SearchCandidate, SearchDecision, SearchScan as LogicalSearchScan,
    SetOpType, SetOperation as LogicalSetOperation, TableFunctionGet as LogicalTableFunctionGet,
    TopN as LogicalTopN, Update as LogicalUpdate, Window as LogicalWindow,
};
use paro_planner::logical::plan::OwnedLogicalPlan;
use paro_storage::search::{SearchIntent, SearchRequestMode};

use self::input::{PreparedChild, PreparedNode};
use super::children::PlanChildrenArena;
use super::edges::{PhysicalEdgeArena, PhysicalEdgeKind};
use super::ids::PhysicalPlanNodeId;
use super::node::{OperatorLabel, PhysicalPlanNode};
use super::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use super::properties::PlanPropertyMap;
use super::row_type::{ColumnIdentity, RowType};
use super::specs::{
    AdaptiveSearchSpec, AggregateSpec, BuildTimeIntegerJoinIndexSpec, ClassicIeJoinSpec,
    CopyToFileSpec, CreateIndexUtilitySpec, CrossProductSpec, CteScanSpec, DeleteSpec,
    DelimJoinSideSpec, DelimJoinSpec, DelimScanSpec, DelimScanTarget, DummyScanSpec,
    EmptyResultSpec, ExternalProjectSpec, ExternalRoutineDescriptor, ExternalTableSpec, FilterSpec,
    FullTextSearchSpec, GraphExpandSpec, GraphProjectSpec, GraphRowFetchMapping, GraphScanSpec,
    GraphShortestPathSpec, HashJoinRuntimeFilterSpec, HashJoinSpec, HashReductionCascadeSpec,
    HashReductionExtremaChannelSpec, HashReductionGroupedExtremaSpec, HashReductionPredicateSpec,
    HashReductionSourcePredicateSpec, HashReductionStepSpec, InsertSpec, LimitSpec,
    MaterializedCteSpec, MutationInputSpoolSpec, NestedLoopJoinSpec, OutputPermutation,
    PartitionAggregateDomain, PartitionAggregateWindowSpec, PerfectHashAggregatePlan,
    PhysicalNodeKind, PostAggregateReductionSpec, ProjectSpec, RecursiveCteSpec,
    RelationalRowFetchMapping, RowFetchProjectionSpec, RowFetchSpec, RowsetColumnProjection,
    RowsetColumnValueProjection, RowsetScanAccessPolicy, RowsetScanSpec, RuntimeFilterWaitPolicy,
    SearchSourceSpec, SortRangeJoinSpec, SortSpec, SparseVectorSearchSpec, SpillExecutionPolicy,
    TableFunctionScanSpec, TopNSpec, UpdateSpec, UtilitySpec, ValuesSpec, VectorSearchSpec,
    WindowSpec,
};
use super::PhysicalPlanVerifier;

pub(crate) mod predicate_builder;

pub(crate) mod aggregate;
mod dml;
pub(crate) mod explain;
mod external;
mod graph;
pub(crate) mod graph_layout;
pub(crate) mod inequality_join_gate;
mod join;
pub(crate) mod join_output;
pub(crate) mod labels;
pub(crate) mod layout;
pub(crate) mod payload;
mod row_fetch;
mod scan;
mod set;
pub(crate) mod values;
pub(crate) mod window;

use explain::*;
use graph_layout::*;
use inequality_join_gate::*;
use join_output::*;
use labels::*;
use layout::*;
use payload::*;
use scan::is_read_csv_table_function;
use values::*;

#[derive(Debug, Clone)]
pub struct PhysicalBuildContext {
    pub force_external: bool,
    /// Spill capability admitted for this artifact class. Physical lowering
    /// must preserve it in operator specs instead of consulting runtime state.
    pub grant_spill_policy: crate::physical::SpillPolicy,
    pub rowset_scan_pushdown: bool,
    pub max_threads: usize,
    pub scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    /// Versioned compile inputs copied into every extracted physical plan
    /// before object-level dependencies are discovered.
    pub dependency_template: crate::physical::PlanDependencies,
}

impl Default for PhysicalBuildContext {
    fn default() -> Self {
        Self {
            force_external: false,
            grant_spill_policy: crate::physical::SpillPolicy::Forbidden,
            rowset_scan_pushdown: true,
            max_threads: 1,
            scan_access_cost: Default::default(),
            dependency_template: Default::default(),
        }
    }
}

impl PhysicalBuildContext {
    fn spill_execution_policy(&self, supported: bool) -> SpillExecutionPolicy {
        if !supported || self.grant_spill_policy == crate::physical::SpillPolicy::Forbidden {
            SpillExecutionPolicy::InMemory
        } else if self.force_external {
            SpillExecutionPolicy::ForcedExternal
        } else {
            SpillExecutionPolicy::Adaptive
        }
    }
}

#[derive(Debug, Default)]
pub struct PhysicalPlanBuilder {
    pub ctx: PhysicalBuildContext,
    pub arena: PhysicalPlanNodeArena,
    pub children: PlanChildrenArena,
    pub properties: PlanPropertyMap,
    pub edges: PhysicalEdgeArena,
    implementation_contracts: crate::physical::ImplementationContracts,
    mutation_barriers: crate::physical::MutationBarriers,
    statement_write_contracts: crate::physical::StatementWriteContracts,
    require_implementation_contracts: bool,
}

type BoxedLoweredNode = (Box<PhysicalNodeKind>, Vec<PhysicalPlanNodeId>);

#[inline(never)]
fn box_lowered(
    lower: impl FnOnce() -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)>,
) -> Result<BoxedLoweredNode> {
    let (kind, children) = lower()?;
    Ok((Box::new(kind), children))
}

fn boxed_leaf(kind: PhysicalNodeKind) -> BoxedLoweredNode {
    (Box::new(kind), Vec::new())
}

impl PhysicalPlanBuilder {
    pub fn new(ctx: PhysicalBuildContext) -> Self {
        Self {
            ctx,
            arena: PhysicalPlanNodeArena::default(),
            children: PlanChildrenArena::default(),
            properties: PlanPropertyMap::default(),
            edges: PhysicalEdgeArena::default(),
            implementation_contracts: Default::default(),
            mutation_barriers: Default::default(),
            statement_write_contracts: Default::default(),
            require_implementation_contracts: false,
        }
    }

    pub(crate) fn with_implementation_contracts(
        mut self,
        contracts: crate::physical::ImplementationContracts,
    ) -> Self {
        self.implementation_contracts = contracts;
        self
    }

    pub(crate) fn with_mutation_barriers(
        mut self,
        contracts: crate::physical::MutationBarriers,
    ) -> Self {
        self.mutation_barriers = contracts;
        self
    }

    pub(crate) fn with_statement_write_contracts(
        mut self,
        contracts: crate::physical::StatementWriteContracts,
    ) -> Self {
        self.statement_write_contracts = contracts;
        self
    }

    /// Query extraction requires an explicit implementation/resource contract
    /// for every selected node.
    /// Utility lowering does not enable this relational contract requirement.
    pub(crate) fn requiring_implementation_contracts(mut self) -> Self {
        self.require_implementation_contracts = true;
        self
    }

    pub fn build(&mut self, logical: OwnedLogicalPlan) -> Result<PhysicalPlan> {
        let selected = PreparedNode::from_owned(logical)?;
        self.extract_selected(&selected)
    }

    pub(crate) fn extract_selected(&mut self, logical: &PreparedNode) -> Result<PhysicalPlan> {
        self.arena = PhysicalPlanNodeArena::default();
        self.children = PlanChildrenArena::default();
        self.properties = PlanPropertyMap::default();
        self.edges = PhysicalEdgeArena::default();
        let root = self.extract_node(logical)?;
        let mut plan = PhysicalPlan::new(
            root,
            mem::take(&mut self.arena),
            mem::take(&mut self.children),
            mem::take(&mut self.properties),
        );
        plan.edges = mem::take(&mut self.edges);
        super::finalize::rewrite_projection_chains(&mut plan);
        plan.compact_reachable();
        populate_plan_dependencies(&mut plan, &self.ctx.dependency_template);
        PhysicalPlanVerifier::verify(&plan)?;
        Ok(plan)
    }

    fn extract_node(&mut self, logical: &PreparedNode) -> Result<PhysicalPlanNodeId> {
        let winner_contract = self.implementation_contracts.get(&logical.id);
        if self.require_implementation_contracts && winner_contract.is_none() {
            return Err(paro_error::internal(format!(
                "query extraction has no verified winner contract for logical node {}",
                logical.id.0
            )));
        }
        let selected_implementation = winner_contract
            .map(|contract| contract.implementation)
            .unwrap_or(crate::physical::PhysicalImplementationFlavor::Structural);
        // Keep the recursive extractor's frame independent of the size and
        // number of `PhysicalNodeKind` variants. In debug builds a direct
        // match over every lowering method otherwise reserves a distinct
        // return slot for every arm (hundreds of KiB per logical level).
        // `box_lowered` is deliberately out-of-line so only a small heap
        // handle remains live while a lowering method recursively visits its
        // children.
        let (mut kind, children) = match &logical.operator {
            LogicalOperator::Get(get) => box_lowered(|| self.lower_get(get))?,
            LogicalOperator::DummyScan => boxed_leaf(PhysicalNodeKind::DummyScan(DummyScanSpec)),
            LogicalOperator::ExpressionGet(values) => {
                box_lowered(|| Ok(self.lower_values(values)))?
            }
            LogicalOperator::EmptyResult(empty) => box_lowered(|| self.lower_empty_result(empty))?,
            LogicalOperator::Filter(filter) => {
                box_lowered(|| self.lower_filter(filter, logical.stats.estimated_cardinality))?
            }
            LogicalOperator::Projection(project) => {
                if let LogicalOperator::RowFetch(fetch) = &project.child.operator {
                    box_lowered(|| self.lower_row_fetch(fetch, Some(project)))?
                } else if is_graph_chain(project.child.as_ref()) {
                    box_lowered(|| self.lower_graph_project(project))?
                } else {
                    box_lowered(|| self.lower_project(project))?
                }
            }
            LogicalOperator::RowFetch(fetch) => box_lowered(|| self.lower_row_fetch(fetch, None))?,
            LogicalOperator::Limit(limit) => box_lowered(|| self.lower_limit(limit))?,
            LogicalOperator::Order(order) => box_lowered(|| self.lower_order(order))?,
            LogicalOperator::TopN(topn) => box_lowered(|| self.lower_topn(topn))?,
            LogicalOperator::SearchScan(scan) => {
                box_lowered(|| self.lower_search_scan(scan, logical))?
            }
            LogicalOperator::Aggregate(aggregate) => {
                box_lowered(|| self.lower_aggregate(aggregate, selected_implementation))?
            }
            LogicalOperator::Distinct(distinct) => box_lowered(|| self.lower_distinct(distinct))?,
            LogicalOperator::Join(join) => box_lowered(|| {
                self.lower_join(
                    join,
                    logical.stats.estimated_cardinality,
                    selected_implementation,
                )
            })?,
            LogicalOperator::Window(window) => {
                box_lowered(|| self.lower_window(window, selected_implementation))?
            }
            LogicalOperator::TableFunctionGet(table_function) => {
                box_lowered(|| self.lower_table_function(table_function))?
            }
            LogicalOperator::DelimGet(delim_get) => {
                box_lowered(|| Ok(self.lower_delim_get(delim_get)))?
            }
            LogicalOperator::FullTextFilterScan(scan) => {
                box_lowered(|| self.lower_fulltext_filter_scan(scan))?
            }
            LogicalOperator::ExternalProject(project) => {
                box_lowered(|| self.lower_external_project(project))?
            }
            LogicalOperator::ExternalTable(table) => {
                box_lowered(|| self.lower_external_table(table))?
            }
            LogicalOperator::GraphScan(scan) => box_lowered(|| Ok(self.lower_graph_scan(scan)))?,
            LogicalOperator::GraphExpand(expand) => {
                box_lowered(|| self.lower_graph_expand(expand))?
            }
            LogicalOperator::Insert(insert) => {
                box_lowered(|| self.lower_insert(logical.id, insert))?
            }
            LogicalOperator::Delete(delete) => {
                box_lowered(|| self.lower_delete(logical.id, delete))?
            }
            LogicalOperator::Update(update) => {
                box_lowered(|| self.lower_update(logical.id, update))?
            }
            LogicalOperator::CopyTo(copy) => box_lowered(|| self.lower_copy_to(copy))?,
            LogicalOperator::SetOperation(setop) => {
                box_lowered(|| self.lower_set_operation(setop))?
            }
            LogicalOperator::MaterializedCTE(cte) => {
                box_lowered(|| self.lower_materialized_cte(cte))?
            }
            LogicalOperator::RecursiveCTE(cte) => box_lowered(|| self.lower_recursive_cte(cte))?,
            LogicalOperator::CTERef(cte_ref) => box_lowered(|| Ok(self.lower_cte_ref(cte_ref)))?,
            LogicalOperator::Explain(explain) => box_lowered(|| self.lower_explain(explain))?,
            LogicalOperator::CreateTable(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreateTable(create.info.clone()),
            )),
            LogicalOperator::Alter(alter) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::Alter(alter.info.clone()),
            )),
            LogicalOperator::CreateView(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreateView(create.info.clone()),
            )),
            LogicalOperator::CreateSchema(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreateSchema(create.info.clone()),
            )),
            LogicalOperator::CreateSequence(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreateSequence(create.info.clone()),
            )),
            LogicalOperator::CreateIndex(create_index) => {
                box_lowered(|| self.lower_create_index(create_index))?
            }
            LogicalOperator::CreateRoutine(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreateRoutine(Box::new(create.info.clone())),
            )),
            LogicalOperator::CreatePropertyGraph(create) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::CreatePropertyGraph(create.info.clone()),
            )),
            LogicalOperator::Drop(drop) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::Drop(drop.info.clone()),
            )),
            LogicalOperator::DropPropertyGraph(drop) => boxed_leaf(PhysicalNodeKind::Utility(
                UtilitySpec::DropPropertyGraph(drop.info.clone()),
            )),
            LogicalOperator::RefreshPropertyGraph(refresh) => boxed_leaf(
                PhysicalNodeKind::Utility(UtilitySpec::RefreshPropertyGraph(refresh.info.clone())),
            ),
            other => box_lowered(|| self.lower_unsupported(other))?,
        };

        let runtime_filter_edge = if matches!(
            selected_implementation,
            crate::physical::PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | crate::physical::PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        ) {
            let contract = self
                .implementation_contracts
                .get(&logical.id)
                .ok_or_else(|| {
                    paro_error::internal(
                        "runtime-filter implementation lost its region-owned artifact identity",
                    )
                })?;
            let mut artifacts = contract
                .owned_artifacts
                .iter()
                .filter(|artifact| {
                    artifact.kind == crate::physical::AuxiliaryArtifactKind::RuntimeFilter
                })
                .map(|artifact| artifact.fingerprint);
            let artifact = artifacts.next().ok_or_else(|| {
                paro_error::internal(
                    "runtime-filter implementation has no region-owned runtime-filter artifact",
                )
            })?;
            if artifacts.next().is_some() {
                return Err(paro_error::internal(
                    "runtime-filter implementation owns multiple runtime-filter artifacts",
                ));
            }
            let PhysicalNodeKind::HashJoin(spec) = kind.as_mut() else {
                return Err(paro_error::internal(
                    "runtime-filter implementation selected for a non-hash join",
                ));
            };
            let [probe, build] = children.as_slice() else {
                return Err(paro_error::internal(
                    "runtime-filter hash join must have probe and build children",
                ));
            };
            let consumers = crate::physical::lineage::runtime_filter_consumers_in(
                &self.arena,
                &self.children,
                *probe,
                spec,
            );
            if consumers.is_empty() {
                return Err(paro_error::internal(
                    "runtime-filter candidate has no row-preserving rowset-scan consumer",
                ));
            }
            let expected_probe_rows = consumers.iter().fold(0_u64, |total, consumer| {
                let rows = self
                    .arena
                    .get(*consumer)
                    .and_then(|node| match &node.kind {
                        PhysicalNodeKind::RowsetScan(scan) => scan
                            .table
                            .statistics()
                            .map(|statistics| statistics.row_count)
                            .filter(|rows| *rows > 0)
                            .or_else(|| node.cardinality.map(|estimate| estimate.expected)),
                        _ => None,
                    })
                    .unwrap_or(0);
                total.saturating_add(rows)
            });
            let (condition_indices, key_types): (Vec<_>, Vec<_>) = spec
                .key_conditions
                .iter()
                .enumerate()
                .filter(|(_, condition)| condition.comparison == JoinComparisonType::Equal)
                .map(|(index, condition)| (index, condition.right.return_type()))
                .unzip();
            spec.runtime_filter = Some(HashJoinRuntimeFilterSpec {
                artifact,
                wait_policy: RuntimeFilterWaitPolicy::WaitComplete,
                condition_indices: condition_indices.into_boxed_slice(),
                resource: crate::physical::RuntimeFilterResourceContract::for_probe_rows(
                    &key_types,
                    u16::try_from(self.ctx.max_threads).unwrap_or(u16::MAX),
                    expected_probe_rows,
                )?,
            });
            Some((*build, consumers, artifact))
        } else {
            None
        };

        let child_outputs = children
            .iter()
            .map(|child| {
                &self
                    .arena
                    .get(*child)
                    .expect("generated child must remain in the physical arena")
                    .output
            })
            .collect::<Vec<_>>();
        let output = physical_output_row_type_for_kind(logical, kind.as_ref(), &child_outputs)?;
        let display_name = kind.name().to_string();
        let label = OperatorLabel::new(logical.id, display_name);
        let id = self.push_node(
            *kind,
            output,
            children,
            label,
            logical.stats.estimated_cardinality,
        );
        if let Some(contract) = self.implementation_contracts.get(&logical.id) {
            let properties = self
                .properties
                .get_mut(id)
                .expect("new physical node must have a property contract");
            properties.required_from_parent = contract.required.clone();
            properties.provided = contract.provided.clone();
            properties.cumulative_cost = contract.cost;
            properties.grant_contract = contract.grant;
            properties.region_owner = contract.region_owner;
            properties.owned_artifacts = contract.owned_artifacts.clone();
            properties.origin = contract.origin;
            properties.winner_goal = contract.goal_fingerprint;
        }
        if let Some((producer, consumers, artifact)) = runtime_filter_edge {
            for consumer in consumers {
                let edge = self.edges.push(
                    producer,
                    consumer,
                    PhysicalEdgeKind::RuntimeFilter(artifact),
                );
                let properties = self.properties.get_mut(consumer).ok_or_else(|| {
                    paro_error::internal("runtime-filter consumer has no property contract")
                })?;
                let mut dependencies = properties.auxiliary_dependencies.to_vec();
                dependencies.push(edge.0);
                properties.auxiliary_dependencies = dependencies.into_boxed_slice();
            }
        }
        self.attach_mutation_barrier(logical, id)
    }

    fn attach_mutation_barrier(
        &mut self,
        logical: &PreparedNode,
        mut child: PhysicalPlanNodeId,
    ) -> Result<PhysicalPlanNodeId> {
        let Some(barrier) = self.mutation_barriers.get(&logical.id).cloned() else {
            return Ok(child);
        };
        let child_node = self
            .arena
            .get(child)
            .ok_or_else(|| paro_error::internal("mutation barrier lost its input"))?;
        let output = child_node.output.clone();
        let cardinality = child_node.cardinality;
        child = self.push_node(
            PhysicalNodeKind::MutationInputSpool(MutationInputSpoolSpec {
                barrier: barrier.barrier,
                targets: barrier.targets,
                snapshot: barrier.snapshot,
            }),
            output,
            vec![child],
            OperatorLabel::new(logical.id, "MUTATION_INPUT"),
            cardinality,
        );
        let properties = self
            .properties
            .get_mut(child)
            .expect("new physical mutation node");
        properties.required_from_parent = barrier.implementation.required;
        properties.provided = barrier.implementation.provided;
        properties.cumulative_cost = barrier.implementation.cost;
        properties.grant_contract = barrier.implementation.grant;
        properties.origin = barrier.implementation.origin;

        Ok(child)
    }

    fn push_node(
        &mut self,
        kind: PhysicalNodeKind,
        output: RowType,
        children: Vec<PhysicalPlanNodeId>,
        label: OperatorLabel,
        cardinality: Option<paro_planner::logical::plan::CardinalityEstimate>,
    ) -> PhysicalPlanNodeId {
        use crate::physical::cost::{CompactRange, PhysicalCost, ScoreSummary};
        use crate::physical::identity::Fingerprint;
        use crate::physical::properties::{
            PhysicalCharacteristics, PhysicalGrantContract, PhysicalNodeProperties, PlanOrigin,
        };
        use crate::physical::requirements::{
            ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
            ProvidedPartitioning, ProvidedProperties, ProvidedReplayability,
            ProvidedRepresentation, RequiredProperties, ResultGuarantee,
        };

        let characteristics = PhysicalCharacteristics {
            blocking: matches!(
                kind,
                PhysicalNodeKind::Sort(_)
                    | PhysicalNodeKind::MutationInputSpool(_)
                    | PhysicalNodeKind::TopN(_)
                    | PhysicalNodeKind::HashJoin(_)
                    | PhysicalNodeKind::CrossProduct(_)
                    | PhysicalNodeKind::Aggregate(_)
                    | PhysicalNodeKind::Window(_)
                    | PhysicalNodeKind::MaterializedCte(_)
                    | PhysicalNodeKind::RecursiveCte(_)
            ),
            spillable: matches!(
                &kind,
                PhysicalNodeKind::TopN(_) | PhysicalNodeKind::Window(_)
            ) || matches!(
                &kind,
                PhysicalNodeKind::Aggregate(spec)
                    if spec.spill_policy != SpillExecutionPolicy::InMemory
            ) || matches!(
                &kind,
                PhysicalNodeKind::CrossProduct(spec)
                    if spec.spill_policy != SpillExecutionPolicy::InMemory
            ) || matches!(
                &kind,
                PhysicalNodeKind::HashJoin(spec)
                    if spec.spill_policy != SpillExecutionPolicy::InMemory
            ) || matches!(
                &kind,
                PhysicalNodeKind::Sort(spec)
                    if spec.spill_policy != SpillExecutionPolicy::InMemory
            ) || matches!(
                &kind,
                PhysicalNodeKind::MaterializedCte(spec)
                    if spec.spill_policy != SpillExecutionPolicy::InMemory
            ),
            parallel: self.ctx.max_threads > 1,
            supports_early_stop: matches!(
                kind,
                PhysicalNodeKind::Limit(_)
                    | PhysicalNodeKind::TopN(_)
                    | PhysicalNodeKind::VectorSearch(_)
                    | PhysicalNodeKind::SparseVectorSearch(_)
                    | PhysicalNodeKind::FullTextSearch(_)
            ),
        };
        let expected = cardinality
            .map(|estimate| estimate.expected as f64)
            .unwrap_or(1.0)
            .max(0.0);
        let range = CompactRange::point(expected).unwrap_or(CompactRange::ZERO);
        let cumulative_cost = PhysicalCost {
            score: ScoreSummary {
                range,
                risk_adjusted: expected,
            },
            work_latency: range,
            critical_path: range,
            ..PhysicalCost::ZERO
        };
        let children = self.children.pack(children);
        let id = self.arena.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output,
            cardinality,
            kind,
            children,
            label,
        });
        self.properties.insert(
            id,
            PhysicalNodeProperties {
                required_from_parent: RequiredProperties::default(),
                provided: ProvidedProperties {
                    ordering: ProvidedOrdering::Unordered,
                    partitioning: ProvidedPartitioning::Singleton,
                    materialization: ProvidedMaterialization::default(),
                    mutation_safety: ProvidedMutationSafety::NotApplicable,
                    representation: ProvidedRepresentation::Flat,
                    replayability: ProvidedReplayability::OnePass,
                    result_guarantee: ResultGuarantee::Exact,
                },
                characteristics,
                output_estimate: cardinality,
                cumulative_cost,
                grant_contract: PhysicalGrantContract::Invariant,
                auxiliary_dependencies: Box::new([]),
                region_owner: None,
                owned_artifacts: Box::new([]),
                origin: PlanOrigin::StatementLowering,
                winner_goal: Fingerprint(id.index() as u128),
            },
        );
        id
    }
}

fn populate_plan_dependencies(
    plan: &mut PhysicalPlan,
    template: &crate::physical::PlanDependencies,
) {
    use crate::physical::identity::{Fingerprint, StableFingerprintBuilder};
    use paro_catalog::entry::CatalogEntry;

    fn domain_fingerprint(domain: u64, value: u64) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_u64(domain);
        builder.write_u64(value);
        builder.finish()
    }

    fn register_table(
        dependencies: &mut crate::physical::PlanDependencies,
        table: &paro_catalog::entry::TableCatalogEntry,
    ) {
        let object = domain_fingerprint(1, table.object_id().raw());
        dependencies
            .catalog_versions
            .insert(object, table.timestamp());
        // Until the catalog publishes an independently moving statistics
        // compatibility epoch, table timestamp is the conservative epoch. It
        // may invalidate more often, but never lets a cached estimate outlive
        // its source artifact.
        dependencies
            .statistics_compatibility
            .insert(object, table.timestamp());
    }

    fn register_search_state(
        dependencies: &mut crate::physical::PlanDependencies,
        table: &paro_catalog::entry::TableCatalogEntry,
    ) {
        let object = domain_fingerprint(1, table.object_id().raw());
        dependencies.search_planning_signatures.insert(
            object,
            table
                .storage
                .as_ref()
                .map_or(0, |storage| storage.search_planning_signature()),
        );
    }

    fn register_search(
        dependencies: &mut crate::physical::PlanDependencies,
        token: &paro_storage::search::CapabilityToken,
    ) {
        dependencies.provider_capabilities.insert(
            domain_fingerprint(2, token.definition_id),
            token.root_version,
        );
        dependencies.search_index_generations.insert(
            domain_fingerprint(3, token.definition_id),
            token.generation_id,
        );
    }

    fn register_routine(
        dependencies: &mut crate::physical::PlanDependencies,
        routine: &ExternalRoutineDescriptor,
    ) {
        if let RoutineCallIdentity::Catalog {
            routine_id,
            generation,
        } = routine.identity
        {
            dependencies
                .routine_artifacts
                .insert(domain_fingerprint(4, routine_id.raw()), generation);
            dependencies
                .external_runtime_profiles
                .insert(domain_fingerprint(5, routine_id.raw()), 1);
        }
    }

    plan.dependencies = template.clone();
    let dependencies = &mut plan.dependencies;

    for node in plan.nodes.iter() {
        match &node.kind {
            PhysicalNodeKind::RowsetScan(spec) => register_table(dependencies, &spec.table),
            PhysicalNodeKind::VectorSearch(spec) => {
                register_table(dependencies, &spec.table);
                register_search_state(dependencies, &spec.table);
                register_search(dependencies, &spec.capability_token);
            }
            PhysicalNodeKind::SparseVectorSearch(spec) => {
                register_table(dependencies, &spec.table);
                register_search_state(dependencies, &spec.table);
                register_search(dependencies, &spec.capability_token);
            }
            PhysicalNodeKind::FullTextSearch(spec) => {
                register_table(dependencies, &spec.table);
                register_search_state(dependencies, &spec.table);
                register_search(dependencies, &spec.capability_token);
            }
            PhysicalNodeKind::AdaptiveSearch(spec) => {
                register_table(dependencies, &spec.table);
                register_search_state(dependencies, &spec.table);
                match spec.selected.as_ref() {
                    SearchSourceSpec::Vector(source) => {
                        register_search(dependencies, &source.capability_token)
                    }
                    SearchSourceSpec::Sparse(source) => {
                        register_search(dependencies, &source.capability_token)
                    }
                    SearchSourceSpec::FullText(source) => {
                        register_search(dependencies, &source.capability_token)
                    }
                }
            }
            PhysicalNodeKind::Insert(spec) => register_table(dependencies, &spec.table),
            PhysicalNodeKind::Update(spec) => register_table(dependencies, &spec.table),
            PhysicalNodeKind::Delete(spec) => register_table(dependencies, &spec.table),
            PhysicalNodeKind::ExternalProject(spec) => {
                for routine in &spec.routines {
                    register_routine(dependencies, routine);
                }
            }
            PhysicalNodeKind::ExternalTable(spec) => register_routine(dependencies, &spec.routine),
            _ => {}
        }
    }
    if plan.properties.iter().any(|(_, properties)| {
        matches!(
            properties.provided.result_guarantee,
            crate::physical::requirements::ResultGuarantee::ApproximateAllowed(_)
        )
    }) {
        dependencies.quality_policy_revision = Some(domain_fingerprint(6, 1));
    }
}

#[cfg(test)]
mod tests;

pub(crate) mod input;
