// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Arena physical-plan generation from logical plans.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
    ExpressionIterator, ReferenceExpression,
};
use paro_planner::operator::join::{
    AntiJoinMode, ComparisonJoin, CrossProduct, Join, JoinComparisonType, JoinCondition, JoinType,
    MarkJoinSemantics,
};
use paro_planner::operator::{
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
use paro_planner::plan::LogicalPlan;
use paro_storage::search::{SearchIntent, SearchRequestMode};

use super::children::PlanChildrenArena;
use super::ids::PhysicalPlanNodeId;
use super::node::{OperatorLabel, PhysicalPlanNode};
use super::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use super::properties::PlanPropertyMap;
use super::row_type::RowType;
use super::specs::{
    AdaptiveSearchSpec, AggregateSpec, BuildTimeIntegerJoinIndexSpec, ClassicIeJoinSpec,
    CopyToFileSpec, CreateIndexUtilitySpec, CrossProductSpec, CteScanSpec, DeleteSpec,
    DelimJoinSideSpec, DelimJoinSpec, DelimScanSpec, DelimScanTarget, DummyScanSpec,
    EmptyResultSpec, ExternalProjectSpec, ExternalTableSpec, FilterSpec, FullTextSearchSpec,
    GraphExpandSpec, GraphProjectSpec, GraphRowFetchMapping, GraphScanSpec, GraphShortestPathSpec,
    HashJoinSpec, HashReductionCascadeSpec, HashReductionExtremaChannelSpec,
    HashReductionGroupedExtremaSpec, HashReductionPredicateSpec, HashReductionSourcePredicateSpec,
    HashReductionStepSpec, InsertSpec, LimitSpec, MaterializedCteSpec, NestedLoopJoinSpec,
    PartitionAggregateDomain, PartitionAggregateWindowSpec, PerfectHashAggregatePlan,
    PhysicalNodeKind, PostAggregateReductionSpec, ProjectSpec, RecursiveCteSpec,
    RelationalRowFetchMapping, RowFetchProjectionSpec, RowFetchSpec, RowsetColumnProjection,
    RowsetColumnValueProjection, RowsetScanAccessPolicy, RowsetScanSpec, SearchSourceSpec,
    SortRangeJoinSpec, SortSpec, SparseVectorSearchSpec, TableFunctionScanSpec, TopNSpec,
    UnsupportedSpec, UpdateSpec, UtilitySpec, ValuesSpec, VectorSearchSpec, WindowSpec,
};

pub(crate) mod predicate_builder;

mod aggregate;
mod dml;
mod external;
mod graph;
mod helpers;
mod inequality_join_gate;
mod join;
mod misc;
mod row_fetch;
mod scan;
mod set;

use helpers::*;
use inequality_join_gate::*;

#[derive(Debug, Clone)]
pub struct PlanBuildContext {
    pub force_external: bool,
    pub rowset_scan_pushdown: bool,
    /// Query-scoped memory available to physical operators. Zero means the
    /// planner has no budget information and must use conservative defaults.
    pub max_memory: usize,
    pub max_threads: usize,
    pub scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

impl Default for PlanBuildContext {
    fn default() -> Self {
        Self {
            force_external: false,
            rowset_scan_pushdown: true,
            max_memory: 0,
            max_threads: 1,
            scan_access_cost: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct PhysicalPlanGenerator {
    pub ctx: PlanBuildContext,
    pub arena: PhysicalPlanNodeArena,
    pub children: PlanChildrenArena,
}

impl PhysicalPlanGenerator {
    pub fn new(ctx: PlanBuildContext) -> Self {
        Self {
            ctx,
            arena: PhysicalPlanNodeArena::default(),
            children: PlanChildrenArena::default(),
        }
    }

    pub fn generate(&mut self, logical: &LogicalPlan) -> Result<PhysicalPlan> {
        self.arena = PhysicalPlanNodeArena::default();
        self.children = PlanChildrenArena::default();
        let root = self.generate_node(logical)?;
        let mut plan = PhysicalPlan::new(
            root,
            mem::take(&mut self.arena),
            mem::take(&mut self.children),
            PlanPropertyMap::default(),
        );
        super::rewrite::rewrite_projection_chains(&mut plan);
        Ok(plan)
    }

    fn generate_node(&mut self, logical: &LogicalPlan) -> Result<PhysicalPlanNodeId> {
        let (kind, children) = match &logical.operator {
            LogicalOperator::Get(get) => self.lower_get(get)?,
            LogicalOperator::DummyScan => (PhysicalNodeKind::DummyScan(DummyScanSpec), Vec::new()),
            LogicalOperator::ExpressionGet(values) => self.lower_values(values),
            LogicalOperator::EmptyResult(empty) => self.lower_empty_result(empty)?,
            LogicalOperator::Filter(filter) => {
                self.lower_filter(filter, logical.stats.estimated_cardinality)?
            }
            LogicalOperator::Projection(project) => {
                if let LogicalOperator::RowFetch(fetch) = &project.child.operator {
                    self.lower_row_fetch(fetch, Some(project))?
                } else if is_graph_chain(project.child.as_ref()) {
                    self.lower_graph_project(project)?
                } else {
                    self.lower_project(project)?
                }
            }
            LogicalOperator::RowFetch(fetch) => self.lower_row_fetch(fetch, None)?,
            LogicalOperator::Limit(limit) => self.lower_limit(limit)?,
            LogicalOperator::Order(order) => self.lower_order(order)?,
            LogicalOperator::TopN(topn) => self.lower_topn(topn)?,
            LogicalOperator::SearchScan(scan) => self.lower_search_scan(scan)?,
            LogicalOperator::Aggregate(aggregate) => self.lower_aggregate(aggregate)?,
            LogicalOperator::Distinct(distinct) => self.lower_distinct(distinct)?,
            LogicalOperator::Join(join) => {
                self.lower_join(join, logical.stats.estimated_cardinality)?
            }
            LogicalOperator::Window(window) => self.lower_window(window)?,
            LogicalOperator::TableFunctionGet(table_function) => {
                self.lower_table_function(table_function)?
            }
            LogicalOperator::DelimGet(delim_get) => self.lower_delim_get(delim_get),
            LogicalOperator::FullTextFilterScan(scan) => self.lower_fulltext_filter_scan(scan)?,
            LogicalOperator::ExternalProject(project) => self.lower_external_project(project)?,
            LogicalOperator::ExternalTable(table) => self.lower_external_table(table)?,
            LogicalOperator::GraphScan(scan) => self.lower_graph_scan(scan),
            LogicalOperator::GraphExpand(expand) => self.lower_graph_expand(expand)?,
            LogicalOperator::Insert(insert) => self.lower_insert(insert)?,
            LogicalOperator::Delete(delete) => self.lower_delete(delete)?,
            LogicalOperator::Update(update) => self.lower_update(update)?,
            LogicalOperator::CopyTo(copy) => self.lower_copy_to(copy)?,
            LogicalOperator::SetOperation(setop) => self.lower_set_operation(setop)?,
            LogicalOperator::MaterializedCTE(cte) => self.lower_materialized_cte(cte)?,
            LogicalOperator::RecursiveCTE(cte) => self.lower_recursive_cte(cte)?,
            LogicalOperator::CTERef(cte_ref) => self.lower_cte_ref(cte_ref),
            LogicalOperator::Explain(explain) => self.lower_explain(explain)?,
            LogicalOperator::CreateTable(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreateTable(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::Alter(alter) => (
                PhysicalNodeKind::Utility(UtilitySpec::Alter(alter.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::CreateView(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreateView(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::CreateSchema(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreateSchema(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::CreateSequence(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreateSequence(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::CreateIndex(create_index) => self.lower_create_index(create_index)?,
            LogicalOperator::CreateRoutine(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreateRoutine(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::CreatePropertyGraph(create) => (
                PhysicalNodeKind::Utility(UtilitySpec::CreatePropertyGraph(create.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::Drop(drop) => (
                PhysicalNodeKind::Utility(UtilitySpec::Drop(drop.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::DropPropertyGraph(drop) => (
                PhysicalNodeKind::Utility(UtilitySpec::DropPropertyGraph(drop.info.clone())),
                Vec::new(),
            ),
            LogicalOperator::RefreshPropertyGraph(refresh) => (
                PhysicalNodeKind::Utility(UtilitySpec::RefreshPropertyGraph(refresh.info.clone())),
                Vec::new(),
            ),
            other => self.lower_unsupported(other)?,
        };

        let output = physical_output_row_type_for_kind(logical, &kind)?;
        let display_name = match &kind {
            PhysicalNodeKind::Unsupported(spec) => {
                format!("UNSUPPORTED[{}]: {}", spec.logical_name, spec.reason)
            }
            _ => kind.name().to_string(),
        };
        let label = OperatorLabel::new(logical.id, display_name);
        Ok(self.push_node(
            kind,
            output,
            children,
            label,
            logical.stats.estimated_cardinality,
        ))
    }

    fn push_node(
        &mut self,
        kind: PhysicalNodeKind,
        output: RowType,
        children: Vec<PhysicalPlanNodeId>,
        label: OperatorLabel,
        cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> PhysicalPlanNodeId {
        let children = self.children.pack(children);
        self.arena.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output,
            cardinality,
            kind,
            children,
            label,
        })
    }
}

#[cfg(test)]
mod partition_aggregate_tests;
#[cfg(test)]
mod post_reduction_tests;
#[cfg(test)]
mod row_fetch_tests;
#[cfg(test)]
mod tests;
