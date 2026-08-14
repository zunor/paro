// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Operator
//!
//! The core enum representing nodes in the logical query plan.

use std::ops::ControlFlow;

use paro_common::{error::Result, types::LogicalType};

use crate::plan::LogicalPlan;

use super::{
    Aggregate, Alter, CTERef, ColumnBinding, CopyTo, CreateIndex, CreatePropertyGraph,
    CreateRoutine, CreateSchema, CreateSequence, CreateTable, CreateView, Delete, DelimGet,
    DependentJoin, Distinct, Drop, DropPropertyGraph, EmptyResult, Explain, ExpressionGet, Filter,
    FullTextFilterScan, Get, GraphExpand, GraphMatch, GraphScan, Insert, Join, Limit,
    LogicalExternalProject, LogicalExternalTable, LogicalOperatorType, MaterializedCTE, Order,
    Projection, ProjectionMap, RecursiveCTE, RefreshPropertyGraph, RowFetch, SearchScan, SetOpType,
    SetOperation, TableFunctionGet, TopN, Update, Window,
};

/// The LogicalOperator represents a node in the logical query plan.
#[derive(Debug)]
pub enum LogicalOperator {
    /// Reading data from a table
    Get(Get),
    /// Check data against a condition
    Filter(Filter),
    /// Project columns/expressions
    Projection(Projection),
    /// Materialize base-table columns from stable rowids carried by the child.
    RowFetch(RowFetch),
    /// Row-preserving external routine layer
    ExternalProject(LogicalExternalProject),
    /// Relation-expanding external routine source
    ExternalTable(LogicalExternalTable),
    /// Top N / Limit / Offset
    Limit(Limit),
    /// Order By
    Order(Order),
    /// TopN (optimized ORDER BY + LIMIT)
    TopN(TopN),
    /// Create Table
    CreateTable(CreateTable),
    /// Create Routine
    CreateRoutine(CreateRoutine),
    /// Alter existing catalog entry
    Alter(Alter),
    /// Create Sequence
    CreateSequence(CreateSequence),
    /// Create Schema
    CreateSchema(CreateSchema),
    /// Create Index
    CreateIndex(CreateIndex),
    /// Create View
    CreateView(CreateView),
    /// Drop Table/Schema/Index/View
    Drop(Drop),
    /// Create Property Graph
    CreatePropertyGraph(CreatePropertyGraph),
    /// Drop Property Graph
    DropPropertyGraph(DropPropertyGraph),
    /// Refresh Property Graph
    RefreshPropertyGraph(RefreshPropertyGraph),
    Aggregate(Aggregate),
    Insert(Insert),
    /// Delete rows from a table
    Delete(Delete),
    /// Update rows in a table
    Update(Update),
    ExpressionGet(ExpressionGet),
    /// Join operations (comparison, any, cross product)
    Join(Join),
    /// Duplicate-eliminated scan placeholder owned by a delim/dependent join.
    DelimGet(DelimGet),
    /// Dependent join (for correlated subqueries, temporary during planning)
    DependentJoin(DependentJoin),
    /// Set operations (UNION, INTERSECT, EXCEPT)
    SetOperation(SetOperation),
    /// DISTINCT operation
    Distinct(Distinct),
    /// Window function operation
    Window(Window),
    /// EXPLAIN/EXPLAIN ANALYZE
    Explain(Explain),
    /// Empty result preserving child schema.
    EmptyResult(EmptyResult),
    /// Materialized CTE definition
    MaterializedCTE(MaterializedCTE),
    /// Recursive CTE producer
    RecursiveCTE(RecursiveCTE),
    /// CTE reference
    CTERef(CTERef),
    /// Table function scan
    TableFunctionGet(TableFunctionGet),
    /// Search path scan replacing TopN/Projection/Filter/Get subgraphs.
    SearchScan(SearchScan),
    /// Full-text filter scan replacing Filter/Get subgraphs.
    FullTextFilterScan(FullTextFilterScan),
    /// COPY TO file/stdout
    CopyTo(CopyTo),
    /// Graph pattern match (undecomposed GRAPH_TABLE)
    GraphMatch(GraphMatch),
    /// Graph vertex scan
    GraphScan(GraphScan),
    /// Graph edge expansion
    GraphExpand(GraphExpand),

    /// A dummy scan that produces one row (used for SELECT 1)
    DummyScan,
}

impl LogicalOperator {
    pub fn output_names(&self) -> Vec<String> {
        fn project_names(child_names: &[String], projection_map: &ProjectionMap) -> Vec<String> {
            match projection_map.as_columns() {
                None => child_names.to_vec(),
                Some(indices) => indices
                    .iter()
                    .filter_map(|&idx| child_names.get(idx).cloned())
                    .collect(),
            }
        }

        match self {
            LogicalOperator::Get(op) => op.names.clone(),
            LogicalOperator::Filter(op) => {
                let child_names = op.child.output_names();
                project_names(&child_names, &op.projection_map)
            }
            LogicalOperator::Projection(op) => op.output_names.clone(),
            LogicalOperator::RowFetch(op) => op.output_names(),
            LogicalOperator::ExternalProject(op) => op.output_names.clone(),
            LogicalOperator::ExternalTable(op) => op.output_columns.clone(),
            LogicalOperator::Limit(op) => op.child.output_names(),
            LogicalOperator::Order(op) => {
                let child_names = op.child.output_names();
                project_names(&child_names, &op.projection_map)
            }
            LogicalOperator::TopN(op) => op.child.output_names(),
            LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::DummyScan => vec![],
            LogicalOperator::Aggregate(op) => {
                let mut names = Vec::with_capacity(
                    op.groups.len() + op.aggregates.len() + op.grouping_functions.len(),
                );
                for (idx, group) in op.groups.iter().enumerate() {
                    names.push(expression_output_name(group, idx, "group"));
                }
                for (idx, aggregate) in op.aggregates.iter().enumerate() {
                    names.push(expression_output_name(aggregate, idx, "agg"));
                }
                for idx in 0..op.grouping_functions.len() {
                    names.push(format!("grouping_{}", idx + 1));
                }
                names
            }
            LogicalOperator::Insert(_)
            | LogicalOperator::Delete(_)
            | LogicalOperator::Update(_) => {
                vec!["count".to_string()]
            }
            LogicalOperator::ExpressionGet(op) => op.names.clone(),
            LogicalOperator::Join(j) => {
                let left_names = match j {
                    Join::Comparison(cj) => {
                        project_names(&cj.left.output_names(), &cj.left_projection_map)
                    }
                    Join::Any(aj) => {
                        project_names(&aj.left.output_names(), &aj.left_projection_map)
                    }
                    Join::Cross(cp) => cp.left.output_names(),
                };
                let right_names = match j {
                    Join::Comparison(cj) => {
                        project_names(&cj.right.output_names(), &cj.right_projection_map)
                    }
                    Join::Any(aj) => {
                        project_names(&aj.right.output_names(), &aj.right_projection_map)
                    }
                    Join::Cross(cp) => cp.right.output_names(),
                };

                match j.join_type() {
                    super::join::JoinType::Semi | super::join::JoinType::Anti => left_names,
                    super::join::JoinType::RightSemi | super::join::JoinType::RightAnti => {
                        right_names
                    }
                    super::join::JoinType::Mark => {
                        let mut names = left_names;
                        names.push("mark".to_string());
                        names
                    }
                    _ => {
                        let mut names = left_names;
                        names.extend(right_names);
                        names
                    }
                }
            }
            LogicalOperator::DelimGet(op) => (0..op.chunk_types.len())
                .map(|idx| format!("delim_{}", idx + 1))
                .collect(),
            LogicalOperator::DependentJoin(op) => op.output_names(),
            LogicalOperator::SetOperation(op) => op.left().output_names(),
            LogicalOperator::Distinct(op) => op.child.output_names(),
            LogicalOperator::Window(op) => {
                let mut names = op.child.output_names();
                for (idx, expr) in op.expressions.iter().enumerate() {
                    names.push(window_output_name(expr, idx));
                }
                names
            }
            LogicalOperator::Explain(_) => vec!["QUERY PLAN".to_string()],
            LogicalOperator::EmptyResult(op) => op.child.output_names(),
            LogicalOperator::MaterializedCTE(op) => op.child.output_names(),
            LogicalOperator::RecursiveCTE(op) => op.column_names.clone(),
            LogicalOperator::CTERef(op) => op.column_names.clone(),
            LogicalOperator::TableFunctionGet(op) => op.get_names(),
            LogicalOperator::SearchScan(op) => op.output_names.clone(),
            LogicalOperator::FullTextFilterScan(op) => op.get.names.clone(),
            LogicalOperator::CopyTo(copy) => copy.names.clone(),
            LogicalOperator::GraphMatch(gm) => {
                gm.columns.iter().map(|col| col.alias.clone()).collect()
            }
            LogicalOperator::GraphScan(_gs) => {
                vec!["local_vertex_id".to_string(), "rowid".to_string()]
            }
            LogicalOperator::GraphExpand(op) => {
                let mut names = op.child.output_names();
                names.extend([
                    "edge_rowid".to_string(),
                    "target_local_id".to_string(),
                    "target_rowid".to_string(),
                ]);
                if op.has_path_functions {
                    names.extend([
                        "path_length".to_string(),
                        "path_vertices".to_string(),
                        "path_edges".to_string(),
                    ]);
                }
                names
            }
        }
    }

    pub fn op_type(&self) -> LogicalOperatorType {
        match self {
            LogicalOperator::Get(_) => LogicalOperatorType::Get,
            LogicalOperator::Filter(_) => LogicalOperatorType::Filter,
            LogicalOperator::Projection(_) => LogicalOperatorType::Projection,
            LogicalOperator::RowFetch(_) => LogicalOperatorType::RowFetch,
            LogicalOperator::ExternalProject(_) => LogicalOperatorType::ExternalProject,
            LogicalOperator::ExternalTable(_) => LogicalOperatorType::ExternalTable,
            LogicalOperator::Limit(_) => LogicalOperatorType::Limit,
            LogicalOperator::Order(_) => LogicalOperatorType::Order,
            LogicalOperator::TopN(_) => LogicalOperatorType::TopN,
            LogicalOperator::CreateTable(_) => LogicalOperatorType::CreateTable,
            LogicalOperator::CreateRoutine(_) => LogicalOperatorType::CreateRoutine,
            LogicalOperator::Alter(_) => LogicalOperatorType::Alter,
            LogicalOperator::CreateSequence(_) => LogicalOperatorType::CreateSequence,
            LogicalOperator::CreateSchema(_) => LogicalOperatorType::CreateSchema,
            LogicalOperator::CreateIndex(_) => LogicalOperatorType::CreateIndex,
            LogicalOperator::CreateView(_) => LogicalOperatorType::CreateView,
            LogicalOperator::Drop(_) => LogicalOperatorType::Drop,
            LogicalOperator::CreatePropertyGraph(_) => LogicalOperatorType::CreatePropertyGraph,
            LogicalOperator::DropPropertyGraph(_) => LogicalOperatorType::DropPropertyGraph,
            LogicalOperator::RefreshPropertyGraph(_) => LogicalOperatorType::RefreshPropertyGraph,
            LogicalOperator::Aggregate(_) => LogicalOperatorType::Aggregate,
            LogicalOperator::Insert(_) => LogicalOperatorType::Insert,
            LogicalOperator::Delete(_) => LogicalOperatorType::Delete,
            LogicalOperator::Update(_) => LogicalOperatorType::Update,
            LogicalOperator::ExpressionGet(_) => LogicalOperatorType::Get,
            LogicalOperator::Join(j) => match j {
                Join::Comparison(_) => LogicalOperatorType::ComparisonJoin,
                Join::Any(_) => LogicalOperatorType::AnyJoin,
                Join::Cross(_) => LogicalOperatorType::CrossProduct,
            },
            LogicalOperator::DelimGet(_) => LogicalOperatorType::DelimGet,
            LogicalOperator::DependentJoin(_) => LogicalOperatorType::DependentJoin,
            LogicalOperator::SetOperation(s) => match s.setop_type {
                SetOpType::Union => LogicalOperatorType::LogicalUnion,
                SetOpType::Intersect => LogicalOperatorType::LogicalIntersect,
                SetOpType::Except => LogicalOperatorType::LogicalExcept,
            },
            LogicalOperator::Distinct(_) => LogicalOperatorType::Distinct,
            LogicalOperator::Window(_) => LogicalOperatorType::Window,
            LogicalOperator::Explain(_) => LogicalOperatorType::Explain,
            LogicalOperator::EmptyResult(_) => LogicalOperatorType::EmptyResult,
            LogicalOperator::MaterializedCTE(_) => LogicalOperatorType::MaterializedCTE,
            LogicalOperator::RecursiveCTE(_) => LogicalOperatorType::RecursiveCTE,
            LogicalOperator::CTERef(_) => LogicalOperatorType::CTERef,
            LogicalOperator::TableFunctionGet(_) => LogicalOperatorType::TableFunctionGet,
            LogicalOperator::SearchScan(_) => LogicalOperatorType::SearchScan,
            LogicalOperator::FullTextFilterScan(_) => LogicalOperatorType::FullTextFilterScan,
            LogicalOperator::CopyTo(_) => LogicalOperatorType::LogicalCopy,
            LogicalOperator::GraphMatch(_) => LogicalOperatorType::GraphMatch,
            LogicalOperator::GraphScan(_) => LogicalOperatorType::GraphScan,
            LogicalOperator::GraphExpand(_) => LogicalOperatorType::GraphExpand,
            LogicalOperator::DummyScan => LogicalOperatorType::Get,
        }
    }

    /// Get the logical types of the output of this operator.
    pub fn types(&self) -> Vec<LogicalType> {
        match self {
            LogicalOperator::Get(op) => op.returned_types.clone(),
            LogicalOperator::Filter(op) => {
                let child_types = op.child.types();
                match op.projection_map.as_columns() {
                    None => child_types,
                    Some(indices) => indices
                        .iter()
                        .filter_map(|&idx| child_types.get(idx).cloned())
                        .collect(),
                }
            }
            LogicalOperator::Projection(op) => op.returned_types.clone(),
            LogicalOperator::RowFetch(op) => op.output_types(),
            LogicalOperator::ExternalProject(op) => op.returned_types.clone(),
            LogicalOperator::ExternalTable(op) => op.returned_types.clone(),
            LogicalOperator::Limit(op) => op.child.types(),
            LogicalOperator::Order(op) => {
                let child_types = op.child.types();
                match op.projection_map.as_columns() {
                    None => child_types,
                    Some(indices) => indices
                        .iter()
                        .filter_map(|&idx| child_types.get(idx).cloned())
                        .collect(),
                }
            }
            LogicalOperator::TopN(op) => op.child.types(),
            LogicalOperator::CreateTable(_) => vec![],
            LogicalOperator::CreateRoutine(_) => vec![],
            LogicalOperator::Alter(_) => vec![],
            LogicalOperator::CreateSequence(_) => vec![],
            LogicalOperator::CreateSchema(_) => vec![],
            LogicalOperator::CreateIndex(op) => op.get_types(),
            LogicalOperator::CreateView(_) => vec![],
            LogicalOperator::Drop(_) => vec![],
            LogicalOperator::CreatePropertyGraph(_) => vec![],
            LogicalOperator::DropPropertyGraph(_) => vec![],
            LogicalOperator::RefreshPropertyGraph(_) => vec![],
            LogicalOperator::Aggregate(op) => op.returned_types.clone(),
            LogicalOperator::Insert(_) => vec![LogicalType::BigInt],
            LogicalOperator::Delete(op) => op.get_types(),
            LogicalOperator::Update(op) => op.get_types(),
            LogicalOperator::ExpressionGet(op) => op.types.clone(),
            LogicalOperator::DelimGet(op) => op.get_types(),
            LogicalOperator::Join(j) => j.get_types(),
            LogicalOperator::DependentJoin(d) => d.get_types(),
            LogicalOperator::SetOperation(s) => s.get_types(),
            LogicalOperator::Distinct(d) => d.get_types(),
            LogicalOperator::Window(w) => w.get_types(),
            LogicalOperator::Explain(_) => vec![LogicalType::Varchar],
            LogicalOperator::EmptyResult(op) => op.get_types(),
            LogicalOperator::MaterializedCTE(c) => c.get_types(),
            LogicalOperator::RecursiveCTE(c) => c.get_types(),
            LogicalOperator::CTERef(c) => c.get_types(),
            LogicalOperator::TableFunctionGet(t) => t.get_types(),
            LogicalOperator::SearchScan(search) => search.get_types(),
            LogicalOperator::FullTextFilterScan(scan) => scan.get_types(),
            LogicalOperator::CopyTo(copy) => copy.types.clone(),
            LogicalOperator::GraphMatch(gm) => gm.output_types.clone(),
            LogicalOperator::GraphScan(gs) => gs.output_types.clone(),
            LogicalOperator::GraphExpand(op) => op.output_types(),
            LogicalOperator::DummyScan => vec![],
        }
    }

    /// Get the children of this operator.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalOperator::Get(_) => vec![],
            LogicalOperator::Filter(op) => vec![op.child.as_ref()],
            LogicalOperator::Projection(op) => vec![op.child.as_ref()],
            LogicalOperator::RowFetch(op) => vec![op.child.as_ref()],
            LogicalOperator::ExternalProject(op) => vec![op.child.as_ref()],
            LogicalOperator::ExternalTable(op) => op.child.as_deref().into_iter().collect(),
            LogicalOperator::Limit(op) => vec![op.child.as_ref()],
            LogicalOperator::Order(op) => vec![op.child.as_ref()],
            LogicalOperator::TopN(op) => vec![op.child.as_ref()],
            LogicalOperator::CreateTable(_) => vec![],
            LogicalOperator::CreateRoutine(_) => vec![],
            LogicalOperator::Alter(_) => vec![],
            LogicalOperator::CreateSequence(_) => vec![],
            LogicalOperator::CreateSchema(_) => vec![],
            LogicalOperator::CreateIndex(_) => vec![],
            LogicalOperator::CreateView(_) => vec![],
            LogicalOperator::Drop(_) => vec![],
            LogicalOperator::CreatePropertyGraph(_) => vec![],
            LogicalOperator::DropPropertyGraph(_) => vec![],
            LogicalOperator::RefreshPropertyGraph(_) => vec![],
            LogicalOperator::Aggregate(op) => vec![op.child.as_ref()],
            LogicalOperator::Insert(op) => vec![op.child.as_ref()],
            LogicalOperator::Delete(op) => vec![op.child.as_ref()],
            LogicalOperator::Update(op) => vec![op.child.as_ref()],
            LogicalOperator::ExpressionGet(_) => vec![],
            LogicalOperator::DelimGet(_) => vec![],
            LogicalOperator::Join(j) => vec![j.left(), j.right()],
            LogicalOperator::DependentJoin(d) => vec![d.left.as_ref(), d.right.as_ref()],
            LogicalOperator::SetOperation(s) => vec![s.left(), s.right()],
            LogicalOperator::Distinct(d) => vec![d.child.as_ref()],
            LogicalOperator::Window(w) => vec![w.child.as_ref()],
            LogicalOperator::Explain(e) => vec![e.child.as_ref()],
            LogicalOperator::EmptyResult(e) => vec![e.child.as_ref()],
            LogicalOperator::MaterializedCTE(c) => vec![c.cte_query.as_ref(), c.child.as_ref()],
            LogicalOperator::RecursiveCTE(c) => vec![c.anchor.as_ref(), c.recursive.as_ref()],
            LogicalOperator::CTERef(_) => vec![],
            LogicalOperator::TableFunctionGet(_) => vec![],
            LogicalOperator::SearchScan(_) => vec![],
            LogicalOperator::FullTextFilterScan(_) => vec![],
            LogicalOperator::CopyTo(copy) => vec![copy.child.as_ref()],
            LogicalOperator::GraphMatch(_) => vec![],
            LogicalOperator::GraphScan(_) => vec![],
            LogicalOperator::GraphExpand(ge) => vec![ge.child.as_ref()],
            LogicalOperator::DummyScan => vec![],
        }
    }

    pub fn visit_children_mut<F>(&mut self, mut f: F) -> ControlFlow<()>
    where
        F: for<'a> FnMut(&'a mut LogicalPlan) -> ControlFlow<()>,
    {
        match self {
            LogicalOperator::Get(_) => ControlFlow::Continue(()),
            LogicalOperator::Filter(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::Projection(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::RowFetch(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::ExternalProject(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::ExternalTable(op) => {
                if let Some(child) = &mut op.child {
                    visit_boxed_child(child, &mut f)
                } else {
                    ControlFlow::Continue(())
                }
            }
            LogicalOperator::Limit(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::Order(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::TopN(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::CreateTable(_) => ControlFlow::Continue(()),
            LogicalOperator::CreateRoutine(_) => ControlFlow::Continue(()),
            LogicalOperator::Alter(_) => ControlFlow::Continue(()),
            LogicalOperator::CreateSequence(_) => ControlFlow::Continue(()),
            LogicalOperator::CreateSchema(_) => ControlFlow::Continue(()),
            LogicalOperator::CreateIndex(_) => ControlFlow::Continue(()),
            LogicalOperator::CreateView(_) => ControlFlow::Continue(()),
            LogicalOperator::Drop(_) => ControlFlow::Continue(()),
            LogicalOperator::CreatePropertyGraph(_) => ControlFlow::Continue(()),
            LogicalOperator::DropPropertyGraph(_) => ControlFlow::Continue(()),
            LogicalOperator::RefreshPropertyGraph(_) => ControlFlow::Continue(()),
            LogicalOperator::Aggregate(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::Insert(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::Delete(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::Update(op) => visit_boxed_child(&mut op.child, &mut f),
            LogicalOperator::ExpressionGet(_) => ControlFlow::Continue(()),
            LogicalOperator::Join(join) => match join {
                Join::Comparison(join) => {
                    visit_boxed_child(&mut join.left, &mut f)?;
                    visit_boxed_child(&mut join.right, &mut f)
                }
                Join::Any(join) => {
                    visit_boxed_child(&mut join.left, &mut f)?;
                    visit_boxed_child(&mut join.right, &mut f)
                }
                Join::Cross(join) => {
                    visit_boxed_child(&mut join.left, &mut f)?;
                    visit_boxed_child(&mut join.right, &mut f)
                }
            },
            LogicalOperator::DelimGet(_) => ControlFlow::Continue(()),
            LogicalOperator::DependentJoin(join) => {
                visit_boxed_child(&mut join.left, &mut f)?;
                visit_boxed_child(&mut join.right, &mut f)
            }
            LogicalOperator::SetOperation(setop) => {
                visit_boxed_child(&mut setop.left, &mut f)?;
                visit_boxed_child(&mut setop.right, &mut f)
            }
            LogicalOperator::Distinct(distinct) => visit_boxed_child(&mut distinct.child, &mut f),
            LogicalOperator::Window(window) => visit_boxed_child(&mut window.child, &mut f),
            LogicalOperator::Explain(explain) => visit_boxed_child(&mut explain.child, &mut f),
            LogicalOperator::EmptyResult(empty) => visit_boxed_child(&mut empty.child, &mut f),
            LogicalOperator::MaterializedCTE(cte) => {
                visit_boxed_child(&mut cte.cte_query, &mut f)?;
                visit_boxed_child(&mut cte.child, &mut f)
            }
            LogicalOperator::RecursiveCTE(cte) => {
                visit_boxed_child(&mut cte.anchor, &mut f)?;
                visit_boxed_child(&mut cte.recursive, &mut f)
            }
            LogicalOperator::CTERef(_) => ControlFlow::Continue(()),
            LogicalOperator::TableFunctionGet(_) => ControlFlow::Continue(()),
            LogicalOperator::SearchScan(_) => ControlFlow::Continue(()),
            LogicalOperator::FullTextFilterScan(_) => ControlFlow::Continue(()),
            LogicalOperator::CopyTo(copy) => visit_boxed_child(&mut copy.child, &mut f),
            LogicalOperator::GraphMatch(_) => ControlFlow::Continue(()),
            LogicalOperator::GraphScan(_) => ControlFlow::Continue(()),
            LogicalOperator::GraphExpand(expand) => visit_boxed_child(&mut expand.child, &mut f),
            LogicalOperator::DummyScan => ControlFlow::Continue(()),
        }
    }

    pub(crate) fn try_map_owned_children(
        self,
        f: &mut dyn FnMut(LogicalPlan) -> Result<LogicalPlan>,
    ) -> Result<Self> {
        match self {
            LogicalOperator::Get(op) => Ok(LogicalOperator::Get(op)),
            LogicalOperator::Filter(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Filter(op))
            }
            LogicalOperator::Projection(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Projection(op))
            }
            LogicalOperator::RowFetch(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::RowFetch(op))
            }
            LogicalOperator::ExternalProject(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::ExternalProject(op))
            }
            LogicalOperator::ExternalTable(mut op) => {
                if let Some(child) = op.child.take() {
                    op.child = Some(try_map_boxed_child(child, f)?);
                }
                Ok(LogicalOperator::ExternalTable(op))
            }
            LogicalOperator::Limit(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Limit(op))
            }
            LogicalOperator::Order(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Order(op))
            }
            LogicalOperator::TopN(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::TopN(op))
            }
            LogicalOperator::CreateTable(op) => Ok(LogicalOperator::CreateTable(op)),
            LogicalOperator::CreateRoutine(op) => Ok(LogicalOperator::CreateRoutine(op)),
            LogicalOperator::Alter(op) => Ok(LogicalOperator::Alter(op)),
            LogicalOperator::CreateSequence(op) => Ok(LogicalOperator::CreateSequence(op)),
            LogicalOperator::CreateSchema(op) => Ok(LogicalOperator::CreateSchema(op)),
            LogicalOperator::CreateIndex(op) => Ok(LogicalOperator::CreateIndex(op)),
            LogicalOperator::CreateView(op) => Ok(LogicalOperator::CreateView(op)),
            LogicalOperator::Drop(op) => Ok(LogicalOperator::Drop(op)),
            LogicalOperator::CreatePropertyGraph(op) => {
                Ok(LogicalOperator::CreatePropertyGraph(op))
            }
            LogicalOperator::DropPropertyGraph(op) => Ok(LogicalOperator::DropPropertyGraph(op)),
            LogicalOperator::RefreshPropertyGraph(op) => {
                Ok(LogicalOperator::RefreshPropertyGraph(op))
            }
            LogicalOperator::Aggregate(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Aggregate(op))
            }
            LogicalOperator::Insert(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Insert(op))
            }
            LogicalOperator::Delete(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Delete(op))
            }
            LogicalOperator::Update(mut op) => {
                op.child = try_map_boxed_child(op.child, f)?;
                Ok(LogicalOperator::Update(op))
            }
            LogicalOperator::ExpressionGet(op) => Ok(LogicalOperator::ExpressionGet(op)),
            LogicalOperator::Join(join) => match join {
                Join::Comparison(mut join) => {
                    join.left = try_map_boxed_child(join.left, f)?;
                    join.right = try_map_boxed_child(join.right, f)?;
                    Ok(LogicalOperator::Join(Join::Comparison(join)))
                }
                Join::Any(mut join) => {
                    join.left = try_map_boxed_child(join.left, f)?;
                    join.right = try_map_boxed_child(join.right, f)?;
                    Ok(LogicalOperator::Join(Join::Any(join)))
                }
                Join::Cross(mut join) => {
                    join.left = try_map_boxed_child(join.left, f)?;
                    join.right = try_map_boxed_child(join.right, f)?;
                    Ok(LogicalOperator::Join(Join::Cross(join)))
                }
            },
            LogicalOperator::DelimGet(op) => Ok(LogicalOperator::DelimGet(op)),
            LogicalOperator::DependentJoin(mut join) => {
                join.left = try_map_boxed_child(join.left, f)?;
                join.right = try_map_boxed_child(join.right, f)?;
                Ok(LogicalOperator::DependentJoin(join))
            }
            LogicalOperator::SetOperation(mut setop) => {
                setop.left = try_map_boxed_child(setop.left, f)?;
                setop.right = try_map_boxed_child(setop.right, f)?;
                Ok(LogicalOperator::SetOperation(setop))
            }
            LogicalOperator::Distinct(mut distinct) => {
                distinct.child = try_map_boxed_child(distinct.child, f)?;
                Ok(LogicalOperator::Distinct(distinct))
            }
            LogicalOperator::Window(mut window) => {
                window.child = try_map_boxed_child(window.child, f)?;
                Ok(LogicalOperator::Window(window))
            }
            LogicalOperator::Explain(mut explain) => {
                explain.child = try_map_boxed_child(explain.child, f)?;
                Ok(LogicalOperator::Explain(explain))
            }
            LogicalOperator::EmptyResult(mut empty) => {
                empty.child = try_map_boxed_child(empty.child, f)?;
                Ok(LogicalOperator::EmptyResult(empty))
            }
            LogicalOperator::MaterializedCTE(mut cte) => {
                cte.cte_query = try_map_boxed_child(cte.cte_query, f)?;
                cte.child = try_map_boxed_child(cte.child, f)?;
                Ok(LogicalOperator::MaterializedCTE(cte))
            }
            LogicalOperator::RecursiveCTE(mut cte) => {
                cte.anchor = try_map_boxed_child(cte.anchor, f)?;
                cte.recursive = try_map_boxed_child(cte.recursive, f)?;
                Ok(LogicalOperator::RecursiveCTE(cte))
            }
            LogicalOperator::CTERef(op) => Ok(LogicalOperator::CTERef(op)),
            LogicalOperator::TableFunctionGet(op) => Ok(LogicalOperator::TableFunctionGet(op)),
            LogicalOperator::SearchScan(op) => Ok(LogicalOperator::SearchScan(op)),
            LogicalOperator::FullTextFilterScan(op) => Ok(LogicalOperator::FullTextFilterScan(op)),
            LogicalOperator::CopyTo(mut copy) => {
                copy.child = try_map_boxed_child(copy.child, f)?;
                Ok(LogicalOperator::CopyTo(copy))
            }
            LogicalOperator::GraphMatch(op) => Ok(LogicalOperator::GraphMatch(op)),
            LogicalOperator::GraphScan(op) => Ok(LogicalOperator::GraphScan(op)),
            LogicalOperator::GraphExpand(mut expand) => {
                expand.child = try_map_boxed_child(expand.child, f)?;
                Ok(LogicalOperator::GraphExpand(expand))
            }
            LogicalOperator::DummyScan => Ok(LogicalOperator::DummyScan),
        }
    }

    /// Resolve column bindings for this operator.
    pub fn resolve_column_bindings(&self, _bindings: &[ColumnBinding]) -> Vec<ColumnBinding> {
        vec![]
    }

    /// Get the column bindings produced by this operator.
    ///
    /// This returns a list of ColumnBinding that represents the columns
    /// output by this operator. Each binding contains a (table_index, column_index)
    /// pair that uniquely identifies a column.
    ///
    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        match self {
            LogicalOperator::Get(get) => {
                // Generate bindings for each column in the scan
                Self::generate_column_bindings(get.table_index, get.returned_types.len())
            }
            LogicalOperator::Filter(filter) => {
                let child_bindings = filter.child.get_column_bindings();
                match filter.projection_map.as_columns() {
                    None => child_bindings,
                    Some(indices) => indices
                        .iter()
                        .filter_map(|&idx| child_bindings.get(idx).copied())
                        .collect(),
                }
            }
            LogicalOperator::Projection(proj) => {
                Self::generate_column_bindings(proj.table_index, proj.expressions.len())
            }
            LogicalOperator::RowFetch(fetch) => {
                let mut bindings = fetch.child.get_column_bindings();
                for source in &fetch.sources {
                    bindings.extend(source.needed_columns.iter().map(|&column| {
                        ColumnBinding::new(source.materialized_table_index, column)
                    }));
                }
                bindings
            }
            LogicalOperator::ExternalProject(external) => {
                let mut bindings = external.child.get_column_bindings();
                let base = bindings.len();
                for i in 0..external.expressions.len() {
                    bindings.push(ColumnBinding::new(external.project_index, base + i));
                }
                bindings
            }
            LogicalOperator::ExternalTable(external) => {
                Self::generate_column_bindings(external.table_index, external.returned_types.len())
            }
            LogicalOperator::Limit(limit) => {
                // Limit passes through child's bindings unchanged
                limit.child.get_column_bindings()
            }
            LogicalOperator::Order(order) => {
                let child_bindings = order.child.get_column_bindings();
                match order.projection_map.as_columns() {
                    None => child_bindings,
                    Some(indices) => indices
                        .iter()
                        .filter_map(|&idx| child_bindings.get(idx).copied())
                        .collect(),
                }
            }
            LogicalOperator::TopN(topn) => {
                // TopN passes through child's bindings unchanged
                topn.child.get_column_bindings()
            }
            LogicalOperator::Aggregate(agg) => agg.get_column_bindings(),
            LogicalOperator::Join(join) => {
                // Join combines bindings from both sides
                let left_bindings = join.left().get_column_bindings();
                let right_bindings = join.right().get_column_bindings();

                match join {
                    Join::Comparison(cj) => cj.get_column_bindings(&left_bindings, &right_bindings),
                    Join::Any(aj) => aj.get_column_bindings(&left_bindings, &right_bindings),
                    Join::Cross(_) => {
                        let mut bindings = Vec::new();
                        bindings.extend(left_bindings);
                        bindings.extend(right_bindings);
                        bindings
                    }
                }
            }
            LogicalOperator::DelimGet(delim_get) => {
                Self::generate_column_bindings(delim_get.table_index, delim_get.chunk_types.len())
            }
            LogicalOperator::DependentJoin(dj) => {
                let left_bindings = dj.left.get_column_bindings();
                let right_bindings = dj.right.get_column_bindings();
                dj.get_column_bindings(&left_bindings, &right_bindings)
            }
            LogicalOperator::SetOperation(setop) => {
                // This is similar to Projection - it outputs a new set of columns
                Self::generate_column_bindings(setop.table_index, setop.column_count)
            }
            LogicalOperator::Distinct(distinct) => {
                // Distinct passes through child's bindings
                distinct.child.get_column_bindings()
            }
            LogicalOperator::Window(window) => {
                // Window adds new columns to child's bindings
                let mut bindings = window.child.get_column_bindings();
                for i in 0..window.expressions.len() {
                    bindings.push(ColumnBinding::new(window.window_index, i));
                }
                bindings
            }
            LogicalOperator::Explain(_) => {
                // EXPLAIN returns one VARCHAR column (QUERY PLAN)
                Self::generate_column_bindings(0, 1)
            }
            LogicalOperator::EmptyResult(empty) => empty.get_column_bindings(),
            LogicalOperator::MaterializedCTE(cte) => {
                // CTE returns child's bindings
                cte.child.get_column_bindings()
            }
            LogicalOperator::RecursiveCTE(cte) => {
                Self::generate_column_bindings(cte.cte_index, cte.column_types.len())
            }
            LogicalOperator::CTERef(cte_ref) => {
                Self::generate_column_bindings(cte_ref.table_index, cte_ref.column_types.len())
            }
            LogicalOperator::ExpressionGet(expr_get) => {
                Self::generate_column_bindings(expr_get.table_index, expr_get.types.len())
            }
            LogicalOperator::TableFunctionGet(tf) => {
                Self::generate_column_bindings(tf.table_index, tf.column_types.len())
            }
            LogicalOperator::SearchScan(search) => Self::generate_column_bindings(
                search.projection_table_index,
                search.projections.len(),
            ),
            LogicalOperator::FullTextFilterScan(scan) => {
                Self::generate_column_bindings(scan.get.table_index, scan.get.returned_types.len())
            }
            LogicalOperator::CopyTo(copy) => {
                // CopyTo returns row count (or other COPY return types)
                Self::generate_column_bindings(0, copy.types.len())
            }
            LogicalOperator::GraphMatch(gm) => {
                Self::generate_column_bindings(gm.table_index, gm.output_types.len())
            }
            LogicalOperator::GraphScan(gs) => {
                Self::generate_column_bindings(gs.output_table_index, gs.output_types.len())
            }
            LogicalOperator::GraphExpand(ge) => {
                Self::generate_column_bindings(ge.output_table_index, ge.output_types().len())
            }
            LogicalOperator::Insert(_) => {
                // Insert returns a single column (row count)
                vec![ColumnBinding::new(0, 0)]
            }
            LogicalOperator::Delete(del) => {
                let _ = del;
                // Delete returns a single column (affected row count)
                vec![ColumnBinding::new(0, 0)]
            }
            LogicalOperator::Update(upd) => {
                let _ = upd;
                // Update returns a single column (affected row count)
                vec![ColumnBinding::new(0, 0)]
            }
            // DDL operations don't produce column bindings
            LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::DummyScan => vec![],
        }
    }

    /// Generate column bindings for a given table index and column count.
    ///
    pub fn generate_column_bindings(table_index: usize, column_count: usize) -> Vec<ColumnBinding> {
        (0..column_count)
            .map(|i| ColumnBinding::new(table_index, i))
            .collect()
    }

    /// Convert column bindings to a string for debugging.
    pub fn column_bindings_to_string(bindings: &[ColumnBinding]) -> String {
        let binding_strs: Vec<String> = bindings
            .iter()
            .map(|b| format!("[{}.{}]", b.table_index, b.column_index))
            .collect();
        binding_strs.join(", ")
    }

    /// Get the table indices used by this operator.
    ///
    /// Returns a list of table indices that this operator introduces.
    /// Used for verification to ensure no duplicate table indices exist.
    ///
    pub fn get_table_index(&self) -> Vec<usize> {
        match self {
            LogicalOperator::Get(get) => vec![get.table_index],
            LogicalOperator::Projection(proj) => vec![proj.table_index],
            LogicalOperator::RowFetch(fetch) => fetch
                .sources
                .iter()
                .map(|source| source.materialized_table_index)
                .collect(),
            LogicalOperator::ExternalProject(external) => vec![external.project_index],
            LogicalOperator::ExternalTable(external) => vec![external.table_index],
            LogicalOperator::Aggregate(agg) => {
                let mut indices = Vec::new();
                if !agg.groups.is_empty() {
                    indices.push(agg.group_index);
                }
                if !agg.aggregates.is_empty() {
                    indices.push(agg.aggregate_index);
                }
                if !agg.grouping_functions.is_empty() {
                    indices.push(agg.groupings_index);
                }
                if let Some(reduction) = &agg.post_reduction {
                    indices.push(reduction.reduction_index);
                }
                indices
            }
            LogicalOperator::Window(window) => vec![window.window_index],
            LogicalOperator::RecursiveCTE(cte) => vec![cte.cte_index],
            LogicalOperator::CTERef(cte_ref) => vec![cte_ref.table_index],
            LogicalOperator::ExpressionGet(expr_get) => vec![expr_get.table_index],
            LogicalOperator::DelimGet(delim_get) => vec![delim_get.table_index],
            LogicalOperator::TableFunctionGet(tf) => vec![tf.table_index],
            LogicalOperator::SearchScan(search) => {
                let mut indices = vec![search.get.table_index];
                if search.projection_table_index != search.get.table_index {
                    indices.push(search.projection_table_index);
                }
                indices
            }
            LogicalOperator::FullTextFilterScan(scan) => vec![scan.get.table_index],
            LogicalOperator::GraphMatch(gm) => vec![gm.table_index],
            LogicalOperator::GraphScan(gs) => vec![gs.table_index],
            LogicalOperator::GraphExpand(ge) => {
                vec![ge.edge_table_index, ge.target_table_index]
            }
            // Operators that don't introduce new table indices
            LogicalOperator::Filter(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Join(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::SetOperation(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::Insert(_)
            | LogicalOperator::Delete(_)
            | LogicalOperator::Update(_)
            | LogicalOperator::CopyTo(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::Explain(_)
            | LogicalOperator::EmptyResult(_)
            | LogicalOperator::DummyScan => vec![],
        }
    }

    /// Returns true if this operator is a graph chain root (GraphScan/GraphExpand),
    /// possibly wrapped by filters.
    ///
    /// This is used by multiple passes (column binding resolver, filter pushdown,
    /// physical plan generator) to detect graph projections that require special
    /// handling (late materialization via PhysicalGraphProject).
    pub fn is_graph_chain(&self) -> bool {
        match self {
            LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_) => true,
            LogicalOperator::Filter(f) => f.child.is_graph_chain(),
            LogicalOperator::EmptyResult(e) => e.child.is_graph_chain(),
            _ => false,
        }
    }
}

fn expression_output_name(
    expr: &crate::expression::Expression,
    idx: usize,
    fallback_prefix: &str,
) -> String {
    match expr {
        crate::expression::Expression::ColumnRef(column_ref) => {
            format!("col_{}", column_ref.binding.column_index + 1)
        }
        crate::expression::Expression::Reference(reference) => {
            format!("ref_{}", reference.index + 1)
        }
        crate::expression::Expression::Aggregate(aggregate) => aggregate.function.name.clone(),
        crate::expression::Expression::Window(window) => window.function_name().to_string(),
        crate::expression::Expression::Function(function) => function.function.name.clone(),
        _ => format!("{fallback_prefix}_{}", idx + 1),
    }
}

fn window_output_name(expr: &crate::expression::WindowExpression, idx: usize) -> String {
    if expr.function_name().is_empty() {
        format!("window_{}", idx + 1)
    } else {
        expr.function_name().to_string()
    }
}

fn visit_boxed_child(
    child: &mut Box<LogicalPlan>,
    f: &mut impl for<'a> FnMut(&'a mut LogicalPlan) -> ControlFlow<()>,
) -> ControlFlow<()> {
    f(child.as_mut())
}

fn try_map_boxed_child(
    child: Box<LogicalPlan>,
    f: &mut dyn FnMut(LogicalPlan) -> Result<LogicalPlan>,
) -> Result<Box<LogicalPlan>> {
    Ok(Box::new(f(*child)?))
}

#[cfg(test)]
mod tests {
    use std::{ops::ControlFlow, sync::Arc};

    use super::*;
    use crate::binder::context::BindContext;
    use crate::binder::ir::CTEMaterialize;
    use crate::expression::{
        AggregateExpression, ConstantExpression, Expression, WindowExpression, WindowFrame,
    };
    use crate::operator::{
        Aggregate, AnyJoin, ComparisonJoin, DelimGet, DependentJoin, EmptyResult, ExpandDirection,
        Explain, ExplainSpec, ExpressionGet, Join, JoinType, SearchCandidate, SearchDecision,
        SearchScan,
    };
    use crate::plan::LogicalPlan;
    use paro_catalog::entry::{ColumnDefinition, EdgeTableInfo, TableCatalogEntry};
    use paro_common::runtime_value::Value;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::copy::{register_copy_functions, CopyFunctionBindData, CopyOptions};
    use paro_function::window::WindowFunction;
    use paro_parser::ast::CopySource;
    use paro_storage::search::{
        HnswIntent, NormalizedSearchRequest, ProjectionSpec, SearchIndexKind, SearchIntent,
        SearchRequestMode,
    };
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;

    fn expression_get(table_index: usize, types: Vec<LogicalType>) -> LogicalOperator {
        let names = (0..types.len()).map(|idx| format!("c{}", idx)).collect();
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::<Vec<Expression>>::new(),
            names,
            types,
        ))
    }

    fn lp(op: LogicalOperator) -> LogicalPlan {
        LogicalPlan::new(&BindContext::new(), op)
    }

    fn leaf_plan(bind_ctx: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            bind_ctx,
            expression_get(table_index, vec![LogicalType::Integer]),
        )
    }

    fn boolean_constant(value: bool) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Boolean(value),
            LogicalType::Boolean,
        ))
    }

    fn integer_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn create_storage(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_table(name: &str) -> Arc<TableCatalogEntry> {
        Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            name.to_string(),
            vec![ColumnDefinition::new(
                "c1".to_string(),
                LogicalType::Integer,
            )],
            Arc::new(create_storage(&[LogicalType::Integer])),
            paro_catalog::entry::CatalogObjectId::from_raw(10_001),
            0,
        ))
    }

    fn create_copy_to(child: LogicalPlan) -> CopyTo {
        let copy_function = register_copy_functions()
            .into_iter()
            .next()
            .expect("copy function");
        let names = vec!["c1".to_string()];
        let types = vec![LogicalType::Integer];
        let bind_data: Arc<dyn CopyFunctionBindData> = Arc::from(
            (copy_function.copy_to_bind)(&CopyOptions::default(), &names, &types).unwrap(),
        );

        CopyTo::new(
            copy_function,
            bind_data,
            "out.csv".to_string(),
            CopySource::Stdout,
            CopyOptions::default(),
            child,
            names,
            types,
        )
    }

    fn sample_edge_info() -> EdgeTableInfo {
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 1,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "src".to_string(),
            source_ref_column_ids: vec![0],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "dst".to_string(),
            destination_ref_column_ids: vec![0],
            label: "edge".to_string(),
            property_column_ids: vec![],
        }
    }

    fn sample_non_leaf_operators() -> Vec<(&'static str, LogicalOperator)> {
        vec![
            {
                let ctx = BindContext::new();
                (
                    "filter",
                    LogicalOperator::Filter(Filter::new(leaf_plan(&ctx, 10), vec![])),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "projection",
                    LogicalOperator::Projection(Projection::new(20, leaf_plan(&ctx, 10), vec![])),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "limit",
                    LogicalOperator::Limit(Limit::new(leaf_plan(&ctx, 10), None, None)),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "order",
                    LogicalOperator::Order(Order::new(leaf_plan(&ctx, 10), vec![])),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "topn",
                    LogicalOperator::TopN(TopN::new(leaf_plan(&ctx, 10), vec![], 5, 1)),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "aggregate",
                    LogicalOperator::Aggregate(Aggregate::new(
                        30,
                        31,
                        32,
                        leaf_plan(&ctx, 10),
                        vec![],
                        Vec::new(),
                        vec![],
                        vec![],
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "insert",
                    LogicalOperator::Insert(Insert::new(
                        create_table("insert"),
                        vec![0],
                        vec![LogicalType::Integer],
                        None,
                        leaf_plan(&ctx, 10),
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "delete",
                    LogicalOperator::Delete(Delete::new(
                        create_table("delete"),
                        0,
                        leaf_plan(&ctx, 10),
                        false,
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "update",
                    LogicalOperator::Update(Update::new(
                        create_table("update"),
                        0,
                        vec![0],
                        vec![integer_constant(1)],
                        leaf_plan(&ctx, 10),
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "comparison_join",
                    LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                        JoinType::Inner,
                        leaf_plan(&ctx, 10),
                        leaf_plan(&ctx, 20),
                        vec![],
                    ))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "any_join",
                    LogicalOperator::Join(Join::Any(Box::new(AnyJoin::new(
                        JoinType::Inner,
                        leaf_plan(&ctx, 10),
                        leaf_plan(&ctx, 20),
                        boolean_constant(true),
                    )))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "cross_join",
                    LogicalOperator::Join(Join::cross(leaf_plan(&ctx, 10), leaf_plan(&ctx, 20))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "dependent_join",
                    LogicalOperator::DependentJoin(DependentJoin::scalar(
                        leaf_plan(&ctx, 10),
                        leaf_plan(&ctx, 20),
                        vec![],
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "set_operation",
                    LogicalOperator::SetOperation(SetOperation::union(
                        40,
                        leaf_plan(&ctx, 10),
                        leaf_plan(&ctx, 20),
                        false,
                        vec![LogicalType::Integer],
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "distinct",
                    LogicalOperator::Distinct(Distinct::new(leaf_plan(&ctx, 10))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "window",
                    LogicalOperator::Window(Window::new(50, vec![], leaf_plan(&ctx, 10))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "explain",
                    LogicalOperator::Explain(Explain::new(
                        leaf_plan(&ctx, 10),
                        ExplainSpec::text_plan(),
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "empty_result",
                    LogicalOperator::EmptyResult(EmptyResult::new(leaf_plan(&ctx, 10))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "materialized_cte",
                    LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                        60,
                        "cte".to_string(),
                        vec!["c1".to_string()],
                        vec![LogicalType::Integer],
                        CTEMaterialize::Default,
                        leaf_plan(&ctx, 10),
                        leaf_plan(&ctx, 20),
                    )),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "recursive_cte",
                    LogicalOperator::RecursiveCTE(RecursiveCTE {
                        cte_index: 61,
                        cte_name: "rcte".to_string(),
                        column_names: vec!["c1".to_string()],
                        column_types: vec![LogicalType::Integer],
                        union_all: true,
                        anchor: Box::new(leaf_plan(&ctx, 10)),
                        recursive: Box::new(leaf_plan(&ctx, 20)),
                    }),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "copy_to",
                    LogicalOperator::CopyTo(create_copy_to(leaf_plan(&ctx, 10))),
                )
            },
            {
                let ctx = BindContext::new();
                (
                    "graph_expand",
                    LogicalOperator::GraphExpand(GraphExpand::new(
                        sample_edge_info(),
                        ExpandDirection::Forward,
                        "src".to_string(),
                        10,
                        11,
                        12,
                        13,
                        "dst".to_string(),
                        100,
                        101,
                        "dst_table".to_string(),
                        leaf_plan(&ctx, 10),
                    )),
                )
            },
        ]
    }

    #[test]
    fn mark_join_column_bindings_append_marker_binding() {
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            lp(expression_get(
                10,
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
            lp(expression_get(20, vec![LogicalType::Varchar])),
            vec![],
        );
        join.left_projection_map = vec![1].into();
        join.mark_index = Some(99);

        let bindings = LogicalOperator::Join(Join::Comparison(join)).get_column_bindings();
        assert_eq!(
            bindings,
            vec![ColumnBinding::new(10, 1), ColumnBinding::new(99, 0)]
        );
    }

    #[test]
    fn right_semi_join_column_bindings_only_use_right_projection() {
        let mut join = ComparisonJoin::new(
            JoinType::RightSemi,
            lp(expression_get(10, vec![LogicalType::Integer])),
            lp(expression_get(
                20,
                vec![LogicalType::Varchar, LogicalType::Boolean],
            )),
            vec![],
        );
        join.right_projection_map = vec![1].into();

        let bindings = LogicalOperator::Join(Join::Comparison(join)).get_column_bindings();
        assert_eq!(bindings, vec![ColumnBinding::new(20, 1)]);
    }

    #[test]
    fn any_mark_join_column_bindings_append_marker_binding() {
        let mut join = AnyJoin::new(
            JoinType::Mark,
            lp(expression_get(
                10,
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
            lp(expression_get(20, vec![LogicalType::Varchar])),
            Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        join.left_projection_map = vec![1].into();
        join.mark_index = Some(99);

        let bindings = LogicalOperator::Join(Join::Any(Box::new(join))).get_column_bindings();
        assert_eq!(
            bindings,
            vec![ColumnBinding::new(10, 1), ColumnBinding::new(99, 0)]
        );
    }

    #[test]
    fn any_right_semi_join_column_bindings_only_use_right_projection() {
        let mut join = AnyJoin::new(
            JoinType::RightSemi,
            lp(expression_get(10, vec![LogicalType::Integer])),
            lp(expression_get(
                20,
                vec![LogicalType::Varchar, LogicalType::Boolean],
            )),
            Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        join.right_projection_map = vec![1].into();

        let bindings = LogicalOperator::Join(Join::Any(Box::new(join))).get_column_bindings();
        assert_eq!(bindings, vec![ColumnBinding::new(20, 1)]);
    }

    #[test]
    fn inner_join_bindings_apply_projection_maps_for_comparison_and_any_join() {
        let mut comparison = ComparisonJoin::new(
            JoinType::Inner,
            lp(expression_get(
                10,
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
            lp(expression_get(
                20,
                vec![LogicalType::Varchar, LogicalType::Boolean],
            )),
            vec![],
        );
        comparison.left_projection_map = vec![1].into();
        comparison.right_projection_map = vec![0].into();

        let mut any = AnyJoin::new(
            JoinType::Inner,
            lp(expression_get(
                10,
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
            lp(expression_get(
                20,
                vec![LogicalType::Varchar, LogicalType::Boolean],
            )),
            Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        any.left_projection_map = vec![1].into();
        any.right_projection_map = vec![0].into();

        assert_eq!(
            LogicalOperator::Join(Join::Comparison(comparison)).get_column_bindings(),
            vec![ColumnBinding::new(10, 1), ColumnBinding::new(20, 0)]
        );
        assert_eq!(
            LogicalOperator::Join(Join::Any(Box::new(any))).get_column_bindings(),
            vec![ColumnBinding::new(10, 1), ColumnBinding::new(20, 0)]
        );
    }

    #[test]
    fn empty_join_projection_maps_produce_no_output_names() {
        let mut comparison = ComparisonJoin::new(
            JoinType::Inner,
            lp(expression_get(10, vec![LogicalType::Integer])),
            lp(expression_get(20, vec![LogicalType::Varchar])),
            vec![],
        );
        comparison.left_projection_map.clear();
        comparison.right_projection_map.clear();

        let mut any = AnyJoin::new(
            JoinType::Inner,
            lp(expression_get(10, vec![LogicalType::Integer])),
            lp(expression_get(20, vec![LogicalType::Varchar])),
            Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        any.left_projection_map.clear();
        any.right_projection_map.clear();

        assert!(LogicalOperator::Join(Join::Comparison(comparison))
            .output_names()
            .is_empty());
        assert!(LogicalOperator::Join(Join::Any(Box::new(any)))
            .output_names()
            .is_empty());
    }

    #[test]
    fn delim_get_generates_bindings_from_table_index() {
        let op = LogicalOperator::DelimGet(DelimGet::new(
            77,
            vec![LogicalType::Integer, LogicalType::Boolean],
        ));
        assert_eq!(
            op.get_column_bindings(),
            vec![ColumnBinding::new(77, 0), ColumnBinding::new(77, 1)]
        );
        assert_eq!(op.get_table_index(), vec![77]);
    }

    #[test]
    fn aggregate_column_bindings_split_groups_aggregates_and_groupings() {
        let aggregate = Aggregate::new(
            30,
            31,
            32,
            lp(expression_get(10, vec![LogicalType::Integer])),
            vec![Expression::Constant(ConstantExpression::new(
                Value::Integer(42),
                LogicalType::Integer,
            ))],
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                vec![],
                LogicalType::BigInt,
            ))],
            vec![vec![0]],
        );
        let op = LogicalOperator::Aggregate(aggregate);

        assert_eq!(
            op.types(),
            vec![
                LogicalType::Integer,
                LogicalType::BigInt,
                LogicalType::BigInt
            ]
        );
        assert_eq!(
            op.get_column_bindings(),
            vec![
                ColumnBinding::new(30, 0),
                ColumnBinding::new(31, 0),
                ColumnBinding::new(32, 0),
            ]
        );
        assert_eq!(op.get_table_index(), vec![30, 31, 32]);
    }

    #[test]
    fn window_column_bindings_are_local_to_window_operator() {
        let function = WindowFunction::row_number();
        let window = Window::new(
            77,
            vec![WindowExpression::native(
                function.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                WindowFrame::get_default_frame(&function),
                false,
            )],
            lp(expression_get(
                10,
                vec![LogicalType::Integer, LogicalType::Boolean],
            )),
        );
        let op = LogicalOperator::Window(window);

        assert_eq!(
            op.get_column_bindings(),
            vec![
                ColumnBinding::new(10, 0),
                ColumnBinding::new(10, 1),
                ColumnBinding::new(77, 0),
            ]
        );
    }

    #[test]
    fn search_scan_bindings_use_projection_table_index() {
        let search = SearchScan::new(
            Get::new_without_table(
                10,
                vec!["embedding".to_string(), "body".to_string()],
                vec![LogicalType::Float, LogicalType::Varchar],
            ),
            NormalizedSearchRequest {
                table_id: 10,
                mode: SearchRequestMode::TopK { limit: 5 },
                predicate: None,
                projections: ProjectionSpec {
                    columns: vec![0, 1],
                    include_score: false,
                },
                intents: vec![SearchIntent::Hnsw(HnswIntent {
                    column_id: 1,
                    query_vector: vec![0.1, 0.2],
                })],
                fusion: None,
            },
            SearchDecision::IndexScan {
                candidate: SearchCandidate {
                    intent: SearchIntent::Hnsw(HnswIntent {
                        column_id: 1,
                        query_vector: vec![0.1, 0.2],
                    }),
                    token: paro_storage::search::CapabilityToken {
                        definition_id: 1,
                        generation_id: 1,
                        root_version: 1,
                        capability_state: paro_storage::search::SearchCapabilityState::Queryable,
                    },
                    kind: SearchIndexKind::Hnsw,
                    estimated_cost: None,
                },
                confidence: crate::operator::Confidence::High,
            },
            vec![Expression::Constant(ConstantExpression::new(
                paro_common::runtime_value::Value::Integer(1),
                LogicalType::Integer,
            ))],
            22,
            vec![],
            vec![],
            0,
            Expression::Constant(ConstantExpression::new(
                paro_common::runtime_value::Value::Float(0.5),
                LogicalType::Float,
            )),
            true,
            5,
        )
        .with_output_names(vec!["score".to_string()]);

        let op = LogicalOperator::SearchScan(search);

        assert_eq!(op.output_names(), vec!["score".to_string()]);
        assert_eq!(op.get_column_bindings(), vec![ColumnBinding::new(22, 0)]);
        assert_eq!(op.get_table_index(), vec![10, 22]);
    }

    #[test]
    fn non_leaf_child_primitives_cover_the_same_children() {
        for (name, mut op) in sample_non_leaf_operators() {
            let expected_ids: Vec<_> = op.children().iter().map(|child| child.id).collect();
            assert!(
                !expected_ids.is_empty(),
                "{name} should contribute at least one child"
            );

            let visit_result = op.visit_children_mut(|child| {
                assert!(
                    expected_ids.contains(&child.id),
                    "{name} visited unexpected child"
                );
                ControlFlow::Continue(())
            });
            assert_eq!(visit_result, ControlFlow::Continue(()), "{name}");

            let mut mapped_ids = Vec::new();
            let mapped = op
                .try_map_owned_children(&mut |child| {
                    mapped_ids.push(child.id);
                    Ok(child)
                })
                .unwrap_or_else(|err| panic!("{name} child mapping failed: {err}"));

            let actual_ids: Vec<_> = mapped.children().iter().map(|child| child.id).collect();
            assert_eq!(actual_ids, expected_ids, "{name}");
            assert_eq!(mapped_ids, expected_ids, "{name}");
        }
    }
}
