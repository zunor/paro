// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Operator
//!
//! The core enum representing nodes in the logical query plan.

use std::ops::ControlFlow;

use paro_common::{error::Result, types::LogicalType};

use crate::plan::LogicalPlan;

use super::{
    Aggregate, Alter, BoundReference, CTERef, ColumnBinding, CopyTo, CreateIndex,
    CreatePropertyGraph, CreateRoutine, CreateSchema, CreateSequence, CreateTable, CreateView,
    Delete, DelimGet, DependentJoin, DependentJoinKind, Distinct, Drop, DropPropertyGraph,
    EmptyResult, Explain, ExpressionGet, Filter, FullTextFilterScan, Get, GraphExpand, GraphMatch,
    GraphScan, Insert, Join, JoinType, Limit, LogicalExternalProject, LogicalExternalTable,
    LogicalOperatorType, MaterializedCTE, Order, Projection, ProjectionMap, RecursiveCTE,
    RefreshPropertyGraph, RowFetch, SearchScan, SetOpType, SetOperation, TableFunctionGet, TopN,
    Update, Window,
};

/// The execution-facing positional output layout of one logical plan node.
///
/// Types and bindings are derived together so positional consumers cannot
/// accidentally combine results from two independent tree walks. The fields
/// remain private to keep their lengths aligned. SQL-visible display names are
/// intentionally separate: unlike bindings, they are presentation metadata and
/// need not survive every execution-only projection or rewrite.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalOutputLayout {
    types: Vec<LogicalType>,
    bindings: Vec<ColumnBinding>,
}

impl LogicalOutputLayout {
    /// Construct an aligned local schema without consulting descendant plans.
    pub fn new(types: Vec<LogicalType>, bindings: Vec<ColumnBinding>) -> Self {
        assert_eq!(
            types.len(),
            bindings.len(),
            "logical output types and bindings must stay positionally aligned"
        );
        Self { types, bindings }
    }

    fn for_table(table_index: usize, types: Vec<LogicalType>) -> Self {
        let bindings = LogicalOperator::generate_column_bindings(table_index, types.len());
        Self::new(types, bindings)
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    pub fn types(&self) -> &[LogicalType] {
        &self.types
    }

    pub fn bindings(&self) -> &[ColumnBinding] {
        &self.bindings
    }

    pub fn into_types(self) -> Vec<LogicalType> {
        self.types
    }

    pub fn into_bindings(self) -> Vec<ColumnBinding> {
        self.bindings
    }

    fn push(&mut self, logical_type: LogicalType, binding: ColumnBinding) {
        self.types.push(logical_type);
        self.bindings.push(binding);
    }

    fn append(&mut self, mut other: Self) {
        self.types.append(&mut other.types);
        self.bindings.append(&mut other.bindings);
    }

    fn project(self, projection: &ProjectionMap) -> Self {
        let Some(indices) = projection.as_columns() else {
            return self;
        };
        let mut types = Vec::with_capacity(indices.len());
        let mut bindings = Vec::with_capacity(indices.len());
        for &index in indices {
            if let (Some(logical_type), Some(binding)) =
                (self.types.get(index), self.bindings.get(index))
            {
                types.push(logical_type.clone());
                bindings.push(*binding);
            }
        }
        Self::new(types, bindings)
    }
}

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

    /// Opaque schema boundary used only inside a transformation transaction.
    /// Keep this transport-only variant after every executable operator so
    /// introducing it cannot perturb legacy discriminant-based ordering.
    BoundReference(BoundReference),

    /// A dummy scan that produces one row (used for SELECT 1)
    DummyScan,
}

impl LogicalOperator {
    pub fn output_names(&self) -> Vec<String> {
        derive_output_names(self)
    }

    pub fn op_type(&self) -> LogicalOperatorType {
        match self {
            LogicalOperator::Get(_) => LogicalOperatorType::Get,
            LogicalOperator::BoundReference(_) => LogicalOperatorType::BoundReference,
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
        self.output_layout().into_types()
    }

    /// Derive types and bindings together with a bounded native stack.
    ///
    /// Schema-independent children (for example an aggregate input or a CTE
    /// producer) are not visited. Pass-through and projection operators reuse
    /// their child's owned schema vectors instead of cloning one vector per
    /// plan depth.
    pub fn output_layout(&self) -> LogicalOutputLayout {
        derive_output_layout(self)
    }

    /// Get the children of this operator.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalOperator::Get(_) => vec![],
            LogicalOperator::BoundReference(_) => vec![],
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

    /// Fold a borrowed plan in post-order with one caller-owned state per
    /// subtree and a bounded native stack.
    ///
    /// Read-only validation and analysis passes use this counterpart to
    /// [`LogicalPlan::try_fold_post_order`] so they can reuse completed child
    /// facts instead of starting a fresh subtree traversal at every node.
    pub fn try_fold_ref_post_order<State>(
        &self,
        mut fold: impl FnMut(&LogicalOperator, &[State]) -> Result<State>,
    ) -> Result<State> {
        struct Frame<'a, State> {
            operator: &'a LogicalOperator,
            remaining: std::vec::IntoIter<&'a LogicalPlan>,
            child_states: Vec<State>,
        }

        impl<'a, State> Frame<'a, State> {
            fn new(operator: &'a LogicalOperator) -> Self {
                let children = operator.children();
                let child_count = children.len();
                Self {
                    operator,
                    remaining: children.into_iter(),
                    child_states: Vec::with_capacity(child_count),
                }
            }
        }

        let mut stack = vec![Frame::new(self)];
        loop {
            let frame = stack
                .last_mut()
                .expect("borrowed post-order traversal retains its root frame");
            if let Some(child) = frame.remaining.next() {
                stack.push(Frame::new(&child.operator));
                continue;
            }

            let completed = stack
                .pop()
                .expect("borrowed post-order traversal retains its completed frame");
            let state = fold(completed.operator, &completed.child_states)?;
            let Some(parent) = stack.last_mut() else {
                return Ok(state);
            };
            parent.child_states.push(state);
        }
    }

    pub fn visit_children_mut<F>(&mut self, mut f: F) -> ControlFlow<()>
    where
        F: for<'a> FnMut(&'a mut LogicalPlan) -> ControlFlow<()>,
    {
        match self {
            LogicalOperator::Get(_) => ControlFlow::Continue(()),
            LogicalOperator::BoundReference(_) => ControlFlow::Continue(()),
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
            LogicalOperator::BoundReference(op) => Ok(LogicalOperator::BoundReference(op)),
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
        self.output_layout().into_bindings()
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
            LogicalOperator::BoundReference(_) => vec![],
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

enum OutputNamesTask<'a> {
    Derive(&'a LogicalOperator),
    Project(&'a ProjectionMap),
    ExtendRowFetch(&'a RowFetch),
    FinishJoin(&'a Join),
    FinishDependentJoin(&'a DependentJoin),
    ExtendWindow(&'a Window),
    FinishGraphExpand(&'a GraphExpand),
}

/// Derive presentation names independently from the execution layout while
/// retaining the same bounded-native-stack guarantee.
fn derive_output_names(root: &LogicalOperator) -> Vec<String> {
    use OutputNamesTask::*;

    let mut tasks = vec![Derive(root)];
    let mut outputs = Vec::<Vec<String>>::new();
    while let Some(task) = tasks.pop() {
        match task {
            Derive(operator) => match operator {
                LogicalOperator::Get(get) => outputs.push(get.names.clone()),
                LogicalOperator::BoundReference(reference) => outputs.push(
                    (0..reference.bindings.len())
                        .map(|index| format!("__bound_reference_{index}"))
                        .collect(),
                ),
                LogicalOperator::Filter(filter) => {
                    tasks.push(Project(&filter.projection_map));
                    tasks.push(Derive(&filter.child.operator));
                }
                LogicalOperator::Projection(projection) => {
                    outputs.push(projection.visible_names.clone());
                }
                LogicalOperator::RowFetch(fetch) => {
                    tasks.push(ExtendRowFetch(fetch));
                    tasks.push(Derive(&fetch.child.operator));
                }
                // ExternalProject owns an eagerly aligned presentation
                // contract, so deriving its child again would be redundant.
                LogicalOperator::ExternalProject(project) => {
                    outputs.push(project.output_names.clone());
                }
                LogicalOperator::ExternalTable(table) => {
                    outputs.push(table.output_columns.clone());
                }
                LogicalOperator::Limit(limit) => tasks.push(Derive(&limit.child.operator)),
                LogicalOperator::Order(order) => {
                    tasks.push(Project(&order.projection_map));
                    tasks.push(Derive(&order.child.operator));
                }
                LogicalOperator::TopN(topn) => {
                    tasks.push(Project(&topn.projection_map));
                    tasks.push(Derive(&topn.child.operator));
                }
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
                | LogicalOperator::DummyScan => outputs.push(Vec::new()),
                LogicalOperator::Aggregate(aggregate) => {
                    let mut names = Vec::with_capacity(
                        aggregate.groups.len()
                            + aggregate.aggregates.len()
                            + aggregate.grouping_functions.len(),
                    );
                    for (index, group) in aggregate.groups.iter().enumerate() {
                        names.push(expression_output_name(group, index, "group"));
                    }
                    for (index, aggregate) in aggregate.aggregates.iter().enumerate() {
                        names.push(expression_output_name(aggregate, index, "agg"));
                    }
                    for index in 0..aggregate.grouping_functions.len() {
                        names.push(format!("grouping_{}", index + 1));
                    }
                    outputs.push(names);
                }
                LogicalOperator::Insert(_)
                | LogicalOperator::Delete(_)
                | LogicalOperator::Update(_) => outputs.push(vec!["count".to_string()]),
                LogicalOperator::ExpressionGet(values) => outputs.push(values.names.clone()),
                LogicalOperator::Join(join) => schedule_join_names(join, &mut tasks),
                LogicalOperator::DelimGet(delim) => outputs.push(delim.chunk_names.clone()),
                LogicalOperator::DependentJoin(join) => {
                    tasks.push(FinishDependentJoin(join));
                    match &join.kind {
                        DependentJoinKind::Mark { .. } => {
                            tasks.push(Derive(&join.left.operator));
                        }
                        DependentJoinKind::Scalar { .. } | DependentJoinKind::Lateral { .. } => {
                            tasks.push(Derive(&join.right.operator));
                            tasks.push(Derive(&join.left.operator));
                        }
                    }
                }
                LogicalOperator::SetOperation(setop) => {
                    tasks.push(Derive(&setop.left().operator));
                }
                LogicalOperator::Distinct(distinct) => {
                    tasks.push(Derive(&distinct.child.operator));
                }
                LogicalOperator::Window(window) => {
                    tasks.push(ExtendWindow(window));
                    tasks.push(Derive(&window.child.operator));
                }
                LogicalOperator::Explain(_) => outputs.push(vec!["QUERY PLAN".to_string()]),
                LogicalOperator::EmptyResult(empty) => {
                    tasks.push(Derive(&empty.child.operator));
                }
                LogicalOperator::MaterializedCTE(cte) => {
                    tasks.push(Derive(&cte.child.operator));
                }
                LogicalOperator::RecursiveCTE(cte) => outputs.push(cte.column_names.clone()),
                LogicalOperator::CTERef(cte) => outputs.push(cte.column_names.clone()),
                LogicalOperator::TableFunctionGet(function) => outputs.push(function.get_names()),
                LogicalOperator::SearchScan(search) => outputs.push(search.output_names.clone()),
                LogicalOperator::FullTextFilterScan(scan) => outputs.push(project_output_names(
                    scan.get.names.clone(),
                    &scan.projection_map,
                )),
                LogicalOperator::CopyTo(copy) => outputs.push(copy.names.clone()),
                LogicalOperator::GraphMatch(graph) => outputs.push(
                    graph
                        .columns
                        .iter()
                        .map(|column| column.alias.clone())
                        .collect(),
                ),
                LogicalOperator::GraphScan(_) => {
                    outputs.push(vec!["local_vertex_id".to_string(), "rowid".to_string()])
                }
                LogicalOperator::GraphExpand(expand) => {
                    tasks.push(FinishGraphExpand(expand));
                    tasks.push(Derive(&expand.child.operator));
                }
            },
            Project(projection) => {
                let names = pop_output_names(&mut outputs);
                outputs.push(project_output_names(names, projection));
            }
            ExtendRowFetch(fetch) => {
                let mut names = pop_output_names(&mut outputs);
                for source in &fetch.sources {
                    names.extend(source.needed_columns.iter().filter_map(|&ordinal| {
                        source
                            .table
                            .columns
                            .get(ordinal)
                            .map(|column| column.name.clone())
                    }));
                }
                outputs.push(names);
            }
            FinishJoin(join) => {
                let names = finish_join_names(join, &mut outputs);
                outputs.push(names);
            }
            FinishDependentJoin(join) => {
                let mut left = match &join.kind {
                    DependentJoinKind::Mark { .. } => pop_output_names(&mut outputs),
                    DependentJoinKind::Scalar { .. } | DependentJoinKind::Lateral { .. } => {
                        let right = pop_output_names(&mut outputs);
                        let mut left = pop_output_names(&mut outputs);
                        left.extend(right);
                        left
                    }
                };
                if matches!(&join.kind, DependentJoinKind::Mark { .. }) {
                    left.push("mark".to_string());
                }
                outputs.push(left);
            }
            ExtendWindow(window) => {
                let mut names = pop_output_names(&mut outputs);
                names.extend(
                    window
                        .expressions
                        .iter()
                        .enumerate()
                        .map(|(index, expression)| window_output_name(expression, index)),
                );
                outputs.push(names);
            }
            FinishGraphExpand(expand) => {
                let mut names = pop_output_names(&mut outputs);
                names.extend([
                    "edge_rowid".to_string(),
                    "target_local_id".to_string(),
                    "target_rowid".to_string(),
                ]);
                if expand.has_path_functions {
                    names.extend([
                        "path_length".to_string(),
                        "path_vertices".to_string(),
                        "path_edges".to_string(),
                    ]);
                }
                outputs.push(names);
            }
        }
    }

    assert_eq!(
        outputs.len(),
        1,
        "logical output name derivation must produce exactly one result"
    );
    outputs.pop().expect("root output names were checked")
}

fn schedule_join_names<'a>(join: &'a Join, tasks: &mut Vec<OutputNamesTask<'a>>) {
    tasks.push(OutputNamesTask::FinishJoin(join));
    match join.join_type() {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => {
            tasks.push(OutputNamesTask::Derive(&join.left().operator));
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            tasks.push(OutputNamesTask::Derive(&join.right().operator));
        }
        JoinType::Invalid
        | JoinType::Left
        | JoinType::Right
        | JoinType::Inner
        | JoinType::Outer
        | JoinType::Single => {
            tasks.push(OutputNamesTask::Derive(&join.right().operator));
            tasks.push(OutputNamesTask::Derive(&join.left().operator));
        }
    }
}

fn finish_join_names(join: &Join, outputs: &mut Vec<Vec<String>>) -> Vec<String> {
    let (left_projection, right_projection) = match join {
        Join::Comparison(join) => (
            Some(&join.left_projection_map),
            Some(&join.right_projection_map),
        ),
        Join::Any(join) => (
            Some(&join.left_projection_map),
            Some(&join.right_projection_map),
        ),
        Join::Cross(_) => (None, None),
    };

    match join.join_type() {
        JoinType::Semi | JoinType::Anti => project_output_names(
            pop_output_names(outputs),
            left_projection.expect("projected join"),
        ),
        JoinType::Mark => {
            let mut left = project_output_names(
                pop_output_names(outputs),
                left_projection.expect("projected join"),
            );
            left.push("mark".to_string());
            left
        }
        JoinType::RightSemi | JoinType::RightAnti => project_output_names(
            pop_output_names(outputs),
            right_projection.expect("projected join"),
        ),
        JoinType::Invalid
        | JoinType::Left
        | JoinType::Right
        | JoinType::Inner
        | JoinType::Outer
        | JoinType::Single => {
            let right = match right_projection {
                Some(projection) => project_output_names(pop_output_names(outputs), projection),
                None => pop_output_names(outputs),
            };
            let mut left = match left_projection {
                Some(projection) => project_output_names(pop_output_names(outputs), projection),
                None => pop_output_names(outputs),
            };
            left.extend(right);
            left
        }
    }
}

fn project_output_names(names: Vec<String>, projection: &ProjectionMap) -> Vec<String> {
    let Some(indices) = projection.as_columns() else {
        return names;
    };
    indices
        .iter()
        .filter_map(|&index| names.get(index).cloned())
        .collect()
}

fn pop_output_names(outputs: &mut Vec<Vec<String>>) -> Vec<String> {
    outputs
        .pop()
        .expect("logical output name derivation lost a child result")
}

#[derive(Clone, Copy)]
enum OutputLayoutChildren {
    None,
    First,
    Second,
    Both,
}

enum OutputLayoutTask<'a> {
    Derive(&'a LogicalOperator),
    Finish(&'a LogicalOperator, OutputLayoutChildren),
}

impl LogicalOperator {
    /// Derive this operator's execution layout from already-completed child
    /// layouts in the same order as [`Self::children`].
    ///
    /// Post-order optimizer passes should use this local reducer instead of
    /// starting a fresh subtree traversal at every node.
    pub fn output_layout_from_children(
        &self,
        child_layouts: &[LogicalOutputLayout],
    ) -> LogicalOutputLayout {
        debug_assert_eq!(child_layouts.len(), self.children().len());
        let (first, second) = match output_layout_children(self) {
            OutputLayoutChildren::None => (None, None),
            OutputLayoutChildren::First => (child_layouts.first().cloned(), None),
            OutputLayoutChildren::Second => (None, child_layouts.get(1).cloned()),
            OutputLayoutChildren::Both => (
                child_layouts.first().cloned(),
                child_layouts.get(1).cloned(),
            ),
        };
        derive_local_output_layout(self, first, second)
    }
}

fn derive_output_layout(root: &LogicalOperator) -> LogicalOutputLayout {
    use OutputLayoutChildren::*;

    let mut tasks = vec![OutputLayoutTask::Derive(root)];
    let mut layouts = Vec::new();
    while let Some(task) = tasks.pop() {
        match task {
            OutputLayoutTask::Derive(operator) => {
                let dependencies = output_layout_children(operator);
                if matches!(dependencies, None) {
                    layouts.push(derive_local_output_layout(
                        operator,
                        Option::None,
                        Option::None,
                    ));
                    continue;
                }

                let children = operator.children();
                tasks.push(OutputLayoutTask::Finish(operator, dependencies));
                match dependencies {
                    None => unreachable!("dependency-free layouts finish immediately"),
                    First => {
                        tasks.push(OutputLayoutTask::Derive(&children[0].operator));
                    }
                    Second => {
                        tasks.push(OutputLayoutTask::Derive(&children[1].operator));
                    }
                    Both => {
                        tasks.push(OutputLayoutTask::Derive(&children[1].operator));
                        tasks.push(OutputLayoutTask::Derive(&children[0].operator));
                    }
                }
            }
            OutputLayoutTask::Finish(operator, dependencies) => {
                let (first, second) = match dependencies {
                    None => unreachable!("dependency-free layouts finish immediately"),
                    First => (Some(pop_output_layout(&mut layouts)), Option::None),
                    Second => (Option::None, Some(pop_output_layout(&mut layouts))),
                    Both => {
                        let second = pop_output_layout(&mut layouts);
                        let first = pop_output_layout(&mut layouts);
                        (Some(first), Some(second))
                    }
                };
                layouts.push(derive_local_output_layout(operator, first, second));
            }
        }
    }

    assert_eq!(
        layouts.len(),
        1,
        "logical output derivation must produce exactly one root layout"
    );
    layouts.pop().expect("root output layout was checked")
}

fn output_layout_children(operator: &LogicalOperator) -> OutputLayoutChildren {
    use OutputLayoutChildren::*;

    match operator {
        LogicalOperator::Filter(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_)
        | LogicalOperator::EmptyResult(_)
        | LogicalOperator::GraphExpand(_) => First,
        LogicalOperator::MaterializedCTE(_) => Second,
        LogicalOperator::Join(join) => match join.join_type() {
            JoinType::Semi | JoinType::Anti | JoinType::Mark => First,
            JoinType::RightSemi | JoinType::RightAnti => Second,
            JoinType::Invalid
            | JoinType::Left
            | JoinType::Right
            | JoinType::Inner
            | JoinType::Outer
            | JoinType::Single => Both,
        },
        LogicalOperator::DependentJoin(join) => match &join.kind {
            DependentJoinKind::Mark { .. } => First,
            DependentJoinKind::Scalar { .. } | DependentJoinKind::Lateral { .. } => Both,
        },
        LogicalOperator::Get(_)
        | LogicalOperator::BoundReference(_)
        | LogicalOperator::Projection(_)
        | LogicalOperator::ExternalTable(_)
        | LogicalOperator::CreateTable(_)
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
        | LogicalOperator::Aggregate(_)
        | LogicalOperator::Insert(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Update(_)
        | LogicalOperator::ExpressionGet(_)
        | LogicalOperator::DelimGet(_)
        | LogicalOperator::SetOperation(_)
        | LogicalOperator::Explain(_)
        | LogicalOperator::RecursiveCTE(_)
        | LogicalOperator::CTERef(_)
        | LogicalOperator::TableFunctionGet(_)
        | LogicalOperator::SearchScan(_)
        | LogicalOperator::FullTextFilterScan(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::GraphMatch(_)
        | LogicalOperator::GraphScan(_)
        | LogicalOperator::DummyScan => None,
    }
}

fn derive_local_output_layout(
    operator: &LogicalOperator,
    first: Option<LogicalOutputLayout>,
    second: Option<LogicalOutputLayout>,
) -> LogicalOutputLayout {
    match operator {
        LogicalOperator::Get(get) => {
            LogicalOutputLayout::for_table(get.table_index, get.returned_types.clone())
        }
        LogicalOperator::BoundReference(reference) => {
            LogicalOutputLayout::new(reference.types.clone(), reference.bindings.clone())
        }
        LogicalOperator::Filter(filter) => {
            required_output_layout(first, "filter child").project(&filter.projection_map)
        }
        LogicalOperator::Projection(projection) => LogicalOutputLayout::new(
            projection.returned_types.clone(),
            LogicalOperator::generate_column_bindings(
                projection.table_index,
                projection.expressions.len(),
            ),
        ),
        LogicalOperator::RowFetch(fetch) => {
            let mut layout = required_output_layout(first, "row fetch child");
            for source in &fetch.sources {
                for &ordinal in &source.needed_columns {
                    let Some(column) = source.table.columns.get(ordinal) else {
                        continue;
                    };
                    layout.push(
                        column.logical_type.clone(),
                        ColumnBinding::new(source.materialized_table_index, ordinal),
                    );
                }
            }
            layout
        }
        LogicalOperator::ExternalProject(project) => {
            let mut layout = required_output_layout(first, "external project child");
            let child_width = layout.len();
            for (index, expression) in project.expressions.iter().enumerate() {
                layout.push(
                    expression.expression.return_type(),
                    ColumnBinding::new(project.project_index, child_width + index),
                );
            }
            layout
        }
        LogicalOperator::ExternalTable(table) => {
            LogicalOutputLayout::for_table(table.table_index, table.returned_types.clone())
        }
        LogicalOperator::Limit(_) | LogicalOperator::Distinct(_) => {
            required_output_layout(first, "pass-through child")
        }
        LogicalOperator::TopN(topn) => {
            required_output_layout(first, "topn child").project(&topn.projection_map)
        }
        LogicalOperator::Order(order) => {
            required_output_layout(first, "order child").project(&order.projection_map)
        }
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
        | LogicalOperator::DummyScan => LogicalOutputLayout::default(),
        LogicalOperator::Aggregate(aggregate) => LogicalOutputLayout::new(
            aggregate.returned_types.clone(),
            aggregate.get_column_bindings(),
        ),
        LogicalOperator::Insert(_) | LogicalOperator::Delete(_) | LogicalOperator::Update(_) => {
            LogicalOutputLayout::new(vec![LogicalType::BigInt], vec![ColumnBinding::new(0, 0)])
        }
        LogicalOperator::ExpressionGet(values) => {
            LogicalOutputLayout::for_table(values.table_index, values.types.clone())
        }
        LogicalOperator::Join(join) => finish_join_layout(join, first, second),
        LogicalOperator::DelimGet(delim) => {
            LogicalOutputLayout::for_table(delim.table_index, delim.chunk_types.clone())
        }
        LogicalOperator::DependentJoin(join) => {
            let mut left = required_output_layout(first, "dependent join left child");
            match &join.kind {
                DependentJoinKind::Mark { mark_index, .. } => {
                    left.push(LogicalType::Boolean, ColumnBinding::new(*mark_index, 0));
                }
                DependentJoinKind::Scalar { .. } | DependentJoinKind::Lateral { .. } => {
                    left.append(required_output_layout(second, "dependent join right child"));
                }
            }
            left
        }
        LogicalOperator::SetOperation(setop) => {
            debug_assert_eq!(setop.column_count, setop.types.len());
            LogicalOutputLayout::for_table(setop.table_index, setop.types.clone())
        }
        LogicalOperator::Window(window) => {
            let mut layout = required_output_layout(first, "window child");
            for (index, expression) in window.expressions.iter().enumerate() {
                layout.push(
                    expression.return_type(),
                    ColumnBinding::new(window.window_index, index),
                );
            }
            layout
        }
        LogicalOperator::Explain(_) => {
            LogicalOutputLayout::for_table(0, vec![LogicalType::Varchar])
        }
        LogicalOperator::EmptyResult(_) => required_output_layout(first, "empty-result child"),
        LogicalOperator::MaterializedCTE(_) => {
            required_output_layout(second, "materialized CTE consumer")
        }
        LogicalOperator::RecursiveCTE(cte) => {
            LogicalOutputLayout::for_table(cte.cte_index, cte.column_types.clone())
        }
        LogicalOperator::CTERef(cte) => {
            LogicalOutputLayout::for_table(cte.table_index, cte.column_types.clone())
        }
        LogicalOperator::TableFunctionGet(function) => {
            let ordinals = function
                .projection_ids
                .clone()
                .unwrap_or_else(|| (0..function.column_types.len()).collect::<Vec<_>>());
            let mut types = Vec::with_capacity(ordinals.len());
            let mut bindings = Vec::with_capacity(ordinals.len());
            for ordinal in ordinals {
                let Some(logical_type) = function.column_types.get(ordinal) else {
                    continue;
                };
                types.push(logical_type.clone());
                bindings.push(ColumnBinding::new(function.table_index, ordinal));
            }
            LogicalOutputLayout::new(types, bindings)
        }
        LogicalOperator::SearchScan(search) => LogicalOutputLayout::for_table(
            search.projection_table_index,
            search
                .projections
                .iter()
                .map(|expression| expression.return_type())
                .collect(),
        ),
        LogicalOperator::FullTextFilterScan(scan) => {
            LogicalOutputLayout::for_table(scan.get.table_index, scan.get.returned_types.clone())
                .project(&scan.projection_map)
        }
        LogicalOperator::CopyTo(copy) => LogicalOutputLayout::for_table(0, copy.types.clone()),
        LogicalOperator::GraphMatch(graph) => {
            LogicalOutputLayout::for_table(graph.table_index, graph.output_types.clone())
        }
        LogicalOperator::GraphScan(graph) => {
            LogicalOutputLayout::for_table(graph.output_table_index, graph.output_types.clone())
        }
        LogicalOperator::GraphExpand(expand) => {
            let mut types = required_output_layout(first, "graph expand child").into_types();
            types.extend([
                LogicalType::UBigInt,
                LogicalType::UBigInt,
                LogicalType::UBigInt,
            ]);
            if expand.has_path_functions {
                types.extend([
                    LogicalType::BigInt,
                    super::graph_expand::graph_path_element_list_type(),
                    super::graph_expand::graph_path_element_list_type(),
                ]);
            }
            LogicalOutputLayout::for_table(expand.output_table_index, types)
        }
    }
}

fn finish_join_layout(
    join: &Join,
    left: Option<LogicalOutputLayout>,
    right: Option<LogicalOutputLayout>,
) -> LogicalOutputLayout {
    let (left_projection, right_projection, mark_index) = match join {
        Join::Comparison(join) => (
            Some(&join.left_projection_map),
            Some(&join.right_projection_map),
            join.mark_index,
        ),
        Join::Any(join) => (
            Some(&join.left_projection_map),
            Some(&join.right_projection_map),
            join.mark_index,
        ),
        Join::Cross(_) => (None, None, None),
    };

    match join.join_type() {
        JoinType::Semi | JoinType::Anti => required_output_layout(left, "join left child")
            .project(left_projection.expect("projected join")),
        JoinType::Mark => {
            let mut left = required_output_layout(left, "mark join left child")
                .project(left_projection.expect("projected join"));
            left.push(
                LogicalType::Boolean,
                ColumnBinding::new(mark_index.unwrap_or(0), 0),
            );
            left
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            required_output_layout(right, "join right child")
                .project(right_projection.expect("projected join"))
        }
        JoinType::Invalid
        | JoinType::Left
        | JoinType::Right
        | JoinType::Inner
        | JoinType::Outer
        | JoinType::Single => {
            let right = match right_projection {
                Some(projection) => {
                    required_output_layout(right, "join right child").project(projection)
                }
                None => required_output_layout(right, "cross join right child"),
            };
            let mut left = match left_projection {
                Some(projection) => {
                    required_output_layout(left, "join left child").project(projection)
                }
                None => required_output_layout(left, "cross join left child"),
            };
            left.append(right);
            left
        }
    }
}

fn required_output_layout(layout: Option<LogicalOutputLayout>, role: &str) -> LogicalOutputLayout {
    layout.unwrap_or_else(|| panic!("logical output derivation lost {role}"))
}

fn pop_output_layout(layouts: &mut Vec<LogicalOutputLayout>) -> LogicalOutputLayout {
    layouts
        .pop()
        .expect("logical output derivation lost a completed child")
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
        DenseVectorQuery, HnswIntent, NormalizedSearchRequest, ProjectionSpec, SearchIndexKind,
        SearchIntent, SearchRequestMode,
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
            .expect("copy function")
            .copy_to
            .expect("COPY TO function");
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
                        None,
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
                    query: DenseVectorQuery::Literal(vec![0.1, 0.2]),
                    distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
                    options: Default::default(),
                })],
                fusion: None,
            },
            SearchDecision::IndexScan {
                candidate: SearchCandidate {
                    intent: SearchIntent::Hnsw(HnswIntent {
                        column_id: 1,
                        query: DenseVectorQuery::Literal(vec![0.1, 0.2]),
                        distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
                        options: Default::default(),
                    }),
                    token: paro_storage::search::CapabilityToken {
                        definition_id: 1,
                        generation_id: 1,
                        root_version: 1,
                        capability_state: paro_storage::search::SearchCapabilityState::Queryable,
                    },
                    kind: SearchIndexKind::Hnsw,
                    estimated_cost: None,
                    exact_filter_materialization: None,
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
            Some(0),
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
