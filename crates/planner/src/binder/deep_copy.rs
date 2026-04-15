//! Binder-owned logical plan deep-copy and binding remap helpers.

use std::collections::HashMap;

use crate::binder::context::BindShared;
use crate::binder::CorrelatedColumnInfo;
use crate::expression::{ColumnRefExpression, Expression, SubqueryExpression};
use crate::operator::{
    Aggregate as AggNode, AnyJoin, CTERef, ColumnBinding, ComparisonJoin, CopyTo as CopyToNode,
    CrossProduct, Delete as DelNode, DelimGet as DelimGetNode, DependentJoin as DepJoinNode,
    Distinct as DistNode, EmptyResult as EmptyResNode, Explain as ExplNode,
    ExpressionGet as ExprGetNode, Filter as FilterNode, FullTextFilterScan as FtScanNode,
    GraphExpand as GExpNode, GraphMatch as GMNode, GraphScan as GSNode, Insert as InsNode,
    Join as JoinOp, Limit as LimNode, LogicalOperator, MaterializedCTE as MatCteNode,
    Order as OrdNode, Projection as ProjNode, RecursiveCTE as RecCteNode,
    SearchScan as SearchScanNode, SetOperation as SetOpNode, TableFunctionGet as TblFnGetNode,
    TopN as TopNNode, Update as UpdNode, Window as WinNode,
};
use crate::plan::{LogicalPlan, NodeStats, PlannedStatement};
use crate::visitor::LogicalOperatorVisitor;

/// Deep-copy a logical plan root while remapping all logical indices owned by
/// the embedded operator tree and clearing statistics on the copy.
pub fn deep_copy_plan(plan: &LogicalPlan, bind_shared: &BindShared) -> LogicalPlan {
    let mut copier = LogicalPlanDeepCopy::new_deep();
    copier.deep_copy(bind_shared, plan)
}

/// Deep-copy an operator tree (no outer [`LogicalPlan`] wrapper). Nested [`LogicalPlan`] nodes
/// receive fresh plan ids; table / CTE indices are remapped like [`deep_copy_plan`].
pub fn deep_copy_operator(op: &LogicalOperator, bind_shared: &BindShared) -> LogicalOperator {
    let mut copier = LogicalPlanDeepCopy::new_deep();
    copier.deep_copy_operator_root(bind_shared, op)
}

pub(crate) fn deep_copy_plan_shallow_subqueries(
    plan: &LogicalPlan,
    bind_shared: &BindShared,
) -> LogicalPlan {
    let mut copier = LogicalPlanDeepCopy::new_shallow_subqueries();
    copier.deep_copy(bind_shared, plan)
}

pub(crate) fn deep_copy_operator_shallow_subqueries(
    op: &LogicalOperator,
    bind_shared: &BindShared,
) -> LogicalOperator {
    let mut copier = LogicalPlanDeepCopy::new_shallow_subqueries();
    copier.deep_copy_operator_root(bind_shared, op)
}

/// Deep structural duplicate of a plan while keeping table / CTE indices and expression bindings
/// aligned with the original tree (optimizer snapshots, join-order extraction).
pub fn duplicate_plan_preserving_indices(
    plan: &LogicalPlan,
    bind_shared: &BindShared,
) -> LogicalPlan {
    let mut copier = LogicalPlanDeepCopy::new_preserve_indices();
    copier.duplicate_plan_preserve(bind_shared, plan)
}

/// Like [`duplicate_plan_preserving_indices`] but copies only the operator tree (no outer wrapper).
pub fn duplicate_operator_preserving_indices(
    op: &LogicalOperator,
    bind_shared: &BindShared,
) -> LogicalOperator {
    let mut copier = LogicalPlanDeepCopy::new_preserve_indices();
    copier.table_index_map.clear();
    copier.cte_index_map.clear();
    copier.copy_operator(op, bind_shared)
}

#[derive(Debug)]
struct LogicalPlanDeepCopy {
    table_index_map: HashMap<usize, usize>,
    cte_index_map: HashMap<usize, usize>,
    preserve_logical_indices: bool,
    nested_subquery_copy_mode: NestedSubqueryCopyMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NestedSubqueryCopyMode {
    Deep,
    Shallow,
}

impl LogicalPlanDeepCopy {
    fn new_deep() -> Self {
        Self {
            table_index_map: HashMap::new(),
            cte_index_map: HashMap::new(),
            preserve_logical_indices: false,
            nested_subquery_copy_mode: NestedSubqueryCopyMode::Deep,
        }
    }

    fn new_preserve_indices() -> Self {
        Self {
            table_index_map: HashMap::new(),
            cte_index_map: HashMap::new(),
            preserve_logical_indices: true,
            nested_subquery_copy_mode: NestedSubqueryCopyMode::Deep,
        }
    }

    fn new_shallow_subqueries() -> Self {
        Self {
            table_index_map: HashMap::new(),
            cte_index_map: HashMap::new(),
            preserve_logical_indices: false,
            nested_subquery_copy_mode: NestedSubqueryCopyMode::Shallow,
        }
    }

    fn duplicate_plan_preserve(
        &mut self,
        bind_shared: &BindShared,
        plan: &LogicalPlan,
    ) -> LogicalPlan {
        self.table_index_map.clear();
        self.cte_index_map.clear();
        let operator = self.copy_operator(&plan.operator, bind_shared);
        LogicalPlan {
            id: bind_shared.next_plan_id(),
            stats: plan.stats.clone(),
            operator,
        }
    }

    fn deep_copy(&mut self, bind_shared: &BindShared, plan: &LogicalPlan) -> LogicalPlan {
        self.table_index_map.clear();
        self.cte_index_map.clear();
        let mut out = self.copy_plan(plan, bind_shared);
        let mut replacer = DeepCopyBindingRewriter {
            table_index_map: self.table_index_map.clone(),
            cte_index_map: self.cte_index_map.clone(),
            nested_subquery_copy_mode: self.nested_subquery_copy_mode,
        };
        replacer.visit_operator(&mut out.operator);
        out
    }

    fn deep_copy_operator_root(
        &mut self,
        bind_shared: &BindShared,
        op: &LogicalOperator,
    ) -> LogicalOperator {
        self.table_index_map.clear();
        self.cte_index_map.clear();
        let mut out = self.copy_operator(op, bind_shared);
        let mut replacer = DeepCopyBindingRewriter {
            table_index_map: self.table_index_map.clone(),
            cte_index_map: self.cte_index_map.clone(),
            nested_subquery_copy_mode: self.nested_subquery_copy_mode,
        };
        replacer.visit_operator(&mut out);
        out
    }

    fn copy_plan(&mut self, plan: &LogicalPlan, bind_shared: &BindShared) -> LogicalPlan {
        let operator = self.copy_operator(&plan.operator, bind_shared);
        LogicalPlan {
            id: bind_shared.next_plan_id(),
            stats: NodeStats::default(),
            operator,
        }
    }

    fn next_table_index(&self, bind_shared: &BindShared) -> usize {
        bind_shared.generate_table_index()
    }

    fn remap_table_index(&mut self, bind_shared: &BindShared, index: &mut usize) {
        if self.preserve_logical_indices {
            return;
        }
        let old = *index;
        let new = match self.table_index_map.get(&old) {
            Some(existing) => *existing,
            None => {
                let fresh = self.next_table_index(bind_shared);
                self.table_index_map.insert(old, fresh);
                fresh
            }
        };
        *index = new;
    }

    fn remap_cte_index(&mut self, bind_shared: &BindShared, index: &mut usize) {
        if self.preserve_logical_indices {
            return;
        }
        let old = *index;
        let new = match self.cte_index_map.get(&old) {
            Some(existing) => *existing,
            None => {
                let fresh = self.next_table_index(bind_shared);
                self.cte_index_map.insert(old, fresh);
                fresh
            }
        };
        *index = new;
    }

    fn copy_operator(&mut self, op: &LogicalOperator, bind_shared: &BindShared) -> LogicalOperator {
        match op {
            LogicalOperator::Get(get) => {
                let mut g = get.clone();
                self.remap_table_index(bind_shared, &mut g.table_index);
                LogicalOperator::Get(g)
            }
            LogicalOperator::Filter(f) => {
                let child = self.copy_plan(f.child.as_ref(), bind_shared);
                LogicalOperator::Filter(FilterNode {
                    expressions: f.expressions.clone(),
                    projection_map: f.projection_map.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::Projection(p) => {
                let child = self.copy_plan(p.child.as_ref(), bind_shared);
                let mut table_index = p.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::Projection(ProjNode {
                    table_index,
                    expressions: p.expressions.clone(),
                    output_names: p.output_names.clone(),
                    returned_types: p.returned_types.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::Limit(l) => {
                let child = self.copy_plan(l.child.as_ref(), bind_shared);
                LogicalOperator::Limit(LimNode {
                    limit: l.limit.clone(),
                    offset: l.offset.clone(),
                    hnsw_ef_hint: l.hnsw_ef_hint,
                    child: Box::new(child),
                })
            }
            LogicalOperator::Order(o) => {
                let child = self.copy_plan(o.child.as_ref(), bind_shared);
                LogicalOperator::Order(OrdNode {
                    orders: o.orders.clone(),
                    projection_map: o.projection_map.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::TopN(t) => {
                let child = self.copy_plan(t.child.as_ref(), bind_shared);
                LogicalOperator::TopN(TopNNode {
                    orders: t.orders.clone(),
                    limit: t.limit,
                    offset: t.offset,
                    hnsw_ef_hint: t.hnsw_ef_hint,
                    child: Box::new(child),
                })
            }
            LogicalOperator::Alter(c) => LogicalOperator::Alter(c.clone()),
            LogicalOperator::CreateTable(c) => LogicalOperator::CreateTable(c.clone()),
            LogicalOperator::CreateSequence(c) => LogicalOperator::CreateSequence(c.clone()),
            LogicalOperator::CreateSchema(c) => LogicalOperator::CreateSchema(c.clone()),
            LogicalOperator::CreateIndex(c) => LogicalOperator::CreateIndex(c.clone()),
            LogicalOperator::CreateView(c) => LogicalOperator::CreateView(c.clone()),
            LogicalOperator::Drop(d) => LogicalOperator::Drop(d.clone()),
            LogicalOperator::CreatePropertyGraph(c) => {
                LogicalOperator::CreatePropertyGraph(c.clone())
            }
            LogicalOperator::DropPropertyGraph(d) => LogicalOperator::DropPropertyGraph(d.clone()),
            LogicalOperator::RefreshPropertyGraph(r) => {
                LogicalOperator::RefreshPropertyGraph(r.clone())
            }
            LogicalOperator::Aggregate(a) => {
                let child = self.copy_plan(a.child.as_ref(), bind_shared);
                let mut group_index = a.group_index;
                let mut aggregate_index = a.aggregate_index;
                let mut groupings_index = a.groupings_index;
                self.remap_table_index(bind_shared, &mut group_index);
                self.remap_table_index(bind_shared, &mut aggregate_index);
                self.remap_table_index(bind_shared, &mut groupings_index);
                LogicalOperator::Aggregate(AggNode {
                    group_index,
                    aggregate_index,
                    groupings_index,
                    child: Box::new(child),
                    groups: a.groups.clone(),
                    grouping_sets: a.grouping_sets.clone(),
                    aggregates: a.aggregates.clone(),
                    group_stats: a.group_stats.clone(),
                    returned_types: a.returned_types.clone(),
                    grouping_functions: a.grouping_functions.clone(),
                })
            }
            LogicalOperator::Insert(i) => {
                let child = self.copy_plan(i.child.as_ref(), bind_shared);
                LogicalOperator::Insert(InsNode {
                    table: i.table.clone(),
                    column_index_map: i.column_index_map.clone(),
                    expected_types: i.expected_types.clone(),
                    on_conflict: i.on_conflict.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::Delete(d) => {
                let child = self.copy_plan(d.child.as_ref(), bind_shared);
                LogicalOperator::Delete(DelNode {
                    table: d.table.clone(),
                    table_index: d.table_index,
                    return_chunk: d.return_chunk,
                    is_full_table_delete: d.is_full_table_delete,
                    child: Box::new(child),
                })
            }
            LogicalOperator::Update(u) => {
                let child = self.copy_plan(u.child.as_ref(), bind_shared);
                LogicalOperator::Update(UpdNode {
                    table: u.table.clone(),
                    table_index: u.table_index,
                    return_chunk: u.return_chunk,
                    columns: u.columns.clone(),
                    expressions: u.expressions.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::ExpressionGet(eg) => {
                let mut table_index = eg.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::ExpressionGet(ExprGetNode {
                    table_index,
                    expressions: eg.expressions.clone(),
                    names: eg.names.clone(),
                    types: eg.types.clone(),
                })
            }
            LogicalOperator::DelimGet(dg) => {
                let mut table_index = dg.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::DelimGet(DelimGetNode {
                    table_index,
                    chunk_types: dg.chunk_types.clone(),
                })
            }
            LogicalOperator::Join(join) => LogicalOperator::Join(match join {
                JoinOp::Comparison(cj) => {
                    let left = self.copy_plan(cj.left.as_ref(), bind_shared);
                    let right = self.copy_plan(cj.right.as_ref(), bind_shared);
                    let mut mark_index = cj.mark_index;
                    if let Some(ref mut m) = mark_index {
                        self.remap_table_index(bind_shared, m);
                    }
                    JoinOp::Comparison(ComparisonJoin {
                        join_type: cj.join_type,
                        left: Box::new(left),
                        right: Box::new(right),
                        conditions: cj.conditions.clone(),
                        mark_index,
                        duplicate_eliminated_columns: cj.duplicate_eliminated_columns.clone(),
                        delim_flipped: cj.delim_flipped,
                        left_projection_map: cj.left_projection_map.clone(),
                        right_projection_map: cj.right_projection_map.clone(),
                    })
                }
                JoinOp::Any(aj) => {
                    let left = self.copy_plan(aj.left.as_ref(), bind_shared);
                    let right = self.copy_plan(aj.right.as_ref(), bind_shared);
                    let mut mark_index = aj.mark_index;
                    if let Some(ref mut m) = mark_index {
                        self.remap_table_index(bind_shared, m);
                    }
                    JoinOp::Any(Box::new(AnyJoin {
                        join_type: aj.join_type,
                        left: Box::new(left),
                        right: Box::new(right),
                        condition: aj.condition.clone(),
                        mark_index,
                        left_projection_map: aj.left_projection_map.clone(),
                        right_projection_map: aj.right_projection_map.clone(),
                    }))
                }
                JoinOp::Cross(cp) => {
                    let left = self.copy_plan(cp.left.as_ref(), bind_shared);
                    let right = self.copy_plan(cp.right.as_ref(), bind_shared);
                    JoinOp::Cross(CrossProduct {
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                }
            }),
            LogicalOperator::DependentJoin(dj) => {
                let left = self.copy_plan(dj.left.as_ref(), bind_shared);
                let right = self.copy_plan(dj.right.as_ref(), bind_shared);
                let mut kind = dj.kind.clone();
                if let crate::operator::DependentJoinKind::Mark { mark_index, .. } = &mut kind {
                    self.remap_table_index(bind_shared, mark_index);
                }
                LogicalOperator::DependentJoin(DepJoinNode {
                    left: Box::new(left),
                    right: Box::new(right),
                    correlated_columns: dj.correlated_columns.clone(),
                    kind,
                })
            }
            LogicalOperator::SetOperation(s) => {
                let left = self.copy_plan(s.left.as_ref(), bind_shared);
                let right = self.copy_plan(s.right.as_ref(), bind_shared);
                let mut table_index = s.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::SetOperation(SetOpNode {
                    table_index,
                    column_count: s.column_count,
                    left: Box::new(left),
                    right: Box::new(right),
                    setop_type: s.setop_type,
                    setop_all: s.setop_all,
                    allow_out_of_order: s.allow_out_of_order,
                    types: s.types.clone(),
                })
            }
            LogicalOperator::Distinct(d) => {
                let child = self.copy_plan(d.child.as_ref(), bind_shared);
                LogicalOperator::Distinct(DistNode {
                    distinct_type: d.distinct_type,
                    distinct_targets: d.distinct_targets.clone(),
                    order_by: d.order_by.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::Window(w) => {
                let child = self.copy_plan(w.child.as_ref(), bind_shared);
                let mut window_index = w.window_index;
                self.remap_table_index(bind_shared, &mut window_index);
                LogicalOperator::Window(WinNode {
                    window_index,
                    expressions: w.expressions.clone(),
                    child: Box::new(child),
                })
            }
            LogicalOperator::Explain(e) => {
                let child = self.copy_plan(e.child.as_ref(), bind_shared);
                LogicalOperator::Explain(ExplNode {
                    child: Box::new(child),
                    spec: e.spec,
                    logical_plan_unopt: e.logical_plan_unopt.clone(),
                    logical_plan_opt: e.logical_plan_opt.clone(),
                })
            }
            LogicalOperator::EmptyResult(e) => {
                let child = self.copy_plan(e.child.as_ref(), bind_shared);
                LogicalOperator::EmptyResult(EmptyResNode {
                    child: Box::new(child),
                })
            }
            LogicalOperator::MaterializedCTE(cte) => {
                let cte_query = self.copy_plan(cte.cte_query.as_ref(), bind_shared);
                let child = self.copy_plan(cte.child.as_ref(), bind_shared);
                let mut cte_index = cte.cte_index;
                self.remap_cte_index(bind_shared, &mut cte_index);
                LogicalOperator::MaterializedCTE(MatCteNode {
                    cte_index,
                    cte_name: cte.cte_name.clone(),
                    column_names: cte.column_names.clone(),
                    column_types: cte.column_types.clone(),
                    materialized: cte.materialized,
                    ref_count: cte.ref_count,
                    cte_query: Box::new(cte_query),
                    child: Box::new(child),
                })
            }
            LogicalOperator::RecursiveCTE(cte) => {
                let anchor = self.copy_plan(cte.anchor.as_ref(), bind_shared);
                let recursive = self.copy_plan(cte.recursive.as_ref(), bind_shared);
                let mut cte_index = cte.cte_index;
                self.remap_cte_index(bind_shared, &mut cte_index);
                LogicalOperator::RecursiveCTE(RecCteNode {
                    cte_index,
                    cte_name: cte.cte_name.clone(),
                    column_names: cte.column_names.clone(),
                    column_types: cte.column_types.clone(),
                    union_all: cte.union_all,
                    anchor: Box::new(anchor),
                    recursive: Box::new(recursive),
                })
            }
            LogicalOperator::CTERef(c) => {
                let mut table_index = c.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::CTERef(CTERef {
                    cte_index: c.cte_index,
                    table_index,
                    column_names: c.column_names.clone(),
                    column_types: c.column_types.clone(),
                })
            }
            LogicalOperator::TableFunctionGet(t) => {
                let mut table_index = t.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::TableFunctionGet(TblFnGetNode {
                    function: t.function.clone(),
                    table_index,
                    column_names: t.column_names.clone(),
                    column_types: t.column_types.clone(),
                    arguments: t.arguments.clone(),
                    projection_ids: t.projection_ids.clone(),
                    input_table_types: t.input_table_types.clone(),
                    input_table_names: t.input_table_names.clone(),
                    with_ordinality: t.with_ordinality,
                })
            }
            LogicalOperator::SearchScan(s) => {
                let mut get = s.get.clone();
                self.remap_table_index(bind_shared, &mut get.table_index);
                let mut projection_table_index = s.projection_table_index;
                self.remap_table_index(bind_shared, &mut projection_table_index);
                LogicalOperator::SearchScan(SearchScanNode {
                    get,
                    decision: s.decision.clone(),
                    projections: s.projections.clone(),
                    output_names: s.output_names.clone(),
                    projection_table_index,
                    absorbed_predicates: s.absorbed_predicates.clone(),
                    residual_predicates: s.residual_predicates.clone(),
                    score_projection_index: s.score_projection_index,
                    score_expression: s.score_expression.clone(),
                    order_ascending: s.order_ascending,
                    limit: s.limit,
                })
            }
            LogicalOperator::FullTextFilterScan(s) => {
                let mut get = s.get.clone();
                self.remap_table_index(bind_shared, &mut get.table_index);
                LogicalOperator::FullTextFilterScan(FtScanNode {
                    get,
                    match_expression: s.match_expression.clone(),
                    other_predicates: s.other_predicates.clone(),
                    residual_predicates: s.residual_predicates.clone(),
                    decision: s.decision.clone(),
                })
            }
            LogicalOperator::CopyTo(c) => {
                let child = self.copy_plan(c.child.as_ref(), bind_shared);
                LogicalOperator::CopyTo(CopyToNode {
                    copy_function: c.copy_function.clone(),
                    bind_data: c.bind_data.clone(),
                    file_path: c.file_path.clone(),
                    source: c.source.clone(),
                    options: c.options.clone(),
                    child: Box::new(child),
                    names: c.names.clone(),
                    types: c.types.clone(),
                })
            }
            LogicalOperator::GraphMatch(gm) => {
                let mut table_index = gm.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::GraphMatch(GMNode {
                    graph_entry: gm.graph_entry.clone(),
                    bound_pattern: gm.bound_pattern.clone(),
                    columns: gm.columns.clone(),
                    table_index,
                    output_types: gm.output_types.clone(),
                    path_mode: gm.path_mode.clone(),
                    has_path_functions: gm.has_path_functions,
                })
            }
            LogicalOperator::GraphScan(gs) => {
                let mut table_index = gs.table_index;
                self.remap_table_index(bind_shared, &mut table_index);
                LogicalOperator::GraphScan(GSNode {
                    vertex_info: gs.vertex_info.clone(),
                    filter: gs.filter.clone(),
                    table_index,
                    label: gs.label.clone(),
                    graph_name: gs.graph_name.clone(),
                    schema_name: gs.schema_name.clone(),
                    output_types: gs.output_types.clone(),
                })
            }
            LogicalOperator::GraphExpand(ge) => {
                let child = self.copy_plan(ge.child.as_ref(), bind_shared);
                let mut source_table_index = ge.source_table_index;
                let mut edge_table_index = ge.edge_table_index;
                let mut target_table_index = ge.target_table_index;
                self.remap_table_index(bind_shared, &mut source_table_index);
                self.remap_table_index(bind_shared, &mut edge_table_index);
                self.remap_table_index(bind_shared, &mut target_table_index);
                LogicalOperator::GraphExpand(GExpNode {
                    edge_info: ge.edge_info.clone(),
                    direction: ge.direction,
                    source_label: ge.source_label.clone(),
                    edge_filter: ge.edge_filter.clone(),
                    target_filter: ge.target_filter.clone(),
                    quantifier: ge.quantifier.clone(),
                    path_mode: ge.path_mode.clone(),
                    source_table_index,
                    edge_table_index,
                    target_table_index,
                    target_label: ge.target_label.clone(),
                    source_table_oid: ge.source_table_oid,
                    target_table_oid: ge.target_table_oid,
                    target_table_name: ge.target_table_name.clone(),
                    has_path_functions: ge.has_path_functions,
                    child: Box::new(child),
                })
            }
            LogicalOperator::DummyScan => LogicalOperator::DummyScan,
        }
    }
}

struct DeepCopyBindingRewriter {
    table_index_map: HashMap<usize, usize>,
    cte_index_map: HashMap<usize, usize>,
    nested_subquery_copy_mode: NestedSubqueryCopyMode,
}

impl LogicalOperatorVisitor for DeepCopyBindingRewriter {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        match op {
            LogicalOperator::CTERef(cte_ref) => {
                if let Some(new_cte_index) = self.cte_index_map.get(&cte_ref.cte_index) {
                    cte_ref.cte_index = *new_cte_index;
                }
            }
            LogicalOperator::DependentJoin(dep) => {
                self.remap_correlated_columns(&mut dep.correlated_columns);
            }
            _ => {}
        }
        self.visit_operator_children(op);
        self.visit_operator_expressions(op);
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        if let Some(new_table_index) = self.table_index_map.get(&expr.binding.table_index) {
            expr.binding = ColumnBinding::new(*new_table_index, expr.binding.column_index);
        }
        None
    }

    fn visit_replace_subquery(&mut self, expr: &mut SubqueryExpression) -> Option<Expression> {
        self.remap_correlated_columns(&mut expr.correlated_columns);

        if self.nested_subquery_copy_mode == NestedSubqueryCopyMode::Deep {
            let mut copied_statement = PlannedStatement {
                types: expr.subquery.types.clone(),
                names: expr.subquery.names.clone(),
                plan: deep_copy_plan(&expr.subquery.plan, expr.bind_snapshot.shared().as_ref()),
            };
            self.visit_operator(&mut copied_statement.plan.operator);
            expr.subquery = std::sync::Arc::new(copied_statement);
        }

        None
    }
}

impl DeepCopyBindingRewriter {
    fn remap_correlated_columns(&self, correlated_columns: &mut [CorrelatedColumnInfo]) {
        for corr in correlated_columns {
            if let Some(new_table_index) = self.table_index_map.get(&corr.table_index) {
                corr.table_index = *new_table_index;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;

    use super::deep_copy_plan;
    use crate::binder::context::BindContext;
    use crate::binder::ir::CTEMaterialize;
    use crate::expression::{ColumnRefExpression, Expression};
    use crate::operator::{CTERef, ExpressionGet, LogicalOperator, MaterializedCTE, Projection};
    use crate::plan::{CardinalityEstimate, LogicalPlan, NodeStats, PlanNodeId};

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    #[test]
    fn deep_copy_plan_rebinds_table_indices_and_clears_stats() {
        let bind_context = BindContext::new();
        let original = LogicalPlan {
            id: PlanNodeId(99),
            stats: NodeStats {
                estimated_cardinality: Some(CardinalityEstimate::exact(123)),
            },
            operator: LogicalOperator::Projection(
                Projection::new(
                    11,
                    LogicalPlan::new(&bind_context, expression_get(7)),
                    vec![Expression::ColumnRef(ColumnRefExpression::new(
                        crate::operator::ColumnBinding::new(7, 0),
                        LogicalType::Integer,
                    ))],
                )
                .with_output_names(vec!["alias_v".to_string()]),
            ),
        };

        let copy = deep_copy_plan(&original, bind_context.shared().as_ref());

        assert_ne!(copy.id, original.id);
        assert_eq!(copy.stats, NodeStats::default());

        let LogicalOperator::Projection(proj) = copy.operator else {
            panic!("expected projection");
        };
        assert_eq!(proj.output_names, vec!["alias_v".to_string()]);
        assert_ne!(proj.table_index, 11);

        let LogicalOperator::ExpressionGet(expr_get) = &proj.child.operator else {
            panic!("expected expression get");
        };
        assert_ne!(expr_get.table_index, 7);

        let Expression::ColumnRef(column_ref) = &proj.expressions[0] else {
            panic!("expected column ref");
        };
        assert_eq!(column_ref.binding.table_index, expr_get.table_index);
    }

    #[test]
    fn deep_copy_plan_rewrites_internal_cte_refs() {
        let bind_context = BindContext::new();
        let original = LogicalPlan {
            id: PlanNodeId(7),
            stats: NodeStats::default(),
            operator: LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                4,
                "cte".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Default,
                LogicalPlan::new(&bind_context, expression_get(1)),
                LogicalPlan::new(
                    &bind_context,
                    LogicalOperator::CTERef(CTERef::new(
                        4,
                        2,
                        vec!["v".to_string()],
                        vec![LogicalType::Integer],
                    )),
                ),
            )),
        };

        let copy = deep_copy_plan(&original, bind_context.shared().as_ref());

        let LogicalOperator::MaterializedCTE(cte) = copy.operator else {
            panic!("expected materialized cte");
        };
        let LogicalOperator::CTERef(cte_ref) = &cte.child.operator else {
            panic!("expected cte ref");
        };

        assert_ne!(cte.cte_index, 4);
        assert_ne!(cte_ref.table_index, 2);
        assert_eq!(cte_ref.cte_index, cte.cte_index);
    }
}
