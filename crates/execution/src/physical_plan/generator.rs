//! Physical plan generation from logical operators.

use crate::column_binding_resolver::ColumnBindingResolver;
use crate::explain::annotated_operator::ExplainAnnotatedOperator;
use crate::explain::profiler::ExplainProfiler;
use crate::explain::types::{ExplainLogicalInfo, ExplainSchema, ExplainSearchInfo};
use crate::operator::set::cte::CteWorkingTable;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::operator::{ExplainMode, ExplainSpec, LogicalOperator};
use paro_planner::operator::{SearchDecision, SearchType};
use paro_planner::plan::LogicalPlan;
use paro_planner::verify::verify_physical_planner_invariants;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

#[derive(Clone)]
struct ExplainGenerationState {
    next_node_id: Arc<AtomicU64>,
    profiler: Option<Arc<ExplainProfiler>>,
}

#[derive(Default)]
pub(crate) struct PlanContext {
    pub(crate) cte_tables: HashMap<usize, PlannedCteTable>,
}

#[derive(Clone)]
pub(crate) struct PlannedCteTable {
    pub(crate) working_table: Arc<CteWorkingTable>,
    pub(crate) register_dependency: bool,
    pub(crate) cte_name: String,
}

/// Lowers logical plans into executable physical operators.
pub struct PhysicalPlanGenerator {
    /// Client context for catalog access.
    pub context: Arc<StatementContext>,
    explain: Option<ExplainGenerationState>,
    pub(crate) plan_context: RefCell<PlanContext>,
}

impl PhysicalPlanGenerator {
    /// Create a new PhysicalPlanGenerator.
    pub fn new(context: Arc<StatementContext>) -> Self {
        Self {
            context,
            explain: None,
            plan_context: RefCell::new(PlanContext::default()),
        }
    }

    pub(crate) fn for_explain(&self, spec: ExplainSpec) -> Self {
        Self {
            context: self.context.clone(),
            explain: Some(ExplainGenerationState {
                next_node_id: Arc::new(AtomicU64::new(1)),
                profiler: matches!(spec.mode, ExplainMode::Analyze).then(ExplainProfiler::new),
            }),
            plan_context: RefCell::new(PlanContext::default()),
        }
    }

    /// Create a physical plan from a logical plan root.
    pub fn plan(&self, logical: &mut LogicalPlan) -> Result<Arc<dyn PhysicalOperator>> {
        self.plan_context.replace(PlanContext::default());
        let started_at = Instant::now();
        debug!(
            target: targets::EXECUTOR,
            explain = self.explain.is_some(),
            "Physical plan generation started"
        );
        let plan = self.resolve_and_plan(logical)?;
        debug!(
            target: targets::EXECUTOR,
            explain = self.explain.is_some(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "Physical plan generation completed"
        );
        Ok(plan)
    }

    /// Physical planning when only a bare [`LogicalOperator`] root is available (e.g. unit tests).
    pub fn plan_operator(&self, op: &mut LogicalOperator) -> Result<Arc<dyn PhysicalOperator>> {
        self.plan_context.replace(PlanContext::default());
        let started_at = Instant::now();
        debug!(
            target: targets::EXECUTOR,
            explain = self.explain.is_some(),
            "Physical plan generation started"
        );
        verify_physical_planner_invariants(op)?;
        ColumnBindingResolver::resolve(op)?;
        let plan = self.create_plan(op)?;
        debug!(
            target: targets::EXECUTOR,
            explain = self.explain.is_some(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "Physical plan generation completed"
        );
        Ok(plan)
    }

    /// Resolve column bindings and create the physical plan.
    fn resolve_and_plan(&self, plan: &mut LogicalPlan) -> Result<Arc<dyn PhysicalOperator>> {
        verify_physical_planner_invariants(&plan.operator)?;

        // Resolve column references before lowering.
        ColumnBindingResolver::resolve(&mut plan.operator)?;

        // Create the main physical plan.
        self.plan_internal(plan)
    }

    /// Create the physical plan internally.
    fn plan_internal(&self, plan: &LogicalPlan) -> Result<Arc<dyn PhysicalOperator>> {
        self.create_plan_from_logical_plan(plan)
    }

    /// Lower a single [`LogicalPlan`] node (dispatches on `plan.operator`).
    #[inline]
    pub fn create_plan_from_logical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let physical = self.create_plan_unannotated(&plan.operator)?;
        Ok(self.annotate_logical_plan(plan, physical))
    }

    /// Create the physical plan for the given logical operator.
    ///
    /// This is the main dispatcher that converts each logical operator
    /// to its physical counterpart.
    pub fn create_plan(&self, op: &LogicalOperator) -> Result<Arc<dyn PhysicalOperator>> {
        let plan = self.create_plan_unannotated(op)?;
        Ok(self.annotate_logical_operator(op, plan))
    }

    fn create_plan_unannotated(&self, op: &LogicalOperator) -> Result<Arc<dyn PhysicalOperator>> {
        match op {
            // LOGICAL_GET
            LogicalOperator::Get(get) => self.create_plan_get(get),

            // LOGICAL_PROJECTION
            LogicalOperator::Projection(proj) => {
                // Check if this is a graph projection (Projection over GraphScan/GraphExpand)
                if proj.child.is_graph_chain() {
                    self.create_plan_graph_projection(proj)
                } else {
                    let child = self.create_plan_from_logical_plan(proj.child.as_ref())?;
                    self.create_plan_projection(proj, child)
                }
            }

            // LOGICAL_FILTER
            LogicalOperator::Filter(filter) => {
                let child = self.create_plan_from_logical_plan(filter.child.as_ref())?;
                self.create_plan_filter(filter, child)
            }

            // LOGICAL_LIMIT
            LogicalOperator::Limit(limit) => {
                let child = self.create_plan_from_logical_plan(limit.child.as_ref())?;
                self.create_plan_limit(limit, child)
            }

            // LOGICAL_ORDER_BY
            LogicalOperator::Order(order) => {
                let child = self.create_plan_from_logical_plan(order.child.as_ref())?;
                self.create_plan_order(order, child)
            }

            // LOGICAL_TOP_N
            LogicalOperator::TopN(topn) => {
                let child = self.create_plan_from_logical_plan(topn.child.as_ref())?;
                self.create_plan_topn(topn, child)
            }

            // LOGICAL_DISTINCT
            LogicalOperator::Distinct(distinct) => {
                let child = self.create_plan_from_logical_plan(distinct.child.as_ref())?;
                self.create_plan_distinct(distinct, child)
            }

            // LOGICAL_AGGREGATE_AND_GROUP_BY
            LogicalOperator::Aggregate(agg) => {
                let child = self.create_plan_from_logical_plan(agg.child.as_ref())?;
                self.create_plan_aggregate(agg, child)
            }

            // LOGICAL_INSERT
            LogicalOperator::Insert(insert) => {
                let child = self.create_plan_from_logical_plan(insert.child.as_ref())?;
                self.create_plan_insert(insert, child)
            }

            // LOGICAL_COPY_TO
            LogicalOperator::CopyTo(copy) => {
                let child = self.create_plan_from_logical_plan(copy.child.as_ref())?;
                self.create_plan_copy_to(copy, child)
            }

            // LOGICAL_EXPRESSION_GET
            LogicalOperator::ExpressionGet(expr_get) => self.create_plan_expression_get(expr_get),

            // LOGICAL_DELIM_GET
            LogicalOperator::DelimGet(delim_get) => self.create_plan_delim_get(delim_get),

            // LOGICAL_EMPTY_RESULT
            LogicalOperator::EmptyResult(empty) => self.create_plan_empty_result(empty),

            // LOGICAL_CREATE_TABLE
            LogicalOperator::CreateTable(create_table) => {
                self.create_plan_create_table(create_table)
            }

            // LOGICAL_ALTER
            LogicalOperator::Alter(alter) => self.create_plan_alter(alter),

            // LOGICAL_CREATE_SEQUENCE
            LogicalOperator::CreateSequence(create_sequence) => {
                self.create_plan_create_sequence(create_sequence)
            }

            // LOGICAL_CREATE_SCHEMA
            LogicalOperator::CreateSchema(create_schema) => {
                self.create_plan_create_schema(create_schema)
            }

            // LOGICAL_DUMMY_SCAN
            LogicalOperator::DummyScan => {
                Ok(Arc::new(crate::operator::scan::dummy_scan::PhysicalDummyScan::new())
                    as Arc<dyn PhysicalOperator>)
            }

            // LOGICAL_COMPARISON_JOIN / LOGICAL_ANY_JOIN / etc.
            LogicalOperator::Join(join) => self.create_plan_join(join),

            // LOGICAL_UNION / LOGICAL_EXCEPT / LOGICAL_INTERSECT
            LogicalOperator::SetOperation(setop) => self.create_plan_set_operation(setop),

            // LOGICAL_DROP
            LogicalOperator::Drop(drop) => self.create_plan_drop(drop),

            // LOGICAL_DELETE
            LogicalOperator::Delete(delete) => self.create_plan_delete(delete),

            // LOGICAL_UPDATE
            LogicalOperator::Update(update) => self.create_plan_update(update),

            // LOGICAL_WINDOW
            LogicalOperator::Window(window) => self.create_plan_window(window),

            // LOGICAL_MATERIALIZED_CTE
            LogicalOperator::MaterializedCTE(cte) => self.create_plan_cte(cte),

            // LOGICAL_RECURSIVE_CTE
            LogicalOperator::RecursiveCTE(cte) => self.create_plan_recursive_cte(cte),

            // LOGICAL_CTE_REF
            LogicalOperator::CTERef(cte_ref) => self.create_plan_cte_ref(cte_ref),

            // Table function (similar to LOGICAL_GET with function)
            LogicalOperator::TableFunctionGet(table_func) => {
                self.create_plan_table_function(table_func)
            }

            // LOGICAL_SEARCH_SCAN
            LogicalOperator::SearchScan(search) => self.create_plan_search_scan(search),

            // LOGICAL_FULLTEXT_FILTER_SCAN
            LogicalOperator::FullTextFilterScan(scan) => self.create_plan_fulltext_filter_scan(scan),

            // LOGICAL_DEPENDENT_JOIN
            LogicalOperator::DependentJoin(_) => Err(paro_common::error::not_implemented(
                "Physical plan for Dependent Join not yet implemented. It should be flattened by the planner.",
            )),

            // LOGICAL_EXPLAIN
            LogicalOperator::Explain(explain) => self.create_plan_explain(explain),

            // LOGICAL_CREATE_INDEX
            LogicalOperator::CreateIndex(create_index) => {
                self.create_plan_create_index(create_index)
            }

            // LOGICAL_CREATE_VIEW
            LogicalOperator::CreateView(create_view) => self.create_plan_create_view(create_view),

            // LOGICAL_CREATE_PROPERTY_GRAPH
            LogicalOperator::CreatePropertyGraph(create_pg) => {
                self.create_plan_create_property_graph(create_pg)
            }

            // LOGICAL_DROP_PROPERTY_GRAPH
            LogicalOperator::DropPropertyGraph(drop_pg) => {
                self.create_plan_drop_property_graph(drop_pg)
            }

            // LOGICAL_REFRESH_PROPERTY_GRAPH
            LogicalOperator::RefreshPropertyGraph(refresh_pg) => {
                self.create_plan_refresh_property_graph(refresh_pg)
            }

            // LOGICAL_GRAPH_MATCH — should be decomposed by optimizer before reaching here
            LogicalOperator::GraphMatch(_) => Err(paro_common::error::not_implemented(
                "GraphMatch should be decomposed into GraphScan + GraphExpand by the optimizer",
            )),

            // LOGICAL_GRAPH_SCAN
            LogicalOperator::GraphScan(scan) => self.create_plan_graph_scan(scan),

            // LOGICAL_GRAPH_EXPAND
            LogicalOperator::GraphExpand(expand) => {
                let child = self.create_graph_chain(expand.child.as_ref())?;
                self.create_plan_graph_expand(expand, child)
            }
        }
    }

    pub(crate) fn annotate_schema(
        &self,
        plan: Arc<dyn PhysicalOperator>,
        schema: ExplainSchema,
    ) -> Arc<dyn PhysicalOperator> {
        self.annotate_schema_with_logical(plan, schema, ExplainLogicalInfo::default())
    }

    pub(crate) fn annotate_schema_with_logical(
        &self,
        plan: Arc<dyn PhysicalOperator>,
        schema: ExplainSchema,
        logical: ExplainLogicalInfo,
    ) -> Arc<dyn PhysicalOperator> {
        let Some(explain) = &self.explain else {
            return plan;
        };
        if plan.explain_node_id().is_some() {
            return plan;
        }
        Arc::new(ExplainAnnotatedOperator::new(
            explain.next_node_id.fetch_add(1, Ordering::Relaxed),
            schema,
            logical,
            explain.profiler.clone(),
            plan,
        ))
    }

    pub(crate) fn passthrough_schema(
        &self,
        child: &Arc<dyn PhysicalOperator>,
        output_names: Vec<String>,
    ) -> ExplainSchema {
        let mut schema = child.explain_schema().cloned().unwrap_or_default();
        schema.output_names = output_names;
        schema
    }

    fn annotate_logical_operator(
        &self,
        op: &LogicalOperator,
        plan: Arc<dyn PhysicalOperator>,
    ) -> Arc<dyn PhysicalOperator> {
        self.annotate_schema_with_logical(
            plan,
            self.explain_schema_for_logical(op),
            self.explain_logical_info_for_operator(op),
        )
    }

    fn annotate_logical_plan(
        &self,
        plan_node: &LogicalPlan,
        plan: Arc<dyn PhysicalOperator>,
    ) -> Arc<dyn PhysicalOperator> {
        self.annotate_schema_with_logical(
            plan,
            self.explain_schema_for_logical(&plan_node.operator),
            self.explain_logical_info_for_plan(plan_node),
        )
    }

    fn explain_schema_for_logical(&self, op: &LogicalOperator) -> ExplainSchema {
        match op {
            LogicalOperator::Get(get) => ExplainSchema {
                output_names: op.output_names(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
            LogicalOperator::SearchScan(search) => ExplainSchema {
                output_names: op.output_names(),
                relation_name: search.get.relation_name.clone(),
                relation_alias: search.get.relation_alias.clone(),
            },
            LogicalOperator::FullTextFilterScan(scan) => ExplainSchema {
                output_names: op.output_names(),
                relation_name: scan.get.relation_name.clone(),
                relation_alias: scan.get.relation_alias.clone(),
            },
            _ => ExplainSchema {
                output_names: op.output_names(),
                relation_name: None,
                relation_alias: None,
            },
        }
    }

    fn explain_logical_info_for_plan(&self, plan: &LogicalPlan) -> ExplainLogicalInfo {
        let mut logical = self.explain_logical_info_for_operator(&plan.operator);
        logical.estimated_cardinality = plan.stats.estimated_cardinality;
        logical
    }

    fn explain_logical_info_for_operator(&self, op: &LogicalOperator) -> ExplainLogicalInfo {
        ExplainLogicalInfo {
            estimated_cardinality: None,
            search: match op {
                LogicalOperator::SearchScan(search) => {
                    Some(self.explain_search_info(&search.decision))
                }
                LogicalOperator::FullTextFilterScan(scan) => {
                    Some(self.explain_search_info(&scan.decision))
                }
                _ => None,
            },
        }
    }

    fn explain_search_info(&self, decision: &SearchDecision) -> ExplainSearchInfo {
        match decision {
            SearchDecision::IndexScan {
                search_type,
                estimated_cost,
                confidence,
            } => ExplainSearchInfo {
                summary: format!(
                    "INDEX_SCAN {} cost={:.3}",
                    Self::describe_search_type(search_type),
                    estimated_cost
                ),
                confidence: Some(format!("{confidence:?}").to_uppercase()),
                candidates: Vec::new(),
            },
            SearchDecision::DeferToRuntime {
                candidates,
                sequential_cost,
            } => ExplainSearchInfo {
                summary: format!(
                    "DEFER_TO_RUNTIME sequential_cost={:.3} candidates={}",
                    sequential_cost,
                    candidates.len()
                ),
                confidence: None,
                candidates: candidates
                    .iter()
                    .map(|candidate| {
                        format!(
                            "{} cost={:.3} threshold={}",
                            Self::describe_search_type(&candidate.search_type),
                            candidate.estimated_cost,
                            candidate.threshold
                        )
                    })
                    .collect(),
            },
        }
    }

    fn describe_search_type(search_type: &SearchType) -> String {
        match search_type {
            SearchType::HnswVector { column_id } => format!("HNSW(column_id={column_id})"),
            SearchType::SparseVector { column_id } => format!("SPARSE(column_id={column_id})"),
            SearchType::FullTextTopK { column_id } => {
                format!("FULLTEXT_TOPK(column_id={column_id})")
            }
            SearchType::FullTextFilter { column_id } => {
                format!("FULLTEXT_FILTER(column_id={column_id})")
            }
        }
    }

    /// Create physical plan for CreateTable.
    fn create_plan_create_table(
        &self,
        op: &paro_planner::operator::create_table::CreateTable,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(
            crate::operator::ddl::create_table::CreateTable::new(op.info.clone()),
        ))
    }

    /// Create physical plan for CreateSchema.
    fn create_plan_create_schema(
        &self,
        op: &paro_planner::operator::create_schema::CreateSchema,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(
            crate::operator::ddl::create_schema::CreateSchema::new(op.info.clone()),
        ))
    }

    /// Create physical plan for CreatePropertyGraph.
    fn create_plan_create_property_graph(
        &self,
        op: &paro_planner::operator::create_property_graph::CreatePropertyGraph,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(
            crate::operator::ddl::create_property_graph::CreatePropertyGraph::new(op.info.clone()),
        ))
    }

    /// Create physical plan for DropPropertyGraph.
    fn create_plan_drop_property_graph(
        &self,
        op: &paro_planner::operator::drop_property_graph::DropPropertyGraph,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(
            crate::operator::ddl::drop_property_graph::DropPropertyGraph::new(op.info.clone()),
        ))
    }

    /// Create physical plan for RefreshPropertyGraph.
    fn create_plan_refresh_property_graph(
        &self,
        op: &paro_planner::operator::refresh_property_graph::RefreshPropertyGraph,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(
            crate::operator::ddl::refresh_property_graph::RefreshPropertyGraph::new(
                op.info.clone(),
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::PhysicalPlanGenerator;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ComparisonType, Expression, SubqueryExpression, SubqueryPlanningState, SubqueryType,
    };
    use paro_planner::operator::{DependentJoin, ExpressionGet, LogicalOperator, Projection};
    use paro_planner::plan::LogicalPlan;
    use paro_planner::plan::PlannedStatement;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    #[test]
    fn physical_plan_generator_rejects_remaining_dependent_join() {
        let generator = PhysicalPlanGenerator::new(test_session());
        let ctx = BindContext::new();
        let mut logical = LogicalOperator::DependentJoin(DependentJoin::scalar(
            LogicalPlan::new(&ctx, expression_get(0)),
            LogicalPlan::new(&ctx, expression_get(1)),
            vec![],
        ));

        let err = generator
            .plan_operator(&mut logical)
            .expect_err("plan should fail");
        assert!(err.to_string().contains("DependentJoin"));
    }

    #[test]
    fn physical_plan_generator_rejects_remaining_subquery_expression() {
        let generator = PhysicalPlanGenerator::new(test_session());
        let ctx = BindContext::new();
        let mut logical = LogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                42,
                LogicalPlan::new(&ctx, expression_get(0)),
                vec![Expression::Subquery(SubqueryExpression {
                    subquery_type: SubqueryType::Scalar,
                    subquery: Arc::new(PlannedStatement {
                        types: vec![LogicalType::Integer],
                        names: vec!["v".to_string()],
                        plan: LogicalPlan::new(&ctx, expression_get(99)),
                    }),
                    children: vec![],
                    child_types: vec![],
                    child_targets: vec![],
                    comparison_type: ComparisonType::Equal,
                    return_type: LogicalType::Integer,
                    correlated_columns: vec![],
                    bind_snapshot: BindContext::new().snapshot(),
                    planning_state: SubqueryPlanningState::Unplanned,
                })],
            )),
        );

        let err = generator.plan(&mut logical).expect_err("plan should fail");
        assert!(err.to_string().contains("Expression::Subquery"));
    }
}
