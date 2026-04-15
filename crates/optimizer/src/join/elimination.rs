//! Conservative join elimination optimizer.
//!
//! This pass removes redundant `LEFT`/`RIGHT` comparison joins when:
//! - the eliminated side is not referenced by ancestors
//! - the eliminated side is a `Get`
//! - equality predicates cover a UNIQUE/PRIMARY KEY constraint on that side
//!
//! The implementation is intentionally narrow so we do not trade correctness
//! for extra rewrite coverage.

use std::collections::HashSet;

use paro_catalog::entry::ConstraintType;
use paro_planner::expression::{Expression, WindowExpression, WindowFrameBound};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

pub struct JoinElimination;

impl JoinElimination {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let required_bindings = output_bindings(&plan.operator);
        self.optimize_required_plan(plan, &required_bindings)
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let required_bindings = output_bindings(&plan.operator);
        self.optimize_required_plan(plan, &required_bindings)
    }

    fn optimize_required_plan(
        &mut self,
        plan: LogicalPlan,
        required_bindings: &HashSet<ColumnBinding>,
    ) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let operator = match operator {
            LogicalOperator::Filter(mut filter) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, filter.child.as_ref());
                collect_bindings_from_exprs(&filter.expressions, &mut child_required);

                let child = *filter.child;
                filter.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Projection(mut projection) => {
                let child_required =
                    projection_child_required_bindings(&projection, required_bindings);
                let child = *projection.child;
                projection.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Projection(projection)
            }
            LogicalOperator::Limit(mut limit) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, limit.child.as_ref());
                if let Some(limit_expr) = &limit.limit {
                    collect_bindings_from_expr(limit_expr, &mut child_required);
                }
                if let Some(offset_expr) = &limit.offset {
                    collect_bindings_from_expr(offset_expr, &mut child_required);
                }

                let child = *limit.child;
                limit.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::Order(mut order) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, order.child.as_ref());
                for order_by in &order.orders {
                    collect_bindings_from_expr(&order_by.expression, &mut child_required);
                }

                let child = *order.child;
                order.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Order(order)
            }
            LogicalOperator::TopN(mut topn) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, topn.child.as_ref());
                for order_by in &topn.orders {
                    collect_bindings_from_expr(&order_by.expression, &mut child_required);
                }

                let child = *topn.child;
                topn.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Aggregate(mut aggregate) => {
                let mut child_required = HashSet::new();
                collect_bindings_from_exprs(&aggregate.groups, &mut child_required);
                collect_bindings_from_exprs(&aggregate.aggregates, &mut child_required);

                let child = *aggregate.child;
                aggregate.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Aggregate(aggregate)
            }
            LogicalOperator::Join(join) => self.optimize_join(join, required_bindings),
            LogicalOperator::DependentJoin(mut join) => {
                let mut left_required = output_bindings(&join.left.operator);
                let mut right_required = output_bindings(&join.right.operator);

                if let Some(payload) = join.any_all_payload() {
                    collect_bindings_from_exprs(&payload.expression_children, &mut left_required);
                    collect_bindings_from_exprs(&payload.expression_children, &mut right_required);
                }
                if let Some(condition) = join.join_condition() {
                    collect_bindings_from_expr(condition, &mut left_required);
                    collect_bindings_from_expr(condition, &mut right_required);
                }

                let left = *join.left;
                let right = *join.right;
                join.left = Box::new(self.optimize_required_plan(left, &left_required));
                join.right = Box::new(self.optimize_required_plan(right, &right_required));
                LogicalOperator::DependentJoin(join)
            }
            LogicalOperator::SetOperation(mut setop) => {
                let left_required = output_bindings(&setop.left.operator);
                let right_required = output_bindings(&setop.right.operator);

                let left = *setop.left;
                let right = *setop.right;
                setop.left = Box::new(self.optimize_required_plan(left, &left_required));
                setop.right = Box::new(self.optimize_required_plan(right, &right_required));
                LogicalOperator::SetOperation(setop)
            }
            LogicalOperator::Distinct(mut distinct) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, distinct.child.as_ref());
                collect_bindings_from_exprs(&distinct.distinct_targets, &mut child_required);
                if let Some(order_by) = &distinct.order_by {
                    for order in order_by {
                        collect_bindings_from_expr(&order.expression, &mut child_required);
                    }
                }

                let child = *distinct.child;
                distinct.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::Window(mut window) => {
                let mut child_required =
                    filter_required_bindings(required_bindings, window.child.as_ref());
                let child_binding_count = window.child.get_column_bindings().len();

                for (idx, expression) in window.expressions.iter().enumerate() {
                    let window_binding =
                        ColumnBinding::new(window.window_index, child_binding_count + idx);
                    if required_bindings.contains(&window_binding) {
                        collect_bindings_from_window_expr(expression, &mut child_required);
                    }
                }

                let child = *window.child;
                window.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Window(window)
            }
            LogicalOperator::Explain(mut explain) => {
                let child_required = output_bindings(&explain.child.operator);
                let child = *explain.child;
                explain.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Explain(explain)
            }
            LogicalOperator::EmptyResult(mut empty) => {
                let child_required =
                    filter_required_bindings(required_bindings, empty.child.as_ref());
                let child = *empty.child;
                empty.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::EmptyResult(empty)
            }
            LogicalOperator::MaterializedCTE(mut cte) => {
                let cte_query_required = output_bindings(&cte.cte_query.operator);
                let child_required =
                    filter_required_bindings(required_bindings, cte.child.as_ref());

                let cte_query = *cte.cte_query;
                let child = *cte.child;
                cte.cte_query =
                    Box::new(self.optimize_required_plan(cte_query, &cte_query_required));
                cte.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::MaterializedCTE(cte)
            }
            LogicalOperator::RecursiveCTE(mut cte) => {
                let anchor_required = output_bindings(&cte.anchor.operator);
                let recursive_required = output_bindings(&cte.recursive.operator);

                let anchor = *cte.anchor;
                let recursive = *cte.recursive;
                cte.anchor = Box::new(self.optimize_required_plan(anchor, &anchor_required));
                cte.recursive =
                    Box::new(self.optimize_required_plan(recursive, &recursive_required));
                LogicalOperator::RecursiveCTE(cte)
            }
            LogicalOperator::CopyTo(mut copy) => {
                let child_required = output_bindings(&copy.child.operator);
                let child = *copy.child;
                copy.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::CopyTo(copy)
            }
            LogicalOperator::Delete(mut delete) => {
                let child_required = output_bindings(&delete.child.operator);
                let child = *delete.child;
                delete.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Delete(delete)
            }
            LogicalOperator::Update(mut update) => {
                let mut child_required = output_bindings(&update.child.operator);
                collect_bindings_from_exprs(&update.expressions, &mut child_required);

                let child = *update.child;
                update.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Update(update)
            }
            LogicalOperator::Insert(mut insert) => {
                let child_required = output_bindings(&insert.child.operator);
                let child = *insert.child;
                insert.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::Insert(insert)
            }
            LogicalOperator::GraphExpand(mut expand) => {
                let child_required = output_bindings(&expand.child.operator);
                let child = *expand.child;
                expand.child = Box::new(self.optimize_required_plan(child, &child_required));
                LogicalOperator::GraphExpand(expand)
            }
            other => other,
        };
        LogicalPlan {
            id,
            stats,
            operator,
        }
    }

    fn optimize_join(
        &mut self,
        join: Join,
        required_bindings: &HashSet<ColumnBinding>,
    ) -> LogicalOperator {
        match join {
            Join::Comparison(mut comparison) => {
                let left_child_bindings = output_bindings(&comparison.left.operator);
                let right_child_bindings = output_bindings(&comparison.right.operator);

                let mut left_required =
                    filter_required_by_output(required_bindings, &left_child_bindings);
                let mut right_required =
                    filter_required_by_output(required_bindings, &right_child_bindings);
                add_join_local_bindings(
                    &comparison,
                    &left_child_bindings,
                    &mut left_required,
                    &right_child_bindings,
                    &mut right_required,
                );

                let left = *comparison.left;
                let right = *comparison.right;
                comparison.left = Box::new(self.optimize_required_plan(left, &left_required));
                comparison.right = Box::new(self.optimize_required_plan(right, &right_required));

                if self.can_eliminate_right_side(&comparison, required_bindings) {
                    let left = *comparison.left;
                    return left.operator;
                }
                if self.can_eliminate_left_side(&comparison, required_bindings) {
                    let right = *comparison.right;
                    return right.operator;
                }

                LogicalOperator::Join(Join::Comparison(comparison))
            }
            Join::Any(mut any_join) => {
                let left_child_bindings = output_bindings(&any_join.left.operator);
                let right_child_bindings = output_bindings(&any_join.right.operator);

                let mut left_required =
                    filter_required_by_output(required_bindings, &left_child_bindings);
                let mut right_required =
                    filter_required_by_output(required_bindings, &right_child_bindings);
                add_bindings_for_child(
                    &any_join.condition,
                    &left_child_bindings,
                    &mut left_required,
                );
                add_bindings_for_child(
                    &any_join.condition,
                    &right_child_bindings,
                    &mut right_required,
                );

                let left = *any_join.left;
                let right = *any_join.right;
                any_join.left = Box::new(self.optimize_required_plan(left, &left_required));
                any_join.right = Box::new(self.optimize_required_plan(right, &right_required));
                LogicalOperator::Join(Join::Any(any_join))
            }
            Join::Cross(mut cross) => {
                let left_child_bindings = output_bindings(&cross.left.operator);
                let right_child_bindings = output_bindings(&cross.right.operator);

                let left_required =
                    filter_required_by_output(required_bindings, &left_child_bindings);
                let right_required =
                    filter_required_by_output(required_bindings, &right_child_bindings);

                let left = *cross.left;
                let right = *cross.right;
                cross.left = Box::new(self.optimize_required_plan(left, &left_required));
                cross.right = Box::new(self.optimize_required_plan(right, &right_required));
                LogicalOperator::Join(Join::Cross(cross))
            }
        }
    }

    fn can_eliminate_right_side(
        &self,
        join: &ComparisonJoin,
        required_bindings: &HashSet<ColumnBinding>,
    ) -> bool {
        if join.join_type != JoinType::Left {
            return false;
        }
        if !self.join_shape_supported(join) {
            return false;
        }
        if has_required_bindings_from_child(required_bindings, join.right.as_ref()) {
            return false;
        }

        let LogicalOperator::Get(get) = &join.right.operator else {
            return false;
        };
        self.conditions_cover_unique_key(join, get, true)
    }

    fn can_eliminate_left_side(
        &self,
        join: &ComparisonJoin,
        required_bindings: &HashSet<ColumnBinding>,
    ) -> bool {
        if join.join_type != JoinType::Right {
            return false;
        }
        if !self.join_shape_supported(join) {
            return false;
        }
        if has_required_bindings_from_child(required_bindings, join.left.as_ref()) {
            return false;
        }

        let LogicalOperator::Get(get) = &join.left.operator else {
            return false;
        };
        self.conditions_cover_unique_key(join, get, false)
    }

    fn join_shape_supported(&self, join: &ComparisonJoin) -> bool {
        join.mark_index.is_none()
            && join.left_projection_map.is_empty()
            && join.right_projection_map.is_empty()
            && join.duplicate_eliminated_columns.is_empty()
            && !join.delim_flipped
            && !join.conditions.is_empty()
    }

    fn conditions_cover_unique_key(
        &self,
        join: &ComparisonJoin,
        get: &paro_planner::operator::Get,
        eliminate_right: bool,
    ) -> bool {
        let Some(table) = get.table.as_ref() else {
            return false;
        };

        let mut key_columns = HashSet::new();
        for condition in &join.conditions {
            if condition.comparison != JoinComparisonType::Equal {
                return false;
            }

            let preserved_expr = if eliminate_right {
                &condition.left
            } else {
                &condition.right
            };
            let eliminated_expr = if eliminate_right {
                &condition.right
            } else {
                &condition.left
            };

            if !matches!(preserved_expr, Expression::ColumnRef(_)) {
                return false;
            }

            let Expression::ColumnRef(column_ref) = eliminated_expr else {
                return false;
            };
            if column_ref.binding.table_index != get.table_index {
                return false;
            }
            let Some(column_id) = get.column_ids.get(column_ref.binding.column_index) else {
                return false;
            };
            key_columns.insert(*column_id);
        }

        if key_columns.is_empty() {
            return false;
        }

        table.constraints.iter().any(|constraint| {
            matches!(
                constraint.constraint_type,
                ConstraintType::Unique | ConstraintType::PrimaryKey
            ) && !constraint.columns.is_empty()
                && constraint
                    .columns
                    .iter()
                    .all(|column| key_columns.contains(column))
        })
    }
}

fn output_bindings(op: &LogicalOperator) -> HashSet<ColumnBinding> {
    op.get_column_bindings().into_iter().collect()
}

fn filter_required_bindings(
    required_bindings: &HashSet<ColumnBinding>,
    child: &LogicalPlan,
) -> HashSet<ColumnBinding> {
    let child_outputs = output_bindings(&child.operator);
    filter_required_by_output(required_bindings, &child_outputs)
}

fn filter_required_by_output(
    required_bindings: &HashSet<ColumnBinding>,
    child_outputs: &HashSet<ColumnBinding>,
) -> HashSet<ColumnBinding> {
    required_bindings
        .iter()
        .copied()
        .filter(|binding| child_outputs.contains(binding))
        .collect()
}

fn projection_child_required_bindings(
    projection: &Projection,
    required_bindings: &HashSet<ColumnBinding>,
) -> HashSet<ColumnBinding> {
    let mut child_required = HashSet::new();
    for (idx, expression) in projection.expressions.iter().enumerate() {
        let projection_binding = ColumnBinding::new(projection.table_index, idx);
        if required_bindings.contains(&projection_binding) {
            collect_bindings_from_expr(expression, &mut child_required);
        }
    }
    child_required
}

fn add_join_local_bindings(
    join: &ComparisonJoin,
    left_child_bindings: &HashSet<ColumnBinding>,
    left_required: &mut HashSet<ColumnBinding>,
    right_child_bindings: &HashSet<ColumnBinding>,
    right_required: &mut HashSet<ColumnBinding>,
) {
    for condition in &join.conditions {
        add_bindings_for_child(&condition.left, left_child_bindings, left_required);
        add_bindings_for_child(&condition.right, left_child_bindings, left_required);
        add_bindings_for_child(&condition.left, right_child_bindings, right_required);
        add_bindings_for_child(&condition.right, right_child_bindings, right_required);
    }
    for expression in &join.duplicate_eliminated_columns {
        add_bindings_for_child(expression, left_child_bindings, left_required);
        add_bindings_for_child(expression, right_child_bindings, right_required);
    }
}

fn add_bindings_for_child(
    expr: &Expression,
    child_bindings: &HashSet<ColumnBinding>,
    target: &mut HashSet<ColumnBinding>,
) {
    let mut expr_bindings = HashSet::new();
    collect_bindings_from_expr(expr, &mut expr_bindings);
    target.extend(
        expr_bindings
            .into_iter()
            .filter(|binding| child_bindings.contains(binding)),
    );
}

fn has_required_bindings_from_child(
    required_bindings: &HashSet<ColumnBinding>,
    child: &LogicalPlan,
) -> bool {
    let child_outputs = output_bindings(&child.operator);
    required_bindings
        .iter()
        .any(|binding| child_outputs.contains(binding))
}

fn collect_bindings_from_exprs(expressions: &[Expression], bindings: &mut HashSet<ColumnBinding>) {
    for expression in expressions {
        collect_bindings_from_expr(expression, bindings);
    }
}

fn collect_bindings_from_window_expr(
    expression: &WindowExpression,
    bindings: &mut HashSet<ColumnBinding>,
) {
    collect_bindings_from_exprs(&expression.children, bindings);
    collect_bindings_from_exprs(&expression.partitions, bindings);
    for order in &expression.orders {
        collect_bindings_from_expr(&order.expression, bindings);
    }
    if let WindowFrameBound::Offset(expr) = &expression.frame.start_bound {
        collect_bindings_from_expr(expr, bindings);
    }
    if let WindowFrameBound::Offset(expr) = &expression.frame.end_bound {
        collect_bindings_from_expr(expr, bindings);
    }
}

fn collect_bindings_from_expr(expr: &Expression, bindings: &mut HashSet<ColumnBinding>) {
    match expr {
        Expression::ColumnRef(column_ref) => {
            bindings.insert(column_ref.binding);
        }
        Expression::Aggregate(aggregate) => {
            collect_bindings_from_exprs(&aggregate.children, bindings);
            if let Some(filter) = &aggregate.filter {
                collect_bindings_from_expr(filter, bindings);
            }
            for order_by in &aggregate.order_bys {
                collect_bindings_from_expr(&order_by.expression, bindings);
            }
        }
        Expression::Case(case_expr) => {
            collect_bindings_from_expr(&case_expr.check, bindings);
            collect_bindings_from_expr(&case_expr.result_if_true, bindings);
            collect_bindings_from_expr(&case_expr.result_if_false, bindings);
        }
        Expression::Cast(cast_expr) => {
            collect_bindings_from_expr(&cast_expr.child, bindings);
        }
        Expression::Comparison(comparison) => {
            collect_bindings_from_expr(&comparison.left, bindings);
            collect_bindings_from_expr(&comparison.right, bindings);
        }
        Expression::Conjunction(conjunction) => {
            collect_bindings_from_exprs(&conjunction.children, bindings);
        }
        Expression::Function(function) => {
            collect_bindings_from_exprs(&function.children, bindings);
        }
        Expression::Operator(operator) => {
            collect_bindings_from_exprs(&operator.children, bindings);
        }
        Expression::Subquery(subquery) => {
            collect_bindings_from_exprs(&subquery.children, bindings);
        }
        Expression::Window(window) => {
            collect_bindings_from_window_expr(window, bindings);
        }
        Expression::Constant(_) | Expression::Reference(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::JoinElimination;
    use std::sync::Arc;

    use paro_catalog::entry::{ColumnDefinition, Constraint, TableCatalogEntry};
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        ColumnBinding, Get, Join, JoinCondition, JoinType, LogicalOperator, Projection,
    };
    use paro_planner::plan::LogicalPlan;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;

    fn create_storage(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn col(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    fn create_table(
        name: &str,
        column_count: usize,
        constraints: Vec<Constraint>,
    ) -> Arc<TableCatalogEntry> {
        let types = vec![LogicalType::Integer; column_count];
        let storage = Arc::new(create_storage(&types));
        let columns = (0..column_count)
            .map(|idx| ColumnDefinition::new(format!("c{idx}"), LogicalType::Integer))
            .collect();

        let mut table = TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            name.to_string(),
            columns,
            storage,
            0,
        );
        table.constraints = constraints;
        Arc::new(table)
    }

    fn create_get(
        table_index: usize,
        table: Arc<TableCatalogEntry>,
        column_ids: Vec<usize>,
    ) -> LogicalOperator {
        let names = column_ids
            .iter()
            .map(|idx| table.columns[*idx].name.clone())
            .collect::<Vec<_>>();
        let types = column_ids
            .iter()
            .map(|idx| table.columns[*idx].logical_type.clone())
            .collect::<Vec<_>>();
        let mut get = Get::new(table_index, names, types.clone(), table);
        get.column_ids = column_ids;
        get.column_types = types.clone();
        get.returned_types = types;
        LogicalOperator::Get(get)
    }

    fn create_projection(
        table_index: usize,
        child: LogicalOperator,
        expressions: Vec<Expression>,
    ) -> LogicalOperator {
        LogicalOperator::Projection(Projection::new(
            table_index,
            LogicalPlan::synthetic(child),
            expressions,
        ))
    }

    fn create_join(
        join_type: JoinType,
        left: LogicalOperator,
        right: LogicalOperator,
        conditions: Vec<JoinCondition>,
    ) -> LogicalOperator {
        LogicalOperator::Join(Join::comparison(
            join_type,
            LogicalPlan::synthetic(left),
            LogicalPlan::synthetic(right),
            conditions,
        ))
    }

    #[test]
    fn eliminates_unused_unique_right_side_of_left_join() {
        let left = create_get(0, create_table("left_t", 1, vec![]), vec![0]);
        let right = create_get(
            1,
            create_table("right_t", 1, vec![Constraint::unique(vec![0])]),
            vec![0],
        );
        let plan = create_projection(
            10,
            create_join(
                JoinType::Left,
                left,
                right,
                vec![JoinCondition::equality(col(0, 0), col(1, 0))],
            ),
            vec![col(0, 0)],
        );

        let optimized = JoinElimination::new().optimize(LogicalPlan::synthetic(plan));
        let LogicalOperator::Projection(projection) = optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Get(get) = &projection.child.operator else {
            panic!("expected left get after elimination");
        };
        assert_eq!(get.table_index, 0);
    }

    #[test]
    fn keeps_left_join_when_inner_side_is_projected() {
        let left = create_get(0, create_table("left_keep", 1, vec![]), vec![0]);
        let right = create_get(
            1,
            create_table("right_keep", 1, vec![Constraint::unique(vec![0])]),
            vec![0],
        );
        let plan = create_projection(
            10,
            create_join(
                JoinType::Left,
                left,
                right,
                vec![JoinCondition::equality(col(0, 0), col(1, 0))],
            ),
            vec![col(1, 0)],
        );

        let optimized = JoinElimination::new().optimize(LogicalPlan::synthetic(plan));
        let LogicalOperator::Projection(projection) = optimized.operator else {
            panic!("expected projection");
        };
        assert!(matches!(
            &projection.child.operator,
            LogicalOperator::Join(Join::Comparison(_))
        ));
    }

    #[test]
    fn eliminates_unused_unique_left_side_of_right_join() {
        let left = create_get(
            0,
            create_table("left_unique", 1, vec![Constraint::primary_key(vec![0])]),
            vec![0],
        );
        let right = create_get(1, create_table("right_probe", 1, vec![]), vec![0]);
        let plan = create_projection(
            10,
            create_join(
                JoinType::Right,
                left,
                right,
                vec![JoinCondition::equality(col(0, 0), col(1, 0))],
            ),
            vec![col(1, 0)],
        );

        let optimized = JoinElimination::new().optimize(LogicalPlan::synthetic(plan));
        let LogicalOperator::Projection(projection) = optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Get(get) = &projection.child.operator else {
            panic!("expected right get after elimination");
        };
        assert_eq!(get.table_index, 1);
    }

    #[test]
    fn keeps_join_when_only_part_of_unique_key_is_bound() {
        let left = create_get(0, create_table("left_composite", 1, vec![]), vec![0]);
        let right = create_get(
            1,
            create_table("right_composite", 2, vec![Constraint::unique(vec![0, 1])]),
            vec![0, 1],
        );
        let plan = create_projection(
            10,
            create_join(
                JoinType::Left,
                left,
                right,
                vec![JoinCondition::equality(col(0, 0), col(1, 0))],
            ),
            vec![col(0, 0)],
        );

        let optimized = JoinElimination::new().optimize(LogicalPlan::synthetic(plan));
        let LogicalOperator::Projection(projection) = optimized.operator else {
            panic!("expected projection");
        };
        assert!(matches!(
            &projection.child.operator,
            LogicalOperator::Join(Join::Comparison(_))
        ));
    }

    #[test]
    fn maps_logical_get_column_ids_to_catalog_constraint_columns() {
        let left = create_get(0, create_table("left_mapping", 1, vec![]), vec![0]);
        let right = create_get(
            1,
            create_table("right_mapping", 2, vec![Constraint::unique(vec![1])]),
            vec![1],
        );
        let plan = create_projection(
            10,
            create_join(
                JoinType::Left,
                left,
                right,
                vec![JoinCondition::equality(col(0, 0), col(1, 0))],
            ),
            vec![col(0, 0)],
        );

        let optimized = JoinElimination::new().optimize(LogicalPlan::synthetic(plan));
        let LogicalOperator::Projection(projection) = optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Get(get) = &projection.child.operator else {
            panic!("expected left get after elimination");
        };
        assert_eq!(get.table_index, 0);
    }
}
