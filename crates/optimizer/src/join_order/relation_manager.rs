// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Relation extraction and bookkeeping for join-order optimization.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_planner::expression::{Expression, ExpressionIterator};
use paro_planner::operator::{Join, JoinType, LogicalOperator, LogicalOperatorType};

use crate::expression::join_tree_has_evaluation_fence;
use crate::join_order::query_graph::FilterInfo;
use crate::join_order::relation::JoinRelationSetManager;

/// A filter extracted from the logical plan together with the join semantics it came from.
#[derive(Debug, Clone)]
pub struct ExtractedFilter {
    pub expression: Expression,
    pub join_type: JoinType,
}

impl ExtractedFilter {
    pub fn new(expression: Expression, join_type: JoinType) -> Self {
        Self {
            expression,
            join_type,
        }
    }

    pub fn inner(expression: Expression) -> Self {
        Self::new(expression, JoinType::Inner)
    }
}

/// Statistics for a single relation.
///
#[derive(Debug, Clone, Default)]
pub struct RelationStats {
    /// Estimated distinct count for each column.
    pub column_distinct_count: Vec<DistinctCount>,
    /// Estimated cardinality (row count).
    pub cardinality: usize,
    /// Filter strength (selectivity factor).
    pub filter_strength: f64,
    /// Whether statistics have been initialized.
    pub stats_initialized: bool,
}

impl RelationStats {
    /// Create new empty statistics.
    pub fn new() -> Self {
        Self {
            column_distinct_count: Vec::new(),
            cardinality: 1,
            filter_strength: 1.0,
            stats_initialized: false,
        }
    }

    /// Create statistics with a given cardinality.
    pub fn with_cardinality(cardinality: usize) -> Self {
        Self {
            cardinality,
            stats_initialized: true,
            ..Self::new()
        }
    }
}

/// Distinct count information for a column.
#[derive(Debug, Clone, Default)]
pub struct DistinctCount {
    /// The estimated distinct count.
    pub distinct_count: usize,
    /// Whether this count came from HyperLogLog.
    pub from_hll: bool,
}

impl DistinctCount {
    /// Create a new distinct count.
    pub fn new(distinct_count: usize, from_hll: bool) -> Self {
        Self {
            distinct_count,
            from_hll,
        }
    }
}

/// Represents a single relation and its metadata.
///
#[derive(Debug)]
pub struct SingleJoinRelation {
    /// The logical operator for this relation.
    pub op: LogicalOperator,
    /// The parent operator (if any).
    pub parent: Option<Box<LogicalOperator>>,
    /// Statistics for this relation.
    pub stats: RelationStats,
}

impl SingleJoinRelation {
    /// Create a new single join relation.
    fn new(op: LogicalOperator, parent: Option<LogicalOperator>, stats: RelationStats) -> Self {
        Self {
            op,
            parent: parent.map(Box::new),
            stats,
        }
    }
}

/// The RelationManager manages relations for join order optimization.
///
#[derive(Debug, Default)]
pub struct RelationManager {
    /// Set of all relations considered in the join optimizer.
    relations: Vec<SingleJoinRelation>,
    /// Mapping from table index to relation index.
    relation_mapping: HashMap<usize, usize>,
    /// Relations that cannot have cross products.
    no_cross_product_relations: HashSet<usize>,
}

impl RelationManager {
    /// Create a new RelationManager.
    pub fn new() -> Self {
        Self {
            relations: Vec::new(),
            relation_mapping: HashMap::new(),
            no_cross_product_relations: HashSet::new(),
        }
    }

    /// Get the number of relations.
    pub fn num_relations(&self) -> usize {
        self.relations.len()
    }

    /// Check if a relation allows cross products.
    pub fn cross_product_with_relation_allowed(&self, relation_id: usize) -> bool {
        !self.no_cross_product_relations.contains(&relation_id)
    }

    /// Add a relation to the manager.
    pub fn add_relation(
        &mut self,
        op: LogicalOperator,
        parent: Option<LogicalOperator>,
        stats: RelationStats,
    ) {
        let relation_id = self.relations.len();

        // Get table indices from the operator
        let table_indices = op.get_table_index();

        if table_indices.is_empty() {
            // For operators without table indices (like joins), get all column bindings
            let bindings = op.get_column_bindings();
            for binding in bindings {
                self.relation_mapping
                    .entry(binding.table_index)
                    .or_insert(relation_id);
            }
        } else {
            // Normal case: map each table index to this relation
            for table_index in table_indices {
                debug_assert!(
                    !self.relation_mapping.contains_key(&table_index),
                    "Table index {} already mapped",
                    table_index
                );
                self.relation_mapping.insert(table_index, relation_id);
            }
        }

        self.relations
            .push(SingleJoinRelation::new(op, parent, stats));
    }

    /// Add a relation for aggregate or window operators.
    pub fn add_aggregate_or_window_relation(
        &mut self,
        op: LogicalOperator,
        parent: Option<LogicalOperator>,
        stats: RelationStats,
    ) {
        let relation_id = self.relations.len();

        // Get column bindings and map them
        let bindings = op.get_column_bindings();
        for binding in bindings {
            self.relation_mapping
                .entry(binding.table_index)
                .or_insert(relation_id);
        }

        self.relations
            .push(SingleJoinRelation::new(op, parent, stats));
    }

    /// Mark a relation as not allowing cross products.
    pub fn mark_no_cross_product(&mut self, relation_id: usize) {
        self.no_cross_product_relations.insert(relation_id);
    }

    /// Get the relation ID for a table index.
    pub fn get_relation_id(&self, table_index: usize) -> Option<usize> {
        self.relation_mapping.get(&table_index).copied()
    }

    /// Get all relation statistics.
    pub fn get_relation_stats(&self) -> Vec<RelationStats> {
        self.relations.iter().map(|r| r.stats.clone()).collect()
    }

    /// Take ownership of all relations.
    pub fn take_relations(&mut self) -> Vec<SingleJoinRelation> {
        std::mem::take(&mut self.relations)
    }

    /// Get a reference to a relation by ID.
    pub fn get_relation(&self, relation_id: usize) -> Option<&SingleJoinRelation> {
        self.relations.get(relation_id)
    }

    /// Get a mutable reference to a relation by ID.
    pub fn get_relation_mut(&mut self, relation_id: usize) -> Option<&mut SingleJoinRelation> {
        self.relations.get_mut(relation_id)
    }

    /// Extract bindings from an expression.
    ///
    /// Returns true if the expression can be reordered, false otherwise.
    /// Populates `bindings` with the relation IDs referenced by the expression.
    pub fn extract_bindings(&self, expression: &Expression, bindings: &mut HashSet<usize>) -> bool {
        match expression {
            Expression::ColumnRef(colref) => {
                if let Some(&relation_id) = self.relation_mapping.get(&colref.binding.table_index) {
                    bindings.insert(relation_id);
                }
                true
            }
            Expression::Reference(_) => {
                // Bound reference - cannot reorder
                bindings.clear();
                false
            }
            Expression::Subquery(_) => {
                // Subqueries cannot be reordered
                false
            }
            _ => {
                let mut reorderable = true;
                ExpressionIterator::enumerate_children(expression, |child| {
                    if reorderable && !self.extract_bindings(child, bindings) {
                        reorderable = false;
                    }
                });
                reorderable
            }
        }
    }

    /// Extract edges (filters) from filter operators.
    ///
    /// Returns a list of FilterInfo representing the join conditions.
    pub fn extract_edges(
        &self,
        filter_expressions: &[ExtractedFilter],
        set_manager: &mut JoinRelationSetManager,
    ) -> Vec<Arc<FilterInfo>> {
        let mut filters = Vec::new();
        let mut seen_expressions = HashSet::new();

        for extracted_filter in filter_expressions {
            // Skip duplicate expressions
            let expr_key = format!(
                "{:?}:{:?}",
                extracted_filter.join_type, extracted_filter.expression
            );
            if seen_expressions.contains(&expr_key) {
                continue;
            }
            seen_expressions.insert(expr_key);

            // Extract bindings from the expression
            let mut bindings = HashSet::new();
            if !self.extract_bindings(&extracted_filter.expression, &mut bindings) {
                continue;
            }

            if bindings.is_empty() {
                // Expression doesn't reference any relations
                continue;
            }

            // Create the relation set
            let set = set_manager.get_relation_from_set(&bindings);

            // Create the filter info
            let mut filter_info = FilterInfo::new(
                extracted_filter.expression.clone(),
                set,
                filters.len(),
                extracted_filter.join_type,
            );

            self.populate_filter_info_bindings(extracted_filter, set_manager, &mut filter_info);
            filters.push(Arc::new(filter_info));
        }

        filters
    }

    /// Check if a join is reorderable.
    pub fn join_is_reorderable(join: &Join) -> bool {
        if Self::join_contains_delim_get(join) || join_tree_has_evaluation_fence(join) {
            return false;
        }

        match join {
            Join::Cross(_) => true,
            Join::Comparison(cj) => {
                if !cj.duplicate_eliminated_columns.is_empty() {
                    return false;
                }
                match cj.join_type {
                    JoinType::Inner | JoinType::Semi | JoinType::Anti => {
                        // Check if conditions reference columns from both sides
                        for cond in &cj.conditions {
                            if Self::expression_contains_column_ref(&cond.left)
                                && Self::expression_contains_column_ref(&cond.right)
                            {
                                return true;
                            }
                        }
                        false
                    }
                    _ => false,
                }
            }
            Join::Any(_) => false,
        }
    }

    fn join_contains_delim_get(join: &Join) -> bool {
        Self::operator_contains_delim_get(&join.left().operator)
            || Self::operator_contains_delim_get(&join.right().operator)
    }

    fn operator_contains_delim_get(op: &LogicalOperator) -> bool {
        matches!(op, LogicalOperator::DelimGet(_))
            || op
                .children()
                .into_iter()
                .any(|child| Self::operator_contains_delim_get(&child.operator))
    }

    /// Check if an expression contains a column reference.
    fn expression_contains_column_ref(expr: &Expression) -> bool {
        match expr {
            Expression::ColumnRef(_) => true,
            Expression::Function(func) => func
                .children
                .iter()
                .any(Self::expression_contains_column_ref),
            Expression::Cast(cast) => Self::expression_contains_column_ref(&cast.child),
            Expression::Conjunction(conj) => conj
                .children
                .iter()
                .any(Self::expression_contains_column_ref),
            Expression::Comparison(comp) => {
                Self::expression_contains_column_ref(&comp.left)
                    || Self::expression_contains_column_ref(&comp.right)
            }
            Expression::Operator(op) => {
                op.children.iter().any(Self::expression_contains_column_ref)
            }
            Expression::Case(case) => {
                Self::expression_contains_column_ref(&case.check)
                    || Self::expression_contains_column_ref(&case.result_if_true)
                    || Self::expression_contains_column_ref(&case.result_if_false)
            }
            _ => false,
        }
    }

    /// Check if an operator needs to be treated as a relation.
    pub(crate) fn operator_needs_relation(op_type: LogicalOperatorType) -> bool {
        matches!(
            op_type,
            LogicalOperatorType::Projection
                | LogicalOperatorType::Get
                | LogicalOperatorType::Aggregate
                | LogicalOperatorType::Window
                | LogicalOperatorType::CTERef
        )
    }

    /// Check if an operator is non-reorderable.
    #[cfg(test)]
    fn operator_is_non_reorderable(op_type: LogicalOperatorType) -> bool {
        matches!(
            op_type,
            LogicalOperatorType::LogicalUnion
                | LogicalOperatorType::LogicalExcept
                | LogicalOperatorType::LogicalIntersect
                | LogicalOperatorType::AnyJoin
        )
    }

    fn populate_filter_info_bindings(
        &self,
        extracted_filter: &ExtractedFilter,
        set_manager: &mut JoinRelationSetManager,
        filter_info: &mut FilterInfo,
    ) {
        match &extracted_filter.expression {
            Expression::Comparison(comp) => {
                let mut left_bindings = HashSet::new();
                let mut right_bindings = HashSet::new();
                if !self.extract_bindings(&comp.left, &mut left_bindings)
                    || !self.extract_bindings(&comp.right, &mut right_bindings)
                {
                    return;
                }

                if !left_bindings.is_empty() && !right_bindings.is_empty() {
                    filter_info.set_left_set(set_manager.get_relation_from_set(&left_bindings));
                    filter_info.set_right_set(set_manager.get_relation_from_set(&right_bindings));
                }

                if let Some(binding) = Self::extract_column_binding(&comp.left) {
                    filter_info.set_left_binding(binding);
                }
                if let Some(binding) = Self::extract_column_binding(&comp.right) {
                    filter_info.set_right_binding(binding);
                }
            }
            Expression::Conjunction(conj)
                if matches!(extracted_filter.join_type, JoinType::Semi | JoinType::Anti) =>
            {
                let mut left_bindings = HashSet::new();
                let mut right_bindings = HashSet::new();
                for child in &conj.children {
                    let Expression::Comparison(comp) = child else {
                        continue;
                    };
                    let mut child_left = HashSet::new();
                    let mut child_right = HashSet::new();
                    if self.extract_bindings(&comp.left, &mut child_left)
                        && self.extract_bindings(&comp.right, &mut child_right)
                    {
                        left_bindings.extend(child_left);
                        right_bindings.extend(child_right);
                    }

                    if filter_info.left_binding.is_none() {
                        if let Some(binding) = Self::extract_column_binding(&comp.left) {
                            filter_info.set_left_binding(binding);
                        }
                    }
                    if filter_info.right_binding.is_none() {
                        if let Some(binding) = Self::extract_column_binding(&comp.right) {
                            filter_info.set_right_binding(binding);
                        }
                    }
                }

                if !left_bindings.is_empty() && !right_bindings.is_empty() {
                    filter_info.set_left_set(set_manager.get_relation_from_set(&left_bindings));
                    filter_info.set_right_set(set_manager.get_relation_from_set(&right_bindings));
                }
            }
            _ => {}
        }
    }

    fn extract_column_binding(expr: &Expression) -> Option<paro_planner::operator::ColumnBinding> {
        match expr {
            Expression::ColumnRef(colref) => Some(colref.binding),
            Expression::Cast(cast) => Self::extract_column_binding(&cast.child),
            _ => None,
        }
    }

    /// Get the relation mapping.
    pub fn relation_mapping(&self) -> &HashMap<usize, usize> {
        &self.relation_mapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        CaseExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ConstantExpression, FunctionExpression, WindowExpression, WindowFrame, WindowFrameBound,
        WindowFrameType,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, DelimGet, Get, JoinComparisonType, JoinCondition,
    };
    use paro_planner::plan::LogicalPlan;

    fn create_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn create_constant(value: i64) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::BigInt(value),
            return_type: LogicalType::BigInt,
        })
    }

    fn create_test_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::Get(Get {
            table_index,
            returned_types: vec![LogicalType::Integer],
            names: vec!["col".to_string()],
            relation_name: None,
            relation_alias: None,
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            table: None,
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        })
    }

    fn volatile_expression_with_column(table_index: usize) -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        let random = || {
            Expression::Function(FunctionExpression::new(
                function.clone(),
                vec![],
                LogicalType::Double,
            ))
        };
        Expression::Case(CaseExpression::new(
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::Equal,
                random(),
                random(),
            )),
            create_column_ref(table_index, 0),
            create_column_ref(table_index, 0),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn test_relation_manager_new() {
        let manager = RelationManager::new();
        assert_eq!(manager.num_relations(), 0);
    }

    #[test]
    fn test_extract_bindings_visits_window_frame_offsets() {
        let mut manager = RelationManager::new();
        manager.add_relation(create_test_get(7), None, RelationStats::new());
        let expression = Expression::Window(WindowExpression {
            function: paro_function::window::WindowFunction::row_number(),
            children: vec![],
            partitions: vec![],
            orders: vec![],
            frame: WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(create_column_ref(7, 0))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
        });
        let mut bindings = HashSet::new();

        assert!(manager.extract_bindings(&expression, &mut bindings));
        assert_eq!(bindings, HashSet::from([0]));
    }

    #[test]
    fn test_add_relation() {
        let mut manager = RelationManager::new();

        let op = create_test_get(0);
        let stats = RelationStats::with_cardinality(100);
        manager.add_relation(op, None, stats);

        assert_eq!(manager.num_relations(), 1);
        assert_eq!(manager.get_relation_id(0), Some(0));
    }

    #[test]
    fn test_add_multiple_relations() {
        let mut manager = RelationManager::new();

        // Add first relation
        let op1 = create_test_get(0);
        manager.add_relation(op1, None, RelationStats::with_cardinality(100));

        // Add second relation
        let op2 = create_test_get(1);
        manager.add_relation(op2, None, RelationStats::with_cardinality(200));

        assert_eq!(manager.num_relations(), 2);
        assert_eq!(manager.get_relation_id(0), Some(0));
        assert_eq!(manager.get_relation_id(1), Some(1));
    }

    #[test]
    fn test_extract_bindings_column_ref() {
        let mut manager = RelationManager::new();

        // Add a relation
        let op = create_test_get(5);
        manager.add_relation(op, None, RelationStats::new());

        // Extract bindings from a column reference
        let expr = create_column_ref(5, 0);
        let mut bindings = HashSet::new();
        let can_reorder = manager.extract_bindings(&expr, &mut bindings);

        assert!(can_reorder);
        assert_eq!(bindings.len(), 1);
        assert!(bindings.contains(&0)); // relation_id = 0
    }

    #[test]
    fn test_extract_bindings_comparison() {
        let mut manager = RelationManager::new();

        // Add two relations
        let op1 = create_test_get(0);
        manager.add_relation(op1, None, RelationStats::new());

        let op2 = create_test_get(1);
        manager.add_relation(op2, None, RelationStats::new());

        // Create comparison: t1.a = t2.b
        let expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        });

        let mut bindings = HashSet::new();
        let can_reorder = manager.extract_bindings(&expr, &mut bindings);

        assert!(can_reorder);
        assert_eq!(bindings.len(), 2);
        assert!(bindings.contains(&0));
        assert!(bindings.contains(&1));
    }

    #[test]
    fn test_extract_bindings_constant() {
        let manager = RelationManager::new();

        let expr = create_constant(42);
        let mut bindings = HashSet::new();
        let can_reorder = manager.extract_bindings(&expr, &mut bindings);

        assert!(can_reorder);
        assert!(bindings.is_empty());
    }

    #[test]
    fn test_cross_product_allowed() {
        let mut manager = RelationManager::new();

        // Add a relation
        let op = create_test_get(0);
        manager.add_relation(op, None, RelationStats::new());

        // By default, cross product is allowed
        assert!(manager.cross_product_with_relation_allowed(0));

        // Mark as no cross product
        manager.mark_no_cross_product(0);
        assert!(!manager.cross_product_with_relation_allowed(0));
    }

    #[test]
    fn test_get_relation_stats() {
        let mut manager = RelationManager::new();

        let op1 = create_test_get(0);
        manager.add_relation(op1, None, RelationStats::with_cardinality(100));

        let op2 = create_test_get(1);
        manager.add_relation(op2, None, RelationStats::with_cardinality(200));

        let stats = manager.get_relation_stats();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].cardinality, 100);
        assert_eq!(stats[1].cardinality, 200);
    }

    #[test]
    fn test_extract_edges() {
        let mut manager = RelationManager::new();
        let mut set_manager = JoinRelationSetManager::new();

        // Add two relations
        let op1 = create_test_get(0);
        manager.add_relation(op1, None, RelationStats::new());

        let op2 = create_test_get(1);
        manager.add_relation(op2, None, RelationStats::new());

        // Create filter expressions
        let filter1 = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        });

        let filter2 = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_constant(10)),
            comparison_type: ComparisonType::GreaterThan,
        });

        let filters = manager.extract_edges(
            &[
                ExtractedFilter::inner(filter1),
                ExtractedFilter::inner(filter2),
            ],
            &mut set_manager,
        );

        assert_eq!(filters.len(), 2);
        // First filter references both relations
        assert_eq!(filters[0].set.count(), 2);
        assert_eq!(filters[0].join_type, JoinType::Inner);
        assert!(filters[0].left_set.is_some());
        assert!(filters[0].right_set.is_some());
        // Second filter references only one relation
        assert_eq!(filters[1].set.count(), 1);
    }

    #[test]
    fn test_extract_edges_preserves_join_type_and_bindings() {
        let mut manager = RelationManager::new();
        let mut set_manager = JoinRelationSetManager::new();

        manager.add_relation(create_test_get(0), None, RelationStats::new());
        manager.add_relation(create_test_get(1), None, RelationStats::new());

        let filter = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::GreaterThan,
        });

        let filters = manager.extract_edges(
            &[ExtractedFilter::new(filter, JoinType::Semi)],
            &mut set_manager,
        );

        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].join_type, JoinType::Semi);
        assert_eq!(filters[0].left_binding, Some(ColumnBinding::new(0, 0)));
        assert_eq!(filters[0].right_binding, Some(ColumnBinding::new(1, 0)));
        assert!(filters[0].left_set.is_some());
        assert!(filters[0].right_set.is_some());
    }

    #[test]
    fn test_operator_needs_relation() {
        assert!(RelationManager::operator_needs_relation(
            LogicalOperatorType::Get
        ));
        assert!(RelationManager::operator_needs_relation(
            LogicalOperatorType::Projection
        ));
        assert!(RelationManager::operator_needs_relation(
            LogicalOperatorType::Aggregate
        ));
        assert!(RelationManager::operator_needs_relation(
            LogicalOperatorType::Window
        ));
        assert!(!RelationManager::operator_needs_relation(
            LogicalOperatorType::Filter
        ));
        assert!(!RelationManager::operator_needs_relation(
            LogicalOperatorType::Limit
        ));
    }

    #[test]
    fn test_operator_is_non_reorderable() {
        assert!(RelationManager::operator_is_non_reorderable(
            LogicalOperatorType::LogicalUnion
        ));
        assert!(RelationManager::operator_is_non_reorderable(
            LogicalOperatorType::LogicalExcept
        ));
        assert!(RelationManager::operator_is_non_reorderable(
            LogicalOperatorType::LogicalIntersect
        ));
        assert!(RelationManager::operator_is_non_reorderable(
            LogicalOperatorType::AnyJoin
        ));
        assert!(!RelationManager::operator_is_non_reorderable(
            LogicalOperatorType::ComparisonJoin
        ));
    }

    #[test]
    fn test_join_with_duplicate_elimination_is_not_reorderable() {
        let left = create_test_get(0);
        let right = create_test_get(1);
        let mut join = ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(left),
            LogicalPlan::synthetic(right),
            vec![JoinCondition::new(
                create_column_ref(0, 0),
                create_column_ref(1, 0),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![create_column_ref(0, 0)];

        assert!(!RelationManager::join_is_reorderable(&Join::Comparison(
            join
        )));
    }

    #[test]
    fn test_join_with_delim_get_subtree_is_not_reorderable() {
        let left = create_test_get(0);
        let right = LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer]));
        let join = ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(left),
            LogicalPlan::synthetic(right),
            vec![JoinCondition::new(
                create_column_ref(0, 0),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );

        assert!(!RelationManager::join_is_reorderable(&Join::Comparison(
            join
        )));
    }

    #[test]
    fn test_join_with_volatile_condition_is_not_reorderable() {
        let join = ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(create_test_get(0)),
            LogicalPlan::synthetic(create_test_get(1)),
            vec![JoinCondition::new(
                create_column_ref(0, 0),
                volatile_expression_with_column(1),
                JoinComparisonType::Equal,
            )],
        );

        assert!(!RelationManager::join_is_reorderable(&Join::Comparison(
            join
        )));
    }
    #[test]
    fn test_distinct_count() {
        let dc = DistinctCount::new(100, true);
        assert_eq!(dc.distinct_count, 100);
        assert!(dc.from_hll);
    }
}
