//! Combine compatible filters into a tighter equivalent set.
//!
//! The combiner:
//!
//! 1. Prunes obsolete filter conditions: `X > 5 AND X > 7` → `X > 7`
//! 2. Generates new filters for expressions in the same equivalence set:
//!    `X = Y AND X = 500` → `Y = 500`
//! 3. Prunes branches that have unsatisfiable filters:
//!    `X = 5 AND X > 6` → FALSE (prune branch)

use std::collections::HashMap;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::ComparisonType;
use paro_planner::expression::{ComparisonExpression, ConstantExpression, Expression};
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};

/// Result of adding a filter to the combiner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterResult {
    /// Filter was successfully added.
    Success,
    /// Filter combination is unsatisfiable (e.g., X = 5 AND X > 6).
    Unsatisfiable,
    /// Filter type is not supported by the combiner.
    Unsupported,
}

/// Result of comparing two value constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueComparisonResult {
    /// Left constraint is more restrictive, prune right.
    PruneRight,
    /// Right constraint is more restrictive, prune left.
    PruneLeft,
    /// Constraints are contradictory.
    Unsatisfiable,
    /// Neither constraint can be pruned.
    PruneNothing,
}

/// Information about a constant comparison for an expression.
#[derive(Debug, Clone)]
pub struct ExpressionValueInformation {
    /// The constant value being compared.
    pub constant: Value,
    /// The type of comparison.
    pub comparison_type: ComparisonType,
}

/// Key for expression equality in hash maps.
/// Uses table_index and column_index for ColumnRef expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExpressionKey {
    ColumnRef {
        table_index: usize,
        column_index: usize,
    },
    // Future: support other expression types
}

impl ExpressionKey {
    fn from_expression(expr: &Expression) -> Option<Self> {
        match expr {
            Expression::ColumnRef(col) => Some(ExpressionKey::ColumnRef {
                table_index: col.binding.table_index,
                column_index: col.binding.column_index,
            }),
            _ => None,
        }
    }
}

/// The FilterCombiner combines several filters and generates a logically equivalent
/// set that is more efficient.
pub struct FilterCombiner {
    /// Filters that couldn't be processed by the combiner.
    remaining_filters: Vec<Expression>,
    /// Map from expression key to stored expression.
    stored_expressions: HashMap<ExpressionKey, Expression>,
    /// Map from expression key to equivalence set index.
    equivalence_set_map: HashMap<ExpressionKey, usize>,
    /// Map from equivalence set index to constant value information.
    constant_values: HashMap<usize, Vec<ExpressionValueInformation>>,
    /// Map from equivalence set index to expressions in that set.
    equivalence_map: HashMap<usize, Vec<ExpressionKey>>,
    /// Next equivalence set index.
    set_index: usize,
}

impl FilterCombiner {
    /// Create a new FilterCombiner.
    pub fn new() -> Self {
        Self {
            remaining_filters: Vec::new(),
            stored_expressions: HashMap::new(),
            equivalence_set_map: HashMap::new(),
            constant_values: HashMap::new(),
            equivalence_map: HashMap::new(),
            set_index: 0,
        }
    }

    /// Add a filter expression to the combiner.
    ///
    /// Returns the result of adding the filter:
    /// - `Success`: Filter was added successfully
    /// - `Unsatisfiable`: Filter combination is impossible
    /// - `Unsupported`: Filter type is not supported
    pub fn add_filter(&mut self, expr: Expression) -> FilterResult {
        let result = self.add_filter_internal(&expr);
        if result == FilterResult::Unsupported {
            // Unsupported filter, push into remaining filters
            self.remaining_filters.push(expr);
            return FilterResult::Success;
        }
        result
    }

    /// Check if the combiner has any filters.
    pub fn has_filters(&self) -> bool {
        !self.remaining_filters.is_empty()
            || !self.equivalence_map.is_empty()
            || !self.constant_values.values().all(|v| v.is_empty())
    }

    /// Generate optimized filters from the combiner.
    ///
    /// This consumes the combiner state and returns a list of optimized filter expressions.
    pub fn generate_filters(&mut self) -> Vec<Expression> {
        let mut result = Vec::new();

        // First, add remaining filters
        result.append(&mut self.remaining_filters);

        // Generate equality filters between expressions in the same equivalence set
        for (equiv_set, keys) in &self.equivalence_map {
            // Generate equality comparisons between all pairs
            for i in 0..keys.len() {
                for j in (i + 1)..keys.len() {
                    if let (Some(left), Some(right)) = (
                        self.stored_expressions.get(&keys[i]),
                        self.stored_expressions.get(&keys[j]),
                    ) {
                        let comparison = Expression::Comparison(ComparisonExpression::new(
                            ComparisonType::Equal,
                            left.clone(),
                            right.clone(),
                        ));
                        result.push(comparison);
                    }
                }

                // Generate constant comparisons for each expression
                if let Some(constant_list) = self.constant_values.get(equiv_set) {
                    if let Some(expr) = self.stored_expressions.get(&keys[i]) {
                        // Try to generate a BETWEEN expression if we have both lower and upper bounds
                        let mut lower_bound: Option<&ExpressionValueInformation> = None;
                        let mut upper_bound: Option<&ExpressionValueInformation> = None;

                        for info in constant_list {
                            match info.comparison_type {
                                ComparisonType::GreaterThan
                                | ComparisonType::GreaterThanOrEqual => {
                                    lower_bound = Some(info);
                                }
                                ComparisonType::LessThan | ComparisonType::LessThanOrEqual => {
                                    upper_bound = Some(info);
                                }
                                _ => {
                                    // Generate individual comparison
                                    let constant = Expression::Constant(ConstantExpression {
                                        value: info.constant.clone(),
                                        return_type: expr.return_type(),
                                    });
                                    let comparison =
                                        Expression::Comparison(ComparisonExpression::new(
                                            info.comparison_type,
                                            expr.clone(),
                                            constant,
                                        ));
                                    result.push(comparison);
                                }
                            }
                        }

                        // Generate range comparisons
                        if let Some(lower) = lower_bound {
                            let constant = Expression::Constant(ConstantExpression {
                                value: lower.constant.clone(),
                                return_type: expr.return_type(),
                            });
                            let comparison = Expression::Comparison(ComparisonExpression::new(
                                lower.comparison_type,
                                expr.clone(),
                                constant,
                            ));
                            result.push(comparison);
                        }

                        if let Some(upper) = upper_bound {
                            let constant = Expression::Constant(ConstantExpression {
                                value: upper.constant.clone(),
                                return_type: expr.return_type(),
                            });
                            let comparison = Expression::Comparison(ComparisonExpression::new(
                                upper.comparison_type,
                                expr.clone(),
                                constant,
                            ));
                            result.push(comparison);
                        }
                    }
                }
            }
        }

        // Clear state
        self.stored_expressions.clear();
        self.equivalence_set_map.clear();
        self.constant_values.clear();
        self.equivalence_map.clear();

        result
    }

    /// Internal method to add a filter.
    fn add_filter_internal(&mut self, expr: &Expression) -> FilterResult {
        match expr {
            Expression::Comparison(comp) => self.add_comparison_filter(comp),
            Expression::Conjunction(conj) => self.add_conjunction_filter(conj),
            Expression::Constant(c) => {
                // Scalar condition - check if it's always true or false
                if c.return_type == LogicalType::Boolean {
                    if c.value.is_null() {
                        return FilterResult::Unsatisfiable;
                    }
                    match &c.value {
                        Value::Boolean(true) => FilterResult::Success,
                        Value::Boolean(false) => FilterResult::Unsatisfiable,
                        _ => FilterResult::Unsupported,
                    }
                } else {
                    FilterResult::Unsupported
                }
            }
            _ => FilterResult::Unsupported,
        }
    }

    /// Add a comparison filter.
    fn add_comparison_filter(&mut self, comp: &ComparisonExpression) -> FilterResult {
        // Check if comparison type is supported
        if !Self::is_supported_comparison(comp.comparison_type) {
            return FilterResult::Unsupported;
        }

        // Check if one side is a constant
        let left_is_constant = matches!(comp.left.as_ref(), Expression::Constant(_));
        let right_is_constant = matches!(comp.right.as_ref(), Expression::Constant(_));

        if left_is_constant || right_is_constant {
            // Comparison with constant
            self.add_constant_comparison(comp, left_is_constant)
        } else {
            // Comparison between two non-constants
            self.add_non_constant_comparison(comp)
        }
    }

    /// Add a comparison with a constant value.
    fn add_constant_comparison(
        &mut self,
        comp: &ComparisonExpression,
        left_is_constant: bool,
    ) -> FilterResult {
        let (node_expr, constant_expr, comparison_type) = if left_is_constant {
            (
                comp.right.as_ref(),
                comp.left.as_ref(),
                Self::flip_comparison(comp.comparison_type),
            )
        } else {
            (
                comp.left.as_ref(),
                comp.right.as_ref(),
                comp.comparison_type,
            )
        };

        // Get the constant value
        let constant_value = match constant_expr {
            Expression::Constant(c) => c.value.clone(),
            _ => return FilterResult::Unsupported,
        };

        // NULL comparisons are always unsatisfiable (except for IS DISTINCT FROM)
        if constant_value.is_null()
            && comparison_type != ComparisonType::DistinctFrom
            && comparison_type != ComparisonType::NotDistinctFrom
        {
            return FilterResult::Unsatisfiable;
        }

        // Get or create the equivalence set for this expression
        let equiv_set = match self.get_or_create_equivalence_set(node_expr) {
            Some(set) => set,
            None => return FilterResult::Unsupported,
        };

        // Create the value information
        let info = ExpressionValueInformation {
            constant: constant_value.clone(),
            comparison_type,
        };

        // Add to constant values
        let info_list = self.constant_values.entry(equiv_set).or_default();
        let result = Self::add_constant_to_list_static(info_list, info);
        result
    }

    /// Add a comparison between two non-constant expressions.
    fn add_non_constant_comparison(&mut self, comp: &ComparisonExpression) -> FilterResult {
        // Only handle equality comparisons between non-constants
        if comp.comparison_type != ComparisonType::Equal {
            return FilterResult::Unsupported;
        }

        let left_key = match ExpressionKey::from_expression(comp.left.as_ref()) {
            Some(k) => k,
            None => return FilterResult::Unsupported,
        };

        let right_key = match ExpressionKey::from_expression(comp.right.as_ref()) {
            Some(k) => k,
            None => return FilterResult::Unsupported,
        };

        if left_key == right_key {
            // Same expression, trivially true
            return FilterResult::Success;
        }

        // Store expressions
        self.stored_expressions
            .entry(left_key.clone())
            .or_insert_with(|| comp.left.as_ref().clone());
        self.stored_expressions
            .entry(right_key.clone())
            .or_insert_with(|| comp.right.as_ref().clone());

        // Get or create equivalence sets
        let left_set = self.get_or_create_equivalence_set_by_key(&left_key);
        let right_set = self.get_or_create_equivalence_set_by_key(&right_key);

        if left_set == right_set {
            // Already in the same equivalence set
            return FilterResult::Success;
        }

        // Merge right set into left set
        if let Some(right_keys) = self.equivalence_map.remove(&right_set) {
            for key in right_keys {
                self.equivalence_set_map.insert(key.clone(), left_set);
                self.equivalence_map.entry(left_set).or_default().push(key);
            }
        }

        // Merge constant values - take ownership to avoid borrow issues
        if let Some(right_constants) = self.constant_values.remove(&right_set) {
            for info in right_constants {
                let left_constants = self.constant_values.entry(left_set).or_default();
                if Self::add_constant_to_list_static(left_constants, info)
                    == FilterResult::Unsatisfiable
                {
                    return FilterResult::Unsatisfiable;
                }
            }
        }

        FilterResult::Success
    }

    /// Add a conjunction (AND/OR) filter.
    fn add_conjunction_filter(&mut self, conj: &ConjunctionExpression) -> FilterResult {
        match conj.conjunction_type {
            ConjunctionType::And => {
                // For AND, all children must be satisfiable
                for (_i, child) in conj.children.iter().enumerate() {
                    let result = self.add_filter_internal(child);
                    if result == FilterResult::Unsatisfiable {
                        return FilterResult::Unsatisfiable;
                    }
                }
                FilterResult::Success
            }
            ConjunctionType::Or => {
                // OR filters are not directly supported by the combiner
                FilterResult::Unsupported
            }
        }
    }

    /// Get or create an equivalence set for an expression.
    fn get_or_create_equivalence_set(&mut self, expr: &Expression) -> Option<usize> {
        let key = ExpressionKey::from_expression(expr)?;

        // Store the expression
        self.stored_expressions
            .entry(key.clone())
            .or_insert_with(|| expr.clone());

        Some(self.get_or_create_equivalence_set_by_key(&key))
    }

    /// Get or create an equivalence set by key.
    fn get_or_create_equivalence_set_by_key(&mut self, key: &ExpressionKey) -> usize {
        if let Some(&set) = self.equivalence_set_map.get(key) {
            return set;
        }

        let set = self.set_index;
        self.set_index += 1;
        self.equivalence_set_map.insert(key.clone(), set);
        self.equivalence_map
            .entry(set)
            .or_default()
            .push(key.clone());
        self.constant_values.entry(set).or_default();
        set
    }

    /// Add a constant comparison to a list, pruning redundant comparisons.
    fn add_constant_to_list_static(
        info_list: &mut Vec<ExpressionValueInformation>,
        info: ExpressionValueInformation,
    ) -> FilterResult {
        if info.constant.is_null() {
            return FilterResult::Unsatisfiable;
        }

        let mut i = 0;
        while i < info_list.len() {
            let comparison = Self::compare_value_information(&info_list[i], &info);
            match comparison {
                ValueComparisonResult::PruneLeft => {
                    // Remove the existing entry
                    info_list.remove(i);
                    // Don't increment i
                }
                ValueComparisonResult::PruneRight => {
                    // The new info is redundant, don't add it
                    return FilterResult::Success;
                }
                ValueComparisonResult::Unsatisfiable => {
                    // Combination is unsatisfiable
                    info_list.push(info);
                    return FilterResult::Unsatisfiable;
                }
                ValueComparisonResult::PruneNothing => {
                    i += 1;
                }
            }
        }

        // Add the new info
        info_list.push(info);
        FilterResult::Success
    }

    /// Compare two value information entries to determine pruning.
    fn compare_value_information(
        left: &ExpressionValueInformation,
        right: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        // Handle equality comparisons specially
        if left.comparison_type == ComparisonType::Equal {
            return Self::compare_with_equality(left, right);
        }
        if right.comparison_type == ComparisonType::Equal {
            return Self::invert_comparison_result(Self::compare_with_equality(right, left));
        }

        // Handle not-equal comparisons
        if left.comparison_type == ComparisonType::NotEqual {
            return Self::compare_with_not_equal(left, right);
        }
        if right.comparison_type == ComparisonType::NotEqual {
            return Self::invert_comparison_result(Self::compare_with_not_equal(right, left));
        }

        // Both are range comparisons
        let left_is_greater = Self::is_greater_than(left.comparison_type);
        let right_is_greater = Self::is_greater_than(right.comparison_type);

        if left_is_greater && right_is_greater {
            // Both are > or >=
            Self::compare_greater_than_bounds(left, right)
        } else if !left_is_greater && !right_is_greater {
            // Both are < or <=
            Self::compare_less_than_bounds(left, right)
        } else if !left_is_greater && right_is_greater {
            // left is < and right is >
            Self::check_range_satisfiability(left, right)
        } else {
            // left is > and right is <
            Self::invert_comparison_result(Self::check_range_satisfiability(right, left))
        }
    }

    /// Compare when left is an equality.
    fn compare_with_equality(
        left: &ExpressionValueInformation,
        right: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        let prune_right = match right.comparison_type {
            ComparisonType::LessThan => left.constant < right.constant,
            ComparisonType::LessThanOrEqual => left.constant <= right.constant,
            ComparisonType::GreaterThan => left.constant > right.constant,
            ComparisonType::GreaterThanOrEqual => left.constant >= right.constant,
            ComparisonType::NotEqual => left.constant != right.constant,
            ComparisonType::Equal => left.constant == right.constant,
            _ => false,
        };

        if prune_right {
            ValueComparisonResult::PruneRight
        } else {
            ValueComparisonResult::Unsatisfiable
        }
    }

    /// Compare when left is a not-equal.
    fn compare_with_not_equal(
        left: &ExpressionValueInformation,
        right: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        let prune_left = match right.comparison_type {
            ComparisonType::LessThan => left.constant >= right.constant,
            ComparisonType::LessThanOrEqual => left.constant > right.constant,
            ComparisonType::GreaterThan => left.constant <= right.constant,
            ComparisonType::GreaterThanOrEqual => left.constant < right.constant,
            ComparisonType::NotEqual => left.constant == right.constant,
            _ => false,
        };

        if prune_left {
            ValueComparisonResult::PruneLeft
        } else {
            ValueComparisonResult::PruneNothing
        }
    }

    /// Compare two greater-than bounds.
    fn compare_greater_than_bounds(
        left: &ExpressionValueInformation,
        right: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        if left.constant > right.constant {
            ValueComparisonResult::PruneRight
        } else if left.constant < right.constant {
            ValueComparisonResult::PruneLeft
        } else {
            // Same value - prefer the stricter comparison (> over >=)
            if left.comparison_type == ComparisonType::GreaterThanOrEqual {
                ValueComparisonResult::PruneLeft
            } else {
                ValueComparisonResult::PruneRight
            }
        }
    }

    /// Compare two less-than bounds.
    fn compare_less_than_bounds(
        left: &ExpressionValueInformation,
        right: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        if left.constant < right.constant {
            ValueComparisonResult::PruneRight
        } else if left.constant > right.constant {
            ValueComparisonResult::PruneLeft
        } else {
            // Same value - prefer the stricter comparison (< over <=)
            if left.comparison_type == ComparisonType::LessThanOrEqual {
                ValueComparisonResult::PruneLeft
            } else {
                ValueComparisonResult::PruneRight
            }
        }
    }

    /// Check if a range (< and >) is satisfiable.
    fn check_range_satisfiability(
        less_than: &ExpressionValueInformation,
        greater_than: &ExpressionValueInformation,
    ) -> ValueComparisonResult {
        // The less-than constant must be greater than the greater-than constant
        if less_than.constant >= greater_than.constant {
            ValueComparisonResult::PruneNothing
        } else {
            ValueComparisonResult::Unsatisfiable
        }
    }

    /// Invert a comparison result.
    fn invert_comparison_result(result: ValueComparisonResult) -> ValueComparisonResult {
        match result {
            ValueComparisonResult::PruneLeft => ValueComparisonResult::PruneRight,
            ValueComparisonResult::PruneRight => ValueComparisonResult::PruneLeft,
            other => other,
        }
    }

    /// Check if a comparison type is a greater-than variant.
    fn is_greater_than(comp_type: ComparisonType) -> bool {
        matches!(
            comp_type,
            ComparisonType::GreaterThan | ComparisonType::GreaterThanOrEqual
        )
    }

    /// Check if a comparison type is supported.
    fn is_supported_comparison(comp_type: ComparisonType) -> bool {
        matches!(
            comp_type,
            ComparisonType::Equal
                | ComparisonType::NotEqual
                | ComparisonType::LessThan
                | ComparisonType::LessThanOrEqual
                | ComparisonType::GreaterThan
                | ComparisonType::GreaterThanOrEqual
        )
    }

    /// Flip a comparison type (for when constant is on the left).
    fn flip_comparison(comp_type: ComparisonType) -> ComparisonType {
        match comp_type {
            ComparisonType::LessThan => ComparisonType::GreaterThan,
            ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
            ComparisonType::GreaterThan => ComparisonType::LessThan,
            ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
            other => other, // Equal, NotEqual, etc. are symmetric
        }
    }
}

impl Default for FilterCombiner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::expression::ColumnRefExpression;

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_null_constant() -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Null(LogicalType::Integer),
            return_type: LogicalType::Integer,
        })
    }

    fn make_comparison(
        comp_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comp_type, left, right))
    }

    fn make_and(children: Vec<Expression>) -> Expression {
        Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children,
        })
    }

    #[test]
    fn test_filter_combiner_new() {
        let combiner = FilterCombiner::new();
        assert!(!combiner.has_filters());
    }

    #[test]
    fn test_add_simple_comparison() {
        let mut combiner = FilterCombiner::new();

        // x > 5
        let filter = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );

        let result = combiner.add_filter(filter);
        assert_eq!(result, FilterResult::Success);
        assert!(combiner.has_filters());
    }

    #[test]
    fn test_prune_redundant_greater_than() {
        let mut combiner = FilterCombiner::new();

        // x > 5 AND x > 7 should become x > 7
        let filter1 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(7),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // Should have one filter: x > 7
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::GreaterThan);
            if let Expression::Constant(c) = comp.right.as_ref() {
                assert_eq!(c.value, Value::Integer(7));
            } else {
                panic!("Expected constant on right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_prune_redundant_less_than() {
        let mut combiner = FilterCombiner::new();

        // x < 10 AND x < 5 should become x < 5
        let filter1 = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 0),
            make_constant(10),
        );
        let filter2 = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 0),
            make_constant(5),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::LessThan);
            if let Expression::Constant(c) = comp.right.as_ref() {
                assert_eq!(c.value, Value::Integer(5));
            } else {
                panic!("Expected constant on right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_unsatisfiable_equal_and_greater() {
        let mut combiner = FilterCombiner::new();

        // x = 5 AND x > 6 is unsatisfiable
        let filter1 = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(6),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Unsatisfiable);
    }

    #[test]
    fn test_unsatisfiable_range() {
        let mut combiner = FilterCombiner::new();

        // x > 10 AND x < 5 is unsatisfiable
        let filter1 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(10),
        );
        let filter2 = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 0),
            make_constant(5),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Unsatisfiable);
    }

    #[test]
    fn test_satisfiable_range() {
        let mut combiner = FilterCombiner::new();

        // x > 5 AND x < 10 is satisfiable
        let filter1 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 0),
            make_constant(10),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // Should have two filters: x > 5 AND x < 10
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_equality_prunes_range() {
        let mut combiner = FilterCombiner::new();

        // x = 7 AND x > 5 should become x = 7 (equality prunes the range)
        let filter1 = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_constant(7),
        );
        let filter2 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // Should have one filter: x = 7
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::Equal);
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_null_comparison_unsatisfiable() {
        let mut combiner = FilterCombiner::new();

        // x = NULL is unsatisfiable
        let filter = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_null_constant(),
        );

        assert_eq!(combiner.add_filter(filter), FilterResult::Unsatisfiable);
    }

    #[test]
    fn test_equivalence_set_merge() {
        let mut combiner = FilterCombiner::new();

        // x = y AND x = 5 should generate y = 5
        let filter1 = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0), // x
            make_column_ref(0, 1), // y
        );
        let filter2 = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0), // x
            make_constant(5),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // Should have: x = y, x = 5, y = 5
        assert!(filters.len() >= 2);
    }

    #[test]
    fn test_constant_on_left_side() {
        let mut combiner = FilterCombiner::new();

        // 5 < x should be treated as x > 5
        let filter = make_comparison(
            ComparisonType::LessThan,
            make_constant(5),
            make_column_ref(0, 0),
        );

        assert_eq!(combiner.add_filter(filter), FilterResult::Success);

        let filters = combiner.generate_filters();
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::GreaterThan);
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_and_conjunction() {
        let mut combiner = FilterCombiner::new();

        // (x > 5 AND x < 10)
        let filter = make_and(vec![
            make_comparison(
                ComparisonType::GreaterThan,
                make_column_ref(0, 0),
                make_constant(5),
            ),
            make_comparison(
                ComparisonType::LessThan,
                make_column_ref(0, 0),
                make_constant(10),
            ),
        ]);

        assert_eq!(combiner.add_filter(filter), FilterResult::Success);

        let filters = combiner.generate_filters();
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_prefer_stricter_comparison() {
        let mut combiner = FilterCombiner::new();

        // x > 5 AND x >= 5 should become x > 5 (stricter)
        let filter1 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::GreaterThanOrEqual,
            make_column_ref(0, 0),
            make_constant(5),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::GreaterThan);
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_not_equal_with_range() {
        let mut combiner = FilterCombiner::new();

        // x <> 5 AND x > 10 should prune x <> 5 (since x > 10 implies x <> 5)
        let filter1 = make_comparison(
            ComparisonType::NotEqual,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(10),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // x > 10 implies x <> 5, so only x > 10 should remain
        assert_eq!(filters.len(), 1);

        if let Expression::Comparison(comp) = &filters[0] {
            assert_eq!(comp.comparison_type, ComparisonType::GreaterThan);
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_unsupported_filter_goes_to_remaining() {
        let mut combiner = FilterCombiner::new();

        // A Reference expression (used in physical layer) is unsupported by FilterCombiner
        let filter = Expression::Reference(paro_planner::expression::ReferenceExpression {
            index: 0,
            return_type: LogicalType::Boolean,
        });

        // Should return Success but add to remaining filters
        assert_eq!(combiner.add_filter(filter), FilterResult::Success);
        assert!(combiner.has_filters());

        let filters = combiner.generate_filters();
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_true_constant_filter() {
        let mut combiner = FilterCombiner::new();

        let filter = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });

        assert_eq!(combiner.add_filter(filter), FilterResult::Success);
    }

    #[test]
    fn test_false_constant_filter() {
        let mut combiner = FilterCombiner::new();

        let filter = Expression::Constant(ConstantExpression {
            value: Value::Boolean(false),
            return_type: LogicalType::Boolean,
        });

        assert_eq!(combiner.add_filter(filter), FilterResult::Unsatisfiable);
    }

    #[test]
    fn test_multiple_columns() {
        let mut combiner = FilterCombiner::new();

        // x > 5 AND y < 10
        let filter1 = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0), // x
            make_constant(5),
        );
        let filter2 = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 1), // y
            make_constant(10),
        );

        assert_eq!(combiner.add_filter(filter1), FilterResult::Success);
        assert_eq!(combiner.add_filter(filter2), FilterResult::Success);

        let filters = combiner.generate_filters();
        // Should have two independent filters
        assert_eq!(filters.len(), 2);
    }
}
