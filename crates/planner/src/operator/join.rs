// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Inner/outer/cross/semi/anti joins and comparison or general join conditions.
//! Mark/single/asof joins and dedup columns are modeled elsewhere or not yet.

use std::collections::HashSet;

use super::ColumnBinding;
use crate::expression::Expression;
use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;

/// Type of join operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Invalid join type
    Invalid,
    /// LEFT [OUTER] JOIN - all rows from left, matching from right (NULL if no match)
    Left,
    /// RIGHT [OUTER] JOIN - all rows from right, matching from left (NULL if no match)
    Right,
    /// INNER JOIN - only matching rows from both sides
    Inner,
    /// FULL [OUTER] JOIN - all rows from both sides (NULL where no match)
    Outer,
    /// LEFT SEMI JOIN - rows from left that have a match in right (no right columns)
    Semi,
    /// LEFT ANTI JOIN - rows from left that have NO match in right
    Anti,
    /// MARK JOIN - returns marker indicating whether there is a join partner
    Mark,
    /// SINGLE JOIN - like LEFT OUTER but returns at most one partner per left entry
    Single,
    /// RIGHT SEMI JOIN - rows from right that have a match in left (no left columns)
    RightSemi,
    /// RIGHT ANTI JOIN - rows from right that have NO match in left
    RightAnti,
}

/// NULL semantics used by a left anti join.
///
/// A regular anti join implements `NOT EXISTS`: NULL probe keys simply do not
/// match. A null-aware anti join implements a scalar `NOT IN`: a NULL anywhere
/// on the build side rejects every probe row, while NULL probe keys are
/// rejected whenever the build side is non-empty.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AntiJoinMode {
    #[default]
    Regular,
    NullAware,
}

impl JoinType {
    /// Returns true if this is a left or full outer join.
    pub fn is_left_outer(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Outer)
    }

    /// Returns true if this is a right or full outer join.
    pub fn is_right_outer(&self) -> bool {
        matches!(self, JoinType::Right | JoinType::Outer)
    }

    /// Returns true if the build side is propagated out of the join.
    pub fn propagates_build_side(&self) -> bool {
        matches!(
            self,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer | JoinType::Single
        )
    }

    /// Returns the inverse join type if it exists.
    pub fn inverse(&self) -> Option<JoinType> {
        match self {
            JoinType::Left => Some(JoinType::Right),
            JoinType::Right => Some(JoinType::Left),
            JoinType::Inner => Some(JoinType::Inner),
            JoinType::Outer => Some(JoinType::Outer),
            JoinType::Semi => Some(JoinType::RightSemi),
            JoinType::RightSemi => Some(JoinType::Semi),
            JoinType::Anti => Some(JoinType::RightAnti),
            JoinType::RightAnti => Some(JoinType::Anti),
            _ => None,
        }
    }
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Invalid => write!(f, "INVALID"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Outer => write!(f, "FULL OUTER"),
            JoinType::Semi => write!(f, "SEMI"),
            JoinType::Anti => write!(f, "ANTI"),
            JoinType::Mark => write!(f, "MARK"),
            JoinType::Single => write!(f, "SINGLE"),
            JoinType::RightSemi => write!(f, "RIGHT SEMI"),
            JoinType::RightAnti => write!(f, "RIGHT ANTI"),
        }
    }
}

fn project_types(child_types: &[LogicalType], projection_map: &[usize]) -> Vec<LogicalType> {
    projection_map
        .iter()
        .filter_map(|&idx| child_types.get(idx).cloned())
        .collect()
}

fn project_bindings(
    child_bindings: &[ColumnBinding],
    projection_map: &[usize],
) -> Vec<ColumnBinding> {
    projection_map
        .iter()
        .filter_map(|&idx| child_bindings.get(idx).copied())
        .collect()
}

fn full_projection(width: usize) -> Vec<usize> {
    (0..width).collect()
}

fn default_join_projections(
    join_type: JoinType,
    left_width: usize,
    right_width: usize,
) -> (Vec<usize>, Vec<usize>) {
    match join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => {
            (full_projection(left_width), Vec::new())
        }
        JoinType::RightSemi | JoinType::RightAnti => (Vec::new(), full_projection(right_width)),
        _ => (full_projection(left_width), full_projection(right_width)),
    }
}

fn join_output_types(
    join_type: JoinType,
    left_types: Vec<LogicalType>,
    right_types: Vec<LogicalType>,
) -> Vec<LogicalType> {
    match join_type {
        JoinType::Semi | JoinType::Anti => left_types,
        JoinType::RightSemi | JoinType::RightAnti => right_types,
        JoinType::Mark => {
            let mut types = left_types;
            types.push(LogicalType::Boolean);
            types
        }
        _ => {
            let mut types = left_types;
            types.extend(right_types);
            types
        }
    }
}

fn join_output_bindings(
    join_type: JoinType,
    left_bindings: Vec<ColumnBinding>,
    right_bindings: Vec<ColumnBinding>,
    mark_index: Option<usize>,
) -> Vec<ColumnBinding> {
    match join_type {
        JoinType::Semi | JoinType::Anti => left_bindings,
        JoinType::RightSemi | JoinType::RightAnti => right_bindings,
        JoinType::Mark => {
            let mut bindings = left_bindings;
            bindings.push(ColumnBinding::new(mark_index.unwrap_or(0), 0));
            bindings
        }
        _ => {
            let mut bindings = left_bindings;
            bindings.extend(right_bindings);
            bindings
        }
    }
}

/// Comparison type for join conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinComparisonType {
    /// Equal (=)
    Equal,
    /// Not equal (<> or !=)
    NotEqual,
    /// Less than (<)
    LessThan,
    /// Greater than (>)
    GreaterThan,
    /// Less than or equal (<=)
    LessThanOrEqual,
    /// Greater than or equal (>=)
    GreaterThanOrEqual,
    /// IS NOT DISTINCT FROM
    NotDistinctFrom,
    /// IS DISTINCT FROM
    DistinctFrom,
}

impl JoinComparisonType {
    /// Flip the comparison (swap left and right operands).
    pub fn flip(&self) -> Self {
        match self {
            JoinComparisonType::Equal => JoinComparisonType::Equal,
            JoinComparisonType::NotEqual => JoinComparisonType::NotEqual,
            JoinComparisonType::LessThan => JoinComparisonType::GreaterThan,
            JoinComparisonType::GreaterThan => JoinComparisonType::LessThan,
            JoinComparisonType::LessThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
            JoinComparisonType::GreaterThanOrEqual => JoinComparisonType::LessThanOrEqual,
            JoinComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
            JoinComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
        }
    }
}

/// A single join condition comparing expressions from left and right sides.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Expression from the left side of the join.
    pub left: Expression,
    /// Expression from the right side of the join.
    pub right: Expression,
    /// The comparison type.
    pub comparison: JoinComparisonType,
}

impl JoinCondition {
    /// Create a new join condition.
    pub fn new(left: Expression, right: Expression, comparison: JoinComparisonType) -> Self {
        Self {
            left,
            right,
            comparison,
        }
    }

    /// Create an equality join condition.
    pub fn equality(left: Expression, right: Expression) -> Self {
        Self::new(left, right, JoinComparisonType::Equal)
    }
}

/// Indicates which side of a join an expression references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    /// Expression references neither side (constant).
    None,
    /// Expression references only the left side.
    Left,
    /// Expression references only the right side.
    Right,
    /// Expression references both sides.
    Both,
}

impl JoinSide {
    /// Combine two join sides.
    pub fn combine(left: JoinSide, right: JoinSide) -> JoinSide {
        if left == JoinSide::Both || right == JoinSide::Both {
            return JoinSide::Both;
        }
        if left == JoinSide::None {
            return right;
        }
        if right == JoinSide::None {
            return left;
        }
        if left != right {
            return JoinSide::Both;
        }
        left
    }

    /// Get the join side for a table binding.
    pub fn get_side(
        table_binding: usize,
        left_bindings: &HashSet<usize>,
        right_bindings: &HashSet<usize>,
    ) -> JoinSide {
        let in_left = left_bindings.contains(&table_binding);
        let in_right = right_bindings.contains(&table_binding);

        match (in_left, in_right) {
            (true, true) => JoinSide::Both,
            (true, false) => JoinSide::Left,
            (false, true) => JoinSide::Right,
            (false, false) => JoinSide::None,
        }
    }
}

/// ComparisonJoin represents a join with comparison conditions.
/// This is the most common type of join (e.g., a.id = b.id).
#[derive(Debug)]
pub struct ComparisonJoin {
    /// The type of join (INNER, LEFT, RIGHT, etc.)
    pub join_type: JoinType,
    /// Three-valued-logic mode for `JoinType::Anti`.
    pub anti_join_mode: AntiJoinMode,
    /// Left child operator.
    pub left: Box<LogicalPlan>,
    /// Right child operator.
    pub right: Box<LogicalPlan>,
    /// The comparison conditions (e.g., a.id = b.id).
    pub conditions: Vec<JoinCondition>,
    /// Table index for MARK join results.
    pub mark_index: Option<usize>,
    /// First condition whose NULL result contributes UNKNOWN to a MARK join.
    ///
    /// Correlated MARK joins evaluate correlation predicates before the actual
    /// ANY/IN payload comparison. NULL in those correlation predicates means
    /// the RHS row is not part of this outer row's subquery result, while NULL
    /// in the payload comparison preserves SQL UNKNOWN semantics.
    pub mark_null_condition_start: Option<usize>,
    /// Columns that are duplicate-eliminated and pushed into the RHS.
    pub duplicate_eliminated_columns: Vec<Expression>,
    /// Whether the delim join has been flipped to de-duplicating the RHS instead.
    pub delim_flipped: bool,
    /// Columns from left side to output.
    pub left_projection_map: Vec<usize>,
    /// Columns from right side to output.
    pub right_projection_map: Vec<usize>,
}

impl ComparisonJoin {
    fn is_hash_equality(comparison: JoinComparisonType) -> bool {
        matches!(
            comparison,
            JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
        )
    }

    /// Create a new comparison join.
    pub fn new(
        join_type: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        conditions: Vec<JoinCondition>,
    ) -> Self {
        let (left_projection_map, right_projection_map) =
            default_join_projections(join_type, left.types().len(), right.types().len());
        Self {
            join_type,
            anti_join_mode: AntiJoinMode::Regular,
            left: Box::new(left),
            right: Box::new(right),
            conditions,
            mark_index: None,
            mark_null_condition_start: matches!(join_type, JoinType::Mark).then_some(0),
            duplicate_eliminated_columns: vec![],
            delim_flipped: false,
            left_projection_map,
            right_projection_map,
        }
    }

    /// Lower a MARK join consumed by `NOT(marker)` to a null-aware anti join.
    pub fn make_null_aware_anti(&mut self) {
        self.join_type = JoinType::Anti;
        self.anti_join_mode = AntiJoinMode::NullAware;
        self.mark_index = None;
        self.mark_null_condition_start = None;
    }

    /// Check if this join has at least one equality condition.
    pub fn has_equality(&self) -> bool {
        self.conditions
            .iter()
            .any(|c| Self::is_hash_equality(c.comparison))
    }

    /// Count the number of range conditions (non-equality).
    pub fn range_count(&self) -> usize {
        self.conditions
            .iter()
            .filter(|c| !Self::is_hash_equality(c.comparison))
            .count()
    }

    /// Get the output types for this join.
    pub fn get_types(&self) -> Vec<LogicalType> {
        let left_types = project_types(&self.left.types(), &self.left_projection_map);
        let right_types = project_types(&self.right.types(), &self.right_projection_map);
        join_output_types(self.join_type, left_types, right_types)
    }

    pub fn get_column_bindings(
        &self,
        left_bindings: &[ColumnBinding],
        right_bindings: &[ColumnBinding],
    ) -> Vec<ColumnBinding> {
        let projected_left = project_bindings(left_bindings, &self.left_projection_map);
        let projected_right = project_bindings(right_bindings, &self.right_projection_map);
        join_output_bindings(
            self.join_type,
            projected_left,
            projected_right,
            self.mark_index,
        )
    }
}

/// AnyJoin represents a join with an arbitrary condition.
/// Used when the join condition cannot be expressed as simple comparisons.
#[derive(Debug)]
pub struct AnyJoin {
    /// The type of join (INNER, LEFT, RIGHT, etc.)
    pub join_type: JoinType,
    /// Left child operator.
    pub left: Box<LogicalPlan>,
    /// Right child operator.
    pub right: Box<LogicalPlan>,
    /// The arbitrary join condition.
    pub condition: Expression,
    /// Table index for MARK join results.
    pub mark_index: Option<usize>,
    /// Columns from left side to output.
    pub left_projection_map: Vec<usize>,
    /// Columns from right side to output.
    pub right_projection_map: Vec<usize>,
}

impl AnyJoin {
    /// Create a new any join.
    pub fn new(
        join_type: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expression,
    ) -> Self {
        let (left_projection_map, right_projection_map) =
            default_join_projections(join_type, left.types().len(), right.types().len());
        Self {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            condition,
            mark_index: None,
            left_projection_map,
            right_projection_map,
        }
    }

    /// Get the output types for this join.
    pub fn get_types(&self) -> Vec<LogicalType> {
        let left_types = project_types(&self.left.types(), &self.left_projection_map);
        let right_types = project_types(&self.right.types(), &self.right_projection_map);
        join_output_types(self.join_type, left_types, right_types)
    }

    pub fn get_column_bindings(
        &self,
        left_bindings: &[ColumnBinding],
        right_bindings: &[ColumnBinding],
    ) -> Vec<ColumnBinding> {
        let projected_left = project_bindings(left_bindings, &self.left_projection_map);
        let projected_right = project_bindings(right_bindings, &self.right_projection_map);
        join_output_bindings(
            self.join_type,
            projected_left,
            projected_right,
            self.mark_index,
        )
    }
}

/// CrossProduct represents a cross join (cartesian product).
/// This is a join without any condition.
#[derive(Debug)]
pub struct CrossProduct {
    /// Left child operator.
    pub left: Box<LogicalPlan>,
    /// Right child operator.
    pub right: Box<LogicalPlan>,
}

impl CrossProduct {
    /// Create a new cross product.
    pub fn new(left: LogicalPlan, right: LogicalPlan) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Get the output types for this cross product.
    pub fn get_types(&self) -> Vec<LogicalType> {
        let mut types = self.left.types();
        types.extend(self.right.types());
        types
    }
}

/// Unified Join enum that encompasses all join types.
/// This provides a single entry point for join operations in the logical plan.
#[derive(Debug)]
pub enum Join {
    /// Comparison join (most common, e.g., A.x = B.y)
    Comparison(ComparisonJoin),
    /// Any join (arbitrary condition)
    Any(Box<AnyJoin>),
    /// Cross product (no condition)
    Cross(CrossProduct),
}

impl Join {
    /// Create a comparison join.
    pub fn comparison(
        join_type: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        conditions: Vec<JoinCondition>,
    ) -> Self {
        Join::Comparison(ComparisonJoin::new(join_type, left, right, conditions))
    }

    /// Create an any join.
    pub fn any(
        join_type: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expression,
    ) -> Self {
        Join::Any(Box::new(AnyJoin::new(join_type, left, right, condition)))
    }

    /// Create a cross product.
    pub fn cross(left: LogicalPlan, right: LogicalPlan) -> Self {
        Join::Cross(CrossProduct::new(left, right))
    }

    /// Get the join type.
    pub fn join_type(&self) -> JoinType {
        match self {
            Join::Comparison(j) => j.join_type,
            Join::Any(j) => j.join_type,
            Join::Cross(_) => JoinType::Inner, // Cross is essentially inner with no condition
        }
    }

    /// Get the left child.
    pub fn left(&self) -> &LogicalPlan {
        match self {
            Join::Comparison(j) => j.left.as_ref(),
            Join::Any(j) => j.left.as_ref(),
            Join::Cross(j) => j.left.as_ref(),
        }
    }

    /// Get the right child.
    pub fn right(&self) -> &LogicalPlan {
        match self {
            Join::Comparison(j) => j.right.as_ref(),
            Join::Any(j) => j.right.as_ref(),
            Join::Cross(j) => j.right.as_ref(),
        }
    }

    /// Get mutable reference to the left child.
    pub fn left_mut(&mut self) -> &mut LogicalPlan {
        match self {
            Join::Comparison(j) => j.left.as_mut(),
            Join::Any(j) => j.left.as_mut(),
            Join::Cross(j) => j.left.as_mut(),
        }
    }

    /// Get mutable reference to the right child.
    pub fn right_mut(&mut self) -> &mut LogicalPlan {
        match self {
            Join::Comparison(j) => j.right.as_mut(),
            Join::Any(j) => j.right.as_mut(),
            Join::Cross(j) => j.right.as_mut(),
        }
    }

    /// Get the output types for this join.
    pub fn get_types(&self) -> Vec<LogicalType> {
        match self {
            Join::Comparison(j) => j.get_types(),
            Join::Any(j) => j.get_types(),
            Join::Cross(j) => j.get_types(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::context::BindContext;
    use crate::binder::deep_copy::duplicate_plan_preserving_indices;
    use crate::operator::ExpressionGet;
    use crate::operator::LogicalOperator;

    fn expression_get(table_index: usize, types: Vec<LogicalType>) -> LogicalOperator {
        let names = (0..types.len()).map(|idx| format!("c{}", idx)).collect();
        LogicalOperator::ExpressionGet(ExpressionGet::new(table_index, vec![], names, types))
    }

    fn expression_get_plan(table_index: usize, types: Vec<LogicalType>) -> LogicalPlan {
        let ctx = BindContext::new();
        LogicalPlan::new(&ctx, expression_get(table_index, types))
    }

    fn dummy_plan_pair() -> (LogicalPlan, LogicalPlan) {
        let ctx = BindContext::new();
        (LogicalPlan::dummy_scan(&ctx), LogicalPlan::dummy_scan(&ctx))
    }

    #[test]
    fn test_join_type_display() {
        assert_eq!(format!("{}", JoinType::Inner), "INNER");
        assert_eq!(format!("{}", JoinType::Left), "LEFT");
        assert_eq!(format!("{}", JoinType::Right), "RIGHT");
        assert_eq!(format!("{}", JoinType::Outer), "FULL OUTER");
        assert_eq!(format!("{}", JoinType::Semi), "SEMI");
        assert_eq!(format!("{}", JoinType::Anti), "ANTI");
    }

    #[test]
    fn test_join_type_outer_checks() {
        assert!(JoinType::Left.is_left_outer());
        assert!(JoinType::Outer.is_left_outer());
        assert!(!JoinType::Right.is_left_outer());
        assert!(!JoinType::Inner.is_left_outer());

        assert!(JoinType::Right.is_right_outer());
        assert!(JoinType::Outer.is_right_outer());
        assert!(!JoinType::Left.is_right_outer());
        assert!(!JoinType::Inner.is_right_outer());
    }

    #[test]
    fn test_join_type_inverse() {
        assert_eq!(JoinType::Left.inverse(), Some(JoinType::Right));
        assert_eq!(JoinType::Right.inverse(), Some(JoinType::Left));
        assert_eq!(JoinType::Inner.inverse(), Some(JoinType::Inner));
        assert_eq!(JoinType::Outer.inverse(), Some(JoinType::Outer));
        assert_eq!(JoinType::Semi.inverse(), Some(JoinType::RightSemi));
        assert_eq!(JoinType::RightSemi.inverse(), Some(JoinType::Semi));
        assert_eq!(JoinType::Mark.inverse(), None);
    }

    #[test]
    fn test_comparison_type_flip() {
        assert_eq!(JoinComparisonType::Equal.flip(), JoinComparisonType::Equal);
        assert_eq!(
            JoinComparisonType::LessThan.flip(),
            JoinComparisonType::GreaterThan
        );
        assert_eq!(
            JoinComparisonType::GreaterThan.flip(),
            JoinComparisonType::LessThan
        );
        assert_eq!(
            JoinComparisonType::LessThanOrEqual.flip(),
            JoinComparisonType::GreaterThanOrEqual
        );
    }

    #[test]
    fn test_join_side_combine() {
        assert_eq!(
            JoinSide::combine(JoinSide::None, JoinSide::Left),
            JoinSide::Left
        );
        assert_eq!(
            JoinSide::combine(JoinSide::Left, JoinSide::None),
            JoinSide::Left
        );
        assert_eq!(
            JoinSide::combine(JoinSide::Left, JoinSide::Left),
            JoinSide::Left
        );
        assert_eq!(
            JoinSide::combine(JoinSide::Left, JoinSide::Right),
            JoinSide::Both
        );
        assert_eq!(
            JoinSide::combine(JoinSide::Both, JoinSide::Left),
            JoinSide::Both
        );
        assert_eq!(
            JoinSide::combine(JoinSide::None, JoinSide::None),
            JoinSide::None
        );
    }

    #[test]
    fn test_join_side_get_side() {
        let left_bindings: HashSet<usize> = [0, 1].into_iter().collect();
        let right_bindings: HashSet<usize> = [2, 3].into_iter().collect();

        assert_eq!(
            JoinSide::get_side(0, &left_bindings, &right_bindings),
            JoinSide::Left
        );
        assert_eq!(
            JoinSide::get_side(2, &left_bindings, &right_bindings),
            JoinSide::Right
        );
        assert_eq!(
            JoinSide::get_side(5, &left_bindings, &right_bindings),
            JoinSide::None
        );
    }

    #[test]
    fn test_cross_product_types() {
        // Create dummy operators for testing
        let (left, right) = dummy_plan_pair();

        let cross = CrossProduct::new(left, right);
        let types = cross.get_types();

        // DummyScan returns empty types, so cross product should also be empty
        assert!(types.is_empty());
    }

    #[test]
    fn test_logical_join_enum() {
        let dup_ctx = BindContext::new();
        let (left, right) = dummy_plan_pair();

        // Test cross product creation
        let cross = Join::cross(
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
        );
        assert_eq!(cross.join_type(), JoinType::Inner);

        // Test comparison join creation
        let comp = Join::comparison(
            JoinType::Left,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            vec![],
        );
        assert_eq!(comp.join_type(), JoinType::Left);
    }

    #[test]
    fn comparison_join_treats_not_distinct_from_as_hashable_equality() {
        let (left, right) = dummy_plan_pair();
        let join = ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::new(
                crate::expression::Expression::Reference(
                    crate::expression::ReferenceExpression::new(0, LogicalType::Integer),
                ),
                crate::expression::Expression::Reference(
                    crate::expression::ReferenceExpression::new(0, LogicalType::Integer),
                ),
                JoinComparisonType::NotDistinctFrom,
            )],
        );

        assert!(join.has_equality());
        assert_eq!(join.range_count(), 0);
    }

    #[test]
    fn mark_join_outputs_left_columns_plus_boolean_marker() {
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            expression_get_plan(10, vec![LogicalType::Integer, LogicalType::BigInt]),
            expression_get_plan(20, vec![LogicalType::Varchar]),
            vec![],
        );
        join.left_projection_map = vec![1];

        assert_eq!(
            join.get_types(),
            vec![LogicalType::BigInt, LogicalType::Boolean]
        );
        assert!(join.duplicate_eliminated_columns.is_empty());
        assert!(!join.delim_flipped);
    }

    #[test]
    fn right_semi_and_right_anti_only_output_right_side_types() {
        let right_types = vec![LogicalType::Varchar, LogicalType::Boolean];

        let mut right_semi = ComparisonJoin::new(
            JoinType::RightSemi,
            expression_get_plan(10, vec![LogicalType::Integer]),
            expression_get_plan(20, right_types.clone()),
            vec![],
        );
        right_semi.right_projection_map = vec![1];
        assert_eq!(right_semi.get_types(), vec![LogicalType::Boolean]);

        let right_anti = ComparisonJoin::new(
            JoinType::RightAnti,
            expression_get_plan(10, vec![LogicalType::Integer]),
            expression_get_plan(20, right_types.clone()),
            vec![],
        );
        assert_eq!(right_anti.get_types(), right_types);
    }

    #[test]
    fn any_join_mark_outputs_left_columns_plus_boolean_marker() {
        let mut join = AnyJoin::new(
            JoinType::Mark,
            expression_get_plan(10, vec![LogicalType::Integer, LogicalType::BigInt]),
            expression_get_plan(20, vec![LogicalType::Varchar]),
            crate::expression::Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        join.left_projection_map = vec![1];

        assert_eq!(
            join.get_types(),
            vec![LogicalType::BigInt, LogicalType::Boolean]
        );
    }

    #[test]
    fn any_join_right_semi_and_right_anti_only_output_right_side_types() {
        let right_types = vec![LogicalType::Varchar, LogicalType::Boolean];

        let mut right_semi = AnyJoin::new(
            JoinType::RightSemi,
            expression_get_plan(10, vec![LogicalType::Integer]),
            expression_get_plan(20, right_types.clone()),
            crate::expression::Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        right_semi.right_projection_map = vec![1];
        assert_eq!(right_semi.get_types(), vec![LogicalType::Boolean]);

        let right_anti = AnyJoin::new(
            JoinType::RightAnti,
            expression_get_plan(10, vec![LogicalType::Integer]),
            expression_get_plan(20, right_types.clone()),
            crate::expression::Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        assert_eq!(right_anti.get_types(), right_types);
    }

    #[test]
    fn inner_join_projection_maps_apply_to_both_sides_for_comparison_and_any_join() {
        let dup_ctx = BindContext::new();
        let left = expression_get_plan(10, vec![LogicalType::Integer, LogicalType::BigInt]);
        let right = expression_get_plan(20, vec![LogicalType::Boolean, LogicalType::Varchar]);

        let mut comparison = ComparisonJoin::new(
            JoinType::Inner,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            vec![],
        );
        comparison.left_projection_map = vec![1];
        comparison.right_projection_map = vec![0];

        let mut any = AnyJoin::new(
            JoinType::Inner,
            left,
            right,
            crate::expression::Expression::Constant(crate::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Boolean(true),
                LogicalType::Boolean,
            )),
        );
        any.left_projection_map = vec![1];
        any.right_projection_map = vec![0];

        assert_eq!(
            comparison.get_types(),
            vec![LogicalType::BigInt, LogicalType::Boolean]
        );
        assert_eq!(
            any.get_types(),
            vec![LogicalType::BigInt, LogicalType::Boolean]
        );
    }
}
