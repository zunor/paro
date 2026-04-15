//! `UNION` / `INTERSECT` / `EXCEPT` (with or without `ALL`). `UNION BY NAME` is not modeled here.

use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;

/// Type of set operation for logical planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    /// UNION - combines results, removes duplicates
    Union,
    /// INTERSECT - returns common rows
    Intersect,
    /// EXCEPT - returns rows in left but not in right
    Except,
}

impl std::fmt::Display for SetOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOpType::Union => write!(f, "UNION"),
            SetOpType::Intersect => write!(f, "INTERSECT"),
            SetOpType::Except => write!(f, "EXCEPT"),
        }
    }
}

/// SetOperation represents UNION, INTERSECT, or EXCEPT operations.
///
/// # Example
/// ```sql
/// SELECT a FROM t1 UNION SELECT b FROM t2
/// ```
#[derive(Debug)]
pub struct SetOperation {
    /// Table index for the result.
    pub table_index: usize,
    /// Number of columns in the result.
    pub column_count: usize,
    /// Left child operator.
    pub left: Box<LogicalPlan>,
    /// Right child operator.
    pub right: Box<LogicalPlan>,
    /// Type of set operation (UNION, INTERSECT, EXCEPT).
    pub setop_type: SetOpType,
    /// Whether to keep all rows (ALL) or remove duplicates (DISTINCT).
    /// - true: UNION ALL, INTERSECT ALL, EXCEPT ALL
    /// - false: UNION, INTERSECT, EXCEPT (removes duplicates)
    pub setop_all: bool,
    /// Whether UNION statements can be executed out of order.
    /// Only applicable to UNION operations.
    pub allow_out_of_order: bool,
    /// Result column types (resolved from children).
    pub types: Vec<LogicalType>,
}

impl SetOperation {
    /// Create a new set operation with two children.
    pub fn new(
        table_index: usize,
        left: LogicalPlan,
        right: LogicalPlan,
        setop_type: SetOpType,
        setop_all: bool,
        types: Vec<LogicalType>,
    ) -> Self {
        let column_count = types.len();
        Self {
            table_index,
            column_count,
            left: Box::new(left),
            right: Box::new(right),
            setop_type,
            setop_all,
            allow_out_of_order: true,
            types,
        }
    }

    /// Create a UNION operation.
    pub fn union(
        table_index: usize,
        left: LogicalPlan,
        right: LogicalPlan,
        setop_all: bool,
        types: Vec<LogicalType>,
    ) -> Self {
        Self::new(table_index, left, right, SetOpType::Union, setop_all, types)
    }

    /// Create an INTERSECT operation.
    pub fn intersect(
        table_index: usize,
        left: LogicalPlan,
        right: LogicalPlan,
        setop_all: bool,
        types: Vec<LogicalType>,
    ) -> Self {
        Self::new(
            table_index,
            left,
            right,
            SetOpType::Intersect,
            setop_all,
            types,
        )
    }

    /// Create an EXCEPT operation.
    pub fn except(
        table_index: usize,
        left: LogicalPlan,
        right: LogicalPlan,
        setop_all: bool,
        types: Vec<LogicalType>,
    ) -> Self {
        Self::new(
            table_index,
            left,
            right,
            SetOpType::Except,
            setop_all,
            types,
        )
    }

    /// Get the output types for this set operation.
    /// Types are resolved from the first child (left).
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.types.clone()
    }

    /// Get the left child.
    pub fn left(&self) -> &LogicalPlan {
        &self.left
    }

    /// Get the right child.
    pub fn right(&self) -> &LogicalPlan {
        &self.right
    }

    /// Get mutable reference to the left child.
    pub fn left_mut(&mut self) -> &mut LogicalPlan {
        &mut self.left
    }

    /// Get mutable reference to the right child.
    pub fn right_mut(&mut self) -> &mut LogicalPlan {
        &mut self.right
    }

    /// Check if this is a UNION ALL operation.
    pub fn is_union_all(&self) -> bool {
        self.setop_type == SetOpType::Union && self.setop_all
    }

    /// Get the name of this operation for display.
    pub fn name(&self) -> String {
        let suffix = if self.setop_all { " ALL" } else { "" };
        format!("{}{}", self.setop_type, suffix)
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::context::BindContext;
    use crate::binder::deep_copy::duplicate_plan_preserving_indices;

    use super::*;

    fn dummy_pair() -> (LogicalPlan, LogicalPlan) {
        let ctx = BindContext::new();
        (LogicalPlan::dummy_scan(&ctx), LogicalPlan::dummy_scan(&ctx))
    }

    #[test]
    fn test_setop_type_display() {
        assert_eq!(format!("{}", SetOpType::Union), "UNION");
        assert_eq!(format!("{}", SetOpType::Intersect), "INTERSECT");
        assert_eq!(format!("{}", SetOpType::Except), "EXCEPT");
    }

    #[test]
    fn test_set_operation_name() {
        let dup_ctx = BindContext::new();
        let (left, right) = dummy_pair();
        let types = vec![LogicalType::Integer];

        let union = SetOperation::union(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            false,
            types.clone(),
        );
        assert_eq!(union.name(), "UNION");

        let union_all = SetOperation::union(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            true,
            types.clone(),
        );
        assert_eq!(union_all.name(), "UNION ALL");

        let intersect = SetOperation::intersect(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            false,
            types.clone(),
        );
        assert_eq!(intersect.name(), "INTERSECT");

        let except_all = SetOperation::except(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            true,
            types.clone(),
        );
        assert_eq!(except_all.name(), "EXCEPT ALL");
    }

    #[test]
    fn test_set_operation_is_union_all() {
        let dup_ctx = BindContext::new();
        let (left, right) = dummy_pair();
        let types = vec![LogicalType::Integer];

        let union = SetOperation::union(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            false,
            types.clone(),
        );
        assert!(!union.is_union_all());

        let union_all = SetOperation::union(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            true,
            types.clone(),
        );
        assert!(union_all.is_union_all());

        let intersect_all = SetOperation::intersect(
            0,
            duplicate_plan_preserving_indices(&left, dup_ctx.shared().as_ref()),
            duplicate_plan_preserving_indices(&right, dup_ctx.shared().as_ref()),
            true,
            types.clone(),
        );
        assert!(!intersect_all.is_union_all());
    }

    #[test]
    fn test_set_operation_types() {
        let (left, right) = dummy_pair();
        let types = vec![LogicalType::Integer, LogicalType::Varchar];

        let union = SetOperation::union(0, left, right, false, types.clone());
        assert_eq!(union.get_types(), types);
        assert_eq!(union.column_count, 2);
    }

    #[test]
    fn test_set_operation_children() {
        let (left, right) = dummy_pair();
        let types = vec![LogicalType::Integer];

        let union = SetOperation::union(0, left, right, false, types);

        // Verify children are accessible
        assert!(matches!(
            union.left().operator,
            crate::operator::LogicalOperator::DummyScan
        ));
        assert!(matches!(
            union.right().operator,
            crate::operator::LogicalOperator::DummyScan
        ));
    }
}
