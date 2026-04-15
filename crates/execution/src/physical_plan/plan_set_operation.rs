//! Plan Set Operation - Convert SetOperation to Physical Operators
//!
//!
//! ## Dependencies Check
//! - Union: ✅
//! - HashJoin: ✅
//! - NestedLoopJoin: ✅ (current EXCEPT/INTERSECT path for `IS NOT DISTINCT FROM`)
//! - HashAggregate: ✅ (for distinct)
//!
//! ## Implementation Notes
//! - UNION ALL: Union directly
//! - UNION: Union + HashAggregate (for distinct)
//! - EXCEPT: Comparison join with ANTI join type + distinct
//! - INTERSECT: Comparison join with SEMI join type + distinct
//! - EXCEPT ALL / INTERSECT ALL: Not yet implemented (requires window functions)

use super::generator::PhysicalPlanGenerator;
use crate::operator::scan::column_data_scan::{
    ColumnDataCollectionSink, ColumnDataScanBinding, PhysicalColumnDataScan,
};
use crate::operator::set::union::Union;
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
use paro_planner::operator::set_operation::{SetOpType, SetOperation};

use std::sync::Arc;

impl PhysicalPlanGenerator {
    fn create_set_join_conditions(types: &[LogicalType]) -> Vec<JoinCondition> {
        use paro_planner::expression::{Expression, ReferenceExpression};

        let mut conditions = Vec::with_capacity(types.len());
        for (i, typ) in types.iter().enumerate() {
            let left_expr = Expression::Reference(ReferenceExpression::new(i, typ.clone()));
            let right_expr = Expression::Reference(ReferenceExpression::new(i, typ.clone()));

            conditions.push(JoinCondition::new(
                left_expr,
                right_expr,
                JoinComparisonType::NotDistinctFrom,
            ));
        }
        conditions
    }

    /// Create physical plan for SetOperation.
    ///
    /// # Set Operation Mapping
    /// - UNION ALL → Union
    /// - UNION → Union + HashAggregate (for distinct)
    /// - EXCEPT → Comparison join (ANTI) + distinct
    /// - INTERSECT → Comparison join (SEMI) + distinct
    pub fn create_plan_set_operation(
        &self,
        setop: &SetOperation,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Create plans for children
        let left = self.create_plan_from_logical_plan(setop.left.as_ref())?;
        let right = self.create_plan_from_logical_plan(setop.right.as_ref())?;

        // Verify type compatibility
        let left_types = left.types();
        let right_types = right.types();

        if left_types.len() != right_types.len() {
            return Err(paro_error::internal(format!(
                "Type mismatch for SET OPERATION: left has {} columns, right has {}",
                left_types.len(),
                right_types.len()
            )));
        }

        match setop.setop_type {
            SetOpType::Union => self.create_plan_union(setop, left, right),
            SetOpType::Except => self.create_plan_except(setop, left, right),
            SetOpType::Intersect => self.create_plan_intersect(setop, left, right),
        }
    }

    /// Create physical plan for UNION / UNION ALL.
    fn create_plan_union(
        &self,
        setop: &SetOperation,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let mut scans = Vec::with_capacity(2);
        let mut inputs = Vec::with_capacity(2);
        let mut sinks = Vec::with_capacity(2);

        for child in [left, right] {
            let binding = Arc::new(ColumnDataScanBinding::new(None));
            scans.push(Arc::new(PhysicalColumnDataScan::with_binding(
                setop.types.clone(),
                binding.clone(),
            )) as Arc<dyn PhysicalOperator>);
            inputs.push(child.clone());
            sinks.push(Arc::new(ColumnDataCollectionSink::new(
                child.types().to_vec(),
                binding,
            )) as Arc<dyn PhysicalOperator>);
        }

        // Create Union
        let union_op: Arc<dyn PhysicalOperator> = Arc::new(Union::new(
            setop.types.clone(),
            scans,
            inputs,
            sinks,
            setop.allow_out_of_order,
        ));

        if setop.setop_all {
            // UNION ALL: Just return the union
            Ok(union_op)
        } else {
            // UNION: Add HashAggregate for distinct
            self.create_distinct_aggregate(union_op, &setop.types)
        }
    }

    /// Create physical plan for EXCEPT / EXCEPT ALL.
    fn create_plan_except(
        &self,
        setop: &SetOperation,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if setop.setop_all {
            // EXCEPT ALL requires window functions (ROW_NUMBER)
            return Err(paro_error::not_implemented(
                "EXCEPT ALL not yet implemented (requires window functions)".to_string(),
            ));
        }

        // EXCEPT is implemented as ANTI JOIN
        self.create_set_join(setop, left, right, JoinType::Anti)
    }

    /// Create physical plan for INTERSECT / INTERSECT ALL.
    fn create_plan_intersect(
        &self,
        setop: &SetOperation,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if setop.setop_all {
            // INTERSECT ALL requires window functions (ROW_NUMBER)
            return Err(paro_error::not_implemented(
                "INTERSECT ALL not yet implemented (requires window functions)".to_string(),
            ));
        }

        // INTERSECT is implemented as SEMI JOIN
        self.create_set_join(setop, left, right, JoinType::Semi)
    }

    /// Create a comparison join + distinct aggregate for EXCEPT/INTERSECT operations.
    fn create_set_join(
        &self,
        setop: &SetOperation,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let conditions = Self::create_set_join_conditions(&setop.types);
        let join_child: Arc<dyn PhysicalOperator> = Arc::new(
            crate::operator::join::nested_loop_join::NestedLoopJoin::new_comparison(
                left,
                right,
                join_type,
                conditions,
                vec![],
                vec![],
            ),
        );

        self.create_distinct_aggregate(join_child, &setop.types)
    }

    /// Create a HashAggregate for distinct operation (used by UNION).
    fn create_distinct_aggregate(
        &self,
        child: Arc<dyn PhysicalOperator>,
        types: &[paro_common::types::LogicalType],
    ) -> Result<Arc<dyn PhysicalOperator>> {
        use crate::operator::aggregate::grouped_aggregate_data::GroupedAggregateData;
        use crate::operator::aggregate::hash_aggregate::HashAggregate;
        use paro_planner::expression::{Expression, ReferenceExpression};

        // Create group by expressions for all columns
        let mut groups = Vec::new();
        for (i, typ) in types.iter().enumerate() {
            groups.push(Expression::Reference(ReferenceExpression::new(
                i,
                typ.clone(),
            )));
        }

        let aggregate_data = GroupedAggregateData {
            projection_exprs: Vec::new(),
            payload_types: types.to_vec(),
            groups,
            grouping_sets: Vec::new(),
            aggregates: Vec::new(),
            grouping_functions: Vec::new(),
            aggregate_inputs: Vec::new(),
            aggregate_filters: Vec::new(),
            aggregate_orders: Vec::new(),
        };

        let hash_agg = HashAggregate::new(aggregate_data, types.to_vec(), child)?;

        Ok(Arc::new(hash_agg))
    }
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::generator::PhysicalPlanGenerator;
    use paro_common::types::LogicalType;
    use paro_planner::expression::Expression;
    use paro_planner::operator::join::JoinComparisonType;

    #[test]
    fn set_operation_join_conditions_use_not_distinct_from() {
        let conditions = PhysicalPlanGenerator::create_set_join_conditions(&[
            LogicalType::Integer,
            LogicalType::Varchar,
        ]);

        assert_eq!(conditions.len(), 2);
        assert!(conditions
            .iter()
            .all(|condition| matches!(condition.comparison, JoinComparisonType::NotDistinctFrom)));
    }

    #[test]
    fn set_operation_join_conditions_use_reference_expressions() {
        let conditions = PhysicalPlanGenerator::create_set_join_conditions(&[LogicalType::Integer]);

        assert!(matches!(conditions[0].left, Expression::Reference(_)));
        assert!(matches!(conditions[0].right, Expression::Reference(_)));
    }
}
