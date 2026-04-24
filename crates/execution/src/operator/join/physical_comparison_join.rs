// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared comparison join contract.
//!
//!

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::vector::SelectionVector;
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

use crate::explain::explain_node::format_join_condition;
use crate::operator::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
};
use crate::operator::PhysicalOperator;

use super::physical_join::PhysicalJoin;

#[derive(Debug)]
pub struct PhysicalComparisonJoin {
    pub join: PhysicalJoin,
    pub conditions: Vec<JoinCondition>,
    pub equality_conditions: Vec<JoinCondition>,
    pub residual_conditions: Vec<JoinCondition>,
}

impl PhysicalComparisonJoin {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        mut conditions: Vec<JoinCondition>,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Self {
        Self::reorder_conditions(&mut conditions);
        let (equality_conditions, residual_conditions) = Self::split_conditions(&conditions);
        let join = PhysicalJoin::new(
            left,
            right,
            join_type,
            left_projection_map,
            right_projection_map,
        );

        Self {
            join,
            conditions,
            equality_conditions,
            residual_conditions,
        }
    }

    pub fn is_hash_equality(comparison: &JoinComparisonType) -> bool {
        matches!(
            comparison,
            JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
        )
    }

    pub fn reorder_conditions(conditions: &mut Vec<JoinCondition>) {
        let mut equality_conditions = Vec::new();
        let mut residual_conditions = Vec::new();

        for condition in conditions.drain(..) {
            if Self::is_hash_equality(&condition.comparison) {
                equality_conditions.push(condition);
            } else {
                residual_conditions.push(condition);
            }
        }

        conditions.extend(equality_conditions);
        conditions.extend(residual_conditions);
    }

    pub fn split_conditions(
        conditions: &[JoinCondition],
    ) -> (Vec<JoinCondition>, Vec<JoinCondition>) {
        let mut equality_conditions = Vec::new();
        let mut residual_conditions = Vec::new();

        for condition in conditions {
            if Self::is_hash_equality(&condition.comparison) {
                equality_conditions.push(condition.clone());
            } else {
                residual_conditions.push(condition.clone());
            }
        }

        (equality_conditions, residual_conditions)
    }

    pub fn condition_info(&self) -> String {
        self.conditions
            .iter()
            .map(format_join_condition)
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    pub fn construct_empty_join_result(
        &self,
        input: &Chunk,
        result: &mut Chunk,
        has_null: bool,
    ) -> Result<()> {
        match self.join.join_type {
            JoinType::Anti => {
                let mut sel =
                    SelectionVector::try_with_capacity(input.size(), input.allocator().clone())?;
                sel.set_len(input.size());
                for idx in 0..input.size() {
                    sel.set(idx, idx);
                }
                construct_anti_join_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    result,
                )?;
            }
            JoinType::Mark => {
                let markers = if has_null {
                    vec![None; input.size()]
                } else {
                    vec![Some(false); input.size()]
                };
                construct_mark_join_result(
                    input,
                    &self.join.left_projection_map,
                    &markers,
                    result,
                )?;
            }
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                let mut sel =
                    SelectionVector::try_with_capacity(input.size(), input.allocator().clone())?;
                sel.set_len(input.size());
                for idx in 0..input.size() {
                    sel.set(idx, idx);
                }
                construct_left_outer_result(
                    input,
                    &sel,
                    input.size(),
                    &self.join.left_projection_map,
                    &self.join.right_output_types,
                    result,
                )?;
            }
            _ => {
                result.set_cardinality(0);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PhysicalComparisonJoin;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ConstantExpression, Expression};
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition};

    fn constant_i32(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn reorder_conditions_moves_equalities_to_the_front() {
        let mut conditions = vec![
            JoinCondition::new(
                constant_i32(1),
                constant_i32(1),
                JoinComparisonType::LessThan,
            ),
            JoinCondition::new(constant_i32(2), constant_i32(2), JoinComparisonType::Equal),
            JoinCondition::new(
                constant_i32(3),
                constant_i32(3),
                JoinComparisonType::NotDistinctFrom,
            ),
        ];

        PhysicalComparisonJoin::reorder_conditions(&mut conditions);

        assert!(matches!(
            conditions[0].comparison,
            JoinComparisonType::Equal
        ));
        assert!(matches!(
            conditions[1].comparison,
            JoinComparisonType::NotDistinctFrom
        ));
        assert!(matches!(
            conditions[2].comparison,
            JoinComparisonType::LessThan
        ));
    }

    #[test]
    fn split_conditions_keeps_residuals_separate() {
        let conditions = vec![
            JoinCondition::new(constant_i32(1), constant_i32(1), JoinComparisonType::Equal),
            JoinCondition::new(
                constant_i32(2),
                constant_i32(2),
                JoinComparisonType::GreaterThan,
            ),
        ];

        let (equality_conditions, residual_conditions) =
            PhysicalComparisonJoin::split_conditions(&conditions);

        assert_eq!(equality_conditions.len(), 1);
        assert_eq!(residual_conditions.len(), 1);
    }
}
