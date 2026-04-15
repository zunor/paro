//! Physical Plan Generation for JOIN
//!
//!
//! ## Dependencies Check
//! - HashJoin: ✅
//! - CrossProduct: ✅
//!
//! ## Implementation Notes
//! - Comparison joins with equality conditions use HashJoin
//! - Single range predicates use PiecewiseMergeJoin
//! - Dual range predicates use IEJoin
//! - Cross products use CrossProduct
//! - Remaining comparison/any joins use NestedLoopJoin

use super::generator::PhysicalPlanGenerator;
use crate::operator::join::cross_product::CrossProduct;
use crate::operator::join::hash_join::operator::HashJoin;
use crate::operator::join::iejoin::IEJoin;
use crate::operator::join::nested_loop_join::NestedLoopJoin;
use crate::operator::join::piecewise_merge_join::PiecewiseMergeJoin;
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_planner::operator::join::{JoinComparisonType, JoinType};
use paro_planner::operator::{AnyJoin, ComparisonJoin, CrossProduct as LogicalCrossProduct, Join};
use std::sync::Arc;

impl PhysicalPlanGenerator {
    pub(crate) fn supports_hash_join(join_type: JoinType) -> bool {
        matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Outer
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub(crate) fn unsupported_hash_join_error(
        join_type: JoinType,
    ) -> paro_common::error::ParoError {
        paro_error::not_implemented(format!(
            "{} JOIN result construction in HashJoin",
            join_type
        ))
    }

    pub(crate) fn supports_piecewise_merge_join(join_type: JoinType) -> bool {
        matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Outer
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub(crate) fn supports_ie_join(join_type: JoinType) -> bool {
        Self::supports_piecewise_merge_join(join_type)
    }

    /// Create physical plan for Join.
    pub fn create_plan_join(&self, join: &Join) -> Result<Arc<dyn PhysicalOperator>> {
        match join {
            Join::Comparison(comp_join) => self.create_plan_comparison_join(comp_join),
            Join::Any(any_join) => self.create_plan_any_join(any_join),
            Join::Cross(cross) => self.create_plan_cross_product(cross),
        }
    }

    /// Create physical plan for comparison join.
    fn create_plan_comparison_join(
        &self,
        join: &ComparisonJoin,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let left = self.create_plan_from_logical_plan(join.left.as_ref())?;
        let right = self.create_plan_from_logical_plan(join.right.as_ref())?;
        self.create_plan_delim_join_from_children(join, left, right)
    }

    pub(crate) fn create_plan_regular_comparison_join_from_children(
        &self,
        join: &ComparisonJoin,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Check if we have any conditions
        if join.conditions.is_empty() {
            if matches!(join.join_type, JoinType::Inner) {
                // No conditions - this is a cross product
                return self.create_plan_cross_product_from_children(left, right);
            }
            return Err(paro_error::not_implemented(format!(
                "{} JOIN without join conditions",
                join.join_type
            )));
        }

        let has_equality = join.has_equality();
        let has_not_distinct_from = join
            .conditions
            .iter()
            .any(|cond| matches!(cond.comparison, JoinComparisonType::NotDistinctFrom));

        if has_equality && !has_not_distinct_from {
            if !Self::supports_hash_join(join.join_type) {
                return Err(Self::unsupported_hash_join_error(join.join_type));
            }

            let mut reordered_conditions = join.conditions.clone();
            crate::operator::join::physical_comparison_join::PhysicalComparisonJoin::reorder_conditions(
                &mut reordered_conditions,
            );

            // Use hash join for equality conditions
            let mut hash_join = HashJoin::new(
                left,
                right,
                join.join_type,
                reordered_conditions.clone(),
                join.left_projection_map.clone(),
                join.right_projection_map.clone(),
            )?;

            // Enable internal filter pushdown for equality conditions
            let mut join_conditions = Vec::new();
            let mut probe_info = Vec::new();
            let mut condition_types = Vec::new();

            for cond in reordered_conditions.iter() {
                if matches!(cond.comparison, JoinComparisonType::Equal) {
                    let equality_idx = join_conditions.len();
                    join_conditions.push(equality_idx);
                    condition_types.push(cond.left.return_type().clone());

                    // Internal pushdown: filter the probe key column itself
                    // In HashJoin, the probe keys are in a separate Chunk
                    // where column index matches condition index.
                    probe_info.push(
                        crate::operator::join::join_filter_pushdown::JoinFilterPushdownFilter {
                            join_condition_idx: equality_idx,
                            probe_column: crate::operator::join::join_filter_pushdown::JoinFilterPushdownColumn {
                                filter_idx: equality_idx,
                                filter_col_idx: equality_idx,
                            },
                        },
                    );
                }
            }

            if !join_conditions.is_empty() {
                hash_join.set_filter_pushdown(
                    crate::operator::join::join_filter_pushdown::JoinFilterPushdownInfo::new(
                        join_conditions,
                        probe_info,
                        condition_types,
                        true, // Enable Bloom Filter by default
                    ),
                );
            }

            Ok(Arc::new(hash_join))
        } else if join.conditions.len() == 1
            && Self::supports_piecewise_merge_join(join.join_type)
            && PiecewiseMergeJoin::supports_comparison(join.conditions[0].comparison)
        {
            Ok(Arc::new(PiecewiseMergeJoin::new(
                left,
                right,
                join.join_type,
                join.conditions[0].clone(),
                join.left_projection_map.clone(),
                join.right_projection_map.clone(),
            )?))
        } else if Self::supports_ie_join(join.join_type)
            && IEJoin::supports_conditions(&join.conditions)
        {
            Ok(Arc::new(IEJoin::new(
                left,
                right,
                join.join_type,
                join.conditions.clone(),
                join.left_projection_map.clone(),
                join.right_projection_map.clone(),
            )?))
        } else {
            Ok(Arc::new(NestedLoopJoin::new_comparison(
                left,
                right,
                join.join_type,
                join.conditions.clone(),
                join.left_projection_map.clone(),
                join.right_projection_map.clone(),
            )))
        }
    }

    /// Create physical plan for any join (arbitrary condition).
    fn create_plan_any_join(&self, join: &AnyJoin) -> Result<Arc<dyn PhysicalOperator>> {
        let left = self.create_plan_from_logical_plan(join.left.as_ref())?;
        let right = self.create_plan_from_logical_plan(join.right.as_ref())?;
        Ok(Arc::new(NestedLoopJoin::new_any(
            left,
            right,
            join.join_type,
            join.condition.clone(),
            join.left_projection_map.clone(),
            join.right_projection_map.clone(),
        )))
    }

    /// Create physical plan for cross product.
    fn create_plan_cross_product(
        &self,
        cross: &LogicalCrossProduct,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let left = self.create_plan_from_logical_plan(cross.left.as_ref())?;
        let right = self.create_plan_from_logical_plan(cross.right.as_ref())?;
        self.create_plan_cross_product_from_children(left, right)
    }

    /// Create cross product from already-created children.
    fn create_plan_cross_product_from_children(
        &self,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(CrossProduct::new(left, right)))
    }
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::generator::PhysicalPlanGenerator;
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    };
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
    use paro_planner::operator::Join;
    use paro_planner::operator::{ColumnBinding, ExpressionGet, LogicalOperator};

    use crate::operator::join::hash_join::operator::HashJoin;
    use crate::operator_type::PhysicalOperatorType;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn test_generator() -> PhysicalPlanGenerator {
        PhysicalPlanGenerator::new(test_session())
    }

    fn expression_get(table_index: usize, value: i32, name: &str) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![vec![Expression::Constant(ConstantExpression::new(
                Value::Integer(value),
                LogicalType::Integer,
            ))]],
            vec![name.to_string()],
            vec![LogicalType::Integer],
        ))
    }

    fn lp(op: LogicalOperator) -> paro_planner::plan::LogicalPlan {
        paro_planner::plan::LogicalPlan::new(&paro_planner::binder::context::BindContext::new(), op)
    }

    #[test]
    fn supports_hash_join_matches_implemented_join_shapes() {
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Inner));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Left));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Right));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Outer));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Semi));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Anti));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Mark));
        assert!(PhysicalPlanGenerator::supports_hash_join(JoinType::Single));
        assert!(PhysicalPlanGenerator::supports_hash_join(
            JoinType::RightSemi
        ));
        assert!(PhysicalPlanGenerator::supports_hash_join(
            JoinType::RightAnti
        ));
        assert!(!PhysicalPlanGenerator::supports_hash_join(
            JoinType::Invalid
        ));
    }

    #[test]
    fn single_range_comparison_join_uses_piecewise_merge_join() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 2, "r");
        let mut plan = LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                paro_planner::operator::join::JoinComparisonType::GreaterThan,
            )],
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(
            plan.operator_type(),
            PhysicalOperatorType::PiecewiseMergeJoin
        );
    }

    #[test]
    fn hash_join_conditions_are_reordered_with_equalities_first() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 2, "r");
        let mut plan = LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::GreaterThan,
                ),
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::Equal,
                ),
            ],
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::HashJoin);

        let hash_join = plan
            .as_any()
            .downcast_ref::<HashJoin>()
            .expect("plan should be a physical hash join");
        assert!(matches!(
            hash_join.conditions()[0].comparison,
            JoinComparisonType::Equal
        ));
        assert!(matches!(
            hash_join.conditions()[1].comparison,
            JoinComparisonType::GreaterThan
        ));
    }

    #[test]
    fn any_join_uses_nested_loop_fallback() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 2, "r");
        let mut plan = LogicalOperator::Join(Join::any(
            JoinType::Inner,
            lp(left),
            lp(right),
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
            )),
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::NestedLoopJoin);
    }

    #[test]
    fn not_distinct_from_comparison_join_falls_back_to_nested_loop() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 1, "r");
        let mut plan = LogicalOperator::Join(Join::comparison(
            JoinType::Semi,
            lp(left),
            lp(right),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                paro_planner::operator::join::JoinComparisonType::NotDistinctFrom,
            )],
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::NestedLoopJoin);
    }

    #[test]
    fn dual_range_comparison_join_uses_iejoin() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 2, "r");
        let mut plan = LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::GreaterThan,
                ),
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::LessThan,
                ),
            ],
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::IEJoin);
    }

    #[test]
    fn three_range_conditions_still_fall_back_to_nested_loop() {
        let left = expression_get(0, 1, "l");
        let right = expression_get(1, 2, "r");
        let mut plan = LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::GreaterThan,
                ),
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::LessThan,
                ),
                JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::GreaterThanOrEqual,
                ),
            ],
        ));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::NestedLoopJoin);
    }

    #[test]
    fn duplicate_eliminated_comparison_join_uses_left_delim_join() {
        let left = expression_get(0, 1, "l");
        let right = LogicalOperator::DelimGet(paro_planner::operator::DelimGet::new(
            99,
            vec![LogicalType::Integer],
        ));
        let mut join = paro_planner::operator::join::ComparisonJoin::new(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                paro_planner::operator::join::JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ))];
        let mut plan = LogicalOperator::Join(Join::Comparison(join));

        let plan = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(plan.operator_type(), PhysicalOperatorType::LeftDelimJoin);
    }
}
