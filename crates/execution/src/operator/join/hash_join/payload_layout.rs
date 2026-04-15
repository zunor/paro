use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::join::{JoinCondition, JoinType};

#[derive(Debug, Clone)]
pub(super) struct BuildPayloadLayout {
    pub(super) build_payload_columns: Vec<usize>,
    pub(super) build_payload_types: Vec<LogicalType>,
    pub(super) right_projection_map_for_build: Vec<usize>,
    pub(super) residual_conditions_on_build_payload: Vec<JoinCondition>,
}

fn projection_indices(column_count: usize, projection_map: &[usize]) -> Vec<usize> {
    if projection_map.is_empty() {
        (0..column_count).collect()
    } else {
        projection_map.to_vec()
    }
}

fn collect_expression_reference_indices(expr: &Expression, refs: &mut Vec<usize>) {
    match expr {
        Expression::Reference(reference) => refs.push(reference.index),
        Expression::ColumnRef(column_ref) => refs.push(column_ref.binding.column_index),
        Expression::Function(function) => {
            for child in &function.children {
                collect_expression_reference_indices(child, refs);
            }
        }
        Expression::Cast(cast) => {
            collect_expression_reference_indices(&cast.child, refs);
        }
        Expression::Conjunction(conjunction) => {
            for child in &conjunction.children {
                collect_expression_reference_indices(child, refs);
            }
        }
        Expression::Case(case_expr) => {
            collect_expression_reference_indices(&case_expr.check, refs);
            collect_expression_reference_indices(&case_expr.result_if_true, refs);
            collect_expression_reference_indices(&case_expr.result_if_false, refs);
        }
        Expression::Comparison(comparison) => {
            collect_expression_reference_indices(&comparison.left, refs);
            collect_expression_reference_indices(&comparison.right, refs);
        }
        Expression::Operator(operator) => {
            for child in &operator.children {
                collect_expression_reference_indices(child, refs);
            }
        }
        Expression::Aggregate(aggregate) => {
            for child in &aggregate.children {
                collect_expression_reference_indices(child, refs);
            }
            if let Some(filter) = &aggregate.filter {
                collect_expression_reference_indices(filter, refs);
            }
            for order in &aggregate.order_bys {
                collect_expression_reference_indices(&order.expression, refs);
            }
        }
        Expression::Subquery(subquery) => {
            for child in &subquery.children {
                collect_expression_reference_indices(child, refs);
            }
        }
        Expression::Window(window) => {
            for child in &window.children {
                collect_expression_reference_indices(child, refs);
            }
            for partition in &window.partitions {
                collect_expression_reference_indices(partition, refs);
            }
            for order in &window.orders {
                collect_expression_reference_indices(&order.expression, refs);
            }
        }
        Expression::Constant(_) => {}
    }
}

pub(super) fn remap_expression_to_build_payload(
    expr: &Expression,
    original_to_payload: &[Option<usize>],
) -> Result<Expression> {
    match expr {
        Expression::Constant(_) => Ok(expr.clone()),
        Expression::Reference(reference) => {
            let payload_idx = original_to_payload
                .get(reference.index)
                .and_then(|idx| *idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "RHS reference index {} is not available in hash join payload",
                        reference.index
                    ))
                })?;
            Ok(Expression::Reference(ReferenceExpression::new(
                payload_idx,
                reference.return_type.clone(),
            )))
        }
        Expression::ColumnRef(column_ref) => {
            let mut mapped = column_ref.clone();
            mapped.binding.column_index = original_to_payload
                .get(column_ref.binding.column_index)
                .and_then(|idx| *idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "RHS column index {} is not available in hash join payload",
                        column_ref.binding.column_index
                    ))
                })?;
            Ok(Expression::ColumnRef(mapped))
        }
        Expression::Function(function) => {
            let mut mapped = function.clone();
            mapped.children = function
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Function(mapped))
        }
        Expression::Cast(cast) => {
            let mut mapped = cast.clone();
            mapped.child = Box::new(remap_expression_to_build_payload(
                &cast.child,
                original_to_payload,
            )?);
            Ok(Expression::Cast(mapped))
        }
        Expression::Conjunction(conjunction) => {
            let mut mapped = conjunction.clone();
            mapped.children = conjunction
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Conjunction(mapped))
        }
        Expression::Case(case_expr) => {
            let mut mapped = case_expr.clone();
            mapped.check = Box::new(remap_expression_to_build_payload(
                &case_expr.check,
                original_to_payload,
            )?);
            mapped.result_if_true = Box::new(remap_expression_to_build_payload(
                &case_expr.result_if_true,
                original_to_payload,
            )?);
            mapped.result_if_false = Box::new(remap_expression_to_build_payload(
                &case_expr.result_if_false,
                original_to_payload,
            )?);
            Ok(Expression::Case(mapped))
        }
        Expression::Comparison(comparison) => {
            let mut mapped = comparison.clone();
            mapped.left = Box::new(remap_expression_to_build_payload(
                &comparison.left,
                original_to_payload,
            )?);
            mapped.right = Box::new(remap_expression_to_build_payload(
                &comparison.right,
                original_to_payload,
            )?);
            Ok(Expression::Comparison(mapped))
        }
        Expression::Operator(operator) => {
            let mut mapped = operator.clone();
            mapped.children = operator
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Operator(mapped))
        }
        Expression::Aggregate(aggregate) => {
            let mut mapped = aggregate.clone();
            mapped.children = aggregate
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            mapped.filter = aggregate
                .filter
                .as_ref()
                .map(|filter| {
                    remap_expression_to_build_payload(filter, original_to_payload).map(Box::new)
                })
                .transpose()?;
            mapped.order_bys = aggregate
                .order_bys
                .iter()
                .map(|order| {
                    let mut mapped_order = order.clone();
                    mapped_order.expression =
                        remap_expression_to_build_payload(&order.expression, original_to_payload)?;
                    Ok(mapped_order)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Aggregate(mapped))
        }
        Expression::Subquery(subquery) => {
            let mut mapped = subquery.clone();
            mapped.children = subquery
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Subquery(mapped))
        }
        Expression::Window(window) => {
            let mut mapped = window.clone();
            mapped.children = window
                .children
                .iter()
                .map(|child| remap_expression_to_build_payload(child, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            mapped.partitions = window
                .partitions
                .iter()
                .map(|partition| remap_expression_to_build_payload(partition, original_to_payload))
                .collect::<Result<Vec<_>>>()?;
            mapped.orders = window
                .orders
                .iter()
                .map(|order| {
                    let mut mapped_order = order.clone();
                    mapped_order.expression =
                        remap_expression_to_build_payload(&order.expression, original_to_payload)?;
                    Ok(mapped_order)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Window(mapped))
        }
    }
}

pub(super) fn derive_build_payload_layout(
    join_type: JoinType,
    right_types: &[LogicalType],
    right_projection_map: &[usize],
    residual_conditions: &[JoinCondition],
) -> Result<BuildPayloadLayout> {
    let right_col_count = right_types.len();
    let mut required_columns = vec![false; right_col_count];

    let output_columns = if matches!(
        join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Outer
            | JoinType::Single
            | JoinType::RightSemi
            | JoinType::RightAnti
    ) {
        projection_indices(right_col_count, right_projection_map)
    } else {
        Vec::new()
    };
    for &idx in &output_columns {
        if idx < right_col_count {
            required_columns[idx] = true;
        }
    }

    for condition in residual_conditions {
        let mut references = Vec::new();
        collect_expression_reference_indices(&condition.right, &mut references);
        for idx in references {
            if idx < right_col_count {
                required_columns[idx] = true;
            }
        }
    }

    let build_payload_columns = required_columns
        .iter()
        .enumerate()
        .filter_map(|(column_idx, required)| required.then_some(column_idx))
        .collect::<Vec<_>>();

    let mut original_to_payload = vec![None; right_col_count];
    for (payload_idx, original_idx) in build_payload_columns.iter().copied().enumerate() {
        original_to_payload[original_idx] = Some(payload_idx);
    }

    let build_payload_types = build_payload_columns
        .iter()
        .map(|idx| right_types[*idx].clone())
        .collect::<Vec<_>>();

    let right_projection_map_for_build = output_columns
        .iter()
        .map(|idx| {
            original_to_payload
                .get(*idx)
                .and_then(|mapped_idx| *mapped_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "RHS output column {} missing from hash join payload layout",
                        idx
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let residual_conditions_on_build_payload = residual_conditions
        .iter()
        .map(|condition| {
            let mut mapped = condition.clone();
            mapped.right =
                remap_expression_to_build_payload(&condition.right, &original_to_payload)?;
            Ok(mapped)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BuildPayloadLayout {
        build_payload_columns,
        build_payload_types,
        right_projection_map_for_build,
        residual_conditions_on_build_payload,
    })
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ConstantExpression, Expression, ReferenceExpression};
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

    use crate::operator::join::physical_comparison_join::PhysicalComparisonJoin;

    use super::derive_build_payload_layout;

    fn constant_i32(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn reference_i32(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    #[test]
    fn split_conditions_keeps_only_hashable_predicates_in_probe_keys() {
        let conditions = vec![
            JoinCondition::new(constant_i32(1), constant_i32(1), JoinComparisonType::Equal),
            JoinCondition::new(
                constant_i32(2),
                constant_i32(2),
                JoinComparisonType::LessThan,
            ),
            JoinCondition::new(
                constant_i32(3),
                constant_i32(3),
                JoinComparisonType::NotDistinctFrom,
            ),
        ];

        let (equality_conditions, residual_conditions) =
            PhysicalComparisonJoin::split_conditions(&conditions);

        assert_eq!(equality_conditions.len(), 2);
        assert_eq!(residual_conditions.len(), 1);
        assert!(matches!(
            equality_conditions[0].comparison,
            JoinComparisonType::Equal
        ));
        assert!(matches!(
            equality_conditions[1].comparison,
            JoinComparisonType::NotDistinctFrom
        ));
        assert!(matches!(
            residual_conditions[0].comparison,
            JoinComparisonType::LessThan
        ));
    }

    #[test]
    fn payload_pruning_uses_right_projection_map() {
        let layout = derive_build_payload_layout(
            JoinType::Inner,
            &[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
            &[2],
            &[],
        )
        .expect("payload layout should be derived");

        assert_eq!(layout.build_payload_columns, vec![2]);
        assert_eq!(layout.build_payload_types, vec![LogicalType::Integer]);
        assert_eq!(layout.right_projection_map_for_build, vec![0]);
    }

    #[test]
    fn payload_pruning_keeps_residual_referenced_columns_and_remaps_rhs() {
        let layout = derive_build_payload_layout(
            JoinType::Inner,
            &[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
            &[1],
            &[JoinCondition::new(
                reference_i32(0),
                reference_i32(2),
                JoinComparisonType::LessThan,
            )],
        )
        .expect("payload layout should be derived");

        assert_eq!(layout.build_payload_columns, vec![1, 2]);
        assert_eq!(
            layout.build_payload_types,
            vec![LogicalType::Integer, LogicalType::Integer]
        );
        assert_eq!(layout.right_projection_map_for_build, vec![0]);

        match &layout.residual_conditions_on_build_payload[0].right {
            Expression::Reference(reference) => assert_eq!(reference.index, 1),
            other => panic!("Expected remapped RHS reference, got: {other:?}"),
        }
    }

    #[test]
    fn semi_join_without_residual_can_prune_build_payload_to_empty() {
        let layout = derive_build_payload_layout(
            JoinType::Semi,
            &[LogicalType::Integer, LogicalType::Integer],
            &[],
            &[],
        )
        .expect("payload layout should be derived");

        assert!(layout.build_payload_columns.is_empty());
        assert!(layout.build_payload_types.is_empty());
        assert!(layout.right_projection_map_for_build.is_empty());
    }
}
