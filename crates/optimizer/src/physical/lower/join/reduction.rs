// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn plan_grouped_extrema_reduction(
    key_conditions: &[JoinCondition],
    build_residual_conditions: &[JoinCondition],
    predicates: &[HashReductionPredicateSpec],
    source_predicates: &[HashReductionSourcePredicateSpec],
    steps: &[HashReductionStepSpec],
) -> Option<HashReductionGroupedExtremaSpec> {
    let [key] = key_conditions else {
        return None;
    };
    let [predicate] = predicates else {
        return None;
    };
    let condition = build_residual_conditions.get(predicate.build_residual_offset)?;
    if key.comparison != JoinComparisonType::Equal
        || key.left.return_type() != LogicalType::BigInt
        || key.right.return_type() != LogicalType::BigInt
        || condition.comparison != JoinComparisonType::NotEqual
        || condition.left.return_type() != LogicalType::BigInt
        || condition.right.return_type() != LogicalType::BigInt
    {
        return None;
    }
    let Expression::Reference(source_value) = &condition.left else {
        return None;
    };
    let source_value_index = source_value.index;

    let source_bits = source_predicates
        .iter()
        .fold(0u8, |mask, predicate| mask | predicate.predicate_mask);
    let mut channels: Vec<HashReductionExtremaChannelSpec> = Vec::new();
    for step in steps {
        if step.predicate_mask & predicate.predicate_mask != predicate.predicate_mask {
            return None;
        }
        let source_predicate_mask = step.predicate_mask & !predicate.predicate_mask;
        if source_predicate_mask & !source_bits != 0 {
            return None;
        }
        if let Some(channel) = channels
            .iter_mut()
            .find(|channel| channel.source_predicate_mask == source_predicate_mask)
        {
            channel.match_mask |= step.match_mask;
        } else {
            channels.push(HashReductionExtremaChannelSpec {
                source_predicate_mask,
                match_mask: step.match_mask,
            });
        }
    }
    let mut channel_map = [0_u8; 256];
    for source_mask in 0_u8..=u8::MAX {
        let mut channel_mask = 0_u8;
        for (channel_idx, channel) in channels.iter().enumerate() {
            if source_mask & channel.source_predicate_mask == channel.source_predicate_mask {
                channel_mask |= 1_u8 << channel_idx;
            }
        }
        channel_map[source_mask as usize] = channel_mask;
    }
    Some(HashReductionGroupedExtremaSpec {
        source_value_index,
        build_residual_offset: predicate.build_residual_offset,
        channels: channels.into_boxed_slice(),
        channel_map: std::sync::Arc::new(channel_map),
    })
}

pub(super) struct ReductionScanBranch<'a> {
    pub(super) get: &'a Get,
    /// Base-table column id for each column exposed to the reduction join.
    output_column_ids: Vec<usize>,
    /// Base-table column id for each column bound directly against the Get.
    pub(super) filter_column_ids: Vec<usize>,
    /// Predicates local to this logical alias, still bound to the Get input.
    pub(super) filters: Vec<Expression>,
}

impl<'a> ReductionScanBranch<'a> {
    pub(super) fn inspect(plan: &'a PreparedNode) -> Option<Self> {
        match &plan.operator {
            LogicalOperator::Get(get) => {
                let output_column_ids = (0..get.returned_types.len())
                    .map(|index| get.stored_column(index))
                    .collect::<Option<Vec<_>>>()?;
                Some(Self {
                    get,
                    filter_column_ids: output_column_ids.clone(),
                    output_column_ids,
                    filters: Vec::new(),
                })
            }
            LogicalOperator::Filter(filter) => {
                let LogicalOperator::Get(get) = &filter.child.operator else {
                    return None;
                };
                let output_column_ids = filter
                    .projection_map
                    .to_indices(get.returned_types.len())
                    .into_iter()
                    .map(|index| get.stored_column(index))
                    .collect::<Option<Vec<_>>>()?;
                let filter_column_ids = (0..get.returned_types.len())
                    .map(|index| get.stored_column(index))
                    .collect::<Option<Vec<_>>>()?;
                Some(Self {
                    get,
                    output_column_ids,
                    filter_column_ids,
                    filters: filter.expressions.clone(),
                })
            }
            _ => None,
        }
    }

    /// Column ids for expressions bound above the optional Filter projection.
    pub(super) fn condition_column_ids(&self) -> &[usize] {
        &self.output_column_ids
    }

    /// Column ids for expressions stored directly on the underlying Get.
    pub(super) fn filter_column_ids(&self) -> &[usize] {
        &self.filter_column_ids
    }
}

pub(super) fn remap_reduction_expression(
    expression: &Expression,
    source_column_ids: &[usize],
    source_table_index: usize,
    target_column_ids: &[usize],
    target_table_index: usize,
) -> Option<Expression> {
    fn remap(
        expression: &mut Expression,
        source_column_ids: &[usize],
        source_table_index: usize,
        target_column_ids: &[usize],
        target_table_index: usize,
    ) -> bool {
        match expression {
            Expression::ColumnRef(column) => {
                if column.depth != 0 || column.binding.table_index != source_table_index {
                    return false;
                }
                let Some(column_id) = source_column_ids.get(column.binding.column_index) else {
                    return false;
                };
                let Some(target_index) = target_column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return false;
                };
                **column = ColumnRefExpression::new(
                    paro_planner::logical::operator::ColumnBinding::new(
                        target_table_index,
                        target_index,
                    ),
                    column.return_type.clone(),
                );
                true
            }
            Expression::Reference(reference) => {
                let Some(column_id) = source_column_ids.get(reference.index) else {
                    return false;
                };
                let Some(target_index) = target_column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return false;
                };
                reference.index = target_index;
                true
            }
            _ => {
                let mut valid = true;
                ExpressionIterator::enumerate_children_mut(expression, |child| {
                    valid &= remap(
                        child,
                        source_column_ids,
                        source_table_index,
                        target_column_ids,
                        target_table_index,
                    );
                });
                valid
            }
        }
    }

    let mut expression = expression.clone();
    remap(
        &mut expression,
        source_column_ids,
        source_table_index,
        target_column_ids,
        target_table_index,
    )
    .then_some(expression)
}

/// Bind a merged scan expression to its physical chunk layout. Reduction
/// predicates cross the logical/physical boundary here, so execution never
/// has to reinterpret a logical table binding as a vector position.
pub(super) fn bind_reduction_source_expression(
    mut expression: Expression,
    source_table_index: usize,
) -> Option<Expression> {
    fn bind(expression: &mut Expression, source_table_index: usize) -> bool {
        match expression {
            Expression::ColumnRef(column) => {
                if column.depth != 0 || column.binding.table_index != source_table_index {
                    return false;
                }
                *expression = Expression::Reference(
                    ReferenceExpression::new(
                        column.binding.column_index,
                        column.return_type.clone(),
                    )
                    .into(),
                );
                true
            }
            Expression::Reference(_) => true,
            _ => {
                let mut valid = true;
                ExpressionIterator::enumerate_children_mut(expression, |child| {
                    valid &= bind(child, source_table_index);
                });
                valid
            }
        }
    }

    bind(&mut expression, source_table_index).then_some(expression)
}

pub(super) fn scan_orders_are_fusion_compatible(
    left: Option<&paro_storage::table::segment_reorderer::SegmentOrderOptions>,
    right: Option<&paro_storage::table::segment_reorderer::SegmentOrderOptions>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.is_fusion_compatible(right),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

pub(super) fn plan_reduction_runtime_filter_fusion(
    branches: Vec<Option<Vec<Expression>>>,
    shared_scan_width: usize,
    independent_scan_width: usize,
) -> Option<Vec<Expression>> {
    let branches = branches.into_iter().collect::<Option<Vec<_>>>()?;
    let first = branches.first()?.clone();
    if branches
        .iter()
        .skip(1)
        .all(|branch| same_expression_conjunction(&first, branch))
    {
        return Some(first);
    }
    if branches.iter().any(Vec::is_empty) {
        return None;
    }

    // Each independent scan decodes its own projected columns. A fused scan
    // decodes their union once, but pays one boolean-dispatch unit for every
    // additional branch in the predicate disjunction. This width-based model
    // is intentionally independent of workload names and declines fusion when
    // projections do not overlap enough to pay for the wider predicate.
    let disjunction_cost = branches.len().saturating_sub(1);
    if shared_scan_width.saturating_add(disjunction_cost) > independent_scan_width {
        return None;
    }
    let branch_predicates = branches
        .into_iter()
        .map(|expressions| combine_boolean_terms(ConjunctionType::And, expressions))
        .collect::<Option<Vec<_>>>()?;
    combine_boolean_terms(ConjunctionType::Or, branch_predicates).map(|expression| vec![expression])
}

pub(super) fn same_expression_conjunction(left: &[Expression], right: &[Expression]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut matched = vec![false; right.len()];
    left.iter().all(|expression| {
        right
            .iter()
            .enumerate()
            .find(|(idx, candidate)| !matched[*idx] && expression.equals(candidate))
            .is_some_and(|(idx, _)| {
                matched[idx] = true;
                true
            })
    })
}

pub(super) fn combine_boolean_terms(
    conjunction_type: ConjunctionType,
    mut expressions: Vec<Expression>,
) -> Option<Expression> {
    match expressions.len() {
        0 => None,
        1 => expressions.pop(),
        _ => Some(Expression::Conjunction(
            ConjunctionExpression::new(conjunction_type, expressions).into(),
        )),
    }
}

/// One namespace for both build-residual and source-local predicate bits.
/// Build payload offsets deliberately remain a separate dense sequence.
#[derive(Debug, Default)]
pub(super) struct ReductionPredicateBits {
    next: usize,
}

impl ReductionPredicateBits {
    pub(super) fn allocate(&mut self) -> Option<u8> {
        if self.next >= u8::BITS as usize {
            return None;
        }
        let bit = 1u8 << self.next;
        self.next += 1;
        Some(bit)
    }
}

pub(super) fn same_reduction_keys(left: &[JoinCondition], right: &[JoinCondition]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.comparison == right.comparison
                && same_reduction_key_expression(&left.left, &right.left)
                && same_reduction_key_expression(&left.right, &right.right)
        })
}

pub(super) fn same_reduction_condition(left: &JoinCondition, right: &JoinCondition) -> bool {
    left.comparison == right.comparison
        && same_reduction_predicate_expression(&left.left, &right.left)
        && same_reduction_predicate_expression(&left.right, &right.right)
}

pub(super) fn reduction_condition_can_share_evaluation(condition: &JoinCondition) -> bool {
    condition
        .left
        .evaluation_properties()
        .can_share_evaluation()
        && condition
            .right
            .evaluation_properties()
            .can_share_evaluation()
}

pub(super) fn same_reduction_predicate_expression(left: &Expression, right: &Expression) -> bool {
    if same_reduction_key_expression(left, right) {
        return true;
    }
    match (left, right) {
        (Expression::Constant(left), Expression::Constant(right)) => {
            left.return_type == right.return_type && left.value == right.value
        }
        _ => false,
    }
}

pub(super) fn same_reduction_key_expression(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Reference(left), Expression::Reference(right)) => {
            left.index == right.index && left.return_type == right.return_type
        }
        (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
            left.binding == right.binding
                && left.depth == right.depth
                && left.return_type == right.return_type
        }
        _ => false,
    }
}
