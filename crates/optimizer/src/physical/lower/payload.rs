// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) fn extract_payload_expression(
    expr: Expression,
    projection_exprs: &mut Vec<Expression>,
    payload_types: &mut Vec<paro_common::types::LogicalType>,
) -> Expression {
    let return_type = expr.return_type();
    if expr.evaluation_properties().can_share_evaluation() {
        if let Some(reference_index) = projection_exprs
            .iter()
            .position(|existing| existing.equals(&expr))
        {
            return Expression::Reference(
                ReferenceExpression::new(reference_index, return_type).into(),
            );
        }
    }
    let reference_index = projection_exprs.len();
    payload_types.push(return_type.clone());
    projection_exprs.push(expr);
    Expression::Reference(ReferenceExpression::new(reference_index, return_type).into())
}
