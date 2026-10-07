// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) fn collect_union_all_row_literals(
    setop: &LogicalSetOperation<PreparedChild>,
) -> Result<Option<Vec<Box<[Expression]>>>> {
    let mut rows = Vec::new();
    if collect_row_literal_plan(setop.left.as_ref(), setop.types.len(), &mut rows)?
        && collect_row_literal_plan(setop.right.as_ref(), setop.types.len(), &mut rows)?
    {
        return Ok(Some(rows));
    }
    Ok(None)
}

pub(crate) fn collect_row_literal_plan(
    plan: &PreparedNode,
    output_width: usize,
    rows: &mut Vec<Box<[Expression]>>,
) -> Result<bool> {
    match &plan.operator {
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == SetOpType::Union && setop.setop_all =>
        {
            if setop.types.len() != output_width {
                return Err(paro_error::internal(format!(
                    "UNION ALL child has {} columns, expected {output_width}",
                    setop.types.len()
                )));
            }
            Ok(
                collect_row_literal_plan(setop.left.as_ref(), output_width, rows)?
                    && collect_row_literal_plan(setop.right.as_ref(), output_width, rows)?,
            )
        }
        LogicalOperator::Projection(project)
            if matches!(project.child.operator, LogicalOperator::DummyScan) =>
        {
            if project.expressions.len() != output_width {
                return Err(paro_error::internal(format!(
                    "row-literal projection has {} expressions, expected {output_width}",
                    project.expressions.len()
                )));
            }
            rows.push(project.expressions.clone().into_boxed_slice());
            Ok(true)
        }
        LogicalOperator::ExpressionGet(values) => {
            for row in &values.expressions {
                if row.len() != output_width {
                    return Err(paro_error::internal(format!(
                        "row-literal values row has {} expressions, expected {output_width}",
                        row.len()
                    )));
                }
                rows.push(row.clone().into_boxed_slice());
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
