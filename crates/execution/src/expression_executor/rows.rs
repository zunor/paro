// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compiled execution for row-oriented expression matrices.
//!
//! SQL `VALUES` and expression scans store one expression list per output row,
//! while the execution engine consumes columnar chunks. This adapter maps the
//! complete matrix once, compiles its dynamic roots as one physical expression
//! program, retains local state for the source task, and writes literal and
//! parameter roots directly into columns without boxing through
//! [`Value`](paro_common::runtime_value::Value).

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::runtime::ParameterBindings;

use super::executor::{ExpressionExecutor, VectorKernelInput};

const DIRECT_ROOT: usize = usize::MAX;

#[derive(Debug)]
pub(crate) struct ExpressionRowsExecutor {
    executor: ExpressionExecutor,
    dummy_input: Chunk,
    scalar_scratch: Vec<Vector>,
    output_types: Box<[LogicalType]>,
    root_to_dynamic: Box<[usize]>,
    row_count: usize,
}

impl ExpressionRowsExecutor {
    pub(crate) fn try_new(
        rows: &[Box<[Expression]>],
        output_types: &[LogicalType],
        session: &paro_context::StatementContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let column_count = output_types.len();
        let expected_roots = rows.len().checked_mul(column_count).ok_or_else(|| {
            paro_error::internal("expression row matrix dimensions overflowed usize")
        })?;
        let mut expression_refs = Vec::with_capacity(expected_roots);
        let mut root_to_dynamic = Vec::with_capacity(expected_roots);
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != column_count {
                return Err(paro_error::internal(format!(
                    "expression row {row_idx} has {} expressions but output has {column_count} columns",
                    row.len()
                )));
            }
            for (column_idx, expression) in row.iter().enumerate() {
                let expected = &output_types[column_idx];
                let actual = expression.return_type();
                // Untyped NULL is valid for every output column. The binder keeps
                // its logical type as Null even after the surrounding VALUES row
                // has established a concrete column type.
                if actual != LogicalType::Null && &actual != expected {
                    return Err(paro_error::internal(format!(
                        "expression row {row_idx} column {column_idx} has type {actual:?}, expected {expected:?}"
                    )));
                }
                if can_write_direct(expression) {
                    root_to_dynamic.push(DIRECT_ROOT);
                } else {
                    root_to_dynamic.push(expression_refs.len());
                    expression_refs.push(expression);
                }
            }
        }

        let executor =
            ExpressionExecutor::with_expression_refs_for_session(&expression_refs, session);
        debug_assert_eq!(executor.expression_count(), expression_refs.len());

        let mut dummy_input = Chunk::try_init_empty(&[], Arc::clone(&allocator))?;
        dummy_input.try_set_cardinality(1)?;
        let scalar_scratch = output_types
            .iter()
            .map(|ty| Vector::try_new(ty.clone(), 1, Arc::clone(&allocator)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            executor,
            dummy_input,
            scalar_scratch,
            output_types: output_types.to_vec().into_boxed_slice(),
            root_to_dynamic: root_to_dynamic.into_boxed_slice(),
            row_count: rows.len(),
        })
    }

    pub(crate) fn execute_batch(
        &mut self,
        start_row: usize,
        row_count: usize,
        rows: &[Box<[Expression]>],
        params: &ParameterBindings,
        runtime: &dyn FunctionExecContext,
        output: &mut Chunk,
    ) -> Result<()> {
        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| paro_error::internal("expression row batch range overflowed usize"))?;
        if end_row > self.row_count {
            return Err(paro_error::internal(format!(
                "expression row batch [{start_row}, {end_row}) exceeds {} rows",
                self.row_count
            )));
        }
        if rows.len() != self.row_count {
            return Err(paro_error::internal(format!(
                "expression row program was built for {} rows but received {}",
                self.row_count,
                rows.len()
            )));
        }

        let column_count = self.output_types.len();
        for (offset, row) in rows[start_row..end_row].iter().enumerate() {
            if row.len() != column_count {
                return Err(paro_error::internal(format!(
                    "expression row {} has {} expressions but output has {column_count} columns",
                    start_row + offset,
                    row.len()
                )));
            }
        }

        if output.column_count() != self.output_types.len() || output.capacity() < row_count.max(1)
        {
            *output = Chunk::try_initialize(
                &self.output_types,
                row_count.max(1),
                runtime.allocator(MemoryTag::BaseTable),
            )?;
        } else {
            output.try_reset(output.allocator().clone())?;
        }

        for output_row in 0..row_count {
            let source_row = start_row + output_row;
            let root_base = source_row.checked_mul(column_count).ok_or_else(|| {
                paro_error::internal("expression row root index overflowed usize")
            })?;
            for column_idx in 0..column_count {
                let target = output.data.get_mut(column_idx).ok_or_else(|| {
                    paro_error::internal("expression row output column is missing")
                })?;
                let target = Vector::try_make_arc_mut(target)?;
                let root_idx = root_base + column_idx;
                let dynamic_idx = self.root_to_dynamic[root_idx];
                if dynamic_idx == DIRECT_ROOT {
                    let expression = &rows[source_row][column_idx];
                    let value = match expression {
                        Expression::Constant(expression) => &expression.value,
                        Expression::Parameter(expression) => {
                            params.value_for_slot(&expression.slot)?
                        }
                        _ => {
                            return Err(paro_error::internal(
                                "direct expression row root is not constant or parameter",
                            ))
                        }
                    };
                    if !target.try_set_scalar_value(output_row, value)? {
                        return Err(paro_error::internal(
                            "direct expression row root has unsupported runtime value",
                        ));
                    }
                    continue;
                }

                let scalar = &mut self.scalar_scratch[column_idx];
                scalar.try_reset_for_execution(1, runtime.allocator(MemoryTag::BaseTable))?;
                scalar.try_set_count(1)?;
                self.executor.execute_kernel_into(
                    dynamic_idx,
                    VectorKernelInput::from_eval_input(crate::runtime::ExpressionEvalInput {
                        params,
                        columns: &self.dummy_input,
                    })
                    .with_count(1),
                    runtime,
                    scalar,
                )?;
                target.try_copy_at(output_row, scalar, 0)?;
            }
        }
        output.try_set_cardinality(row_count)?;
        Ok(())
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }
}

fn can_write_direct(expression: &Expression) -> bool {
    match expression {
        Expression::Constant(expression) => matches!(
            expression.value,
            Value::Null(_)
                | Value::Boolean(_)
                | Value::TinyInt(_)
                | Value::SmallInt(_)
                | Value::Integer(_)
                | Value::BigInt(_)
                | Value::HugeInt(_)
                | Value::UTinyInt(_)
                | Value::USmallInt(_)
                | Value::UInteger(_)
                | Value::UBigInt(_)
                | Value::UHugeInt(_)
                | Value::Uuid(_)
                | Value::Float(_)
                | Value::Double(_)
                | Value::Decimal(..)
                | Value::Varchar(_)
                | Value::Blob(_)
                | Value::Date(_)
                | Value::Time(_)
                | Value::Timestamp(_)
                | Value::TimestampTz(_)
        ),
        Expression::Parameter(expression) => direct_parameter_type(&expression.slot.ty),
        _ => false,
    }
}

fn direct_parameter_type(ty: &LogicalType) -> bool {
    !matches!(
        ty,
        LogicalType::Interval
            | LogicalType::Array(..)
            | LogicalType::List(..)
            | LogicalType::Struct(..)
            | LogicalType::Unknown
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConstantExpression, ParameterExpression,
    };

    use crate::memory_runtime::QueryMemoryPool;
    use crate::runtime::{ParameterBindingEpoch, QueryOutputPort, QueryRuntimeContext};

    fn constant(value: Value, ty: LogicalType) -> Expression {
        Expression::Constant(ConstantExpression::new(value, ty))
    }

    fn row(expressions: Vec<Expression>) -> Box<[Expression]> {
        expressions.into_boxed_slice()
    }

    fn query(params: ParameterBindings) -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(params),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::discarding(),
        )
    }

    #[test]
    fn constant_matrix_executes_across_batches_without_boxing() {
        let rows = vec![
            row(vec![
                constant(Value::BigInt(1), LogicalType::BigInt),
                constant(Value::Varchar("one".into()), LogicalType::Varchar),
            ]),
            row(vec![
                constant(Value::BigInt(2), LogicalType::BigInt),
                constant(
                    Value::Varchar("a string longer than inline".into()),
                    LogicalType::Varchar,
                ),
            ]),
            row(vec![
                constant(Value::BigInt(3), LogicalType::BigInt),
                constant(Value::Null(LogicalType::Null), LogicalType::Null),
            ]),
        ];
        let types = [LogicalType::BigInt, LogicalType::Varchar];
        let query = query(ParameterBindings::empty());
        let allocator = paro_common::test_utils::test_allocator();
        let mut executor = ExpressionRowsExecutor::try_new(
            &rows,
            &types,
            query.session.as_ref(),
            Arc::clone(&allocator),
        )
        .expect("expression row executor");
        let mut output = Chunk::try_initialize(&types, 2, allocator).expect("output chunk");

        executor
            .execute_batch(0, 2, &rows, query.params.as_ref(), &query, &mut output)
            .expect("first expression row batch");
        assert_eq!(output.get_value(0, 0), Some(Value::BigInt(1)));
        assert_eq!(
            output.get_value(1, 1),
            Some(Value::Varchar("a string longer than inline".into()))
        );

        executor
            .execute_batch(2, 1, &rows, query.params.as_ref(), &query, &mut output)
            .expect("second expression row batch");
        assert_eq!(output.get_value(0, 0), Some(Value::BigInt(3)));
        assert_eq!(
            output.get_value(1, 0),
            Some(Value::Null(LogicalType::Varchar))
        );
    }

    #[test]
    fn parameters_use_current_binding_epoch() {
        let slot = ParameterSlot::new(RuntimeParamId::new(0), LogicalType::Integer);
        let rows = vec![row(vec![Expression::Parameter(ParameterExpression::new(
            slot,
        ))])];
        let types = [LogicalType::Integer];
        let initial = query(
            ParameterBindings::new(
                vec![Value::Integer(7)],
                types.to_vec(),
                ParameterBindingEpoch::new(1),
            )
            .expect("initial bindings"),
        );
        let allocator = paro_common::test_utils::test_allocator();
        let mut executor = ExpressionRowsExecutor::try_new(
            &rows,
            &types,
            initial.session.as_ref(),
            Arc::clone(&allocator),
        )
        .expect("expression row executor");
        let mut output = Chunk::try_initialize(&types, 1, allocator).expect("output chunk");

        executor
            .execute_batch(0, 1, &rows, initial.params.as_ref(), &initial, &mut output)
            .expect("initial parameter evaluation");
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(7)));

        let rebound = ParameterBindings::new(
            vec![Value::Integer(11)],
            types.to_vec(),
            ParameterBindingEpoch::new(2),
        )
        .expect("rebound parameters");
        executor
            .execute_batch(0, 1, &rows, &rebound, &initial, &mut output)
            .expect("rebound parameter evaluation");
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(11)));
    }

    #[test]
    fn complex_and_nested_roots_keep_general_executor_semantics() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let rows = vec![row(vec![
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::Equal,
                constant(Value::Integer(9), LogicalType::Integer),
                constant(Value::Integer(9), LogicalType::Integer),
            )),
            constant(
                Value::Array(
                    vec![Value::Integer(4), Value::Integer(5)],
                    LogicalType::Integer,
                    2,
                ),
                array_type.clone(),
            ),
        ])];
        let types = [LogicalType::Boolean, array_type.clone()];
        let query = query(ParameterBindings::empty());
        let allocator = paro_common::test_utils::test_allocator();
        let mut executor = ExpressionRowsExecutor::try_new(
            &rows,
            &types,
            query.session.as_ref(),
            Arc::clone(&allocator),
        )
        .expect("expression row executor");
        let mut output = Chunk::try_initialize(&types, 1, allocator).expect("output chunk");

        executor
            .execute_batch(0, 1, &rows, query.params.as_ref(), &query, &mut output)
            .expect("general expression row evaluation");
        assert_eq!(output.get_value(0, 0), Some(Value::Boolean(true)));
        assert_eq!(
            output.get_value(1, 0),
            Some(Value::Array(
                vec![Value::Integer(4), Value::Integer(5)],
                LogicalType::Integer,
                2,
            ))
        );
    }

    #[test]
    fn malformed_expression_matrix_fails_during_local_initialization() {
        let rows = vec![row(vec![constant(Value::Integer(1), LogicalType::Integer)])];
        let query = query(ParameterBindings::empty());
        let error = ExpressionRowsExecutor::try_new(
            &rows,
            &[LogicalType::Integer, LogicalType::Integer],
            query.session.as_ref(),
            paro_common::test_utils::test_allocator(),
        )
        .expect_err("malformed expression rows should fail");

        assert!(error.to_string().contains("has 1 expressions"));
    }

    #[test]
    fn changed_row_width_fails_before_batch_execution() {
        let rows = vec![row(vec![constant(Value::Integer(1), LogicalType::Integer)])];
        let types = [LogicalType::Integer];
        let query = query(ParameterBindings::empty());
        let allocator = paro_common::test_utils::test_allocator();
        let mut executor = ExpressionRowsExecutor::try_new(
            &rows,
            &types,
            query.session.as_ref(),
            Arc::clone(&allocator),
        )
        .expect("expression row executor");
        let malformed_rows = vec![row(vec![])];
        let mut output = Chunk::try_initialize(&types, 1, allocator).expect("output chunk");

        let error = executor
            .execute_batch(
                0,
                1,
                &malformed_rows,
                query.params.as_ref(),
                &query,
                &mut output,
            )
            .expect_err("changed row width should fail before indexing");

        assert!(error.to_string().contains("row 0 has 0 expressions"));
    }
}
