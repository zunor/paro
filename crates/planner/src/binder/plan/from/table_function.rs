// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::{BoundExternalRoutine, BoundTableFunction};
use crate::binder::Binder;
use crate::operator::{LogicalExternalTable, LogicalOperator, Projection, TableFunctionGet};
use crate::plan::LogicalPlan;
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_table_function_ref(
        &mut self,
        tf_ref: BoundTableFunction,
    ) -> Result<LogicalOperator> {
        let table_function_get = TableFunctionGet::new(
            tf_ref.function,
            tf_ref.table_index,
            tf_ref.column_names,
            tf_ref.column_types,
            tf_ref.bound_arguments,
        )
        .with_ordinality_flag(tf_ref.with_ordinality);
        Ok(LogicalOperator::TableFunctionGet(table_function_get))
    }

    pub(crate) fn plan_external_routine_ref(
        &mut self,
        routine_ref: BoundExternalRoutine,
    ) -> Result<LogicalOperator> {
        let projection_index = self.bind_context.generate_table_index();
        let child = LogicalPlan::synthetic(LogicalOperator::Projection(
            Projection::new(
                projection_index,
                LogicalPlan::synthetic(LogicalOperator::DummyScan),
                routine_ref.bound_arguments.clone(),
            )
            .with_output_names(
                (0..routine_ref.bound_arguments.len())
                    .map(|idx| format!("__external_arg_{}", idx + 1))
                    .collect(),
            ),
        ));

        Ok(LogicalOperator::ExternalTable(
            LogicalExternalTable::new(
                routine_ref.table_index,
                routine_ref.column_names,
                routine_ref.column_types,
                routine_ref.call_expression,
                routine_ref.call,
            )
            .with_child(
                child,
                routine_ref.lateral,
                !routine_ref.correlated_columns.is_empty(),
            ),
        ))
    }
}
