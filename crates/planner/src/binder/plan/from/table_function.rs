use crate::binder::ir::BoundTableFunction;
use crate::binder::Binder;
use crate::operator::{LogicalOperator, TableFunctionGet};
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
}
