//! Decorrelate [`DependentJoin`](crate::operator::DependentJoin) into regular joins.

mod flattener;
mod helpers;

use crate::binder::Binder;
use crate::operator::{DependentJoin, LogicalOperator};
use paro_common::error::Result;
use std::sync::Arc;

use flattener::DependentJoinFlattener;

pub(crate) fn flatten_dependent_join(
    binder: &mut Binder,
    dependent_join: DependentJoin,
) -> Result<LogicalOperator> {
    let delim_table_index = binder.bind_context.generate_table_index();
    let correlated_columns = dependent_join.correlated_columns.clone();

    let mut flattener = DependentJoinFlattener::new(
        Arc::clone(binder.bind_context.snapshot().shared()),
        correlated_columns,
        delim_table_index,
        Arc::clone(&binder.cast_functions),
    );

    flattener.flatten(binder, dependent_join)
}
