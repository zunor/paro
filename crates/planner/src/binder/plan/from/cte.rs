// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundFromCTE;
use crate::binder::Binder;
use crate::operator::{CTERef, LogicalOperator};
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_cte_ref(&mut self, cte_ref: BoundFromCTE) -> Result<LogicalOperator> {
        Ok(LogicalOperator::CTERef(CTERef::new(
            cte_ref.cte_index,
            cte_ref.table_index,
            cte_ref.column_names,
            cte_ref.column_types,
        )))
    }
}
