// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundBaseTable;
use crate::binder::Binder;
use crate::operator::{Get, LogicalOperator};
use paro_common::error::Result;
use paro_common::types::LogicalType;

impl Binder {
    pub(crate) fn plan_base_table_ref(
        &mut self,
        base_ref: BoundBaseTable,
    ) -> Result<LogicalOperator> {
        let table_names: Vec<String> = base_ref
            .table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let table_types: Vec<_> = base_ref
            .table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let (table_names, table_types) = if self.needs_row_id_binding(base_ref.table_index) {
            let mut names = table_names;
            let mut types = table_types;
            names.push("rowid".to_string());
            types.push(LogicalType::BigInt);
            (names, types)
        } else {
            (table_names, table_types)
        };
        let get = Get::new(
            base_ref.table_index,
            table_names,
            table_types,
            base_ref.table,
        )
        .with_relation(base_ref.relation_name, base_ref.relation_alias);
        Ok(LogicalOperator::Get(get))
    }
}
