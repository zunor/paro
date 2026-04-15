// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plans `INSERT`, `DELETE`, `UPDATE`. No `RETURNING`, no `DELETE … USING`, no `UPDATE … FROM`, no bound constraints yet.

use crate::binder::ir::statement::{BoundDeleteInfo, BoundInsertInfo, BoundUpdateInfo};
use crate::binder::Binder;
use crate::operator::{Delete, Filter, Get, Insert, LogicalOperator, Update};
use paro_common::error::Result;
use paro_common::types::LogicalType;

impl Binder {
    pub(crate) fn plan_insert(&mut self, info: BoundInsertInfo) -> Result<LogicalOperator> {
        let child = self.plan_query(*info.source)?;
        let op = Insert::new(
            info.table,
            info.column_indices,
            info.expected_types,
            info.on_conflict,
            self.wrap_plan(child),
        );
        Ok(LogicalOperator::Insert(op))
    }

    pub(crate) fn plan_delete(&mut self, info: BoundDeleteInfo) -> Result<LogicalOperator> {
        let is_full_table_delete = info.condition.is_none();
        let column_names: Vec<String> = info.table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<_> = info
            .table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let mut column_names = column_names;
        let mut column_types = column_types;
        column_names.push("rowid".to_string());
        column_types.push(LogicalType::BigInt);

        let scan = Get::new(
            info.table_index,
            column_names,
            column_types,
            info.table.clone(),
        );
        let mut root = LogicalOperator::Get(scan);

        if let Some(condition) = info.condition {
            let filter = Filter::new(self.wrap_plan(root), vec![condition]);
            root = LogicalOperator::Filter(filter);
        }

        let delete_table_index = self.bind_context.generate_table_index() as u32;
        let delete = Delete::new(
            info.table,
            delete_table_index,
            self.wrap_plan(root),
            is_full_table_delete,
        );

        Ok(LogicalOperator::Delete(delete))
    }

    pub(crate) fn plan_update(&mut self, info: BoundUpdateInfo) -> Result<LogicalOperator> {
        let column_names: Vec<String> = info.table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<_> = info
            .table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let mut column_names = column_names;
        let mut column_types = column_types;
        column_names.push("rowid".to_string());
        column_types.push(LogicalType::BigInt);

        let scan = Get::new(
            info.table_index,
            column_names,
            column_types,
            info.table.clone(),
        );
        let mut root = LogicalOperator::Get(scan);

        if let Some(condition) = info.condition {
            let filter = Filter::new(self.wrap_plan(root), vec![condition]);
            root = LogicalOperator::Filter(filter);
        }

        let update_table_index = self.bind_context.generate_table_index() as u32;
        let update = Update::new(
            info.table,
            update_table_index,
            info.column_indices,
            info.expressions,
            self.wrap_plan(root),
        );

        Ok(LogicalOperator::Update(update))
    }
}
