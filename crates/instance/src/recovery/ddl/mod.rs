// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod alter;
mod codec;
mod graph;
mod index;
mod schema;
mod sequence;
mod table;
mod view;

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::catalog::DEFAULT_SCHEMA;
use paro_common::ddl::{DdlChange, DdlChangeRecord};
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::effect::CatalogTxnOp;
use paro_common::error as paro_error;
use paro_parser::ast::{AlterTableAction, Statement, TableReference};
use paro_parser::parse_one;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatalogApplyPhase {
    Create,
    Alter,
    Drop,
}

pub(super) fn catalog_apply_phase(change: &DdlChangeRecord) -> CatalogApplyPhase {
    match &change.change {
        DdlChange::CreateSchema(_)
        | DdlChange::CreateTable(_)
        | DdlChange::CreateView(_)
        | DdlChange::CreateIndex(_)
        | DdlChange::CreatePropertyGraph(_)
        | DdlChange::CreateSequence(_) => CatalogApplyPhase::Create,
        DdlChange::AlterEntry(_) => CatalogApplyPhase::Alter,
        DdlChange::DropSchema(_)
        | DdlChange::DropTable(_)
        | DdlChange::DropView(_)
        | DdlChange::DropIndex(_)
        | DdlChange::DropPropertyGraph(_)
        | DdlChange::DropSequence(_) => CatalogApplyPhase::Drop,
    }
}

pub(super) fn route_registry_table_keys(
    change: &DdlChangeRecord,
    default_database: &str,
) -> paro_common::error::Result<Vec<DdlObjectKey>> {
    match &change.change {
        DdlChange::CreateTable(_) | DdlChange::DropTable(_)
            if change.key.kind == DdlObjectKind::Table =>
        {
            Ok(vec![change.key.clone()])
        }
        DdlChange::AlterEntry(payload) => {
            route_registry_table_keys_for_alter(&payload.sql, default_database)
        }
        _ => Ok(Vec::new()),
    }
}

fn route_registry_table_keys_for_alter(
    sql: &str,
    default_database: &str,
) -> paro_common::error::Result<Vec<DdlObjectKey>> {
    let statement = parse_one(sql).map_err(|err| {
        paro_error::serialization_error(format!("failed to parse ALTER ENTRY SQL: {err}"))
    })?;
    match statement.stmt {
        Statement::RenameTable(stmt) => {
            let database_name = stmt
                .database
                .map(|ident| ident.name)
                .unwrap_or_else(|| default_database.to_string());
            let schema_name = stmt
                .schema
                .map(|ident| ident.name)
                .unwrap_or_else(|| DEFAULT_SCHEMA.to_string());
            let target_database_name = stmt
                .new_database
                .map(|ident| ident.name)
                .unwrap_or_else(|| database_name.clone());
            let target_schema_name = stmt
                .new_schema
                .map(|ident| ident.name)
                .unwrap_or_else(|| schema_name.clone());
            Ok(vec![
                DdlObjectKey::new(
                    database_name,
                    Some(schema_name),
                    stmt.table.name,
                    DdlObjectKind::Table,
                ),
                DdlObjectKey::new(
                    target_database_name,
                    Some(target_schema_name),
                    stmt.new_table.name,
                    DdlObjectKind::Table,
                ),
            ])
        }
        Statement::AlterTable(stmt) => {
            let TableReference::Table {
                database,
                schema,
                table,
                ..
            } = stmt.table_reference
            else {
                return Ok(Vec::new());
            };
            let database_name = database
                .map(|ident| ident.name)
                .unwrap_or_else(|| default_database.to_string());
            let schema_name = schema
                .map(|ident| ident.name)
                .unwrap_or_else(|| DEFAULT_SCHEMA.to_string());
            let mut keys = vec![DdlObjectKey::new(
                database_name.clone(),
                Some(schema_name.clone()),
                table.name,
                DdlObjectKind::Table,
            )];
            if let AlterTableAction::RenameTable { new_table } = stmt.action {
                keys.push(DdlObjectKey::new(
                    database_name,
                    Some(schema_name),
                    new_table.name,
                    DdlObjectKind::Table,
                ));
            }
            Ok(keys)
        }
        _ => Ok(Vec::new()),
    }
}

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_catalog_ops_by_phase(
        &mut self,
        ops: &[CatalogTxnOp],
        commit_id: u64,
        phase: CatalogApplyPhase,
    ) -> paro_common::error::Result<()> {
        for op in ops
            .iter()
            .filter(|op| catalog_apply_phase(&op.change) == phase)
        {
            self.replay_catalog_txn_op(op, commit_id)?;
        }
        Ok(())
    }

    pub(in crate::recovery) fn replay_catalog_non_drop_ops(
        &mut self,
        ops: &[CatalogTxnOp],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_catalog_ops_by_phase(ops, commit_id, CatalogApplyPhase::Create)?;
        self.replay_catalog_ops_by_phase(ops, commit_id, CatalogApplyPhase::Alter)?;
        Ok(())
    }

    pub(in crate::recovery) fn replay_catalog_drop_ops(
        &mut self,
        ops: &[CatalogTxnOp],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_catalog_ops_by_phase(ops, commit_id, CatalogApplyPhase::Drop)
    }

    pub(in crate::recovery) fn replay_catalog_txn_op(
        &mut self,
        op: &CatalogTxnOp,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match &op.change.change {
            DdlChange::CreateSchema(payload) => {
                self.replay_create_schema(&op.change.key.name, payload, commit_id)
            }
            DdlChange::CreateTable(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn CREATE TABLE missing schema")
                })?;
                self.replay_create_table(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::CreateView(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn CREATE VIEW missing schema")
                })?;
                self.replay_create_view(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::DropTable(_) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP TABLE missing schema")
                })?;
                self.replay_drop_table(schema_name, &op.change.key.name, commit_id)
            }
            DdlChange::DropView(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP VIEW missing schema")
                })?;
                self.replay_drop_view(
                    schema_name,
                    &op.change.key.name,
                    payload.if_exists,
                    commit_id,
                )
            }
            DdlChange::DropSchema(_) => self.replay_drop_schema(&op.change.key.name, commit_id),
            DdlChange::DropIndex(_) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP INDEX missing schema")
                })?;
                self.replay_drop_index(schema_name, &op.change.key.name, commit_id)
            }
            DdlChange::CreateIndex(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn CREATE INDEX missing schema")
                })?;
                self.replay_create_index(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::CreatePropertyGraph(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "catalog txn CREATE PROPERTY GRAPH missing schema",
                    )
                })?;
                self.replay_create_property_graph(schema_name, payload, commit_id)
            }
            DdlChange::DropPropertyGraph(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "catalog txn DROP PROPERTY GRAPH missing schema",
                    )
                })?;
                self.replay_drop_property_graph(
                    schema_name,
                    &op.change.key.name,
                    payload.if_exists,
                    commit_id,
                )
            }
            DdlChange::CreateSequence(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn CREATE SEQUENCE missing schema")
                })?;
                self.replay_create_sequence(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::DropSequence(_) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP SEQUENCE missing schema")
                })?;
                self.replay_drop_sequence(schema_name, &op.change.key.name, commit_id)
            }
            DdlChange::AlterEntry(payload) => self.replay_alter_entry(&payload.sql, commit_id),
        }
    }
}
