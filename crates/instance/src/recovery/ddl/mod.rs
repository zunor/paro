// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod alter;
mod codec;
mod graph;
mod index;
mod routine;
mod schema;
mod sequence;
mod table;
mod view;

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_common::ddl::{DdlChange, DdlChangeRecord};
use paro_common::effect::CatalogTxnOp;
use paro_common::error as paro_error;

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
        | DdlChange::CreateSequence(_)
        | DdlChange::CreateRoutine(_) => CatalogApplyPhase::Create,
        DdlChange::AlterEntry(_) => CatalogApplyPhase::Alter,
        DdlChange::DropSchema(_)
        | DdlChange::DropTable(_)
        | DdlChange::DropView(_)
        | DdlChange::DropIndex(_)
        | DdlChange::DropPropertyGraph(_)
        | DdlChange::DropSequence(_)
        | DdlChange::DropRoutine(_) => CatalogApplyPhase::Drop,
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
            DdlChange::CreateRoutine(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn CREATE FUNCTION missing schema")
                })?;
                self.replay_create_routine(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::DropSequence(_) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP SEQUENCE missing schema")
                })?;
                self.replay_drop_sequence(schema_name, &op.change.key.name, commit_id)
            }
            DdlChange::DropRoutine(payload) => {
                let schema_name = op.change.key.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error("catalog txn DROP FUNCTION missing schema")
                })?;
                self.replay_drop_routine(schema_name, &op.change.key.name, payload, commit_id)
            }
            DdlChange::AlterEntry(payload) => self.replay_alter_entry(&payload.sql, commit_id),
        }
    }
}
