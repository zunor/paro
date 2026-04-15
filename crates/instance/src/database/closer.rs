// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::handle::{DatabaseCloseAction, DatabaseHandle};
use paro_common::logging::targets;

pub struct DatabaseCloser;

impl DatabaseCloser {
    pub fn close(db: &DatabaseHandle, action: DatabaseCloseAction) -> anyhow::Result<()> {
        let _guard = db.state_handle().close_guard();

        if !db.state_handle().mark_closed() {
            return Ok(());
        }

        let should_checkpoint = match action {
            DatabaseCloseAction::Checkpoint | DatabaseCloseAction::TryCheckpoint => {
                db.has_storage_manager() && db.path() != ":memory:"
            }
        };

        if should_checkpoint {
            match db.checkpoint() {
                Ok(()) => {
                    tracing::info!(
                        target: targets::CHECKPOINT,
                        db = %db.name(),
                        "Database checkpointed on close"
                    );
                }
                Err(err) => match action {
                    DatabaseCloseAction::Checkpoint => {
                        Self::cleanup(db);
                        return Err(err);
                    }
                    DatabaseCloseAction::TryCheckpoint => {
                        tracing::warn!(
                            target: targets::CHECKPOINT,
                            db = %db.name(),
                            err = %err,
                            "Failed to checkpoint on close"
                        );
                    }
                },
            }
        }

        Self::cleanup(db);
        Ok(())
    }

    pub fn on_detach(db: &DatabaseHandle) -> anyhow::Result<()> {
        Self::cleanup(db);
        db.state_handle().set_dropped();
        db.state_handle().mark_closed();
        tracing::info!(
            target: targets::INSTANCE,
            db = %db.name(),
            "Database detached"
        );
        Ok(())
    }

    fn cleanup(db: &DatabaseHandle) {
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            "Cleaning up database resources"
        );
        db.compaction().shutdown();
    }
}
