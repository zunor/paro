// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::handle::DatabaseCloseAction;
use crate::{Instance, InstanceDdlOwner};
use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceShutdownMode {
    Checkpoint,
    TryCheckpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceShutdownDisposition {
    Dirty,
    Clean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceShutdownReport {
    pub databases_closed: usize,
    pub databases_failed: usize,
    pub disposition: InstanceShutdownDisposition,
    pub clean_shutdown_persisted: bool,
}

#[derive(Debug, Default)]
struct InstanceShutdownFailures {
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct InstanceQuiesceProof {
    pub(crate) boot_id: u64,
    pub(crate) _private: (),
}

impl Instance {
    pub fn verify_quiesced_for_clean_shutdown(&self) -> Result<InstanceQuiesceProof> {
        self.lifecycle.admission.request_shutdown()?;

        let active_connections = self
            .runtime
            .connection_registry()
            .get_active_connection_count();
        if active_connections != 0 {
            return Err(paro_error::cannot_connect_now().detail(format!(
                "instance clean shutdown requires all tracked connections to drain; {} active tracked connection(s) remain",
                active_connections
            )));
        }

        Ok(InstanceQuiesceProof {
            boot_id: self.lifecycle.boot_id,
            _private: (),
        })
    }

    pub fn shutdown_dirty(&self, mode: InstanceShutdownMode) -> Result<InstanceShutdownReport> {
        self.perform_shutdown(mode, false)
    }

    pub fn shutdown_clean(
        &self,
        mode: InstanceShutdownMode,
        proof: InstanceQuiesceProof,
    ) -> Result<InstanceShutdownReport> {
        if proof.boot_id != self.lifecycle.boot_id {
            return Err(paro_error::cannot_connect_now()
                .detail("instance quiesce proof does not match the current boot"));
        }
        self.perform_shutdown(mode, true)
    }

    fn perform_shutdown(
        &self,
        mode: InstanceShutdownMode,
        allow_clean: bool,
    ) -> Result<InstanceShutdownReport> {
        self.lifecycle.admission.request_shutdown()?;
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::Shutdown)?;

        let mut failures = InstanceShutdownFailures::default();

        let dirty_state_persisted = match self
            .metadata
            .persist_dirty_run_state(self.lifecycle.boot_id)
        {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    target: paro_common::logging::targets::INSTANCE,
                    err = %err,
                    "Failed to persist dirty instance run state before shutdown"
                );
                failures.record(format!("persist dirty run state: {}", err));
                false
            }
        };

        let mut databases_closed = 0;
        let mut databases_failed = 0;
        for db in self.database_service.managed_runtime_databases() {
            match db.close(mode.into()) {
                Ok(()) => databases_closed += 1,
                Err(err) => {
                    databases_failed += 1;
                    tracing::warn!(
                        target: paro_common::logging::targets::INSTANCE,
                        db = %db.name(),
                        err = %err,
                        "Managed database failed to close during instance shutdown"
                    );
                    failures.record(format!("close managed database {}: {}", db.name(), err));
                }
            }
        }

        let mut disposition = InstanceShutdownDisposition::Dirty;
        let mut clean_shutdown_persisted = false;
        for db in self.database_service.system_runtime_databases() {
            if let Err(err) = db.close(mode.into()) {
                tracing::warn!(
                    target: paro_common::logging::targets::INSTANCE,
                    db = %db.name(),
                    err = %err,
                    "System database failed to close during instance shutdown"
                );
                failures.record(format!("close system database {}: {}", db.name(), err));
            }
        }

        if allow_clean && dirty_state_persisted && databases_failed == 0 && failures.is_empty() {
            let database_count = databases_closed as u64;
            let default_database_id = self.database_service.registry().default_database_id();
            match self.metadata.persist_clean_run_state(
                self.lifecycle.boot_id,
                database_count,
                default_database_id,
            ) {
                Ok(()) => {
                    disposition = InstanceShutdownDisposition::Clean;
                    clean_shutdown_persisted = true;
                }
                Err(err) => {
                    tracing::warn!(
                        target: paro_common::logging::targets::INSTANCE,
                        err = %err,
                        "Failed to persist clean instance run state during shutdown"
                    );
                    failures.record(format!("persist clean run state: {}", err));
                }
            }
        }

        self.lifecycle.admission.terminate();

        let report = InstanceShutdownReport {
            databases_closed,
            databases_failed,
            disposition,
            clean_shutdown_persisted,
        };

        if failures.is_empty() {
            Ok(report)
        } else {
            Err(
                paro_error::system_error("instance shutdown completed with failures")
                    .detail(failures.render_detail(&report)),
            )
        }
    }
}

impl From<InstanceShutdownMode> for DatabaseCloseAction {
    fn from(value: InstanceShutdownMode) -> Self {
        match value {
            InstanceShutdownMode::Checkpoint => DatabaseCloseAction::Checkpoint,
            InstanceShutdownMode::TryCheckpoint => DatabaseCloseAction::TryCheckpoint,
        }
    }
}

impl InstanceShutdownFailures {
    fn record(&mut self, reason: String) {
        self.reasons.push(reason);
    }

    fn is_empty(&self) -> bool {
        self.reasons.is_empty()
    }

    fn render_detail(&self, report: &InstanceShutdownReport) -> String {
        format!(
            "databases_closed={}, databases_failed={}, disposition={:?}, clean_shutdown_persisted={}; failures: {}",
            report.databases_closed,
            report.databases_failed,
            report.disposition,
            report.clean_shutdown_persisted,
            self.reasons.join(" | ")
        )
    }
}
