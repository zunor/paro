// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::transaction::commit::CommitPipeline;
use crate::{CommitFailure, CommitOutcome, Session};
use paro_context::StatementTimeoutDriver;
use paro_instance::Instance;
use std::sync::Arc;

/// Test-only helper for session/integration tests that need a concrete instance.
#[derive(Clone)]
pub struct TestSessionBuilder {
    session_id: u64,
    user_name: String,
    instance: Option<Arc<Instance>>,
    statement_timeout_driver: Option<Arc<dyn StatementTimeoutDriver>>,
}

impl TestSessionBuilder {
    pub fn minimal() -> Self {
        Self {
            session_id: 1,
            user_name: "paro".to_string(),
            instance: None,
            statement_timeout_driver: None,
        }
    }

    pub fn with_session_id(mut self, session_id: u64) -> Self {
        self.session_id = session_id;
        self
    }

    pub fn with_user(mut self, user_name: impl Into<String>) -> Self {
        self.user_name = user_name.into();
        self
    }

    pub fn with_instance(mut self, instance: Arc<Instance>) -> Self {
        self.instance = Some(instance);
        self
    }

    pub fn with_timeout_driver(
        mut self,
        statement_timeout_driver: Arc<dyn StatementTimeoutDriver>,
    ) -> Self {
        self.statement_timeout_driver = Some(statement_timeout_driver);
        self
    }

    pub fn build_instance(&self) -> Arc<Instance> {
        self.instance
            .clone()
            .unwrap_or_else(Instance::new_in_memory)
    }

    pub fn build(self) -> Session {
        let instance = self.instance.unwrap_or_else(Instance::new_in_memory);
        let execution_control = self
            .statement_timeout_driver
            .map(crate::SessionExecutionControl::with_timeout_driver)
            .map(Arc::new)
            .unwrap_or_else(|| Arc::new(crate::SessionExecutionControl::new()));
        Session::with_user_and_execution_control(
            self.session_id,
            instance,
            self.user_name,
            execution_control,
        )
    }

    pub fn build_with_instance(self) -> (Arc<Instance>, Session) {
        let instance = self.instance.unwrap_or_else(Instance::new_in_memory);
        let execution_control = self
            .statement_timeout_driver
            .map(crate::SessionExecutionControl::with_timeout_driver)
            .map(Arc::new)
            .unwrap_or_else(|| Arc::new(crate::SessionExecutionControl::new()));
        let session = Session::with_user_and_execution_control(
            self.session_id,
            Arc::clone(&instance),
            self.user_name,
            execution_control,
        );
        (instance, session)
    }
}

/// Commit the current transaction through the durable pipeline but intentionally
/// skip post-commit side effects. Tests use this to simulate a crash after the
/// durable commit record is published.
pub fn durable_commit_without_post_commit(
    session: &mut Session,
) -> std::result::Result<CommitOutcome, CommitFailure> {
    let frozen = session
        .transaction
        .freeze()
        .map_err(|error| CommitFailure {
            error,
            rollback_succeeded: true,
        })?;
    CommitPipeline::new(session, frozen).execute()
}
