// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::Session;
use paro_instance::Instance;
use std::sync::Arc;

/// Test-only helper for session/integration tests that need a concrete instance.
#[derive(Clone)]
pub struct TestSessionBuilder {
    session_id: u64,
    user_name: String,
    instance: Option<Arc<Instance>>,
}

impl TestSessionBuilder {
    pub fn minimal() -> Self {
        Self {
            session_id: 1,
            user_name: "paro".to_string(),
            instance: None,
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

    pub fn build_instance(&self) -> Arc<Instance> {
        self.instance
            .clone()
            .unwrap_or_else(Instance::new_in_memory)
    }

    pub fn build(self) -> Session {
        let instance = self.instance.unwrap_or_else(Instance::new_in_memory);
        Session::with_user(self.session_id, instance, self.user_name)
    }

    pub fn build_with_instance(self) -> (Arc<Instance>, Session) {
        let instance = self.instance.unwrap_or_else(Instance::new_in_memory);
        let session = Session::with_user(self.session_id, Arc::clone(&instance), self.user_name);
        (instance, session)
    }
}
