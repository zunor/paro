// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_instance::{Instance, InstanceConfig};

pub fn create_in_memory_instance() -> Arc<Instance> {
    Instance::new(InstanceConfig::in_memory()).expect("instance")
}
