// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan COPY TO - Convert CopyTo to PhysicalCopyToFile

use std::sync::Arc;

use super::generator::PhysicalPlanGenerator;
use crate::operator::persistent::copy_to_file::PhysicalCopyToFile;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::copy_to::CopyTo;

impl PhysicalPlanGenerator {
    /// Create physical plan for CopyTo.
    pub fn create_plan_copy_to(
        &self,
        copy: &CopyTo,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let physical_copy = PhysicalCopyToFile::new(
            copy.copy_function.clone(),
            copy.bind_data.clone(),
            copy.file_path.clone(),
            copy.options.per_thread_output,
            copy.types.clone(),
            child,
        );
        Ok(Arc::new(physical_copy))
    }
}
