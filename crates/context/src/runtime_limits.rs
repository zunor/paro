// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone)]
pub struct RuntimeLimits {
    pub max_threads: usize,
    pub max_memory: usize,
    pub use_temporary_directory: bool,
    pub temporary_directory: String,
    pub max_temp_directory_size: Option<usize>,
    pub force_external: bool,
    pub rowset_scan_pushdown: bool,
    pub parallel_scheduler: bool,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_threads: 0,
            max_memory: 0,
            use_temporary_directory: false,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: true,
        }
    }
}
