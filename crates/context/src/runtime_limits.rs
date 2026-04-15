// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Default)]
pub struct RuntimeLimits {
    pub max_threads: usize,
    pub max_memory: usize,
    pub use_temporary_directory: bool,
    pub temporary_directory: String,
    pub max_temp_directory_size: Option<usize>,
    pub force_external: bool,
}
