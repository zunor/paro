// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Default)]
pub struct StreamingLimitTransformGlobal {
    pub limit: Option<usize>,
    pub offset: usize,
}

#[derive(Debug, Default)]
pub struct StreamingLimitTransformLocal {
    pub emitted: usize,
    pub current_offset: usize,
}
