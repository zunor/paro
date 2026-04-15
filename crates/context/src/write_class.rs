// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteClass {
    #[default]
    Clean,
    HasDml,
    HasDdl,
    HasDmlAndDdl,
}
