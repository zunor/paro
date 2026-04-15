// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupDescriptor {
    ShutdownTablet {
        tablet_id: u64,
        data_dir_components: Vec<String>,
        move_to_trash: bool,
    },
    RemoveDirectory {
        path_components: Vec<String>,
        recursive: bool,
    },
}
