// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_instance::{Instance, InstanceConfig};
use paro_storage::tablet::{
    current_delete_patch_inline_row_ref_threshold, set_delete_patch_inline_row_ref_threshold,
};

#[test]
fn instance_build_applies_delete_patch_inline_threshold() {
    let previous = current_delete_patch_inline_row_ref_threshold();
    let config = InstanceConfig::in_memory().with_delete_patch_inline_row_ref_threshold(7);

    let instance = Instance::new_in_memory_with_config(config).unwrap();
    assert_eq!(current_delete_patch_inline_row_ref_threshold(), 7);

    drop(instance);
    set_delete_patch_inline_row_ref_threshold(previous);
}
