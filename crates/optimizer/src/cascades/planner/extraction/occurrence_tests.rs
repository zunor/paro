// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn repeated_candidate_definitions_have_distinct_stable_artifact_instances() {
    let definition = Fingerprint(17);
    let root = Fingerprint(0);
    let left = child_occurrence(root, 0);
    let right = child_occurrence(root, 1);
    assert_ne!(
        artifact_instance(definition, left),
        artifact_instance(definition, right)
    );
    assert_ne!(
        artifact_instance(definition, child_occurrence(left, 1)),
        artifact_instance(definition, child_occurrence(right, 0))
    );
    assert_eq!(
        artifact_instance(definition, left),
        artifact_instance(definition, child_occurrence(Fingerprint(0), 0))
    );
    assert_ne!(
        artifact_instance(definition, left),
        artifact_instance(Fingerprint(18), left)
    );
}
