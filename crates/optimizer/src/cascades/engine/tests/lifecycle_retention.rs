// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Retention counters describe the events actually present in the snapshot.

use super::*;

#[test]
fn dense_parent_publications_do_not_hide_logical_or_root_evidence() {
    let (mut engine, root, _) = strong_tree_engine();
    engine.collect_rule_work_profile = true;
    let mut offered = [0_u64; 6];
    for stage in [
        CandidateLifecycleStage::ChildReady,
        CandidateLifecycleStage::TuplePriced,
        CandidateLifecycleStage::ParentPublished,
        CandidateLifecycleStage::LogicalPublished,
        CandidateLifecycleStage::PhysicalRecipePublished,
        CandidateLifecycleStage::RootQualified,
    ] {
        for ordinal in 0..MAX_CANDIDATE_LIFECYCLE_EVENTS + 1 {
            offered[stage as usize] += 1;
            engine.note_candidate_lifecycle(CandidateLifecycleEvent {
                stage,
                elapsed_us: ordinal as u64,
                group: root,
                goal: None,
                candidate: None,
                source: None,
                binding: None,
                source_child: None,
                logical: None,
                physical: None,
                recipe: None,
                rule: None,
                children: Box::new([]),
                facts: Box::new([]),
                expected_cost_bits: None,
                upper_cost_bits: None,
            });
        }
    }
    let ledger = &engine.search_milestones;
    let mut retained = [0_u64; 6];
    for event in &ledger.candidate_lifecycle {
        retained[event.stage as usize] += 1;
    }
    assert_eq!(retained, ledger.candidate_lifecycle_stage_stored);
    assert_eq!(retained, CANDIDATE_LIFECYCLE_STAGE_LIMITS);
    assert!(ledger.candidate_lifecycle.len() <= MAX_CANDIDATE_LIFECYCLE_EVENTS);
    for stage in 0..6 {
        assert_eq!(
            offered[stage],
            retained[stage] + ledger.candidate_lifecycle_stage_dropped[stage]
        );
    }
    assert_eq!(
        ledger.candidate_lifecycle_dropped,
        ledger.candidate_lifecycle_stage_dropped.iter().sum::<u64>()
    );
}
