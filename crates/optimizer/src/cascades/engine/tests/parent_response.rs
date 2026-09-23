// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! A yielded child response is useful before that child's tail is exhausted.

use super::*;

#[test]
fn yielded_child_is_consumed_before_its_unexplored_tail() {
    let (mut engine, root, goal) = strong_tree_engine();
    let root_expr = engine.memo().group(root).unwrap().logical_exprs()[0];
    let child = engine.memo().logical_expr(root_expr).unwrap().key.children[0];
    engine.optimize_group(child, goal).unwrap();
    let child_expr = engine.memo().group(child).unwrap().logical_exprs()[0];
    // Deterministic independently priced domain. Each prefix response improves
    // the leaf; its last legal recipe is strictly better than the first slice.
    // Prepublish recipes so this test isolates consumption, not construction.
    for ordinal in 0..40_u32 {
        let implementation = TreeImplementation {
            id: ImplementationId(10_000 + ordinal),
            operator: Fingerprint(100),
            child: None,
            child_row_goal: None,
            local_score: 1.0 / f64::from(ordinal + 2),
            mandatory: true,
        };
        let candidates = implementation
            .candidates(
                child_expr,
                goal,
                &ImplementationContext {
                    memo: engine.memo(),
                    group: child,
                },
            )
            .unwrap();
        let id = implementation.id();
        engine
            .registry
            .register_implementation(implementation)
            .unwrap();
        for candidate in candidates {
            engine
                .admit_candidate(child, child_expr, id, goal, candidate)
                .unwrap();
        }
    }
    engine.enumerate_implementations(root, goal).unwrap();
    let before = engine.child_combination_cost_synthesis_count;
    engine.preserve_incomplete_physical = true;
    engine.physical_interleave_step_mode = true;
    engine.physical_interleave_step_publications = 0;
    engine.physical_interleave_step_yielded = false;
    engine.optimize_group(root, goal).unwrap();
    assert!(engine.physical_interleave_step_yielded);
    let leaf = engine.memo().group(child).unwrap().winner(goal).unwrap();
    assert!(
        leaf.cost.score.range.expected > 1.0 / 41.0,
        "child tail must remain unexplored"
    );
    let parent = engine
        .memo()
        .group(root)
        .unwrap()
        .winner(goal)
        .expect("parent must consume the child's ready prefix before resuming its tail");
    assert_eq!(parent.children[0].candidate, leaf.candidate);
    assert_eq!(
        parent.cost.score.range.expected,
        leaf.cost.score.range.expected
    );
    assert_eq!(
        engine.child_combination_cost_synthesis_count - before,
        33,
        "32 child publications plus one explicitly counted parent composition"
    );
    assert!(
        !engine.physical_subproblems[&(root, goal)]
            .resident
            .as_ref()
            .unwrap()
            .complete
    );
    assert!(
        !engine.physical_subproblems[&(child, goal)]
            .resident
            .as_ref()
            .unwrap()
            .complete
    );
    let reference = ChildWinnerRef {
        group: root,
        goal,
        candidate: parent.candidate,
    };
    super::super::super::verifier::WinnerVerifier::verify_candidate_tree(engine.memo(), reference)
        .unwrap();
    let frozen = engine.memo().freeze_candidate_tree(reference).unwrap();
    engine.physical_interleave_step_mode = false;
    engine.preserve_incomplete_physical = false;
    engine.optimize_group(root, goal).unwrap();
    let final_winner = engine.memo().group(root).unwrap().winner(goal).unwrap();
    assert_eq!(final_winner.cost.score.range.expected, 1.0 / 41.0);
    assert!(final_winner.cost.score.range.expected < frozen.winner.cost.score.range.expected);
    assert_eq!(
        frozen.children[0].reference.candidate,
        reference_child(&frozen)
    );
}

fn reference_child(frozen: &FrozenCandidate) -> CandidateId {
    frozen.winner.children[0].candidate
}
