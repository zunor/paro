# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from harness.e8_oracle import (CalibrationGate, CandidateObservation, EvidenceStatus,
                               NodeObservation, calibrate_g_stats, candidate_selection_loss,
                               derived_domain, equality_join_domain, filter_domain, group_count,
                               inner_join, left_outer_join, observed_domain, partial_domain,
                               sql_filter, unknown_domain)


def test_finite_domain_filter_is_exact_and_repeated_predicate_is_idempotent() -> None:
    rows = [{"k": 1}, {"k": 1}, {"k": 2}, {"k": None}]
    evidence = observed_domain(rows, "k")
    filtered = filter_domain(evidence, {1, 2}, "k-in-12")
    repeated = filter_domain(filtered, {1, 2}, "k-in-12")
    assert filtered.status is EvidenceStatus.OBSERVED_FULL
    assert filtered.values == frozenset({1, 2})
    assert repeated == filtered
    assert group_count(sql_filter(rows, lambda row: row["k"] == 1), "k") == {1: 2}


def test_duplicate_keys_and_null_do_not_change_inner_join_bag_semantics() -> None:
    left = [{"k": 1}, {"k": 1}, {"k": None}]
    right = [{"k": 1}, {"k": 1}, {"k": None}]
    joined = inner_join(left, right, "k", "k")
    assert len(joined) == 4
    assert all(row["left"]["k"] == 1 for row in joined)
    domain = equality_join_domain(observed_domain(left, "k"), observed_domain(right, "k"))
    assert domain.values == frozenset({1})


def test_outer_join_adds_null_extension_and_preserves_unmatched_left_rows() -> None:
    joined = left_outer_join([{"k": 1}, {"k": 2}], [{"k": 1, "v": "x"}], "k", "k", ["k", "v"])
    assert len(joined) == 2
    assert joined[1]["right"] == {"k": None, "v": None}


@pytest.mark.parametrize("evidence", [
    partial_domain({1, 2}, "k", .5),
    derived_domain({1, 2}, "k"),
    unknown_domain("k"),
])
def test_non_exact_evidence_cannot_shrink_a_conditional_domain(evidence) -> None:
    assert filter_domain(evidence, {1}, "new-condition").status is EvidenceStatus.UNKNOWN


def test_sparse_disjoint_domains_and_cte_reordering_fail_closed_or_remain_exact() -> None:
    left = observed_domain([{"k": 1}, {"k": 3}], "k", source="producer-a")
    right = observed_domain([{"k": 2}, {"k": 4}], "k", source="producer-b")
    assert equality_join_domain(left, right).values == frozenset()
    producer = [{"k": 3}, {"k": 1}, {"k": 3}]
    reordered = list(reversed(producer))
    assert observed_domain(producer, "k").values == observed_domain(reordered, "k").values


def test_g_stats_calibration_requires_held_out_coverage_and_exact_identities() -> None:
    gate = CalibrationGate(q_error_p95_max=2.0, q_error_max=4.0, coverage_min=1.0)
    observations = [
        # p95 is the nearest-rank value, not an interpolated value hidden by a
        # caller-provided percentile.
        NodeObservation("scan/0", "k", 10, 10, 1, held_out=False),
        NodeObservation("join/0", "k", 20, 10, 1, held_out=True),
    ]
    result = calibrate_g_stats(observations, gate)
    assert result["status"] == "passed"
    assert result["held_out_coverage_min"] == 1


def test_candidate_selection_loss_uses_actual_finite_domain_not_predicted_winner() -> None:
    candidates = [
        CandidateObservation("underestimated", predicted_cost=1, actual_cost=10, selected=True),
        CandidateObservation("safe", predicted_cost=2, actual_cost=4, selected=False),
    ]
    assert candidate_selection_loss(candidates) == 2.5
