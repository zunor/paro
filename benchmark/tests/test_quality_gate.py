# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import math
import unittest

from harness.quality_gate import evaluate, q_error, summarize
from corpora.plan_quality import capture, plan_metrics, select_boundary


def report(*estimates):
    return {
        "schema_version": 1,
        "contract": {"corpus_sha256": "corpus", "collector_sha256": "collector",
                     "settings": {"threads": 4},
                     "cases": [{"query": f"q{index}", "sql_sha256": str(index), "expected_rows": 10, "expected_result_rows": 10,
                                "nonincreasing_metrics": ["aggregate_nodes"]}
                               for index, _ in enumerate(estimates)]},
        "evidence": {"build": {"binary_sha256": "binary"}},
        "observations": [{"query": f"q{index}", "sql_sha256": str(index), "status": "ok",
                          "operator": "AGGREGATE", "estimated_rows": value, "actual_rows": 10, "result_rows": 10,
                          "plan": {"plan": {}}, "plan_metrics": {"aggregate_nodes": 2}}
                         for index, value in enumerate(estimates)],
    }


class QualityGateTests(unittest.TestCase):
    def test_symmetric_q_error(self):
        self.assertEqual(q_error(10, 10), 1.0)
        self.assertEqual(q_error(100, 10), q_error(10, 100))
        self.assertTrue(math.isinf(q_error(0, 1)))
        self.assertEqual(q_error(0, 0), 1.0)

    def test_invalid_numeric_values_are_not_quality_evidence(self):
        for value in (-1, math.nan, math.inf, -math.inf, True, "10", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                q_error(value, 10)

    def test_no_query_regression_can_hide_in_the_distribution_tail(self):
        baseline = report(1000, 10)
        result = evaluate(report(500, 20), baseline)
        self.assertFalse(result["passed"])
        self.assertIn("q1: q-error 1 -> 2", result["regressions"])
        self.assertIn("q0: q-error 100 -> 50", result["improvements"])

    def test_summary_is_deterministic_and_infinite_tail_is_strict_json(self):
        self.assertEqual(summarize([1.0, 2.0, 4.0])["q95"], 4.0)
        self.assertIn('"max": "inf"', json.dumps(summarize([1.0, math.inf]), allow_nan=False))

    def test_missing_duplicate_or_failed_observation_is_rejected(self):
        for mutate in (
            lambda value: value["observations"].pop(),
            lambda value: value["observations"].append(value["observations"][0]),
            lambda value: value["observations"][0].update(status="error"),
            lambda value: value["observations"][0].update(actual_rows=9),
            lambda value: value["observations"][0].update(sql_sha256="changed"),
            lambda value: value["observations"][0].pop("plan_metrics"),
            lambda value: value.update(invalidated="changed source"),
            lambda value: value.pop("evidence"),
        ):
            candidate = report(10, 20)
            mutate(candidate)
            with self.subTest(candidate=candidate), self.assertRaises((ValueError, KeyError)):
                evaluate(candidate, report(10, 20))

    def test_contract_changes_require_explicit_new_baseline(self):
        for key in ("corpus_sha256", "collector_sha256", "settings"):
            candidate = report(10)
            candidate["contract"][key] = "different"
            with self.subTest(key=key), self.assertRaises(ValueError):
                evaluate(candidate, report(10))

    def test_source_revision_may_change_but_missing_baseline_data_cannot_pass(self):
        baseline = report(10)
        candidate = copy.deepcopy(baseline)
        candidate["evidence"]["build"]["binary_sha256"] = "new binary"
        self.assertTrue(evaluate(candidate, baseline)["passed"])
        with self.assertRaises(ValueError):
            evaluate(candidate, {"summary": {"max": 100}})

    def test_extra_aggregate_phase_is_independent_of_cardinality_improvements(self):
        baseline = report(100)
        candidate = report(10)
        candidate["observations"][0]["plan_metrics"]["aggregate_nodes"] = 3
        result = evaluate(candidate, baseline)
        self.assertFalse(result["passed"])
        self.assertIn("q0: aggregate_nodes 2 -> 3", result["regressions"])
        candidate["observations"][0]["plan_metrics"]["aggregate_nodes"] = 1
        result = evaluate(candidate, baseline)
        self.assertTrue(result["passed"])
        self.assertIn("q0: aggregate_nodes 2 -> 1", result["improvements"])

    def test_plan_metrics_use_structural_children_not_runtime_pipeline_positions(self):
        tree = {"operator": "AGGREGATE", "children": [
            {"operator": "HASH_JOIN", "children": [
                {"operator": "AGGREGATE"}, {"operator": "ROWSET_SCAN"}]}]}
        self.assertEqual(plan_metrics(tree)["aggregate_nodes"], 2)

    def test_capture_checks_independent_oracle_and_missing_estimate(self):
        class Cursor:
            def fetchone(self):
                return [json.dumps({"format_version": 2,
                                    "plan": {"node_id": 1, "operator": "FILTER", "estimated_rows": 4}})]

            def fetchall(self):
                return [(1,), (2,)]

        class Connection:
            def execute(self, *args, **kwargs):
                return Cursor()

        case = {"query": "example", "sql": "SELECT 1", "expected_rows": 2}
        self.assertEqual(capture(Connection(), case)["actual_rows"], 2)
        case["expected_rows"] = 3
        with self.assertRaises(ValueError):
            capture(Connection(), case)

    def test_missing_or_ambiguous_internal_boundaries_fail_closed(self):
        root = {"operator": "PROJECTION", "children": [{"operator": "ROWSET_SCAN"}]}
        self.assertEqual(select_boundary(root, {"operator": "ROWSET_SCAN"})["operator"], "ROWSET_SCAN")
        with self.assertRaises(ValueError):
            select_boundary(root, {"operator": "FILTER"})
        root["children"].append({"operator": "ROWSET_SCAN"})
        with self.assertRaises(ValueError):
            select_boundary(root, {"operator": "ROWSET_SCAN"})

    def test_having_boundary_survives_fusion_without_selecting_pre_filter_rows(self):
        predicate = "sum(quantity) > 100"
        aggregate = {"operator": "AGGREGATE", "properties": {"Group Key": "key"}}
        selector = {"alternatives": [
            {"operator": "AGGREGATE", "properties": {"Group Key": "key", "Having": predicate}},
            {"operator": "FILTER", "properties": {"Filter": predicate},
             "child": {"operator": "AGGREGATE", "properties": {"Group Key": "key"}}},
        ]}
        with self.assertRaises(ValueError):
            select_boundary(aggregate, selector)
        standalone = {"operator": "FILTER", "properties": {"Filter": predicate}, "children": [aggregate]}
        self.assertIs(select_boundary(standalone, selector), standalone)
        fused = {"operator": "AGGREGATE", "properties": {"Group Key": "key", "Having": predicate}}
        self.assertIs(select_boundary(fused, selector), fused)
        with self.assertRaises(ValueError):
            select_boundary({"operator": "UNION", "children": [standalone, fused]}, selector)
        wrong_input = {**standalone, "children": [{"operator": "ROWSET_SCAN"}]}
        with self.assertRaises(ValueError):
            select_boundary(wrong_input, selector)

    def test_selector_schema_fails_closed_instead_of_ignoring_misspellings(self):
        for selector in ({"alternatives": []}, {"operator": "FILTER", "property": {}},
                         {"operator": "FILTER", "child": {"operatr": "AGGREGATE"}},
                         {"operator": "FILTER", "alternatives": [{"operator": "FILTER"}]}):
            with self.subTest(selector=selector), self.assertRaises(ValueError):
                select_boundary({"operator": "FILTER"}, selector)


if __name__ == "__main__":
    unittest.main()
