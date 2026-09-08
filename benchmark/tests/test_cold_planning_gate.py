# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import math
import unittest

from harness.cold_planning_gate import COUNTERS, evaluate


def report():
    return {
        "schema_version": 2, "configuration": {"process_blocks": 3, "runtime_environment": {"RUST_LOG": None}},
        "evidence": {"build": {"binary_sha256": "binary", "source": {"commit": "commit",
                      "working_tree_sha256": "source"}}, "harness_sha256": "harness",
                     "dataset_sha256": "data", "machine": "machine"},
        "queries": [{"name": "q11", "sql_sha256": "original-sum-of-difference", "samples": [
            {"block": block, "status": "ok", "server": {"pid": 100 + block, "sha256": "binary"},
             "explain_wall_ms": 20, "optimizer_ms": 15, "peak_rss_bytes": 1000, "plan_sha256": "plan",
             "counters": {counter: 0 if counter in ("search_rule_failure_count", "search_deadline_reached") else 1
                          for counter in COUNTERS}} for block in range(3)]}],
    }


class ColdPlanningGateTests(unittest.TestCase):
    def test_deadline_and_logging_changes_cannot_claim_a_speedup(self):
        current = report()
        current["configuration"]["runtime_environment"]["RUST_LOG"] = "debug"
        with self.assertRaises(ValueError):
            evaluate(current, report())
        current = report()
        current["queries"][0]["samples"][0]["counters"]["search_deadline_reached"] = 1
        with self.assertRaises(ValueError):
            evaluate(current, report())

    def test_same_report_passes(self):
        self.assertTrue(evaluate(report(), report())["passed"])

    def test_missing_failed_duplicate_and_nonfinite_samples_fail_closed(self):
        for mutation in (
            lambda r: r["queries"][0]["samples"].pop(),
            lambda r: r["queries"][0]["samples"][0].update(status="error"),
            lambda r: r["queries"][0]["samples"][0].update(block=1),
            lambda r: r["queries"][0]["samples"][0].update(optimizer_ms=math.nan),
            lambda r: r["queries"][0]["samples"][0]["counters"].pop("search_complete"),
            lambda r: r.update(invalidated="source changed"),
        ):
            with self.subTest(mutation=mutation):
                current = report()
                mutation(current)
                with self.assertRaises((ValueError, KeyError)):
                    evaluate(current, report())

    def test_rewrite_or_missing_query_cannot_pass(self):
        for field, value in (("name", "q12"), ("sql_sha256", "sum-x-minus-sum-y")):
            current = report()
            current["queries"][0][field] = value
            with self.assertRaises(ValueError):
                evaluate(current, report())

    def test_faster_search_may_not_hide_lost_closure_or_new_omissions(self):
        current = report()
        for sample in current["queries"][0]["samples"]:
            sample.update(optimizer_ms=1, explain_wall_ms=2)
            sample["counters"].update(search_complete=0, budget_exhaustion_deadline=1)
        self.assertFalse(evaluate(current, report())["passed"])

    def test_time_and_memory_are_both_gated(self):
        for metric in ("optimizer_ms", "explain_wall_ms", "peak_rss_bytes"):
            current = report()
            for sample in current["queries"][0]["samples"]:
                sample[metric] *= 1.25
            self.assertFalse(evaluate(current, report())["passed"])

    def test_new_binary_is_expected_but_mixed_binary_is_not(self):
        current = report()
        current["evidence"]["build"]["binary_sha256"] = "new-binary"
        with self.assertRaises(ValueError):
            evaluate(current, report())
        for sample in current["queries"][0]["samples"]:
            sample["server"]["sha256"] = "new-binary"
        self.assertTrue(evaluate(current, report())["passed"])


if __name__ == "__main__":
    unittest.main()
