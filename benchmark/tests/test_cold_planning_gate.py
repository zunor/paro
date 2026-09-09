# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import math
import copy
import unittest

from harness.cold_planning_gate import COUNTERS, evaluate


def report():
    def trace_for(block, *, statement_id=None, events=None):
        statement_id = block if statement_id is None else statement_id
        if events is None:
            events = [
                ("frontend", "parse_entry"),
                ("compile", "plan_cache_miss"),
                ("compile", "compiler_call_entry"),
                ("compile", "compiler_call_return"),
                ("lifecycle", "statement_scope_begin"),
                ("lifecycle", "statement_scope_return"),
                ("lifecycle", "statement_complete"),
            ]
        return {
            "process_id": 100 + block,
            "session_id": 200 + block,
            "operation_id": statement_id,
            "trace_sample_id": f"sample-{block}",
            "schema_version": 2,
            "statement_id": statement_id,
            "statement_index": 0,
            "query_len": 5,
            "query_fingerprint": 123,
            "events": [
                {"sequence": index, "phase": phase, "event": event,
                 "elapsed_us": index + 1}
                for index, (phase, event) in enumerate(events)
            ],
        }

    return {
        "schema_version": 5,
        "configuration": {
            "process_blocks": 3,
            "runtime_environment": {"RUST_LOG": None, "PARO_STATEMENT_TRACE": "1"},
            "cohort": "diagnostic",
            "trace_mode": "on",
        },
        "evidence": {"build": {"binary_sha256": "binary", "source": {"commit": "commit",
                      "working_tree_sha256": "source"}}, "harness_sha256": "harness",
                     "dataset_sha256": "data", "dataset_path": "/seed", "machine": "machine"},
        "queries": [{"name": "q11", "sql_sha256": "original-sum-of-difference", "samples": [
            {"block": block, "status": "ok", "server": {
                "pid": 100 + block, "sha256": "binary", "data_dir": f"/snapshot/{block}",
                "statement_trace": True, "statement_trace_sample_id": f"sample-{block}",
                "input_snapshot": {"policy": "private_copy_per_process", "seed_path": "/seed",
                                   "seed_sha256": "data", "initial_sha256": "data"}},
             "explain_wall_ms": 20, "optimizer_ms": 15, "peak_rss_bytes": 1000, "plan_sha256": "plan",
             "counters": {counter: 0 if counter in ("search_rule_failure_count", "search_deadline_reached") else 1
                          for counter in COUNTERS},
             "phase_trace_schema_version": 2,
             "trace_query_fingerprint": 123,
             "statement_traces": [trace_for(block)],
             "target_statement_traces": [trace_for(block)]}
            for block in range(3)]}],
    }


class ColdPlanningGateTests(unittest.TestCase):
    def test_reused_unverified_and_in_place_databases_are_not_fresh_samples(self):
        for mutation in (
            lambda s: s.pop("input_snapshot"),
            lambda s: s["input_snapshot"].update(initial_sha256="different"),
            lambda s: s["input_snapshot"].update(seed_path="/unrelated"),
            lambda s: s.update(data_dir="/seed"),
            lambda s: s.update(data_dir="/snapshot/1"),
        ):
            with self.subTest(mutation=mutation):
                current = report()
                mutation(current["queries"][0]["samples"][0]["server"])
                with self.assertRaises(ValueError):
                    evaluate(current)

    def test_deadline_and_logging_changes_cannot_claim_a_speedup(self):
        current = report()
        current["configuration"]["runtime_environment"]["RUST_LOG"] = "debug"
        with self.assertRaises(ValueError):
            evaluate(current, report())

    def test_cold_trace_rejects_cache_hit_negative_time_and_cross_trace_splice(self):
        mutations = []

        def cache_hit(current):
            current["queries"][0]["samples"][0]["target_statement_traces"][0]["events"][1][
                "event"
            ] = "plan_cache_hit"

        mutations.append(cache_hit)

        def negative_time(current):
            current["queries"][0]["samples"][0]["target_statement_traces"][0]["events"][2][
                "elapsed_us"
            ] = -1

        mutations.append(negative_time)

        def cross_trace_splice(current):
            sample = current["queries"][0]["samples"][0]
            target = sample["target_statement_traces"][0]
            target["events"] = [
                event for event in target["events"] if event["event"] != "compiler_call_return"
            ]
            other = copy.deepcopy(target)
            other["statement_id"] = 99
            other["operation_id"] = 99
            other["session_id"] = 299
            other["events"] = [
                {**event, "sequence": index}
                for index, event in enumerate([
                    {"phase": "compile", "event": "compiler_call_return", "elapsed_us": 3},
                    {"phase": "lifecycle", "event": "statement_complete", "elapsed_us": 4},
                ])
            ]
            sample["statement_traces"].append(other)

        mutations.append(cross_trace_splice)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                current = report()
                mutation(current)
                with self.assertRaises(ValueError):
                    evaluate(current)
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
