# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import math
from pathlib import Path
import tempfile
import unittest

from harness.cold_planning_gate import COUNTERS, evaluate, load_run_output_report
from harness.receipt_contract import build_benchmark_cell_payload, uncovered_receipt
from harness.run_output import CampaignOutput


def report():
    return {
        "schema_version": 3,
        "compile_evidence_schema_version": 3,
        "configuration": {
            "process_blocks": 3,
            "runtime_environment": {"RUST_LOG": None, "PARO_STATEMENT_TRACE": "0"},
            "cohort": "diagnostic",
            "trace_mode": "off",
            "compile_document": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)",
            "compile_metrics_source": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document",
        },
        "evidence": {"build": {"binary_sha256": "binary", "source": {"commit": "commit",
                      "working_tree_sha256": "source"}}, "harness_sha256": "harness",
                     "dataset_sha256": "data", "dataset_path": "/seed", "machine": "machine"},
        "queries": [{"name": "q11", "sql_sha256": "original-sum-of-difference", "samples": [
            {"block": block, "status": "ok", "server": {
                "pid": 100 + block, "sha256": "binary", "data_dir": f"/snapshot/{block}",
                "input_snapshot": {"policy": "private_copy_per_process", "seed_path": "/seed",
                                   "seed_sha256": "data", "initial_sha256": "data"}},
             "explain_wall_ms": 20, "optimizer_ms": 15, "peak_rss_bytes": 1000, "plan_structure_id": "00112233445566778899aabbccddeeff",
             "compile_metrics_source": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document",
             "counters": {counter: 0 if counter in ("search_rule_failure_count", "search_deadline_reached") else 1
                          for counter in COUNTERS},
             "compile_query_fingerprint": 123,
             "compile_document": {
                 "schema_version": 3,
                 "outcome": "Success",
                 "artifact": "CompiledArtifactReady",
                 "cache": "ForcedCompile",
                 "admission": "NotExecuted",
                 "execution": "NotExecuted",
                 "artifact_identity": {"Observed": {"schema_version": 3, "artifact": [1, 2], "structure": [3, 4], "dependencies": [5, 6]}},
                 "search_counters": [
                     {"name": "memo_group_count", "value": 1},
                     {"name": "memo_logical_expression_count", "value": 1},
                     {"name": "memo_physical_expression_count", "value": 1},
                     {"name": "search_complete", "value": 1},
                     {"name": "search_rule_failure_count", "value": 0},
                     {"name": "search_deadline_reached", "value": 0},
                     {"name": "settlement_local_hit_count", "value": 0},
                     {"name": "settlement_local_miss_count", "value": 0},
                 ],
                 "omitted_search_counters": 0,
             }}
            for block in range(3)]}],
    }


class ColdPlanningGateTests(unittest.TestCase):
    def test_gate_reads_sealed_runoutput_cell_instead_of_free_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = CampaignOutput.create(
                Path(directory) / "cold-cell",
                source_id="cold",
                cells=[{
                    "query_case": "q11",
                    "arm_id": "diagnostic",
                    "query_cases": 1,
                    "sample_rows": 3,
                    "product_receipts": 3,
                    "summary_captures": 0,
                }],
            )
            attempt = output.attempts[("q11", "diagnostic")]
            source = report()
            payload = build_benchmark_cell_payload(
                campaign_id=output.run.campaign_id,
                run_id=output.run.run_id,
                query_case="q11",
                arm_id="diagnostic",
                workload_name="cold_planning",
                query_payload={
                    "schema_version": source["schema_version"],
                    "configuration": source["configuration"],
                    "evidence": source["evidence"],
                    "query": source["queries"][0],
                },
                compile_receipts=[
                    uncovered_receipt("diagnostic compile is not execution")
                    for _ in range(3)
                ],
                source_id=attempt.source_id,
                attempt_id=attempt.attempt_id,
            )
            output.publish_cell_json(
                query_case="q11", arm_id="diagnostic", payload=payload
            )
            output.publish_campaign_summary()
            output.finish(status="Completed")
            self.assertEqual(
                json.loads((output.run.root / "campaign.json").read_text())["status"],
                "Completed",
            )
            loaded, root = load_run_output_report(output.run.root)
            self.assertTrue(evaluate(loaded, report_root=root)["passed"])
            result_path = next(output.run.root.rglob("result.json"))
            stored = json.loads(result_path.read_text())
            stored["query"]["schema_version"] = 5
            result_path.write_text(json.dumps(stored))
            with self.assertRaises(ValueError):
                load_run_output_report(output.run.root)

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

    def test_compile_document_identity_and_terminal_contract_fail_closed(self):
        mutations = [
            lambda sample: sample["compile_document"].pop("execution"),
            lambda sample: sample["compile_document"].update(schema_version=99),
            lambda sample: sample.pop("compile_query_fingerprint"),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                current = report()
                mutation(current["queries"][0]["samples"][0])
                with self.assertRaises(ValueError):
                    evaluate(current)
        current = report()
        current["queries"][0]["samples"][0]["counters"]["search_deadline_reached"] = 1
        with self.assertRaises(ValueError):
            evaluate(current, report())

    def test_same_report_passes(self):
        self.assertTrue(evaluate(report(), report())["passed"])

    def test_captured_compile_document_is_resolved_and_hash_checked(self):
        current = report()
        sample = current["queries"][0]["samples"][0]
        document = sample.pop("compile_document")
        raw = (json.dumps(document, sort_keys=True) + "\n").encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manifest.json").write_text("{}\n", encoding="utf-8")
            capture = root / "captures" / "block-0000.json"
            capture.parent.mkdir()
            capture.write_bytes(raw)
            sample["compile_document"] = {
                "status": "Captured",
                "path": "captures/block-0000.json",
                "sha256": hashlib.sha256(raw).hexdigest(),
                "schema_version": document["schema_version"],
            }
            self.assertTrue(evaluate(current, report_root=root)["passed"])
            capture.write_bytes(raw + b"changed")
            with self.assertRaises(ValueError):
                evaluate(current, report_root=root)

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
