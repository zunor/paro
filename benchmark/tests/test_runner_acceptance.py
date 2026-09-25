# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Exercise the real direct-run output lifecycle, with only SQL mocked."""

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark import runner
from harness.executor import QueryExecutionResult, WorkloadExecutionResult
from harness.receipt_contract import validate_benchmark_payload
from harness.run_output import RunOutput


class DirectRunnerAcceptanceTests(unittest.TestCase):
    def run_case(self, root, *, passed, external_owner=False, workload=None):
        output = RunOutput.create(root, run_id="direct") if external_owner else None
        args = runner.BenchmarkInvocation(
            workload="one", report_root=root, run_id="direct", iterations=1,
            warmup=0, collect_compile_receipts=False, run_output=output,
        )
        config = runner.resolve_config(args)
        query = QueryExecutionResult(
            id="one", validate_mode="scalar_equals", expected=1,
            samples_ms=[1.0], validation_result="PASS" if passed else "FAIL",
        )
        workload = workload or WorkloadExecutionResult(name="one", params={}, queries=[query])
        with patch.object(runner, "load_selected_workloads", return_value=[SimpleNamespace(queries=workload.queries)]), \
             patch.object(runner.BenchmarkExecutor, "run_workload", return_value=workload), \
             patch.object(runner.BenchmarkReporter, "print_terminal_summary"):
            result = runner.execute_workloads(config, args, {})
        manifest = json.loads((root / "direct" / "manifest.json").read_text())
        return result, manifest, output

    def test_successful_owned_attempt_is_explicitly_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, manifest, _ = self.run_case(Path(tmp), passed=True)
            self.assertFalse(result.failed)
            self.assertEqual(manifest["status"], "Completed")
            self.assertEqual(manifest["registration"]["cells"][0]["accepted_attempt_id"],
                             manifest["attempts"][0]["attempt_id"])

    def test_failed_attempt_is_preserved_without_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, manifest, _ = self.run_case(Path(tmp), passed=False)
            self.assertTrue(result.failed)
            self.assertEqual(manifest["status"], "Failed")
            self.assertEqual(manifest["attempts"][0]["status"], "Failed")
            self.assertIsNone(manifest["registration"]["cells"][0]["accepted_attempt_id"])

    def test_external_run_owner_still_controls_finalization(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, manifest, output = self.run_case(Path(tmp), passed=True, external_owner=True)
            self.assertEqual(manifest["status"], "Running")
            self.assertIsNotNone(manifest["registration"]["cells"][0]["accepted_attempt_id"])
            output.finalize(status="Completed")

    def test_missing_sample_preserves_error_and_later_sample_identity(self):
        queries = [
            QueryExecutionResult(id="failed", validate_mode="scalar_equals", expected=1,
                                 validation_result="FAIL", error="OutOfMemory: fixture"),
            QueryExecutionResult(id="next", validate_mode="scalar_equals", expected=1,
                                 samples_ms=[2.0], validation_result="PASS"),
        ]
        workload = WorkloadExecutionResult(name="one", params={}, queries=queries)
        with tempfile.TemporaryDirectory() as tmp:
            result, manifest, _ = self.run_case(Path(tmp), passed=False, workload=workload)
            validate_benchmark_payload(result.payload)
            first, second = result.payload["workloads"][0]["queries"]
            ids = result.payload["ownership"]["sample_ids"]
            self.assertEqual(first["samples_ms"], [])
            self.assertEqual(first["uncollected_samples"], 1)
            self.assertEqual(first["error"], "OutOfMemory: fixture")
            self.assertEqual(first["compile_receipts"][0]["sample_id"], ids[0])
            self.assertEqual(second["compile_receipts"][0]["sample_id"], ids[1])
            self.assertEqual(second["samples_ms"], [2.0])
            self.assertEqual(manifest["status"], "Failed")

    def test_setup_failure_survives_publication_without_invented_timings(self):
        query = QueryExecutionResult(id="skipped", validate_mode="scalar_equals", expected=1,
                                     validation_result="FAIL", error="SKIPPED: setup failed")
        workload = WorkloadExecutionResult(name="one", params={}, queries=[query],
                                           setup_status="FAIL", setup_error="invalid CSV width")
        with tempfile.TemporaryDirectory() as tmp:
            result, manifest, _ = self.run_case(Path(tmp), passed=False, workload=workload)
            actual = result.payload["workloads"][0]
            self.assertEqual(actual["setup_error"], "invalid CSV width")
            self.assertEqual(actual["queries"][0]["samples_ms"], [])
            self.assertEqual(manifest["status"], "Failed")

    def test_timeout_retains_unexecuted_query_coordinates(self):
        executor = runner.BenchmarkExecutor(connection={}, iterations=1, warmup=0,
                                            timeout_seconds=1, collect_memory=False)
        queries = [SimpleNamespace(id=name, validate="scalar_equals", expected=1)
                   for name in ("first", "remaining")]
        workload = SimpleNamespace(name="one", params={}, minimum_server_buffer_pool_bytes=0,
                                   queries=queries, setup_sql="", build_sql=None, teardown_sql="")
        timeout = QueryExecutionResult(id="first", validate_mode="scalar_equals", expected=1,
                                       validation_result="FAIL", error="TIMEOUT: fixture")
        with patch.object(executor, "connection_factory"), \
             patch.object(executor, "_execute_script"), \
             patch.object(executor, "_run_query", return_value=timeout), \
             patch.object(executor, "_apply_relative_latency_guards"):
            result = executor.run_workload(workload, None)
        self.assertEqual([q.id for q in result.queries], ["first", "remaining"])
        self.assertEqual(result.queries[1].samples_ms, [])
        self.assertTrue(result.queries[1].error.startswith("SKIPPED:"))
