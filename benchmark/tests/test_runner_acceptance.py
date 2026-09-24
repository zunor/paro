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
from harness.run_output import RunOutput


class DirectRunnerAcceptanceTests(unittest.TestCase):
    def run_case(self, root, *, passed, external_owner=False):
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
        workload = WorkloadExecutionResult(name="one", params={}, queries=[query])
        with patch.object(runner, "load_selected_workloads", return_value=[SimpleNamespace(queries=[None])]), \
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
