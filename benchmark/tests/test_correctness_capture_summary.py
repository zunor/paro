# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


path = Path(__file__).resolve().parents[1] / "tools/summarize_correctness_captures.py"
spec = importlib.util.spec_from_file_location("capture_summary", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CaptureSummaryTests(unittest.TestCase):
    def summarize(self, data):
        with tempfile.TemporaryDirectory() as directory:
            capture = Path(directory) / "capture.json"
            capture.write_text(json.dumps(data))
            return module.summarize(capture)

    def test_empty_capture_is_not_a_pass(self):
        self.assertEqual(self.summarize({})["verdict"], "uncovered_no_result")

    def test_error_after_a_result_is_not_a_pass(self):
        result = {"ordinal": 0, "actual": [], "expected": [], "oracle_status": "exact_match"}
        data = {"result_sets": [result], "execution_error": "cancelled", "sqlstate": "57014"}
        summary = self.summarize(data)
        self.assertEqual(summary["verdict"], "execution_error")
        self.assertEqual(summary["sqlstate"], "57014")

    def test_difference_needs_separate_adjudication(self):
        result = {"ordinal": 0, "actual": [], "expected": [], "oracle_status": "mismatch"}
        self.assertEqual(self.summarize({"result_sets": [result]})["verdict"], "unresolved_difference")
