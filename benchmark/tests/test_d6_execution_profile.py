# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the diagnostic-only D6 execution collector."""

from __future__ import annotations

import unittest

from benchmark.corpora.d6_execution_profile import _pipeline_summary, _strip_sql


class D6ExecutionProfileTests(unittest.TestCase):
    def test_strip_sql_removes_only_trailing_semicolons(self) -> None:
        self.assertEqual(_strip_sql("SELECT ';'; ;;"), "SELECT ';'")
        with self.assertRaises(ValueError):
            _strip_sql(" ; ; ")

    def test_pipeline_summary_keeps_runtime_and_logical_coordinates(self) -> None:
        summary = _pipeline_summary(
            [
                {
                    "tree_path": "3/source/7",
                    "operator": "ROWSET_SCAN",
                    "node_id": 7,
                    "logical_node_id": 42,
                    "rows": 100,
                    "total_time_ms": 5.5,
                    "loops": 2,
                    "reported_memory_bytes": None,
                    "startup_time_ms": 1.0,
                    "scheduler_ready_time_us": 2,
                    "scheduler_wait_time_us": 0,
                    "runtime_filter_installed_count": 0,
                    "aggregate_hash_max_radix_partition_skew_percent": None,
                }
            ]
        )
        self.assertEqual(summary[0]["pipeline"], 3)
        self.assertEqual(summary[0]["max_operator_time_ms"], 5.5)
        self.assertEqual(summary[0]["operators"][0]["logical_node_id"], 42)


if __name__ == "__main__":
    unittest.main()
