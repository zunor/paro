#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import json
import unittest
from pathlib import Path

from benchmark.corpora.first_statement_attribution import (
    _component_times,
    _execution_profile_compatibility,
    _plan_coordinates,
)


class FirstStatementAttributionTest(unittest.TestCase):
    def test_component_times_are_read_once_per_component(self) -> None:
        diagnostics = [
            {"name": "memo_exploration", "last_elapsed_us": 1250},
            {"name": "physical_subproblem_requests", "last_elapsed_us": 9999},
        ]
        self.assertEqual(_component_times(diagnostics), {"memo_exploration": 1.25})

    def test_plan_coordinates_join_runtime_only_by_logical_node_id(self) -> None:
        plan = {
            "format_version": 2,
            "plan": {
                "node_id": 1,
                "logical_node_id": 7,
                "operator": "AGGREGATE",
                "estimated_rows": 8,
                "properties": {"Group Key": "ss_customer_sk, d_year"},
                "children": [
                    {
                        "node_id": 2,
                        "logical_node_id": 8,
                        "operator": "ROWSET_SCAN",
                        "relation": "public.date_dim",
                        "estimated_rows": 10,
                        "properties": {
                            "Pushed Predicate": "d_year = 2001 OR d_year = 2002",
                            "Column IDs": ["0", "6"],
                        },
                        "children": [],
                    }
                ],
            },
        }
        runtime = {
            "operators": [
                {"logical_node_id": 7, "node_id": 30, "rows": 4, "total_time_ms": 2.0},
                # A physical id without lineage must not be used as a fallback.
                {"node_id": 2, "rows": 999, "total_time_ms": 99.0},
            ]
        }

        coordinates = _plan_coordinates(json.dumps(plan), runtime)

        self.assertEqual(coordinates["node_count"], 2)
        self.assertTrue(coordinates["logical_node_id_unique"])
        self.assertEqual(
            coordinates["occurrences"][0]["runtime"]["status"],
            "matched",
        )
        self.assertEqual(coordinates["occurrences"][0]["q_error"], 2.0)
        self.assertEqual(
            coordinates["occurrences"][1]["runtime"]["status"],
            "uncovered",
        )
        quality = coordinates["quality_signals"]
        self.assertEqual(quality["narrow_partial_aggregate_count"], 1)
        self.assertEqual(quality["date_scan_with_year_pushdown"], 1)
        self.assertEqual(quality["status"], "incomplete")

    def test_execution_profile_requires_sql_data_and_binary_identity(self) -> None:
        report = {
            "evidence": {
                "dataset_sha256": "data",
                "build": {"binary_sha256": "binary"},
            }
        }
        query = {"sql_sha256": "sql"}
        profile = {
            "query_sha256": "sql",
            "seed": {"sha256": "data"},
            "binary": {"binary_sha256": "different"},
        }

        compatibility = _execution_profile_compatibility(report, query, profile)

        self.assertFalse(compatibility["accepted"])
        self.assertEqual(
            compatibility["checks"],
            {"sql_sha256": True, "dataset_sha256": True, "binary_sha256": False},
        )
        self.assertIn("typed EXPLAIN (COMPILE", compatibility["statement_boundary"])


if __name__ == "__main__":
    unittest.main()
