# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from benchmark_evidence import CompileEvidenceCollector
from cold_planning import _typed_compile_measurements


class CompileCollectorTests(unittest.TestCase):
    def test_compile_document_reader_keeps_raw_and_typed_identity(self):
        class Cursor:
            def __init__(self):
                self.statement = None

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement):
                self.statement = statement

            def fetchall(self):
                return [(
                    "{\"schema_version\":4,\"outcome\":\"Success\","
                    "\"artifact\":\"CompiledArtifactReady\","
                    "\"cache\":\"ForcedCompile\","
                    "\"admission\":\"NotExecuted\","
                    "\"execution\":\"NotExecuted\","
                    "\"search_counters\":[{\"name\":\"planning_completed\",\"value\":1}],"
                    "\"omitted_search_counters\":0,"
                    "\"artifact_identity\":{\"Observed\":{\"schema_version\":4,"
                    "\"artifact\":[1,2],\"structure\":[3,4],\"dependencies\":[5,6]}}}",
                )]

        class Connection:
            def __init__(self):
                self.last_cursor = None

            def cursor(self):
                self.last_cursor = Cursor()
                return self.last_cursor

        connection = Connection()
        raw, document = CompileEvidenceCollector(connection).capture(
            "SELECT 1", detail=True
        )
        self.assertEqual(document["schema_version"], 4)
        self.assertEqual(document["outcome"], "Success")
        self.assertIn("EXPLAIN (COMPILE, DETAIL, FORMAT JSON)", connection.last_cursor.statement)
        self.assertTrue(raw.startswith("{"))
        with self.assertRaises(ValueError):
            CompileEvidenceCollector(connection).capture("SELECT 1", analyze=True)

    def test_compile_metrics_are_read_from_typed_document_not_auxiliary_receipts(self):
        document = {
            "schema_version": 4,
            "outcome": "Success",
            "artifact": "CompiledArtifactReady",
            "cache": "ForcedCompile",
            "admission": "NotExecuted",
            "execution": "NotExecuted",
            "artifact_identity": {"Observed": {"schema_version": 4,
                                                   "artifact": [1, 2],
                                                   "structure": [3, 4],
                                                   "dependencies": [5, 6]}},
            "optimizer_ns": {"Observed": 4_000_000},

            "planning_status": {"Observed": "Planned"},
            "search_counters": [
                {"name": "planning_completed", "value": 1},
                {"name": "selected_nodes", "value": 2},
                {"name": "local_alternatives", "value": 3},
                {"name": "joint_transitions", "value": 4},
                {"name": "response_join_transitions", "value": 5},
                {"name": "completed_region_outputs", "value": 6},
                {"name": "joint_budget_fallbacks", "value": 0},
                {"name": "response_join_fallbacks", "value": 0},
            ],
            "omitted_search_counters": 0,
        }
        metrics = _typed_compile_measurements(document)
        self.assertEqual(metrics["optimizer_ms"], 4.0)
        self.assertEqual(metrics["counters"]["selected_nodes"], 2)
        self.assertEqual(metrics["compile_metrics_source"],
                         "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document")

        with self.assertRaises(ValueError):
            _typed_compile_measurements({
                **document,
                "search_counters": [],
            })


if __name__ == "__main__":
    unittest.main()
