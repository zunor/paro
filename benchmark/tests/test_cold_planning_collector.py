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
                    "{\"schema_version\":3,\"outcome\":\"Success\","
                    "\"artifact\":\"CompiledArtifactReady\","
                    "\"cache\":\"ForcedCompile\","
                    "\"admission\":\"NotExecuted\","
                    "\"execution\":\"NotExecuted\","
                    "\"search_counters\":[{\"name\":\"search_complete\",\"value\":1}],"
                    "\"omitted_search_counters\":0,"
                    "\"artifact_identity\":{\"Observed\":{\"schema_version\":3,"
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
        self.assertEqual(document["schema_version"], 3)
        self.assertEqual(document["outcome"], "Success")
        self.assertIn("EXPLAIN (COMPILE, DETAIL, FORMAT JSON)", connection.last_cursor.statement)
        self.assertTrue(raw.startswith("{"))
        with self.assertRaises(ValueError):
            CompileEvidenceCollector(connection).capture("SELECT 1", analyze=True)

    def test_compile_metrics_are_read_from_typed_document_not_auxiliary_receipts(self):
        document = {
            "schema_version": 3,
            "outcome": "Success",
            "artifact": "CompiledArtifactReady",
            "cache": "ForcedCompile",
            "admission": "NotExecuted",
            "execution": "NotExecuted",
            "artifact_identity": {"Observed": {"schema_version": 3,
                                                   "artifact": [1, 2],
                                                   "structure": [3, 4],
                                                   "dependencies": [5, 6]}},
            "optimizer_ns": {"Observed": 4_000_000},
            "search_complete": {"Observed": True},
            "search_stop": {"Observed": "Complete"},
            "search_counters": [
                {"name": "search_complete", "value": 1},
                {"name": "memo_group_count", "value": 2},
                {"name": "memo_logical_expression_count", "value": 3},
                {"name": "memo_physical_expression_count", "value": 4},
                {"name": "settlement_local_hit_count", "value": 5},
                {"name": "settlement_local_miss_count", "value": 6},
                {"name": "search_rule_failure_count", "value": 0},
                {"name": "search_deadline_reached", "value": 0},
            ],
            "omitted_search_counters": 0,
        }
        metrics = _typed_compile_measurements(document)
        self.assertEqual(metrics["optimizer_ms"], 4.0)
        self.assertEqual(metrics["counters"]["memo_group_count"], 2)
        self.assertEqual(metrics["compile_metrics_source"],
                         "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document")

        with self.assertRaises(ValueError):
            _typed_compile_measurements({
                **document,
                "search_counters": [],
            })


if __name__ == "__main__":
    unittest.main()
