# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from benchmark_evidence import fetch_compile_document
from cold_planning import diagnostic_rows


class DiagnosticRowsTests(unittest.TestCase):
    def test_qualified_pgwire_names_and_typed_values(self):
        columns = ["name", "kind", "last_elapsed_us", "metric_value", "metric_unit", "invocation_count"]
        values = [("memo_exploration", "search", 200, 1, "invocations", 1)]
        self.assertEqual(
            diagnostic_rows(columns, values),
            diagnostic_rows(["paro_optimizers." + name for name in columns], values),
        )
        self.assertEqual(diagnostic_rows(columns, values)[0]["last_elapsed_us"], 200)

    def test_schema_and_row_arity_fail_closed(self):
        with self.assertRaises(ValueError):
            diagnostic_rows(["name", "name"], [(1, 2)])
        with self.assertRaises(ValueError):
            diagnostic_rows(
                ["name", "kind", "last_elapsed_us", "metric_value", "metric_unit", "invocation_count"],
                [(1,)],
            )

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
                return [("{\"schema_version\":2,\"outcome\":\"Success\"}",)]

        class Connection:
            def __init__(self):
                self.last_cursor = None

            def cursor(self):
                self.last_cursor = Cursor()
                return self.last_cursor

        connection = Connection()
        raw, document = fetch_compile_document(connection, "SELECT 1", detail=True)
        self.assertEqual(document["schema_version"], 2)
        self.assertEqual(document["outcome"], "Success")
        self.assertIn("EXPLAIN (COMPILE, DETAIL, FORMAT JSON)", connection.last_cursor.statement)
        self.assertTrue(raw.startswith("{"))


if __name__ == "__main__":
    unittest.main()
