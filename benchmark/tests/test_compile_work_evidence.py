import types
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))

from benchmark.corpora.tpcds_compare import collect_statement_cache_evidence, statement_fingerprint


class Cursor:
    description = [types.SimpleNamespace(name=name) for name in
                   ("name", "kind", "metric_value", "metric_unit")]

    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query):
        assert query == "SELECT * FROM paro_optimizers()"

    def fetchall(self):
        return self.rows


class CompileWorkEvidenceTest(unittest.TestCase):
    def test_only_exact_cache_occurrence_is_attached(self):
        query = "SELECT 1"
        fp = statement_fingerprint(query)
        rows = [(f"statement_plan_cache/{fp:016x}/0", "evidence", 0, "count"),
                (f"statement_compile_work/{fp:016x}/0/optimizer_elapsed_us", "evidence", 17, "microseconds"),
                (f"statement_compile_work/{fp:016x}/1/optimizer_elapsed_us", "evidence", 999, "microseconds"),
                ("statement_compile_work/0000000000000000/0/compiler_elapsed_us", "evidence", 888, "microseconds")]
        connection = types.SimpleNamespace(cursor=lambda: Cursor(rows))
        evidence = collect_statement_cache_evidence(connection, query)
        self.assertEqual(evidence["status"], "verified")
        self.assertEqual(evidence["compile_work"], {"optimizer_elapsed_us": 17})

    def test_disabled_collection_is_missing_not_zero(self):
        query = "SELECT 1"
        fp = statement_fingerprint(query)
        rows = [(f"statement_plan_cache/{fp:016x}/0", "evidence", 0, "count")]
        connection = types.SimpleNamespace(cursor=lambda: Cursor(rows))
        self.assertEqual(collect_statement_cache_evidence(connection, query)["compile_work"], {})
