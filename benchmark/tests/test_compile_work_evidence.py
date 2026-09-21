import json
import types
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))

from benchmark.corpora.tpcds_compare import collect_statement_cache_evidence, statement_fingerprint


class Cursor:
    description = [types.SimpleNamespace(name=name) for name in
                   ("name", "kind", "last_elapsed_us", "metric_value", "metric_unit",
                    "invocation_count", "record_type", "record_id", "payload_json")]

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


def typed_row(record_type, record_id, payload):
    return (
        record_type,
        "receipt",
        0,
        0,
        "receipt",
        1,
        record_type,
        record_id,
        json.dumps(payload),
    )


class CompileWorkEvidenceTest(unittest.TestCase):
    def test_only_exact_cache_occurrence_is_attached(self):
        query = "SELECT 1"
        fp = statement_fingerprint(query)
        identity = {"schema_version": 1, "artifact": [1, 2],
                    "structure": [3, 4], "dependencies": [5, 6]}
        rows = [
            typed_row("statement_cache", 11, {
                "schema_version": 1, "decision_id": 11, "query_fingerprint": fp,
                "occurrence": 9, "cache_hit": False, "artifact_identity": identity,
                "compile_work": {"optimizer_elapsed_us": 17},
            }),
            typed_row("execution_receipt", 12, {
                "schema_version": 1, "execution_id": 12, "statement_decision_id": 11,
                "artifact_identity": identity,
            }),
            typed_row("statement_cache", 13, {
                "schema_version": 1, "decision_id": 13, "query_fingerprint": fp,
                "occurrence": 10, "cache_hit": True, "artifact_identity": identity,
                "compile_work": None,
            }),
            typed_row("execution_receipt", 14, {
                "schema_version": 1, "execution_id": 14, "statement_decision_id": 13,
                "artifact_identity": identity,
            }),
        ]
        connection = types.SimpleNamespace(cursor=lambda: Cursor(rows))
        evidence = collect_statement_cache_evidence(connection, query, before_execution_ids={14})
        self.assertEqual(evidence["status"], "Verified")
        self.assertEqual(evidence["compile_work"], {"optimizer_elapsed_us": 17})
        self.assertEqual(evidence["statement_decision_id"], 11)
        self.assertEqual(evidence["execution_id"], 12)

    def test_disabled_collection_is_missing_not_zero(self):
        query = "SELECT 1"
        fp = statement_fingerprint(query)
        rows = [typed_row("statement_cache", 11, {
            "schema_version": 1, "decision_id": 11, "query_fingerprint": fp,
            "occurrence": 0, "cache_hit": False, "artifact_identity": None,
            "compile_work": None,
        })]
        connection = types.SimpleNamespace(cursor=lambda: Cursor(rows))
        evidence = collect_statement_cache_evidence(connection, query, before_execution_ids=set())
        self.assertEqual(evidence["status"], "Uncovered")
