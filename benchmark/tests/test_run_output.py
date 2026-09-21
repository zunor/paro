import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.receipt_contract import ReceiptContractError, validate_benchmark_payload  # noqa: E402
from harness.run_output import RunOutput, RunOutputError  # noqa: E402
from harness.executor import BenchmarkExecutor  # noqa: E402


class RunOutputTests(unittest.TestCase):
    def test_receipt_collector_ignores_its_own_introspection_execution(self) -> None:
        columns = ["name", "kind", "metric_value", "metric_unit"]

        class Column:
            def __init__(self, name: str) -> None:
                self.name = name

        class Cursor:
            description = [Column(name) for name in columns]

            def __init__(self, rows: list[tuple[object, ...]]) -> None:
                self.rows = rows

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement: str) -> None:
                self.statement = statement

            def fetchall(self):
                return self.rows

        class Connection:
            def __init__(self, rows: list[tuple[object, ...]]) -> None:
                self.rows = rows

            def cursor(self):
                return Cursor(self.rows)

        def row(name: str, kind: str, value: int, unit: str = "receipt"):
            return (name, kind, value, unit)

        def identity(prefix: str, artifact: tuple[int, int]):
            return [
                row(f"statement_{prefix}_receipt/aa/0/identity_schema_version", "receipt", 1),
                row(f"statement_{prefix}_receipt/aa/0/artifact_hi", "receipt", artifact[0], "identity_word"),
                row(f"statement_{prefix}_receipt/aa/0/artifact_lo", "receipt", artifact[1], "identity_word"),
                row(f"statement_{prefix}_receipt/aa/0/structure_hi", "receipt", 3, "identity_word"),
                row(f"statement_{prefix}_receipt/aa/0/structure_lo", "receipt", 4, "identity_word"),
                row(f"statement_{prefix}_receipt/aa/0/dependencies_hi", "receipt", 5, "identity_word"),
                row(f"statement_{prefix}_receipt/aa/0/dependencies_lo", "receipt", 6, "identity_word"),
            ]

        rows = identity("compile", (1, 2)) + [
            row("statement_plan_cache/aa/0", "evidence", 1, "count"),
            row("statement_execution_receipt/7/identity_schema_version", "receipt", 1),
            row("statement_execution_receipt/7/artifact_hi", "receipt", 1, "identity_word"),
            row("statement_execution_receipt/7/artifact_lo", "receipt", 2, "identity_word"),
            row("statement_execution_receipt/7/structure_hi", "receipt", 3, "identity_word"),
            row("statement_execution_receipt/7/structure_lo", "receipt", 4, "identity_word"),
            row("statement_execution_receipt/7/dependencies_hi", "receipt", 5, "identity_word"),
            row("statement_execution_receipt/7/dependencies_lo", "receipt", 6, "identity_word"),
            row("statement_execution_receipt/7/expected_class", "receipt", 2),
            row("statement_execution_receipt/7/actual_class", "receipt", 2),
            row("statement_execution_receipt/7/actual_fingerprint_hi", "receipt", 7, "identity_word"),
            row("statement_execution_receipt/7/actual_fingerprint_lo", "receipt", 8, "identity_word"),
            row("statement_execution_receipt/7/admission", "receipt", 1),
            row("statement_execution_receipt/7/terminal", "receipt", 2),
            row("statement_execution_receipt/7/image", "receipt", 1),
            row("statement_execution_receipt/7/working_set_memory_bytes", "receipt", 100),
            # The collector query itself has the newest execution id, but its
            # artifact is not the target compile receipt.
            row("statement_execution_receipt/8/identity_schema_version", "receipt", 1),
            row("statement_execution_receipt/8/artifact_hi", "receipt", 90, "identity_word"),
            row("statement_execution_receipt/8/artifact_lo", "receipt", 91, "identity_word"),
            row("statement_execution_receipt/8/structure_hi", "receipt", 3, "identity_word"),
            row("statement_execution_receipt/8/structure_lo", "receipt", 4, "identity_word"),
            row("statement_execution_receipt/8/dependencies_hi", "receipt", 5, "identity_word"),
            row("statement_execution_receipt/8/dependencies_lo", "receipt", 6, "identity_word"),
        ]
        executor = BenchmarkExecutor(
            connection={},
            iterations=1,
            warmup=0,
            timeout_seconds=1,
            collect_memory=False,
        )
        result = executor._collect_compile_receipt(Connection(rows))
        self.assertEqual(result["status"], "Verified")
        self.assertEqual(result["execution_id"], 7)
        self.assertEqual(result["compilation"], "CacheHit")
        self.assertEqual(result["compile_state"], "NotExecuted")
        self.assertEqual(result["artifact_identity"]["schema_version"], 1)

    def test_explicit_run_id_is_exclusive_and_attempts_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_root = Path(tmp) / "report"
            run = RunOutput.create(report_root, run_id="run-one")
            first = run.begin_attempt("sql-source")
            first.seal(status="Completed")
            second = run.begin_attempt("sql-source")
            second.write_failure(status="Failed", error="cancelled")
            second.seal(status="Failed", failure_path=second.failure_path)
            run.finalize(status="Failed")

            with self.assertRaises(RunOutputError):
                RunOutput.create(report_root, run_id="run-one")
            with self.assertRaises(RunOutputError):
                run.owned_path(Path(tmp) / "outside")

            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "Failed")
            self.assertEqual(
                [attempt["status"] for attempt in manifest["attempts"]],
                ["Completed", "Failed"],
            )

    def test_campaign_budget_is_frozen_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="budget")
            run.register_cell(
                cell_id="source--attempt-0001",
                query_cases=99,
                sample_rows=198,
                product_receipts=396,
            )
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["registration"]["status"], "WithinBudget")
            self.assertLessEqual(
                manifest["registration"]["budget_bytes"],
                manifest["registration"]["total_limit_bytes"],
            )

            with self.assertRaises(RunOutputError):
                run.register_cell(
                    cell_id="oversized",
                    query_cases=100_000,
                    sample_rows=100_000,
                    product_receipts=100_000,
                )

    def test_receipt_contract_does_not_turn_uncovered_into_success(self) -> None:
        payload = {
            "version": 3,
            "ownership": {"schema_version": 1, "run_id": "run"},
            "workloads": [{
                "name": "w",
                "queries": [{
                    "id": "q",
                    "compile_receipt": {
                        "schema_version": 1,
                        "status": "Uncovered",
                        "reason": "historical source has no receipt",
                    },
                }],
            }],
        }
        validate_benchmark_payload(payload)
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(payload, require_receipts=True)

    def test_verified_receipt_requires_nested_identity(self) -> None:
        payload = {
            "version": 3,
            "ownership": {"schema_version": 1, "run_id": "run"},
            "workloads": [{
                "name": "w",
                "queries": [{
                    "id": "q",
                    "compile_receipt": {
                        "schema_version": 1,
                        "status": "Verified",
                        "association_basis": "latest_execution_same_artifact",
                        "query_fingerprint": "abcd",
                        "occurrence": 0,
                        "compilation": "Executed",
                        "compile_state": "Executed",
                        "execution_id": 1,
                        "artifact_identity": {
                            "schema_version": 1,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        },
                        "compile": {"artifact_identity": {
                            "schema_version": 1,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        }},
                        "execution": {"artifact_identity": {
                            "schema_version": 1,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        }},
                        "selection": {
                            "admission": "Selected",
                            "actual_class": 2,
                            "actual_fingerprint": [7, 8],
                            "image": "Ready",
                            "terminal": "Completed",
                            "resources": {"max_parallel_tasks": 4},
                        },
                    },
                }],
            }],
        }
        validate_benchmark_payload(payload, require_receipts=True)
        mismatched = json.loads(json.dumps(payload))
        mismatched["workloads"][0]["queries"][0]["compile_receipt"]["execution"]["artifact_identity"]["artifact"] = [9, 10]
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(mismatched, require_receipts=True)
        not_executed = json.loads(json.dumps(payload))
        not_executed["workloads"][0]["queries"][0]["compile_receipt"]["selection"]["terminal"] = "NotExecuted"
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(not_executed, require_receipts=True)


if __name__ == "__main__":
    unittest.main()
