import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.receipt_contract import ReceiptContractError, validate_benchmark_payload  # noqa: E402
from harness.run_output import RunOutput, RunOutputError  # noqa: E402
from harness.executor import BenchmarkExecutor  # noqa: E402
from harness.loader import QueryDef  # noqa: E402


class RunOutputTests(unittest.TestCase):
    def test_failed_timed_sample_keeps_elapsed_and_previous_receipt(self) -> None:
        class Validator:
            def check_plan(self, query, conn):
                return SimpleNamespace(status="PASS", detail=None)

            def validate_query(self, query, rows):
                return SimpleNamespace(status="PASS", detail=None)

        class Executor(BenchmarkExecutor):
            def __init__(self):
                super().__init__(
                    connection={},
                    iterations=2,
                    warmup=0,
                    timeout_seconds=1,
                    collect_memory=False,
                    collect_compile_receipts=True,
                )
                self.calls = 0

            def _execute_sql(self, conn, sql, *, fetch):
                self.calls += 1
                if self.calls == 1:
                    return [(1,)]
                raise RuntimeError("second sample failed")

            def _snapshot_compile_execution_ids(self, conn):
                return set()

            def _collect_compile_receipt(self, conn, **kwargs):
                return {
                    "schema_version": 1,
                    "status": "Verified",
                    "sample": len(getattr(self, "receipts", [])) + 1,
                }

        executor = Executor()
        executor.receipts = []
        result = executor._run_query(
            object(),
            QueryDef(id="failure", file=Path("failure.sql"), sql="SELECT 1"),
            Validator(),
        )
        self.assertEqual(len(result.samples_ms), 2)
        self.assertTrue(all(sample >= 0 for sample in result.samples_ms))
        self.assertEqual(len(result.receipt_associations), 2)
        self.assertEqual(result.receipt_associations[0]["status"], "Verified")
        self.assertEqual(result.receipt_associations[1]["status"], "Uncovered")
        self.assertIn("second sample failed", result.error or "")

    def test_receipt_collector_ignores_its_own_introspection_execution(self) -> None:
        columns = [
            "name", "kind", "last_elapsed_us", "metric_value", "metric_unit",
            "invocation_count", "record_type", "record_id", "payload_json",
        ]

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

        def row(record_type: str, record_id: int, payload: dict[str, object] | None):
            return (record_type, record_type, 0, 0, "receipt", 0,
                    record_type, record_id, json.dumps(payload) if payload is not None else "")

        def identity(artifact: tuple[int, int]):
            return {"schema_version": 1, "artifact": list(artifact),
                    "structure": [3, 4], "dependencies": [5, 6]}

        target_identity = identity((1, 2))
        target_decision = {
            "schema_version": 1, "decision_id": 6, "query_fingerprint": 123,
            "occurrence": 0, "cache_hit": True,
            "artifact_identity": target_identity, "compile_work": None,
        }
        target_execution = {
            "schema_version": 1, "execution_id": 7, "statement_decision_id": 6,
            "artifact_identity": target_identity, "expected_class": 2,
            "actual_class": 2, "actual_fingerprint": [7, 8],
            "resources": {
                "class": 2, "minimum_memory_bytes": 100,
                "working_set_memory_bytes": 200, "memory_ceiling_bytes": 1000,
                "memory_completion": "Guaranteed", "max_parallel_tasks": 4,
                "external_worker_slots": 0,
            },
            "admission": "Selected", "fallback": None,
            "reservation": "Committed", "lowering": "Ready",
            "lowering_error": None, "image": "Ready", "terminal": "Completed",
            "terminal_error": None,
        }
        observer_identity = identity((90, 91))
        observer_decision = {
            "schema_version": 1, "decision_id": 9, "query_fingerprint": 456,
            "occurrence": 0, "cache_hit": False,
            "artifact_identity": observer_identity, "compile_work": None,
        }
        observer_execution = {
            "schema_version": 1, "execution_id": 8, "statement_decision_id": 9,
            "artifact_identity": observer_identity, "expected_class": 2,
            "actual_class": None, "actual_fingerprint": None, "resources": None,
            "admission": "Failed", "fallback": None, "reservation": "NotRequired",
            "lowering": "NotStarted", "lowering_error": None, "image": "NotReady",
            "terminal": "NotExecuted", "terminal_error": "not selected",
        }
        rows = [
            row("statement_cache", 6, target_decision),
            row("execution_receipt", 7, target_execution),
            row("statement_cache", 9, observer_decision),
            row("execution_receipt", 8, observer_execution),
        ]
        executor = BenchmarkExecutor(
            connection={},
            iterations=1,
            warmup=0,
            timeout_seconds=1,
            collect_memory=False,
        )
        result = executor._collect_compile_receipt(Connection(rows), before_execution_ids={8})
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
                cell_id="source--default",
                query_cases=99,
                sample_rows=198,
                product_receipts=396,
                query_case="source",
                arm_id="default",
            )
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["registration"]["status"], "WithinBudget")
            self.assertLessEqual(
                manifest["registration"]["budget_bytes"],
                manifest["registration"]["total_limit_bytes"],
            )

            with self.assertRaises(RunOutputError):
                run.register_cell(
                    cell_id="oversized--default",
                    query_cases=100_000,
                    sample_rows=100_000,
                    product_receipts=100_000,
                    query_case="oversized",
                    arm_id="default",
                )

    def test_cells_are_owned_by_query_and_arm_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="cells")
            contract = dict(
                cell_id="q11--control",
                query_cases=1,
                sample_rows=4,
                product_receipts=4,
                query_case="q11",
                arm_id="control",
            )
            run.register_cell(**contract)
            run.register_cell(**contract)
            run.register_cell(
                cell_id="q11--probe",
                query_cases=1,
                sample_rows=4,
                product_receipts=4,
                query_case="q11",
                arm_id="probe",
            )
            with self.assertRaises(RunOutputError):
                run.register_cell(**{**contract, "sample_rows": 5})
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(
                {cell["cell_id"] for cell in manifest["registration"]["cells"]},
                {"q11--control", "q11--probe"},
            )

    def test_registration_seal_and_unregistered_cell_writes_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="sealed")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="control",
            )
            run.registration.seal()
            with self.assertRaises(RunOutputError):
                run.register_cell(
                    cell_id="q--probe",
                    query_cases=1,
                    sample_rows=1,
                    product_receipts=1,
                    query_case="q",
                    arm_id="probe",
                )
            with self.assertRaises(RunOutputError):
                run.cell_writer(query_case="missing", arm_id="control", root=run.root)

    def test_replacement_is_charged_as_final_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="replace")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=4,
                product_receipts=4,
                query_case="q",
                arm_id="control",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")
            writer = attempt.cell_writer()
            writer.write_text("payload.txt", "x" * 100)
            writer.write_text("payload.txt", "y" * 7, overwrite=True)
            registration = run._manifest["registration"]
            cell = registration["cells"][0]
            self.assertEqual(cell["written_bytes"], 7)
            self.assertEqual(registration["written_bytes"], 7)

    def test_publication_failure_becomes_explicit_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="publication")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="control",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")
            original_persist = run._persist_manifest

            def fail_persist() -> None:
                raise OSError("manifest filesystem failure")

            run._persist_manifest = fail_persist  # type: ignore[method-assign]
            with self.assertRaises(OSError):
                attempt.cell_writer().write_text("payload.txt", "payload")
            marker = json.loads(
                (run.root / "publication-unknown.json").read_text(encoding="utf-8")
            )
            self.assertEqual(marker["status"], "PublicationUnknown")
            self.assertEqual(run._manifest["status"], "Incomplete")
            self.assertEqual(
                run._manifest["registration"]["status"], "PublicationUnknown"
            )
            run._persist_manifest = original_persist  # type: ignore[method-assign]

    def test_concurrent_cell_writers_share_one_capacity_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="parallel")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=32,
                product_receipts=32,
                query_case="q",
                arm_id="control",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")

            def publish(index: int) -> Path:
                return attempt.cell_writer().write_text(f"payload-{index}.txt", "x" * 9)

            with ThreadPoolExecutor(max_workers=8) as executor:
                paths = list(executor.map(publish, range(32)))
            self.assertEqual(len(paths), 32)
            self.assertTrue(all(path.exists() for path in paths))
            self.assertEqual(run._manifest["registration"]["written_bytes"], 32 * 9)

    def test_actual_utf8_writer_quota_preserves_existing_output_and_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="quota")
            run.register_cell(
                cell_id="quota--control",
                query_cases=1,
                sample_rows=2,
                product_receipts=1,
                query_case="quota",
                arm_id="control",
            )
            attempt = run.begin_attempt("quota-source", query_case="quota", arm_id="control")
            writer = attempt.cell_writer()
            run._manifest["registration"]["total_limit_bytes"] = 256
            writer.write_text("kept.txt", "ok")
            with self.assertRaises(RunOutputError):
                writer.write_text("too-large.txt", "汉字" * 200)
            self.assertTrue((attempt.root / "kept.txt").exists())
            self.assertFalse((attempt.root / "too-large.txt").exists())
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "Incomplete")
            self.assertEqual(manifest["registration"]["status"], "CapacityExceeded")
            with self.assertRaises(RunOutputError):
                run.finalize(status="Completed")

    def test_manifest_is_published_through_a_bounded_utf8_writer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="manifest")
            registration = run._manifest["registration"]
            registration["manifest_limit_bytes"] = 128
            registration["cells"] = [{"cell_id": "q--control", "detail": "汉字" * 100}]
            with self.assertRaises(RunOutputError):
                run._persist_manifest()
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "Running")
            self.assertEqual(manifest["registration"]["cells"], [])

    def test_terminal_attempt_cannot_be_overwritten_by_retry_or_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="terminal")
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")
            attempt.write_failure(status="Cancelled", error="client cancelled")
            attempt.seal(status="Cancelled", failure_path=attempt.failure_path)
            with self.assertRaises(RunOutputError):
                attempt.write_failure(status="Failed", error="late error")
            with self.assertRaises(RunOutputError):
                attempt.seal(status="Completed")

    def test_receipt_contract_does_not_turn_uncovered_into_success(self) -> None:
        payload = {
            "version": 3,
            "ownership": {"schema_version": 1, "campaign_id": "campaign", "run_id": "run"},
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
            "ownership": {"schema_version": 1, "campaign_id": "campaign", "run_id": "run"},
            "workloads": [{
                "name": "w",
                "queries": [{
                    "id": "q",
                    "compile_receipt": {
                        "schema_version": 1,
                        "status": "Verified",
                        "association_basis": "statement_decision_id",
                        "statement_decision_id": 4,
                        "query_fingerprint": "000000000000abcd",
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
                        }, "decision_id": 4, "cache_hit": False, "receipt": {
                            "schema_version": 1,
                            "artifact_identity": {
                                "schema_version": 1,
                                "artifact": [1, 2],
                                "structure": [3, 4],
                                "dependencies": [5, 6],
                            },
                            "search_stop": {"Observed": "QualityPolicySatisfied"},
                            "search_complete": {"Observed": False},
                            "quality_policy_satisfied": {"Observed": True},
                            "budget_limited": {"Observed": False},
                            "obligations": {"Observed": 0},
                            "groups": {"Observed": 1},
                            "logical_expressions": {"Observed": 1},
                            "physical_expressions": {"Observed": 1},
                            "expected_class": {"Observed": 2},
                            "variant_count": {"Observed": 1},
                            "omitted_variants": 0,
                            "compile_work": None,
                        }},
                        "execution": {"execution_id": 1, "artifact_identity": {
                            "schema_version": 1,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        }, "raw": {
                            "schema_version": 1,
                            "execution_id": 1,
                            "statement_decision_id": 4,
                            "artifact_identity": {
                                "schema_version": 1,
                                "artifact": [1, 2],
                                "structure": [3, 4],
                                "dependencies": [5, 6],
                            },
                            "expected_class": 2,
                            "actual_class": 2,
                            "actual_fingerprint": [7, 8],
                            "resources": {
                                "class": 2,
                                "minimum_memory_bytes": 100,
                                "working_set_memory_bytes": 200,
                                "memory_ceiling_bytes": 1000,
                                "memory_completion": "Guaranteed",
                                "max_parallel_tasks": 4,
                                "external_worker_slots": 0,
                            },
                            "admission": "Selected",
                            "fallback": None,
                            "reservation": "Committed",
                            "lowering": "Ready",
                            "lowering_error": None,
                            "image": "Ready",
                            "terminal": "Completed",
                            "terminal_error": None,
                        }},
                        "selection": {
                            "expected_class": 2,
                            "admission": "Selected",
                            "actual_class": 2,
                            "actual_fingerprint": [7, 8],
                            "image": "Ready",
                            "terminal": "Completed",
                            "reservation": "Committed",
                            "lowering": "Ready",
                            "lowering_error": None,
                            "terminal_error": None,
                            "resources": {
                                "class": 2,
                                "minimum_memory_bytes": 100,
                                "working_set_memory_bytes": 200,
                                "memory_ceiling_bytes": 1000,
                                "memory_completion": "Guaranteed",
                                "max_parallel_tasks": 4,
                                "external_worker_slots": 0,
                            },
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

        same_hash_different_contract = json.loads(json.dumps(payload))
        same_hash_different_contract["workloads"][0]["queries"][0]["compile_receipt"]["execution"]["raw"]["terminal"] = "Failed"
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(same_hash_different_contract, require_receipts=True)

        unknown_producer_version = json.loads(json.dumps(payload))
        unknown_producer_version["workloads"][0]["queries"][0]["compile_receipt"]["execution"]["raw"]["schema_version"] = 99
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(unknown_producer_version, require_receipts=True)


if __name__ == "__main__":
    unittest.main()
