# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import json
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.receipt_contract import (  # noqa: E402
    ReceiptContractError,
    build_benchmark_cell_payload,
    uncovered_receipt,
    validate_benchmark_payload,
    validate_compile_document,
)
from harness.run_output import CampaignOutput, CorpusOutput, RunOutput, RunOutputError  # noqa: E402
from harness.executor import BenchmarkExecutor  # noqa: E402
from harness.loader import QueryDef  # noqa: E402


class RunOutputTests(unittest.TestCase):
    def test_wide_query_metadata_has_a_fixed_bounded_lease(self) -> None:
        from harness.run_output import _cell_budget_bytes, _encode_json, CapacityExceededError
        limit = _cell_budget_bytes(query_cases=1, sample_rows=6, product_receipts=6,
                                   calibration_rows=0, summary_captures=0, attempts=1)
        self.assertEqual(limit, 51_456)
        schema = [{"name": f"month_{i}_sales_per_square_foot",
                   "logical_type": "decimal", "engine_type": "DECIMAL(38,12)"}
                  for i in range(44)]
        payload = {"schema": {"paro": schema, "duckdb": schema},
                   "receipts_and_samples": "x" * 30_000}
        self.assertLess(len(_encode_json(payload, limit_bytes=limit)), limit)
        with self.assertRaises(CapacityExceededError):
            _encode_json({"unbounded_schema": "x" * limit}, limit_bytes=limit)

    @staticmethod
    def cell_payload(
        query_case: str = "q",
        arm_id: str = "normal",
        *,
        campaign_id: str = "campaign",
        run_id: str = "run",
        source_id: str = "source",
        attempt_id: str = "attempt-0001",
        sample_count: int = 1,
        **query: object,
    ) -> dict:
        return build_benchmark_cell_payload(
            campaign_id=campaign_id,
            run_id=run_id,
            query_case=query_case,
            arm_id=arm_id,
            workload_name="test",
            query_payload={"schema_version": 3, "status": "ok", **query},
            compile_receipts=[
                uncovered_receipt("test has no execution receipt")
                for _ in range(sample_count)
            ],
            source_id=source_id,
            attempt_id=attempt_id,
        )

    def test_compile_document_contract_rejects_missing_or_contradictory_state(self) -> None:
        document = {
            "schema_version": 3,
            "outcome": "Success",
            "artifact": "CompiledArtifactReady",
            "cache": "ForcedCompile",
            "admission": "NotExecuted",
            "execution": "NotExecuted",
            "artifact_identity": {"Observed": {"schema_version": 3, "artifact": [1, 2], "structure": [3, 4], "dependencies": [5, 6]}},
            "search_counters": [],
            "omitted_search_counters": 0,
        }
        self.assertEqual(validate_compile_document(document), "Summary")
        for legacy_version in (1, 2, 5):
            legacy = dict(document)
            legacy["schema_version"] = legacy_version
            with self.assertRaises(ReceiptContractError):
                validate_compile_document(legacy)
        for mutation in (
            lambda value: value.pop("artifact"),
            lambda value: value.update(schema_version=99),
            lambda value: value.update(outcome="Success", artifact="NotReady"),
            lambda value: value.update(execution="garbage"),
        ):
            invalid = dict(document)
            mutation(invalid)
            with self.assertRaises(ReceiptContractError):
                validate_compile_document(invalid)

    def test_compile_document_accepts_rust_external_observation_markers(self) -> None:
        base = {
            "schema_version": 3,
            "outcome": "Success",
            "artifact": "CompiledArtifactReady",
            "cache": "ForcedCompile",
            "admission": "NotExecuted",
            "artifact_identity": {"Observed": {"schema_version": 3, "artifact": [1, 2], "structure": [3, 4], "dependencies": [5, 6]}},
            "search_counters": [],
            "omitted_search_counters": 0,
        }
        fixture_path = Path(__file__).parents[1] / "fixtures" / "compile-summary" / "rust-observation-v3.json"
        for marker in json.loads(fixture_path.read_text(encoding="utf-8")):
            document = {**base, "execution": marker}
            self.assertEqual(validate_compile_document(document), "Summary")

        for invalid in (
            {"Observed": None},
            {"Uncovered": {"reason": "not captured"}},
            {"Failed": {"error": "compile failed"}},
            {"Cancelled": {"reason": "client"}},
            {"CapacityLimited": {"omitted_count": 2}},
        ):
            with self.assertRaises(ReceiptContractError):
                validate_compile_document({**base, "execution": invalid})
        with self.assertRaises(ReceiptContractError):
            validate_compile_document({**base, "execution": {"Unknown": {}}})

    def test_optimizer_work_projections_must_close_without_double_counting(self) -> None:
        document = {
            "schema_version": 3, "outcome": "Incomplete", "artifact": "NotReady",
            "cache": "ForcedCompile", "admission": "NotExecuted", "execution": "NotExecuted",
            "search_counters": [], "omitted_search_counters": 0,
            "optimizer_ns": {"Observed": 17},
        }
        work = {
            "total_ns": 17,
            "buckets": [{"kind": "Dependencies", "exclusive_ns": 10, "entries": 2},
                        {"kind": "Unclassified", "exclusive_ns": 7, "entries": 0}],
            "outside_search_ns": 2, "mandatory_ns": 4, "optional_ns": 11,
        }
        document["optimizer_work"] = {"Observed": work}
        self.assertEqual(validate_compile_document(document), "Summary")
        for key in ("total_ns", "mandatory_ns", "optional_ns", "outside_search_ns"):
            invalid = {**document, "optimizer_work": {"Observed": {**work, key: work[key] + 1}}}
            with self.assertRaises(ReceiptContractError):
                validate_compile_document(invalid)

    def test_campaign_output_seals_each_registered_query_arm_cell(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[
                    {
                        "query_case": "q11",
                        "arm_id": "normal",
                        "query_cases": 1,
                        "sample_rows": 2,
                        "product_receipts": 2,
                    },
                    {
                        "query_case": "q11",
                        "arm_id": "diagnostic",
                        "query_cases": 1,
                        "sample_rows": 1,
                        "product_receipts": 1,
                    },
                ],
            )
            output.publish_cell_json(
                query_case="q11", arm_id="normal",
                payload=self.cell_payload(
                    "q11", "normal", campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id, source_id="collector-q11-normal",
                    sample_count=2
                ),
            )
            output.publish_cell_json(
                query_case="q11", arm_id="diagnostic",
                payload=self.cell_payload(
                    "q11", "diagnostic", campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id, source_id="collector-q11-diagnostic"
                ),
            )
            output.publish_campaign_summary()
            campaign = json.loads((output.run.root / "campaign.json").read_text())
            self.assertEqual(campaign["kind"], "CampaignSummary")
            self.assertNotIn("workloads", campaign)
            output.finish(status="Completed")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Completed")
            self.assertEqual(len(manifest["registration"]["cells"]), 2)
            self.assertTrue((output.run.root / "campaign.json").exists())

    def test_campaign_summary_records_explicit_accepted_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "retry.json",
                source_id="collector",
                cells=[{
                    "query_case": "q",
                    "arm_id": "normal",
                    "query_cases": 1,
                    "sample_rows": 1,
                    "product_receipts": 1,
                }],
            )
            attempt = output.attempts[("q", "normal")]
            output.publish_cell_json(
                query_case="q",
                arm_id="normal",
                payload=self.cell_payload(
                    "q", "normal", campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id,
                    source_id=attempt.source_id,
                    attempt_id=attempt.attempt_id,
                ),
            )
            output.finish(status="Completed")
            summary = json.loads((output.run.root / "campaign.json").read_text())
            cell = summary["cells"][0]
            self.assertEqual(cell["accepted_attempt_id"], attempt.attempt_id)
            self.assertEqual(cell["attempts"][0]["attempt_index"], 0)

    def test_run_finalize_completed_rejects_unaccepted_cell(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="unaccepted")
            run.register_cell(
                cell_id="q--normal",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="normal",
            )
            run.registration.seal()
            attempt = run.begin_attempt("source", query_case="q", arm_id="normal")
            payload = self.cell_payload(
                "q", "normal", campaign_id=run.campaign_id, run_id=run.run_id,
                source_id=attempt.source_id, attempt_id=attempt.attempt_id,
            )
            attempt.cell_writer().write_json("result.json", payload)
            attempt.seal(status="Completed")
            with self.assertRaises(RunOutputError):
                run.finalize(status="Completed")

    def test_dual_arm_99_query_registration_stays_within_manifest_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="tpcds-99")
            for index in range(99):
                query_case = f"q{index + 1:02d}"
                for arm_id in ("control", "probe"):
                    run.register_cell(
                        cell_id=f"{query_case}--{arm_id}",
                        query_cases=1,
                        sample_rows=2,
                        product_receipts=2,
                        attempts=2,
                        query_case=query_case,
                        arm_id=arm_id,
                    )
            run.registration.seal()
            for index in range(99):
                query_case = f"q{index + 1:02d}"
                for arm_id in ("control", "probe"):
                    run.begin_attempt(
                        f"tpcds-{query_case}-{arm_id}",
                        query_case=query_case,
                        arm_id=arm_id,
                    )
            manifest = json.loads((run.root / "manifest.json").read_text())
            self.assertEqual(len(manifest["registration"]["cells"]), 198)
            self.assertLess(
                manifest["registration"]["manifest_bytes"],
                manifest["registration"]["manifest_limit_bytes"],
            )

    def test_cell_payload_must_match_registered_identity_and_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[{
                    "query_case": "q11",
                    "arm_id": "normal",
                    "query_cases": 1,
                    "sample_rows": 2,
                    "product_receipts": 2,
                }],
            )
            payload = self.cell_payload(
                "q11",
                "normal",
                campaign_id=output.run.campaign_id,
                run_id=output.run.run_id,
                source_id="collector-q11-normal",
                sample_count=2,
            )
            payload["ownership"]["sample_ids"] = ["q11-sample-0000"]
            with self.assertRaises(RunOutputError):
                output.publish_cell_json(
                    query_case="q11", arm_id="normal", payload=payload
                )

            payload["ownership"]["sample_ids"] = [
                "q11-sample-0000", "q11-sample-0001"
            ]
            payload["ownership"]["attempt_id"] = "attempt-9999"
            with self.assertRaises(RunOutputError):
                output.publish_cell_json(
                    query_case="q11", arm_id="normal", payload=payload
                )

    def test_missing_cell_payload_seals_campaign_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json", source_id="collector",
                cells=[{"query_case": "q", "arm_id": "normal", "query_cases": 1,
                        "sample_rows": 1, "product_receipts": 1}],
            )
            output.finish(status="Completed")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Incomplete")
            self.assertEqual(output.attempts[("q", "normal")].status, "Incomplete")

    def test_campaign_failure_does_not_rewrite_previous_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[
                    {
                        "query_case": "q11",
                        "arm_id": "normal",
                        "query_cases": 1,
                        "sample_rows": 1,
                        "product_receipts": 1,
                    },
                    {
                        "query_case": "q11",
                        "arm_id": "diagnostic",
                        "query_cases": 1,
                        "sample_rows": 1,
                        "product_receipts": 1,
                    },
                ],
            )
            output.publish_cell_json(
                query_case="q11", arm_id="normal",
                payload=self.cell_payload(
                    "q11", "normal", campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id, source_id="collector-q11-normal"
                ),
            )
            output.finish(
                status="Incomplete",
                errors={("q11", "diagnostic"): "cancelled"},
            )
            normal_attempt = json.loads(
                (output.attempts[("q11", "normal")].root / "attempt.json").read_text()
            )
            diagnostic_attempt = json.loads(
                (output.attempts[("q11", "diagnostic")].root / "attempt.json").read_text()
            )
            self.assertEqual(normal_attempt["status"], "Completed")
            self.assertEqual(diagnostic_attempt["status"], "Incomplete")
            self.assertTrue(
                (output.attempts[("q11", "diagnostic")].root / "failure.json").exists()
            )

    def test_terminal_run_rejects_late_payload_without_reclassifying_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[{
                    "query_case": "q11",
                    "arm_id": "normal",
                    "query_cases": 1,
                    "sample_rows": 1,
                    "product_receipts": 1,
                }],
            )
            output.finish(
                status="Incomplete",
                errors={("q11", "normal"): "cancelled before publication"},
            )
            manifest_before = json.loads(
                (output.run.root / "manifest.json").read_text(encoding="utf-8")
            )
            with self.assertRaises(RunOutputError):
                output.publish_cell_json(
                    query_case="q11",
                    arm_id="normal",
                    payload=self.cell_payload(
                        "q11",
                        "normal",
                        campaign_id=output.run.campaign_id,
                        run_id=output.run.run_id,
                        source_id="collector-q11-normal",
                    ),
                )
            manifest_after = json.loads(
                (output.run.root / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest_after["status"], "Incomplete")
            self.assertEqual(
                manifest_after["registration"]["status"],
                manifest_before["registration"]["status"],
            )
            self.assertEqual(
                manifest_after["registration"]["written_bytes"],
                manifest_before["registration"]["written_bytes"],
            )

    def test_capacity_failure_seals_incomplete_and_preserves_terminal_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[{
                    "query_case": "q11",
                    "arm_id": "normal",
                    "query_cases": 1,
                    "sample_rows": 1,
                    "product_receipts": 1,
                }],
            )
            with self.assertRaises(RunOutputError):
                output.publish_cell_json(
                    query_case="q11",
                    arm_id="normal",
                    payload=self.cell_payload(
                        "q11",
                        "normal",
                        campaign_id=output.run.campaign_id,
                        run_id=output.run.run_id,
                        source_id="collector-q11-normal",
                        payload="x" * 100_000,
                    ),
                )
            output.finish(status="Completed")
            attempt = json.loads(
                output.attempts[("q11", "normal")].root.joinpath("attempt.json").read_text()
            )
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Incomplete")
            self.assertEqual(manifest["registration"]["status"], "CapacityExceeded")
            self.assertEqual(attempt["status"], "Incomplete")

    def test_standalone_corpus_output_is_registered_and_sealed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CorpusOutput.create(
                Path(tmp) / "cold.json",
                source_id="cold-planning",
                query_case="q11",
                arm_id="diagnostic",
                sample_rows=2,
                product_receipts=2,
                summary_captures=0,
            )
            output.publish_json(
                self.cell_payload(
                    "q11",
                    "diagnostic",
                    campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id,
                    source_id="cold-planning",
                    sample_count=2,
                )
            )
            output.publish_summary("# diagnostic\n")
            output.finish(status="Completed")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Completed")
            self.assertTrue(output.result_path.exists())
            self.assertTrue(output.summary_path.exists())
            self.assertTrue(manifest["registration"]["registration_sealed"])

    def test_declared_capture_must_be_present_before_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CorpusOutput.create(
                Path(tmp) / "cold.json",
                source_id="cold-planning",
                query_case="q11",
                arm_id="diagnostic",
                sample_rows=1,
                product_receipts=1,
                summary_captures=1,
            )
            output.publish_json(
                self.cell_payload(
                    "q11",
                    "diagnostic",
                    campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id,
                    source_id="cold-planning",
                )
            )
            output.publish_summary("# diagnostic\n")
            output.finish(status="Completed")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Incomplete")
            self.assertEqual(
                json.loads(output.attempt.root.joinpath("attempt.json").read_text())["status"],
                "Incomplete",
            )

    def test_completed_capture_is_verified_by_path_and_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CorpusOutput.create(
                Path(tmp) / "cold.json",
                source_id="cold-planning",
                query_case="q11",
                arm_id="diagnostic",
                sample_rows=1,
                product_receipts=1,
                summary_captures=1,
            )
            capture = output.publish_capture_text("block-0000.json", '{"schema_version":3}\n')
            capture_ref = {
                "status": "Captured",
                "path": capture.relative_to(output.run.root).as_posix(),
                "sha256": hashlib.sha256(capture.read_bytes()).hexdigest(),
                "schema_version": 3,
            }
            output.publish_json(
                self.cell_payload(
                    "q11",
                    "diagnostic",
                    campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id,
                    source_id="cold-planning",
                    compile_document=capture_ref,
                )
            )
            output.publish_summary("# diagnostic\n")
            output.finish(status="Completed")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Completed")

    def test_standalone_corpus_failure_preserves_payload_and_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CorpusOutput.create(
                Path(tmp) / "d6.json",
                source_id="d6",
                query_case="q4",
                arm_id="diagnostic",
                sample_rows=1,
                product_receipts=1,
            )
            output.publish_json(
                self.cell_payload(
                    "q4", "diagnostic", campaign_id=output.run.campaign_id,
                    run_id=output.run.run_id, source_id="d6"
                )
            )
            output.finish(status="Failed", error="watchdog timeout")
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "Failed")
            self.assertTrue(output.result_path.exists())
            self.assertTrue(output.attempt.failure_path.exists())
            with self.assertRaises(RunOutputError):
                output.finish(status="Completed")

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
                    "schema_version": 3,
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

    def test_base_exception_cancellation_keeps_elapsed_and_receipt_state(self) -> None:
        class Cancelled(BaseException):
            pass

        class Validator:
            def check_plan(self, query, conn):
                return SimpleNamespace(status="PASS", detail=None)

            def validate_query(self, query, rows):
                return SimpleNamespace(status="PASS", detail=None)

        class Executor(BenchmarkExecutor):
            def __init__(self):
                super().__init__(
                    connection={},
                    iterations=1,
                    warmup=0,
                    timeout_seconds=1,
                    collect_memory=False,
                    collect_compile_receipts=True,
                )

            def _execute_sql(self, conn, sql, *, fetch):
                raise Cancelled("client cancellation")

            def _snapshot_compile_execution_ids(self, conn):
                return set()

        result = Executor()._run_query(
            object(),
            QueryDef(id="cancelled", file=Path("cancelled.sql"), sql="SELECT 1"),
            Validator(),
        )
        self.assertEqual(len(result.samples_ms), 1)
        self.assertGreaterEqual(result.samples_ms[0], 0.0)
        self.assertEqual(len(result.receipt_associations), 1)
        self.assertEqual(result.receipt_associations[0]["status"], "Uncovered")
        self.assertIn("client cancellation", result.error or "")

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
            return {"schema_version": 3, "artifact": list(artifact),
                    "structure": [3, 4], "dependencies": [5, 6]}

        target_identity = identity((1, 2))
        target_decision = {
            "schema_version": 3, "decision_id": 6, "query_fingerprint": 123,
            "occurrence": 0, "cache_hit": True,
            "artifact_identity": target_identity, "compile_work": None,
            "compile_receipt": {
                "schema_version": 3, "artifact_identity": target_identity,
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
            },
        }
        target_execution = {
            "schema_version": 3, "execution_id": 7, "statement_decision_id": 6,
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
            "schema_version": 3, "decision_id": 9, "query_fingerprint": 456,
            "occurrence": 0, "cache_hit": False,
            "artifact_identity": observer_identity, "compile_work": None,
        }
        observer_execution = {
            "schema_version": 3, "execution_id": 8, "statement_decision_id": 9,
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
        self.assertEqual(result["artifact_identity"]["schema_version"], 3)

    def test_explicit_run_id_is_exclusive_and_attempts_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_root = Path(tmp) / "report"
            run = RunOutput.create(report_root, run_id="run-one")
            first = run.begin_attempt("sql-source")
            with self.assertRaises(RunOutputError):
                first.seal(status="Completed")
            first.write_failure(status="Incomplete", error="no owned payload")
            first.seal(status="Incomplete", failure_path=first.failure_path)
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
                ["Incomplete", "Failed"],
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
            manifest = json.loads((run.root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "Incomplete")
            self.assertEqual(manifest["registration"]["status"], "CapacityExceeded")
            self.assertEqual(manifest["registration"]["omitted_count"], 1)

    def test_campaign_summary_is_typed_json_without_free_text_control_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "campaign.json",
                source_id="collector",
                cells=[{
                    "query_case": "q",
                    "arm_id": "normal",
                    "query_cases": 1,
                    "sample_rows": 1,
                    "product_receipts": 1,
                }],
            )
            output.publish_campaign_summary()
            campaign = json.loads((output.run.root / "campaign.json").read_text())
            self.assertEqual(campaign["kind"], "CampaignSummary")
            self.assertEqual(campaign["schema_version"], 3)
            self.assertFalse((output.run.root / "summary.md").exists())

    def test_manifest_index_supports_a_ninety_nine_query_campaign(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = CampaignOutput.create(
                Path(tmp) / "tpcds.json",
                source_id="tpcds",
                cells=[
                    {
                        "query_case": f"q{query_number:02d}",
                        "arm_id": "normal",
                        "query_cases": 1,
                        "sample_rows": 1,
                        "product_receipts": 1,
                    }
                    for query_number in range(1, 100)
                ],
            )
            manifest = json.loads((output.run.root / "manifest.json").read_text())
            self.assertEqual(len(manifest["registration"]["cells"]), 99)
            self.assertEqual(len(manifest["attempts"]), 99)
            self.assertLessEqual(
                manifest["registration"]["manifest_bytes"],
                manifest["registration"]["manifest_limit_bytes"],
            )
            self.assertTrue(all("started_at" not in attempt for attempt in manifest["attempts"]))
            self.assertTrue(all(attempt["metadata"].endswith("/attempt.json") for attempt in manifest["attempts"]))

    def test_attempt_lifecycle_timestamps_live_in_authoritative_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="attempt-index")
            run.register_cell(
                cell_id="q--normal",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="normal",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="normal")
            manifest = json.loads((run.root / "manifest.json").read_text())
            index = manifest["attempts"][0]
            self.assertNotIn("started_at", index)
            authoritative = json.loads((run.root / index["metadata"]).read_text())
            self.assertEqual(authoritative["status"], "Running")
            self.assertIn("started_at", authoritative)
            attempt.write_failure(status="Incomplete", error="test")
            attempt.seal(status="Incomplete", failure_path=attempt.failure_path)
            authoritative = json.loads((run.root / index["metadata"]).read_text())
            self.assertIn("sealed_at", authoritative)
            self.assertEqual(authoritative["status"], "Incomplete")

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
            with self.assertRaises(RunOutputError):
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

    def test_prepared_write_rejects_external_generation_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="generation")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="control",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")
            prepared = attempt.cell_writer().prepare_text("payload.txt", "before")
            prepared.path.parent.mkdir(parents=True, exist_ok=True)
            prepared.path.write_text("concurrent", encoding="utf-8")
            with self.assertRaises(RunOutputError):
                prepared.publish()
            self.assertEqual(run._manifest["registration"]["status"], "PublicationUnknown")
            self.assertTrue((run.root / "publication-unknown.json").exists())

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

    def test_capacity_terminal_closes_registration_and_records_omission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = RunOutput.create(Path(tmp), run_id="capacity-registration")
            run.register_cell(
                cell_id="q--control",
                query_cases=1,
                sample_rows=1,
                product_receipts=1,
                query_case="q",
                arm_id="control",
            )
            attempt = run.begin_attempt("source", query_case="q", arm_id="control")
            run._manifest["registration"]["total_limit_bytes"] = 64
            with self.assertRaises(RunOutputError):
                attempt.cell_writer().write_text("too-large.txt", "x" * 200)

            registration = run._manifest["registration"]
            self.assertEqual(registration["status"], "CapacityExceeded")
            self.assertEqual(run._manifest["status"], "Incomplete")
            self.assertEqual(registration["omitted_count"], 1)
            self.assertGreater(registration["omitted_bytes"], 0)
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
                run.cell_writer(query_case="q", arm_id="control", root=run.root).write_text(
                    "after.txt", "late"
                )

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
            "schema_version": 3,
            "version": 3,
            "ownership": {
                "schema_version": 3,
                "campaign_id": "campaign",
                "run_id": "run",
                "query_case": "q",
                "arm_id": "normal",
                "sample_ids": ["q-sample-0000"],
            },
            "workloads": [{
                "name": "w",
                "queries": [{
                    "id": "q",
                    "compile_receipt": {
                        "schema_version": 3,
                        "status": "Uncovered",
                        "reason": "historical source has no receipt",
                        "sample_id": "q-sample-0000",
                        "query_case": "q",
                        "arm_id": "normal",
                    },
                }],
            }],
        }
        validate_benchmark_payload(payload)
        with self.assertRaises(ReceiptContractError):
            validate_benchmark_payload(payload, require_receipts=True)

    def test_verified_receipt_requires_nested_identity(self) -> None:
        payload = {
            "schema_version": 3,
            "version": 3,
            "ownership": {
                "schema_version": 3,
                "campaign_id": "campaign",
                "run_id": "run",
                "query_case": "q",
                "arm_id": "normal",
                "sample_ids": ["q-sample-0000"],
            },
            "workloads": [{
                "name": "w",
                "queries": [{
                    "id": "q",
                    "compile_receipt": {
                        "schema_version": 3,
                        "status": "Verified",
                        "sample_id": "q-sample-0000",
                        "query_case": "q",
                        "arm_id": "normal",
                        "association_basis": "statement_decision_id",
                        "statement_decision_id": 4,
                        "query_fingerprint": "000000000000abcd",
                        "occurrence": 0,
                        "compilation": "Executed",
                        "compile_state": "Executed",
                        "execution_id": 1,
                        "artifact_identity": {
                            "schema_version": 3,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        },
                        "compile": {"artifact_identity": {
                            "schema_version": 3,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        }, "decision_id": 4, "cache_hit": False, "receipt": {
                            "schema_version": 3,
                            "artifact_identity": {
                                "schema_version": 3,
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
                            "schema_version": 3,
                            "artifact": [1, 2],
                            "structure": [3, 4],
                            "dependencies": [5, 6],
                        }, "raw": {
                            "schema_version": 3,
                            "execution_id": 1,
                            "statement_decision_id": 4,
                            "artifact_identity": {
                                "schema_version": 3,
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
        occurrence_free = json.loads(json.dumps(payload))
        del occurrence_free["workloads"][0]["queries"][0]["compile_receipt"]["occurrence"]
        validate_benchmark_payload(occurrence_free, require_receipts=True)
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
