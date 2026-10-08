# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import unittest

from benchmark.harness.executor import (
    BenchmarkExecutor,
    QueryExecutionResult,
    WorkloadExecutionResult,
)
from benchmark.harness.loader import QueryDef, WorkloadDef, _parse_byte_size


class WorkloadRequirementTests(unittest.TestCase):
    def test_byte_size_parser_distinguishes_decimal_and_binary_units(self) -> None:
        manifest = Path("workload.toml")
        self.assertEqual(
            _parse_byte_size("2GB", manifest_path=manifest, field_name="memory"),
            2_000_000_000,
        )
        self.assertEqual(
            _parse_byte_size("2GiB", manifest_path=manifest, field_name="memory"),
            2 << 30,
        )

    def test_server_memory_requirement_rejects_physical_overcommit(self) -> None:
        executor = BenchmarkExecutor(
            connection={},
            iterations=1,
            warmup=0,
            timeout_seconds=1,
            collect_memory=False,
        )
        executor._execute_sql = lambda *_args, **_kwargs: [(1 << 30,)]
        workload = WorkloadDef(
            name="analytical",
            description="",
            run_order=1,
            minimum_server_buffer_pool_bytes=2 << 30,
            root=Path("."),
            params={},
            setup_sql="",
            teardown_sql="",
            build_sql=None,
            queries=(),
        )

        with self.assertRaisesRegex(RuntimeError, "buffer-pool memory limit"):
            executor._validate_workload_requirements(object(), workload)

    def test_relative_median_guard_compares_queries_from_the_same_run(self) -> None:
        executor = BenchmarkExecutor(
            connection={},
            iterations=1,
            warmup=0,
            timeout_seconds=1,
            collect_memory=False,
        )
        workload = WorkloadDef(
            name="optimizer_planning",
            description="",
            run_order=1,
            minimum_server_buffer_pool_bytes=0,
            root=Path("."),
            params={},
            setup_sql="",
            teardown_sql="",
            build_sql=None,
            queries=(
                QueryDef(
                    id="disabled",
                    file=Path("disabled.sql"),
                    sql="EXPLAIN SELECT 1",
                    validate="none",
                ),
                QueryDef(
                    id="enabled",
                    file=Path("enabled.sql"),
                    sql="EXPLAIN SELECT 1",
                    validate="none",
                    max_median_ratio_to="disabled",
                    max_median_ratio=1.2,
                ),
            ),
        )
        result = WorkloadExecutionResult(
            name=workload.name,
            params={},
            queries=[
                QueryExecutionResult(
                    id="disabled",
                    validate_mode="none",
                    expected=None,
                    samples_ms=[1.0, 1.0, 1.0],
                    validation_result="PASS",
                ),
                QueryExecutionResult(
                    id="enabled",
                    validate_mode="none",
                    expected=None,
                    samples_ms=[1.3, 1.3, 1.3],
                    validation_result="PASS",
                ),
            ],
        )

        executor._apply_relative_latency_guards(workload, result)

        enabled = result.queries[1]
        self.assertEqual(enabled.validation_result, "FAIL")
        self.assertAlmostEqual(enabled.relative_median_ratio or 0.0, 1.3)


if __name__ == "__main__":
    unittest.main()
