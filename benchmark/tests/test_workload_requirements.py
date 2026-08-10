# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import unittest

from benchmark.harness.executor import BenchmarkExecutor
from benchmark.harness.loader import WorkloadDef, _parse_byte_size


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
            minimum_server_memory_bytes=2_000_000_000,
            root=Path("."),
            params={},
            setup_sql="",
            teardown_sql="",
            build_sql=None,
            queries=(),
        )

        with self.assertRaisesRegex(RuntimeError, "physical memory limit"):
            executor._validate_workload_requirements(object(), workload)


if __name__ == "__main__":
    unittest.main()
