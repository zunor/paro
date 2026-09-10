# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Tests for JSON and text EXPLAIN ANALYZE sidecar parsing."""

from __future__ import annotations

import unittest

from benchmark.harness.executor import (
    BenchmarkExecutor,
    _extract_explain_execution_time_ms,
    _flatten_explain_profile,
)


TEXT_PROFILE = """PIPELINE 0
  SOURCE #0 ROWSET_SCAN (actual time=0.214..2.751 rows=73049 loops=21 scheduler_worker_count=1 scheduler_ready_time_us=20)
  TRANSFORM #1 HASH_AGGREGATE_BUILD (actual time=1.000..3.500 rows=100 loops=2 aggregate_hash_max_radix_partition_skew_percent=139)
  SINK #2 CLIENT_RESULT (actual time=3.500..3.500 rows=90 loops=1 peak_memory_bytes=524248)
PROFILE schema_version=1 query_id=7 events=42 parallelism=4 workers=4 worker_utilization=0.9277 ready_time_us=487 wait_time_us=0 wake_coalesce=3 backpressure=0 runtime_filter_installed=0 runtime_filter_no_wait=0
MEMORY_PROFILE grant_bytes=0 revoked_bytes=0 revocable_bytes=0 spill_bytes=0 spill_latency_us=0 yield_latency_us=0 repartition_depth=0
Execution Time: 447.812 ms
Rows Returned: 90
"""


class ExplainProfileTests(unittest.TestCase):
    def test_text_profile_is_flattened_with_shared_fields(self) -> None:
        profiles = _flatten_explain_profile(TEXT_PROFILE)

        self.assertEqual(len(profiles), 3)
        self.assertEqual(profiles[0]["node_id"], 0)
        self.assertEqual(profiles[0]["rows"], 73049)
        self.assertEqual(profiles[0]["startup_time_ms"], 0.214)
        self.assertEqual(profiles[0]["total_time_ms"], 2.751)
        self.assertEqual(profiles[0]["profile_parallelism"], 4)
        self.assertEqual(profiles[0]["profile_event_count"], 42)
        self.assertAlmostEqual(profiles[0]["profile_worker_utilization"], 0.9277)
        self.assertEqual(profiles[1]["aggregate_hash_max_radix_partition_skew_percent"], 139)
        self.assertEqual(profiles[2]["reported_memory_bytes"], 524248)
        self.assertEqual(_extract_explain_execution_time_ms(TEXT_PROFILE), 447.812)

    def test_fetch_joins_one_row_per_text_line(self) -> None:
        executor = BenchmarkExecutor(
            connection={},
            iterations=1,
            warmup=0,
            timeout_seconds=1,
            collect_memory=False,
        )
        executor._execute_sql = lambda _conn, _sql, fetch: [
            ("PIPELINE 0",),
            ("  SOURCE #0 ROWSET_SCAN",),
        ]

        self.assertEqual(
            executor._fetch_explain_profile_json(None, "SELECT 1"),
            "PIPELINE 0\n  SOURCE #0 ROWSET_SCAN",
        )


if __name__ == "__main__":
    unittest.main()
