# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from harness.normalizers import apply_normalizers, normalizer_profiles


def test_apply_explain_operator_timing_normalizer_rewrites_actual_time() -> None:
    lines = [
        "FILTER  (actual time=0.018..0.024 rows=2)",
        "->  SEQ_SCAN on t  (actual time=1.2..3.45 rows=3)",
        '{"actual":{"startup_time_ms":0.018,"total_time_ms":0.024,"rows":2}}',
    ]

    assert apply_normalizers(lines, ("explain_operator_timing",)) == [
        "FILTER  (actual time=<time-range> rows=2)",
        "->  SEQ_SCAN on t  (actual time=<time-range> rows=3)",
        '{"actual":{"startup_time_ms": 0.0,"total_time_ms": 0.0,"rows":2}}',
    ]


def test_apply_explain_summary_timing_normalizer_rewrites_summary_lines() -> None:
    lines = [
        "Planning Time: 12.34 ms",
        "Execution Time: 56.78 ms",
        "Rows Returned: 2",
        '{"summary":{"Planning Time":"1.23 ms","Execution Time":"4.56 ms"}}',
    ]

    assert apply_normalizers(lines, ("explain_summary_timing",)) == [
        "Planning Time: <time-ms>",
        "Execution Time: <time-ms>",
        "Rows Returned: 2",
        '{"summary":{"Planning Time": "<time-ms>","Execution Time": "<time-ms>"}}',
    ]


def test_apply_explain_runtime_bytes_normalizer_rewrites_only_target_fields() -> None:
    lines = [
        "Sort Method: external merge",
        "Disk: 2.0 MB",
        "Peak Memory: 128.0 kB",
        "Temp Storage: 1.0 GB",
        "Total Temp Storage: 512 B",
        "Rows Returned: 2",
        '{"actual":{"peak_memory_bytes":128,"temp_storage_bytes":512},"summary":{"Total Temp Storage":512}}',
    ]

    assert apply_normalizers(lines, ("explain_runtime_bytes",)) == [
        "Sort Method: external merge",
        "Disk: <bytes>",
        "Peak Memory: <bytes>",
        "Temp Storage: <bytes>",
        "Total Temp Storage: <bytes>",
        "Rows Returned: 2",
        '{"actual":{"peak_memory_bytes": 0,"temp_storage_bytes": 0},"summary":{"Total Temp Storage": 0}}',
    ]


def test_apply_explain_runtime_alias_combines_operator_and_summary_timing() -> None:
    lines = [
        "FILTER  (actual time=0.018..0.024 rows=2)",
        "Planning Time: 12.34 ms",
        "Execution Time: 56.78 ms",
    ]

    assert apply_normalizers(lines, ("explain_runtime",)) == [
        "FILTER  (actual time=<time-range> rows=2)",
        "Planning Time: <time-ms>",
        "Execution Time: <time-ms>",
    ]


def test_apply_copy_rowcount_normalizer_rewrites_copy_status_lines() -> None:
    lines = [
        "COPY 6144",
        "COPY 3616",
        "INSERT 0 1",
    ]

    assert apply_normalizers(lines, ("copy_rowcount",)) == [
        "COPY <rows>",
        "COPY <rows>",
        "INSERT 0 1",
    ]


def test_normalizers_preserve_structure_and_non_target_fields() -> None:
    lines = [
        "QUERY PLAN",
        "PROJECTION",
        "  Output: ref#0",
        "  ->  FILTER  (actual time=0.010..0.020 rows=2)",
        "        Filter: (ref#0 > 1)",
        "Peak Memory: 64.0 kB",
        "Rows Returned: 2",
    ]

    normalized = apply_normalizers(
        lines,
        ("explain_operator_timing", "explain_runtime_bytes"),
    )

    assert len(normalized) == len(lines)
    assert normalized == [
        "QUERY PLAN",
        "PROJECTION",
        "  Output: ref#0",
        "  ->  FILTER  (actual time=<time-range> rows=2)",
        "        Filter: (ref#0 > 1)",
        "Peak Memory: <bytes>",
        "Rows Returned: 2",
    ]


def test_normalizer_profiles_returns_registered_names() -> None:
    assert normalizer_profiles() == (
        "explain_operator_timing",
        "explain_summary_timing",
        "explain_runtime_bytes",
        "explain_runtime",
        "copy_rowcount",
    )


def test_apply_normalizers_unknown_profile_raises() -> None:
    with pytest.raises(ValueError, match="unknown normalizer profile"):
        apply_normalizers(["x"], ("unknown_profile",))
