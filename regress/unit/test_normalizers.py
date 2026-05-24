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


def test_apply_explain_operator_counters_normalizer_rewrites_rows_and_loops() -> None:
    lines = [
        "FILTER  (actual time=<time-range> rows=14000 loops=7)",
        "->  SEQ_SCAN on t  (actual time=<time-range> rows=3 loops=2)",
        '{"actual":{"rows":14000,"loops":7,"startup_time_ms":0.018,"total_time_ms":0.024}}',
    ]

    assert apply_normalizers(lines, ("explain_operator_counters",)) == [
        "FILTER  (actual time=<time-range> rows=<rows> loops=<loops>)",
        "->  SEQ_SCAN on t  (actual time=<time-range> rows=<rows> loops=<loops>)",
        '{"actual":{"rows": 0,"loops": 0,"startup_time_ms":0.018,"total_time_ms":0.024}}',
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


def test_apply_explain_routine_ids_normalizer_rewrites_catalog_ids_only() -> None:
    lines = [
        "Routines: py_explain[10315@1]",
        "Routine: py_scale[2048@7]",
        "Runtime: submissions=1 blocked=0 input_rows=2 output_rows=2",
    ]

    assert apply_normalizers(lines, ("explain_routine_ids",)) == [
        "Routines: py_explain[<routine-id>@1]",
        "Routine: py_scale[<routine-id>@7]",
        "Runtime: submissions=1 blocked=0 input_rows=2 output_rows=2",
    ]


def test_apply_explain_external_runtime_normalizer_rewrites_latency_line() -> None:
    lines = [
        "Latency(us): acquire=1 queue=0 kernel=34738 encode_decode=34693",
        "Data Plane: 20 bytes, warm=0 cold=1 retired=0",
    ]

    assert apply_normalizers(lines, ("explain_external_runtime",)) == [
        "Latency(us): acquire=<us> queue=<us> kernel=<us> encode_decode=<us>",
        "Data Plane: 20 bytes, warm=0 cold=1 retired=0",
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


def test_apply_transaction_ids_normalizer_rewrites_concurrency_ids() -> None:
    lines = [
        (
            "ERROR: transaction 4611686018427387921 blocked by conflicting locks held "
            "by [TxnId(4611686018427387920)]"
        ),
        (
            "txn_id: TxnId(7), read_ts: ReadTs(8), commit_ts: CommitTs(9), "
            "table_id: TableId(10), database_id: DatabaseId(11), "
            "ssi_state_epoch: 12, tenant_id: 0, tablet_id: 99"
        ),
    ]

    assert apply_normalizers(lines, ("transaction_ids",)) == [
        "ERROR: transaction <id> blocked by conflicting locks held by [TxnId(<id>)]",
        (
            "txn_id: TxnId(<id>), read_ts: ReadTs(<id>), commit_ts: CommitTs(<id>), "
            "table_id: TableId(<id>), database_id: DatabaseId(<id>), "
            "ssi_state_epoch: <id>, tenant_id: <id>, tablet_id: <id>"
        ),
    ]


def test_apply_regress_paths_normalizer_rewrites_workspace_specific_fixture_paths() -> None:
    lines = [
        (
            "IMPORTS "
            "('/home/runner/work/paro/paro/regress/report/fixtures/python_udf/scalar/"
            "fixed_width_fast_path/python_udf/modules/fake_numpy/numpy.py')"
        ),
        (
            "ERROR: Python runtime is misconfigured for CREATE FUNCTION "
            "(SQLSTATE=39P04; DETAIL=failed to bootstrap Python interpreter "
            "'/Users/linjunhong/workspace/paro/regress/fixtures/python_udf/bin/"
            "python_misconfigured.py': simulated misconfigured python runtime)"
        ),
    ]

    assert apply_normalizers(lines, ("regress_paths",)) == [
        (
            "IMPORTS "
            "('<repo>/regress/report/fixtures/python_udf/scalar/fixed_width_fast_path/"
            "python_udf/modules/fake_numpy/numpy.py')"
        ),
        (
            "ERROR: Python runtime is misconfigured for CREATE FUNCTION "
            "(SQLSTATE=39P04; DETAIL=failed to bootstrap Python interpreter "
            "'<repo>/regress/fixtures/python_udf/bin/python_misconfigured.py': "
            "simulated misconfigured python runtime)"
        ),
    ]


def test_apply_python_runtime_retry_hint_normalizer_rewrites_countdown() -> None:
    lines = [
        (
            "ERROR: Python runtime is degraded and unavailable "
            "(SQLSTATE=39P04; HINT=next automatic Python runtime probe in 999 ms)"
        )
    ]

    assert apply_normalizers(lines, ("python_runtime_retry_hint",)) == [
        (
            "ERROR: Python runtime is degraded and unavailable "
            "(SQLSTATE=39P04; HINT=next automatic Python runtime probe in <ms> ms)"
        )
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
        "explain_operator_counters",
        "explain_summary_timing",
        "explain_runtime_bytes",
        "explain_routine_ids",
        "explain_external_runtime",
        "explain_runtime",
        "copy_rowcount",
        "transaction_ids",
        "regress_paths",
        "python_runtime_retry_hint",
    )


def test_apply_normalizers_unknown_profile_raises() -> None:
    with pytest.raises(ValueError, match="unknown normalizer profile"):
        apply_normalizers(["x"], ("unknown_profile",))
