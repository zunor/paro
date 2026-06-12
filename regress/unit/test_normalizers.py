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


def test_apply_explain_operator_timing_normalizer_rewrites_runtime_profile() -> None:
    lines = [
        (
            "PROFILE schema_version=1 query_id=42 events=9 parallelism=10 workers=2 "
            "worker_utilization=0.0992 ready_time_us=7 wait_time_us=8 "
            "wake_coalesce=3 backpressure=4 runtime_filter_installed=1 "
            "runtime_filter_no_wait=0"
        ),
        (
            "MEMORY_PROFILE grant_bytes=10 revoked_bytes=11 revocable_bytes=12 "
            "spill_bytes=13 spill_latency_us=14 yield_latency_us=15 repartition_depth=2"
        ),
        (
            '{"profile":{"parallelism":{"operator_time_ms":1.23,"worker_utilization":0.45,'
            '"max_threads":10,"observed_workers":2,"ready_time_us":7,"wait_time_us":8,'
            '"wake_count":9,"wake_coalesce_count":3,"backpressure_count":4}},'
            '"profile_events":[{"query_id":42,"thread_id":7,"total_threads":10,'
            '"time":{"duration_ms":0.5,"start_ms":1.0,"end_ms":1.5}}]}'
        ),
    ]

    assert apply_normalizers(lines, ("explain_operator_timing",)) == [
        (
            "PROFILE schema_version=1 query_id=<id> events=<events> "
            "parallelism=<threads> workers=<workers> worker_utilization=<ratio> "
            "ready_time_us=<us> wait_time_us=<us> wake_coalesce=<count> "
            "backpressure=<count> runtime_filter_installed=1 runtime_filter_no_wait=0"
        ),
        (
            "MEMORY_PROFILE grant_bytes=<bytes> revoked_bytes=<bytes> "
            "revocable_bytes=<bytes> spill_bytes=<bytes> spill_latency_us=<us> "
            "yield_latency_us=<us> repartition_depth=2"
        ),
        (
            '{"profile":{"parallelism":{"operator_time_ms": 0.0,"worker_utilization": 0.0,'
            '"max_threads": 0,"observed_workers": 0,"ready_time_us": 0,"wait_time_us": 0,'
            '"wake_count": 0,"wake_coalesce_count": 0,"backpressure_count": 0}},'
            '"profile_events":[{"query_id": 0,"thread_id": 0,"total_threads": 0,'
            '"time":{"duration_ms": 0.0,"start_ms": 0.0,"end_ms": 0.0}}]}'
        ),
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
        "HASH_AGGREGATE_BUILD (spilled=true spilled_bytes=4687424)",
        "Rows Returned: 2",
        (
            '{"actual":{"peak_memory_bytes":128,"temp_storage_bytes":512},'
            '"profile":{"memory":{"grant_bytes":1,"revocable_bytes":2,'
            '"revoked_bytes":3,"spill_bytes":4}},"summary":{"Total Temp Storage":512}}'
        ),
    ]

    assert apply_normalizers(lines, ("explain_runtime_bytes",)) == [
        "Sort Method: external merge",
        "Disk: <bytes>",
        "Peak Memory: <bytes>",
        "Temp Storage: <bytes>",
        "Total Temp Storage: <bytes>",
        "HASH_AGGREGATE_BUILD (spilled=true spilled_bytes=<bytes>)",
        "Rows Returned: 2",
        (
            '{"actual":{"peak_memory_bytes": 0,"temp_storage_bytes": 0},'
            '"profile":{"memory":{"grant_bytes": 0,"revocable_bytes": 0,'
            '"revoked_bytes": 0,"spill_bytes": 0}},"summary":{"Total Temp Storage": 0}}'
        ),
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


def test_apply_explain_search_ids_normalizer_rewrites_dynamic_ids_only() -> None:
    lines = [
        "  ->  FULLTEXT_SCAN on public.docs  (rows=4)",
        "    Search Definition: 10706",
        "    Search Generation: 3",
        "    Search Root: 12",
        "    Search Capability: Queryable",
        "    Column: 2",
        "    Mode: Filter",
    ]

    assert apply_normalizers(lines, ("explain_search_ids",)) == [
        "  ->  FULLTEXT_SCAN on public.docs  (rows=4)",
        "    Search Definition: <search-definition-id>",
        "    Search Generation: <search-generation-id>",
        "    Search Root: <search-root-id>",
        "    Search Capability: Queryable",
        "    Column: 2",
        "    Mode: Filter",
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
        "explain_search_ids",
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
