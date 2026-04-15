# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest

from harness.comparator import (
    ResultMismatch,
    build_transcript,
    compare_result_file,
    decode_cell,
    encode_cell,
    parse_result_text,
)
from harness.executor import QueryOutput


def _query_output(
    *,
    mode: str = "nosort",
    epsilon: float | None = None,
    sql: str = "SELECT v FROM t;",
    columns: list[str] | None = None,
    rows: list[list[str]] | None = None,
    raw_rows: list[tuple[object, ...]] | None = None,
    normalizers: tuple[str, ...] = (),
    copy_direction: str | None = None,
    copy_data_lines: tuple[str, ...] = (),
    copy_fail_message: str | None = None,
    status: str | None = None,
    is_statement: bool = False,
) -> QueryOutput:
    return QueryOutput(
        block_index=1,
        line_no=1,
        sql=sql,
        mode=mode,
        epsilon=epsilon,
        columns=columns or ["v"],
        rows=rows or [["1"]],
        raw_rows=raw_rows or [(1,)],
        normalizers=normalizers,
        copy_direction=copy_direction,
        copy_data_lines=copy_data_lines,
        copy_fail_message=copy_fail_message,
        status=status,
        is_statement=is_statement,
    )


def test_encode_decode_roundtrip_reserved_and_escaped_tokens() -> None:
    values = [
        None,
        "",
        "NULL",
        "(empty)",
        "a\tb",
        "line1\nline2",
        "left\\right",
        "carriage\rreturn",
    ]

    for value in values:
        encoded = encode_cell(value)
        decoded = decode_cell(encoded)
        assert decoded == value


def test_parse_transcript_blocks() -> None:
    output = _query_output(columns=["a", "b"], rows=[["1", "x"]], raw_rows=[(1, "x")])
    transcript = build_transcript([output])
    blocks = parse_result_text(transcript)

    assert len(blocks) == 1
    assert blocks[0].mode == "nosort"
    assert blocks[0].columns == ["a", "b"]
    assert blocks[0].rows == [["1", "x"]]


def test_parse_transcript_with_normalize_directive() -> None:
    transcript = "\n".join(
        [
            "-- @query rowsort",
            "-- @normalize explain_operator_timing,explain_runtime_bytes",
            "SELECT v FROM t;",
            "v",
            "1",
            "",
        ]
    )
    blocks = parse_result_text(transcript)
    assert len(blocks) == 1
    assert blocks[0].mode == "rowsort"
    assert blocks[0].normalizers == ("explain_operator_timing", "explain_runtime_bytes")


def test_compare_exact_mismatch_writes_actual(tmp_path: Path) -> None:
    expected_output = _query_output(rows=[["1"]], raw_rows=[(1,)])
    expected_file = tmp_path / "case.result"
    expected_file.write_text(build_transcript([expected_output]), encoding="utf-8")

    actual_output = _query_output(rows=[["2"]], raw_rows=[(2,)])
    with pytest.raises(ResultMismatch):
        compare_result_file(
            expected_path=expected_file,
            query_outputs=[actual_output],
            write_actual=True,
        )

    assert expected_file.with_suffix(".result.actual").exists()


def test_compare_mismatch_can_disable_actual_write(tmp_path: Path) -> None:
    expected_output = _query_output(rows=[["1"]], raw_rows=[(1,)])
    expected_file = tmp_path / "case.result"
    expected_file.write_text(build_transcript([expected_output]), encoding="utf-8")

    actual_output = _query_output(rows=[["3"]], raw_rows=[(3,)])
    with pytest.raises(ResultMismatch):
        compare_result_file(
            expected_path=expected_file,
            query_outputs=[actual_output],
            write_actual=False,
        )

    assert not expected_file.with_suffix(".result.actual").exists()


def test_compare_hash_mode(tmp_path: Path) -> None:
    output = _query_output(
        mode="hash",
        columns=["v"],
        rows=[["1"], ["2"], ["3"]],
        raw_rows=[(1,), (2,), (3,)],
    )
    expected_path = tmp_path / "hash.result"
    expected_path.write_text(build_transcript([output]), encoding="utf-8")

    compare_result_file(expected_path=expected_path, query_outputs=[output], write_actual=False)


def test_compare_hash_mode_mismatch(tmp_path: Path) -> None:
    expected = _query_output(
        mode="hash",
        columns=["v"],
        rows=[["1"], ["2"]],
        raw_rows=[(1,), (2,)],
    )
    actual = _query_output(
        mode="hash",
        columns=["v"],
        rows=[["1"], ["3"]],
        raw_rows=[(1,), (3,)],
    )

    expected_path = tmp_path / "hash_mismatch.result"
    expected_path.write_text(build_transcript([expected]), encoding="utf-8")

    with pytest.raises(ResultMismatch, match="hash mismatch"):
        compare_result_file(expected_path=expected_path, query_outputs=[actual], write_actual=False)


def test_compare_applies_normalizers_before_text_diff(tmp_path: Path) -> None:
    expected = _query_output(
        sql="EXPLAIN ANALYZE SELECT * FROM t WHERE id > 1;",
        columns=["QUERY PLAN"],
        rows=[
            ["FILTER  (actual time=<time-range> rows=2)"],
            ["Planning Time: <time-ms>"],
            ["Execution Time: <time-ms>"],
        ],
        raw_rows=[
            ("FILTER  (actual time=<time-range> rows=2)",),
            ("Planning Time: <time-ms>",),
            ("Execution Time: <time-ms>",),
        ],
        normalizers=("explain_runtime",),
    )
    actual = _query_output(
        sql="EXPLAIN ANALYZE SELECT * FROM t WHERE id > 1;",
        columns=["QUERY PLAN"],
        rows=[
            ["FILTER  (actual time=0.011..0.022 rows=2)"],
            ["Planning Time: 2.33 ms"],
            ["Execution Time: 4.55 ms"],
        ],
        raw_rows=[
            ("FILTER  (actual time=0.011..0.022 rows=2)",),
            ("Planning Time: 2.33 ms",),
            ("Execution Time: 4.55 ms",),
        ],
    )

    expected_file = tmp_path / "normalize.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_compare_applies_normalizers_with_rowsort_mode(tmp_path: Path) -> None:
    expected = _query_output(
        mode="rowsort",
        sql="EXPLAIN ANALYZE SELECT * FROM t;",
        columns=["QUERY PLAN"],
        rows=[["FILTER  (actual time=<time-range> rows=2)"]],
        raw_rows=[("FILTER  (actual time=<time-range> rows=2)",)],
        normalizers=("explain_runtime",),
    )
    actual = _query_output(
        mode="rowsort",
        sql="EXPLAIN ANALYZE SELECT * FROM t;",
        columns=["QUERY PLAN"],
        rows=[["FILTER  (actual time=0.010..0.020 rows=2)"]],
        raw_rows=[("FILTER  (actual time=0.010..0.020 rows=2)",)],
    )

    expected_file = tmp_path / "normalize_rowsort.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_compare_allows_normalize_with_hash_mode(tmp_path: Path) -> None:
    expected = _query_output(
        mode="hash",
        sql="SELECT v FROM t;",
        columns=["v"],
        rows=[["1"], ["2"]],
        raw_rows=[(1,), (2,)],
        normalizers=("explain_runtime",),
    )
    actual = _query_output(
        mode="hash",
        sql="SELECT v FROM t;",
        columns=["v"],
        rows=[["1"], ["2"]],
        raw_rows=[(1,), (2,)],
    )

    expected_file = tmp_path / "normalize_hash.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_compare_json_mode_canonicalizes_object_key_order(tmp_path: Path) -> None:
    expected = _query_output(
        mode="json",
        sql="EXPLAIN SELECT 1 FORMAT JSON;",
        columns=["QUERY PLAN"],
        rows=[['{"b":2,"a":{"y":2,"x":1}}']],
        raw_rows=[('{"b":2,"a":{"y":2,"x":1}}',)],
    )
    actual = _query_output(
        mode="json",
        sql="EXPLAIN SELECT 1 FORMAT JSON;",
        columns=["QUERY PLAN"],
        rows=[['{"a":{"x":1,"y":2},"b":2}']],
        raw_rows=[('{"a":{"x":1,"y":2},"b":2}',)],
    )

    expected_file = tmp_path / "json.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_compare_json_mode_applies_normalizers_before_json_compare(tmp_path: Path) -> None:
    expected = _query_output(
        mode="json",
        sql="EXPLAIN ANALYZE SELECT 1 FORMAT JSON;",
        columns=["QUERY PLAN"],
        rows=[
            [
                '{"summary":{"Execution Time":"<time-ms>"},"plan":{"actual":{"rows":1,"loops":1}}}'
            ]
        ],
        raw_rows=[
            (
                '{"summary":{"Execution Time":"<time-ms>"},"plan":{"actual":{"rows":1,"loops":1}}}',
            )
        ],
        normalizers=("explain_summary_timing",),
    )
    actual = _query_output(
        mode="json",
        sql="EXPLAIN ANALYZE SELECT 1 FORMAT JSON;",
        columns=["QUERY PLAN"],
        rows=[
            [
                '{"summary":{"Execution Time":"12.345 ms"},"plan":{"actual":{"rows":1,"loops":1}}}'
            ]
        ],
        raw_rows=[
            (
                '{"summary":{"Execution Time":"12.345 ms"},"plan":{"actual":{"rows":1,"loops":1}}}',
            )
        ],
    )

    expected_file = tmp_path / "json_normalized.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_compare_approx_mode_passes(tmp_path: Path) -> None:
    expected = _query_output(
        mode="approx",
        epsilon=1e-3,
        rows=[["1.000000"]],
        raw_rows=[(1.0,)],
    )
    actual = _query_output(
        mode="approx",
        epsilon=1e-3,
        rows=[["1.000500"]],
        raw_rows=[(1.0005,)],
    )

    expected_file = tmp_path / "approx.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[actual], write_actual=False)


def test_missing_expected_file_writes_actual_snapshot(tmp_path: Path) -> None:
    output = _query_output(rows=[["9"]], raw_rows=[(9,)])
    expected_file = tmp_path / "missing.result"

    with pytest.raises(ResultMismatch, match="missing expected result file"):
        compare_result_file(expected_path=expected_file, query_outputs=[output], write_actual=True)

    assert expected_file.with_suffix(".result.actual").exists()


def test_copy_transcript_roundtrip() -> None:
    output = _query_output(
        sql="COPY t TO STDOUT WITH (FORMAT csv);",
        copy_direction="out",
        copy_data_lines=("1,alpha", "2,beta"),
        status="COPY 2",
        is_statement=True,
    )

    transcript = build_transcript([output])
    blocks = parse_result_text(transcript)

    assert len(blocks) == 1
    assert blocks[0].kind == "copy"
    assert blocks[0].copy_direction == "out"
    assert blocks[0].raw_result_lines == [
        "-- @copydata",
        "1,alpha",
        "2,beta",
    ]


def test_compare_copy_block(tmp_path: Path) -> None:
    expected = _query_output(
        sql="COPY t FROM STDIN WITH (FORMAT csv);",
        copy_direction="in",
        copy_data_lines=("1,alpha", "2,beta"),
        copy_fail_message="client abort",
        status="ERROR: COPY from stdin aborted by client: client abort",
        is_statement=True,
    )

    expected_file = tmp_path / "copy.result"
    expected_file.write_text(build_transcript([expected]), encoding="utf-8")

    compare_result_file(expected_path=expected_file, query_outputs=[expected], write_actual=False)
