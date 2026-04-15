# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from harness.parser import ParseError, parse_sql_text


def test_parse_core_directives() -> None:
    sql = """
-- @setup
CREATE TABLE t1 (
  a INT,
  b TEXT
);

-- @statement ok
INSERT INTO t1 VALUES (1, 'x');

-- @statement count 2
INSERT INTO t1 VALUES (2, 'y'), (3, 'z');

-- @statement error duplicate key
INSERT INTO t1 VALUES (1, 'dup');

-- @query rowsort
SELECT a, b FROM t1 ORDER BY a;

-- @query approx(1e-5)
SELECT 0.1000001::DOUBLE;

-- @teardown
DROP TABLE t1;
"""

    blocks = parse_sql_text(sql)
    assert [block.kind for block in blocks] == [
        "setup",
        "statement",
        "statement",
        "statement",
        "query",
        "query",
        "teardown",
    ]

    assert blocks[1].statement_expect == "ok"
    assert blocks[2].statement_expect == "count"
    assert blocks[2].expected_count == 2
    assert blocks[3].statement_expect == "error"
    assert blocks[3].error_pattern == "duplicate key"
    assert blocks[4].query_mode == "rowsort"
    assert blocks[5].query_mode == "approx"
    assert blocks[5].epsilon == pytest.approx(1e-5)


def test_parse_query_default_mode_and_multiline_sql() -> None:
    sql = """
-- @query
SELECT
  'a;still-in-string' AS v,
  1 AS n;
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 1
    assert blocks[0].kind == "query"
    assert blocks[0].query_mode == "nosort"
    assert "still-in-string" in blocks[0].sql


def test_parse_handles_comments_and_block_comment_semicolon() -> None:
    sql = """
-- @query nosort
SELECT /* ; inside comment */ 1 AS a; -- trailing comment
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 1
    assert blocks[0].sql.startswith("SELECT")


def test_parse_skipif_and_onlyif_blocks() -> None:
    sql = """
-- @skipif paro
SELECT 1;

-- @onlyif postgres
-- @statement ok
SELECT 2;
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 3

    assert blocks[0].kind == "skipif"
    assert blocks[0].engine == "paro"
    assert blocks[0].sql == "SELECT 1;"

    assert blocks[1].kind == "onlyif"
    assert blocks[1].engine == "postgres"
    assert blocks[1].sql == ""

    assert blocks[2].kind == "statement"
    assert blocks[2].statement_expect == "ok"


def test_parse_invalid_statement_mode_raises() -> None:
    sql = """
-- @statement unknown
SELECT 1;
"""
    with pytest.raises(ParseError):
        parse_sql_text(sql)


def test_parse_statement_error_without_pattern() -> None:
    sql = """
-- @statement error
INSERT INTO t VALUES (1);
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 1
    assert blocks[0].kind == "statement"
    assert blocks[0].statement_expect == "error"
    assert blocks[0].error_pattern is None


def test_parse_missing_semicolon_raises() -> None:
    sql = """
-- @query nosort
SELECT 1
"""
    with pytest.raises(ParseError):
        parse_sql_text(sql)


def test_parse_normalize_profiles_for_query_and_statement() -> None:
    sql = """
-- @normalize explain_operator_timing
-- @query rowsort
SELECT 1;

SELECT 2;

-- @statement ok
-- @normalize explain_summary_timing, explain_runtime_bytes
INSERT INTO t VALUES (1);
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 3

    assert blocks[0].kind == "query"
    assert blocks[0].query_mode == "rowsort"
    assert blocks[0].normalizers == ("explain_operator_timing",)

    # @normalize only applies to the next query/statement block.
    assert blocks[1].kind == "query"
    assert blocks[1].normalizers == ()

    assert blocks[2].kind == "statement"
    assert blocks[2].statement_expect == "ok"
    assert blocks[2].normalizers == ("explain_summary_timing", "explain_runtime_bytes")


def test_parse_normalize_unknown_profile_raises() -> None:
    sql = """
-- @normalize unknown_profile
SELECT 1;
"""
    with pytest.raises(ParseError, match="unknown '@normalize' profile"):
        parse_sql_text(sql)


def test_parse_json_query_mode() -> None:
    sql = """
-- @query json
EXPLAIN SELECT 1 FORMAT JSON;
"""
    blocks = parse_sql_text(sql)
    assert len(blocks) == 1
    assert blocks[0].kind == "query"
    assert blocks[0].query_mode == "json"


def test_parse_copy_blocks() -> None:
    sql = """
-- @copy out
COPY t TO STDOUT WITH (FORMAT csv, HEADER true);

-- @copy in
COPY t FROM STDIN WITH (FORMAT csv);
-- @copydata
1,alpha
2,beta
-- @copyfail client abort
-- @endcopy
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2

    assert blocks[0].kind == "copy"
    assert blocks[0].copy_direction == "out"
    assert blocks[0].copy_data_lines == ()
    assert blocks[0].copy_fail_message is None

    assert blocks[1].kind == "copy"
    assert blocks[1].copy_direction == "in"
    assert blocks[1].copy_data_lines == ("1,alpha", "2,beta")
    assert blocks[1].copy_fail_message == "client abort"


def test_parse_control_block() -> None:
    sql = """
-- @control restart
SELECT 1;
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].kind == "control"
    assert blocks[0].control_action == "restart"
    assert blocks[0].control_args == ()
    assert blocks[1].kind == "query"
