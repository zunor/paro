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


def test_parse_control_block_with_key_value_args() -> None:
    sql = """
-- @control connect user=routine_builder database=postgres
SELECT current_user();
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].kind == "control"
    assert blocks[0].control_action == "connect"
    assert blocks[0].control_args == ("user=routine_builder", "database=postgres")
    assert blocks[1].kind == "query"


def test_parse_session_directive_applies_to_next_auto_block() -> None:
    sql = """
-- @session s1 user=alice database=analytics
BEGIN;

-- @session s1
SELECT 1;
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].kind == "statement"
    assert blocks[0].session_name == "s1"
    assert blocks[0].session_args == ("user=alice", "database=analytics")
    assert blocks[1].kind == "query"
    assert blocks[1].session_name == "s1"
    assert blocks[1].session_args == ()


def test_parse_session_directive_combines_with_query_directive() -> None:
    sql = """
-- @session reader
-- @query rowsort
SELECT v FROM t;
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 1
    assert blocks[0].kind == "query"
    assert blocks[0].query_mode == "rowsort"
    assert blocks[0].session_name == "reader"


def test_parse_session_async_directive() -> None:
    sql = """
-- @session writer async=blocked user=alice
INSERT INTO t VALUES (1);

-- @await blocked timeout=5s
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].kind == "statement"
    assert blocks[0].session_name == "writer"
    assert blocks[0].session_args == ("user=alice",)
    assert blocks[0].async_label == "blocked"
    assert blocks[1].kind == "await"
    assert blocks[1].await_label == "blocked"
    assert blocks[1].await_timeout_ms == 5000


def test_parse_sleep_and_wait_expect_directives() -> None:
    sql = """
-- @sleep 25ms

-- @wait_expect interval=10ms timeout=2s
SELECT v FROM t;
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].kind == "sleep"
    assert blocks[0].sleep_ms == 25
    assert blocks[1].kind == "query"
    assert blocks[1].wait_expect_interval_ms == 10
    assert blocks[1].wait_expect_timeout_ms == 2000


def test_parse_wait_expect_rejects_statement_target() -> None:
    sql = """
-- @wait_expect interval=10ms timeout=1s
INSERT INTO t VALUES (1);
"""

    with pytest.raises(ParseError, match="can only target a query"):
        parse_sql_text(sql)


def test_parse_duplicate_async_label_raises() -> None:
    sql = """
-- @session s1 async=work
SELECT 1;

-- @session s2 async=work
SELECT 2;
"""

    with pytest.raises(ParseError, match="duplicate async label"):
        parse_sql_text(sql)


def test_parse_session_rejects_setup_target() -> None:
    sql = """
-- @session s1
-- @setup
CREATE TABLE t(v INT);
"""
    with pytest.raises(ParseError, match="always runs on the default session"):
        parse_sql_text(sql)


def test_parse_fixture_directive_applies_to_next_sql_block() -> None:
    sql = """
-- @fixture python_udf/modules/basic_math
-- @query file
FILE '{{fixture:python_udf/modules/basic_math}}/basic_math.py';

SELECT 1;
"""

    blocks = parse_sql_text(sql)
    assert len(blocks) == 2
    assert blocks[0].fixture_refs == ("python_udf/modules/basic_math",)
    assert blocks[1].fixture_refs == ()


def test_parse_fixture_rejects_path_traversal() -> None:
    sql = """
-- @fixture ../outside
SELECT 1;
"""
    with pytest.raises(ParseError, match="fixture path must stay within regress/fixtures"):
        parse_sql_text(sql)
