# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
import time

import pytest

from harness.executor import ExecutionError, execute_blocks
from harness.parser import Block


@dataclass
class _Step:
    sql: str
    rowcount: int = 0
    columns: list[str] | None = None
    rows: list[tuple[Any, ...]] | None = None
    error: Exception | None = None
    delay_seconds: float = 0.0


@dataclass
class _CopyStep:
    sql: str
    direction: str
    statusmessage: str = "COPY 0"
    read_chunks: list[str | bytes] = field(default_factory=list)
    error_on_exit: Exception | None = None
    writes: list[str] = field(default_factory=list)


class _FakeDbError(Exception):
    def __init__(self, message: str, *, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self._conn = conn
        self.rowcount = -1
        self.description = None
        self.statusmessage = None
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        if not self._conn.steps:
            raise AssertionError(f"unexpected SQL (no more steps): {sql}")
        step = self._conn.steps.pop(0)
        if isinstance(step, _CopyStep):
            raise AssertionError(f"expected COPY for step {step.sql}, got execute({sql})")
        assert step.sql == sql
        if step.delay_seconds:
            time.sleep(step.delay_seconds)
        if step.error is not None:
            raise step.error

        self.rowcount = step.rowcount
        if step.columns is None:
            self.description = None
        else:
            self.description = [SimpleNamespace(name=col) for col in step.columns]
        self._rows = list(step.rows or [])

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def copy(self, sql: str) -> "_FakeCopy":
        if not self._conn.steps:
            raise AssertionError(f"unexpected COPY (no more steps): {sql}")
        step = self._conn.steps.pop(0)
        if not isinstance(step, _CopyStep):
            raise AssertionError(f"expected SQL step for copy({sql}), got {step!r}")
        assert step.sql == sql
        return _FakeCopy(self, step)


class _FakeCopy:
    def __init__(self, cursor: _FakeCursor, step: _CopyStep) -> None:
        self._cursor = cursor
        self._step = step

    def __enter__(self) -> "_FakeCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is not None and self._step.error_on_exit is not None:
            raise self._step.error_on_exit
        self._cursor.statusmessage = self._step.statusmessage
        return None

    def read(self) -> str | bytes:
        if not self._step.read_chunks:
            return ""
        return self._step.read_chunks.pop(0)

    def write(self, data: str) -> None:
        self._step.writes.append(data)


class _FakeConnection:
    def __init__(self, steps: list[_Step | _CopyStep], *, transaction_status: str = "IDLE") -> None:
        self.steps = list(steps)
        self.autocommit = False
        self.rollback_calls = 0
        self.close_calls = 0
        self.info = SimpleNamespace(transaction_status=SimpleNamespace(name=transaction_status))

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.info.transaction_status = SimpleNamespace(name="IDLE")

    def close(self) -> None:
        self.close_calls += 1


def test_setup_failure_skips_main_but_runs_teardown() -> None:
    conn = _FakeConnection(
        [
            _Step("CREATE TABLE t(a INT);", error=_FakeDbError("boom")),
            _Step("DROP TABLE IF EXISTS t;"),
        ]
    )
    blocks = [
        Block(kind="setup", line_no=1, sql="CREATE TABLE t(a INT);"),
        Block(
            kind="statement",
            line_no=2,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="ok",
        ),
        Block(kind="teardown", line_no=3, sql="DROP TABLE IF EXISTS t;"),
    ]

    result = execute_blocks(conn, blocks)
    assert result.skipped_due_to_setup_error
    assert conn.autocommit is True
    assert conn.steps == []


def test_statement_count_mismatch_raises() -> None:
    conn = _FakeConnection([_Step("UPDATE t SET a = 2;", rowcount=1)])
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="UPDATE t SET a = 2;",
            statement_expect="count",
            expected_count=2,
        )
    ]

    with pytest.raises(ExecutionError, match="expected 2, got 1"):
        execute_blocks(conn, blocks)


def test_statement_error_matches_pattern_and_rolls_back() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "INSERT INTO t VALUES (1);",
                error=_FakeDbError("duplicate key value violates unique constraint", sqlstate="23505"),
            )
        ]
    )
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="error",
            error_pattern="duplicate key",
        )
    ]

    execute_blocks(conn, blocks)
    assert conn.rollback_calls == 1


def test_statement_error_pattern_can_assert_sqlstate() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "DROP TABLE parent;",
                error=_FakeDbError(
                    "cannot drop table because other objects depend on it",
                    sqlstate="2BP01",
                ),
            )
        ]
    )
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="DROP TABLE parent;",
            statement_expect="error",
            error_pattern="SQLSTATE=2BP01",
        )
    ]

    execute_blocks(conn, blocks)
    assert conn.rollback_calls == 1


def test_statement_error_mismatch_reports_sqlstate() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "INSERT INTO t VALUES (1);",
                error=_FakeDbError("duplicate key value violates unique constraint", sqlstate="23505"),
            )
        ]
    )
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="error",
            error_pattern="some other pattern",
        )
    ]

    with pytest.raises(ExecutionError, match="SQLSTATE: 23505"):
        execute_blocks(conn, blocks)


def test_statement_error_without_pattern_accepts_any_error() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "INSERT INTO t VALUES (1);",
                error=_FakeDbError("boom", sqlstate="XX001"),
            )
        ]
    )
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="error",
            error_pattern=None,
        )
    ]

    execute_blocks(conn, blocks)
    assert conn.rollback_calls == 1


def test_query_rowsort_returns_sorted_normalized_rows() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "SELECT a, b FROM t;",
                columns=["a", "b"],
                rows=[(2, "y"), (1, "x")],
            )
        ]
    )
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT a, b FROM t;",
            query_mode="rowsort",
        )
    ]

    result = execute_blocks(conn, blocks, float_precision=6)
    assert len(result.query_outputs) == 1
    assert result.query_outputs[0].columns == ["a", "b"]
    assert result.query_outputs[0].rows == [["1", "x"], ["2", "y"]]


def test_statement_output_carries_normalizers() -> None:
    conn = _FakeConnection([_Step("INSERT INTO t VALUES (1);", rowcount=1)])
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="ok",
            normalizers=("explain_runtime",),
        )
    ]

    result = execute_blocks(conn, blocks)
    assert len(result.query_outputs) == 1
    assert result.query_outputs[0].normalizers == ("explain_runtime",)


def test_query_output_carries_normalizers() -> None:
    conn = _FakeConnection(
        [
            _Step(
                "SELECT a FROM t;",
                columns=["a"],
                rows=[(1,)],
            )
        ]
    )
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT a FROM t;",
            query_mode="nosort",
            normalizers=("explain_runtime", "explain_table_ids"),
        )
    ]

    result = execute_blocks(conn, blocks)
    assert len(result.query_outputs) == 1
    assert result.query_outputs[0].normalizers == ("explain_runtime", "explain_table_ids")


def test_skipif_onlyif_controls_next_block() -> None:
    conn = _FakeConnection([_Step("SELECT 2;", columns=["v"], rows=[(2,)])])
    blocks = [
        Block(kind="skipif", line_no=1, sql="", engine="paro"),
        Block(kind="query", line_no=2, sql="SELECT 1;", query_mode="nosort"),
        Block(kind="onlyif", line_no=3, sql="", engine="paro"),
        Block(kind="query", line_no=4, sql="SELECT 2;", query_mode="nosort"),
    ]

    result = execute_blocks(conn, blocks, engine="paro")
    assert len(result.query_outputs) == 1
    assert result.query_outputs[0].sql == "SELECT 2;"


def test_teardown_runs_when_main_fails() -> None:
    conn = _FakeConnection(
        [
            _Step("INSERT INTO t VALUES (1);", error=_FakeDbError("failed")),
            _Step("DROP TABLE IF EXISTS t;"),
        ]
    )
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="ok",
        ),
        Block(kind="teardown", line_no=2, sql="DROP TABLE IF EXISTS t;"),
    ]

    result = execute_blocks(conn, blocks)
    assert len(result.query_outputs) == 1
    assert result.query_outputs[0].status.startswith("ERROR")
    assert conn.steps == []


def test_copy_out_captures_payload_and_status() -> None:
    conn = _FakeConnection(
        [
            _CopyStep(
                "COPY t TO STDOUT WITH (FORMAT csv);",
                direction="out",
                statusmessage="COPY 2",
                read_chunks=["1,alpha\n", "2,beta\n"],
            )
        ]
    )
    blocks = [
        Block(
            kind="copy",
            line_no=1,
            sql="COPY t TO STDOUT WITH (FORMAT csv);",
            copy_direction="out",
        )
    ]

    result = execute_blocks(conn, blocks)
    assert len(result.query_outputs) == 1
    output = result.query_outputs[0]
    assert output.copy_direction == "out"
    assert output.copy_data_lines == ("1,alpha", "2,beta")
    assert output.status == "COPY 2"


def test_copy_in_writes_payload_and_status() -> None:
    step = _CopyStep(
        "COPY t FROM STDIN WITH (FORMAT csv);",
        direction="in",
        statusmessage="COPY 2",
    )
    conn = _FakeConnection([step])
    blocks = [
        Block(
            kind="copy",
            line_no=1,
            sql="COPY t FROM STDIN WITH (FORMAT csv);",
            copy_direction="in",
            copy_data_lines=("1,alpha", "2,beta"),
        )
    ]

    result = execute_blocks(conn, blocks)
    output = result.query_outputs[0]
    assert step.writes == ["1,alpha\n2,beta\n"]
    assert output.copy_direction == "in"
    assert output.status == "COPY 2"


def test_copy_in_abort_returns_server_error_output() -> None:
    conn = _FakeConnection(
        [
            _CopyStep(
                "COPY t FROM STDIN WITH (FORMAT csv);",
                direction="in",
                error_on_exit=_FakeDbError("COPY from stdin aborted by client: client abort"),
            )
        ]
    )
    blocks = [
        Block(
            kind="copy",
            line_no=1,
            sql="COPY t FROM STDIN WITH (FORMAT csv);",
            copy_direction="in",
            copy_data_lines=("1,alpha",),
            copy_fail_message="client abort",
        )
    ]

    result = execute_blocks(conn, blocks)
    output = result.query_outputs[0]
    assert output.status == "ERROR: COPY from stdin aborted by client: client abort"
    assert output.copy_fail_message == "client abort"


def test_control_block_uses_handler_and_replaces_connection() -> None:
    conn1 = _FakeConnection([])
    conn2 = _FakeConnection(
        [
            _Step("SELECT 1;", columns=["?column?"], rows=[(1,)]),
        ]
    )
    blocks = [
        Block(kind="control", line_no=1, sql="", control_action="restart"),
        Block(kind="query", line_no=2, sql="SELECT 1;", query_mode="nosort"),
    ]

    seen_actions: list[str] = []

    def handler(conn: Any, block: Block) -> Any:
        assert conn is conn1
        seen_actions.append(block.control_action or "")
        return conn2

    result = execute_blocks(conn1, blocks, control_handler=handler)
    assert seen_actions == ["restart"]
    assert result.query_outputs[0].rows == [["1"]]


def test_named_sessions_reuse_connections_and_label_outputs() -> None:
    default = _FakeConnection([])
    named = _FakeConnection(
        [
            _Step("BEGIN;"),
            _Step("SELECT 1;", columns=["v"], rows=[(1,)]),
        ]
    )
    blocks = [
        Block(kind="statement", line_no=1, sql="BEGIN;", statement_expect="ok", session_name="s1"),
        Block(kind="query", line_no=2, sql="SELECT 1;", query_mode="nosort", session_name="s1"),
    ]
    opened: list[tuple[str, dict[str, str]]] = []

    def factory(name: str, options: dict[str, str]) -> _FakeConnection:
        opened.append((name, dict(options)))
        return named

    result = execute_blocks(default, blocks, session_connection_factory=factory)

    assert opened == [("s1", {})]
    assert named.autocommit is True
    assert named.close_calls == 1
    assert result.query_outputs[0].session_name == "s1"
    assert result.query_outputs[1].session_name == "s1"


def test_named_session_options_must_match_first_open() -> None:
    default = _FakeConnection([])
    named = _FakeConnection([_Step("SELECT 1;", columns=["v"], rows=[(1,)])])
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT 1;",
            query_mode="nosort",
            session_name="s1",
            session_args=("user=alice",),
        ),
        Block(
            kind="query",
            line_no=2,
            sql="SELECT 1;",
            query_mode="nosort",
            session_name="s1",
            session_args=("user=bob",),
        ),
    ]

    with pytest.raises(ExecutionError, match="different options"):
        execute_blocks(default, blocks, session_connection_factory=lambda _name, _options: named)

    assert named.close_calls == 1


def test_named_session_open_transaction_fails_case_and_rolls_back() -> None:
    default = _FakeConnection([])
    named = _FakeConnection(
        [_Step("BEGIN;")],
        transaction_status="INTRANS",
    )
    blocks = [
        Block(kind="statement", line_no=1, sql="BEGIN;", statement_expect="ok", session_name="s1")
    ]

    with pytest.raises(ExecutionError, match="left an open transaction"):
        execute_blocks(default, blocks, session_connection_factory=lambda _name, _options: named)

    assert named.rollback_calls == 1
    assert named.close_calls == 1


def test_async_block_output_is_collected_at_await_position() -> None:
    default = _FakeConnection([_Step("SELECT 'before';", columns=["v"], rows=[("before",)])])
    writer = _FakeConnection([_Step("INSERT INTO t VALUES (1);", rowcount=1)])
    blocks = [
        Block(kind="query", line_no=1, sql="SELECT 'before';", query_mode="nosort"),
        Block(
            kind="statement",
            line_no=2,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="ok",
            session_name="writer",
            async_label="insert_one",
        ),
        Block(
            kind="await",
            line_no=3,
            sql="",
            await_label="insert_one",
            await_timeout_ms=1000,
        ),
    ]

    result = execute_blocks(
        default,
        blocks,
        session_connection_factory=lambda _name, _options: writer,
    )

    assert [output.sql for output in result.query_outputs] == [
        "SELECT 'before';",
        "INSERT INTO t VALUES (1);",
    ]
    assert result.query_outputs[1].session_name == "writer"
    assert writer.close_calls == 1


def test_async_block_must_be_awaited() -> None:
    default = _FakeConnection([])
    writer = _FakeConnection([_Step("INSERT INTO t VALUES (1);", rowcount=1)])
    blocks = [
        Block(
            kind="statement",
            line_no=1,
            sql="INSERT INTO t VALUES (1);",
            statement_expect="ok",
            session_name="writer",
            async_label="insert_one",
        )
    ]

    with pytest.raises(ExecutionError, match="unawaited async"):
        execute_blocks(
            default,
            blocks,
            session_connection_factory=lambda _name, _options: writer,
        )

    assert writer.close_calls == 1


def test_async_await_timeout_disconnects_session() -> None:
    default = _FakeConnection([])
    writer = _FakeConnection(
        [_Step("SELECT pg_sleep;", columns=["v"], rows=[("late",)], delay_seconds=0.2)]
    )
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT pg_sleep;",
            query_mode="nosort",
            session_name="writer",
            async_label="slow",
        ),
        Block(
            kind="await",
            line_no=2,
            sql="",
            await_label="slow",
            await_timeout_ms=10,
        ),
    ]

    with pytest.raises(ExecutionError, match="timed out"):
        execute_blocks(
            default,
            blocks,
            session_connection_factory=lambda _name, _options: writer,
        )

    assert writer.close_calls == 1


def test_wait_expect_retries_until_match() -> None:
    conn = _FakeConnection(
        [
            _Step("SELECT ready FROM state;", columns=["ready"], rows=[(False,)]),
            _Step("SELECT ready FROM state;", columns=["ready"], rows=[(True,)]),
        ]
    )
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT ready FROM state;",
            query_mode="nosort",
            wait_expect_interval_ms=1,
            wait_expect_timeout_ms=100,
        )
    ]
    calls = 0

    def matcher(_index: int, output: Any) -> str | None:
        nonlocal calls
        calls += 1
        return None if output.rows == [["true"]] else "not ready"

    result = execute_blocks(conn, blocks, wait_expect_matcher=matcher)

    assert calls == 2
    assert result.query_outputs[0].rows == [["true"]]
    assert conn.steps == []


def test_wait_expect_timeout_reports_last_mismatch() -> None:
    conn = _FakeConnection(
        [
            _Step("SELECT ready FROM state;", columns=["ready"], rows=[(False,)])
            for _ in range(10)
        ]
    )
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="SELECT ready FROM state;",
            query_mode="nosort",
            wait_expect_interval_ms=1,
            wait_expect_timeout_ms=5,
        )
    ]

    with pytest.raises(ExecutionError, match="not ready"):
        execute_blocks(
            conn,
            blocks,
            wait_expect_matcher=lambda _index, _output: "not ready",
        )
